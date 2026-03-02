use futures::{SinkExt, channel::mpsc::unbounded};
use futures_util::stream::StreamExt;
use solana_pubkey::Pubkey;
use std::{collections::HashMap, error::Error, sync::atomic::Ordering};
use tokio::task;
use tonic::{
    Request, Status,
    metadata::AsciiMetadataValue,
    transport::ClientTlsConfig,
};
use tracing::{Level, info};

use crate::{
    config::{Config, Endpoint},
    proto::geyser::{
        SubscribePreprocessedRequest, SubscribePreprocessedRequestFilterTransactions,
        SubscribeRequestPing,
        geyser_client::GeyserClient,
        subscribe_preprocessed_update::UpdateOneof,
    },
    utils::{TransactionData, get_current_timestamp, open_log_file, write_log_entry},
};

use super::{
    GeyserProvider, ProviderContext,
    common::{
        TransactionAccumulator, build_signature_envelope, enqueue_signature, fatal_connection_error,
    },
};

#[derive(Clone)]
struct XTokenInterceptor {
    x_token: Option<AsciiMetadataValue>,
}

impl tonic::service::Interceptor for XTokenInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(token) = self.x_token.clone() {
            request.metadata_mut().insert("x-token", token);
        }
        Ok(request)
    }
}

pub struct HeliusPreprocessedProvider;

impl GeyserProvider for HeliusPreprocessedProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        context: ProviderContext,
    ) -> task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        task::spawn(async move { process_helius_endpoint(endpoint, config, context).await })
    }
}

async fn process_helius_endpoint(
    endpoint: Endpoint,
    config: Config,
    context: ProviderContext,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let ProviderContext {
        shutdown_tx,
        mut shutdown_rx,
        start_wallclock_secs,
        start_instant,
        comparator,
        signature_tx,
        shared_counter,
        shared_shutdown,
        target_transactions,
        total_producers,
        progress,
    } = context;

    let signature_sender = signature_tx;
    let account_pubkey = config.account.parse::<Pubkey>()?;
    let endpoint_name = endpoint.name.clone();

    let mut log_file = if tracing::enabled!(Level::TRACE) {
        Some(open_log_file(&endpoint_name)?)
    } else {
        None
    };

    let endpoint_url = endpoint.url.clone();
    let x_token: Option<AsciiMetadataValue> = endpoint
        .x_token
        .as_ref()
        .filter(|token| !token.trim().is_empty())
        .map(|token| token.parse())
        .transpose()
        .map_err(|_| "Invalid x-token value")?;

    info!(endpoint = %endpoint_name, url = %endpoint_url, "Connecting");

    let channel = tonic::transport::Channel::from_shared(endpoint_url.clone())
        .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?
        .tls_config(ClientTlsConfig::new().with_native_roots())
        .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?
        .connect()
        .await
        .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));

    let interceptor = XTokenInterceptor { x_token };
    let mut client = GeyserClient::with_interceptor(channel, interceptor)
        .max_decoding_message_size(64 * 1024 * 1024);

    info!(endpoint = %endpoint_name, "Connected");

    let filter = SubscribePreprocessedRequestFilterTransactions {
        vote: Some(false),
        signature: None,
        account_include: vec![config.account.clone()],
        account_exclude: vec![],
        account_required: vec![],
    };

    let mut transactions = HashMap::new();
    transactions.insert("tx".to_string(), filter);

    let request = SubscribePreprocessedRequest {
        transactions,
        ping: None,
    };

    let (mut subscribe_tx, subscribe_rx) = unbounded::<SubscribePreprocessedRequest>();
    subscribe_tx.send(request).await?;

    let response = client
        .subscribe_preprocessed(subscribe_rx)
        .await?;
    let mut stream = response.into_inner();

    let mut accumulator = TransactionAccumulator::new();
    let mut transaction_count = 0usize;

    loop {
        tokio::select! { biased;
            _ = shutdown_rx.recv() => {
                info!(endpoint = %endpoint_name, "Received stop signal");
                break;
            }

            message = stream.next() => {
                let Some(Ok(msg)) = message else {
                    if message.is_none() {
                        info!(endpoint = %endpoint_name, "Stream closed by server");
                    }
                    break;
                };

                match msg.update_oneof {
                    Some(UpdateOneof::Transaction(tx_update)) => {
                        let Some(tx_info) = tx_update.transaction else { continue };
                        let Some(tx) = tx_info.transaction else { continue };
                        let Some(tx_msg) = tx.message else { continue };

                        let has_account = tx_msg
                            .account_keys
                            .iter()
                            .any(|key: &Vec<u8>| key.as_slice() == account_pubkey.as_ref());

                        if !has_account {
                            continue;
                        }

                        let wallclock = get_current_timestamp();
                        let elapsed = start_instant.elapsed();
                        let signature = bs58::encode(&tx_info.signature).into_string();

                        if let Some(file) = log_file.as_mut() {
                            write_log_entry(file, wallclock, &endpoint_name, &signature)?;
                        }

                        let tx_data = TransactionData {
                            wallclock_secs: wallclock,
                            elapsed_since_start: elapsed,
                            start_wallclock_secs,
                        };

                        let updated = accumulator.record(signature.clone(), tx_data.clone());

                        if updated && let Some(envelope) = build_signature_envelope(
                            &comparator,
                            &endpoint_name,
                            &signature,
                            tx_data,
                            total_producers,
                        ) {
                            if let Some(target) = target_transactions {
                                let shared = shared_counter.fetch_add(1, Ordering::AcqRel) + 1;
                                if let Some(tracker) = progress.as_ref() {
                                    tracker.record(shared);
                                }
                                if shared >= target && !shared_shutdown.swap(true, Ordering::AcqRel) {
                                    info!(endpoint = %endpoint_name, target, "Reached shared signature target; broadcasting shutdown");
                                    let _ = shutdown_tx.send(());
                                }
                            }

                            if let Some(sender) = signature_sender.as_ref() {
                                enqueue_signature(sender, &endpoint_name, &signature, envelope);
                            }
                        }

                        transaction_count += 1;
                    }
                    Some(UpdateOneof::Ping(_)) => {
                        subscribe_tx
                            .send(SubscribePreprocessedRequest {
                                transactions: HashMap::new(),
                                ping: Some(SubscribeRequestPing { id: 1 }),
                            })
                            .await?;
                    }
                    Some(UpdateOneof::Pong(_)) | None => {}
                }
            }
        }
    }

    let unique_signatures = accumulator.len();
    let collected = accumulator.into_inner();
    comparator.add_batch(&endpoint_name, collected);
    info!(
        endpoint = %endpoint_name,
        total_transactions = transaction_count,
        unique_signatures,
        "Stream closed after dispatching transactions"
    );
    Ok(())
}
