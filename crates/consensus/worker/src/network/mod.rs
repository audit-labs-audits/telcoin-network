use std::{collections::HashSet, sync::Arc, time::Duration};

use error::WorkerNetworkError;
use handler::RequestHandler;
use message::{RequestBatchesResponse, WorkerGossip, WorkerRPCError};
pub use message::{WorkerRequest, WorkerResponse};
use tn_config::ConsensusConfig;
use tn_network_libp2p::{
    error::NetworkError,
    types::{IdentTopic, NetworkEvent, NetworkHandle, NetworkResult},
    GossipMessage, Multiaddr, PeerId, ResponseChannel,
};
use tn_network_types::{
    FetchBatchResponse, FetchBatchesRequest, PrimaryToWorkerClient, WorkerSynchronizeMessage,
};
use tn_storage::{
    tables::Batches,
    traits::{Database, DbTxMut},
};
use tn_types::{
    encode, network_public_key_to_libp2p, now, BatchValidation, BlockHash, Committee,
    NetworkPublicKey, Noticer, SealedBatch, TaskManager, WorkerCache, WorkerId,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tracing::{debug, error, trace, warn};

use crate::batch_fetcher::BatchFetcher;

mod error;
mod handler;
pub(crate) mod message;

/// Convenience type for Primary network.
pub(crate) type Req = WorkerRequest;
/// Convenience type for Primary network.
pub(crate) type Res = WorkerResponse;

#[derive(Clone)]
pub struct WorkerNetworkHandle {
    handle: NetworkHandle<Req, Res>,
}

impl WorkerNetworkHandle {
    pub fn new(handle: NetworkHandle<Req, Res>) -> Self {
        Self { handle }
    }

    /// Dial a peer.
    ///
    /// Return swarm error to caller.
    pub async fn dial(&self, peer_id: PeerId, peer_addr: Multiaddr) -> NetworkResult<()> {
        self.handle.dial(peer_id, peer_addr).await
    }

    /// Publish a certificate to the consensus network.
    /// NOTE: this is a publish, it is not specific to this client but here for convience.
    pub async fn publish_batch(&self, batch_digest: BlockHash) -> NetworkResult<()> {
        let data = encode(&WorkerGossip::Batch(batch_digest));
        self.handle.publish(IdentTopic::new("tn-worker"), data).await?;
        Ok(())
    }

    /// Report a new batch to a peer.
    pub async fn report_batch(
        &self,
        peer_id: PeerId,
        sealed_batch: SealedBatch,
    ) -> NetworkResult<()> {
        let request = WorkerRequest::ReportBatch { sealed_batch };
        let res = self.handle.send_request(request, peer_id).await?;
        let res = res.await??;
        match res {
            WorkerResponse::ReportBatch => Ok(()),
            WorkerResponse::RequestBatches { .. } => Err(NetworkError::RPCError(
                "Got wrong response, not a report batch is request batches!".to_string(),
            )),
            WorkerResponse::Error(WorkerRPCError(s)) => Err(NetworkError::RPCError(s)),
        }
    }

    /// Report a new batch to a peer.
    pub fn report_batch_to_peers(
        &self,
        peer_ids: Vec<PeerId>,
        sealed_batch: SealedBatch,
    ) -> Vec<JoinHandle<NetworkResult<()>>> {
        let mut result = vec![];
        for peer_id in peer_ids {
            let handle = self.clone();
            let batch = sealed_batch.clone();
            result.push(tokio::spawn(async move { handle.report_batch(peer_id, batch).await }));
        }
        result
    }

    /// Request a group of batches by hashes.
    pub async fn request_batches(
        &self,
        peer_id: PeerId,
        batch_digests: Vec<BlockHash>,
    ) -> NetworkResult<RequestBatchesResponse> {
        let request = WorkerRequest::RequestBatches { batch_digests };
        let res = self.handle.send_request(request, peer_id).await?;
        let res = res.await??;
        match res {
            WorkerResponse::ReportBatch => Err(NetworkError::RPCError(
                "Got wrong response, not a request batchs is report batch!".to_string(),
            )),
            WorkerResponse::RequestBatches(response) => Ok(response),
            WorkerResponse::Error(WorkerRPCError(s)) => Err(NetworkError::RPCError(s)),
        }
    }
}

/// Handle inter-node communication between primaries.
pub struct WorkerNetwork<DB> {
    /// Receiver for network events.
    network_events: mpsc::Receiver<NetworkEvent<Req, Res>>,
    /// Network handle to send commands.
    network_handle: WorkerNetworkHandle,
    // Request handler to process requests and return responses.
    request_handler: RequestHandler<DB>,
    /// Shutdown notification.
    shutdown_rx: Noticer,
}

impl<DB> WorkerNetwork<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub fn new(
        network_events: mpsc::Receiver<NetworkEvent<Req, Res>>,
        network_handle: WorkerNetworkHandle,
        consensus_config: ConsensusConfig<DB>,
        id: WorkerId,
        validator: Arc<dyn BatchValidation>,
    ) -> Self {
        let shutdown_rx = consensus_config.shutdown().subscribe();
        let request_handler = RequestHandler::new(id, validator, consensus_config);
        Self { network_events, network_handle, request_handler, shutdown_rx }
    }

    /// Run the network.
    pub fn spawn(mut self, task_manager: &TaskManager) {
        task_manager.spawn_task("worker network events", async move {
            loop {
                tokio::select!(
                    _ = &self.shutdown_rx => break,
                    event = self.network_events.recv() => {
                        match event {
                            Some(e) => self.process_network_event(e),
                            None => break,
                        }
                    }
                )
            }
        });
    }

    /// Handle events concurrently.
    fn process_network_event(&self, event: NetworkEvent<Req, Res>) {
        // match event
        match event {
            NetworkEvent::Request { peer, request, channel, cancel } => match request {
                WorkerRequest::ReportBatch { sealed_batch } => {
                    self.process_report_batch(peer, sealed_batch, channel, cancel);
                }
                WorkerRequest::RequestBatches { batch_digests } => {
                    self.process_request_batches(peer, batch_digests, channel, cancel);
                }
            },
            NetworkEvent::Gossip(msg) => {
                self.process_gossip(msg);
            }
        }
    }

    /// Process a new reported batch.
    ///
    /// Spawn a task to evaluate a peer's proposed header and return a response.
    fn process_report_batch(
        &self,
        _peer: PeerId,
        sealed_batch: SealedBatch,
        channel: ResponseChannel<WorkerResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        tokio::spawn(async move {
            tokio::select! {
                res = request_handler.process_report_batch(sealed_batch) => {
                    let response = match res {
                        Ok(()) => WorkerResponse::ReportBatch,
                        Err(err) => WorkerResponse::Error(message::WorkerRPCError(err.to_string())),
                    };
                    let _ = network_handle.handle.send_response(response, channel).await;
                },
                // cancel notification from network layer
                _ = cancel => (),
            }
        });
    }

    /// Attempt to return requested batches.
    fn process_request_batches(
        &self,
        _peer: PeerId,
        batch_digests: Vec<BlockHash>,
        channel: ResponseChannel<WorkerResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        tokio::spawn(async move {
            tokio::select! {
                res = request_handler.process_request_batches(batch_digests) => {
                    let response = match res {
                        Ok(r) => WorkerResponse::RequestBatches(r),
                        Err(err) => WorkerResponse::Error(message::WorkerRPCError(err.to_string())),
                    };

                    // TODO: penalize peer's reputation for bad request

                    let _ = network_handle.handle.send_response(response, channel).await;
                }
                // cancel notification from network layer
                _ = cancel => (),
            }
        });
    }

    /// Process gossip from a worker.
    fn process_gossip(&self, msg: GossipMessage) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        tokio::spawn(async move {
            if let Err(e) = request_handler.process_gossip(&msg).await {
                warn!(target: "worker::network", ?e, "process_gossip");
                // TODO: peers don't track reputation yet
                //
                // NOTE: the network ensures the peer id is present before forwarding the msg
                if let Some(peer_id) = msg.source {
                    if let Err(e) =
                        network_handle.handle.set_application_score(peer_id, -100.0).await
                    {
                        error!(target: "worker::network", ?e, "failed to penalize malicious peer")
                    }
                }

                // match on error to lower peer score
                //todo!();
            }
        });
    }
}

/// Defines how the network receiver handles incoming primary messages.
pub struct PrimaryReceiverHandler<DB> {
    /// The id of this worker.
    pub id: WorkerId,
    /// The committee information.
    pub committee: Committee,
    /// The worker information cache.
    pub worker_cache: WorkerCache,
    /// The batch store
    pub store: DB,
    /// Timeout on RequestBatches RPC.
    pub request_batches_timeout: Duration,
    /// Synchronize header payloads from other workers.
    pub network: Option<WorkerNetworkHandle>,
    /// Fetch certificate payloads from other workers.
    pub batch_fetcher: Option<BatchFetcher<DB>>,
    /// Validate incoming batches
    pub validator: Arc<dyn BatchValidation>,
}

#[async_trait::async_trait]
impl<DB: Database> PrimaryToWorkerClient for PrimaryReceiverHandler<DB> {
    async fn synchronize(
        &self,
        _worker_name: NetworkPublicKey,
        message: WorkerSynchronizeMessage,
    ) -> eyre::Result<()> {
        let Some(network) = self.network.as_ref() else {
            return Err(eyre::eyre!(
                "synchronize() is unsupported via RPC interface, please call via local worker handler instead".to_string(),
            ));
        };
        let mut missing = HashSet::new();
        for digest in message.digests.iter() {
            // Check if we already have the batch.
            match self.store.get::<Batches>(digest) {
                Ok(None) => {
                    missing.insert(*digest);
                    debug!("Requesting sync for batch {digest}");
                }
                Ok(Some(_)) => {
                    trace!("Digest {digest} already in store, nothing to sync");
                }
                Err(e) => {
                    return Err(eyre::eyre!("failed to read from batch store: {e:?}"));
                }
            };
        }
        if missing.is_empty() {
            return Ok(());
        }

        let worker_name = match self.worker_cache.worker(
            self.committee
                .authority(&message.target)
                .expect("own workers in worker cache")
                .protocol_key(),
            &self.id,
        ) {
            Ok(worker_info) => worker_info.name,
            Err(e) => {
                return Err(eyre::eyre!(
                    "The primary asked worker to sync with an unknown node: {e}"
                ));
            }
        };
        let peer = network_public_key_to_libp2p(&worker_name);

        // Attempt to retrieve missing batches.
        // Retried at a higher level in Synchronizer::sync_batches_internal().
        debug!("Sending RequestBatchesRequest to {worker_name}");

        let response = tokio::time::timeout(
            self.request_batches_timeout,
            network.request_batches(peer, missing.iter().cloned().collect()),
        )
        .await??;

        let sealed_batches_from_response: Vec<SealedBatch> = missing
            .iter()
            .cloned()
            .zip(response.batches)
            .map(|(digest, batch)| SealedBatch::new(batch, digest))
            .collect();

        for sealed_batch in sealed_batches_from_response.into_iter() {
            if !message.is_certified {
                // This batch is not part of a certificate, so we need to validate it.
                if let Err(err) = self.validator.validate_batch(sealed_batch.clone()) {
                    return Err(eyre::eyre!("Invalid batch: {err}"));
                }
            }

            let (mut batch, digest) = sealed_batch.split();
            if missing.remove(&digest) {
                // Set received_at timestamp for remote batch.
                batch.set_received_at(now());
                let mut tx = self.store.write_txn().map_err(|e| {
                    WorkerNetworkError::Internal(format!(
                        "failed to create batch transaction to commit: {e:?}"
                    ))
                })?;
                tx.insert::<Batches>(&digest, &batch).map_err(|e| {
                    WorkerNetworkError::Internal(format!(
                        "failed to batch transaction to commit: {e:?}"
                    ))
                })?;
                tx.commit().map_err(|e| {
                    WorkerNetworkError::Internal(format!("failed to commit batch: {e:?}"))
                })?;
            }
        }

        if missing.is_empty() {
            return Ok(());
        }
        Err(eyre::eyre!("failed to synchronize batches!".to_string()))
    }

    async fn fetch_batches(
        &self,
        _worker_name: NetworkPublicKey,
        request: FetchBatchesRequest,
    ) -> eyre::Result<FetchBatchResponse> {
        let Some(batch_fetcher) = self.batch_fetcher.as_ref() else {
            return Err(eyre::eyre!(
                "fetch_batches() is unsupported via RPC interface, please call via local worker handler instead".to_string(),
            ));
        };
        let batches = batch_fetcher.fetch(request.digests, request.known_workers).await;
        Ok(FetchBatchResponse { batches })
    }
}
