//! Primary Receiver Handler is the entrypoint for peer network requests.
//!
//! This module includes implementations for when the primary receives network
//! requests from it's own workers and other primaries.

use crate::{proposer::OurDigestMessage, synchronizer::Synchronizer, ConsensusBus};
use handler::RequestHandler;
pub use message::{MissingCertificatesRequest, PrimaryRequest, PrimaryResponse};
use std::sync::Arc;
use tn_config::ConsensusConfig;
use tn_network::{error::LocalClientError, WorkerToPrimaryClient};
use tn_network_libp2p::{
    types::{IntoResponse as _, NetworkEvent, NetworkHandle},
    GossipMessage, PeerId, ResponseChannel,
};
use tn_network_types::{WorkerOthersBatchMessage, WorkerOwnBatchMessage};
use tn_storage::{traits::Database, PayloadStore};
use tn_types::{BlockHash, Certificate, Header, Noticer, TaskManager, TnSender};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, warn};
pub mod client;
pub mod handler;
mod message;

#[cfg(test)]
#[path = "../tests/network_tests.rs"]
mod network_tests;

/// Convenience type for Primary network.
pub(crate) type Req = PrimaryRequest;
/// Convenience type for Primary network.
pub(crate) type Res = PrimaryResponse;

/// Handle inter-node communication between primaries.
pub struct PrimaryNetwork<DB> {
    /// Receiver for network events.
    network_events: mpsc::Receiver<NetworkEvent<Req, Res>>,
    /// Network handle to send commands.
    network_handle: NetworkHandle<Req, Res>,
    /// Request handler to process requests and return responses.
    request_handler: RequestHandler<DB>,
    /// Shutdown notification.
    shutdown_rx: Noticer,
}

impl<DB> PrimaryNetwork<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub fn new(
        network_events: mpsc::Receiver<NetworkEvent<Req, Res>>,
        network_handle: NetworkHandle<Req, Res>,
        consensus_config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBus,
        synchronizer: Arc<Synchronizer<DB>>,
    ) -> Self {
        let shutdown_rx = consensus_config.shutdown().subscribe();
        let request_handler = RequestHandler::new(consensus_config, consensus_bus, synchronizer);
        Self { network_events, network_handle, request_handler, shutdown_rx }
    }

    /// Run the network.
    pub fn spawn(mut self, task_manager: &TaskManager) {
        task_manager.spawn_task("latest block", async move {
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
                PrimaryRequest::Vote { header, parents } => {
                    self.process_vote_request(peer, header, parents, channel, cancel);
                }
                PrimaryRequest::MissingCertificates { inner } => {
                    self.process_request_for_missing_certs(peer, inner, channel, cancel)
                }
                PrimaryRequest::ConsensusHeader { number, hash } => {
                    self.process_consensus_output_request(peer, number, hash, channel, cancel)
                }
            },
            NetworkEvent::Gossip(msg) => {
                self.process_gossip(msg);
            }
        }
    }

    /// Process vote request.
    ///
    /// Spawn a task to evaluate a peer's proposed header and return a response.
    fn process_vote_request(
        &self,
        peer: PeerId,
        header: Header,
        parents: Vec<Certificate>,
        channel: ResponseChannel<PrimaryResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        tokio::spawn(async move {
            tokio::select! {
                vote = request_handler.vote(peer, header, parents) => {
                    let response = vote.into_response();
                    let _ = network_handle.send_response(response, channel).await;
                }
                // cancel notification from network layer
                _ = cancel => (),
            }
        });
    }

    /// Attempt to retrieve certificates for a peer that's missing them.
    fn process_request_for_missing_certs(
        &self,
        _peer: PeerId,
        request: MissingCertificatesRequest,
        channel: ResponseChannel<PrimaryResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        tokio::spawn(async move {
            tokio::select! {
                certs = request_handler.retrieve_missing_certs(request) => {
                    let response = certs.into_response();

                    // TODO: penalize peer's reputation for bad request
                    // if response.is_err() { }

                    let _ = network_handle.send_response(response, channel).await;
                }
                // cancel notification from network layer
                _ = cancel => (),
            }
        });
    }

    /// Attempt to retrieve consensus chain header from the database.
    fn process_consensus_output_request(
        &self,
        _peer: PeerId,
        number: Option<u64>,
        hash: Option<BlockHash>,
        channel: ResponseChannel<PrimaryResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        tokio::spawn(async move {
            tokio::select! {
                header =
                    request_handler.retrieve_consensus_header(number, hash) => {
                        let response = header.into_response();
                        // TODO: penalize peer's reputation for bad request
                        // if response.is_err() { }
                        let _ = network_handle.send_response(response, channel).await;
                    }
                // cancel notification from network layer
                _ = cancel => (),
            }
        });
    }

    /// Process gossip from committee.
    fn process_gossip(&self, msg: GossipMessage) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        tokio::spawn(async move {
            if let Err(e) = request_handler.process_gossip(&msg).await {
                warn!(target: "primary::network", ?e, "process_gossip");
                // TODO: peers don't track reputation yet
                //
                // NOTE: the network ensures the peer id is present before forwarding the msg
                if let Some(peer_id) = msg.source {
                    if let Err(e) = network_handle.set_application_score(peer_id, -100.0).await {
                        error!(target: "primary::network", ?e, "failed to penalize malicious peer")
                    }
                }

                // match on error to lower peer score
                //todo!();
            }
        });
    }
}

/// Defines how the network receiver handles incoming workers messages.
#[derive(Clone)]
pub(super) struct WorkerReceiverHandler<DB> {
    consensus_bus: ConsensusBus,
    payload_store: PayloadStore<DB>,
}

impl<DB: Database> WorkerReceiverHandler<DB> {
    /// Create a new instance of Self.
    pub fn new(consensus_bus: ConsensusBus, payload_store: PayloadStore<DB>) -> Self {
        Self { consensus_bus, payload_store }
    }
}

#[async_trait::async_trait]
impl<DB: Database> WorkerToPrimaryClient for WorkerReceiverHandler<DB> {
    async fn report_own_batch(
        &self,
        message: WorkerOwnBatchMessage,
    ) -> Result<(), LocalClientError> {
        let (tx_ack, rx_ack) = oneshot::channel();
        let response = self
            .consensus_bus
            .our_digests()
            .send(OurDigestMessage {
                digest: message.digest,
                worker_id: message.worker_id,
                timestamp: message.timestamp,
                ack_channel: tx_ack,
            })
            .await
            .map_err(|e| LocalClientError::Internal(e.to_string()))?;

        // If we are ok, then wait for the ack
        rx_ack.await.map_err(|e| LocalClientError::Internal(e.to_string()))?;

        Ok(response)
    }

    async fn report_others_batch(
        &self,
        message: WorkerOthersBatchMessage,
    ) -> Result<(), LocalClientError> {
        self.payload_store
            .write(&message.digest, &message.worker_id)
            .map_err(|e| LocalClientError::Internal(e.to_string()))?;
        Ok(())
    }
}
