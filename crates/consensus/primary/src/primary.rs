//! The Primary type

use crate::{
    certificate_fetcher::CertificateFetcher,
    certifier::Certifier,
    consensus::LeaderSchedule,
    network::{PrimaryNetwork, PrimaryRequest, PrimaryResponse, WorkerReceiverHandler},
    proposer::Proposer,
    state_handler::StateHandler,
    state_sync::StateSynchronizer,
    ConsensusBus,
};
use anemo::PeerId;
use std::sync::Arc;
use tn_config::ConsensusConfig;
use tn_network_libp2p::types::{NetworkEvent, NetworkHandle};
use tn_storage::traits::Database;
use tn_types::{traits::EncodeDecodeBase64, TaskManager};
use tokio::sync::mpsc;
use tracing::info;

#[cfg(test)]
#[path = "tests/primary_tests.rs"]
pub mod primary_tests;

pub struct Primary<DB> {
    /// The Primary's network.
    network_handle: NetworkHandle<PrimaryRequest, PrimaryResponse>,
    // Hold onto the network event stream until spawn "takes" it.
    primary_network: Option<PrimaryNetwork<DB>>,
    state_sync: StateSynchronizer<DB>,
}

impl<DB: Database> Primary<DB> {
    pub fn new(
        config: ConsensusConfig<DB>,
        consensus_bus: &ConsensusBus,
        network_handle: NetworkHandle<PrimaryRequest, PrimaryResponse>,
        network_event_stream: mpsc::Receiver<NetworkEvent<PrimaryRequest, PrimaryResponse>>,
    ) -> Self {
        // Write the parameters to the logs.
        config.parameters().tracing();

        // Some info statements
        let own_peer_id = PeerId(config.key_config().primary_network_public_key().0.to_bytes());
        info!(
            "Boot primary node with peer id {} and public key {}",
            own_peer_id,
            config.authority().protocol_key().encode_base64(),
        );

        let worker_receiver_handler = WorkerReceiverHandler::new(
            consensus_bus.clone(),
            config.node_storage().payload_store.clone(),
        );

        // TODO: remove this
        config
            .local_network()
            .set_worker_to_primary_local_handler(Arc::new(worker_receiver_handler));

        let state_sync = StateSynchronizer::new(config.clone(), consensus_bus.clone());
        let primary_network = PrimaryNetwork::new(
            network_event_stream,
            network_handle.clone(),
            config.clone(),
            consensus_bus.clone(),
            state_sync.clone(),
        );

        Self { network_handle, primary_network: Some(primary_network), state_sync }
    }

    /// Spawns the primary.
    pub fn spawn(
        &mut self,
        config: ConsensusConfig<DB>,
        consensus_bus: &ConsensusBus,
        leader_schedule: LeaderSchedule,
        task_manager: &TaskManager,
    ) {
        if consensus_bus.node_mode().borrow().is_active_cvv() {
            self.state_sync.spawn(task_manager);
        }

        info!(
            "Primary {} listening to network admin messages on 127.0.0.1:{}",
            config.authority().id(),
            config.parameters().network_admin_server.primary_network_admin_server_port
        );

        Certifier::spawn(
            config.clone(),
            consensus_bus.clone(),
            self.state_sync.clone(),
            self.network_handle.clone(),
            task_manager,
        );

        // The `CertificateFetcher` waits to receive all the ancestors of a certificate before
        // looping it back to the `Synchronizer` for further processing.
        CertificateFetcher::spawn(
            config.clone(),
            self.network_handle.clone(),
            consensus_bus.clone(),
            self.state_sync.clone(),
            task_manager,
        );

        // Only run the proposer task if we are a CVV.
        if consensus_bus.node_mode().borrow().is_cvv() {
            // When the `Synchronizer` collects enough parent certificates, the `Proposer` generates
            // a new header with new block digests from our workers and sends it to the `Certifier`.
            let proposer = Proposer::new(config.clone(), consensus_bus.clone(), leader_schedule);

            proposer.spawn(task_manager);
        }

        // Keeps track of the latest consensus round and allows other tasks to clean up their their
        // internal state
        StateHandler::spawn(
            config.authority().id(),
            consensus_bus,
            config.shutdown().subscribe(),
            self.network_handle.clone(),
            task_manager,
        );
        let primary_network = self.primary_network.take().expect("no network event stream!");
        primary_network.spawn(task_manager);

        // NOTE: This log entry is used to compute performance.
        info!(
            "Primary {} successfully booted on {}",
            config.authority().id(),
            config.authority().primary_network_address()
        );
    }

    /// Return a reference to the Primary's network.
    pub fn network_handle(&self) -> &NetworkHandle<PrimaryRequest, PrimaryResponse> {
        &self.network_handle
    }
}
