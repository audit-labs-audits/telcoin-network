//! The main worker type

use crate::{
    batch_fetcher::BatchFetcher,
    batch_provider::BatchProvider,
    metrics::{Metrics, WorkerMetrics},
    network::{PrimaryReceiverHandler, WorkerNetworkHandle},
    quorum_waiter::QuorumWaiter,
};
use std::sync::Arc;
use tn_config::ConsensusConfig;
use tn_network_libp2p::network_public_key_to_libp2p;
use tn_network_types::local::LocalNetwork;
use tn_storage::traits::Database;
use tn_types::{BatchValidation, WorkerId};
use tracing::info;

/// The default channel capacity for each channel of the worker.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// The main worker struct that holds all information needed for worker.
pub struct Worker {}

impl Worker {
    /// Spawn the worker.
    ///
    /// Create an instance of `Self` and start all tasks to participate in consensus.
    pub fn new_batch_provider<DB: Database>(
        id: WorkerId,
        validator: Arc<dyn BatchValidation>,
        metrics: Metrics,
        consensus_config: ConsensusConfig<DB>,
        network_handle: WorkerNetworkHandle,
    ) -> BatchProvider<DB, QuorumWaiter> {
        let worker_name = consensus_config.key_config().worker_network_public_key();
        let worker_peer_id = network_public_key_to_libp2p(&worker_name);
        info!("Boot worker node with id {} peer id {:?}", id, worker_peer_id,);

        let node_metrics = metrics.worker_metrics.clone();

        let batch_fetcher = BatchFetcher::new(
            worker_name,
            network_handle.clone(),
            consensus_config.node_storage().batch_store.clone(),
            node_metrics.clone(),
        );
        consensus_config.local_network().set_primary_to_worker_local_handler(
            worker_peer_id,
            Arc::new(PrimaryReceiverHandler {
                id,
                committee: consensus_config.committee().clone(),
                worker_cache: consensus_config.worker_cache().clone(),
                store: consensus_config.database().clone(),
                request_batches_timeout: consensus_config.parameters().sync_retry_delay,
                network: Some(network_handle.clone()),
                batch_fetcher: Some(batch_fetcher),
                validator,
            }),
        );
        let batch_provider = Self::new_batch_provider_internal(
            id,
            &consensus_config,
            node_metrics,
            consensus_config.local_network().clone(),
            network_handle.clone(),
        );

        // NOTE: This log entry is used to compute performance.
        info!(target: "worker::worker",
            "Worker {} successfully booted on {}",
            id,
            consensus_config
                .worker_cache()
                .worker(consensus_config.authority().protocol_key(), &id)
                .expect("Our public key or worker id is not in the worker cache")
                .transactions
        );

        batch_provider
    }

    /// Builds a new batch provider responsible for handling client transactions.
    fn new_batch_provider_internal<DB: Database>(
        id: WorkerId,
        consensus_config: &ConsensusConfig<DB>,
        node_metrics: Arc<WorkerMetrics>,
        client: LocalNetwork,
        network_handle: WorkerNetworkHandle,
    ) -> BatchProvider<DB, QuorumWaiter> {
        info!(target: "worker::worker", "Starting handler for transactions");

        // The `QuorumWaiter` waits for 2f authorities to acknowledge receiving the batch
        // before forwarding the batch to the `Processor`
        let quorum_waiter = QuorumWaiter::new(
            consensus_config.authority().clone(),
            id,
            consensus_config.committee().clone(),
            consensus_config.worker_cache().clone(),
            network_handle,
            node_metrics.clone(),
        );

        BatchProvider::new(
            id,
            quorum_waiter,
            node_metrics,
            client,
            consensus_config.database().clone(),
            consensus_config.parameters().batch_vote_timeout,
        )
    }
}
