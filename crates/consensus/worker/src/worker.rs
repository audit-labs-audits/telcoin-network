//! The main worker type

use crate::{
    batch_fetcher::BatchFetcher,
    batch_provider::BatchProvider,
    metrics::{Metrics, WorkerMetrics},
    network::{
        PrimaryReceiverHandler, WorkerNetwork, WorkerNetworkHandle, WorkerRequest, WorkerResponse,
    },
    quorum_waiter::QuorumWaiter,
};
use std::sync::Arc;
use tn_config::ConsensusConfig;
use tn_network_libp2p::{network_public_key_to_libp2p, types::NetworkEvent};
use tn_network_types::local::LocalNetwork;
use tn_storage::traits::Database;
use tn_types::{BatchValidation, TaskManager, WorkerId};
use tokio::sync::mpsc;
use tracing::info;

/// The default channel capacity for each channel of the worker.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// The main worker struct that holds all information needed for worker.
pub struct Worker {}

impl Worker {
    /// Spawn the worker.
    ///
    /// Create an instance of `Self` and start all tasks to participate in consensus.
    pub fn spawn<DB: Database>(
        id: WorkerId,
        validator: Arc<dyn BatchValidation>,
        metrics: Metrics,
        consensus_config: ConsensusConfig<DB>,
        task_manager: &TaskManager,
        network_handle: WorkerNetworkHandle,
        network_event_stream: mpsc::Receiver<NetworkEvent<WorkerRequest, WorkerResponse>>,
    ) -> BatchProvider<DB, QuorumWaiter> {
        let worker_name = consensus_config.key_config().worker_network_public_key();
        let worker_peer_id = network_public_key_to_libp2p(&worker_name);
        info!("Boot worker node with id {} peer id {:?}", id, worker_peer_id,);

        let node_metrics = metrics.worker_metrics.clone();

        // Receive incoming messages from other workers.
        WorkerNetwork::new(
            network_event_stream,
            network_handle.clone(),
            consensus_config.clone(),
            id,
            validator.clone(),
        )
        .spawn(task_manager);

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
        /*
                let mut peer_types = HashMap::new();

                let other_workers = consensus_config
                    .worker_cache()
                    .others_workers_by_id(consensus_config.authority().protocol_key(), &id)
                    .into_iter()
                    .map(|(_, info)| (info.name, info.worker_address));

                // Add other workers we want to talk with to the known peers set.
                for (public_key, address) in other_workers {
                    let (peer_id, address) = Self::add_peer_in_network(&network, public_key, &address);
                    peer_types.insert(peer_id, "other_worker".to_string());
                    info!(target: "worker::worker", "Adding others workers with peer id {} and address {}", peer_id, address);
                }

                // Connect worker to its corresponding primary.
                let (peer_id, address) = Self::add_peer_in_network(
                    &network,
                    consensus_config.authority().network_key(),
                    consensus_config.authority().primary_network_address(),
                );
                peer_types.insert(peer_id, "our_primary".to_string());
                info!(target: "worker::worker", "Adding our primary with peer id {} and address {}", peer_id, address);

                // update the peer_types with the "other_primary". We do not add them in the Network
                // struct, otherwise the networking library will try to connect to it
                let other_primaries: Vec<(AuthorityIdentifier, Multiaddr, NetworkPublicKey)> =
                    consensus_config.committee().others_primaries_by_id(consensus_config.authority().id());
                for (_, _, network_key) in other_primaries {
                    peer_types.insert(PeerId(network_key.0.to_bytes()), "other_primary".to_string());
                }

                let _ = tn_network::connectivity::ConnectionMonitor::spawn(
                    network_handle.clone(),
                    metrics.network_connection_metrics.clone(),
                    peer_types,
                    consensus_config.shutdown().subscribe(),
                    task_manager,
                );

                // TODO: revisit this soon
                let network_admin_server_base_port = consensus_config
                    .parameters()
                    .network_admin_server
                    .worker_network_admin_server_base_port
                    .checked_add(id)
                    .expect("only 1 worker for now, so 0 added to valid admin server port");
                info!(target: "worker::worker",
                    "Worker {} listening to network admin messages on 127.0.0.1:{}",
                    id, network_admin_server_base_port
                );

                tn_network::admin::start_admin_server(
                    network_admin_server_base_port,
                    network.clone(),
                    consensus_config.shutdown().subscribe(),
                    task_manager,
                );
        */
        let batch_provider = Self::new_batch_provider(
            id,
            &consensus_config,
            node_metrics,
            consensus_config.local_network().clone(),
            network_handle.clone(),
        );

        /* XXXX Self::shutdown_network_listener(
            consensus_config.shutdown().subscribe(),
            network_handle.clone(),
            task_manager,
        );*/

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

    /* XXXX
    /// Spawns a task responsible for explicitly shutting down the network
    /// when a shutdown signal has been sent to the node.
    fn shutdown_network_listener(
        rx_shutdown: Noticer,
        network_handle: NetworkHandle<WorkerRequest, WorkerResponse>,
        task_manager: &TaskManager,
    ) {
        task_manager.spawn_task(
            "worker shutdown network listener task",
            monitored_future!(
                async move {
                    rx_shutdown.await;
                    if let Err(e) = network.shutdown().await {
                        error!(target: "worker::worker", "Error while shutting down network: {e}");
                    }
                    info!(target: "worker::worker", "Worker network server shutdown");
                },
                "WorkerShutdownNetworkListenerTask"
            ),
        );
    }
    */

    /// Builds a new batch provider responsible for handling client transactions.
    fn new_batch_provider<DB: Database>(
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
