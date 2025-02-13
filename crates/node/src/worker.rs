//! Hierarchical type to hold tasks spawned for a worker in the network.
use std::sync::Arc;
use tn_config::ConsensusConfig;
use tn_network_libp2p::{network_public_key_to_libp2p, types::NetworkEvent, PeerId};
use tn_storage::traits::Database as ConsensusDatabase;
use tn_types::{BatchValidation, TaskManager, WorkerId};
use tn_worker::{
    metrics::Metrics, quorum_waiter::QuorumWaiter, BatchProvider, Worker, WorkerNetworkHandle,
    WorkerRequest, WorkerResponse,
};
use tokio::sync::{mpsc, RwLock};
use tracing::instrument;

pub struct WorkerNodeInner<CDB> {
    /// The worker's id
    id: WorkerId,
    /// The consensus configuration.
    consensus_config: ConsensusConfig<CDB>,
    /// Peer ID used for local connections.
    own_peer_id: Option<PeerId>,
}

impl<CDB: ConsensusDatabase> WorkerNodeInner<CDB> {
    /// Starts the worker node with the provided info. If the node is already running then this
    /// method will return an error instead.
    #[instrument(name = "worker", skip_all)]
    async fn start(
        &mut self,
        validator: Arc<dyn BatchValidation>,
        network_handle: WorkerNetworkHandle,
        network_event_stream: mpsc::Receiver<NetworkEvent<WorkerRequest, WorkerResponse>>,
    ) -> eyre::Result<(TaskManager, BatchProvider<CDB, QuorumWaiter>)> {
        let task_manager = TaskManager::new("Worker Task Manager");
        self.own_peer_id = Some(network_public_key_to_libp2p(
            &self.consensus_config.key_config().primary_network_public_key(),
        ));

        let metrics = Metrics::default();

        let batch_provider = Worker::spawn(
            self.id,
            validator,
            metrics,
            self.consensus_config.clone(),
            &task_manager,
            network_handle,
            network_event_stream,
        );

        Ok((task_manager, batch_provider))
    }
}

#[derive(Clone)]
pub struct WorkerNode<CDB> {
    internal: Arc<RwLock<WorkerNodeInner<CDB>>>,
}

impl<CDB: ConsensusDatabase> WorkerNode<CDB> {
    pub fn new(id: WorkerId, consensus_config: ConsensusConfig<CDB>) -> WorkerNode<CDB> {
        let inner = WorkerNodeInner { id, consensus_config, own_peer_id: None };

        Self { internal: Arc::new(RwLock::new(inner)) }
    }

    pub async fn start(
        &self,
        validator: Arc<dyn BatchValidation>,
        network_handle: WorkerNetworkHandle,
        network_event_stream: mpsc::Receiver<NetworkEvent<WorkerRequest, WorkerResponse>>,
    ) -> eyre::Result<(TaskManager, BatchProvider<CDB, QuorumWaiter>)> {
        let mut guard = self.internal.write().await;
        guard.start(validator, network_handle, network_event_stream).await
    }
}
