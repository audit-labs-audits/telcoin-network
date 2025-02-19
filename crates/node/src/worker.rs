//! Hierarchical type to hold tasks spawned for a worker in the network.
use std::sync::Arc;
use tn_config::ConsensusConfig;
use tn_storage::traits::Database as ConsensusDatabase;
use tn_types::{BatchValidation, WorkerId};
use tn_worker::{
    metrics::Metrics, quorum_waiter::QuorumWaiter, BatchProvider, Worker, WorkerNetworkHandle,
};
use tokio::sync::RwLock;
use tracing::instrument;

pub struct WorkerNodeInner<CDB> {
    /// The worker's id
    id: WorkerId,
    /// The consensus configuration.
    consensus_config: ConsensusConfig<CDB>,
}

impl<CDB: ConsensusDatabase> WorkerNodeInner<CDB> {
    /// Starts the worker node with the provided info. If the node is already running then this
    /// method will return an error instead.
    #[instrument(name = "worker", skip_all)]
    async fn start(
        &mut self,
        validator: Arc<dyn BatchValidation>,
        network_handle: WorkerNetworkHandle,
    ) -> eyre::Result<BatchProvider<CDB, QuorumWaiter>> {
        let metrics = Metrics::default();

        let batch_provider = Worker::new_batch_provider(
            self.id,
            validator,
            metrics,
            self.consensus_config.clone(),
            network_handle,
        );

        Ok(batch_provider)
    }
}

#[derive(Clone)]
pub struct WorkerNode<CDB> {
    internal: Arc<RwLock<WorkerNodeInner<CDB>>>,
}

impl<CDB: ConsensusDatabase> WorkerNode<CDB> {
    pub fn new(id: WorkerId, consensus_config: ConsensusConfig<CDB>) -> WorkerNode<CDB> {
        let inner = WorkerNodeInner { id, consensus_config };

        Self { internal: Arc::new(RwLock::new(inner)) }
    }

    pub async fn start(
        &self,
        validator: Arc<dyn BatchValidation>,
        network_handle: WorkerNetworkHandle,
    ) -> eyre::Result<BatchProvider<CDB, QuorumWaiter>> {
        let mut guard = self.internal.write().await;
        guard.start(validator, network_handle).await
    }
}
