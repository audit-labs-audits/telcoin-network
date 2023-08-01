use lattice_executor::SubscriberError;
use thiserror::Error;
use tn_types::consensus::WorkerId;

/// Error types for running CL.
#[derive(Debug, Error, Clone)]
pub enum NodeError {
    #[error("Failure while booting node: {0}")]
    NodeBootstrapError(#[from] SubscriberError),

    #[error("Node is already running")]
    NodeAlreadyRunning,

    #[error("Worker nodes with ids {0:?} already running")]
    WorkerNodesAlreadyRunning(Vec<WorkerId>),
}
