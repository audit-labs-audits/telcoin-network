//! Error types for spawning a full node

use eyre::ErrReport;
use narwhal_executor::SubscriberError;
use reth::init::InitDatabaseError;
use reth_beacon_consensus::BeaconForkChoiceUpdateError;
use reth_interfaces::RethError;
use reth_provider::ProviderError;
use thiserror::Error;
use tn_types::WorkerId;

#[derive(Debug, Error)]
pub enum NodeError {
    #[error("Failure while booting node: {0}")]
    NodeBootstrapError(#[from] SubscriberError),

    #[error("Node is already running")]
    NodeAlreadyRunning,

    #[error("Worker nodes with ids {0:?} already running")]
    WorkerNodesAlreadyRunning(Vec<WorkerId>),

    /// Error when creating a new registry
    #[error(transparent)]
    Registry(#[from] prometheus::Error),

    /// Error types when creating the execution layer for node.
    #[error(transparent)]
    Execution(#[from] ExecutionError),
}

/// Error types when spawning the ExecutionNode
#[derive(Debug, Error)]
pub enum ExecutionError {
    /// Error from init genesis
    #[error(transparent)]
    InitGenesis(#[from] InitDatabaseError),

    /// Error creating the blockchain tree
    #[error(transparent)]
    Reth(#[from] RethError),

    /// Error creating temp db
    #[error(transparent)]
    Tempdb(#[from] std::io::Error),

    #[error(transparent)]
    Report(#[from] ErrReport),

    /// Creating blockchain provider
    #[error(transparent)]
    Provider(#[from] ProviderError),

    /// Forkchoice updated to genesis when the node spawns.
    #[error(transparent)]
    FinalizeGenesis(#[from] BeaconForkChoiceUpdateError),

    /// Attempt to retrieve the optional [TaskManager]'s task executor.
    #[error("TaskManager is not running.")]
    TaskManagerNotStarted,

    /// Worker id is not included in the execution node's known worker hashmap.
    #[error("Worker not found: {0:?}")]
    WorkerNotFound(WorkerId),

    /// RPC handle unable to provide socket address for worker bc it's not running
    #[error("Worker {0:?} does not have a {1} server running")]
    WorkerRPCNotRunning(WorkerId, String),
}
