//! Error types emitted by types or implementations of this crate.

use revm::primitives::EVMError;
use tn_types::execution::H256;
use tokio::sync::oneshot;

use super::BatchBuilderServiceCommand;

/// Possible error variants during payload building.
#[derive(Debug, thiserror::Error)]
pub enum BatchBuilderError {
    /// Thrown whe the parent block is missing.
    #[error("missing parent block {0:?}")]
    MissingParentBlock(H256),
    /// An oneshot channels has been closed.
    #[error("sender has been dropped")]
    ChannelClosed,
    /// Other internal error
    #[error(transparent)]
    Internal(#[from] execution_interfaces::Error),
    /// Unrecoverable error during evm execution.
    #[error("evm execution error: {0:?}")]
    EvmExecutionError(EVMError<execution_interfaces::Error>),
    /// Thrown if the payload requests withdrawals before Shanghai activation.
    #[error("withdrawals set before Shanghai activation")]
    WithdrawalsBeforeShanghai,
    /// Thrown if the batch payload builder can't find the finalized state
    #[error("missing finalized state to build next batch")]
    LatticeBatch,
    /// Thrown if the batch payload builder can't find the latest state (after genesis)
    #[error("missing genesis state for next batch")]
    LatticeBatchFromGenesis,
    /// Thrwon if the batch payload can't create a timestamp when initialized the BlockEnv.
    #[error("Failed to capture System Time.")]
    LatticeBatchSystemTime(#[from] std::time::SystemTimeError),
    /// Thrown if the receiver for the oneshot channel is dropped by the worker which requested the batch.
    #[error("Channel closed before batch could be sent: {0:?}")]
    LatticeBatchChannelClosed(String),
    /// Thrown if the batch handle can't send to the batch builder service.
    #[error("Handle can't send to the batch builder service: {0:?}")]
    BatchBuilderHandleToService(#[from] tokio::sync::mpsc::error::SendError<BatchBuilderServiceCommand>),
    /// The built batch is empty. This error is required so the worker doesn't seal an empty batch.
    #[error("Built batch is empty.")]
    EmptyBatch,
}

impl From<oneshot::error::RecvError> for BatchBuilderError {
    fn from(_: oneshot::error::RecvError) -> Self {
        BatchBuilderError::ChannelClosed
    }
}
