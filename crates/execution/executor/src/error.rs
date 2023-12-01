//! Error types for building and executing output from consensus.

use reth_interfaces::executor::BlockExecutionError;
use thiserror::Error;

/// Executor error types
#[derive(Error, Debug, Clone)]
pub(crate) enum ExecutorError {
    /// Errors from BlockExecution
    #[error("Block execution error: {0}")]
    Execution(#[from] BlockExecutionError),
    /// Error when creating a new [SealedBlockWithSenders]
    #[error("Failed to seal block with senders")]
    UnevenSendersForSealedBlock,
    /// Failed to decode transaction bytes
    #[error("RLP error decoding transaction within consensus output: {0}")]
    DecodeTransaction(#[from] alloy_rlp::Error),
}
