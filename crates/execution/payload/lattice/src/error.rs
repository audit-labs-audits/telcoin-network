//! Error types emitted by types or implementations of this crate.

use anemo::types::response::StatusCode;
use execution_transaction_pool::error::PoolError;
use lattice_network::LocalClientError;
use revm::primitives::EVMError;
use tn_types::execution::{H256, TransactionSigned};
use tokio::sync::oneshot;

use crate::LatticePayloadBuilderServiceCommand;

/// Possible error variants during payload building.
#[derive(Debug, thiserror::Error)]
pub enum LatticePayloadBuilderError {
    /// Thrown whe the parent block is missing.
    #[error("missing parent block {0:?}")]
    MissingParentBlock(H256),
    /// Thrown when there is no finalized block during header building.
    #[error("missing finalized block for round {0:?}")]
    MissingFinalizedBlock(u64),
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
    #[error("missing genesis block")]
    LatticeBlockFromGenesis,
    /// Thrwon if the batch payload can't create a timestamp when initialized the BlockEnv.
    #[error("Failed to capture System Time.")]
    LatticeBatchSystemTime(#[from] std::time::SystemTimeError),
    /// Thrown if the receiver for the oneshot channel is dropped by the worker which requested the batch.
    #[error("Channel closed before batch could be sent: {0:?}")]
    LatticeBatchChannelClosed(String),
    /// Thrown if the batch handle can't send to the batch builder service.
    #[error("Handle can't send to the batch builder service: {0:?}")]
    LatticePayloadBuilderHandleToService(#[from] tokio::sync::mpsc::error::SendError<LatticePayloadBuilderServiceCommand>),
    /// The built batch is empty. This error is required so the worker doesn't seal an empty batch.
    #[error("Built batch is empty.")]
    EmptyBatch,
    /// Missing batch in SealedPool
    #[error("Missing batch: {0:?}")]
    Pool(#[from] PoolError),
    /// Local network error when batch or header sent to CL
    #[error("Local network error: {0:?}")]
    LocalNetwork(#[from] LocalClientError),
    /// Error decoding transaction bytes
    #[error("Failed to decode transaction: {0}")]
    Decode(#[from] execution_rlp::DecodeError),
    /// Error recoving signature for transaction
    #[error("Failed to recover tx signature: {0:?}")]
    RecoverSignature(TransactionSigned),
}

impl From<oneshot::error::RecvError> for LatticePayloadBuilderError {
    fn from(_: oneshot::error::RecvError) -> Self {
        LatticePayloadBuilderError::ChannelClosed
    }
}

impl From<LatticePayloadBuilderError> for anemo::rpc::Status {
    fn from(error: LatticePayloadBuilderError) -> anemo::rpc::Status {
        anemo::rpc::Status::new_with_message(
            StatusCode::InternalServerError,
            format!("{error:?}"),
        )
    }
}
