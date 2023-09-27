use std::sync::Arc;

use crate::{
    engine::error::LatticeOnNewPayloadError,
    LatticeConsensusEngineEvent,
};
use execution_rpc_types::engine::{BatchExecutionPayload, BatchPayloadStatus};
use lattice_payload_builder::BlockPayload;
use tokio::sync::{mpsc::UnboundedSender, oneshot};

/// A message for the lattice engine from other components of the node (engine RPC API invoked by
/// the consensus layer).
#[derive(Debug)]
pub enum LatticeEngineMessage {
    /// Message with new batch from peer.
    ///
    /// Akin to `NewPayload`
    ValidateBatch {
        /// The execution payload received by Engine API.
        payload: BatchExecutionPayload,
        /// The sender for returning payload status result.
        tx: oneshot::Sender<Result<BatchPayloadStatus, LatticeOnNewPayloadError>>,
    },
    /// Message with certificate from round.
    /// Akin to `ForkchoiceUpdated`
    Consensus {
        /// The next canonical block.
        payload: Arc<BlockPayload>,
        /// the sender for returning the canonical chain updated result.
        tx: oneshot::Sender<Result<(), execution_interfaces::Error>>,
    },
    /// Message with exchanged transition configuration.
    TransitionConfigurationExchanged,
    /// Add a new listener for [`LatticeEngineMessage`].
    EventListener(UnboundedSender<LatticeConsensusEngineEvent>),
}
