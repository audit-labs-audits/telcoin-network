//! `LatticeConsensusEngine` external API

use crate::{
    engine::message::OnForkChoiceUpdated, LatticeConsensusEngineEvent, LatticeEngineMessage,
    LatticeForkChoiceUpdateError, LatticeOnNewPayloadError,
};
use execution_payload_builder::{PayloadId, PayloadStore};
use execution_rpc_types::engine::{
    ExecutionPayload, ForkchoiceState, ForkchoiceUpdated, PayloadAttributes, PayloadStatus, BatchPayloadStatus,
};
use futures::TryFutureExt;
use tn_types::consensus::{Batch, OutputAttributes};
use tokio::sync::{mpsc, mpsc::UnboundedSender, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;

use super::error::LatticeNextBatchError;

/// A _shareable_ lattice consensus frontend type. Used to interact with the spawned lattice
/// consensus engine task.
///
/// See also [`LatticeConsensusEngine`](crate::engine::LatticeConsensusEngine).
#[derive(Clone, Debug)]
pub struct LatticeConsensusEngineHandle {
    /// Sender channel to Engine.
    pub(crate) to_engine: UnboundedSender<LatticeEngineMessage>,
}

// === impl LatticeConsensusEngineHandle ===

impl LatticeConsensusEngineHandle {
    /// Creates a new lattice consensus engine handle.
    pub fn new(
        to_engine: UnboundedSender<LatticeEngineMessage>,
    ) -> Self {
        Self { to_engine }
    }

    /// Validate a new batch from a worker's peer.
    pub async fn validate_batch(
        &self,
        batch: Batch,
    ) -> Result<(), LatticeOnNewPayloadError> {
        let (tx, rx) = oneshot::channel();
        let _ = self.to_engine.send(LatticeEngineMessage::ValidateBatch { payload: batch.into(), tx });
        // rx.await.map_err(|_| LatticeOnNewPayloadError::EngineUnavailable)?
        let status = rx.await.map_err(|_| LatticeOnNewPayloadError::EngineUnavailable)??;
        match status {
            BatchPayloadStatus::Valid => Ok(()),
            BatchPayloadStatus::Invalid { validation_error } => Err(LatticeOnNewPayloadError::InvalidBatch(validation_error))
        }
    }

    /// Produce the next canonical block based on certificate from the CL.
    /// 
    /// Akin to `fork_choice_updated` in beacon engine api.
    pub async fn handle_consensus_output(
        &self,
        attributes: OutputAttributes
    ) -> Result<(), LatticeForkChoiceUpdateError> {
        todo!()
    }

    /// Sends a forkchoice update message to the lattice consensus engine and waits for a response.
    ///
    /// See also <https://github.com/ethereum/execution-apis/blob/3d627c95a4d3510a8187dd02e0250ecb4331d27e/src/engine/shanghai.md#engine_forkchoiceupdatedv2>
    pub async fn fork_choice_updated(
        &self,
        state: ForkchoiceState,
        payload_attrs: Option<PayloadAttributes>,
    ) -> Result<ForkchoiceUpdated, LatticeForkChoiceUpdateError> {
        Ok(self
            .send_fork_choice_updated(state, payload_attrs)
            .map_err(|_| LatticeForkChoiceUpdateError::EngineUnavailable)
            .await??
            .await?)
    }

    /// Sends a forkchoice update message to the lattice consensus engine and returns the receiver
    /// to wait for a response.
    fn send_fork_choice_updated(
        &self,
        state: ForkchoiceState,
        payload_attrs: Option<PayloadAttributes>,
    ) -> oneshot::Receiver<Result<OnForkChoiceUpdated, execution_interfaces::Error>> {
        let (tx, rx) = oneshot::channel();
        let _ = self.to_engine.send(LatticeEngineMessage::ForkchoiceUpdated {
            state,
            payload_attrs,
            tx,
        });
        rx
    }

    /// Sends a transition configuration exchagne message to the lattice consensus engine.
    ///
    /// See also <https://github.com/ethereum/execution-apis/blob/3d627c95a4d3510a8187dd02e0250ecb4331d27e/src/engine/paris.md#engine_exchangetransitionconfigurationv1>
    pub async fn transition_configuration_exchanged(&self) {
        let _ = self.to_engine.send(LatticeEngineMessage::TransitionConfigurationExchanged);
    }

    /// Creates a new [`LatticeConsensusEngineEvent`] listener stream.
    pub fn event_listener(&self) -> UnboundedReceiverStream<LatticeConsensusEngineEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        let _ = self.to_engine.send(LatticeEngineMessage::EventListener(tx));
        UnboundedReceiverStream::new(rx)
    }
}
