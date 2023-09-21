//! Handle for payload builder

use std::sync::Arc;
use tn_types::consensus::ConsensusOutput;
use tn_network_types::BuildHeaderRequest;
use tokio::sync::{mpsc, oneshot};
use crate::{LatticePayloadBuilderServiceCommand, LatticePayloadBuilderError, HeaderPayload, BlockPayload};

/// Communication handle for the [BatchBuilderService].
/// 
/// This is the API used to create new batches.
#[derive(Clone)]
pub struct LatticePayloadBuilderHandle {
    /// TODO: should this be a bounded channel?
    /// - unbounded can be used in async/sync fn
    /// Sender half of the message channel to the [LatticePayloadBuilderService].
    pub(crate) to_service: mpsc::UnboundedSender<LatticePayloadBuilderServiceCommand>,
}

//==== impl LatticePayloadBuilderHandle
impl LatticePayloadBuilderHandle {
    /// Create a new instance of [Self]
    pub fn new(to_service: mpsc::UnboundedSender<LatticePayloadBuilderServiceCommand>) -> Self {
        Self { to_service }
    }
    /// Return a new batch for the requesting worker.
    pub async fn new_batch(&self) -> Result<(), LatticePayloadBuilderError> {
        // TODO: should this be an arg from the worker instead?
        let (tx, rx) = oneshot::channel();

        // send command to service
        self.to_service.send(LatticePayloadBuilderServiceCommand::NewBatch(tx))?;

        // await job future
        match rx.await {
            Ok(fut) => fut.await,
            Err(e) => Err(e.into()),
        }
    }

    /// Handle method for building a new header based on the passed attributes.
    pub async fn new_header(&self, attributes: BuildHeaderRequest) -> Result<Arc<HeaderPayload>, LatticePayloadBuilderError> {
        let (tx, rx) = oneshot::channel();

        self.to_service
            .send(LatticePayloadBuilderServiceCommand::NewHeader{ tx, attributes })
            .map_err(LatticePayloadBuilderError::from)?;

        match rx.await {
            Ok(fut) => fut.await,
            Err(e) => Err(e.into()),
        }
    }

    /// Handle consensus output. This is called to build the next canonical block.
    pub async fn new_canonical_block(
        &self,
        output: ConsensusOutput,
    ) -> Result<Arc<BlockPayload>, LatticePayloadBuilderError> {
        let (tx, rx) = oneshot::channel();
        self.to_service
            .send(LatticePayloadBuilderServiceCommand::NewCanonicalBlock{ output, tx })
            .map_err(LatticePayloadBuilderError::from)?;
            
        match rx.await {
            Ok(fut) => fut.await,
            Err(e) => Err(e.into())
        }
    }

}
