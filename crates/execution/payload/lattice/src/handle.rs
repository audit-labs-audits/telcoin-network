use std::sync::Arc;
use anemo::async_trait;
use tn_types::consensus::{BatchDigest, CertificateDigest,};
use tn_network_types::{PrimaryToEngine, BuildHeaderRequest, HeaderPayloadResponse, WorkerToEngine};
use tokio::sync::{mpsc, oneshot};
use crate::{LatticePayloadBuilderServiceCommand, BatchPayload, LatticePayloadBuilderError, HeaderPayload};

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

    // /// Execute transactions by batch digest to create EL data for Primary's proposed header.
    // pub async fn new_header(&self) -> Result<Arc<HeaderPayload>, LatticePayloadBuilderError> {
    //     // TODO: should this be an arg from the worker instead?
    //     let (tx, rx) = oneshot::channel();

    //     // send command to service
    //     self.to_service.send(LatticePayloadBuilderServiceCommand::NewHeader(tx))?;

    //     // await job future
    //     match rx.await {
    //         Ok(fut) => fut.await,
    //         Err(e) => Err(e.into()),
    //     }
    // }

    // /// The Primary signals that the Block reached a quorum of votes and a certificate was issued.
    // pub async fn header_sealed(&self, header: Arc<HeaderPayload>, digest: CertificateDigest) -> Result<(), LatticePayloadBuilderError> {
    //     self.to_service.send(LatticePayloadBuilderServiceCommand::HeaderSealed { header, digest })?;
    //     Ok(())
    // }

}

/// Implement the receiving side of WorkerToEngine trait for the 
/// handle to the payload builder service.
#[async_trait]
impl PrimaryToEngine for LatticePayloadBuilderHandle {
    async fn build_header(
        &self,
        request: anemo::Request<BuildHeaderRequest>,
    ) -> Result<anemo::Response<HeaderPayloadResponse>, anemo::rpc::Status> {
        let attributes = request.into_body();

        let (reply, rx) = oneshot::channel();

        // send command to service
        self.to_service
            .send(LatticePayloadBuilderServiceCommand::NewHeader{ reply, attributes })
            .map_err(LatticePayloadBuilderError::from)?;

        match rx.await.map_err(LatticePayloadBuilderError::from) {
            Ok(fut) => {
                let payload = fut.await?;
                Ok(anemo::Response::new(payload.as_ref().into()))
            }
            Err(e) => {
                Err(e.into())
            }
        }
    }
}
