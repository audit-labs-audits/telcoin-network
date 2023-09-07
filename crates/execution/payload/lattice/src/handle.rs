use std::sync::Arc;
use anemo::async_trait;
use tn_types::consensus::{BatchDigest, CertificateDigest, PrimaryToEngine, BuildHeaderMessage, HeaderPayloadResponse};
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
    pub async fn new_batch(&self) -> Result<Arc<BatchPayload>, LatticePayloadBuilderError> {
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
    
    /// The worker signals that the batch is sealed.
    /// This method updates the transaction pool.
    /// 
    /// TODO: this method only works for one worker.
    /// If two workers request a new batch, they are likely to get duplicate
    /// transactions from the pending pool using this approach.
    /// 
    /// Testnet is only designed using one worker, so this is fine for now.
    /// 
    /// Maybe: When more workers are available, they should keep track of requests and only send
    /// one at a time to prevent duplicate transactions from being included in batches?
    /// 
    /// Maybe: Include tx arg from worker on `new_batch()`. The job polls until pending is done, then
    /// sends the BatchPayload to the worker to seal. Once the worker seals the batch, the digest is returned 
    /// and the future resolves.
    pub async fn batch_sealed(&self, batch: Arc<BatchPayload>, digest: BatchDigest) ->  Result<(), LatticePayloadBuilderError> {
        tracing::debug!("received batch sealed handle call");
        self.to_service.send(LatticePayloadBuilderServiceCommand::BatchSealed{batch, digest})?;
        tracing::debug!("sent batch sealed handle call to service");
        Ok(())
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

// /// The handle for incoming messages from the Primary.
// pub struct PrimaryReceiverHandle {
//     /// The network interface for communicating with the primary.
//     pub network: anemo::Network,
//     /// Sender half of the message channel to the [LatticePayloadBuilderService].
//     pub(crate) to_service: mpsc::UnboundedSender<LatticePayloadBuilderServiceCommand>,
// }

/// Implement the receiving side of PrimaryToEngine trait for the 
/// handle to the payload builder service.
#[async_trait]
impl PrimaryToEngine for LatticePayloadBuilderHandle {
    async fn build_header(
        &self,
        request: anemo::Request<BuildHeaderMessage>,
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
