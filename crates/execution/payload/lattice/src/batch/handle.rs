use std::sync::Arc;
use tn_types::consensus::BatchDigest;
use tokio::sync::{mpsc, oneshot};
use super::{generator::BuiltBatch, BatchBuilderError, BatchBuilderServiceCommand};

/// Communication handle for the [BatchBuilderService].
/// 
/// This is the API used to create new batches.
#[derive(Clone)]
pub struct BatchBuilderHandle {
    /// TODO: should this be a bounded channel?
    /// - unbounded can be used in async/sync fn
    /// Sender half of the message channel to the [BatchBuilderService].
    pub(crate) to_service: mpsc::UnboundedSender<BatchBuilderServiceCommand>,
}

//==== impl BatchBuilderHandle
impl BatchBuilderHandle {
    /// Create a new instance of [Self]
    pub fn new(to_service: mpsc::UnboundedSender<BatchBuilderServiceCommand>) -> Self {
        Self { to_service }
    }
    /// Return a new batch for the requesting worker.
    pub async fn new_batch(&self) -> Result<Arc<BuiltBatch>, BatchBuilderError> {
        // TODO: should this be an arg from the worker instead?
        let (tx, rx) = oneshot::channel();

        // send command to service
        self.to_service.send(BatchBuilderServiceCommand::NewPayload(tx))?;

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
    /// sends the BuiltBatch to the worker to seal. Once the worker seals the batch, the digest is returned 
    /// and the future resolves.
    pub async fn batch_sealed(&self, batch: Arc<BuiltBatch>, digest: BatchDigest) ->  Result<(), BatchBuilderError> {
        tracing::debug!("received batch sealed handle call");
        self.to_service.send(BatchBuilderServiceCommand::BatchSealed{batch, digest})?;
        tracing::debug!("sent batch sealed handle call to service");
        Ok(())
    }

}
