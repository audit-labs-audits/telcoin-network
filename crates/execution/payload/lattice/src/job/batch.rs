//! Structs for the specific "job" of building a batch payload.

use execution_payload_builder::database::CachedReads;
use execution_provider::StateProviderFactory;
use execution_tasks::TaskSpawner;
use execution_transaction_pool::{TransactionPool, TransactionId};
use revm::primitives::{CfgEnv, BlockEnv};
use tokio::sync::oneshot;
use tracing::{trace, warn};
use std::{future::Future, sync::{Arc, atomic::AtomicBool}, pin::Pin, task::{Context, Poll}};
use tn_types::execution::SealedBlock;
use futures_core::ready;
use futures_util::future::FutureExt;
use crate::{BatchPayloadSizeMetric, PayloadTaskGuard, LatticePayloadBuilderServiceMetrics, LatticePayloadBuilderError, Cancelled, helpers::create_batch};

/// Helper type to represent a CL Batch.
/// 
/// TODO: add batch size and gas used as metrics to struct
/// since they're already calculated in the job.
#[derive(Debug)]
pub struct BatchPayload {
    batch: Vec<Vec<u8>>,
    executed_txs: Vec<TransactionId>,
    size_metric: BatchPayloadSizeMetric,
}

impl BatchPayload {
    /// Create a new instance of [Self]
    pub fn new(
        batch: Vec<Vec<u8>>,
        executed_txs: Vec<TransactionId>,
        size_metric: BatchPayloadSizeMetric,
    ) -> Self {
        Self { batch, executed_txs, size_metric }
    }

    /// Reference to the batch of transactions
    pub fn get_batch(&self) -> &Vec<Vec<u8>> {
        &self.batch
    }

    /// Reference to the batch's transaction ids for updating the pool.
    pub fn get_transaction_ids(&self) -> &Vec<TransactionId> {
        &self.executed_txs
    }

    /// Return the size metric for the built batch.
    /// The size metric is used to indicate why the payload job
    /// was completed.
    /// 
    /// This method is used by the worker's metrics to provide a 
    /// reason for why the batch was sealed.
    pub fn reason(&self) -> &BatchPayloadSizeMetric {
        &self.size_metric
    }
}
/// Future representing a return [BatchPayload].
pub(crate) type BatchPayloadFuture = Pin<Box<dyn Future<Output = Result<Arc<BatchPayload>, LatticePayloadBuilderError>> + Send>>;

/// The job that starts building a batch on a separate task.
/// 
/// The struct is also a [Future] and polls `Ready` when the batch is finished building
/// or an error returns.
pub struct BatchPayloadJob<Client, Pool, Tasks> {
    /// The configuration for how to build the batch.
    pub(crate) config: BatchPayloadConfig,
    /// Client to interact with chain.
    pub(crate) client: Client,
    /// The transaction pool.
    pub(crate) pool: Pool,
    /// How to spawn building tasks
    pub(crate) executor: Tasks,
    /// Receiver for the block that is currently being built.
    pub(crate) pending_batch: Option<PendingBatch>,
    /// Restricts how many generator tasks can be executed at once.
    /// 
    /// TODO: can this be used to prevent batches/headers while canonical state is changing?
    pub(crate) payload_task_guard: PayloadTaskGuard,
    /// Caches all disk reads for the state the new payloads builds on
    ///
    /// This is used to avoid reading the same state over and over again when new attempts are
    /// triggerd, because during the building process we'll repeatedly execute the transactions.
    pub(crate) cached_reads: Option<CachedReads>,
    /// metrics for this type
    pub(crate) metrics: LatticePayloadBuilderServiceMetrics,
}

impl<Client, Pool, Tasks> Future for BatchPayloadJob<Client, Pool, Tasks>
where
    Client: StateProviderFactory + Clone + Unpin + 'static,
    Pool: TransactionPool + Unpin + 'static,
    Tasks: TaskSpawner + Clone + 'static,
{
    type Output = Result<Arc<BatchPayload>, LatticePayloadBuilderError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // poll the pending batch if it exists
        if let Some(mut fut) = this.pending_batch.take() {
            let poll_status = match fut.poll_unpin(cx) {
                Poll::Ready(Ok(batch)) => {
                    let payload = Arc::new(batch);
                    Poll::Ready(Ok(payload))
                }
                Poll::Ready(Err(e)) => {
                    // TODO: if batch is empty, this returns an error
                    trace!(?e, "batch build attempt failed");
                    this.metrics.inc_failed_batch_jobs();
                    Poll::Ready(Err(e))
                }
                Poll::Pending => {
                    warn!("Pending batch is still pending!!");
                    this.pending_batch = Some(fut);
                    Poll::Pending
                }
            };

            return poll_status
        }

        // otherwise start building the batch
        let (tx, rx) = oneshot::channel();
        let client = this.client.clone();
        let pool = this.pool.clone();
        let cancel = Cancelled::default();
        let _cancel = cancel.clone();
        let guard = this.payload_task_guard.clone();
        let payload_config = this.config.clone();

        this.metrics.inc_initiated_batch_jobs();

        let cached_reads = this.cached_reads.take().unwrap_or_default();

        let waker = cx.waker().clone();

        this.executor.spawn_blocking(Box::pin(async move {
            // acquire the permit for executing the task
            let _permit = guard.0.acquire().await;
            create_batch(
                client,
                pool,
                cached_reads,
                payload_config,
                cancel,
                tx,
                waker,
            )
        }));

        // store the pending batch for next poll
        this.pending_batch = Some(PendingBatch { _cancel, payload: rx });

        Poll::Pending
    }
}

/// A future that resolves to the result of the batch building job.
#[derive(Debug)]
pub(crate) struct PendingBatch {
    /// The marker to cancel the job on drop
    _cancel: Cancelled,
    /// The channel to send the result to.
    payload: oneshot::Receiver<Result<BatchPayload, LatticePayloadBuilderError>>,
}

impl Future for PendingBatch {
    type Output = Result<BatchPayload, LatticePayloadBuilderError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let res = ready!(self.payload.poll_unpin(cx));
        Poll::Ready(res.map_err(Into::into).and_then(|res| res))
    }
}

/// Static config for how to build a batch payload
/// using the block env, state env, and parent block.
#[derive(Clone)]
pub(crate) struct BatchPayloadConfig {
    /// Pre-configured block environment.
    pub(crate) initialized_block_env: BlockEnv,
    /// Configuration for the environment.
    pub(crate) initialized_cfg: CfgEnv,
    /// The parent block.
    pub(crate) parent_block: Arc<SealedBlock>,
    /// The maximum size of the batch (measured in bytes).
    pub(crate) max_batch_size: usize,
}

#[cfg(test)]
mod test {

    #[tokio::test(flavor = "multi_thread")]
    async fn test_batch_payload_job_genesis() {
        todo!()
    }

    #[tokio::test]
    async fn test_batch_payload_job() {
        todo!()
    }
}
