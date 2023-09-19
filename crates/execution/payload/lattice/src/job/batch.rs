//! Structs for the specific "job" of building a batch payload.

use anemo::types::Version;
use execution_payload_builder::database::CachedReads;
use execution_provider::StateProviderFactory;
use execution_tasks::TaskSpawner;
use execution_transaction_pool::{TransactionPool, TransactionId, BatchInfo};
use lattice_network::EngineToWorkerClient;
use revm::primitives::{CfgEnv, BlockEnv};
use tn_network_types::SealedBatchResponse;
use tokio::sync::oneshot;
use tracing::{trace, warn};
use std::{future::Future, sync::{Arc, atomic::AtomicBool}, pin::Pin, task::{Context, Poll}};
use tn_types::{execution::SealedBlock, consensus::VersionedMetadata};
use futures_core::ready;
use futures_util::future::FutureExt;
use crate::{BatchPayloadSizeMetric, PayloadTaskGuard, LatticePayloadBuilderServiceMetrics,
    LatticePayloadBuilderError, Cancelled, helpers::{create_batch, seal_batch}
};

/// Helper type to represent a CL Batch.
/// 
/// TODO: add batch size and gas used as metrics to struct
/// since they're already calculated in the job.
#[derive(Debug)]
pub struct BatchPayload {
    batch: Vec<Vec<u8>>,
    metadata: VersionedMetadata,
    executed_tx_ids: Vec<TransactionId>,
    size_metric: BatchPayloadSizeMetric,
}

impl BatchPayload {
    /// Create a new instance of [Self]
    pub fn new(
        batch: Vec<Vec<u8>>,
        metadata: VersionedMetadata,
        executed_tx_ids: Vec<TransactionId>,
        size_metric: BatchPayloadSizeMetric,
    ) -> Self {
        Self { batch, metadata, executed_tx_ids, size_metric }
    }

    /// Reference to the batch of transactions
    pub fn get_batch(&self) -> &Vec<Vec<u8>> {
        &self.batch
    }

    /// Reference to the metadata from the sealed header
    pub fn get_metadata(&self) -> &VersionedMetadata {
        &self.metadata
    }

    /// Reference to the batch's transaction ids for updating the pool.
    pub fn get_transaction_ids(&self) -> &Vec<TransactionId> {
        &self.executed_tx_ids
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
pub(crate) type BatchPayloadFuture = Pin<Box<dyn Future<Output = Result<(), LatticePayloadBuilderError>> + Send>>;

/// The job that starts building a batch on a separate task.
/// 
/// The struct is also a [Future] and polls `Ready` when the batch is finished building
/// or an error returns.
pub struct BatchPayloadJob<Client, Pool, Tasks, Network> {
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
    /// Receiver for the batch that is currently being broadcast.
    pub(crate) pending_broadcast: Option<PendingBroadcast>,
    /// The built payload.
    pub(crate) payload_transactions: Vec<TransactionId>,
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
    /// Network type for passing payload to quorum waiter for broadcasting the batch.
    pub(crate) network: Network,
}

impl<Client, Pool, Tasks, Network> Future for BatchPayloadJob<Client, Pool, Tasks, Network>
where
    Client: StateProviderFactory + Clone + Unpin + 'static,
    Pool: TransactionPool + Unpin + 'static,
    Tasks: TaskSpawner + Clone + 'static,
    Network: EngineToWorkerClient + Clone + Unpin + Send + Sync + 'static,
{
    type Output = Result<(), LatticePayloadBuilderError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // poll the network call if it exists
        if let Some(mut fut) = this.pending_broadcast.take() {
            let poll_status = match fut.poll_unpin(cx) {
                Poll::Ready(Ok(sealed_batch)) => {
                    // update pool
                    let batch_info = BatchInfo::new(
                        sealed_batch.digest,
                        this.payload_transactions.clone(),
                        sealed_batch.worker_id,
                    );
                    // update pool and return Ready
                    this.pool.on_sealed_batch(batch_info);
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(e)) => {
                    // TODO: if batch is empty, this returns an error
                    trace!(?e, "batch build attempt failed");
                    this.metrics.inc_failed_batch_jobs();
                    Poll::Ready(Err(e))
                }
                Poll::Pending => {
                    warn!("Pending batch is still pending!!");
                    this.pending_broadcast = Some(fut);
                    Poll::Pending
                }
            };
            return poll_status
        }

        // poll the pending batch if it exists and pass to quorum waiter
        if let Some(mut fut) = this.pending_batch.take() {
            let poll_status = match fut.poll_unpin(cx) {
                Poll::Ready(Ok(payload)) => {
                    // use the local network to send the batch to the quorum waiter
                    let (tx, rx) = oneshot::channel();
                    let guard = this.payload_task_guard.clone();
                    let network_client = this.network.clone();
                    let cancel = Cancelled::default();
                    let _cancel = cancel.clone();
                    let waker = cx.waker().clone();
                    let batch = payload.get_batch().clone();
                    let metadata = payload.get_metadata().clone();
                    // need synchronous IO
                    this.executor.spawn_blocking(Box::pin(async move {
                        // acquire the permit for executing the task
                        let _permit = guard.0.acquire().await;
                        seal_batch(
                            network_client,
                            batch,
                            metadata,
                            tx,
                            cancel,
                            waker,
                        ).await
                    }));
                    this.payload_transactions = payload.get_transaction_ids().clone();
                    this.pending_broadcast = Some(PendingBroadcast { _cancel, network_result: rx });
                    Poll::Pending
                }
                Poll::Ready(Err(e)) => {
                    // if batch is empty, this returns an error
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

/// A future that resolves to the result of the batch building job.
#[derive(Debug)]
pub(crate) struct PendingBroadcast {
    // /// The batch payload to broadcast
    // payload: Arc<BatchPayload>,
    /// The marker to cancel the job on drop
    _cancel: Cancelled,
    /// The channel to send the result to.
    network_result: oneshot::Receiver<Result<SealedBatchResponse, LatticePayloadBuilderError>>,
}

impl Future for PendingBroadcast {
    type Output = Result<SealedBatchResponse, LatticePayloadBuilderError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let res = ready!(self.network_result.poll_unpin(cx));
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
