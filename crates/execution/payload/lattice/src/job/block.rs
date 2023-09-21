//! Structs for the specific "job" of building the next canonical block
//! from `ConsensusOutput`.

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
use tn_types::{execution::{SealedBlock, ChainSpec, U256}, consensus::{VersionedMetadata, ConsensusOutput}};
use futures_core::ready;
use futures_util::future::FutureExt;
use crate::{PayloadTaskGuard, LatticePayloadBuilderServiceMetrics,
    LatticePayloadBuilderError, Cancelled, helpers::create_block, 
};

/// The result of the built block job.
/// 
/// TODO: add batch size and gas used as metrics to struct
/// since they're already calculated in the job.
#[derive(Debug)]
pub struct BlockPayload {
    /// The built block
    block: SealedBlock,
    /// The fees for the leader.
    fees: U256,
}

impl BlockPayload {
    /// Create a new instance of [Self]
    pub(crate) fn new(
        block: SealedBlock,
        fees: U256,
    ) -> Self {
        Self { block, fees }
    }

    /// Reference to the sealed block.
    pub fn get_block(&self) -> &SealedBlock {
        &self.block
    }

    /// The fees for the leader of the round.
    pub fn get_fees(&self) -> U256 {
        self.fees
    }

}

/// Future representing a return [BatchPayload].
pub(crate) type BlockPayloadFuture = Pin<Box<dyn Future<Output = Result<Arc<BlockPayload>, LatticePayloadBuilderError>> + Send>>;

/// The job that starts building a batch on a separate task.
/// 
/// The struct is also a [Future] and polls `Ready` when the batch is finished building
/// or an error returns.
pub struct BlockPayloadJob<Client, Pool, Tasks> {
    /// The configuration for how to build the batch.
    pub(crate) config: BlockPayloadConfig,
    /// Client to interact with chain.
    pub(crate) client: Client,
    /// The transaction pool.
    pub(crate) pool: Pool,
    /// How to spawn building tasks
    pub(crate) executor: Tasks,
    /// Receiver for the block that is currently being built.
    pub(crate) pending_block: Option<PendingBlock>,
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

impl<Client, Pool, Tasks> Future for BlockPayloadJob<Client, Pool, Tasks>
where
    Client: StateProviderFactory + Clone + Unpin + 'static,
    Pool: TransactionPool + Unpin + 'static,
    Tasks: TaskSpawner + Clone + 'static,
{
    type Output = Result<Arc<BlockPayload>, LatticePayloadBuilderError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // poll the pending block
        if let Some(mut fut) = this.pending_block.take() {
            let poll_status = match fut.poll_unpin(cx) {
                Poll::Ready(Ok(payload)) => {
                    let payload = Arc::new(payload);
                    Poll::Ready(Ok(payload))
                }
                Poll::Ready(Err(e)) => {
                    // if batch is empty, this returns an error
                    trace!(?e, "batch build attempt failed");
                    this.metrics.inc_failed_batch_jobs();
                    Poll::Ready(Err(e))
                }
                Poll::Pending => {
                    warn!("Pending batch is still pending!!");
                    this.pending_block = Some(fut);
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
            create_block(
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
        this.pending_block = Some(PendingBlock { _cancel, payload: rx });

        Poll::Pending
    }
}

/// A future that resolves to the result of the batch building job.
#[derive(Debug)]
pub(crate) struct PendingBlock {
    /// The marker to cancel the job on drop
    _cancel: Cancelled,
    /// The channel to send the result to.
    payload: oneshot::Receiver<Result<BlockPayload, LatticePayloadBuilderError>>,
}

impl Future for PendingBlock {
    type Output = Result<BlockPayload, LatticePayloadBuilderError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let res = ready!(self.payload.poll_unpin(cx));
        Poll::Ready(res.map_err(Into::into).and_then(|res| res))
    }
}

/// Static config for how to build a batch payload
/// using the block env, state env, and parent block.
#[derive(Clone)]
pub(crate) struct BlockPayloadConfig {
    /// Pre-configured block environment.
    pub(crate) initialized_block_env: BlockEnv,
    /// Configuration for the environment.
    pub(crate) initialized_cfg: CfgEnv,
    /// The parent block.
    pub(crate) parent_block: Arc<SealedBlock>,
    /// The chain spec
    pub(crate) chain_spec: Arc<ChainSpec>,
    /// Output from consensus
    pub(crate) output: ConsensusOutput,
}
