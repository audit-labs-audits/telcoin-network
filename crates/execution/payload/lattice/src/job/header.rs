//! Header payload builder.
//! 
//! Primary requests the next Header to propose.
//!
//! The service queries the `SealedPool` to find all transactions that match the `BatchDigest` in the
//! Header's payload to recreate the `Batch`. The digest is taken to ensure all transactions are present.
//! If the digest doesn't match, a request is sent to the `Workers to retrieve the batch from storage.
//! 
//! The resulting `HeaderPayload` is sent back to the Primary to propose through the `Certifier`.
//!
//! Structs for the specific "job" of building a header payload.

use execution_payload_builder::database::CachedReads;
use execution_provider::StateProviderFactory;
use execution_tasks::TaskSpawner;
use execution_transaction_pool::{TransactionPool, TransactionId};
use revm::primitives::{CfgEnv, BlockEnv};
use tokio::sync::oneshot;
use tracing::{trace, warn};
use std::{future::Future, sync::{Arc, atomic::AtomicBool}, pin::Pin, task::{Context, Poll}, collections::HashMap};
use tn_types::{
    execution::{SealedBlock, H256, ChainSpec, Header, SealedHeader},
    consensus::{BatchDigest, WorkerId, TimestampMs, Batch}
};
use tn_network_types::{
    BuildHeaderRequest, HeaderPayloadResponse,
};
use futures_core::ready;
use futures_util::future::FutureExt;
use indexmap::IndexMap;
use crate::{PayloadTaskGuard, LatticePayloadBuilderServiceMetrics, LatticePayloadBuilderError, helpers::{create_batch, create_header}, Cancelled};

/// The payload for a proposed header.
/// 
/// TODO: add batch size and gas used as metrics to struct
/// since they're already calculated in the job.
#[derive(Debug)]
pub struct HeaderPayload {
    /// Execution results: header
    sealed_header: SealedHeader,
}

impl HeaderPayload {
    /// Create a new instance of [Self]
    pub fn new(
        header: Header,
    ) -> Self {
        let sealed_header = header.seal_slow();
        Self { sealed_header }
    }

    /// Reference to the sealed header for this payload
    pub fn get_sealed_header(&self) -> &SealedHeader {
        &self.sealed_header
    }
}

// Used for RPC reply.
impl From<&HeaderPayload> for HeaderPayloadResponse {
    fn from(payload: &HeaderPayload) -> Self {
        Self {
            sealed_header: payload.sealed_header.clone()
        }
    }
}

pub(crate) type HeaderPayloadFuture = Pin<Box<dyn Future<Output = Result<Arc<HeaderPayload>, LatticePayloadBuilderError>> + Send>>;

/// The job that starts building a header on a separate task.
/// 
/// The struct is also a [Future] and polls `Ready` when the header is finished building
/// or an error returns.
pub struct HeaderPayloadJob<Client, Pool, Tasks> {
    /// The configuration for how to build the header.
    pub(crate) config: HeaderPayloadConfig,
    /// Client to interact with chain.
    pub(crate) client: Client,
    /// The transaction pool.
    pub(crate) pool: Pool,
    /// How to spawn building tasks
    pub(crate) executor: Tasks,
    /// Receiver for the block that is currently being built.
    pub(crate) pending_header: Option<PendingHeader>,
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
    /// Receiver for missing batches (if any).
    pub(crate) missing_batches_rx: Option<oneshot::Receiver<HashMap<BatchDigest, Batch>>>,
}

impl<Client, Pool, Tasks> Future for HeaderPayloadJob<Client, Pool, Tasks>
where
    Client: StateProviderFactory + Clone + Unpin + 'static,
    Pool: TransactionPool + Unpin + 'static,
    Tasks: TaskSpawner + Clone + 'static,
{
    type Output = Result<Arc<HeaderPayload>, LatticePayloadBuilderError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();


        // poll the pending header if it exists
        if let Some(mut fut) = this.pending_header.take() {
            let poll_status = match fut.poll_unpin(cx) {
                Poll::Ready(Ok(header)) => {
                    let payload = Arc::new(header);
                    Poll::Ready(Ok(payload))
                }
                Poll::Ready(Err(e)) => {
                    trace!(?e, "header build attempt failed");
                    this.metrics.inc_failed_header_jobs();
                    Poll::Ready(Err(e))
                }
                Poll::Pending => {
                    warn!("Pending header is still pending!!");
                    this.pending_header = Some(fut);
                    Poll::Pending
                }
            };

            return poll_status
        }

        let missing_batches = match this.missing_batches_rx.take() {
            Some(mut receiver) => {
                // wait for missing batches from worker
                let batches = match ready!(receiver.poll_unpin(cx)) {
                    Ok(batches) => batches,
                    Err(e) => return Poll::Ready(Err(e.into())),
                };

                Some(batches)
            }
            None => None
        };

        // otherwise start building the header
        let (tx, rx) = oneshot::channel();
        let client = this.client.clone();
        let pool = this.pool.clone();
        let cancel = Cancelled::default();
        let _cancel = cancel.clone();
        let guard = this.payload_task_guard.clone();
        let payload_config = this.config.clone();
        let digests = this.config.attributes.payload.clone();

        this.metrics.inc_initiated_header_jobs(); // TODO: update metrics

        let cached_reads = this.cached_reads.take().unwrap_or_default();

        let waker = cx.waker().clone();

        this.executor.spawn_blocking(Box::pin(async move {
            // acquire the permit for executing the task
            let _permit = guard.0.acquire().await;
            create_header(
                client,
                pool,
                cached_reads,
                payload_config,
                cancel,
                tx,
                waker,
                digests,
                missing_batches,
            )
        }));

        // store the pending header for next poll
        this.pending_header = Some(PendingHeader { _cancel, payload: rx });

        Poll::Pending
    }
}

/// A future that resolves to the result of the header building job.
#[derive(Debug)]
pub(crate) struct PendingHeader {
    /// The marker to cancel the job on drop
    _cancel: Cancelled,
    /// The channel to send the result to.
    payload: oneshot::Receiver<Result<HeaderPayload, LatticePayloadBuilderError>>,
}

impl Future for PendingHeader {
    type Output = Result<HeaderPayload, LatticePayloadBuilderError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let res = ready!(self.payload.poll_unpin(cx));
        Poll::Ready(res.map_err(Into::into).and_then(|res| res))
    }
}

/// Static config for how to build a header payload
/// using the block env, state env, and parent block.
#[derive(Clone)]
pub(crate) struct HeaderPayloadConfig {
    /// Attributes of the requested header payload.
    pub(crate) attributes: BuildHeaderRequest,
    /// Pre-configured block environment.
    pub(crate) initialized_block_env: BlockEnv,
    /// Configuration for the environment.
    pub(crate) initialized_cfg: CfgEnv,
    /// The parent block.
    pub(crate) parent_block: Arc<SealedBlock>,
    /// The maximum size of the header (measured in number of digests).
    pub(crate) max_header_size: usize,
    /// The chain spec.
    pub(crate) chain_spec: Arc<ChainSpec>,
}
