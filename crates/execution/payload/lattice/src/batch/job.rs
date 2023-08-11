//! Batch payload building job.

use crate::{metrics::PayloadBuilderMetrics, batch::helpers::{commit_withdrawals, is_better_payload, build_payload}};
use execution_payload_builder::{
    database::CachedReads, error::PayloadBuilderError, BuiltPayload, KeepPayloadJobAlive,
    PayloadBuilderAttributes, PayloadJob,
};
use execution_provider::{PostState, StateProviderFactory};
use execution_revm::{
    database::{State, SubState},
    env::tx_env_with_recovered,
    executor::commit_state_changes,
    into_execution_log,
};
use execution_tasks::TaskSpawner;
use execution_transaction_pool::TransactionPool;
use futures_core::ready;
use futures_util::FutureExt;
use revm::{
    db::CacheDB,
    primitives::{BlockEnv, CfgEnv, EVMError, Env, InvalidTransaction, ResultAndState},
};
use std::{
    future::Future,
    pin::Pin,
    sync::{atomic::AtomicBool, Arc},
    task::{Context, Poll},
};
use tn_types::execution::{
    bytes::Bytes,
    constants::{
        BEACON_NONCE, EMPTY_RECEIPTS, EMPTY_TRANSACTIONS, EMPTY_WITHDRAWALS,
    },
    proofs, Block, ChainSpec, Header, IntoRecoveredTransaction, Receipt,
    SealedBlock, Withdrawal, EMPTY_OMMER_ROOT, H256, U256,
};
use tokio::{
    sync::oneshot,
    time::{Interval, Sleep},
};
use tracing::{debug, trace};

use super::generator::PayloadTaskGuard;

/// A basic payload job that continuously builds a payload with the best transactions from the pool.
pub struct BatchPayloadJob<Client, Pool, Tasks> {
    /// The configuration for how the payload will be created.
    pub(super) config: PayloadConfig,
    /// The client that can interact with the chain.
    pub(super) client: Client,
    /// The transaction pool.
    pub(super) pool: Pool,
    /// How to spawn building tasks
    pub(super) executor: Tasks,
    /// The deadline when this job should resolve.
    pub(super) deadline: Pin<Box<Sleep>>,
    /// The interval at which the job should build a new payload after the last.
    pub(super) interval: Interval,
    /// The best payload so far.
    pub(super) best_payload: Option<Arc<BuiltPayload>>,
    /// Receiver for the block that is currently being built.
    pub(super) pending_block: Option<PendingPayload>,
    /// Restricts how many generator tasks can be executed at once.
    pub(super) payload_task_guard: PayloadTaskGuard,
    /// Caches all disk reads for the state the new payloads builds on
    ///
    /// This is used to avoid reading the same state over and over again when new attempts are
    /// triggerd, because during the building process we'll repeatedly execute the transactions.
    pub(super) cached_reads: Option<CachedReads>,
    /// metrics for this type
    pub(super) metrics: PayloadBuilderMetrics,
}

impl<Client, Pool, Tasks> Future for BatchPayloadJob<Client, Pool, Tasks>
where
    Client: StateProviderFactory + Clone + Unpin + 'static,
    Pool: TransactionPool + Unpin + 'static,
    Tasks: TaskSpawner + Clone + 'static,
{
    type Output = Result<(), PayloadBuilderError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // check if the deadline is reached
        if this.deadline.as_mut().poll(cx).is_ready() {
            trace!("Payload building deadline reached");
            return Poll::Ready(Ok(()))
        }

        // check if the interval is reached
        while this.interval.poll_tick(cx).is_ready() {
            // start a new job if there is no pending block and we haven't reached the deadline
            if this.pending_block.is_none() {
                trace!("spawn new payload build task");
                let (tx, rx) = oneshot::channel();
                let client = this.client.clone();
                let pool = this.pool.clone();
                let cancel = Cancelled::default();
                let _cancel = cancel.clone();
                let guard = this.payload_task_guard.clone();
                let payload_config = this.config.clone();
                let best_payload = this.best_payload.clone();
                this.metrics.inc_initiated_payload_builds();
                let cached_reads = this.cached_reads.take().unwrap_or_default();
                this.executor.spawn_blocking(Box::pin(async move {
                    // acquire the permit for executing the task
                    let _permit = guard.0.acquire().await;
                    build_payload(
                        client,
                        pool,
                        cached_reads,
                        payload_config,
                        cancel,
                        best_payload,
                        tx,
                    )
                }));
                this.pending_block = Some(PendingPayload { _cancel, payload: rx });
            }
        }

        // poll the pending block
        if let Some(mut fut) = this.pending_block.take() {
            match fut.poll_unpin(cx) {
                Poll::Ready(Ok(outcome)) => {
                    this.interval.reset();
                    match outcome {
                        BuildOutcome::Better { payload, cached_reads } => {
                            this.cached_reads = Some(cached_reads);
                            trace!("built better payload");
                            let payload = Arc::new(payload);
                            this.best_payload = Some(payload);
                        }
                        BuildOutcome::Aborted { fees, cached_reads } => {
                            this.cached_reads = Some(cached_reads);
                            trace!(?fees, "skipped payload build of worse block");
                        }
                        BuildOutcome::Cancelled => {
                            unreachable!("the cancel signal never fired")
                        }
                    }
                }
                Poll::Ready(Err(err)) => {
                    // job failed, but we simply try again next interval
                    trace!(?err, "payload build attempt failed");
                    this.metrics.inc_failed_payload_builds();
                }
                Poll::Pending => {
                    this.pending_block = Some(fut);
                }
            }
        }

        Poll::Pending
    }
}

impl<Client, Pool, Tasks> PayloadJob for BatchPayloadJob<Client, Pool, Tasks>
where
    Client: StateProviderFactory + Clone + Unpin + 'static,
    Pool: TransactionPool + Unpin + 'static,
    Tasks: TaskSpawner + Clone + 'static,
{
    type ResolvePayloadFuture = ResolveBestPayload;

    fn best_payload(&self) -> Result<Arc<BuiltPayload>, PayloadBuilderError> {
        if let Some(ref payload) = self.best_payload {
            return Ok(payload.clone())
        }
        // No payload has been built yet, but we need to return something that the CL then can
        // deliver, so we need to return an empty payload.
        //
        // Note: it is assumed that this is unlikely to happen, as the payload job is started right
        // away and the first full block should have been built by the time CL is requesting the
        // payload.
        self.metrics.inc_requested_empty_payload();
        build_empty_payload(&self.client, self.config.clone()).map(Arc::new)
    }

    // TODO: is it okay to return an empty payload here if best_payload.is_none()?
    fn resolve(&mut self) -> (Self::ResolvePayloadFuture, KeepPayloadJobAlive) {
        let best_payload = self.best_payload.take();
        let maybe_better = self.pending_block.take();
        let mut empty_payload = None;

        if best_payload.is_none() {
            // if no payload has been built yet
            self.metrics.inc_requested_empty_payload();
            // no payload built yet, so we need to return an empty payload
            let (tx, rx) = oneshot::channel();
            let client = self.client.clone();
            let config = self.config.clone();
            self.executor.spawn_blocking(Box::pin(async move {
                let res = build_empty_payload(&client, config);
                let _ = tx.send(res);
            }));

            empty_payload = Some(rx);
        }

        let fut = ResolveBestPayload { best_payload, maybe_better, empty_payload };

        (fut, KeepPayloadJobAlive::No)
    }
}

/// The future that returns the best payload to be served to the consensus layer.
///
/// This returns the payload that's supposed to be sent to the CL.
///
/// If payload has been built so far, it will return that, but it will check if there's a better
/// payload available from an in progress build job. If so it will return that.
///
/// If no payload has been built so far, it will either return an empty payload or the result of the
/// in progress build job, whatever finishes first.
#[derive(Debug)]
pub struct ResolveBestPayload {
    /// Best payload so far.
    best_payload: Option<Arc<BuiltPayload>>,
    /// Regular payload job that's currently running that might produce a better payload.
    maybe_better: Option<PendingPayload>,
    /// The empty payload building job in progress.
    empty_payload: Option<oneshot::Receiver<Result<BuiltPayload, PayloadBuilderError>>>,
}

impl Future for ResolveBestPayload {
    type Output = Result<Arc<BuiltPayload>, PayloadBuilderError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // check if there is a better payload before returning the best payload
        if let Some(fut) = Pin::new(&mut this.maybe_better).as_pin_mut() {
            if let Poll::Ready(res) = fut.poll(cx) {
                this.maybe_better = None;
                if let Ok(BuildOutcome::Better { payload, .. }) = res {
                    return Poll::Ready(Ok(Arc::new(payload)))
                }
            }
        }

        if let Some(best) = this.best_payload.take() {
            return Poll::Ready(Ok(best))
        }

        let mut empty_payload = this.empty_payload.take().expect("polled after completion");
        match empty_payload.poll_unpin(cx) {
            Poll::Ready(Ok(res)) => Poll::Ready(res.map(Arc::new)),
            Poll::Ready(Err(err)) => Poll::Ready(Err(err.into())),
            Poll::Pending => {
                this.empty_payload = Some(empty_payload);
                Poll::Pending
            }
        }
    }
}

/// A future that resolves to the result of the block building job.
#[derive(Debug)]
pub(super) struct PendingPayload {
    /// The marker to cancel the job on drop
    _cancel: Cancelled,
    /// The channel to send the result to.
    payload: oneshot::Receiver<Result<BuildOutcome, PayloadBuilderError>>,
}

impl Future for PendingPayload {
    type Output = Result<BuildOutcome, PayloadBuilderError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let res = ready!(self.payload.poll_unpin(cx));
        Poll::Ready(res.map_err(Into::into).and_then(|res| res))
    }
}

/// A marker that can be used to cancel a job.
///
/// If dropped, it will set the `cancelled` flag to true.
#[derive(Default, Clone, Debug)]
pub(super) struct Cancelled(Arc<AtomicBool>);

// === impl Cancelled ===

impl Cancelled {
    /// Returns true if the job was cancelled.
    pub(super) fn is_cancelled(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Drop for Cancelled {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Static config for how to build a payload.
#[derive(Clone)]
pub(super) struct PayloadConfig {
    /// Pre-configured block environment.
    pub(super) initialized_block_env: BlockEnv,
    /// Configuration for the environment.
    pub(super) initialized_cfg: CfgEnv,
    /// The parent block.
    pub(super) parent_block: Arc<SealedBlock>,
    /// Block extra data.
    pub(super) extra_data: Bytes,
    /// Requested attributes for the payload.
    pub(super) attributes: PayloadBuilderAttributes,
    /// The chain spec.
    pub(super) chain_spec: Arc<ChainSpec>,
}

#[derive(Debug)]
pub(super) enum BuildOutcome {
    /// Successfully built a better block.
    Better {
        /// The new payload that was built.
        payload: BuiltPayload,
        /// The cached reads that were used to build the payload.
        cached_reads: CachedReads,
    },
    /// Aborted payload building because resulted in worse block wrt. fees.
    Aborted {
        fees: U256,
        /// The cached reads that were used to build the payload.
        cached_reads: CachedReads,
    },
    /// Build job was cancelled
    Cancelled,
}
/// Builds an empty payload without any transactions.
fn build_empty_payload<Client>(
    client: &Client,
    config: PayloadConfig,
) -> Result<BuiltPayload, PayloadBuilderError>
where
    Client: StateProviderFactory,
{
    let PayloadConfig {
        initialized_block_env,
        parent_block,
        extra_data,
        attributes,
        chain_spec,
        ..
    } = config;

    debug!(parent_hash=?parent_block.hash, parent_number=parent_block.number,  "building empty payload");

    let state = client.state_by_block_hash(parent_block.hash)?;
    let mut db = SubState::new(State::new(state));
    let mut post_state = PostState::default();

    let base_fee = initialized_block_env.basefee.to::<u64>();
    let block_number = initialized_block_env.number.to::<u64>();
    let block_gas_limit: u64 = initialized_block_env.gas_limit.try_into().unwrap_or(u64::MAX);

    let WithdrawalsOutcome { withdrawals_root, withdrawals } = commit_withdrawals(
        &mut db,
        &mut post_state,
        &chain_spec,
        block_number,
        attributes.timestamp,
        attributes.withdrawals,
    )?;

    // calculate the state root
    let state_root = db.db.0.state_root(post_state)?;

    let header = Header {
        parent_hash: parent_block.hash,
        ommers_hash: EMPTY_OMMER_ROOT,
        beneficiary: initialized_block_env.coinbase,
        state_root,
        transactions_root: EMPTY_TRANSACTIONS,
        withdrawals_root,
        receipts_root: EMPTY_RECEIPTS,
        logs_bloom: Default::default(),
        timestamp: attributes.timestamp,
        mix_hash: attributes.prev_randao,
        nonce: BEACON_NONCE,
        base_fee_per_gas: Some(base_fee),
        number: parent_block.number + 1,
        gas_limit: block_gas_limit,
        difficulty: U256::ZERO,
        gas_used: 0,
        extra_data: extra_data.into(),
    };

    let block = Block { header, body: vec![], ommers: vec![], withdrawals };
    let sealed_block = block.seal_slow();

    Ok(BuiltPayload::new(attributes.id, sealed_block, U256::ZERO))
}

/// Represents the outcome of committing withdrawals to the runtime database and post state.
/// Pre-shanghai these are `None` values.
pub(super) struct WithdrawalsOutcome {
    pub(super) withdrawals: Option<Vec<Withdrawal>>,
    pub(super) withdrawals_root: Option<H256>,
}

// TODO: there is no "pre-shanghai" for telcoin network
impl WithdrawalsOutcome {
    /// No withdrawals pre shanghai
    pub(super) fn pre_shanghai() -> Self {
        Self { withdrawals: None, withdrawals_root: None }
    }

    pub(super) fn empty() -> Self {
        Self { withdrawals: Some(vec![]), withdrawals_root: Some(EMPTY_WITHDRAWALS) }
    }
}
