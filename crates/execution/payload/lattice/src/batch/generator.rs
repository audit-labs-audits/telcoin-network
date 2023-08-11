//! Generator for building batch payload jobs.

use execution_payload_builder::{
    error::PayloadBuilderError, PayloadBuilderAttributes, PayloadJobGenerator,
};
use execution_provider::{BlockReaderIdExt, BlockSource, StateProviderFactory};
use execution_rlp::Encodable;
use execution_tasks::TaskSpawner;
use execution_transaction_pool::TransactionPool;
use std::{
    time::Duration,
    sync::Arc,
};
use tn_types::execution::{
    bytes::{Bytes, BytesMut},
    constants::{
        EXECUTION_CLIENT_VERSION, 
        ETHEREUM_BLOCK_GAS_LIMIT, SLOT_DURATION,
    },
    BlockNumberOrTag, ChainSpec,
};
use tokio::sync::Semaphore;

use super::job::{BatchPayloadJob, PayloadConfig};

/// The [PayloadJobGenerator] that creates [BatchPayloadJob]s.
pub struct BatchPayloadJobGenerator<Client, Pool, Tasks> {
    /// The client that can interact with the chain.
    client: Client,
    /// txpool
    pool: Pool,
    /// How to spawn building tasks
    executor: Tasks,
    /// The configuration for the job generator.
    config: BatchPayloadJobGeneratorConfig,
    /// Restricts how many generator tasks can be executed at once.
    payload_task_guard: PayloadTaskGuard,
    /// The chain spec.
    chain_spec: Arc<ChainSpec>,
}

// === impl BatchPayloadJobGenerator ===

impl<Client, Pool, Tasks> BatchPayloadJobGenerator<Client, Pool, Tasks> {
    /// Creates a new [BatchPayloadJobGenerator] with the given config.
    pub fn new(
        client: Client,
        pool: Pool,
        executor: Tasks,
        config: BatchPayloadJobGeneratorConfig,
        chain_spec: Arc<ChainSpec>,
    ) -> Self {
        Self {
            client,
            pool,
            executor,
            payload_task_guard: PayloadTaskGuard::new(config.max_payload_tasks),
            config,
            chain_spec,
        }
    }
}

// === impl BatchPayloadJobGenerator ===

impl<Client, Pool, Tasks> BatchPayloadJobGenerator<Client, Pool, Tasks> {}

impl<Client, Pool, Tasks> PayloadJobGenerator for BatchPayloadJobGenerator<Client, Pool, Tasks>
where
    Client: StateProviderFactory + BlockReaderIdExt + Clone + Unpin + 'static,
    Pool: TransactionPool + Unpin + 'static,
    Tasks: TaskSpawner + Clone + Unpin + 'static,
{
    type Job = BatchPayloadJob<Client, Pool, Tasks>;

    fn new_payload_job(
        &self,
        attributes: PayloadBuilderAttributes,
    ) -> Result<Self::Job, PayloadBuilderError> {
        let parent_block = if attributes.parent.is_zero() {
            // use latest block if parent is zero: genesis block
            self.client
                .block_by_number_or_tag(BlockNumberOrTag::Latest)?
                .ok_or_else(|| PayloadBuilderError::MissingParentBlock(attributes.parent))?
                .seal_slow()
        } else {
            let block = self
                .client
                .find_block_by_hash(attributes.parent, BlockSource::Any)?
                .ok_or_else(|| PayloadBuilderError::MissingParentBlock(attributes.parent))?;

            // we already know the hash, so we can seal it
            block.seal(attributes.parent)
        };

        // configure evm env based on parent block
        let (initialized_cfg, initialized_block_env) =
            attributes.cfg_and_block_env(&self.chain_spec, &parent_block);

        // TODO: make a `new()` method once fields are finalized
        let config = PayloadConfig {
            initialized_block_env,
            initialized_cfg,
            parent_block: Arc::new(parent_block),
            extra_data: self.config.extradata.clone(),
            attributes,
            chain_spec: Arc::clone(&self.chain_spec),
        };

        let until = tokio::time::Instant::now() + self.config.deadline;
        let deadline = Box::pin(tokio::time::sleep_until(until));

        // TODO: make a `new()` method once fields are finalized
        Ok(BatchPayloadJob {
            config,
            client: self.client.clone(),
            pool: self.pool.clone(),
            executor: self.executor.clone(),
            deadline,
            interval: tokio::time::interval(self.config.interval),
            best_payload: None,
            pending_block: None,
            cached_reads: None,
            payload_task_guard: self.payload_task_guard.clone(),
            metrics: Default::default(),
        })
    }
}

/// Restricts how many generator tasks can be executed at once.
#[derive(Clone)]
pub(super) struct PayloadTaskGuard(pub(super) Arc<Semaphore>);

// === impl PayloadTaskGuard ===

impl PayloadTaskGuard {
    fn new(max_payload_tasks: usize) -> Self {
        Self(Arc::new(Semaphore::new(max_payload_tasks)))
    }
}

/// Settings for the [BatchPayloadJobGenerator].
#[derive(Debug, Clone)]
pub struct BatchPayloadJobGeneratorConfig {
    /// Data to include in the block's extra data field.
    extradata: Bytes,
    /// Target gas ceiling for built blocks, defaults to [ETHEREUM_BLOCK_GAS_LIMIT] gas.
    max_gas_limit: u64,
    /// The interval at which the job should build a new payload after the last.
    interval: Duration,
    /// The deadline for when the payload builder job should resolve.
    deadline: Duration,
    /// Maximum number of tasks to spawn for building a payload.
    max_payload_tasks: usize,
}

// === impl BatchPayloadJobGeneratorConfig ===

impl BatchPayloadJobGeneratorConfig {
    /// Sets the interval at which the job should build a new payload after the last.
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Sets the deadline when this job should resolve.
    pub fn deadline(mut self, deadline: Duration) -> Self {
        self.deadline = deadline;
        self
    }

    /// Sets the maximum number of tasks to spawn for building a payload(s).
    ///
    /// # Panics
    ///
    /// If `max_payload_tasks` is 0.
    pub fn max_payload_tasks(mut self, max_payload_tasks: usize) -> Self {
        assert!(max_payload_tasks > 0, "max_payload_tasks must be greater than 0");
        self.max_payload_tasks = max_payload_tasks;
        self
    }

    /// Sets the data to include in the block's extra data field.
    ///
    /// Defaults to the current client version: `rlp(EXECUTION_CLIENT_VERSION)`.
    pub fn extradata(mut self, extradata: Bytes) -> Self {
        self.extradata = extradata;
        self
    }

    /// Sets the target gas ceiling for mined blocks.
    ///
    /// Defaults to [ETHEREUM_BLOCK_GAS_LIMIT] gas.
    pub fn max_gas_limit(mut self, max_gas_limit: u64) -> Self {
        self.max_gas_limit = max_gas_limit;
        self
    }
}

impl Default for BatchPayloadJobGeneratorConfig {
    fn default() -> Self {
        let mut extradata = BytesMut::new();
        EXECUTION_CLIENT_VERSION.as_bytes().encode(&mut extradata);
        Self {
            extradata: extradata.freeze(),
            max_gas_limit: ETHEREUM_BLOCK_GAS_LIMIT,
            interval: Duration::from_secs(1),
            // 12s slot time
            deadline: SLOT_DURATION,
            max_payload_tasks: 3,
        }
    }
}
