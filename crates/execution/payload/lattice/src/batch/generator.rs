//! Generator for building batch payload jobs.

use execution_payload_builder::{
    error::PayloadBuilderError, database::CachedReads,
};
use execution_provider::{BlockReaderIdExt, StateProviderFactory};
use execution_rlp::Encodable;
use execution_tasks::TaskSpawner;
use execution_transaction_pool::{TransactionPool, TransactionId, BatchInfo};
use revm::primitives::{CfgEnv, BlockEnv, Address};
use tracing::{warn, debug, info};
use std::{
    time::{Duration, UNIX_EPOCH},
    sync::Arc,
};
use tn_types::{execution::{
    bytes::{Bytes, BytesMut},
    constants::{
        EXECUTION_CLIENT_VERSION, 
        ETHEREUM_BLOCK_GAS_LIMIT,
    },
    BlockNumberOrTag, ChainSpec, U256,
}, consensus::{ConditionalBroadcastReceiver, BatchDigest}};
use tokio::sync::{Semaphore, oneshot, mpsc::Receiver};

use crate::batch::{helpers::create_batch, job::{BatchPayloadConfig, Cancelled}};

use super::{job::{BatchPayloadFuture, BatchPayloadJob}, traits::BatchJobGenerator, BatchBuilderError, metrics::PayloadSizeMetric};

/// Helper type to represent a CL Batch.
/// 
/// TODO: add batch size and gas used as metrics to struct
/// since they're already calculated in the job.
#[derive(Debug)]
pub struct BuiltBatch {
    batch: Vec<Vec<u8>>,
    executed_txs: Vec<TransactionId>,
    size_metric: PayloadSizeMetric,
}

impl BuiltBatch {
    /// Create a new instance of [Self]
    pub fn new(
        batch: Vec<Vec<u8>>,
        executed_txs: Vec<TransactionId>,
        size_metric: PayloadSizeMetric,
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
    pub fn reason(&self) -> &PayloadSizeMetric {
        &self.size_metric
    }
}

/// The [PayloadJobGenerator] that creates [BatchPayloadJob]s.
/// 
/// Responsible for initializing the block and environment for 
/// pending transactions to execute against.
/// 
/// The generator also
/// updates the transaction pool once a batch is sealed.
pub struct BatchPayloadJobGenerator<Client, Pool, Tasks> {
    /// The client that can interact with the chain.
    client: Client,
    /// txpool
    pool: Pool,
    /// How to spawn building tasks
    executor: Tasks,
    /// The configuration for the job generator.
    /// 
    /// TODO: actually use this
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

impl<Client, Pool, Tasks> BatchJobGenerator for BatchPayloadJobGenerator<Client, Pool, Tasks>
where
    Client: StateProviderFactory + BlockReaderIdExt + Clone + Unpin + 'static,
    Pool: TransactionPool + Unpin + 'static,
    Tasks: TaskSpawner + Clone + Unpin + 'static,
{
    type Job = BatchPayloadJob<Client, Pool, Tasks>;

    /// Method is called each time a worker requests a new batch.
    /// 
    /// TODO: this function uses a lot of defaults for now (zero u256, etc.)
    fn new_batch_job(
        &self,
        // tx_to_worker: oneshot::Sender<(Batch, oneshot::Sender<BatchDigest>)>,
    ) -> Result<Self::Job, BatchBuilderError> {
        // TODO: is it better to use "latest" for all blocks or "finalized"
        // between leaders, the canonical chain isn't finalized yet, so 
        // batches are built on a validator's state
        //
        // should batches be built on finalized state or "latest"?
        //
        // Update: Batches should be built off this primary's "pending" block that's reached quorum?
        // - how to account for other certs already received?
        // if a cert is issued, it means quorum for the block.
        // if quorum is reached for the block, is it _impossible_ for the block not to be included?

        // TODO: figure out way to determine genesis parent block
        // eth protocol expects the CL to pass payload attributes,
        // but worker doesn't need to know any of this information.
        let parent_block = // if attributes.parent.is_zero() {
            // use latest block if parent is zero: genesis block
            self.client
                .block_by_number_or_tag(BlockNumberOrTag::Latest)?
                .ok_or_else(|| BatchBuilderError::LatticeBatchFromGenesis)?
                .seal_slow();
        // } else {
        //     self
        //         .client
        //         // build off canonical state
        //         .block_by_number_or_tag(BlockNumberOrTag::Finalized)?
        //         .ok_or_else(|| PayloadBuilderError::LatticeBatch)?
        //         .seal_slow()
        // };

        // TODO: CfgEnv has a lot of options that may be useful for TN environment
        // configure evm env based on parent block
        let initialized_cfg = CfgEnv {
            chain_id: U256::from(self.chain_spec.chain().id()),
            // ensure we're not missing any timestamp based hardforks
            spec_id: revm::primitives::SHANGHAI,
            ..Default::default()
        };

        // TODO: use better values
        // - coinbase
        // - prevrandao
        // - gas_limit
        // - basefee
        let timestamp = std::time::SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        // create the block environment to execute transactions from
        let initialized_block_env = BlockEnv {
            number: U256::from(parent_block.number + 1),
            coinbase: Address::zero(),
            timestamp: U256::from(timestamp),
            difficulty: U256::ZERO,
            prevrandao: Some(U256::ZERO.into()),
            gas_limit: U256::MAX,
            // TODO: calculate basefee based on parent block's gas usage?
            basefee: U256::ZERO,
        };

        let config = BatchPayloadConfig {
            initialized_block_env,
            initialized_cfg,
            parent_block: Arc::new(parent_block),
            // TODO: get this from worker or configuration
            max_batch_size: 5_000_000,
        };

        Ok(BatchPayloadJob {
            config,
            client: self.client.clone(),
            pool: self.pool.clone(),
            executor: self.executor.clone(),
            pending_batch: None,
            cached_reads: None,
            payload_task_guard: self.payload_task_guard.clone(),
            metrics: Default::default(),
        })
    }

    fn batch_sealed(
        &self,
        batch: Arc<BuiltBatch>,
        digest: BatchDigest,
    ) -> Result<(), BatchBuilderError> {
        let batch_info = BatchInfo::new(digest, batch.get_transaction_ids().clone());
        self.pool.on_sealed_batch(batch_info);
        Ok(())
    }
}

/// Restricts how many generator tasks can be executed at once.
#[derive(Clone)]
pub(crate) struct PayloadTaskGuard(pub(super) Arc<Semaphore>);

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

    /// Sets the maximum number of tasks to spawn for building a batch payload(s).
    ///
    /// # Panics
    ///
    /// If `max_payload_tasks` is 0.
    pub fn max_payload_tasks(mut self, max_payload_tasks: usize) -> Self {
        assert!(max_payload_tasks > 0, "max_payload_tasks must be greater than 0");
        self.max_payload_tasks = max_payload_tasks;
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
            // TODO: set default size limits
            max_gas_limit: ETHEREUM_BLOCK_GAS_LIMIT,
            // TODO: remove?
            interval: Duration::from_secs(1),
            // only 1 batch can be built from the pending pool at a time
            max_payload_tasks: 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use execution_provider::test_utils::{MockEthProvider, blocks::BlockChainTestData};
    use execution_tasks::TokioTaskExecutor;
    use execution_transaction_pool::test_utils::testing_pool;
    use telcoin_network::args::utils::genesis_value_parser;

    fn create_mock_provider() -> MockEthProvider {
        let provider = MockEthProvider::default();
        let data = BlockChainTestData::default();
        // add a known genesis block
        provider.add_block(data.genesis.hash, data.genesis.into());
        provider
    }

    #[tokio::test]
    async fn test_generator_new_batch_job() {
        let client = MockEthProvider::default();
        let data = BlockChainTestData::default();
        // add a known genesis block
        client.add_block(data.genesis.hash, data.genesis.clone().into());

        let pool = testing_pool();
        let tasks = TokioTaskExecutor::default();
        let config = BatchPayloadJobGeneratorConfig::default();
        let chain_spec = genesis_value_parser("lattice").unwrap();
        let generator = BatchPayloadJobGenerator::new(
            client,
            pool,
            tasks,
            config,
            chain_spec,
        );

        let job = generator.new_batch_job().unwrap();

        // TODO: assert parent block
        // let parent_block = data.genesis.hash();
        // assert_eq!(job.config.parent_block.hash, parent_block);

        // assert block environment
        assert_eq!(job.config.initialized_block_env.number, U256::from(1));
        assert_eq!(job.config.initialized_block_env.coinbase, Address::zero());
        assert_eq!(job.config.initialized_block_env.difficulty, U256::ZERO);
        assert_eq!(job.config.initialized_block_env.prevrandao, Some(U256::ZERO.into()));
        assert_eq!(job.config.initialized_block_env.gas_limit, U256::MAX);
        assert_eq!(job.config.initialized_block_env.basefee, U256::ZERO);
        assert!(job.pending_batch.is_none());

    }

}
