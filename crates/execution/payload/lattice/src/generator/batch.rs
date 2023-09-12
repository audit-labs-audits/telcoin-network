//! Implement generator for building batch payload jobs.

use execution_payload_builder::{
    error::PayloadBuilderError, database::CachedReads,
};
use execution_provider::{BlockReaderIdExt, StateProviderFactory};
use execution_rlp::Encodable;
use execution_tasks::TaskSpawner;
use execution_transaction_pool::{TransactionPool, TransactionId, BatchInfo};
use lattice_network::EngineToWorkerClient;
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
}, consensus::{ConditionalBroadcastReceiver, BatchDigest,}};
use tn_network_types::EngineToWorker;
use tokio::sync::{Semaphore, oneshot, mpsc::Receiver};

use crate::{HeaderPayloadJob, BatchPayloadJob, BatchPayloadJobGenerator, LatticePayloadBuilderError, BatchPayload, BatchPayloadConfig, LatticePayloadJobGenerator};

// === impl BatchPayloadJobGenerator ===

impl<Client, Pool, Tasks, Network> BatchPayloadJobGenerator for LatticePayloadJobGenerator<Client, Pool, Tasks, Network>
where
    Client: StateProviderFactory + BlockReaderIdExt + Clone + Unpin + 'static,
    Pool: TransactionPool + Unpin + 'static,
    Tasks: TaskSpawner + Clone + Unpin + 'static,
    Network: EngineToWorkerClient + Clone + Unpin + Send + Sync + 'static,
{
    type Job = BatchPayloadJob<Client, Pool, Tasks, Network>;

    /// Method is called each time a worker requests a new batch.
    /// 
    /// TODO: this function uses a lot of defaults for now (zero u256, etc.)
    fn new_batch_job(
        &self,
    ) -> Result<Self::Job, LatticePayloadBuilderError> {
        // TODO: is it better to use "latest" for all blocks or "finalized"
        // between leaders, the canonical chain isn't finalized yet, so 
        // batches are built on a validator's state
        //
        // should batches be built on finalized state or "latest"?
        //
        // Update: Latticees should be built off this primary's "pending" block that's reached quorum?
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
                .ok_or_else(|| LatticePayloadBuilderError::LatticeBatchFromGenesis)?
                .seal_slow();
        // } else {
        //     self
        //         .client
        //         // build off canonical state
        //         .block_by_number_or_tag(BlockNumberOrTag::Finalized)?
        //         .ok_or_else(|| PayloadBuilderError::LatticeLattice)?
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
            pending_broadcast: None,
            payload_transactions: vec![],
            cached_reads: None,
            payload_task_guard: self.payload_task_guard.clone(),
            metrics: Default::default(),
            network: self.network.clone(),
        })
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LatticePayloadJobGeneratorConfig;
    use std::num::NonZeroUsize;
    use execution_provider::test_utils::{MockEthProvider, blocks::BlockChainTestData};
    use execution_tasks::TokioTaskExecutor;
    use execution_transaction_pool::test_utils::testing_pool;
    use fastcrypto::traits::KeyPair;
    use lattice_network::client::NetworkClient;
    use lattice_test_utils::CommitteeFixture;
    use telcoin_network::args::utils::genesis_value_parser;

    fn _create_mock_provider() -> MockEthProvider {
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
        let config = LatticePayloadJobGeneratorConfig::default();
        let chain_spec = genesis_value_parser("lattice").unwrap();
        // TODO: abstract this to test utils
        let fixture = CommitteeFixture::builder()
            .number_of_workers(NonZeroUsize::new(1).unwrap())
            .randomize_ports(true)
            .build();
        let authority = fixture.authorities().next().unwrap();
        let network = NetworkClient::new_from_keypair(&authority.network_keypair(), &authority.engine_network_keypair().public());

        let generator = LatticePayloadJobGenerator::new(
            client,
            pool,
            tasks,
            config,
            chain_spec,
            network,
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
