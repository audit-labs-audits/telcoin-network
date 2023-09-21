//! Implement generator for building the next canonical block.
//! The block is created using the output from consensus.

use execution_provider::{BlockReaderIdExt, StateProviderFactory};
use execution_tasks::TaskSpawner;
use execution_transaction_pool::TransactionPool;
use revm::primitives::{CfgEnv, BlockEnv, Address};
use std::sync::Arc;
use tn_types::{
    execution::{BlockNumberOrTag, U256},
    consensus::ConsensusOutput
};
use lattice_network::EngineToWorkerClient;

use crate::{LatticePayloadJobGenerator, traits::BlockPayloadJobGenerator, BlockPayloadJob, LatticePayloadBuilderError, BlockPayloadConfig};

// === impl BatchPayloadJobGenerator ===

impl<Client, Pool, Tasks, Network> BlockPayloadJobGenerator for LatticePayloadJobGenerator<Client, Pool, Tasks, Network>
where
    Client: StateProviderFactory + BlockReaderIdExt + Clone + Unpin + 'static,
    Pool: TransactionPool + Unpin + 'static,
    Tasks: TaskSpawner + Clone + Unpin + 'static,
    Network: EngineToWorkerClient + Clone + Send + Sync,
{
    type Job = BlockPayloadJob<Client, Pool, Tasks>;

    /// Method is called each time a worker requests a new batch.
    /// 
    /// TODO: this function uses a lot of defaults for now (zero u256, etc.)
    fn new_canonical_block(
            &self,
            output: ConsensusOutput,
        ) -> Result<Self::Job, crate::LatticePayloadBuilderError> {
        // 1. find canonical parent block to build off
        // 2. initialize cfg and block env
        // 3. execute every transaction in order (skip txpool)
        
        // TODO: Message contains parents for all CertificateDigests
        // - could these be used to construct the "parent block" between
        //   rounds of finality?
        let leader_round = output.leader_round();
        let parent = output.parent_hash().to_owned();

        // each block should be associated with the round number
        let parent_block = if leader_round == 0 {
            // use latest block if parent is zero: genesis block
            self.client
                .block_by_number_or_tag(BlockNumberOrTag::Latest)?
                .ok_or_else(|| LatticePayloadBuilderError::LatticeBlockFromGenesis)?
                .seal_slow()
        } else {
            // TODO: better to find by hash or finalized?
            self
                .client
                // build off canonical state only
                .block_by_number_or_tag(BlockNumberOrTag::Finalized)?
                // finding block by hash could result in building off an older canonical
                // block
                //
                // .block_by_hash(parent)?
                .ok_or_else(|| LatticePayloadBuilderError::MissingParentBlock(parent))?
                .seal_slow()
        };

        // ensure parents match
        assert_eq!(&parent_block.hash, output.parent_hash());

        // TODO: CfgEnv has a lot of options that may be useful for TN environment
        // configure evm env based on parent block
        let initialized_cfg = CfgEnv {
            chain_id: U256::from(self.chain_spec.chain().id()),
            // ensure we're not missing any timestamp based hardforks
            spec_id: revm::primitives::SHANGHAI,
            ..Default::default()
        };

        // create the block environment to execute transactions from parent
        let initialized_block_env = BlockEnv {
            number: U256::from(parent_block.number + 1),
            // TODO: Authority needs address and public/private keys for signing
            coinbase: Address::zero(),
            timestamp: U256::from(output.committed_at()),
            difficulty: U256::ZERO,
            // TODO: should this be based on parent certs?
            //
            // bls signatures + epoch
            // https://soliditydeveloper.com/prevrandao
            //
            // for now, hash the aggregate signature bytes for the cert.
            // leader is selected by stake-weighted choice seeded by the round
            // for what amounts to essentially a round-robin
            //
            // see [Committee::leader()] for specifics
            prevrandao: Some(output.prevrandao()),
            // TODO: gas = total_num_batches * 30mil
            // 100 batches = 3bil gas max
            gas_limit: U256::MAX,
            // TODO: use default for genesis
            basefee: U256::from(parent_block.next_block_base_fee().unwrap_or_default()),
        };

        let config = BlockPayloadConfig {
            initialized_block_env,
            initialized_cfg,
            parent_block: Arc::new(parent_block),
            chain_spec: self.chain_spec.clone(),
            output,
        };

        Ok(BlockPayloadJob {
            config,
            client: self.client.clone(),
            pool: self.pool.clone(),
            executor: self.executor.clone(),
            cached_reads: None,
            payload_task_guard: self.payload_task_guard.clone(),
            metrics: Default::default(),
            pending_block: None,
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

    // TODO: test block after genesis - so parent is finalized block
    // TODO: include payloads with build header message

    #[tokio::test]
    async fn test_generator_new_block_job() {
        todo!()
    }
}
