//! Implement generator for building header payload jobs.

use execution_payload_builder::{
    error::PayloadBuilderError, database::CachedReads,
};
use execution_provider::{BlockReaderIdExt, StateProviderFactory};
use execution_rlp::Encodable;
use execution_tasks::TaskSpawner;
use execution_transaction_pool::{TransactionPool, TransactionId, BatchInfo};
use fastcrypto::hash::Hash;
use indexmap::IndexMap;
use revm::primitives::{CfgEnv, BlockEnv, Address};
use tracing::{warn, debug, info};
use std::{
    time::{Duration, UNIX_EPOCH},
    sync::Arc, collections::{HashMap, HashSet},
};
use tn_types::{execution::{
    bytes::{Bytes, BytesMut},
    constants::{
        EXECUTION_CLIENT_VERSION, 
        ETHEREUM_BLOCK_GAS_LIMIT,
    },
    BlockNumberOrTag, ChainSpec, U256, IntoRecoveredTransaction,
}, consensus::{ConditionalBroadcastReceiver, BatchDigest, CertificateDigest, WorkerId, TimestampMs, Batch}};
use tn_network_types::BuildHeaderRequest;
use tokio::sync::{Semaphore, oneshot, mpsc::Receiver};
use crate::{HeaderPayloadJob, BatchPayloadJob, BatchPayloadJobGenerator, LatticePayloadBuilderError, BatchPayload, BatchPayloadConfig, LatticePayloadJobGenerator, HeaderPayloadJobGenerator, HeaderPayloadConfig, HeaderPayload};
use lattice_network::EngineToWorkerClient;

// === impl BatchPayloadJobGenerator ===

impl<Client, Pool, Tasks, Network> HeaderPayloadJobGenerator for LatticePayloadJobGenerator<Client, Pool, Tasks, Network>
where
    Client: StateProviderFactory + BlockReaderIdExt + Clone + Unpin + 'static,
    Pool: TransactionPool + Unpin + 'static,
    Tasks: TaskSpawner + Clone + Unpin + 'static,
    Network: EngineToWorkerClient + Clone + Send + Sync,
{
    type Job = HeaderPayloadJob<Client, Pool, Tasks>;

    /// Method is called each time a worker requests a new batch.
    /// 
    /// TODO: this function uses a lot of defaults for now (zero u256, etc.)
    fn new_header_job(
        &self,
        attr: BuildHeaderRequest,
    ) -> Result<Self::Job, LatticePayloadBuilderError> {
        // TODO: Message contains parents for all CertificateDigests
        // - could these be used to construct the "parent block" between
        //   rounds of finality?

        // each block should be associated with the round number
        let parent_block = if attr.round == 0 {
            // use latest block if parent is zero: genesis block
            self.client
                .block_by_number_or_tag(BlockNumberOrTag::Latest)?
                .ok_or_else(|| LatticePayloadBuilderError::LatticeBatchFromGenesis)?
                .seal_slow()
        } else {
            // TODO: better to find by round or finalized?
            self
                .client
                // build off canonical state
                .block_by_number_or_tag(BlockNumberOrTag::Finalized)?
                .ok_or_else(|| LatticePayloadBuilderError::MissingFinalizedBlock(attr.round))?
                .seal_slow()
        };

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
        // create the block environment to execute transactions from parent
        let initialized_block_env = BlockEnv {
            number: U256::from(parent_block.number + 1),
            // number: U256::from(attr.round),
            coinbase: Address::zero(),
            timestamp: U256::from(attr.created_at),
            difficulty: U256::ZERO,
            // bls signatures + epoch
            // https://soliditydeveloper.com/prevrandao
            prevrandao: Some(U256::ZERO.into()),
            gas_limit: U256::MAX,
            // TODO: calculate basefee based on parent block's gas usage?
            basefee: U256::ZERO,
        };

        // check for batches here
        // optional receiver if any batches are missing/corrupted
        let missing_batches_rx = self.verify_batches_present(&attr.payload);

        let config = HeaderPayloadConfig {
            initialized_block_env,
            initialized_cfg,
            parent_block: Arc::new(parent_block),
            // TODO: get this from worker or configuration
            max_header_size: 3,
            chain_spec: self.chain_spec.clone(),
            attributes: attr,
        };

        Ok(HeaderPayloadJob {
            config,
            client: self.client.clone(),
            pool: self.pool.clone(),
            executor: self.executor.clone(),
            pending_header: None,
            cached_reads: None,
            payload_task_guard: self.payload_task_guard.clone(),
            metrics: Default::default(),
            missing_batches_rx,
        })
    }

    fn header_sealed(
        &self,
        header: Arc<HeaderPayload>,
        digest: CertificateDigest,
    ) -> Result<(), LatticePayloadBuilderError> {
        // let batch_info = BatchInfo::new(digest, batch.get_transaction_ids().clone());
        // self.pool.on_sealed_batch(batch_info);
        // Ok(())
        todo!()
    }

    fn verify_batches_present(
        &self,
        payload: &IndexMap<BatchDigest, (WorkerId, TimestampMs)>
    ) -> Option<oneshot::Receiver<HashMap<BatchDigest, Batch>>> {
    // ) {
        // either return oneshot channel
        // then check if Some(rx) { wait for rx}
        //
        // or just check, make the call and wait here for the reply

        // network_client.missing_batches()

        // collect missing batches by worker id
        let mut missing_batches: HashMap<&WorkerId, HashSet<BatchDigest>> = HashMap::new();//HashSet::new();
        for (digest, (worker_id, _timestamp)) in payload.iter() {
            let batch_pool = match self.pool.get_batch_transactions(digest) {
                Ok(pool) => pool,
                Err(_) => {
                    missing_batches
                        .entry(worker_id)
                        .and_modify(|set| {
                            set.insert(*digest);
                        })
                        .or_insert_with(|| {
                            let mut batches = HashSet::new();
                            batches.insert(*digest);
                            batches
                        });
                    continue
                }
            };

            let batch: Batch = batch_pool
                .all_transactions()
                .iter()
                .map(|tx| tx.to_recovered_transaction().into_signed().envelope_encoded().into())
                .collect::<Vec<Vec<u8>>>()
                .into();
            
            if batch.digest() != *digest {
                missing_batches
                    .entry(worker_id)
                    .and_modify(|set| {
                        set.insert(*digest);
                    })
                    .or_insert_with(|| {
                        let mut batches = HashSet::new();
                        batches.insert(*digest);
                        batches
                    });
            }
        }
            
        // spawn a task to get the missing batches from the worker
        // if any batches are missing and pass the receiver to the job
        let missing_batches_channel = match missing_batches.is_empty() {
            true => None,
            false => {
                let (tx, missing) = oneshot::channel();
                let pool = self.pool.clone();
                let network = self.network.clone();

                // need worker's peer_id for requesting batch from worker

                // two problems:
                // 1. request missing batches from worker by worker_id (need PeerId for network)
                //  can pass worker cache to proposer
                //  can pass worker cache to engine
                //  can add HashMap<WorkerId, PeerId> to NetworkClient
                //  can include worker's PeerId in OurBatchMessage, then map
                //  can store the batches 
                //  or - el only talks to a worker interface, the worker interface has the worker cache
                //
                // 2. update pool
                //  - add_missing_batches()
                //      - validate transactions based on batch's parent block
                //      - add txs to the pool
                //      - TODO: if this errors, then the node is stuck
                //              bc the primary will continue to propose
                //              the missing digests in the next round

                self.executor.spawn(Box::pin(async move {
                    // await worker response
                    // then update pool to ensure they're in the sealed pool

                    // TODO: request batches from worker store
                    // let response = network.missing_batches(worker_id, anemo::Request::new(missing_batches.into())).await;
                    // let returned_batches = match response {
                    //     Ok(msg) => msg.body().batches.to_owned(),
                    //     Err(_) => HashMap::default(), // return empty hashmap so job will fail
                    // };

                    // this does nothing - job will error
                    let returned_batches = HashMap::default();

                    // missing batches at this stage mean our batch reached quorum
                    // and is missing from the txpool
                    //
                    // this could happen if `on_sealed_batch` failed
                    // or if the engine/txpool restarted
                    //
                    // consider new method for `add_missing_batch` for sealed pool
                    //
                    // or
                    //
                    // retrieve batches and add to the txpool from scratch using the batch's
                    // `parent_block` to guranatee same execution when adding
                    //
                    // in order to be in this predicament, the txpool must have already
                    // validated the txs and passed them to a worker.
                    // therefore, it should be safe to short-circuit most of the txpool
                    // validation checks and just insert the batch directly into the 
                    // sealed pool so the header job can retrieve it and build the next
                    // block.

                    //  TODO: add_missing_batch() to pool that
                    // validates the batch based on the batch's parent block,
                    // adds to the pending pool, and calls on_sealed_batch
                    // while still obtaining the lock on pool.

                    // for now, easier to pass batches directly to job
                    // than try to validate and add through all pools
                    let _ = tx.send(returned_batches);
                }));
                Some(missing)
            }
        };

        missing_batches_channel
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
    async fn test_generator_new_header_job() {
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

        let request = BuildHeaderRequest {
            round: 0,
            epoch: 0,
            created_at: 0,
            payload: Default::default(),
            parents: Default::default(),
        };

        let job = generator.new_header_job(request).unwrap();

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
        assert!(job.pending_header.is_none());

    }
}
