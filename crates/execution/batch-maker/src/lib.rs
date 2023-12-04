//! A [Consensus] implementation for local testing purposes
//! that automatically seals blocks.
//!
//! The Mining task polls a [MiningMode], and will return a list of transactions that are ready to
//! be mined.
//!
//! These downloaders poll the miner, assemble the block, and return transactions that are ready to
//! be mined.

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/paradigmxyz/reth/main/assets/reth-docs.png",
    html_favicon_url = "https://avatars0.githubusercontent.com/u/97369466?s=256",
    issue_tracker_base_url = "https://github.com/paradigmxyz/reth/issues/"
)]
#![warn(missing_debug_implementations, missing_docs, unreachable_pub, rustdoc::all)]
#![deny(unused_must_use, rust_2018_idioms)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

use consensus_metrics::metered_channel::Sender;
use narwhal_types::{NewBatch, now};
use reth_interfaces::{
    consensus::{Consensus, ConsensusError},
    executor::{BlockExecutionError, BlockValidationError},
};
use reth_primitives::{
    constants::{EMPTY_RECEIPTS, EMPTY_TRANSACTIONS, ETHEREUM_BLOCK_GAS_LIMIT},
    proofs, Address, Block, BlockBody, BlockHash, BlockHashOrNumber, BlockNumber, Bloom, ChainSpec,
    Header, ReceiptWithBloom, SealedBlock, SealedHeader, TransactionSigned, B256,
    EMPTY_OMMER_ROOT_HASH, U256,
};
use reth_provider::{
    BlockExecutor, BlockReaderIdExt, BundleStateWithReceipts, StateProviderFactory,
};
use reth_revm::{
    database::StateProviderDatabase, db::states::bundle_state::BundleRetention,
    processor::EVMProcessor, State,
};
use reth_transaction_pool::TransactionPool;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::{debug, trace, warn};

mod client;
mod mode;
mod task;

pub use crate::client::AutoSealClient;
pub use mode::{FixedBlockTimeMiner, MiningMode, ReadyTransactionMiner};
pub use task::MiningTask;

/// A consensus implementation intended for local development and testing purposes.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct AutoSealConsensus {
    /// Configuration
    chain_spec: Arc<ChainSpec>,
}

impl AutoSealConsensus {
    /// Create a new instance of [AutoSealConsensus]
    pub fn new(chain_spec: Arc<ChainSpec>) -> Self {
        Self { chain_spec }
    }
}

impl Consensus for AutoSealConsensus {
    fn validate_header(&self, _header: &SealedHeader) -> Result<(), ConsensusError> {
        Ok(())
    }

    fn validate_header_against_parent(
        &self,
        _header: &SealedHeader,
        _parent: &SealedHeader,
    ) -> Result<(), ConsensusError> {
        Ok(())
    }

    fn validate_header_with_total_difficulty(
        &self,
        _header: &Header,
        _total_difficulty: U256,
    ) -> Result<(), ConsensusError> {
        Ok(())
    }

    fn validate_block(&self, _block: &SealedBlock) -> Result<(), ConsensusError> {
        Ok(())
    }
}

/// Builder type for configuring the setup
#[derive(Debug)]
pub struct BatchMakerBuilder<Client, Pool> {
    client: Client,
    consensus: AutoSealConsensus,
    pool: Pool,
    mode: MiningMode,
    storage: Storage,
    to_worker: Sender<NewBatch>,
}

// === impl AutoSealBuilder ===

impl<Client, Pool: TransactionPool> BatchMakerBuilder<Client, Pool>
where
    Client: BlockReaderIdExt,
{
    /// Creates a new builder instance to configure all parts.
    pub fn new(
        chain_spec: Arc<ChainSpec>,
        client: Client,
        pool: Pool,
        to_worker: Sender<NewBatch>,
        mode: MiningMode,
    ) -> Self {
        let latest_header = client
            .latest_header()
            .ok()
            .flatten()
            .unwrap_or_else(|| chain_spec.sealed_genesis_header());

        Self {
            storage: Storage::new(latest_header),
            client,
            consensus: AutoSealConsensus::new(chain_spec),
            pool,
            mode,
            to_worker,
        }
    }

    /// Sets the [MiningMode] it operates in, default is [MiningMode::Auto]
    pub fn mode(mut self, mode: MiningMode) -> Self {
        self.mode = mode;
        self
    }

    /// Consumes the type and returns all components
    #[track_caller]
    pub fn build(self) -> (AutoSealConsensus, AutoSealClient, MiningTask<Client, Pool>) {
        let Self { client, consensus, pool, mode, storage, to_worker } = self;
        let auto_client = AutoSealClient::new(storage.clone());
        let task = MiningTask::new(
            Arc::clone(&consensus.chain_spec),
            mode,
            to_worker,
            storage,
            client,
            pool,
        );
        (consensus, auto_client, task)
    }
}

/// In memory storage
#[derive(Debug, Clone, Default)]
pub(crate) struct Storage {
    inner: Arc<RwLock<StorageInner>>,
}

// == impl Storage ===

impl Storage {
    fn new(header: SealedHeader) -> Self {
        let (header, best_hash) = header.split();
        let mut storage = StorageInner {
            best_hash,
            total_difficulty: header.difficulty,
            best_block: header.number,
            ..Default::default()
        };
        storage.headers.insert(0, header);
        storage.bodies.insert(best_hash, BlockBody::default());
        Self { inner: Arc::new(RwLock::new(storage)) }
    }

    /// Returns the write lock of the storage
    pub(crate) async fn write(&self) -> RwLockWriteGuard<'_, StorageInner> {
        self.inner.write().await
    }

    /// Returns the read lock of the storage
    pub(crate) async fn read(&self) -> RwLockReadGuard<'_, StorageInner> {
        self.inner.read().await
    }
}

/// In-memory storage for the chain the auto seal engine is building.
#[derive(Default, Debug)]
pub(crate) struct StorageInner {
    /// Headers buffered for download.
    pub(crate) headers: HashMap<BlockNumber, Header>,
    /// A mapping between block hash and number.
    pub(crate) hash_to_number: HashMap<BlockHash, BlockNumber>,
    /// Bodies buffered for download.
    pub(crate) bodies: HashMap<BlockHash, BlockBody>,
    /// Tracks best block
    pub(crate) best_block: u64,
    /// Tracks hash of best block
    pub(crate) best_hash: B256,
    /// The total difficulty of the chain until this block
    pub(crate) total_difficulty: U256,
}

// === impl StorageInner ===

impl StorageInner {
    /// Returns the block hash for the given block number if it exists.
    pub(crate) fn block_hash(&self, num: u64) -> Option<BlockHash> {
        self.hash_to_number.iter().find_map(|(k, v)| num.eq(v).then_some(*k))
    }

    /// Returns the matching header if it exists.
    pub(crate) fn header_by_hash_or_number(
        &self,
        hash_or_num: BlockHashOrNumber,
    ) -> Option<Header> {
        let num = match hash_or_num {
            BlockHashOrNumber::Hash(hash) => self.hash_to_number.get(&hash).copied()?,
            BlockHashOrNumber::Number(num) => num,
        };
        self.headers.get(&num).cloned()
    }

    /// Inserts a new header+body pair
    pub(crate) fn insert_new_block(&mut self, mut header: Header, body: BlockBody) {
        header.number = self.best_block + 1;
        header.parent_hash = self.best_hash;

        self.best_hash = header.hash_slow();
        self.best_block = header.number;
        self.total_difficulty += header.difficulty;

        trace!(target: "execution::batch_maker", num=self.best_block, hash=?self.best_hash, "inserting new block");
        self.headers.insert(header.number, header);
        self.bodies.insert(self.best_hash, body);
        self.hash_to_number.insert(self.best_hash, self.best_block);
        tracing::debug!(target: "execution::batch_maker", storage_size=?self.bodies.len());
    }

    /// Fills in pre-execution header fields based on the current best block and given
    /// transactions.
    pub(crate) fn build_header_template(
        &self,
        transactions: &Vec<TransactionSigned>,
        chain_spec: Arc<ChainSpec>,
        parent: SealedHeader,
    ) -> Header {
        // // check previous block for base fee
        // let base_fee_per_gas = self
        //     .headers
        //     .get(&self.best_block)
        //     .and_then(|parent| parent.next_block_base_fee(chain_spec.base_fee_params));

        // use finalized parent for this batch base fee
        let base_fee_per_gas = parent.next_block_base_fee(chain_spec.base_fee_params(now()));

        let mut header = Header {
            parent_hash: parent.hash,
            ommers_hash: EMPTY_OMMER_ROOT_HASH,
            beneficiary: Default::default(),
            state_root: Default::default(),
            transactions_root: Default::default(),
            receipts_root: Default::default(),
            withdrawals_root: None,
            logs_bloom: Default::default(),
            difficulty: U256::ZERO,
            number: parent.number + 1,
            gas_limit: ETHEREUM_BLOCK_GAS_LIMIT,
            gas_used: 0,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
            mix_hash: Default::default(),
            nonce: 0,
            base_fee_per_gas,
            blob_gas_used: None,
            excess_blob_gas: None,
            extra_data: Default::default(),
            parent_beacon_block_root: None,
        };

        header.transactions_root = if transactions.is_empty() {
            EMPTY_TRANSACTIONS
        } else {
            proofs::calculate_transaction_root(transactions)
        };

        header
    }

    /// Executes the block with the given block and senders, on the provided [EVMProcessor].
    ///
    /// This returns the poststate from execution and post-block changes, as well as the gas used.
    pub(crate) fn execute(
        &mut self,
        block: &Block,
        executor: &mut EVMProcessor<'_>,
        senders: Vec<Address>,
    ) -> Result<(BundleStateWithReceipts, u64), BlockExecutionError> {
        trace!(target: "execution::batch_maker", transactions=?&block.body, "executing transactions");
        // TODO: there isn't really a parent beacon block root here, so not sure whether or not to
        // call the 4788 beacon contract

        // set the first block to find the correct index in bundle state
        executor.set_first_block(block.number);

        let (receipts, gas_used) =
            executor.execute_transactions(block, U256::ZERO, Some(senders))?;

        // Save receipts.
        executor.save_receipts(receipts)?;

        // add post execution state change
        // Withdrawals, rewards etc.
        executor.apply_post_execution_state_change(block, U256::ZERO)?;

        // merge transitions
        executor.db_mut().merge_transitions(BundleRetention::Reverts);

        // apply post block changes
        Ok((executor.take_output_state(), gas_used))
    }

    /// Fills in the post-execution header fields based on the given BundleState and gas used.
    /// In doing this, the state root is calculated and the final header is returned.
    pub(crate) fn complete_header<S: StateProviderFactory>(
        &self,
        mut header: Header,
        bundle_state: &BundleStateWithReceipts,
        client: &S,
        gas_used: u64,
    ) -> Result<Header, BlockExecutionError> {
        let receipts = bundle_state.receipts_by_block(header.number);
        header.receipts_root = if receipts.is_empty() {
            EMPTY_RECEIPTS
        } else {
            let receipts_with_bloom = receipts
                .iter()
                .map(|r| (*r).clone().expect("receipts have not been pruned").into())
                .collect::<Vec<ReceiptWithBloom>>();
            header.logs_bloom =
                receipts_with_bloom.iter().fold(Bloom::ZERO, |bloom, r| bloom | r.bloom);
            proofs::calculate_receipt_root(&receipts_with_bloom)
        };

        header.gas_used = gas_used;

        // calculate the state root
        let state_root = client
            .latest()
            .map_err(|_| BlockExecutionError::ProviderError)?
            .state_root(bundle_state)
            .unwrap();
        header.state_root = state_root;
        Ok(header)
    }

    /// Builds and executes a new block with the given transactions, on the provided [EVMProcessor].
    ///
    /// This returns the header of the executed block, as well as the poststate from execution.
    pub(crate) fn build_and_execute(
        &mut self,
        transactions: Vec<TransactionSigned>,
        client: &(impl StateProviderFactory + BlockReaderIdExt),
        chain_spec: Arc<ChainSpec>,
    ) -> Result<(SealedHeader, BundleStateWithReceipts), BlockExecutionError> {
        // use the last canonical block for next batch
        debug!(target: "execution::batch_maker", latest=?client.latest_header().unwrap());
        // TODO: ensure forkchoice updated is sent to engine as components start up
        //
        // TODO: use error types here
        let parent = client.finalized_header().unwrap();
        debug!(target: "execution::batch_maker", finalized=?parent);
        let parent = client.latest_header().unwrap().unwrap();
        let header = self.build_header_template(&transactions, chain_spec.clone(), parent);

        let block = Block { header, body: transactions, ommers: vec![], withdrawals: None };

        let senders = TransactionSigned::recover_signers(&block.body, block.body.len())
            .ok_or(BlockExecutionError::Validation(BlockValidationError::SenderRecoveryError))?;

        trace!(target: "execution::batch_maker", transactions=?&block.body, "executing transactions");

        // now execute the block
        let db = State::builder()
            .with_database_boxed(Box::new(StateProviderDatabase::new(client.latest().unwrap())))
            .with_bundle_update()
            .build();
        let mut executor = EVMProcessor::new_with_state(chain_spec, db);

        let (bundle_state, gas_used) = self.execute(&block, &mut executor, senders)?;

        let Block { header, body, .. } = block;
        let body = BlockBody { transactions: body, ommers: vec![], withdrawals: None };

        trace!(target: "execution::batch_maker", ?bundle_state, ?header, ?body, "executed block, calculating state root and completing header");

        // fill in the rest of the fields
        let header = self.complete_header(header, &bundle_state, client, gas_used)?;

        trace!(target: "execution::batch_maker", root=?header.state_root, ?body, "calculated root");

        // finally insert into storage
        self.insert_new_block(header.clone(), body);

        // set new header with hash that should have been updated by insert_new_block
        let new_header = header.seal(self.best_hash);

        Ok((new_header, bundle_state))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use fastcrypto::hash::Hash;
    use narwhal_types::{
        test_utils::{get_gas_price, TransactionFactory},
        yukon_genesis, BatchAPI, MetadataAPI,
    };
    use reth::{init::init_genesis, tasks::TokioTaskExecutor};
    use reth_blockchain_tree::noop::NoopBlockchainTree;
    use reth_db::test_utils::create_test_rw_db;
    use reth_interfaces::p2p::{
        headers::client::{HeadersClient, HeadersRequest},
        priority::Priority,
    };
    use reth_primitives::{GenesisAccount, HeadersDirection};
    use reth_provider::{providers::BlockchainProvider, ProviderFactory};
    use reth_tracing::init_test_tracing;
    use reth_transaction_pool::{
        blobstore::InMemoryBlobStore, PoolConfig, TransactionValidationTaskExecutor,
    };
    use std::{str::FromStr, time::Duration};
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_make_batch() {
        init_test_tracing();
        let genesis = yukon_genesis();
        let mut tx_factory = TransactionFactory::new();
        let factory_address = tx_factory.address();
        debug!("seeding factory address: {factory_address:?}");

        // fund factory with 99mil TEL
        let account = vec![(
            factory_address,
            GenesisAccount::default().with_balance(
                U256::from_str("0x51E410C0F93FE543000000").expect("account balance is parsed"),
            ),
        )];

        let genesis = genesis.extend_accounts(account);
        debug!("seeded genesis: {genesis:?}");
        let head_timestamp = genesis.timestamp;
        let chain: Arc<ChainSpec> = Arc::new(genesis.into());

        // init genesis
        let db = create_test_rw_db();
        let genesis_hash = init_genesis(db.clone(), chain.clone()).expect("init genesis");

        debug!("genesis hash: {genesis_hash:?}");

        // provider
        let factory = ProviderFactory::new(Arc::clone(&db), Arc::clone(&chain));
        let blockchain_db = BlockchainProvider::new(factory, NoopBlockchainTree::default())
            .expect("test blockchain provider");

        let task_executor = TokioTaskExecutor::default();

        // txpool
        let blob_store = InMemoryBlobStore::default();
        let validator = TransactionValidationTaskExecutor::eth_builder(Arc::clone(&chain))
            .with_head_timestamp(head_timestamp)
            .with_additional_tasks(1)
            .build_with_tasks(blockchain_db.clone(), task_executor, blob_store.clone());

        let txpool =
            reth_transaction_pool::Pool::eth_pool(validator, blob_store, PoolConfig::default());
        let max_transactions = 1;
        let mining_mode =
            MiningMode::instant(max_transactions, txpool.pending_transactions_listener());

        // worker channel
        let (to_worker, mut worker_rx) = narwhal_types::test_channel!(1);

        // build batch maker
        let (_, client, task) = BatchMakerBuilder::new(
            Arc::clone(&chain),
            blockchain_db.clone(),
            txpool.clone(),
            to_worker,
            mining_mode,
        )
        .build();

        let gas_price = get_gas_price(blockchain_db.clone());
        debug!("gas price: {gas_price:?}");
        let value =
            U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256").into();

        // create 3 transactions
        let transaction1 = tx_factory.create_eip1559(
            chain.clone(),
            gas_price,
            Address::ZERO,
            value, // 1 TEL
        );
        debug!("transaction 1: {transaction1:?}");

        let transaction2 = tx_factory.create_eip1559(
            chain.clone(),
            gas_price,
            Address::ZERO,
            value, // 1 TEL
        );
        debug!("transaction 2: {transaction2:?}");

        let transaction3 = tx_factory.create_eip1559(
            chain.clone(),
            gas_price,
            Address::ZERO,
            value, // 1 TEL
        );
        debug!("transaction 3: {transaction3:?}");

        let added_result = tx_factory.submit_tx_to_pool(transaction1.clone(), txpool.clone()).await;
        assert_matches!(added_result, hash if hash == transaction1.hash());

        let added_result = tx_factory.submit_tx_to_pool(transaction2.clone(), txpool.clone()).await;
        assert_matches!(added_result, hash if hash == transaction2.hash());

        let added_result = tx_factory.submit_tx_to_pool(transaction3.clone(), txpool.clone()).await;
        assert_matches!(added_result, hash if hash == transaction3.hash());

        // txpool size
        let pending_pool_len = txpool.pool_size().pending;
        debug!("pool_size(): {:?}", txpool.pool_size());
        assert_eq!(pending_pool_len, 3);

        // spawn mining task
        let _mining_task = tokio::spawn(Box::pin(task));

        // wait for new batch
        let too_long = Duration::from_secs(5);
        let new_batch = timeout(too_long, worker_rx.recv())
            .await
            .expect("new batch created within time")
            .expect("new batch is Some()");

        debug!("new batch: {new_batch:?}");
        // number of transactions in the batch
        let batch_txs = new_batch.batch.transactions();

        // check max tx for task matches num of transactions in batch
        let num_batch_txs = batch_txs.len();
        assert_eq!(max_transactions, num_batch_txs);

        // ensure decoded batch transaction is transaction1
        let batch_tx_bytes = batch_txs.first().cloned().expect("one tx in batch");
        let decoded_batch_tx = TransactionSigned::decode_enveloped(&mut batch_tx_bytes.as_ref())
            .expect("tx bytes are uncorrupted");
        assert_eq!(decoded_batch_tx, transaction1);

        // send the worker's ack to task
        let digest = new_batch.batch.digest();
        let _ack = new_batch.ack.send(digest);

        // retrieve block number 1 from storage
        //
        // at this point, we know the task has completed because
        // the task's Storage write lock must be dropped for the
        // read lock to be available here
        let storage_header = client
            .get_headers_with_priority(
                HeadersRequest { start: 1.into(), limit: 1, direction: HeadersDirection::Rising },
                Priority::Normal,
            )
            .await
            .expect("header is available from storage")
            .into_data()
            .first()
            .expect("header included")
            .to_owned();

        debug!("awaited first reply from storage header");

        let storage_sealed_header = storage_header.seal_slow();

        debug!("storage sealed header: {storage_sealed_header:?}");

        // TODO: this isn't the right thing to test bc storage should be removed
        //
        assert_eq!(new_batch.batch.versioned_metadata().sealed_header(), &storage_sealed_header,);

        // txpool size after mining
        let pending_pool_len = txpool.pool_size().pending;
        debug!("pool_size(): {:?}", txpool.pool_size());
        assert_eq!(pending_pool_len, 2);

        // ensure tx1 is removed
        assert!(!txpool.contains(transaction1.hash_ref()));
        // ensure tx2 & tx3 are in the pool still
        assert!(txpool.contains(transaction2.hash_ref()));
        assert!(txpool.contains(transaction3.hash_ref()));
    }
}
