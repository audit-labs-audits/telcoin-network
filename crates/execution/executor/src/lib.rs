//! A block builder implementation for executing the output of consenus.
//!
//! The Mining task polls a [MiningMode], and will return all transactions to include in the next
//! payload.
//!
//! These downloaders poll the miner, assemble the block, and return transactions that are ready to
//! be mined.

#![doc(
    html_logo_url = "https://www.telco.in/logos/TEL.svg",
    html_favicon_url = "https://www.telco.in/logos/TEL.svg",
    issue_tracker_base_url = "https://github.com/telcoin-association/telcoin-network/issues/"
)]
#![warn(missing_debug_implementations, missing_docs, unreachable_pub, rustdoc::all)]
#![deny(unused_must_use, rust_2018_idioms)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

use consensus_metrics::metered_channel::Receiver;
use reth_beacon_consensus::BeaconEngineMessage;
use reth_interfaces::{
    consensus::{Consensus, ConsensusError},
    executor::{BlockExecutionError, BlockValidationError},
};
use reth_primitives::{
    constants::{EMPTY_RECEIPTS, EMPTY_TRANSACTIONS, ETHEREUM_BLOCK_GAS_LIMIT},
    proofs, Address, Block, BlockBody, BlockHash, BlockHashOrNumber, BlockNumber, Bloom, ChainSpec,
    Header, ReceiptWithBloom, SealedBlock, SealedBlockWithSenders, SealedHeader, TransactionSigned,
    B256, EMPTY_OMMER_ROOT_HASH, U256,
};
use reth_provider::{
    BlockExecutor, BlockReaderIdExt, BundleStateWithReceipts, CanonStateNotificationSender,
    StateProviderFactory,
};
use reth_revm::{
    database::StateProviderDatabase, db::states::bundle_state::BundleRetention,
    processor::EVMProcessor, State,
};
use std::{collections::HashMap, sync::Arc};
use tn_types::{now, BatchAPI, ConsensusOutput};
use tokio::sync::{mpsc::UnboundedSender, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::{debug, trace, warn};

mod client;
// mod mode;
mod builder;
mod error;
mod task;
pub use builder::*;

pub use crate::client::AutoSealClient;
use error::ExecutorError;
// pub use mode::{FixedBlockTimeMiner, MiningMode, ReadyTransactionMiner};
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
pub struct Executor<Client> {
    client: Client,
    consensus: AutoSealConsensus,
    storage: Storage,
    from_consensus: Receiver<ConsensusOutput>,
    to_engine: UnboundedSender<BeaconEngineMessage>,
    canon_state_notification: CanonStateNotificationSender,
}

// === impl AutoSealBuilder ===

impl<Client> Executor<Client>
where
    Client: BlockReaderIdExt,
{
    /// Creates a new builder instance to configure all parts.
    pub fn new(
        chain_spec: Arc<ChainSpec>,
        client: Client,
        from_consensus: Receiver<ConsensusOutput>,
        to_engine: UnboundedSender<BeaconEngineMessage>,
        canon_state_notification: CanonStateNotificationSender,
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
            from_consensus,
            to_engine,
            canon_state_notification,
        }
    }

    /// Consumes the type and returns all components
    #[track_caller]
    pub fn build(self) -> (AutoSealConsensus, AutoSealClient, MiningTask<Client>) {
        let Self {
            client,
            consensus,
            storage,
            from_consensus,
            to_engine,
            canon_state_notification,
        } = self;
        let auto_client = AutoSealClient::new(storage.clone());
        let task = MiningTask::new(
            Arc::clone(&consensus.chain_spec),
            to_engine,
            canon_state_notification,
            storage,
            client,
            from_consensus,
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

        trace!(target: "execution::executor", num=self.best_block, hash=?self.best_hash, "inserting new block");
        self.headers.insert(header.number, header);
        self.bodies.insert(self.best_hash, body);
        self.hash_to_number.insert(self.best_hash, self.best_block);
        tracing::debug!(target: "execution::executor", storage_size=?self.bodies.len());
    }

    /// Fills in pre-execution header fields based on the current best block and given
    /// transactions.
    pub(crate) fn build_header_template(
        &self,
        transactions: &Vec<TransactionSigned>,
        chain_spec: Arc<ChainSpec>,
        parent: SealedHeader,
        timestamp: u64,
        beneficiary: Address,
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
            beneficiary,
            state_root: Default::default(),
            transactions_root: Default::default(),
            receipts_root: Default::default(),
            withdrawals_root: None,
            logs_bloom: Default::default(),
            difficulty: U256::ZERO,
            number: parent.number + 1,
            gas_limit: ETHEREUM_BLOCK_GAS_LIMIT,
            gas_used: 0,
            timestamp,
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
    ) -> Result<(BundleStateWithReceipts, u64), ExecutorError> {
        trace!(target: "execution::executor", transactions=?&block.body, "executing transactions");
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
    ) -> Result<Header, ExecutorError> {
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
        // transactions: Vec<TransactionSigned>,
        output: ConsensusOutput,
        client: &(impl StateProviderFactory + BlockReaderIdExt),
        chain_spec: Arc<ChainSpec>,
        // ) -> Result<(SealedHeader, BundleStateWithReceipts, Vec<TransactionSigned>,
        // Vec<Address>), BlockExecutionError> {
    ) -> Result<(SealedBlockWithSenders, BundleStateWithReceipts), ExecutorError> {
        // use the last canonical block for next batch
        debug!(target: "execution::executor", latest=?client.latest_header().unwrap());

        // capture timestamp and beneficiary before consuming output
        let timestamp = output.committed_at();
        let beneficiary = output.beneficiary();

        // try to recover transactions and signers
        let transactions = self.try_recover_transactions(output)?;

        // TODO: ensure forkchoice updated is sent to engine as components start up
        //
        // TODO: use error types here
        let parent = client.finalized_header().unwrap();
        debug!(target: "execution::executor", finalized=?parent);
        let parent = client.latest_header().unwrap().unwrap();
        let header = self.build_header_template(
            &transactions,
            chain_spec.clone(),
            parent,
            timestamp,
            beneficiary,
        );

        let block = Block {
            header: header.clone(),
            body: transactions.clone(),
            ommers: vec![],
            withdrawals: None,
        };

        let senders = TransactionSigned::recover_signers(&block.body, block.body.len())
            .ok_or(BlockExecutionError::Validation(BlockValidationError::SenderRecoveryError))?;

        trace!(target: "execution::executor", transactions=?&block.body, "executing transactions");

        // now execute the block
        let db = State::builder()
            .with_database_boxed(Box::new(StateProviderDatabase::new(client.latest().unwrap())))
            .with_bundle_update()
            .build();
        let mut executor = EVMProcessor::new_with_state(chain_spec, db);

        let (bundle_state, gas_used) = self.execute(&block, &mut executor, senders.clone())?;

        // let Block { header, body, .. } = block;
        // let body = block.body.clone();
        let body = BlockBody { transactions, ommers: vec![], withdrawals: None };

        trace!(target: "execution::executor", ?bundle_state, ?header, ?body, "executed block, calculating state root and completing header");

        // fill in the rest of the fields
        let header = self.complete_header(header, &bundle_state, client, gas_used)?;

        trace!(target: "execution::executor", root=?header.state_root, ?body, "calculated root");

        // finally insert into storage
        self.insert_new_block(header.clone(), body);

        // TODO: am I okay with this approach?
        // prevents re-hashing the header, but if removing Storage, then `insert_new_block` won't
        // have the hash anymore
        //
        // set new header with hash that should have been updated by insert_new_block
        let sealed_block = block.seal(self.best_hash);

        // let sealed_block = block.seal_slow();

        debug!(target: "execution::executor", ?sealed_block, ?senders, "attempting to seal block with senders");
        let sealed_block_with_senders = SealedBlockWithSenders::new(sealed_block, senders)
            .ok_or(ExecutorError::UnevenSendersForSealedBlock)?;

        Ok((sealed_block_with_senders, bundle_state))
    }

    /// Try to recover the transactions and senders from the transaction bytes.
    ///
    /// Batches are validated by peers at this point, so decoding and recovering signers should not
    /// error.
    ///
    /// TODO: optimize the `for` loops
    fn try_recover_transactions(
        &self,
        output: ConsensusOutput,
    ) -> Result<Vec<TransactionSigned>, ExecutorError> {
        // collect recovered txs and senders
        let mut transactions = vec![];
        // let mut senders = vec![];

        // loop through all transactions in order
        for batches in output.batches.into_iter() {
            for batch in batches.iter() {
                for tx in batch.transactions_owned() {
                    // batches must be validated by this point,
                    // so encoding and decoding has already happened
                    // and is not expected to fail
                    let recovered =
                        TransactionSigned::decode_enveloped(&mut tx.as_ref()).map_err(|e| {
                            // error!(target: "execution::executor", "Failed to decode enveloped tx:
                            // {tx:?}");
                            ExecutorError::DecodeTransaction(e)
                        })?;
                    // let sender =
                    // recovered.recover_signer().
                    // ok_or(BlockExecutionError::Validation(BlockValidationError::SenderRecoveryError))?
                    // ;
                    transactions.push(recovered);
                    // senders.push(sender);
                }
            }
        }
        Ok(transactions)

        // TODO: optimize by returning `impl Iterator<Item = Vec<Bytes>` and using `flat_map` to
        // return an iter of nested vecs since this is the only place where
        // `.transactions_owned` method is used. this would save memory allocation discussion: https://users.rust-lang.org/t/how-far-to-take-iterators-to-avoid-for-loops/30167/10
        //
        // output.batches.into_iter().flat_map(|batches| batches.into_iter().flat_map(|batch|
        // batch.transactions_owned().into_iter().flat_map(|tx|
        // TransactionSigned::decode_enveloped(tx.into())))).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use reth::{cli::components::RethNodeComponentsImpl, init::init_genesis};
    use reth_beacon_consensus::{
        hooks::EngineHooks, BeaconConsensusEngine, MIN_BLOCKS_FOR_PIPELINE_RUN,
    };
    use reth_blockchain_tree::{
        BlockchainTree, BlockchainTreeConfig, ShareableBlockchainTree, TreeExternals,
    };
    use reth_config::Config;
    use reth_db::test_utils::create_test_rw_db;
    use reth_interfaces::p2p::{
        headers::client::{HeadersClient, HeadersRequest},
        priority::Priority,
    };
    use reth_primitives::{GenesisAccount, Head, HeadersDirection};
    use reth_provider::{
        providers::BlockchainProvider, CanonStateNotification, CanonStateSubscriptions,
        ProviderFactory,
    };
    use reth_revm::EvmProcessorFactory;
    use reth_rpc_types::engine::ForkchoiceState;
    use reth_tasks::TaskManager;
    use reth_tracing::init_test_tracing;
    use reth_transaction_pool::noop::NoopTransactionPool;
    use std::{str::FromStr, time::Duration};
    use tn_types::{
        adiri_genesis, execution_args, BatchAPI, Certificate, CommittedSubDag, ReputationScores,
    };
    use tokio::{
        runtime::Handle,
        sync::{mpsc::unbounded_channel, oneshot},
        time::timeout,
    };
    use tracing::info;

    /// Unit test at this time is very complicated.
    ///
    /// Transactions need to be valid, but the helper functions to create certificates
    /// are not
    #[tokio::test]
    async fn test_execute_consensus_output() {
        init_test_tracing();

        //=== Consensus
        //
        // create consensus output bc transactions in batches
        // are randomly generated
        //
        // for each tx, seed address with funds in genesis
        //
        // TODO: this does not use a "real" `ConsensusOutput`
        //
        // refactor with valid data once test util helpers are in place
        let leader = Certificate::default();
        let sub_dag_index = 1;
        let reputation_scores = ReputationScores::default();
        let previous_sub_dag = None;
        let batches = tn_types::test_utils::batches(4); // create 4 batches
        let beneficiary = Address::from_str("0xdbdbdb2cbd23b783741e8d7fcf51e459b497e4a6")
            .expect("beneficiary address from str");
        let consensus_output = ConsensusOutput {
            sub_dag: CommittedSubDag::new(
                vec![Certificate::default()],
                leader,
                sub_dag_index,
                reputation_scores,
                previous_sub_dag,
            )
            .into(),
            batches: vec![batches.clone()],
            beneficiary,
        };

        //=== Execution

        let genesis = adiri_genesis();
        let args = execution_args();

        // collect txs and addresses for later assertions
        let mut txs_in_output = vec![];
        let mut senders_in_output = vec![];

        let mut accounts_to_seed = Vec::new();
        for batch in batches.into_iter() {
            for tx in batch.transactions_owned() {
                let tx_signed = TransactionSigned::decode_enveloped(&mut tx.as_ref())
                    .expect("decode tx signed");
                let address = tx_signed.recover_signer().expect("signer recoverable");
                txs_in_output.push(tx_signed);
                senders_in_output.push(address);
                // fund account with 99mil TEL
                let account = (
                    address,
                    GenesisAccount::default().with_balance(
                        U256::from_str("0x51E410C0F93FE543000000")
                            .expect("account balance is parsed"),
                    ),
                );
                accounts_to_seed.push(account);
            }
        }
        debug!("accounts to seed: {accounts_to_seed:?}");

        // genesis
        let genesis = genesis.extend_accounts(accounts_to_seed);
        let chain: Arc<ChainSpec> = Arc::new(genesis.into());
        let db = create_test_rw_db();
        let genesis_hash = init_genesis(db.clone(), chain.clone()).expect("init genesis");

        debug!("genesis hash: {genesis_hash:?}");

        let consensus: Arc<dyn Consensus> = Arc::new(AutoSealConsensus::new(Arc::clone(&chain)));

        let provider_factory = ProviderFactory::new(Arc::clone(&db), Arc::clone(&chain));

        // configure blockchain tree
        let tree_externals = TreeExternals::new(
            provider_factory.clone(),
            Arc::clone(&consensus),
            EvmProcessorFactory::new(chain.clone()),
        );

        // TODO: add prune config for full node
        let tree = BlockchainTree::new(
            tree_externals,
            BlockchainTreeConfig::default(), // default is more than enough
            None,                            // TODO: prune config
        )
        .expect("blockchain tree is valid");

        let canon_state_notification_sender = tree.canon_state_notification_sender();
        let blockchain_tree = ShareableBlockchainTree::new(tree);

        // provider
        let blockchain_db = BlockchainProvider::new(provider_factory.clone(), blockchain_tree)
            .expect("blockchain provider is valid");

        // skip txpool

        // task executor
        let manager = TaskManager::new(Handle::current());
        let task_executor = manager.executor();

        let head: Head = lookup_head(provider_factory.clone()).expect("lookup head successful");

        // network
        let network = build_network(
            chain.clone(),
            task_executor.clone(),
            provider_factory.clone(),
            head,
            NoopTransactionPool::default(),
            &args.network,
        )
        .await
        .expect("build network successful with no peers");

        // engine channel
        let (to_executor, from_consensus) = tn_types::test_channel!(1);
        let (to_engine, from_engine) = unbounded_channel();
        let mut canon_state_notification_receiver = blockchain_db.subscribe_to_canonical_state();

        // note on canon state stream:
        // SENDERS:
        //  - BlockchainTree::make_canonical() sends broadcast
        //      - engine calls this method
        //
        // RECEIVERS:
        //  - rpc (stream)
        //  - txpool maintenance task
        //      - stream events inside loop calling `.next()`
        //  - event handler
        //
        // Provider's CanonChainTracker:
        //  - rpc uses this to get chain info

        // build batch maker
        let (_, client, mut task) = Executor::new(
            Arc::clone(&chain),
            blockchain_db.clone(),
            from_consensus,
            to_engine.clone(),
            canon_state_notification_sender,
        )
        .build();

        let config = Config::default();
        let (metrics_tx, _sync_metrics_rx) = unbounded_channel();

        let mut pipeline = build_networked_pipeline(
            &config,
            client.clone(),
            consensus,
            provider_factory.clone(),
            &task_executor,
            metrics_tx,
            chain,
        )
        .await
        .expect("networked pipeline build was successful");

        // TODO: is this necessary?
        let pipeline_events = pipeline.events();
        task.set_pipeline_events(pipeline_events);

        // spawn task to execute consensus output
        task_executor.spawn(Box::pin(task));

        // capture pipeline events one more time for events stream
        // before passing pipeline to beacon engine
        // TODO: incompatible with temp db
        // let pipeline_events = pipeline.events();

        // spawn engine
        let hooks = EngineHooks::new();
        let components = RethNodeComponentsImpl {
            provider: blockchain_db.clone(),
            pool: NoopTransactionPool::default(),
            network: network.clone(),
            task_executor: task_executor.clone(),
            events: blockchain_db.clone(),
        };
        let payload_builder = spawn_payload_builder_service(components, &args.builder)
            .expect("payload builder service");
        let (beacon_consensus_engine, beacon_engine_handle) = BeaconConsensusEngine::with_channel(
            client.clone(),
            pipeline,
            blockchain_db.clone(),
            Box::new(task_executor.clone()),
            Box::new(network.clone()),
            None,  // max block
            false, // self.debug.continuous,
            payload_builder.clone(),
            None, // initial_target
            MIN_BLOCKS_FOR_PIPELINE_RUN,
            to_engine,
            from_engine,
            hooks,
        )
        .expect("beacon consensus engine spawned");

        // TODO: events handler doesn't work with temp db
        //
        // stream events
        // let events = stream_select!(
        //     network.event_listener().map(Into::into),
        //     beacon_engine_handle.event_listener().map(Into::into),
        //     pipeline_events.map(Into::into),
        //     ConsensusLayerHealthEvents::new(Box::new(blockchain_db.clone())).map(Into::into),
        // );
        // monitor and print events
        // task_executor.spawn_critical(
        //     "events task",
        //     events::handle_events(Some(network.clone()), Some(head.number), events, db.clone()),
        // );

        // Run consensus engine to completion
        let (tx, _rx) = oneshot::channel();
        info!(target: "reth::cli", "Starting consensus engine");
        task_executor.spawn_critical_blocking("consensus engine", async move {
            let res = beacon_consensus_engine.await;
            let _ = tx.send(res);
        });

        // finalize genesis
        let genesis_state = ForkchoiceState {
            head_block_hash: genesis_hash,
            finalized_block_hash: genesis_hash,
            safe_block_hash: genesis_hash,
        };

        let _res = beacon_engine_handle
            .fork_choice_updated(genesis_state, None)
            .await
            .expect("fork choice updated to finalize genesis");
        tracing::debug!("genesis finalized :D");

        //=== Testing begins

        // send output to executor
        let res = to_executor.send(consensus_output).await;
        assert!(res.is_ok());

        // wait for next canonical block
        let too_long = Duration::from_secs(5);
        let canon_update = timeout(too_long, canon_state_notification_receiver.recv())
            .await
            .expect("next canonical block created within time")
            .expect("canon update is Some()");

        assert_matches!(canon_update, CanonStateNotification::Commit { .. });
        let canonical_tip = canon_update.tip();
        debug!("canon update: {:?}", canonical_tip);
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

        // ensure canonical tip is the next block
        assert_eq!(canonical_tip.header, storage_sealed_header);

        // ensure database and provider are updated
        let canonical_hash = canonical_tip.hash();

        // wait for forkchoice to finish updating
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let mut current_finalized_header = blockchain_db
                .finalized_header()
                .expect("blockchain db has some finalized header 1")
                .expect("some finalized header 2");
            while canonical_hash != current_finalized_header.hash() {
                // sleep - then look up finalized in db again
                println!("\nwaiting for engine to complete forkchoice update...\n");
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                current_finalized_header = blockchain_db
                    .finalized_header()
                    .expect("blockchain db has some finalized header 1")
                    .expect("some finalized header 2");
            }
            tx.send(current_finalized_header)
        });

        let current_finalized_header = timeout(too_long, rx)
            .await
            .expect("next canonical block created within time")
            .expect("finalized block retrieved from db");

        debug!("update completed...");
        assert_eq!(canonical_hash, current_finalized_header.hash());

        // assert canonical tip contains all txs and senders in batches
        assert_eq!(canonical_tip.block.body, txs_in_output);
        assert_eq!(canonical_tip.block.beneficiary, beneficiary);
        assert_eq!(canonical_tip.senders, senders_in_output);
    }
}
