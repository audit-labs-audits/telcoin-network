//! The lattice engine loop.
//! 
//! The engine polls messages from the handle to validate batches
//! and append canonical blocks from consensus.

use crate::engine::metrics::EngineMetrics;
use execution_db::database::Database;
use execution_interfaces::{
    blockchain_tree::BlockchainTreeEngine,
    consensus::{ForkchoiceState, ConsensusError},
    Error, executor::BlockExecutionError,
};
use execution_provider::{
    BlockReader, BlockSource, CanonChainTracker, ProviderError, StageCheckpointReader, DatabaseProvider, BlockWriter,
};
use futures::{Future, StreamExt};
use lattice_payload_builder::BlockPayload;
use tracing::debug;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tn_types::execution::{
    constants::EPOCH_SLOTS, listener::EventListeners, ChainSpec,
};
use tokio::sync::{
    mpsc,
    mpsc::{UnboundedReceiver, UnboundedSender},
};
use tokio_stream::wrappers::UnboundedReceiverStream;

mod message;
pub use message::LatticeEngineMessage;

mod error;
pub use error::{
    LatticeConsensusEngineError, LatticeEngineResult,
    LatticeOnNewPayloadError,
};

mod event;
pub use event::LatticeConsensusEngineEvent;
mod metrics;

mod handle;
pub use handle::LatticeConsensusEngineHandle;
mod batch;

// /// The maximum number of invalid headers that can be tracked by the engine.
// const MAX_INVALID_HEADERS: u32 = 512u32;

/// The largest gap for which the tree will be used for sync. See docs for `pipeline_run_threshold`
/// for more information.
///
/// This is the default threshold, the distance to the head that the tree will be used for sync.
/// If the distance exceeds this threshold, the pipeline will be used for sync.
pub const MIN_BLOCKS_FOR_PIPELINE_RUN: u64 = EPOCH_SLOTS;

/// The lattice consensus engine is the driver that switches between historical and live sync.
///
/// The lattice consensus engine is itself driven by messages from the Consensus Layer, which are
/// received by Engine API (JSON-RPC).
///
/// The consensus engine is idle until it receives the first
/// [LatticeEngineMessage::ForkchoiceUpdated] message from the CL which would initiate the sync. At
/// first, the consensus engine would run the [Pipeline] until the latest known block hash.
/// Afterward, it would attempt to create/restore the [`BlockchainTreeEngine`] from the blocks
/// that are currently available. In case the restoration is successful, the consensus engine would
/// run in a live sync mode, populating the [`BlockchainTreeEngine`] with new blocks as they arrive
/// via engine API and downloading any missing blocks from the network to fill potential gaps.
///
/// The consensus engine has two data input sources:
///
/// ## New Payload (`engine_newPayloadV{}`)
///
/// The engine receives new payloads from the CL. If the payload is connected to the canonical
/// chain, it will be fully validated added to a chain in the [BlockchainTreeEngine]: `VALID`
///
/// If the payload's chain is disconnected (at least 1 block is missing) then it will be buffered:
/// `SYNCING` ([BlockStatus::Disconnected]).
///
/// ## Forkchoice Update (FCU) (`engine_forkchoiceUpdatedV{}`)
///
/// This contains the latest forkchoice state and the payload attributes. The engine will attempt to
/// make a new canonical chain based on the `head_hash` of the update and trigger payload building
/// if the `payload_attrs` are present and the FCU is `VALID`.
///
/// The `head_hash` forms a chain by walking backwards from the `head_hash` towards the canonical
/// blocks of the chain.
///
/// Making a new canonical chain can result in the following relevant outcomes:
///
/// ### The chain is connected
///
/// All blocks of the `head_hash`'s chain are present in the [BlockchainTreeEngine] and are
/// committed to the canonical chain. This also includes reorgs.
///
/// ### The chain is disconnected
///
/// In this case the [BlockchainTreeEngine] doesn't know how the new chain connects to the existing
/// canonical chain. It could be a simple commit (new blocks extend the current head) or a re-org
/// that requires unwinding the canonical chain.
///
/// This further distinguishes between two variants:
///
/// #### `head_hash`'s block exists
///
/// The `head_hash`'s block was already received/downloaded, but at least one block is missing to
/// form a _connected_ chain. The engine will attempt to download the missing blocks from the
/// network by walking backwards (`parent_hash`), and then try to make the block canonical as soon
/// as the chain becomes connected.
///
/// However, it still can be the case that the chain and the FCU is `INVALID`.
///
/// #### `head_hash` block is missing
///
/// This is similar to the previous case, but the `head_hash`'s block is missing. At which point the
/// engine doesn't know where the new head will point to: new chain could be a re-org or a simple
/// commit. The engine will download the missing head first and then proceed as in the previous
/// case.
///
/// # Panics
///
/// If the future is polled more than once. Leads to undefined state.
#[must_use = "Future does nothing unless polled"]
pub struct LatticeConsensusEngine<DB, BT>
where
    BT: BlockchainTreeEngine + BlockReader + CanonChainTracker + StageCheckpointReader,
    DB: Database,
{
    /// The type we can use to query both the database and the blockchain tree.
    blockchain: BT,
    /// The database, used to commit the canonical chain.
    db: DB,
    /// Chain spec
    chain_spec: Arc<ChainSpec>,
    /// The Engine API message receiver.
    engine_message_rx: UnboundedReceiverStream<LatticeEngineMessage>,
    /// A clone of the handle
    handle: LatticeConsensusEngineHandle,
    /// Listeners for engine events.
    listeners: EventListeners<LatticeConsensusEngineEvent>,

    // /// Tracks the header of invalid payloads that were rejected by the engine because they're
    // /// invalid.
    // invalid_headers: InvalidHeaderCache,

    /// Consensus engine metrics.
    metrics: EngineMetrics,
}

impl<DB, BT> LatticeConsensusEngine<DB, BT>
where
    BT: BlockchainTreeEngine + BlockReader + CanonChainTracker + StageCheckpointReader + 'static,
    DB: Database,
{
    /// Create a new instance of the [LatticeConsensusEngine].
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        blockchain: BT,
        db: DB,
        chain_spec: Arc<ChainSpec>,
    ) -> Result<(Self, LatticeConsensusEngineHandle), execution_interfaces::Error> {
        let (to_engine, rx) = mpsc::unbounded_channel();
        Self::with_channel(
            blockchain,
            db,
            chain_spec,
            to_engine,
            rx,
        )
    }

    /// Create a new instance of the [LatticeConsensusEngine] using the given channel to configure
    /// the [LatticeEngineMessage] communication channel.
    ///
    /// By default the engine is started with idle pipeline.
    /// The pipeline can be launched immediately in one of the following ways descending in
    /// priority:
    /// - Explicit [Option::Some] target block hash provided via a constructor argument.
    /// - The process was previously interrupted amidst the pipeline run. This is checked by
    ///   comparing the checkpoints of the first ([StageId::Headers]) and last ([StageId::Finish])
    ///   stages. In this case, the latest available header in the database is used as the target.
    ///
    /// Propagates any database related error.
    #[allow(clippy::too_many_arguments)]
    pub fn with_channel(
        blockchain: BT,
        db: DB,
        chain_spec: Arc<ChainSpec>,
        to_engine: UnboundedSender<LatticeEngineMessage>,
        rx: UnboundedReceiver<LatticeEngineMessage>,
    ) -> Result<(Self, LatticeConsensusEngineHandle), execution_interfaces::Error> {
        let handle = LatticeConsensusEngineHandle { to_engine };

        let this = Self {
            blockchain,
            db,
            chain_spec,
            engine_message_rx: UnboundedReceiverStream::new(rx),
            handle: handle.clone(),
            listeners: EventListeners::default(),
            // invalid_headers: InvalidHeaderCache::new(MAX_INVALID_HEADERS),
            metrics: EngineMetrics::default(),
        };

        Ok((this, handle))
    }

    /// Returns a new [`LatticeConsensusEngineHandle`] that can be cloned and shared.
    ///
    /// The [`LatticeConsensusEngineHandle`] can be used to interact with this
    /// [`LatticeConsensusEngine`]
    pub fn handle(&self) -> LatticeConsensusEngineHandle {
        self.handle.clone()
    }

    /// Update the canonical chain after consensus is reached for an even round.
    pub fn on_consensus(
        &mut self,
        payload: Arc<BlockPayload>,
    ) -> Result<(), execution_interfaces::Error> {
        // TODO: update metrics
        
        // TODO: default ForkchoiceState works here bc the method only updates
        // a timestamp, and the arg isn't used.
        self.blockchain.on_forkchoice_update_received(&ForkchoiceState::default());

        self.commit_canonical_payload(payload.clone())?;

        // update local tracked canon chain for next round
        self.update_canon_chain_trackers(payload)
    }

    /// Update canonical chain. Called when EL receives `ConsensusOutput` after
    /// a leader is selected for an even round.
    fn commit_canonical_payload(
        &self,
        payload: Arc<BlockPayload>,
    ) -> Result<(), execution_interfaces::Error> {
        // TODO: this is overkill, but prevents a default() from
        // being committed
        //
        // block hash must be valid
        if payload.get_block().hash().is_zero() {
            return Err(
                execution_interfaces::Error::Custom(
                    "Block hash cannot be zero".to_string()
                )
            )
        }

        let parent_hash = payload.get_block().parent_hash;
        if parent_hash != self.blockchain.canonical_tip().hash {
            return Err(
                ConsensusError::ParentHashMismatch {
                    expected_parent_hash: self.blockchain.canonical_tip().hash,
                    got_parent_hash: parent_hash,
                }
                .into()
            )
        }

        // taken from blockchain_tree::commit_canonical()
        let provider = DatabaseProvider::new_rw(
            self.db.tx_mut()?,
            self.chain_spec.clone(),
        );

        let blocks = vec![payload.get_block().to_owned()];
        let state = payload.get_poststate().to_owned();

        provider
            .append_blocks_with_post_state(blocks, state)
            .map_err(|e| execution_interfaces::Error::Execution(
                BlockExecutionError::CanonicalCommit { inner: e.to_string() }
            ))?;

        provider.commit()?;
        Ok(())
    }

    /// Sets the state of the canon chain tracker based to the given head.
    ///
    /// This expects the given head to be the new canonical head.
    ///
    /// This updates the canon chain tracker and the canon chain tip
    /// used to validate batches from peers.
    fn update_canon_chain_trackers(
        &mut self,
        payload: Arc<BlockPayload>,
    ) -> Result<(), execution_interfaces::Error> {
        // update tree for batch validation
        self.blockchain.update_canonical_tip_after_commit(payload.get_block().to_owned(), payload.get_block_num());

        let head = payload.get_block().header.to_owned();

        // we update the the tracked header first
        self.blockchain.set_canonical_head(head);

        let block_hash = payload.get_block().hash;
        let finalized = self
            .blockchain
            .find_block_by_hash(block_hash, BlockSource::Any)?
            .ok_or_else(|| {
                Error::Provider(ProviderError::UnknownBlockHash(block_hash))
            })?;
        
        // TODO: safe isn't necessary
        // set finalized and safe
        self.blockchain.set_finalized(finalized.header.clone().seal(block_hash));
        self.blockchain.set_safe(finalized.header.seal(block_hash));

        // TODO: update listeners to react to this event
        // - txpool?
        // - batch maker?
        self.listeners.notify(LatticeConsensusEngineEvent::CanonicalBlockAdded(payload));

        Ok(())
    }

    //      TODO: is this useful for storing validated batches then producing the next canonical block?
    //
    // /// When the pipeline is actively syncing the tree is unable to commit any additional blocks
    // /// since the pipeline holds exclusive access to the database.
    // ///
    // /// In this scenario we buffer the payload in the tree if the payload is valid, once the
    // /// pipeline finished syncing the tree is then able to also use the buffered payloads to commit
    // /// to a (newer) canonical chain.
    // ///
    // /// This will return `SYNCING` if the block was buffered successfully, and an error if an error
    // /// occurred while buffering the block.
    // #[instrument(level = "trace", skip_all, target = "consensus::engine", ret)]
    // fn try_buffer_payload(
    //     &mut self,
    //     block: SealedBlock,
    // ) -> Result<BatchPayloadStatus, InsertBlockError> {
    //     self.blockchain.buffer_block_without_senders(block)?;
    //     Ok(BatchPayloadStatus::from_status(BatchPayloadStatusEnum::Syncing))
    // }

}

/// On initialization, the consensus engine will poll the message receiver and return
/// [Poll::Pending] until the first forkchoice update message is received.
///
/// As soon as the consensus engine receives the first forkchoice updated message and updates the
/// local forkchoice state, it will launch the pipeline to sync to the head hash.
/// While the pipeline is syncing, the consensus engine will keep processing messages from the
/// receiver and forwarding them to the blockchain tree.
impl<DB, BT> Future for LatticeConsensusEngine<DB, BT>
where
    BT: BlockchainTreeEngine
        + BlockReader
        + CanonChainTracker
        + StageCheckpointReader
        + Unpin
        + 'static,
    DB: Database + Unpin,
{
    type Output = Result<(), LatticeConsensusEngineError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // Process all incoming messages from the CL, these can affect the state of the
        // SyncController, hence they are polled first, and they're also time sensitive.
        loop {
            // handle next engine message
            match this.engine_message_rx.poll_next_unpin(cx) {
                Poll::Ready(Some(msg)) => match msg {
                    // worker received peer's batch
                    LatticeEngineMessage::ValidateBatch { payload, tx } => {
                        this.metrics.validate_batch_request.increment(1);
                        let res = this.validate_batch(payload);
                        let _ = tx.send(res);
                    }
                    LatticeEngineMessage::Consensus { payload, tx } => {
                        this.metrics.canonical_block_update_request.increment(1);
                        debug!("\n\n!!! received consensus message from handle");
                        let res = this.on_consensus(payload);
                        let _ = tx.send(res);
                    }
                    // TODO: does EL need to know this information?
                    LatticeEngineMessage::TransitionConfigurationExchanged => {
                        this.blockchain.on_transition_configuration_exchanged();
                    }
                    LatticeEngineMessage::EventListener(tx) => {
                        this.listeners.push_listener(tx);
                    }
                },
                Poll::Ready(None) => {
                    unreachable!("Engine holds a sender to the message channel")
                }
                Poll::Pending => {
                    // no more CL messages to process
                    return Poll::Pending
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use execution_blockchain_tree::{ShareableBlockchainTree, TreeExternals, BlockchainTree, BlockchainTreeConfig};
    use execution_db::{DatabaseEnv, test_utils::create_test_rw_db, database::Database};
    use execution_interfaces::test_utils::{TestConsensus, generators::{random_block, self, random_batch}};
    use execution_provider::{providers::BlockchainProvider, test_utils::{TestExecutorFactory, blocks::BlockChainTestData}, PostState, ProviderFactory, BlockWriter};
    use lattice_payload_builder::BatchPayload;
    use tn_types::{execution::{ChainSpecBuilder, LATTICE, ChainSpec, SealedBlock, U256, SealedBlockWithSenders}, consensus::Batch};
    use tokio::sync::oneshot::{self, error::TryRecvError};
    use assert_matches::assert_matches;

    use super::*;

    type TestLatticeConsensusEngine = LatticeConsensusEngine<
        Arc<DatabaseEnv>,
        BlockchainProvider<
            Arc<DatabaseEnv>,
            ShareableBlockchainTree<Arc<DatabaseEnv>, TestConsensus, TestExecutorFactory>,
        >,
    >;

    struct TestEnv<DB> {
        db: DB,
        engine_handle: LatticeConsensusEngineHandle,
    }

    impl<DB> TestEnv<DB> {
        fn new(
            db: DB,
            engine_handle: LatticeConsensusEngineHandle,
        ) -> Self {
            Self { db, engine_handle }
        }

        async fn send_new_canonical_block(
            &self,
            payload: Arc<BlockPayload>,
        ) -> Result<(), execution_interfaces::Error> {
            self.engine_handle.new_canonical_block(payload).await
        }

        async fn send_batch_from_peer(
            &self,
            batch: Batch,
        ) -> Result<(), LatticeOnNewPayloadError> {
            self.engine_handle.validate_batch(batch).await
        }
    }

    struct TestConsensusEngineBuilder {
        chain_spec: Arc<ChainSpec>,
        executor_results: Vec<PostState>,
    }

    impl TestConsensusEngineBuilder {
        /// Create a new `Self` with the `LATTICE` chainspec.
        fn new(chain_spec: Arc<ChainSpec>) -> Self {
            Self {
                chain_spec,
                executor_results: Vec::new(),
            }
        }

        /// Set the executor results to use for the test consensus engine.
        fn with_executor_results(mut self, executor_results: Vec<PostState>) -> Self {
            self.executor_results = executor_results;
            self
        }

        /// Build the test consensus engine.
        fn build(self) -> (TestLatticeConsensusEngine, TestEnv<Arc<DatabaseEnv>>) {
            tn_tracing::init_test_tracing();
            let db = create_test_rw_db();
            let consensus = TestConsensus::default();
            // let genesis_block = SealedBlock {
            //     header: self.chain_spec.sealed_genesis_header(),
            //     body: vec![],
            //     ommers: vec![],
            //     withdrawals: Some(vec![]),
            // };

            let executor_factory = TestExecutorFactory::new(self.chain_spec.clone());
            executor_factory.extend(self.executor_results);
            // insert_blocks(&db.as_ref(), self.chain_spec.clone(), [&genesis_block].into_iter());

            // Setup blockchain tree
            let externals = TreeExternals::new(
                db.clone(),
                consensus,
                executor_factory,
                self.chain_spec.clone(),
            );
            let config = BlockchainTreeConfig::new(1, 2, 3, 2);
            let (canon_state_notification_sender, _) = tokio::sync::broadcast::channel(3);
            let tree = ShareableBlockchainTree::new(
                BlockchainTree::new(externals, canon_state_notification_sender, config)
                    .expect("failed to create tree"),
            );
            let shareable_db = ProviderFactory::new(db.clone(), self.chain_spec.clone());
            let latest = self.chain_spec.genesis_header().seal_slow();
            let blockchain_provider = BlockchainProvider::with_latest(shareable_db, tree, latest);


            let (engine, handle) = LatticeConsensusEngine::new(blockchain_provider, db.clone(), self.chain_spec.clone())
                .expect("Consensus engine should be fine");

            (engine, TestEnv::new(db, handle))
        }

    }

    fn spawn_consensus_engine(
        engine: TestLatticeConsensusEngine,
    ) -> oneshot::Receiver<Result<(), LatticeConsensusEngineError>> {
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let result = engine.await;
            tx.send(result).expect("failed to forward consensus engine result");
        });
        rx
    }

    fn insert_blocks<'a, DB: Database>(
        db: &DB,
        chain: Arc<ChainSpec>,
        mut blocks: impl Iterator<Item = &'a SealedBlock>,
    ) {
        let factory = ProviderFactory::new(db, chain);
        let provider = factory.provider_rw().unwrap();
        blocks
            .try_for_each(|b| provider.insert_block(b.clone(), None).map(|_| ()))
            .expect("failed to insert");
        provider.commit().unwrap();
    }

    #[tokio::test]
    async fn test_genesis_and_one_block() {
        // chain spec
        let chain_spec = Arc::new(
            ChainSpecBuilder::default()
                .chain(LATTICE.chain)
                .genesis(LATTICE.genesis.clone())
                .paris_activated()
                .build(),
        );

        let (engine, env) = TestConsensusEngineBuilder::new(chain_spec.clone()).build();

        // spawn engine
        let mut engine_rx = spawn_consensus_engine(engine);

        // create blocks
        let mut rng = generators::rng();
        // genesis
        let genesis = random_block(&mut rng, 0, None, None, Some(0))
            .try_seal_with_senders()
            .unwrap();
        let genesis_hash = genesis.hash;
        let genesis_payload = Arc::new(BlockPayload::new(genesis, PostState::default(), U256::ZERO));

        env.send_new_canonical_block(genesis_payload).await.unwrap();

        // block 1 with wrong parent hash should fail
        let block1_invalid_header = random_block(&mut rng, 1, Some(chain_spec.genesis_hash()), None, Some(0))
            .try_seal_with_senders()
            .unwrap();
        let payload = Arc::new(BlockPayload::new(block1_invalid_header, PostState::default(), U256::ZERO));

        assert!(env.send_new_canonical_block(payload).await.is_err());

        // block 1 with valid parent hash should pass
        let block1_valid = random_block(&mut rng, 1, Some(genesis_hash), None, Some(0))
            .try_seal_with_senders()
            .unwrap();
        let payload = Arc::new(BlockPayload::new(block1_valid, PostState::default(), U256::ZERO));

        assert!(env.send_new_canonical_block(payload).await.is_ok());

        assert_matches!(engine_rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test]
    async fn test_validate_batch() {
        let data = BlockChainTestData::default();
        let mut block1 = data.blocks[0].0.block.clone();
        let exec_result = data.blocks[0].1.clone();
        block1 = block1.unseal().seal_slow();

        // chain spec
        let chain_spec = Arc::new(
            ChainSpecBuilder::default()
                .chain(LATTICE.chain)
                .genesis(LATTICE.genesis.clone())
                .paris_activated()
                .build(),
        );

        let (engine, env) = TestConsensusEngineBuilder::new(chain_spec.clone())
            .with_executor_results(Vec::from([exec_result.clone()]))
            .build();

        // spawn engine
        let mut engine_rx = spawn_consensus_engine(engine);

        let block1_hash = block1.hash();

        // update canonical chain with block1
        let payload = Arc::new(BlockPayload::new(
            block1.seal_with_senders().unwrap(),
            exec_result,
            U256::ZERO
        ));
        env.send_new_canonical_block(payload).await.unwrap();

        // create batch
        let mut rng = generators::rng();

        // ensure default parent hash (H256::ZERO)
        // fails when batch number != 1
        let invalid_batch = random_batch(&mut rng, 2, None, Some(3));
        assert!(env.send_batch_from_peer(invalid_batch).await.is_err());

        // ensure old parent hash fails
        let invalid_batch = random_batch(&mut rng, 2, Some(data.genesis.hash), Some(3));
        assert!(env.send_batch_from_peer(invalid_batch).await.is_err());

        // ensure unknown parent hash fails
        let invalid_batch = random_batch(&mut rng, 2, Some(data.blocks[1].0.block.hash), Some(3));
        assert!(env.send_batch_from_peer(invalid_batch).await.is_err());

        // validate next batch - built off block1
        let batch = random_batch(&mut rng, 2, Some(block1_hash), Some(3));
        env.send_batch_from_peer(batch).await.unwrap();
        assert_matches!(engine_rx.try_recv(), Err(TryRecvError::Empty));
    }
}
