//! Run the execution layer
pub mod cl_events;
mod events;
use crate::{
    args::{utils::genesis_value_parser, PayloadBuilderArgs},
    dirs::{DataDirPath, MaybePlatformPath},
    node::cl_events::ConsensusLayerHealthEvents,
    runner::CliContext,
    RpcServerArgs,
};
use clap::{crate_version, Parser};
use execution_basic_payload_builder::{BasicPayloadJobGenerator, BasicPayloadJobGeneratorConfig};
use execution_blockchain_tree::{
    BlockchainTree, BlockchainTreeConfig, ShareableBlockchainTree, TreeExternals,
};
use execution_db::{
    database::Database, init_db, mdbx::{Env, WriteMap},
};
use execution_downloaders::{
    bodies::bodies::BodiesDownloaderBuilder,
    headers::reverse_headers::ReverseHeadersDownloaderBuilder,
};
use execution_interfaces::{
    consensus::Consensus,
    p2p::{
        bodies::{client::BodiesClient, downloader::BodyDownloader},
        headers::{client::HeadersClient, downloader::HeaderDownloader},
    },
    test_utils::NoopFullBlockClient,
};
use execution_network_api::noop::NoopNetwork;
use execution_payload_builder::PayloadBuilderService;
use execution_provider::{
    providers::BlockchainProvider, BlockHashReader, CanonStateSubscriptions, HeaderProvider,
    ProviderFactory, StageCheckpointReader,
};
use execution_revm::{stack::Hook, Factory};
use execution_rpc_engine_api::EngineApi;
use eyre::WrapErr;
use futures::{stream_select, StreamExt};
use tn_types::execution::{
    stage::StageId, ChainSpec, Head, H256,
};
use tracing::{debug, info};
use crate::init::init_genesis;
use execution_config::{config::StageConfig, Config};
use execution_lattice_consensus::{
    LatticeConsensus, LatticeConsensusEngine, LatticeNetworkAdapter, MIN_BLOCKS_FOR_PIPELINE_RUN,
};
use execution_revm_inspectors::stack::InspectorStackConfig;
use execution_stages::{
    sets::DefaultStages,
    stages::{
        AccountHashingStage, ExecutionStage, ExecutionStageThresholds, HeaderSyncMode,
        IndexAccountHistoryStage, IndexStorageHistoryStage, MerkleStage, SenderRecoveryStage,
        StorageHashingStage, TotalDifficultyStage, TransactionLookupStage,
    },
    Pipeline, StageSet,
};
use execution_tasks::TaskExecutor;
use execution_transaction_pool::EthTransactionValidator;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::{mpsc::unbounded_channel, oneshot, watch};
use tokio_util::sync::CancellationToken;

/// Result type for node command.
type Result<T> = eyre::Result<T>;

/// Run the node
#[derive(Parser, Debug)]
pub struct Command {
    /// The path to the configuration file to use.
    #[arg(long, value_name = "FILE", verbatim_doc_comment)]
    config: Option<PathBuf>,

    /// The chain this node is running.
    ///
    /// Possible values are either a built-in chain or the path to a chain specification file.
    ///
    /// Built-in chains:
    /// - lattice
    #[arg(
        long,
        value_name = "CHAIN_OR_PATH",
        verbatim_doc_comment,
        default_value = "lattice",
        value_parser = genesis_value_parser
    )]
    chain: Arc<ChainSpec>,

    /// The path to the data dir for all related files and subdirectories.
    ///
    /// Defaults to the OS-specific data directory:
    ///
    /// - Linux: `$XDG_DATA_HOME/telcoin-network/` or `$HOME/.local/share/telcoin-network/`
    /// - Windows: `{FOLDERID_RoamingAppData}/lattice/`
    /// - macOS: `$HOME/Library/Application Support/lattice/`
    #[arg(long, value_name = "DATA_DIR", verbatim_doc_comment, default_value_t)]
    datadir: MaybePlatformPath<DataDirPath>,

    /// RPC Server configuration
    #[clap(flatten)]
    rpc: RpcServerArgs,

    /// Payload builder configuration
    #[clap(flatten)]
    payload: PayloadBuilderArgs,

    // /// Gateway configuration
    // #[clap(flatten)]
    // endpoints: EndpointArgs,
    /// Dev flag indicates temporary data should be removed after process exits
    #[arg(long)]
    pub dev: bool,
}

impl Command {
    /// Main execution function for execution layer
    pub async fn execute(self, cli_ctx: CliContext) -> Result<()> {
        info!(target: "lattice::cli", "lattice {} starting", crate_version!());

        // create custom chain spec
        // let chain = Arc::new(self.build_chain_spec()?);
        // let chain = LATTICE;

        // config
        let data_dir = if self.dev {
            self.datadir.temp_chain(self.chain.chain.clone())
        } else {
            self.datadir.unwrap_or_chain_default(self.chain.chain.clone())
        };

        let config_path = self.config.clone().unwrap_or(data_dir.config_path());
        let mut config = self.load_config(&config_path)?;

        let db_path = data_dir.db_path();
        let db = Arc::new(init_db(&db_path, None)?);

        // TODO: start metrics

        // TODO: for now, start genesis here and in NW
        // eventually, it makes more sense for NW to initiate genesis

        // initialize genesis
        let genesis_hash = init_genesis(db.clone(), self.chain.clone())?;
        info!(?genesis_hash, "Genesis hash:");

        // AutoSeal didn't work, so NarwhalSeal is based on Beacon
        //
        // but if a worker's batch fails, then it needs to be retried somehow
        // unless, it's the batch itself that causes the worker to fail
        let consensus: Arc<dyn Consensus> = Arc::new(LatticeConsensus::new(self.chain.clone()));

        // configure blockchain tree
        let tree_externals = TreeExternals::new(
            db.clone(),
            Arc::clone(&consensus),
            Factory::new(self.chain.clone()),
            Arc::clone(&self.chain),
        );

        // default tree config for now
        let tree_config = BlockchainTreeConfig::default();

        // TODO: probably don't need to reconfigure tree since consensus is final
        // so, remove the canon_state_notification_sender (could still be useful for impl epochs?)
        //
        // original from reth:
        // The size of the broadcast is twice the maximum reorg depth, because at maximum reorg
        // depth at least N blocks must be sent at once.
        let (canon_state_notification_sender, _receiver) =
            tokio::sync::broadcast::channel(tree_config.max_reorg_depth() as usize * 2);

        // ShareableBlockchainTree is how blocks are finalized and committed to the DB
        let blockchain_tree = ShareableBlockchainTree::new(BlockchainTree::new(
            tree_externals,
            canon_state_notification_sender.clone(),
            tree_config,
        )?);

        // setup the blockchain provider - main struct for interacting with the blockchain
        let factory = ProviderFactory::new(Arc::clone(&db), Arc::clone(&self.chain));
        let blockchain_db = BlockchainProvider::new(factory, blockchain_tree.clone())?;

        // TODO: consider passing different PoolConfig
        // currently: Default::default()
        //
        // `Pool` lib also contains constants for setting max tx byte size, contract size, etc.
        let transaction_pool = execution_transaction_pool::Pool::eth_pool(
            EthTransactionValidator::with_additional_tasks(
                blockchain_db.clone(),
                Arc::clone(&self.chain),
                cli_ctx.task_executor.clone(),
                1,
            ),
            Default::default(),
        );
        info!("transaction pool initialized");

        // spawn txpool maintenance task
        //
        // listens for changes to canonical chain and updates pool
        {
            let pool = transaction_pool.clone();
            let chain_events = blockchain_db.canonical_state_stream();
            let client = blockchain_db.clone();
            cli_ctx.task_executor.spawn_critical(
                "txpool maintenance task",
                execution_transaction_pool::maintain::maintain_transaction_pool_future(
                    client,
                    pool,
                    chain_events,
                ),
            );
            debug!("Spawned txpool maintenance task");
        }

        // skip execution network stuff for now

        // sender to `Workers` and receiver to `LatticeConsensusEngine`
        let (consensus_engine_tx, consensus_engine_rx) = unbounded_channel();

        let payload_generator = BasicPayloadJobGenerator::new(
            blockchain_db.clone(),
            transaction_pool.clone(),
            cli_ctx.task_executor.clone(),
            // TODO: this is where max gas per block is set
            // also see, PayloadBuilderAttributes.cfg_and_block_env()
            //
            // if payloads should be larger for batches, this value should change
            // consider if running more than one worker
            //
            // also, deadline may not be relevant here?
            BasicPayloadJobGeneratorConfig::default()
                .interval(self.payload.interval)
                .deadline(self.payload.deadline)
                .max_payload_tasks(self.payload.max_payload_tasks)
                .extradata(self.payload.extradata_bytes())
                .max_gas_limit(self.payload.max_gas_limit),
            Arc::clone(&self.chain),
        );

        // payload_builder is the handle for sending PayloadServiceCommands to the spawned payload
        // service task options: BuildNewPayload, BestPayload, and Resolve
        let (payload_service, payload_builder) = PayloadBuilderService::new(payload_generator);

        debug!(target: "tn::cli", "Spawning payload builder service");
        cli_ctx.task_executor.spawn_critical("payload builder service", payload_service);

        // see execution/consensus/auto-seal/task.rs:L96 for how to create blocks
        //
        // TODO: NarwhalSeal stores blocks in memory
        //
        // blocks in memory may be useful
        //  - happy path all worker batches succeed
        //  - pending batches won't block future transactions
        //      - prevents tx_nonce: 2 from pending until tx_nonce: 1 to be finalized
        //  - BUT what if gas changes, then pending blocks need to be updated
        //
        // BeaconConsensus may be best if storage code is hairy to impl
        //
        // maybe create NarwhalConsensus crate with custom implementation

        // Configure the pipeline
        // let (_, network, mut task) = NWBSBuilder::new(
        //     Arc::clone(&chain),
        //     blockchain_db.clone(),
        //     transaction_pool.clone(),
        //     consensus_engine_tx.clone(),
        //     canon_state_notification_sender,
        // )
        // .build();

        let network_client = NoopFullBlockClient::default();
        let network = NoopNetwork {};

        // Pipeline TODOs
        //  - this is connected with the network and responsible for syncing
        //      - syncing should only be done by NW consensus layer
        let mut pipeline = self
            .build_networked_pipeline(
                &mut config,
                network_client.clone(),
                Arc::clone(&consensus),
                db.clone(),
                &cli_ctx.task_executor,
            )
            .await?;

        // passing some to the BeaconConsensusEngine will start the pipeline immediately
        let initial_target = None; // Some(genesis_hash)
        let stubbed_out_network_sync = LatticeNetworkAdapter::default();

        // create pipeline_events again before moving pipeline
        let pipeline_events = pipeline.events();

        // Configure the consensus engine
        // let (consensus_engine, engine_handle) = LatticeConsensusEngine::with_channel(
        let (consensus_engine, engine_handle) = execution_beacon_consensus::BeaconConsensusEngine::with_channel(
            network_client,
            pipeline,
            blockchain_db.clone(),
            Box::new(cli_ctx.task_executor.clone()),
            Box::new(stubbed_out_network_sync),
            None,
            false, // true for continuous pipeline
            payload_builder.clone(),
            initial_target,
            MIN_BLOCKS_FOR_PIPELINE_RUN,
            consensus_engine_tx,
            consensus_engine_rx,
        )?;
        info!("consensus engine initialized");

        let head = self.lookup_head(Arc::clone(&db))?;

        // log events from the node
        // let events = stream_select!(
        //     // network.event_listener().map(Into::into),
        //     engine_handle.event_listener().map(Into::into),
        //     pipeline_events.map(Into::into),
        //     ConsensusLayerHealthEvents::new(Box::new(blockchain_db.clone())).map(Into::into),
        // );

        // cli_ctx
        //     .task_executor
        //     .spawn_critical("events task", events::handle_events(None, Some(head.number), events));

        // //
        // // TODO: engine api may not be necessary when running everything locally
        // // consensus_engine_tx/consensus_engine_rx for NWBSEngine struct
        // //
        // // but rpc expects this
        let engine_api = EngineApi::new(
            blockchain_db.clone(),
            self.chain.clone(),
            engine_handle,
            payload_builder.into(),
        );
        info!(target: "tn::cli", "Engine API handler initialized");

        // extract the jwt secret from the args if possible
        let default_jwt_path = data_dir.jwt_path();
        let jwt_secret = self.rpc.jwt_secret(default_jwt_path)?;

        // Start RPC servers
        let (_rpc_server, _auth_server) = self
            .rpc
            .start_servers(
                blockchain_db.clone(),
                transaction_pool.clone(),
                network,
                cli_ctx.task_executor.clone(),
                blockchain_tree,
                engine_api,
                jwt_secret,
            )
            .await?;

        // // Run consensus engine to completion
        // let (tx, rx) = oneshot::channel();
        // info!(target: "tn::cli", "Starting consensus engine");
        // cli_ctx.task_executor.spawn_critical_blocking("consensus engine", async move {
        //     let res = beacon_consensus_engine.await;
        //     let _ = tx.send(res);
        // });

        // rx.await??;

        // info!(target: "tn::cli", "Consensus engine has exited.");
        info!(target: "tn::cli", "Executon layer ready.");

        // build consensus layer:
        //
        // create committee fixture
        //
        // pass TransactionClient to engine handler
        //
        // request next batch => send payload proto to worker




        let forever = futures::future::pending::<()>();
        let _ = forever.await;
        Ok(())
    }

    fn load_config(&self, config_path: &PathBuf) -> Result<Config> {
        confy::load_path::<Config>(config_path)
            .wrap_err_with(|| format!("Failed to load config: {:?}", config_path))
    }

    // fn build_chain_spec(&self) -> Result<ChainSpec> {
    //     let chain = Chain::Id(2600);
    //     let genesis = self.build_genesis()?;
    //     let mut chain_spec = ChainSpecBuilder::default()
    //         .chain(chain)
    //         // set shanghai to enable latest features for block building
    //         .shanghai_activated()
    //         .genesis(genesis)
    //         .build();

    //     let genesis_hash = chain_spec.genesis_hash();
    //     chain_spec.genesis_hash = Some(genesis_hash);

    //     Ok(chain_spec)
    // }

    // /// Build the genesis struct for chain spec
    // fn build_genesis(&self) -> Result<Genesis> {
    //     // whitelist for setting transaction categories
    //     let whitelist_address = hex!("1a771c3000000000000000000000000000000000").into();
    //     let whitelist_contract = GenesisAccount::default()
    //         .with_nonce(Some(1))
    //         .with_balance(U256::ZERO)
    //         .with_code(Some(Bytes::from(b"code")));

    //     // some wallets require mnemonic phrase, others want a private key
    //     //
    //     // mnemonic: chronic good mimic tube glance wreck badge theme park expect select empty
    //     let mnemonic_address = hex!("F8DC4E397B48E85106a7c3CEd81D45de71E4Caf3").into();
    //     let mnemonic_account = GenesisAccount::default().with_balance(U256::MAX);

    //     // bob private key: 0x99b3c12287537e38c90a9219d4cb074a89a16e9cdb20bf85728ebd97c343e342
    //     let bob_address = hex!("6Be02d1d3665660d22FF9624b7BE0551ee1Ac91b").into();
    //     let bob_account = GenesisAccount::default().with_balance(U256::MAX);

    //     // create contract, and two accounts with max TEL
    //     let accounts = HashMap::from([
    //         (whitelist_address, whitelist_contract),
    //         (bob_address, bob_account),
    //         (mnemonic_address, mnemonic_account),
    //     ]);

    //     let genesis = Genesis::default().extend_accounts(accounts);

    //     Ok(genesis)
    // }

    // /// Build the genesis block from the genesis struct in chain spec
    // fn build_genesis_block(&self, state_root: H256) -> SealedBlock {
    //     SealedBlock {
    //         header: Header {
    //             number: 0,
    //             state_root,
    //             withdrawals_root: Some(EMPTY_ROOT),
    //             ..Default::default()
    //         }
    //         .seal_slow(),
    //         withdrawals: Some(vec![Withdrawal::default()]),
    //         ..Default::default()
    //     }
    // }

    // fn initialize_genesis<DB: Database>(&self, db: Arc<DB>, chain: Arc<ChainSpec>) -> Result<()>
    // {     init_genesis(db.clone(), chain.clone())?;
    //     let state_root = genesis_state_root(&chain.genesis().alloc);
    //     let block = self.build_genesis_block(state_root);
    //     // let mut tx = Transaction::new(db.as_ref())?;
    //     // insert_canonical_block(tx.deref_mut(), block, None)?;
    //     // tx.put::<tables::AccountsTrie>(state_root, vec![0x80])?;
    //     // tx.commit()?;
    //     // tx.close();
    //     Ok(())
    // }

    // TODO: this does not need to be wired to a network
    // pipeline only needs stages for block creation
    //
    // NW consensus manages all syncing state internally
    // and feeds certs to execution layer.
    //
    // network layer may be useful for requesting block number / block hash from peers
    // to give scores and ensure new block integrity
    //
    // example:
    //      - produce next block
    //      - share block number and hash
    //      - peers confirm this matches their db
    //      - ensure execution layer is honest
    //          - is this necessary?
    //
    /// Constructs a [Pipeline] that's wired to the network
    async fn build_networked_pipeline<DB, Client>(
        &self,
        config: &mut Config,
        client: Client,
        consensus: Arc<dyn Consensus>,
        db: DB,
        task_executor: &TaskExecutor,
        // chain: Arc<ChainSpec>,
    ) -> eyre::Result<Pipeline<DB>>
    where
        DB: Database + Unpin + Clone + 'static,
        Client: HeadersClient + BodiesClient + Clone + 'static,
    {
        let max_block = None;

        // TODO: remove downloaders - execution layer only syncs from NW consensus
        //
        // building network downloaders using the fetch client
        let header_downloader = ReverseHeadersDownloaderBuilder::default()
            .build(client.clone(), Arc::clone(&consensus))
            .into_task_with(task_executor);

        let body_downloader = BodiesDownloaderBuilder::default()
            .build(client, Arc::clone(&consensus), db.clone())
            .into_task_with(task_executor);

        let pipeline = self
            .build_pipeline(
                db,
                config,
                header_downloader,
                body_downloader,
                consensus,
                max_block,
                false,
                // self.chain,
            )
            .await?;

        Ok(pipeline)
    }

    // TODO: remove header/body downloader
    //
    // for now, NarwhalSeal stubbs them out
    #[allow(clippy::too_many_arguments)]
    async fn build_pipeline<DB, H, B>(
        &self,
        db: DB,
        config: &Config,
        header_downloader: H,
        body_downloader: B,
        consensus: Arc<dyn Consensus>,
        max_block: Option<u64>,
        continuous: bool,
        // chain: Arc<ChainSpec>,
    ) -> eyre::Result<Pipeline<DB>>
    where
        DB: Database + Clone + 'static,
        H: HeaderDownloader + 'static,
        B: BodyDownloader + 'static,
    {
        let stage_config = StageConfig::default();

        let mut builder = Pipeline::builder();

        if let Some(max_block) = max_block {
            debug!(target: "tn::cli", max_block, "Configuring builder to use max block");
            builder = builder.with_max_block(max_block)
        }

        let (tip_tx, tip_rx) = watch::channel(H256::zero());
        // use execution_revm_inspectors::stack::InspectorStackConfig;
        let factory = execution_revm::Factory::new(self.chain.clone());

        // TODO: this might be helpful for tracing tx/blocks in revm
        let stack_config = InspectorStackConfig { use_printer_tracer: false, hook: Hook::None };

        let factory = factory.with_stack_config(stack_config);

        // TODO: should this be continuous?
        let header_mode = HeaderSyncMode::Tip(tip_rx);
        // old: if continuous { HeaderSyncMode::Continuous } else { HeaderSyncMode::Tip(tip_rx) };

        // TODO: clean up pipeline since syncing doesn't need to happen
        //
        // not sure if it's worth it right now, so leaving default since network is stubbed out for
        // NarwhalSealConsensus
        //
        // double check that don't need:
        //  - TotalDifficulty
        //
        // might only need OfflineStages
        let pipeline = builder
            .with_tip_sender(tip_tx)
            .add_stages(
                // TODO: only use OfflineStages instead of Default?
                // default includes downloaders and header sync mode
                //
                // online also listens for beacon consensus updates?
                DefaultStages::new(
                    header_mode,
                    Arc::clone(&consensus),
                    header_downloader,
                    body_downloader,
                    factory.clone(),
                )
                .set(
                    TotalDifficultyStage::new(consensus)
                        .with_commit_threshold(stage_config.total_difficulty.commit_threshold),
                )
                .set(SenderRecoveryStage {
                    commit_threshold: stage_config.sender_recovery.commit_threshold,
                })
                .set(ExecutionStage::new(
                    factory,
                    ExecutionStageThresholds {
                        max_blocks: stage_config.execution.max_blocks,
                        max_changes: stage_config.execution.max_changes,
                    },
                ))
                .set(AccountHashingStage::new(
                    stage_config.account_hashing.clean_threshold,
                    stage_config.account_hashing.commit_threshold,
                ))
                .set(StorageHashingStage::new(
                    stage_config.storage_hashing.clean_threshold,
                    stage_config.storage_hashing.commit_threshold,
                ))
                .set(MerkleStage::new_execution(stage_config.merkle.clean_threshold))
                .set(TransactionLookupStage::new(stage_config.transaction_lookup.commit_threshold))
                .set(IndexAccountHistoryStage::new(
                    stage_config.index_account_history.commit_threshold,
                ))
                .set(IndexStorageHistoryStage::new(
                    stage_config.index_storage_history.commit_threshold,
                )),
            )
            .build(db, self.chain.clone());

        Ok(pipeline)
    }

    fn lookup_head(&self, db: Arc<Env<WriteMap>>) -> Result<Head> {
        let factory = ProviderFactory::new(db, self.chain.clone());
        let provider = factory.provider()?;

        let head = provider.get_stage_checkpoint(StageId::Finish)?.unwrap_or_default().block_number;

        let header = provider
            .header_by_number(head)?
            .expect("the header for the latest block is missing, database is corrupt");

        let total_difficulty = provider
            .header_td_by_number(head)?
            .expect("the total difficulty for the latest block is missing, database is corrupt");

        let hash = provider
            .block_hash(head)?
            .expect("the hash for the latest block is missing, database is corrupt");

        Ok(Head {
            number: head,
            hash,
            difficulty: header.difficulty,
            total_difficulty,
            timestamp: header.timestamp,
        })
    }
}
