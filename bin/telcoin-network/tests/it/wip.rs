// Copyright (c) Telcoin, LLC

use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
};

use clap::Parser;
use eyre::Context;
use futures::{pin_mut, stream_select, StreamExt};
use reth::{
    args::{get_secret_key, DiscoveryArgs, NetworkArgs, PayloadBuilderArgs},
    cli::{
        components::{RethNodeComponents, RethNodeComponentsImpl},
        config::PayloadBuilderConfig,
    },
    init::init_genesis,
    node::{cl_events::ConsensusLayerHealthEvents, events, NodeCommand},
    rpc::builder::RpcServerHandle,
};
use reth_auto_seal_consensus::{AutoSealBuilder, MiningMode};
use reth_basic_payload_builder::{BasicPayloadJobGenerator, BasicPayloadJobGeneratorConfig};
use reth_beacon_consensus::{
    hooks::EngineHooks, BeaconConsensus, BeaconConsensusEngine, MIN_BLOCKS_FOR_PIPELINE_RUN,
};
use reth_blockchain_tree::{
    BlockchainTree, BlockchainTreeConfig, ShareableBlockchainTree, TreeExternals,
};
use reth_config::{config::PruneConfig, Config};
use reth_db::{database::Database, test_utils::create_test_rw_db, DatabaseEnv};
use reth_downloaders::{
    bodies::bodies::BodiesDownloaderBuilder,
    headers::reverse_headers::ReverseHeadersDownloaderBuilder,
};
use reth_interfaces::{
    consensus::Consensus,
    p2p::{
        bodies::{client::BodiesClient, downloader::BodyDownloader},
        headers::{client::HeadersClient, downloader::HeaderDownloader},
    },
};
use reth_network::{NetworkEvents, NetworkHandle, NetworkManager};
use reth_payload_builder::{PayloadBuilderHandle, PayloadBuilderService};
use reth_primitives::{stage::StageId, ChainSpec, Genesis, Head, B256};
use reth_provider::{
    providers::BlockchainProvider, BlockHashReader, BlockReader, CanonStateSubscriptions,
    HeaderProvider, ProviderFactory, StageCheckpointReader,
};
use reth_revm::Factory;
use reth_revm_inspectors::stack::Hook;
use reth_stages::{
    sets::DefaultStages,
    stages::{
        AccountHashingStage, ExecutionStage, ExecutionStageThresholds, HeaderSyncMode,
        IndexAccountHistoryStage, IndexStorageHistoryStage, MerkleStage, SenderRecoveryStage,
        StorageHashingStage, TotalDifficultyStage, TransactionLookupStage,
    },
    Pipeline, StageSet,
};
use reth_tasks::{TaskExecutor, TaskManager, TaskSpawner};
use reth_tracing::init_test_tracing;
use reth_transaction_pool::{
    blobstore::InMemoryBlobStore, TransactionPool, TransactionValidationTaskExecutor,
};
use tokio::{
    runtime::Handle,
    sync::{mpsc::unbounded_channel, oneshot, watch},
};
use tracing::debug;

#[tokio::test]
async fn wip() -> eyre::Result<(), eyre::Error> {
    init_test_tracing();

    // lattice chain
    let chain = custom_chain();
    let db = create_test_rw_db();

    debug!("db path: {:?}", db);

    let genesis_hash = init_genesis(db.clone(), chain.clone())?;
    debug!("genesis hash: {genesis_hash:?}");

    // beacon consensus
    // TODO: consider replacing trait methods with batch logic
    let consensus: Arc<dyn Consensus> = Arc::new(BeaconConsensus::new(Arc::clone(&chain)));

    // configure blockchain tree
    let tree_externals = TreeExternals::new(
        Arc::clone(&db),
        Arc::clone(&consensus),
        Factory::new(chain.clone()),
        Arc::clone(&chain),
    );

    let tree = BlockchainTree::new(
        tree_externals,
        BlockchainTreeConfig::default(),
        // prune_config.clone().map(|config| config.segments),
        None,
    )?;

    let canon_state_notification_sender = tree.canon_state_notification_sender();
    let blockchain_tree = ShareableBlockchainTree::new(tree);
    debug!(target: "tn::cli", "configured blockchain tree");

    // fetch the head block from the database
    let head = lookup_head(db.db(), Arc::clone(&chain)).wrap_err("the head block is missing")?;

    // task executor
    let manager = TaskManager::new(Handle::current());
    let task_executor = manager.executor();

    // setup the blockchain provider
    let factory = ProviderFactory::new(Arc::clone(&db), Arc::clone(&chain));
    let blockchain_db = BlockchainProvider::new(factory, blockchain_tree.clone())?;
    let blob_store = InMemoryBlobStore::default();
    let validator = TransactionValidationTaskExecutor::eth_builder(Arc::clone(&chain))
        .with_head_timestamp(head.timestamp)
        // .kzg_settings(self.kzg_settings()?)
        .with_additional_tasks(1)
        .build_with_tasks(blockchain_db.clone(), task_executor.clone(), blob_store.clone());

    // default pool config
    let transaction_pool =
        reth_transaction_pool::Pool::eth_pool(validator, blob_store, Default::default());
    debug!(target: "tn::cli", "Transaction pool initialized");

    // spawn txpool maintenance task
    {
        let pool = transaction_pool.clone();
        let chain_events = blockchain_db.canonical_state_stream();
        let client = blockchain_db.clone();
        task_executor.spawn_critical(
            "txpool maintenance task",
            reth_transaction_pool::maintain::maintain_transaction_pool_future(
                client,
                pool,
                chain_events,
                task_executor.clone(),
                Default::default(),
            ),
        );
        debug!(target: "tn::cli", "Spawned txpool maintenance task");
    }
    // network
    let network_client = ProviderFactory::new(db.clone(), chain.clone());
    let network = build_network(
        chain.clone(),
        task_executor.clone(),
        network_client,
        head,
        transaction_pool.clone(),
    )
    .await?;

    // components
    let components = RethNodeComponentsImpl {
        provider: blockchain_db.clone(),
        pool: transaction_pool.clone(),
        network: network.clone(),
        task_executor: task_executor.clone(),
        events: blockchain_db.clone(),
    };

    let payload_builder = spawn_payload_builder_service(components.clone())?;

    let (sync_metrics_tx, _sync_metrics_rx) = unbounded_channel();
    let (consensus_engine_tx, consensus_engine_rx) = unbounded_channel();

    // default config for now
    let config = Config::default();

    // configure pipeline
    let max_transactions = 5;
    let mining_mode =
        MiningMode::instant(max_transactions, transaction_pool.pending_transactions_listener());

    let (_, client, mut task) = AutoSealBuilder::new(
        Arc::clone(&chain),
        blockchain_db.clone(),
        transaction_pool.clone(),
        consensus_engine_tx.clone(),
        canon_state_notification_sender,
        mining_mode,
    )
    .build();

    let mut pipeline = build_networked_pipeline(
        &config,
        client.clone(),
        Arc::clone(&consensus),
        db.clone(),
        &task_executor,
        sync_metrics_tx,
        chain.clone(),
        // prune_config.clone()
        // max_block
    )
    .await?;

    let pipeline_events = pipeline.events();
    task.set_pipeline_events(pipeline_events);
    debug!(target: "tn::cli", "Spawning auto mine task");
    task_executor.spawn(Box::pin(task));

    let pipeline_events = pipeline.events();
    let hooks = EngineHooks::new();

    // Configure the consensus engine
    let (beacon_consensus_engine, beacon_engine_handle) = BeaconConsensusEngine::with_channel(
        client,
        pipeline,
        blockchain_db.clone(),
        Box::new(task_executor.clone()),
        Box::new(network.clone()),
        None,  // max_block,
        false, //self.debug.continuous,
        payload_builder.clone(),
        None, // initial_target,
        MIN_BLOCKS_FOR_PIPELINE_RUN,
        consensus_engine_tx,
        consensus_engine_rx,
        hooks,
    )?;

    debug!(target: "tn::cli", "Consensus engine initialized");

    let events = stream_select!(
        network.event_listener().map(Into::into),
        beacon_engine_handle.event_listener().map(Into::into),
        pipeline_events.map(Into::into),
        ConsensusLayerHealthEvents::new(Box::new(blockchain_db.clone())).map(Into::into),
    );

    // events
    task_executor.spawn_critical(
        "events task",
        events::handle_events(Some(network.clone()), Some(head.number), events),
    );

    // start rpc
    let _rpc_server_handle = start_rpc(components).await?;

    // Run consensus engine to completion
    let (tx, rx) = oneshot::channel();
    debug!(target: "reth::cli", "Starting consensus engine");
    task_executor.spawn_critical_blocking("consensus engine", async move {
        let res = beacon_consensus_engine.await;
        let _ = tx.send(res);
    });

    rx.await??;

    Ok(())
}

fn custom_chain() -> Arc<ChainSpec> {
    let custom_genesis = r#"
{
    "nonce": "0x0",
    "timestamp": "0x6553A8CC",
    "extraData": "0x5343",
    "gasLimit": "0x1c9c380",
    "difficulty": "0x0",
    "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "coinbase": "0x0000000000000000000000000000000000000000",
    "alloc": {
        "0x6Be02d1d3665660d22FF9624b7BE0551ee1Ac91b": {
            "balance": "0x4a47e3c12448f4ad000000"
        }
    },
    "number": "0x0",
    "gasUsed": "0x0",
    "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "config": {
        "ethash": {},
        "chainId": 2600,
        "homesteadBlock": 0,
        "eip150Block": 0,
        "eip155Block": 0,
        "eip158Block": 0,
        "byzantiumBlock": 0,
        "constantinopleBlock": 0,
        "petersburgBlock": 0,
        "istanbulBlock": 0,
        "berlinBlock": 0,
        "londonBlock": 0,
        "terminalTotalDifficulty": 0,
        "terminalTotalDifficultyPassed": true,
        "shanghaiTime": 0
    }
}
"#;
    let genesis: Genesis = serde_json::from_str(custom_genesis).unwrap();
    Arc::new(genesis.into())
}

fn genesis_string() -> String {
    let custom_genesis = r#"
{
    "nonce": "0x0",
    "timestamp": "0x6553A8CC",
    "extraData": "0x5343",
    "gasLimit": "0x1388",
    "difficulty": "0x0",
    "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "coinbase": "0x0000000000000000000000000000000000000000",
    "alloc": {
        "0x6Be02d1d3665660d22FF9624b7BE0551ee1Ac91b": {
            "balance": "0x4a47e3c12448f4ad000000"
        }
    },
    "number": "0x0",
    "gasUsed": "0x0",
    "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "config": {
        "ethash": {},
        "chainId": 2600,
        "homesteadBlock": 0,
        "eip150Block": 0,
        "eip155Block": 0,
        "eip158Block": 0,
        "byzantiumBlock": 0,
        "constantinopleBlock": 0,
        "petersburgBlock": 0,
        "istanbulBlock": 0,
        "berlinBlock": 0,
        "londonBlock": 0,
        "terminalTotalDifficulty": 0,
        "terminalTotalDifficultyPassed": true,
        "shanghaiTime": 0
    }
}
"#;
    custom_genesis.to_string()
}

/// Fetches the head block from the database.
///
/// If the database is empty, returns the genesis block.
fn lookup_head(db: &DatabaseEnv, chain: Arc<ChainSpec>) -> eyre::Result<Head> {
    let factory = ProviderFactory::new(db, chain);
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

    assert_eq!(head, 0, "head should be genesis at block 0");

    Ok(Head {
        number: head,
        hash,
        difficulty: header.difficulty,
        total_difficulty,
        timestamp: header.timestamp,
    })
}

async fn build_network<C, Pool>(
    chain: Arc<ChainSpec>,
    executor: TaskExecutor,
    client: C,
    head: Head,
    pool: Pool,
) -> eyre::Result<NetworkHandle>
where
    C: BlockReader + HeaderProvider + Clone + Unpin + 'static,
    Pool: TransactionPool + Unpin + 'static,
{
    // build network config
    let args = NetworkArgs {
        discovery: DiscoveryArgs {
            disable_discovery: true,
            disable_dns_discovery: false,
            disable_discv4_discovery: false,
            addr: Ipv4Addr::UNSPECIFIED,
            port: 30303,
        },
        trusted_peers: vec![],
        trusted_only: true,
        bootnodes: None,
        peers_file: None,
        identity: "tn-batch-maker".to_string(),
        p2p_secret_key: None,
        no_persist_peers: true,
        nat: Default::default(),
        addr: Ipv4Addr::UNSPECIFIED,
        port: 30303,
        max_outbound_peers: None,
        max_inbound_peers: None,
    };

    let secret_key_path =
        tempfile::TempDir::new().expect("Failed to make tempdir for secret file").into_path();
    debug!("secret_key_path: {secret_key_path:?}");
    let secret_key = get_secret_key(&secret_key_path)?;
    let default_peers_path =
        tempfile::TempDir::new().expect("Failed to make tempdir for peers file").into_path();
    debug!("default_peers_path: {default_peers_path:?}");
    let network_config = args
        .network_config(&Config::default(), chain, secret_key, default_peers_path)
        .with_task_executor(Box::new(executor.clone()))
        .set_head(head)
        .listener_addr(SocketAddr::V4(SocketAddrV4::new(args.addr, args.port)))
        .discovery_addr(SocketAddr::V4(SocketAddrV4::new(args.addr, args.port)))
        .build(client);

    // start network
    let client = network_config.client.clone();
    let (handle, network, txpool, eth) = NetworkManager::builder(network_config)
        .await?
        .transactions(pool)
        .request_handler(client)
        .split_with_handle();

    executor.spawn_critical("p2p txpool", Box::pin(txpool));
    executor.spawn_critical("p2p eth request handler", Box::pin(eth));

    executor.spawn_critical_with_signal("p2p network task", |shutdown| async move {
        pin_mut!(network, shutdown);
        tokio::select! {
            _ = &mut network => {},
            _ = shutdown => {},
        }
    });

    Ok(handle)
}

fn spawn_payload_builder_service<Components>(
    components: Components,
) -> eyre::Result<PayloadBuilderHandle>
where
    Components: RethNodeComponents,
{
    let builder_args = PayloadBuilderArgs::default();
    let payload_job_config = BasicPayloadJobGeneratorConfig::default()
        .interval(builder_args.interval())
        .deadline(builder_args.deadline())
        .max_payload_tasks(builder_args.max_payload_tasks())
        .extradata(builder_args.extradata_rlp_bytes())
        .max_gas_limit(builder_args.max_gas_limit());

    let payload_builder = reth_basic_payload_builder::EthereumPayloadBuilder::default();

    let payload_generator = BasicPayloadJobGenerator::with_builder(
        components.provider(),
        components.pool(),
        components.task_executor(),
        payload_job_config,
        components.chain_spec(),
        payload_builder,
    );

    let (payload_service, payload_builder) = PayloadBuilderService::new(payload_generator);
    components.task_executor().spawn_critical("payload builder service", Box::pin(payload_service));

    Ok(payload_builder)
}

/// Constructs a [Pipeline] that's wired to the network
async fn build_networked_pipeline<DB, Client>(
    config: &Config,
    client: Client,
    consensus: Arc<dyn Consensus>,
    db: DB,
    task_executor: &TaskExecutor,
    metrics_tx: reth_stages::MetricEventsSender,
    chain: Arc<ChainSpec>,
    // prune_config: Option<PruneConfig>,
    // max_block: Option<BlockNumber>,
) -> eyre::Result<Pipeline<DB>>
where
    DB: Database + Unpin + Clone + 'static,
    Client: HeadersClient + BodiesClient + Clone + 'static,
{
    // building network downloaders using the fetch client
    let header_downloader = ReverseHeadersDownloaderBuilder::from(config.stages.headers)
        .build(client.clone(), Arc::clone(&consensus))
        .into_task_with(task_executor);

    let body_downloader = BodiesDownloaderBuilder::from(config.stages.bodies)
        .build(client, Arc::clone(&consensus), db.clone())
        .into_task_with(task_executor);

    let pipeline = build_pipeline(
        db,
        config,
        header_downloader,
        body_downloader,
        consensus,
        metrics_tx,
        chain,
        None, // max block
        None, // prune config
    )
    .await?;

    Ok(pipeline)
}

async fn build_pipeline<DB, H, B>(
    db: DB,
    config: &Config,
    header_downloader: H,
    body_downloader: B,
    consensus: Arc<dyn Consensus>,
    metrics_tx: reth_stages::MetricEventsSender,
    chain: Arc<ChainSpec>,
    max_block: Option<u64>,
    prune_config: Option<PruneConfig>,
) -> eyre::Result<Pipeline<DB>>
where
    DB: Database + Clone + 'static,
    H: HeaderDownloader + 'static,
    B: BodyDownloader + 'static,
{
    let stage_config = &config.stages;

    let mut builder = Pipeline::builder();

    if let Some(max_block) = max_block {
        debug!(target: "reth::cli", max_block, "Configuring builder to use max block");
        builder = builder.with_max_block(max_block)
    }

    let (tip_tx, _tip_rx) = watch::channel(B256::ZERO);
    use reth_revm_inspectors::stack::InspectorStackConfig;
    let factory = reth_revm::Factory::new(chain.clone());

    let stack_config = InspectorStackConfig { use_printer_tracer: true, hook: Hook::None };

    let factory = factory.with_stack_config(stack_config);

    // default prune modes
    let prune_modes = prune_config.map(|prune| prune.segments).unwrap_or_default();

    let pipeline = builder
        .with_tip_sender(tip_tx)
        .with_metrics_tx(metrics_tx.clone())
        .add_stages(
            DefaultStages::new(
                HeaderSyncMode::Continuous,
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
            .set(
                ExecutionStage::new(
                    factory,
                    ExecutionStageThresholds {
                        max_blocks: stage_config.execution.max_blocks,
                        max_changes: stage_config.execution.max_changes,
                        max_cumulative_gas: stage_config.execution.max_cumulative_gas,
                    },
                    stage_config
                        .merkle
                        .clean_threshold
                        .max(stage_config.account_hashing.clean_threshold)
                        .max(stage_config.storage_hashing.clean_threshold),
                    prune_modes.clone(),
                )
                .with_metrics_tx(metrics_tx),
            )
            .set(AccountHashingStage::new(
                stage_config.account_hashing.clean_threshold,
                stage_config.account_hashing.commit_threshold,
            ))
            .set(StorageHashingStage::new(
                stage_config.storage_hashing.clean_threshold,
                stage_config.storage_hashing.commit_threshold,
            ))
            .set(MerkleStage::new_execution(stage_config.merkle.clean_threshold))
            .set(TransactionLookupStage::new(
                stage_config.transaction_lookup.commit_threshold,
                prune_modes.transaction_lookup,
            ))
            .set(IndexAccountHistoryStage::new(
                stage_config.index_account_history.commit_threshold,
                prune_modes.account_history,
            ))
            .set(IndexStorageHistoryStage::new(
                stage_config.index_storage_history.commit_threshold,
                prune_modes.storage_history,
            )),
        )
        .build(db, chain);

    Ok(pipeline)
}

async fn start_rpc<Components>(components: Components) -> eyre::Result<RpcServerHandle>
where
    Components: RethNodeComponents,
{
    let genesis = genesis_string();
    let cmd = NodeCommand::<()>::try_parse_from(["reth", "--dev", "--chain", &genesis])?;
    let rpc_args = cmd.rpc;
    let handle = rpc_args
        .start_rpc_server(
            components.provider(),
            components.pool(),
            components.network(),
            components.task_executor(),
            components.events(),
        )
        .await?;

    Ok(handle)
}
