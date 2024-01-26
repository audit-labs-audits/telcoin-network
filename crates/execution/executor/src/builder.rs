//! Components that the execution layer needs to function normally.
//!
//! TODO: Organize this in a better place.
//!
//! reth creates these components in the CLI, but TN should consolidate
//! these methods into a heirarchical struct, like worker and primary

//! Maybe place inside builder?
//!
//! Things that the executor needs:
//!     - engine
//!     - pipeline
//!     - db
//!     - payload builder

use futures::pin_mut;
use reth::{
    args::{get_secret_key, NetworkArgs, PayloadBuilderArgs, RpcServerArgs},
    cli::{components::RethNodeComponents, config::PayloadBuilderConfig},
    rpc::builder::RpcServerHandle,
};
use reth_basic_payload_builder::{BasicPayloadJobGenerator, BasicPayloadJobGeneratorConfig};
use reth_config::{config::PruneConfig, Config};
use reth_db::database::Database;
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
use reth_network::{NetworkHandle, NetworkManager};
use reth_payload_builder::{PayloadBuilderHandle, PayloadBuilderService};
use reth_primitives::{stage::StageId, ChainSpec, Head, B256};
use reth_provider::{
    BlockHashReader, BlockReader, HeaderProvider, HeaderSyncMode, ProviderFactory,
    StageCheckpointReader,
};
use reth_revm_inspectors::stack::Hook;
use reth_stages::{
    sets::DefaultStages,
    stages::{
        AccountHashingStage, ExecutionStage, ExecutionStageThresholds, IndexAccountHistoryStage,
        IndexStorageHistoryStage, MerkleStage, SenderRecoveryStage, StorageHashingStage,
        TotalDifficultyStage, TransactionLookupStage,
    },
    Pipeline, StageSet,
};
use reth_tasks::{TaskExecutor, TaskSpawner};
use reth_transaction_pool::TransactionPool;
use std::{
    net::{SocketAddr, SocketAddrV4},
    sync::Arc,
};
use tokio::sync::watch;
use tracing::debug;

/// Fetches the head block from the database.
///
/// If the database is empty, returns the genesis block.
pub fn lookup_head<DB>(provider_factory: ProviderFactory<DB>) -> eyre::Result<Head>
where
    DB: Database + Clone,
{
    let provider = provider_factory.provider()?;

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

/// Build the network component for execution layer.
///
/// The network listens for a request from the engine
/// to download the canonical block that was built
/// by the EL executor.
pub async fn build_network<C, Pool>(
    chain: Arc<ChainSpec>,
    executor: TaskExecutor,
    client: C,
    head: Head,
    pool: Pool,
    network_args: &NetworkArgs,
) -> eyre::Result<NetworkHandle>
where
    C: BlockReader + HeaderProvider + Clone + Unpin + 'static,
    Pool: TransactionPool + Unpin + 'static,
{
    // TODO: using tempfile here since there should never be any peers
    let secret_key_path = tempfile::TempDir::new()
        .expect("Failed to make tempdir for secret file")
        .path()
        .join("secret_file");
    debug!("secret_key_path: {secret_key_path:?}");
    let secret_key = get_secret_key(&secret_key_path)?;
    let default_peers_path =
        tempfile::TempDir::new().expect("Failed to make tempdir for peers file").into_path();
    debug!("default_peers_path: {default_peers_path:?}");

    // network config
    let network_config = network_args
        .network_config(&Config::default(), chain, secret_key, default_peers_path)
        .with_task_executor(Box::new(executor.clone()))
        .set_head(head)
        .listener_addr(SocketAddr::V4(SocketAddrV4::new(network_args.addr, network_args.port)))
        .discovery_addr(SocketAddr::V4(SocketAddrV4::new(network_args.addr, network_args.port)))
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

    executor.spawn_critical_with_shutdown_signal("p2p network task", |shutdown| async move {
        pin_mut!(network, shutdown);
        tokio::select! {
            _ = &mut network => {},
            _ = shutdown => {},
        }
    });

    Ok(handle)
}

/// Construct a new payload builder.
pub fn spawn_payload_builder_service<Components>(
    components: Components,
    builder_args: &PayloadBuilderArgs,
) -> eyre::Result<PayloadBuilderHandle>
where
    Components: RethNodeComponents,
{
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

/// Constructs a [Pipeline] that's wired to the network.
pub async fn build_networked_pipeline<DB, Client>(
    config: &Config,
    client: Client,
    consensus: Arc<dyn Consensus>,
    provider_factory: ProviderFactory<DB>,
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
        .build(client, Arc::clone(&consensus), provider_factory.clone())
        .into_task_with(task_executor);

    let pipeline = build_pipeline(
        provider_factory,
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

/// Build the pipeline.
pub async fn build_pipeline<DB, H, B>(
    provider_factory: ProviderFactory<DB>,
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
    let factory = reth_revm::EvmProcessorFactory::new(chain.clone());

    let stack_config = InspectorStackConfig { use_printer_tracer: true, hook: Hook::None };

    let factory = factory.with_stack_config(stack_config);

    // default prune modes
    let prune_modes = prune_config.map(|prune| prune.segments).unwrap_or_default();

    let pipeline = builder
        .with_tip_sender(tip_tx)
        .with_metrics_tx(metrics_tx.clone())
        .add_stages(
            DefaultStages::new(
                provider_factory.clone(),
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
        .build(provider_factory);

    Ok(pipeline)
}

/// Start the RPC.
pub async fn start_rpc<Components>(
    components: Components,
    rpc_args: &RpcServerArgs,
) -> eyre::Result<RpcServerHandle>
where
    Components: RethNodeComponents,
{
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
