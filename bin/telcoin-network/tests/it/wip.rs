// Copyright (c) Telcoin, LLC

// use std::{
//     net::{Ipv4Addr, SocketAddr, SocketAddrV4},
//     sync::Arc,
// };

// use clap::Parser;
// use eyre::Context;
// use futures::{pin_mut, stream_select, StreamExt};
// use reth::{
//     args::{get_secret_key, DiscoveryArgs, NetworkArgs, PayloadBuilderArgs},
//     cli::{
//         components::{RethNodeComponents, RethNodeComponentsImpl},
//         config::PayloadBuilderConfig,
//     },
//     init::init_genesis,
//     node::{cl_events::ConsensusLayerHealthEvents, events, NodeCommand},
//     rpc::builder::RpcServerHandle,
// };
// use reth_auto_seal_consensus::{AutoSealBuilder, MiningMode};
// use reth_basic_payload_builder::{BasicPayloadJobGenerator, BasicPayloadJobGeneratorConfig};
// use reth_beacon_consensus::{
//     hooks::EngineHooks, BeaconConsensus, BeaconConsensusEngine, MIN_BLOCKS_FOR_PIPELINE_RUN,
// };
// use reth_blockchain_tree::{
//     BlockchainTree, BlockchainTreeConfig, ShareableBlockchainTree, TreeExternals,
// };
// use reth_config::{config::PruneConfig, Config};
// use reth_db::{database::Database, test_utils::create_test_rw_db, DatabaseEnv};
// use reth_downloaders::{
//     bodies::bodies::BodiesDownloaderBuilder,
//     headers::reverse_headers::ReverseHeadersDownloaderBuilder,
// };
// use reth_interfaces::{
//     consensus::Consensus,
//     p2p::{
//         bodies::{client::BodiesClient, downloader::BodyDownloader},
//         headers::{client::HeadersClient, downloader::HeaderDownloader},
//     },
// };
// use reth_network::{NetworkEvents, NetworkHandle, NetworkManager};
// use reth_payload_builder::{PayloadBuilderHandle, PayloadBuilderService};
// use reth_primitives::{stage::StageId, ChainSpec, Genesis, Head, B256};
// use reth_provider::{
//     providers::BlockchainProvider, BlockHashReader, BlockReader, CanonStateSubscriptions,
//     HeaderProvider, ProviderFactory, StageCheckpointReader,
// };
// use reth_revm::Factory;
// use reth_revm_inspectors::stack::Hook;
// use reth_stages::{
//     sets::DefaultStages,
//     stages::{
//         AccountHashingStage, ExecutionStage, ExecutionStageThresholds, HeaderSyncMode,
//         IndexAccountHistoryStage, IndexStorageHistoryStage, MerkleStage, SenderRecoveryStage,
//         StorageHashingStage, TotalDifficultyStage, TransactionLookupStage,
//     },
//     Pipeline, StageSet,
// };
// use reth_tasks::{TaskExecutor, TaskManager, TaskSpawner};
// use reth_tracing::init_test_tracing;
// use reth_transaction_pool::{
//     blobstore::InMemoryBlobStore, TransactionPool, TransactionValidationTaskExecutor,
// };
// use tokio::{
//     runtime::Handle,
//     sync::{mpsc::unbounded_channel, oneshot, watch},
// };
// use tracing::debug;

// #[tokio::test]
// async fn wip() -> eyre::Result<(), eyre::Error> {
//     init_test_tracing();

//     // lattice chain
//     let chain = custom_chain();
//     let db = create_test_rw_db();

//     debug!("db path: {:?}", db);

//     let genesis_hash = init_genesis(db.clone(), chain.clone())?;
//     debug!("genesis hash: {genesis_hash:?}");

//     // beacon consensus
//     // TODO: consider replacing trait methods with batch logic
//     let consensus: Arc<dyn Consensus> = Arc::new(BeaconConsensus::new(Arc::clone(&chain)));

//     // configure blockchain tree
//     let tree_externals = TreeExternals::new(
//         Arc::clone(&db),
//         Arc::clone(&consensus),
//         Factory::new(chain.clone()),
//         Arc::clone(&chain),
//     );

//     let tree = BlockchainTree::new(
//         tree_externals,
//         BlockchainTreeConfig::default(),
//         // prune_config.clone().map(|config| config.segments),
//         None,
//     )?;

//     let canon_state_notification_sender = tree.canon_state_notification_sender();
//     let blockchain_tree = ShareableBlockchainTree::new(tree);
//     debug!(target: "tn::cli", "configured blockchain tree");

//     // fetch the head block from the database
//     let head = lookup_head(db.db(), Arc::clone(&chain)).wrap_err("the head block is missing")?;

//     // task executor
//     let manager = TaskManager::new(Handle::current());
//     let task_executor = manager.executor();

//     // setup the blockchain provider
//     let factory = ProviderFactory::new(Arc::clone(&db), Arc::clone(&chain));
//     let blockchain_db = BlockchainProvider::new(factory, blockchain_tree.clone())?;
//     let blob_store = InMemoryBlobStore::default();
//     let validator = TransactionValidationTaskExecutor::eth_builder(Arc::clone(&chain))
//         .with_head_timestamp(head.timestamp)
//         // .kzg_settings(self.kzg_settings()?)
//         .with_additional_tasks(1)
//         .build_with_tasks(blockchain_db.clone(), task_executor.clone(), blob_store.clone());

//     // default pool config
//     let transaction_pool =
//         reth_transaction_pool::Pool::eth_pool(validator, blob_store, Default::default());
//     debug!(target: "tn::cli", "Transaction pool initialized");

//     // spawn txpool maintenance task
//     {
//         let pool = transaction_pool.clone();
//         let chain_events = blockchain_db.canonical_state_stream();
//         let client = blockchain_db.clone();
//         task_executor.spawn_critical(
//             "txpool maintenance task",
//             reth_transaction_pool::maintain::maintain_transaction_pool_future(
//                 client,
//                 pool,
//                 chain_events,
//                 task_executor.clone(),
//                 Default::default(),
//             ),
//         );
//         debug!(target: "tn::cli", "Spawned txpool maintenance task");
//     }
//     // network
//     let network_client = ProviderFactory::new(db.clone(), chain.clone());
//     let network = build_network(
//         chain.clone(),
//         task_executor.clone(),
//         network_client,
//         head,
//         transaction_pool.clone(),
//     )
//     .await?;

//     // components
//     let components = RethNodeComponentsImpl {
//         provider: blockchain_db.clone(),
//         pool: transaction_pool.clone(),
//         network: network.clone(),
//         task_executor: task_executor.clone(),
//         events: blockchain_db.clone(),
//     };

//     let payload_builder = spawn_payload_builder_service(components.clone())?;

//     let (sync_metrics_tx, _sync_metrics_rx) = unbounded_channel();
//     let (consensus_engine_tx, consensus_engine_rx) = unbounded_channel();

//     // default config for now
//     let config = Config::default();

//     // configure pipeline
//     let max_transactions = 5;
//     let mining_mode =
//         MiningMode::instant(max_transactions, transaction_pool.pending_transactions_listener());

//     let (_, client, mut task) = AutoSealBuilder::new(
//         Arc::clone(&chain),
//         blockchain_db.clone(),
//         transaction_pool.clone(),
//         consensus_engine_tx.clone(),
//         canon_state_notification_sender,
//         mining_mode,
//     )
//     .build();

//     let mut pipeline = build_networked_pipeline(
//         &config,
//         client.clone(),
//         Arc::clone(&consensus),
//         db.clone(),
//         &task_executor,
//         sync_metrics_tx,
//         chain.clone(),
//         // prune_config.clone()
//         // max_block
//     )
//     .await?;

//     let pipeline_events = pipeline.events();
//     task.set_pipeline_events(pipeline_events);
//     debug!(target: "tn::cli", "Spawning auto mine task");
//     task_executor.spawn(Box::pin(task));

//     let pipeline_events = pipeline.events();
//     let hooks = EngineHooks::new();

//     // Configure the consensus engine
//     let (beacon_consensus_engine, beacon_engine_handle) = BeaconConsensusEngine::with_channel(
//         client,
//         pipeline,
//         blockchain_db.clone(),
//         Box::new(task_executor.clone()),
//         Box::new(network.clone()),
//         None,  // max_block,
//         false, //self.debug.continuous,
//         payload_builder.clone(),
//         None, // initial_target,
//         MIN_BLOCKS_FOR_PIPELINE_RUN,
//         consensus_engine_tx,
//         consensus_engine_rx,
//         hooks,
//     )?;

//     debug!(target: "tn::cli", "Consensus engine initialized");

//     let events = stream_select!(
//         network.event_listener().map(Into::into),
//         beacon_engine_handle.event_listener().map(Into::into),
//         pipeline_events.map(Into::into),
//         ConsensusLayerHealthEvents::new(Box::new(blockchain_db.clone())).map(Into::into),
//     );

//     // events
//     task_executor.spawn_critical(
//         "events task",
//         events::handle_events(Some(network.clone()), Some(head.number), events),
//     );

//     // start rpc
//     let _rpc_server_handle = start_rpc(components).await?;

//     // Run consensus engine to completion
//     let (tx, rx) = oneshot::channel();
//     debug!(target: "reth::cli", "Starting consensus engine");
//     task_executor.spawn_critical_blocking("consensus engine", async move {
//         let res = beacon_consensus_engine.await;
//         let _ = tx.send(res);
//     });

//     rx.await??;

//     Ok(())
// }
