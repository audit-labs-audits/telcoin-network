//! Executor engine that creates batches and handles consensus output.

use crate::error::ExecutionError;
use consensus_metrics::metered_channel::{Receiver, Sender};
use narwhal_types::{execution_args, AuthorityIdentifier, ConsensusOutput, NewBatch, WorkerId};
use reth::{
    args::{NetworkArgs, RpcServerArgs},
    cli::components::RethNodeComponentsImpl,
    init::init_genesis,
    node::NodeCommand,
    rpc::builder::RpcServerHandle,
};
use reth_auto_seal_consensus::AutoSealConsensus;
use reth_beacon_consensus::{
    hooks::EngineHooks, BeaconConsensus, BeaconConsensusEngine, MIN_BLOCKS_FOR_PIPELINE_RUN,
};
use reth_blockchain_tree::{
    BlockchainTree, BlockchainTreeConfig, ShareableBlockchainTree, TreeExternals,
};
use reth_config::Config;
use reth_db::{database::Database, init_db, DatabaseEnv};
use reth_interfaces::{blockchain_tree::BlockchainTreeEngine, consensus::Consensus};
use reth_primitives::{Address, ChainSpec, Head};
use reth_provider::{
    providers::BlockchainProvider, BlockchainTreePendingStateProvider,
    CanonStateNotificationSender, CanonStateSubscriptions, ProviderFactory,
};
use reth_revm::EvmProcessorFactory;
use reth_rpc_types::engine::ForkchoiceState;
use reth_tasks::TaskManager;
use reth_transaction_pool::{
    blobstore::InMemoryBlobStore, noop::NoopTransactionPool, PoolConfig, TransactionPool,
    TransactionValidationTaskExecutor,
};
use std::{collections::HashMap, sync::Arc};
use tempfile::tempdir;
use tn_batch_maker::{BatchMakerBuilder, MiningMode};
use tn_batch_validator::BatchValidator;
use tn_executor::{
    build_network, build_networked_pipeline, lookup_head, spawn_payload_builder_service, start_rpc,
    Executor,
};
// cargo test --package narwhal-test-utils --lib -- cluster::cluster_tests::basic_cluster_setup
// --exact --nocapture
use tokio::{
    runtime::Handle,
    sync::{mpsc::unbounded_channel, RwLock},
};
use tracing::{debug, instrument, warn};

/// Inner type for holding execution layer types.
struct ExecutionNodeInner<DB, Tree> {
    /// The id for the authority within the network.
    authority_id: AuthorityIdentifier,
    /// The [Address] for the authority.
    ///
    /// The address refers to the execution layer's address
    /// based on the authority's secp256k1 public key.
    address: Address,
    // TODO: additional node values to track?
    //  - primary address?
    //  -
    /// Chain spec for execution.
    chain: Arc<ChainSpec>,
    /// Node command for constructing components.
    /// The struct is parsed from reth's CLI, and the
    /// ports are adjusted to prevent collisions.
    args: NodeCommand,
    /// The task manager wrapped as an option for spawning tasks to handle
    /// output from consensus.
    ///
    /// When the engine is started, a [TaskManager] is created to
    /// spawn tasks. When the engine is shutdown, the manager tries
    /// to gracefully shutdown (consumes `self`).
    engine_task_manager: Option<TaskManager>,
    // /// The task manager wrapped as an option for spawning tasks to handle
    // /// worker tasks..
    // ///
    // /// When a worker is started, a [TaskManager] is created to
    // /// spawn tasks. When the worker is shutdown, the manager tries
    // /// to gracefully shutdown (consumes `self`).
    // ///
    // /// TODO: only handles one worker for now.
    // worker_task_manager: Option<TaskManager>,
    /// Provider factory
    provider_factory: ProviderFactory<DB>,
    /// Auto-seal consensus used for batch making
    auto_consensus: Arc<dyn Consensus>,
    /// Blockchain database provider.
    blockchain_db: BlockchainProvider<DB, Tree>,
    /// Canon state notification sender
    canon_state_notification_sender: CanonStateNotificationSender,
    // /// Atomic boolean that's updated in the start/stop methods.
    // worker_is_running: Arc<AtomicBool>,
    /// Collection of execution components by worker.
    worker_components: HashMap<WorkerId, WorkerExecutionComponents>,
}

/// Collection of worker's execution components.
pub struct WorkerExecutionComponents {
    /// The task manager for the worker's tasks.
    task_manager: Option<TaskManager>,
    /// The worker's RPC handle.
    rpc_handle: RpcServerHandle,
    // TODO: consider more here?
    // - txpool
}

impl<DB, Tree> ExecutionNodeInner<DB, Tree>
where
    DB: Database + Clone + Unpin + 'static,
    Tree: BlockchainTreeEngine
        + CanonStateSubscriptions
        + BlockchainTreePendingStateProvider
        + Unpin
        + Clone
        + 'static,
{
    pub fn new(
        authority_id: AuthorityIdentifier,
        address: Address,
        chain: Arc<ChainSpec>,
        args: NodeCommand,
        engine_task_manager: Option<TaskManager>,
        provider_factory: ProviderFactory<DB>,
        auto_consensus: Arc<dyn Consensus>,
        blockchain_db: BlockchainProvider<DB, Tree>,
        canon_state_notification_sender: CanonStateNotificationSender,
    ) -> ExecutionNodeInner<DB, Tree> {
        // ) -> ExecutionNodeInner<Arc<DatabaseEnv>, ShareableBlockchainTree<Arc<DatabaseEnv>,
        // EvmProcessorFactory>> {
        Self {
            authority_id,
            address,
            chain,
            args,
            engine_task_manager,
            provider_factory,
            auto_consensus,
            blockchain_db,
            canon_state_notification_sender,
            worker_components: HashMap::default(),
        }
    }

    /// Initial adjustment for ports based on the authority's identifier.
    ///
    /// These values are unique per instace of `ExecutionNode`.
    ///
    /// Worker ports are further adjusted based on instance of worker.
    #[instrument(level = "info", skip_all)]
    fn adjust_engine_ports(&mut self, instance: u16) -> eyre::Result<()> {
        debug!("{} - adjusting ports for instance: {instance:?}", self.authority_id);
        // http port is scaled by a factor of -instance
        self.args.rpc.http_port -= instance - 1;
        debug!("{} - http port now: {:?}", self.args.rpc.http_port, self.authority_id);
        // ws port is scaled by a factor of instance * 2
        self.args.rpc.ws_port += instance * 2 - 2;
        debug!("{} - ws port now: {:?}", self.args.rpc.ws_port, self.authority_id);
        // network port is scaled by a factor of +instance
        self.args.network.port += instance - 1;
        debug!("{} - network port now: {:?}", self.args.network.port, self.authority_id);

        Ok(())
    }

    /// Further adjust the ports for the rpc and the network port based on instance of worker.
    ///
    /// Returns values based on the node's `args` field. Values are not mutated so adjustments
    /// per worker are consistent.
    #[instrument(level = "info", skip_all)]
    fn adjust_worker_ports(
        &mut self,
        instance: u16,
        rpc_args: &mut RpcServerArgs,
        network_args: &mut NetworkArgs,
        // network_args: &mut NetworkArgs,
    ) -> eyre::Result<()> {
        debug!("{} - adjusting ports for worker instance: {instance:?}", self.authority_id);
        // http port is scaled by a factor of -instance
        rpc_args.http_port -= instance * 100 - 100;
        debug!("{} - http port now: {:?}", self.args.rpc.http_port, self.authority_id);
        // ws port is scaled by a factor of instance * 2
        rpc_args.ws_port += instance * 100 - 100;
        debug!("{} - ws port now: {:?}", self.args.rpc.ws_port, self.authority_id);
        // network port is scaled by a factor of +instance
        // network_args.port -= 1;
        network_args.port += instance * 100;

        debug!("{} - network port now: {:?}", self.args.network.port, self.authority_id);

        Ok(())
    }

    /// Return a reference to the execution node's `BlockchainProvider`.
    fn get_provider(&self) -> BlockchainProvider<DB, Tree> {
        self.blockchain_db.clone()
    }

    // Return the worker's [SocketAddr] of the http server if started.
    /// Return the worker's [RpcServerHandle].
    fn worker_rpc_server_handle(
        &self,
        worker_id: &WorkerId,
    ) -> Result<RpcServerHandle, ExecutionError> {
        let handle = self
            .worker_components
            .get(worker_id)
            .ok_or(ExecutionError::WorkerNotFound(worker_id.to_owned()))?
            .rpc_handle
            .clone();
        // .http_local_addr()
        // .ok_or(ExecutionError::WorkerRPCNotRunning(worker_id.to_owned(), "http".to_string()))?;
        Ok(handle)
    }

    // /// Return a [TaskExecutor] for spawning tasks from the [TaskManager]'s runtime handle that's
    // /// associated with [`Self::start_engine`].
    // ///
    // /// Used to execute tasks to handle the primary's consensus output.
    // fn engine_task_executor(&mut self) -> eyre::Result<TaskExecutor, ExecutionError> {
    //     let task_manager =
    //         self.engine_task_manager.take().ok_or(ExecutionError::TaskManagerNotStarted)?;
    //     let executor = task_manager.executor();
    //     self.engine_task_manager = Some(task_manager);
    //     Ok(executor)
    // }

    // /// Return a [TaskExecutor] for spawning tasks from the [TaskManager]'s runtime handle that's
    // /// associated with [`Self::start_batch_maker`] and [`Self::start_batch_validator`].
    // ///
    // /// Used to execute tasks to handle for worker's execution.
    // fn worker_task_executor(
    //     &mut self,
    //     worker_id: &WorkerId,
    // ) -> eyre::Result<TaskExecutor, ExecutionError> {
    //     // let task_manager =
    //     // self.worker_task_manager.take().ok_or(ExecutionError::TaskManagerNotStarted)?;
    //     // let executor = task_manager.executor();
    //     // self.engine_task_manager = Some(task_manager);
    //     let task_manager = self
    //         .worker_components
    //         .get(worker_id)
    //         .ok_or(ExecutionError::WorkerNotFound(worker_id.to_owned()))?
    //         .task_manager
    //         .as_ref()
    //         .ok_or(ExecutionError::TaskManagerNotStarted)?;
    //     let executor = task_manager.executor();
    //     Ok(executor)
    // }

    /// Spawn tasks associated with executing output from consensus.
    ///
    /// The method is consumed by [PrimaryNodeInner::start].
    /// All tasks are spawned with the [ExecutionNodeInner]'s [TaskManager].
    #[instrument(level = "info", skip_all)]
    async fn start_engine(
        &mut self,
        from_consensus: Receiver<ConsensusOutput>,
        // ) -> eyre::Result<Vec<JoinHandle<()>>, ExecutionError> {
    ) -> eyre::Result<(), ExecutionError> {
        //==============
        //=== EXECUTOR
        //==============

        // create task manager
        let task_manager = TaskManager::new(Handle::current());
        let task_executor = task_manager.executor();

        self.engine_task_manager = Some(task_manager);

        // build rest of the components for executor
        let head: Head = lookup_head(self.provider_factory.clone())?;

        // network
        let network = build_network(
            self.chain.clone(),
            task_executor.clone(),
            self.provider_factory.clone(),
            head,
            NoopTransactionPool::default(),
            &self.args.network,
        )
        .await?;

        // engine channel
        let (to_engine, from_engine) = unbounded_channel();

        // build executor
        let (_, client, mut task) = Executor::new(
            Arc::clone(&self.chain),
            self.blockchain_db.clone(),
            from_consensus,
            to_engine.clone(),
            self.canon_state_notification_sender.clone(),
        )
        .build();

        let config = Config::default();
        let (metrics_tx, _sync_metrics_rx) = unbounded_channel();

        // pipeline
        let mut pipeline = build_networked_pipeline(
            &config,
            client.clone(),
            Arc::clone(&self.auto_consensus),
            self.provider_factory.clone(),
            &task_executor,
            metrics_tx,
            Arc::clone(&self.chain),
        )
        .await?;

        // TODO: is this necessary?
        let pipeline_events = pipeline.events();
        task.set_pipeline_events(pipeline_events);

        // spawn engine
        let hooks = EngineHooks::new();
        let components = RethNodeComponentsImpl {
            provider: self.blockchain_db.clone(),
            pool: NoopTransactionPool::default(),
            network: network.clone(),
            task_executor: task_executor.clone(),
            events: self.blockchain_db.clone(),
        };

        // TODO: send forkchoice update for genesis to finalize block
        let payload_builder = spawn_payload_builder_service(components, &self.args.builder)?;

        let (beacon_consensus_engine, beacon_engine_handle) = BeaconConsensusEngine::with_channel(
            client.clone(),
            pipeline,
            self.blockchain_db.clone(),
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
        )?;

        // let mut handles = vec![];

        // TODO: should handles be collected?
        // spawn critical task vs shutdown vs with signal from primary/worker tasks?

        // spawn task to execute consensus output
        task_executor.spawn_critical("worker minint task", Box::pin(task));
        // handles.push(task_executor.spawn_critical("worker mining task", Box::pin(task)));

        debug!("awaiting beacon engine task...");

        // spawn beacon engine
        task_executor.spawn_critical_blocking("consensus engine", async move {
            let _res = beacon_consensus_engine.await;
            // TODO: return oneshot channel here?
        });

        // handles.push(engine_task);

        // wait for engine to spawn
        tokio::task::yield_now().await;

        // finalize genesis
        let genesis_hash = self.chain.genesis_hash();
        let genesis_state = ForkchoiceState {
            head_block_hash: genesis_hash,
            finalized_block_hash: genesis_hash,
            safe_block_hash: genesis_hash,
        };

        // send forkchoice for genesis to finalize
        let res = beacon_engine_handle.fork_choice_updated(genesis_state, None).await?;

        debug!("genesis finalized: {res:?}");

        // TODO: return handles here?
        // Ok(handles)
        Ok(())
    }

    #[instrument(level = "info", skip_all)]
    async fn start_batch_maker(
        &mut self,
        to_worker: Sender<NewBatch>,
        worker_id: WorkerId,
    ) -> Result<(), ExecutionError> {
        // create task manager - owns the spawned tasks
        let task_manager = TaskManager::new(Handle::current());
        let task_executor = task_manager.executor();

        let blob_store = InMemoryBlobStore::default();
        let validator = TransactionValidationTaskExecutor::eth_builder(Arc::clone(&self.chain))
            .with_head_timestamp(self.chain.genesis_timestamp())
            .with_additional_tasks(1)
            .build_with_tasks(
                self.blockchain_db.clone(),
                task_executor.clone(),
                blob_store.clone(),
            );

        // one txpool per worker for now
        let txpool =
            reth_transaction_pool::Pool::eth_pool(validator, blob_store, PoolConfig::default());

        //==============
        //=== BATCH
        //=== MAKER
        //==============

        // build batch maker
        let max_transactions = 10;
        let mining_mode =
            MiningMode::instant(max_transactions, txpool.pending_transactions_listener());
        let (_consensus, _client, task) = BatchMakerBuilder::new(
            Arc::clone(&self.chain),
            self.blockchain_db.clone(),
            txpool.clone(),
            to_worker,
            mining_mode,
            self.address.clone(),
        )
        .build();

        // spawn batch maker mining task
        task_executor.spawn_critical("batch maker", task);

        // clone the adjusted execution ports to further adjust on a per-worker basis
        let mut rpc_args = self.args.rpc.clone();

        // TODO: impl custom network type for workers
        // for now, moving forward with this approach bc NetworkArgs aren't cloneable
        let mut network_args = execution_args().network; // network args aren't cloneable
                                                         //let mut network_args = NetworkArgs::default(); // network args aren't cloneable
                                                         // update adjusted port
        network_args.port = self.args.network.port; // default == 30303
                                                    // +1 for instance - adjust ports per worker instance
        self.adjust_worker_ports(worker_id + 1, &mut rpc_args, &mut network_args)?;

        let head: Head = lookup_head(self.provider_factory.clone())?;
        let network = build_network(
            self.chain.clone(),
            task_executor.clone(),
            self.provider_factory.clone(),
            head,
            txpool.clone(),
            &network_args,
        )
        .await?;

        // spawn rpc to receive txs
        let components = RethNodeComponentsImpl {
            provider: self.blockchain_db.clone(),
            pool: txpool,
            // TODO: create custom network type for workers
            //
            // so rpc returns correct information
            network,
            task_executor,
            events: self.blockchain_db.clone(),
        };

        let rpc_handle = start_rpc(components, &self.args.rpc).await?;

        // store the worker's execution info
        let worker_components =
            WorkerExecutionComponents { task_manager: Some(task_manager), rpc_handle };

        self.worker_components.insert(worker_id, worker_components);

        Ok(())
    }

    /// Create a new batch validator.
    fn new_batch_validator(&self) -> BatchValidator<DB, Tree> {
        // let blockchain_tree = self.blockchain_db.clone();
        let consensus: Arc<dyn Consensus> = Arc::new(BeaconConsensus::new(self.chain.clone()));
        BatchValidator::new(
            consensus,
            EvmProcessorFactory::new(self.chain.clone()),
            self.blockchain_db.clone(),
        )
    }

    /// Drop the engine [TaskManager].
    ///
    /// Called with the primary node's shutdown.
    fn shutdown_engine(&mut self) {
        if let Some(task_manager) = self.engine_task_manager.take() {
            drop(task_manager);
        }
    }

    /// Drop the worker's [TaskManager].
    ///
    /// Called with the worker node's shutdown.
    fn shutdown_worker(&mut self, worker_id: &WorkerId) -> Result<(), ExecutionError> {
        // try to find the worker
        let mut worker = self
            .worker_components
            .remove(worker_id)
            // better to just return here?
            .ok_or(ExecutionError::WorkerNotFound(worker_id.to_owned()))?;

        // drop the task manager
        if let Some(task_manager) = worker.task_manager.take() {
            drop(task_manager);
        }

        Ok(())
    }

    /// [TaskManager] tries to gracefully shutdown and consumes `self` leaving `None` in it's place.
    ///
    /// Called with the worker node's shutdown.
    fn shutdown_all_workers(&mut self) {
        // loop through all worker components
        for worker in self.worker_components.values_mut() {
            // drop the task manager
            if let Some(task_manager) = worker.task_manager.take() {
                drop(task_manager);
            }
        }
    }

    /// Shutdown all task manager tasks.
    fn shutdown_all(&mut self) {
        self.shutdown_engine();
        self.shutdown_all_workers();
    }

    /// Atomic boolean that's updated on start/shutdown methods
    fn engine_is_running(&self) -> bool {
        self.engine_task_manager.is_some()
    }

    /// Atomic boolean that's updated on start/shutdown methods
    fn worker_is_running(&self, worker_id: &WorkerId) -> bool {
        if let Some(_) = self.worker_components.get(worker_id) {
            return true;
        }

        // workers are cleared when shutdown
        false
    }

    /// Atomic boolean that's updated on start/shutdown methods
    fn any_workers_running(&self) -> bool {
        // TODO: this may not be the most accurate
        //
        // if a critical task fails, how will it propogate?
        self.worker_components.values().any(|w| w.task_manager.is_some())
    }
}

#[derive(Clone)]
pub struct ExecutionNode {
    internal: Arc<
        RwLock<
            ExecutionNodeInner<
                Arc<DatabaseEnv>,
                ShareableBlockchainTree<Arc<DatabaseEnv>, EvmProcessorFactory>,
            >,
        >,
    >,
}

impl ExecutionNode
// where
//     DB: Database,
//     Tree: BlockchainTreeViewer,
{
    pub fn new(
        authority_id: AuthorityIdentifier,
        chain: Arc<ChainSpec>,
        address: Address,
    ) -> Result<Self, ExecutionError> {
        // TODO: why doesn't this generic work?
        // ) -> Result<ExecutionNode<DB, Tree>, ExecutionError> { // TODO: why doesn't this generic
        // work? ) -> Result<ExecutionNode<Arc<DatabaseEnv>,
        // ShareableBlockchainTree<Arc<DatabaseEnv>, EvmProcessorFactory>>, ExecutionError> {
        // setup EL using test db

        let args = execution_args();
        let db_path = tempdir()?;
        let db_log_level = None; // LogLevel::Fatal ?
        let db = Arc::new(init_db(&db_path, db_log_level)?.with_metrics());
        let genesis_hash = init_genesis(db.clone(), chain.clone())?;

        debug!(target: "execution_node", ?genesis_hash);

        let auto_consensus: Arc<dyn Consensus> =
            Arc::new(AutoSealConsensus::new(Arc::clone(&chain)));

        // neither one of these approaches works
        let provider_factory = ProviderFactory::new(Arc::clone(&db), Arc::clone(&chain));
        // let provider_factory: ProviderFactory<DB> = ProviderFactory::new(Arc::clone(&db),
        // Arc::clone(&chain));

        // configure blockchain tree
        let tree_externals = TreeExternals::new(
            provider_factory.clone(),
            Arc::clone(&auto_consensus),
            EvmProcessorFactory::new(chain.clone()),
        );

        // TODO: add prune config for full node
        let tree = BlockchainTree::new(
            tree_externals,
            BlockchainTreeConfig::default(), // default is more than enough
            None,                            // TODO: prune config
        )?;

        let canon_state_notification_sender = tree.canon_state_notification_sender();

        let blockchain_tree = ShareableBlockchainTree::new(tree);

        // provider
        let blockchain_db =
            BlockchainProvider::new(provider_factory.clone(), blockchain_tree.clone())?;

        // let inner: ExecutionNodeInner<DB, Tree> = ExecutionNodeInner {
        let mut inner = ExecutionNodeInner::new(
            authority_id,
            address,
            chain,
            args,
            None,
            provider_factory,
            auto_consensus,
            blockchain_db,
            canon_state_notification_sender,
        );

        // use inner u16 for instance
        let instance = authority_id.0 + 1;
        inner.adjust_engine_ports(instance as u16)?;

        Ok(ExecutionNode { internal: Arc::new(RwLock::new(inner)) })
    }

    pub async fn start_engine(
        &self,
        rx_notifier: Receiver<ConsensusOutput>,
        // ) -> Result<Vec<JoinHandle<()>>, ExecutionError>
    ) -> Result<(), ExecutionError>
where {
        let mut guard = self.internal.write().await;
        guard.start_engine(rx_notifier).await
    }

    pub async fn start_batch_maker(
        &self,
        to_worker: Sender<NewBatch>,
        worker_id: WorkerId,
    ) -> Result<(), ExecutionError>
where {
        let mut guard = self.internal.write().await;
        guard.start_batch_maker(to_worker, worker_id).await
    }

    /// Return batch validator for worker.
    pub async fn new_batch_validator(
        &self, // ) -> Result<BatchValidator<DB, Tree>, ExecutionError> {
    ) -> Result<
        BatchValidator<
            Arc<DatabaseEnv>,
            ShareableBlockchainTree<Arc<DatabaseEnv>, EvmProcessorFactory>,
        >,
        ExecutionError,
    > {
        let guard = self.internal.read().await;
        let batch_validator = guard.new_batch_validator();
        Ok(batch_validator)
    }

    /// Try to gracefully shutdown engine tasks.
    pub async fn shutdown_engine(&self) {
        let mut guard = self.internal.write().await;
        guard.shutdown_engine()
    }

    /// Try to gracefully shutdown worker tasks.
    pub async fn shutdown_worker(&self, worker_id: &WorkerId) -> Result<(), ExecutionError> {
        let mut guard = self.internal.write().await;
        guard.shutdown_worker(worker_id)
    }

    /// Try to gracefully shutdown all tasks.
    pub async fn shutdown_all(&self) {
        let mut guard = self.internal.write().await;
        guard.shutdown_all()
    }

    /// Read the atomic bool for engine tasks.
    pub async fn engine_is_running(&self) -> bool {
        let guard = self.internal.read().await;
        guard.engine_is_running()
    }

    /// Read the atomic bool for worker tasks.
    ///
    /// TODO: this only supports one worker for now.
    pub async fn worker_is_running(&self, worker_id: &WorkerId) -> bool {
        let guard = self.internal.read().await;
        guard.worker_is_running(worker_id)
    }

    /// Read the atomic bool for worker tasks.
    ///
    /// TODO: this only supports one worker for now.
    pub async fn any_workers_running(&self) -> bool {
        let guard = self.internal.read().await;
        guard.any_workers_running()
    }

    /// Return a reference to the execution node's `BlockchainProvider`.
    /// TODO: why don't generics work here?
    pub async fn get_provider(
        &self,
    ) -> BlockchainProvider<
        Arc<DatabaseEnv>,
        ShareableBlockchainTree<Arc<DatabaseEnv>, EvmProcessorFactory>,
    >
// DB: Database + Clone + Unpin + 'static,
    // Tree: BlockchainTreeEngine
    //     + CanonStateSubscriptions
    //     + BlockchainTreePendingStateProvider
    //     + Unpin
    //     + Clone
    //     + 'static,
    {
        let guard = self.internal.read().await;
        guard.get_provider()
    }
    // TODO: maybe just return latest block?
    //
    // pub async fn blockchain_db(&self) -> BlockchainProvider<DB, Tree> {
    //     let mut guard = self.internal.read().await;
    //     guard.blockchain_db.clone()
    // }

    /// Return an HTTP client for submitting transactions to the RPC.

    /// Return a handle to the worker's RPC server. See [RpcServerHandle] for more methods.
    pub async fn worker_rpc_server_handle(
        &self,
        worker_id: &WorkerId,
    ) -> Result<RpcServerHandle, ExecutionError> {
        let guard = self.internal.read().await;
        guard.worker_rpc_server_handle(worker_id)
    }

    /// Return an HTTP client for submitting transactions to the RPC.
    pub async fn worker_http_client(
        &self,
        worker_id: &WorkerId,
    ) -> Result<Option<jsonrpsee::http_client::HttpClient>, ExecutionError> {
        let handle = self.worker_rpc_server_handle(worker_id).await?;
        Ok(handle.http_client())
    }
}

#[cfg(test)]
mod tests {
    use super::ExecutionNode;
    use anemo::Response;
    use assert_matches::assert_matches;
    use consensus_metrics::metered_channel;
    use fastcrypto::{hash::Hash, traits::KeyPair};
    use jsonrpsee::{core::client::ClientT, rpc_params};
    use narwhal_network::client::NetworkClient;
    use narwhal_network_types::{FetchBatchesResponse, MockPrimaryToWorker};
    use narwhal_primary::{
        consensus::{
            make_certificate_store, make_consensus_store, Bullshark, ConsensusMetrics,
            ConsensusRound, LeaderSchedule, LeaderSwapTable, NUM_SUB_DAGS_PER_SCHEDULE,
        },
        NUM_SHUTDOWN_RECEIVERS,
    };
    use narwhal_types::{
        test_utils::{batch, get_gas_price, CommitteeFixture, TransactionFactory},
        yukon_chain_spec, yukon_genesis, AuthorityIdentifier, BatchAPI, Certificate,
        ConsensusOutput, PreSubscribedBroadcastSender,
    };
    use prometheus::Registry;
    use reth::{
        args::NetworkArgs,
        rpc::builder::constants::{DEFAULT_HTTP_RPC_PORT, DEFAULT_WS_RPC_PORT},
    };
    use reth_primitives::{
        alloy_primitives::U160, Address, ChainSpec, GenesisAccount, TransactionSigned, B256, U256,
    };
    use reth_provider::{BlockReaderIdExt, CanonStateNotification, CanonStateSubscriptions};
    use reth_tasks::TaskManager;
    use reth_tracing::init_test_tracing;
    use std::{
        collections::{BTreeSet, HashMap},
        str::FromStr,
        sync::Arc,
        time::Duration,
    };
    use tokio::{
        runtime::Handle,
        sync::{mpsc::error::TryRecvError, watch},
        time::timeout,
    };
    use tracing::debug;

    #[tokio::test]
    async fn test_rpc_ports_adjust() -> eyre::Result<()> {
        // assert defaults for instance `1`
        let genesis = yukon_genesis();
        let chain: Arc<ChainSpec> = Arc::new(genesis.into());
        // address doesn't affect these tests
        let address = Address::ZERO;

        let node = ExecutionNode::new(AuthorityIdentifier(0), chain.clone(), address.clone())?;
        let mut inner = node.internal.write().await;
        assert_eq!(inner.args.rpc.http_port, DEFAULT_HTTP_RPC_PORT);
        assert_eq!(inner.args.rpc.ws_port, DEFAULT_WS_RPC_PORT);
        assert_eq!(inner.args.network.port, 30303);
        // clone args - simulate `ExecutionNode::start_batch_maker()`
        let mut worker0_rpc_args = inner.args.rpc.clone();
        let mut worker0_network_args = NetworkArgs::default();
        // start_batch_maker adds +1 to worker instance
        inner.adjust_worker_ports(1, &mut worker0_rpc_args, &mut worker0_network_args)?;
        // assert nodes args didn't change
        assert_eq!(inner.args.rpc.http_port, DEFAULT_HTTP_RPC_PORT);
        assert_eq!(inner.args.rpc.ws_port, DEFAULT_WS_RPC_PORT);
        assert_eq!(inner.args.network.port, 30303);
        // assert cloned args changed for worker instance
        assert_eq!(worker0_rpc_args.http_port, DEFAULT_HTTP_RPC_PORT);
        assert_eq!(worker0_rpc_args.ws_port, DEFAULT_WS_RPC_PORT);
        // must be different than engine's network port
        assert_ne!(worker0_network_args.port, 30303);

        // test another worker
        let mut worker1_rpc_args = inner.args.rpc.clone();
        let mut worker1_network_args = NetworkArgs::default();
        worker1_network_args.port = inner.args.network.port;
        inner.adjust_worker_ports(2, &mut worker1_rpc_args, &mut worker1_network_args)?;
        // assert nodes args didn't change
        assert_eq!(inner.args.rpc.http_port, DEFAULT_HTTP_RPC_PORT);
        assert_eq!(inner.args.rpc.ws_port, DEFAULT_WS_RPC_PORT);
        assert_eq!(inner.args.network.port, 30303);
        // assert cloned args changed for worker instance
        assert_eq!(worker1_rpc_args.http_port, DEFAULT_HTTP_RPC_PORT - 100);
        assert_eq!(worker1_rpc_args.ws_port, DEFAULT_WS_RPC_PORT + 100);
        assert_eq!(worker1_network_args.port, 30303 + 2 * 100);

        // assert rpc ports adjusted for instance `2`
        let node = ExecutionNode::new(AuthorityIdentifier(1), chain.clone(), address.clone())?;
        let inner = node.internal.read().await;
        assert_eq!(inner.args.rpc.http_port, 8544);
        assert_eq!(inner.args.rpc.ws_port, 8548);
        assert_eq!(inner.args.network.port, 30304);

        // assert rpc ports adjusted for instance `3`
        let node = ExecutionNode::new(AuthorityIdentifier(2), chain.clone(), address.clone())?;
        let inner = node.internal.read().await;
        assert_eq!(inner.args.rpc.http_port, 8543);
        assert_eq!(inner.args.rpc.ws_port, 8550);
        assert_eq!(inner.args.network.port, 30305);

        // assert rpc ports adjusted for instance `4`
        let node = ExecutionNode::new(AuthorityIdentifier(3), chain.clone(), address.clone())?;
        let inner = node.internal.read().await;
        assert_eq!(inner.args.rpc.http_port, 8542);
        assert_eq!(inner.args.rpc.ws_port, 8552);
        assert_eq!(inner.args.network.port, 30306);

        Ok(())
    }

    #[tokio::test]
    async fn test_shutdowns() -> eyre::Result<()> {
        let genesis = yukon_genesis();
        let chain: Arc<ChainSpec> = Arc::new(genesis.into());
        let address = Address::ZERO;

        let node = ExecutionNode::new(AuthorityIdentifier(0), chain.clone(), address)?;
        let (tx_notifier, rx_notifier) = narwhal_types::test_channel!(1);
        node.start_engine(rx_notifier).await?;

        // atomic bool
        assert!(node.engine_is_running().await);

        // ensure channel is not closed
        assert!(!tx_notifier.is_closed());

        // shutdown engine
        node.shutdown_engine().await;

        let timeout = std::time::Duration::from_secs(3);
        let sleep_duration = std::time::Duration::from_millis(100);
        // ensure engine channel closes after some time
        tokio::time::timeout(timeout, async move {
            // if channel isn't closed, sleep then check again
            while !tx_notifier.is_closed() {
                tokio::time::sleep(sleep_duration).await;
            }
        })
        .await?;

        assert!(!node.engine_is_running().await);

        // test worker shutdown
        let worker_id = 0;
        let (to_worker, mut from_worker) = narwhal_types::test_channel!(1);
        node.start_batch_maker(to_worker.clone(), worker_id).await?;
        drop(to_worker);

        // atomic bool
        assert!(node.worker_is_running(&worker_id).await);
        assert!(node.any_workers_running().await);

        // Disconnected
        if let Err(error) = from_worker.try_recv() {
            assert_eq!(TryRecvError::Empty, error);
        } else {
            panic!("from_worker channel received an unexpected message");
        }

        // shutdown worker
        assert!(node.shutdown_worker(&worker_id).await.is_ok());
        assert!(!node.worker_is_running(&worker_id).await);

        // ensure worker batch channel is disconnected
        tokio::time::timeout(timeout, async move {
            // if channel isn't closed, sleep then check again
            loop {
                if let Err(error) = from_worker.try_recv() {
                    match error {
                        TryRecvError::Empty => tokio::time::sleep(sleep_duration).await,
                        // all senders are dropped
                        TryRecvError::Disconnected => {
                            println!("\n disconnected...");
                            break
                        }
                    }
                }
            }
        })
        .await?;

        // assert!(!node.worker_is_running().await);

        Ok(())
    }

    /// Helper to run consensus to the point of committing one leader.
    ///
    /// Based on `bullshark_tests::commit_one()`.
    ///
    /// Copied from EL `Executor` it tests. Returning receiver intsead of consensus output.
    async fn commit_one() -> ConsensusOutput {
        let fixture = CommitteeFixture::builder().build();
        let committee = fixture.committee();
        let registry = Registry::new();
        // Make certificates for rounds 1 and 2.
        let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();
        let genesis =
            Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
        let (mut certificates, next_parents) =
            narwhal_types::test_utils::make_optimal_certificates(&committee, 1..=2, &genesis, &ids);

        // Make two certificate (f+1) with round 3 to trigger the commits.
        let (_, certificate) = narwhal_types::test_utils::mock_certificate(
            &committee,
            ids[0],
            3,
            next_parents.clone(),
        );
        certificates.push_back(certificate);
        let (_, certificate) =
            narwhal_types::test_utils::mock_certificate(&committee, ids[1], 3, next_parents);
        certificates.push_back(certificate);

        // Spawn the consensus engine and sink the primary channel.
        let (tx_new_certificates, rx_new_certificates) = narwhal_types::test_channel!(1);
        let (tx_primary, mut rx_primary) = narwhal_types::test_channel!(1);
        let (tx_sequence, rx_sequence) = narwhal_types::test_channel!(1);
        let (tx_consensus_round_updates, _rx_consensus_round_updates) =
            watch::channel(ConsensusRound::new(0, 0));

        let mut tx_shutdown = PreSubscribedBroadcastSender::new(NUM_SHUTDOWN_RECEIVERS);

        let store = make_consensus_store(&narwhal_types::test_utils::temp_dir());
        let cert_store = make_certificate_store(&narwhal_types::test_utils::temp_dir());
        let gc_depth = 50;
        let metrics = Arc::new(ConsensusMetrics::new(&registry));

        let _bad_nodes_stake_threshold = 0;
        let bullshark = Bullshark::new(
            committee.clone(),
            store.clone(),
            metrics.clone(),
            NUM_SUB_DAGS_PER_SCHEDULE,
            LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
            0,
        );

        let _consensus_handle = narwhal_primary::consensus::Consensus::spawn(
            committee.clone(),
            gc_depth,
            store,
            cert_store,
            tx_shutdown.subscribe(),
            rx_new_certificates,
            tx_primary,
            tx_consensus_round_updates,
            tx_sequence,
            bullshark,
            metrics,
        );
        tokio::spawn(async move { while rx_primary.recv().await.is_some() {} });

        // Feed all certificates to the consensus. Only the last certificate should trigger
        // commits, so the task should not block.
        while let Some(certificate) = certificates.pop_front() {
            tx_new_certificates.send(certificate).await.unwrap();
        }

        // // Ensure the first 4 ordered certificates are from round 1 (they are the parents of the
        // // committed leader); then the leader's certificate should be committed.
        // let committed_sub_dag: CommittedSubDag = rx_output.recv().await.unwrap();
        // let mut sequence = committed_sub_dag.certificates.into_iter();
        // for _ in 1..=4 {
        //     let output = sequence.next().unwrap();
        //     assert_eq!(output.round(), 1);
        // }
        // let output = sequence.next().unwrap();
        // assert_eq!(output.round(), 2);

        // // AND the reputation scores have not been updated
        // assert_eq!(committed_sub_dag.reputation_score.total_authorities(), 4);
        // assert!(committed_sub_dag.reputation_score.all_zero());

        let primary = fixture.authorities().next().expect("first authority in fixture");
        let authority_id = primary.id();

        // mock worker
        let worker_id = 0;
        let worker = primary.worker(worker_id);
        let worker_peer_id = anemo::PeerId(worker.keypair().public().0.to_bytes());
        let mut mock_server = MockPrimaryToWorker::new();
        // create new batch here since the test is only for sealing whatever comes out of consensus
        mock_server.expect_fetch_batches().returning(move |request| {
            let digests = request.into_inner().digests;
            let mut batches = HashMap::new();
            for digest in digests {
                // this creates a new batch that's different than
                // the one created with the Certificate
                //
                // althought these are different, it is sufficient for mock server
                // bc the test is just executing whatever comes out of consensus
                let _ = batches.insert(digest, batch());
            }
            let response = FetchBatchesResponse { batches };
            Ok(Response::new(response))
        });

        let client = NetworkClient::new_from_keypair(&primary.network_keypair());
        client.set_primary_to_worker_local_handler(worker_peer_id, Arc::new(mock_server));
        // let execution_state = SimpleState
        let metrics = narwhal_executor::ExecutorMetrics::new(&registry);

        // TODO: this is what connects the EL and CL
        let (tx_notifier, mut rx_notifier) =
            metered_channel::channel(narwhal_primary::CHANNEL_CAPACITY, &metrics.tx_notifier);

        // Spawn the client executing the transactions. It can also synchronize with the
        // subscriber handler if it missed some transactions.
        let _executor_handles = narwhal_executor::Executor::spawn(
            authority_id,
            fixture.worker_cache(),
            committee,
            client,
            // execution_state,
            // vec![tx_shutdown.subscribe(), tx_shutdown.subscribe()], // this is dumb
            tx_shutdown.subscribe(),
            rx_sequence,
            vec![], // restored_consensus_output, // pass empty vec for now
            tx_notifier,
            metrics, // TODO: kinda sloppy
        )
        .expect("executor handles spawned successfully");

        // Wait till other services have been able to start up
        tokio::task::yield_now().await;

        rx_notifier.recv().await.expect("output received")
    }

    /// TODO: moving on from this test for now - TEST FAILS.
    ///
    /// Comparing the sealed header produced by the Executor does not match
    /// the finalized header pulled from the database. Specifically, roots (state, tx, etc)
    /// are zero. The test passes sometimes, so I'm going to assume it's a race condition for now.
    ///
    /// Worst case scenario is I ditch the beacon engine all together and commit to the DB directly
    /// using the provider factory or the concept of the shareable blockchain tree.
    #[tokio::test]
    async fn test_consensus_output() -> eyre::Result<()> {
        init_test_tracing();

        let consensus_output = commit_one().await;
        debug!("output received. starting engine...");

        // TODO: better way to do this?
        // seed genesis with output from consensus
        let genesis = yukon_genesis();

        // collect txs and addresses for later assertions
        let mut txs_in_output = vec![];
        let mut senders_in_output = vec![];
        let mut accounts_to_seed = Vec::new();
        // loop through output
        for batches in consensus_output.clone().batches {
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
        }
        debug!("accounts to seed: {accounts_to_seed:?}");

        // genesis
        let genesis = genesis.extend_accounts(accounts_to_seed);
        let chain: Arc<ChainSpec> = Arc::new(genesis.into());
        let address = Address::ZERO;

        debug!("creating execution node..");

        let node = ExecutionNode::new(AuthorityIdentifier(0), chain.clone(), address)?;

        let blockchain_db = node.get_provider().await;
        let mut canon_state_notification_receiver = blockchain_db.subscribe_to_canonical_state();

        let (to_execution, rx_notifier) = narwhal_types::test_channel!(1);
        debug!("created execution node");

        // start engine
        let _handles = node.start_engine(rx_notifier).await?;
        debug!("time to sleep");

        let res = to_execution.send(consensus_output).await;
        assert!(res.is_ok());

        // wait for next canonical block
        let too_long = Duration::from_secs(5);
        let canon_update = timeout(too_long, canon_state_notification_receiver.recv())
            .await
            .expect("next canonical block created within time")
            .expect("canon update is Some()");

        assert_matches!(canon_update, CanonStateNotification::Commit { .. });

        let canonical_tip = canon_update.tip();
        let update_header = canonical_tip.header.clone();
        debug!("canon update: {:?}", canonical_tip);
        debug!(?update_header);
        debug!(db_header = ?blockchain_db.finalized_header()?.unwrap());

        // sleeping doesn't help, and neither does get provider using a .write()
        // tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        // ensure database and provider are updated
        // wait for forkchoice to finish updating
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let expected_header = update_header.clone();

        // spawn task to return finalized header from the db
        tokio::spawn(async move {
            let blockchain_db = node.get_provider().await;
            let mut current_finalized_header = blockchain_db
                .finalized_header()
                .expect("blockchain db has some finalized header 1")
                .expect("some finalized header 2");
            while update_header != current_finalized_header {
                // sleep - then look up finalized in db again
                println!("\nwaiting for engine to complete forkchoice update...\n");
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                let blockchain_db = node.get_provider().await;
                current_finalized_header = blockchain_db
                    .finalized_header()
                    .expect("blockchain db has some finalized header 1")
                    .expect("some finalized header 2");
                println!("\n");
                debug!(retrieved_header = ?current_finalized_header.hash());
                debug!(retrieved_header = ?current_finalized_header);
                println!("\n");
            }

            tx.send(current_finalized_header)
        });

        let current_finalized_header =
            timeout(too_long, rx.recv()).await?.expect("finalized block retrieved from db");

        debug!("update completed...");
        assert_eq!(expected_header, current_finalized_header);
        debug!(?expected_header);
        debug!(?current_finalized_header);

        // sleep to explore
        // tokio::time::sleep(std::time::Duration::from_secs(3600)).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_batch_maker_and_rpc() -> eyre::Result<()> {
        init_test_tracing();
        // genesis
        let chain = yukon_chain_spec();
        let address = Address::from(U160::from(3003));

        debug!("creating execution node..");

        let node = ExecutionNode::new(AuthorityIdentifier(0), chain.clone(), address)?;

        // worker info
        let worker_id = 0;
        let (to_worker, mut worker_rx) = narwhal_types::test_channel!(1);
        node.start_batch_maker(to_worker, worker_id).await?;

        assert!(node.worker_is_running(&worker_id).await);

        // yield for batch maker
        tokio::task::yield_now().await;

        // submit transactions
        let mut tx_factory = TransactionFactory::new();
        let blockchain_db = node.get_provider().await;
        let gas_price = get_gas_price(blockchain_db.clone());
        debug!("gas price: {gas_price:?}");
        let value =
            U256::from(10).checked_pow(U256::from(18)).expect("u256 can handle 1e18").into();

        // create 3 transactions
        let transaction1 = tx_factory
            .create_eip1559(
                chain.clone(),
                gas_price,
                Address::ZERO,
                value, // 1 TEL
            )
            .envelope_encoded();

        let transaction2 = tx_factory
            .create_eip1559(
                chain.clone(),
                gas_price,
                Address::ZERO,
                value, // 1 TEL
            )
            .envelope_encoded();

        let transaction3 = tx_factory
            .create_eip1559(
                chain.clone(),
                gas_price,
                Address::ZERO,
                value, // 1 TEL
            )
            .envelope_encoded();

        // worker's http client
        let http_client = node
            .worker_http_client(&worker_id)
            .await?
            .expect("worker's http rpc server is running");
        let chain_id: String = http_client.request("eth_chainId", rpc_params![]).await?;

        assert_eq!("0xa28", chain_id); // 2600 in hex
        debug!(?chain_id);

        let _tx1_hash: B256 = http_client
            .request("eth_sendRawTransaction", rpc_params![transaction1.clone()])
            .await?;
        let _tx2_hash: B256 = http_client
            .request("eth_sendRawTransaction", rpc_params![transaction2.clone()])
            .await?;
        let _tx3_hash: B256 = http_client
            .request("eth_sendRawTransaction", rpc_params![transaction3.clone()])
            .await?;

        // TODO: update this once the batch maker has a better payload building process

        // create expected tx bytes as Vec<Vec<u8>>
        let expected: Vec<Vec<u8>> = vec![
            transaction1.into(),
            // transaction2.into(),
            // transaction3.into(),
        ];

        // wait for new batch
        let too_long = Duration::from_secs(5);
        let new_batch = timeout(too_long, worker_rx.recv())
            .await?
            // .expect("new batch created within time")
            .expect("new batch is Some()");

        debug!("new batch: {new_batch:?}");
        debug!("new batch num transactions: {:?}", new_batch.batch.transactions().len());

        assert_eq!(new_batch.batch.transactions(), &expected);

        // shutdown worker and ensure rpc is shutdown
        node.shutdown_worker(&worker_id).await?;
        assert!(!node.worker_is_running(&worker_id).await);
        assert!(node.worker_rpc_server_handle(&worker_id).await.is_err());
        assert!(http_client.request::<String, _>("eth_chainId", rpc_params![]).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_alternative_task_management() {
        let task_manager = TaskManager::new(Handle::current());
        let executor = task_manager.executor();

        executor.spawn_critical("sleep panic", async move {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            panic!("time to panic");
        });

        println!("spawned critical. sleeping now..");

        let manager_handle = tokio::spawn(Box::pin(async move {
            let err = task_manager.await;
            println!("{err:?}");
        }));

        assert!(!manager_handle.is_finished());

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        println!("sleep over D: - handle should have panicked");

        tokio::time::timeout(std::time::Duration::from_secs(3), manager_handle)
            .await
            .unwrap()
            .unwrap();

        println!("done.");
    }
}
