//! Main node command
//!
//! Starts the client
use crate::{
    args::clap_genesis_parser,
    dirs::{DataDirPath, TelcoinDirs},
    version::SHORT_VERSION,
};
use clap::{value_parser, Parser};
use consensus_metrics::RegistryService;

use fdlimit::raise_fd_limit;
use narwhal_network::client::NetworkClient;
use prometheus::Registry;
use reth::{
    args::{
        utils::parse_socket_address, DatabaseArgs, DebugArgs, DevArgs, NetworkArgs,
        PayloadBuilderArgs, PruningArgs, RpcServerArgs, TxPoolArgs,
    },
    cli::ext::{DefaultRethNodeCommandConfig, RethCliExt},
    dirs::MaybePlatformPath,
    runner::CliContext,
};
use reth_primitives::ChainSpec;

use std::{net::SocketAddr, path::PathBuf, str::FromStr, sync::Arc};
use tn_config::{
    read_validator_keypair_from_file, traits::ConfigTrait, Config, BLS_KEYFILE,
    PRIMARY_NETWORK_KEYFILE, WORKER_NETWORK_KEYFILE,
};
use tn_node::{engine::ExecutionNode, primary::PrimaryNode, worker::WorkerNode, NodeStorage};
use tn_types::{AuthorityIdentifier, ChainIdentifier, Committee, WorkerCache};

use tracing::*;

/// Start the node
#[derive(Debug, Parser)]
pub struct NodeCommand<Ext: RethCliExt = ()> {
    /// The path to the data dir for all reth files and subdirectories.
    ///
    /// Defaults to the OS-specific data directory:
    ///
    /// - Linux: `$XDG_DATA_HOME/reth/` or `$HOME/.local/share/reth/`
    /// - Windows: `{FOLDERID_RoamingAppData}/reth/`
    /// - macOS: `$HOME/Library/Application Support/reth/`
    #[arg(long, value_name = "DATA_DIR", verbatim_doc_comment, default_value_t)]
    pub datadir: MaybePlatformPath<DataDirPath>,

    /// The path to the configuration file to use.
    #[arg(long, value_name = "FILE", verbatim_doc_comment)]
    pub config: Option<PathBuf>,

    /// The chain this node is running.
    ///
    /// Possible values are either a built-in chain or the path to a chain specification file.
    ///
    /// Defaults to the custom
    #[arg(
        long,
        value_name = "CHAIN_OR_PATH",
        verbatim_doc_comment,
        default_value = "yukon",
        default_value_if("dev", "true", "yukon"),
        value_parser = clap_genesis_parser,
        required = false,
    )]
    pub chain: Arc<ChainSpec>,

    /// Enable Prometheus metrics.
    ///
    /// The metrics will be served at the given interface and port.
    #[arg(long, value_name = "SOCKET", value_parser = parse_socket_address, help_heading = "Metrics")]
    pub metrics: Option<SocketAddr>,

    /// Add a new instance of a node.
    ///
    /// Configures the ports of the node to avoid conflicts with the defaults.
    /// This is useful for running multiple nodes on the same machine.
    ///
    /// Max number of instances is 200. It is chosen in a way so that it's not possible to have
    /// port numbers that conflict with each other.
    ///
    /// Changes to the following port numbers:
    /// - DISCOVERY_PORT: default + `instance` - 1
    /// - AUTH_PORT: default + `instance` * 100 - 100
    /// - HTTP_RPC_PORT: default - `instance` + 1
    /// - WS_RPC_PORT: default + `instance` * 2 - 2
    #[arg(long, value_name = "INSTANCE", global = true, default_value_t = 1, value_parser = value_parser!(u16).range(..=200))]
    pub instance: u16,

    /// Overrides the KZG trusted setup by reading from the supplied file.
    #[arg(long, value_name = "PATH")]
    pub trusted_setup_file: Option<PathBuf>,

    /// All networking related arguments
    #[clap(flatten)]
    pub network: NetworkArgs,

    /// All rpc related arguments
    #[clap(flatten)]
    pub rpc: RpcServerArgs,

    /// All txpool related arguments with --txpool prefix
    #[clap(flatten)]
    pub txpool: TxPoolArgs,

    /// All payload builder related arguments
    #[clap(flatten)]
    pub builder: PayloadBuilderArgs,

    /// All debug related arguments with --debug prefix
    #[clap(flatten)]
    pub debug: DebugArgs,

    /// All database related arguments
    #[clap(flatten)]
    pub db: DatabaseArgs,

    /// All dev related arguments with --dev prefix
    #[clap(flatten)]
    pub dev: DevArgs,

    /// All pruning related arguments
    #[clap(flatten)]
    pub pruning: PruningArgs,

    /// Additional cli arguments
    #[clap(flatten)]
    pub ext: Ext::Node,
}

impl<Ext: RethCliExt> NodeCommand<Ext> {
    /// Replaces the extension of the node command
    pub fn with_ext<E: RethCliExt>(self, ext: E::Node) -> NodeCommand<E> {
        let Self {
            datadir,
            config,
            chain,
            metrics,
            trusted_setup_file,
            instance,
            network,
            rpc,
            txpool,
            builder,
            debug,
            db,
            dev,
            pruning,
            ..
        } = self;
        NodeCommand {
            datadir,
            config,
            chain,
            metrics,
            instance,
            trusted_setup_file,
            network,
            rpc,
            txpool,
            builder,
            debug,
            db,
            dev,
            pruning,
            ext,
        }
    }

    /// Execute `node` command
    pub async fn execute(self, _ctx: CliContext) -> eyre::Result<()> {
        info!(target: "reth::cli", "reth {} starting", SHORT_VERSION);

        // Raise the fd limit of the process.
        // Does not do anything on windows.
        raise_fd_limit();

        // add network name to data dir
        let data_dir = self.datadir.unwrap_or_chain_default(self.chain.chain);
        let config_path = self.config.clone().unwrap_or(data_dir.node_config_path());
        let config: Config = Config::load_from_path(config_path)?;
        info!(target: "telcoin::cli", validator = ?config.validator_info.name, "config loaded");

        // TODO: use this or CLI?
        let _chain = Arc::new(config.chain_spec().clone());

        let terminate_early = self.debug.terminate.clone();

        // get the worker's transaction address from the config
        let Self {
            // datadir,
            // config,
            // chain,
            metrics,
            trusted_setup_file,
            instance,
            network,
            rpc,
            txpool,
            builder,
            debug,
            db,
            dev,
            pruning,
            ..
        } = self;

        let ext = DefaultRethNodeCommandConfig::default();
        let datadir_path = data_dir.to_string();
        let cli = reth::node::NodeCommand::<()> {
            datadir: MaybePlatformPath::from_str(&datadir_path).expect("datadir compatible with platform path"),
            config: self.config.clone(),
            chain: self.chain.clone(),
            metrics,
            instance,
            trusted_setup_file,
            network,
            rpc,
            txpool,
            builder,
            debug,
            db,
            dev,
            pruning,
            ext,
        };

        let engine = ExecutionNode::new(
            AuthorityIdentifier(self.instance), // TODO: where to get this value?
            self.chain.clone(),                 // TODO: get this from config?
            config.execution_address().clone(),
            cli,
        )?;

        info!(target: "telcoin::cli", "execution engine created");

        let narwhal_db_path = data_dir.narwhal_db_path();

        info!(target: "telcoin::cli", "opening node storage at {:?}", narwhal_db_path);

        // open storage for consensus - no metrics passed
        // TODO: pass metrics here?
        let node_storage = NodeStorage::reopen(narwhal_db_path, None);

        info!(target: "telcoin::cli", "node storage open");

        let registry_service = RegistryService::new(Registry::new());
        let network_client =
            NetworkClient::new_from_public_key(config.validator_info.primary_network_key());
        let primary = PrimaryNode::new(config.parameters.clone(), registry_service.clone());
        let (worker_id, _worker_info) = config.workers().first_worker()?;
        let worker =
            WorkerNode::new(worker_id.clone(), config.parameters.clone(), registry_service);

        // TODO: find a better way to manage keys
        //
        // load keys to start the primary
        let validator_keypath = data_dir.validator_keys_path();
        info!(target: "telcoin::cli", "loading validator keys at {:?}", validator_keypath);
        let bls_keypair = read_validator_keypair_from_file(validator_keypath.join(BLS_KEYFILE))?;
        let network_keypair =
            read_validator_keypair_from_file(validator_keypath.join(PRIMARY_NETWORK_KEYFILE))?;

        // load committee from file
        let mut committee: Committee = Config::load_from_path(data_dir.committee_path())?;
        committee.load();
        info!(target: "telcoin::cli", "committee loaded");
        // TODO: make worker cache part of committee?
        let worker_cache: WorkerCache = Config::load_from_path(data_dir.worker_cache_path())?;
        info!(target: "telcoin::cli", "worker cache loaded");

        // TODO: this could be a separate method on `Committee` to have robust checks in place
        // - all public keys are unique
        // - thresholds / stake
        //
        // assert committee loaded correctly
        // assert!(committee.size() >= 4, "not enough validators in committee.");

        // TODO: better assertion here
        // right now, each validator should only have 1 worker
        // this assertion would incorrectly pass if 1 authority had 2 workers and another had 0
        //
        // assert worker cache loaded correctly
        assert!(
            worker_cache.all_workers().len() == committee.size(),
            "each validator within committee must have one worker"
        );

        // start the primary
        primary
            .start(
                bls_keypair,
                network_keypair,
                committee.clone(),
                ChainIdentifier::unknown(), // TODO: use ChainSpec here
                worker_cache.clone(),
                network_client.clone(),
                &node_storage,
                &engine,
            )
            .await?;

        let worker_network_keypair =
            read_validator_keypair_from_file(validator_keypath.join(WORKER_NETWORK_KEYFILE))?;
        // start the worker
        worker
            .start(
                config.primary_public_key()?.clone(), // TODO: remove result for this method
                worker_network_keypair,
                committee,
                worker_cache,
                network_client,
                &node_storage,
                None, // optional metrics
                &engine,
            )
            .await?;

        // let mut config: Config = self.load_config(config_path.clone())?;

        // rx.await??;

        // info!(target: "reth::cli", "Consensus engine has exited.");

        if terminate_early {
            Ok(())
        } else {
            // The pipeline has finished downloading blocks up to `--debug.tip` or
            // `--debug.max-block`. Keep other node components alive for further usage.
            futures::future::pending().await
        }
    }
}
