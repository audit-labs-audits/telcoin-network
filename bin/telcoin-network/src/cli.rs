//! CLI definition and entrypoint for executable

use crate::{
    config,
    dirs::{LogsDir, PlatformPath},
    node,
    runner::CliRunner,
};
use clap::{ArgAction, Args, Parser, Subcommand};
use tn_tracing::{BoxedLayer, FileWorkerGuard};
use tracing::{metadata::LevelFilter, Level, Subscriber};
use tracing_subscriber::{filter::Directive, registry::LookupSpan, EnvFilter};

/// Parse CLI options, create logging, and run the node
pub fn run() -> eyre::Result<()> {
    let opt = Cli::parse();

    let mut layers = vec![tn_tracing::stdout(opt.verbosity.directive())];
    let _guard = opt.logs.layer()?.map(|(layer, guard)| {
        layers.push(layer);
        guard
    });

    tn_tracing::init(layers);

    let runner = CliRunner::default();

    match opt.command {
        // Commands::Gateway(c) => runner.run_command_until_exit(|ctx| c.execute(ctx)),
        // Commands::Engine(c) => runner.run_command_until_exit(|ctx| c.execute(ctx)),
        Commands::Node(c) => runner.run_command_until_exit(|ctx| c.execute(ctx)),
        Commands::Config(command) => runner.run_until_ctrl_c(command.execute()),
    }
}

/// Command options
#[derive(Subcommand, Debug)]
enum Commands {
    /// Run a node: execution and consensus
    #[command(name = "node")]
    Node(node::Command),

    /// Write config to stdout
    #[command(name = "config")]
    Config(config::Command),
    // /// Start a gateway with zmq sockets for:
    // /// - PULL from EthRPC
    // /// - PUSH to Worker
    // /// - PULL from Primary
    // /// - PUSH to Engine
    // #[command(name = "gateway")]
    // Gateway(gateway::Command),

    // /// Start a node instance with:
    // /// - Primary
    // /// - Workers
    // #[command(name = "node")]
    // Node(node::Command),

    // /// - RPC
    // /// - Engine
    // #[command(name = "engine")]
    // Engine(engine::Command),
}

#[derive(Parser, Debug)]
#[command(author, version = "0.1", about = "Lattice")]
struct Cli {
    /// Command to run
    #[clap(subcommand)]
    command: Commands,

    /// Log configuration
    #[clap(flatten)]
    logs: Logs,

    /// Tracing verbosity
    #[clap(flatten)]
    verbosity: Verbosity,
}

/// The log configuration.
#[derive(Debug, Args)]
#[command(next_help_heading = "Logging")]
pub struct Logs {
    /// The flag to enable persistent logs.
    #[arg(long = "log.persistent", global = true, conflicts_with = "journald")]
    persistent: bool,

    /// The path to put log files in.
    #[arg(
        long = "log.directory",
        value_name = "PATH",
        global = true,
        default_value_t,
        conflicts_with = "journald"
    )]
    log_directory: PlatformPath<LogsDir>,

    /// Log events to journald.
    #[arg(long = "log.journald", global = true, conflicts_with = "log_directory")]
    journald: bool,

    /// The filter to use for logs written to the log file.
    #[arg(long = "log.filter", value_name = "FILTER", global = true, default_value = "debug")]
    filter: String,
}

impl Logs {
    /// Builds a tracing layer from the current log options.
    pub fn layer<S>(&self) -> eyre::Result<Option<(BoxedLayer<S>, Option<FileWorkerGuard>)>>
    where
        S: Subscriber,
        for<'a> S: LookupSpan<'a>,
    {
        let filter = EnvFilter::builder().parse(&self.filter)?;

        if self.journald {
            Ok(Some((tn_tracing::journald(filter).expect("Could not connect to journald"), None)))
        } else if self.persistent {
            let (layer, guard) =
                tn_tracing::file(filter, &self.log_directory, "telcoin-network.log");
            Ok(Some((layer, Some(guard))))
        } else {
            Ok(None)
        }
    }
}

/// The verbosity settings for the cli.
#[derive(Debug, Copy, Clone, Args)]
#[command(next_help_heading = "Display")]
pub struct Verbosity {
    /// Set the minimum log level.
    ///
    /// -v      Errors
    /// -vv     Warnings
    /// -vvv    Info
    /// -vvvv   Debug
    /// -vvvvv  Traces (warning: very verbose!)
    #[clap(short, long, action = ArgAction::Count, global = true, default_value_t = 5, verbatim_doc_comment, help_heading = "Display")]
    verbosity: u8,

    /// Silence all log output.
    #[clap(long, alias = "silent", short = 'q', global = true, help_heading = "Display")]
    quiet: bool,
}

impl Verbosity {
    /// Get the corresponding [Directive] for the given verbosity, or none if the verbosity
    /// corresponds to silent.
    pub fn directive(&self) -> Directive {
        if self.quiet {
            LevelFilter::OFF.into()
        } else {
            let level = match self.verbosity - 1 {
                0 => Level::ERROR,
                1 => Level::WARN,
                2 => Level::INFO,
                3 => Level::DEBUG,
                _ => Level::TRACE,
            };

            // TODO: eventually change this when logging is more consistent
            // default_value_t = 4, so this logs Debug +
            //
            // see tracing_subscriber::EnvFilter for more info on how this works
            level.into()
            // format!("lattice::cli={level}").parse().unwrap()
        }
    }
}
