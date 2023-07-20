//! CLI definition and entrypoint for executable

use crate::{
    dirs::{LogsDir, PlatformPath},
    config,
    runner::CliRunner,
    execution,
};
use clap::{ArgAction, Args, Parser, Subcommand};
use lattice_tracing::{
    self,
    tracing::{metadata::LevelFilter, Level, Subscriber},
    tracing_subscriber::{filter::Directive, registry::LookupSpan},
    BoxedLayer, FileWorkerGuard,
};
use std::str::FromStr;

/// Parse CLI options, create logging, and run the node
pub fn run() -> eyre::Result<()> {
    let opt = Cli::parse();

    // TODO: set tracing - default is "trace" for all
    let (layer, _guard) = opt.logs.layer();
    lattice_tracing::init(vec![
        layer,
        lattice_tracing::stdout(opt.verbosity.directive()),
    ]);

    let runner = CliRunner::default();

    match opt.command {
        // Commands::Gateway(c) => runner.run_command_until_exit(|ctx| c.execute(ctx)),
        // Commands::Node(c) => runner.run_command_until_exit(|ctx| c.execute(ctx)),
        // Commands::Engine(c) => runner.run_command_until_exit(|ctx| c.execute(ctx)),
        Commands::Execution(c) => runner.run_command_until_exit(|ctx| c.execute(ctx)),
        Commands::Config(command) => runner.run_until_ctrl_c(command.execute()),
    }
}

/// Command options
#[derive(Subcommand, Debug)]
enum Commands {
    /// Execution layer: RPC && Engine
    #[command(name = "execution")]
    Execution(execution::Command),

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
    #[arg(
        long = "log.filter",
        value_name = "FILTER",
        global = true,
        default_value = "debug"
    )]
    filter: String,
}

impl Logs {
    /// Builds a tracing layer from the current log options.
    pub fn layer<S>(&self) -> (BoxedLayer<S>, Option<FileWorkerGuard>)
    where
        S: Subscriber,
        for<'a> S: LookupSpan<'a>,
    {
        let directive = Directive::from_str(self.filter.as_str())
            .unwrap_or_else(|_| Directive::from_str("debug").unwrap());

        if self.journald {
            (
                lattice_tracing::journald(directive).expect("Could not connect to journald"),
                None,
            )
        } else {
            let (layer, guard) =
                lattice_tracing::file(directive, &self.log_directory, "lattice.log");
            (layer, Some(guard))
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
    #[clap(
        long,
        alias = "silent",
        short = 'q',
        global = true,
        help_heading = "Display"
    )]
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
