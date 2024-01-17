//! Genesis ceremony command.
//!
//! The genesis ceremony is how networks are started.

mod add_validator;
use self::add_validator::AddValidator;
use crate::{args::clap_genesis_parser, dirs::DataDirPath};
use clap::{Args, Subcommand};

use reth::dirs::MaybePlatformPath;
use reth_primitives::ChainSpec;
use std::{path::PathBuf, sync::Arc};

/// Generate keypairs and save them to a file.
#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct GenesisArgs {
    /// Read and write to committe file.
    ///
    /// [Committee] contains quorum of validators.
    #[arg(long, value_name = "DATA_DIR", verbatim_doc_comment, default_value_t, global = true)]
    pub datadir: MaybePlatformPath<DataDirPath>,

    /// The path to the configuration file to use.
    #[arg(long, value_name = "CONFIG_FILE", verbatim_doc_comment)]
    pub config: Option<PathBuf>,

    /// The path to the committee file to use.
    #[arg(long, value_name = "COMMITTEE_FILE", verbatim_doc_comment)]
    pub committee_file: Option<PathBuf>,

    /// The chain this node is running.
    ///
    /// The value parser matches either a known chain, the path
    /// to a json file, or a json formatted string in-memory. The json can be either
    /// a serialized [ChainSpec] or Genesis struct.
    #[arg(
        long,
        value_name = "CHAIN_OR_PATH",
        verbatim_doc_comment,
        default_value = "yukon",
        value_parser = clap_genesis_parser,
        required = false,
    )]
    chain: Arc<ChainSpec>,

    /// Generate command that creates keypairs and writes to file.
    #[command(subcommand)]
    pub command: CeremonySubcommand,
}

///Subcommand to either generate keys or read public keys.
#[derive(Debug, Clone, Subcommand)]
pub enum CeremonySubcommand {
    /// Add validator to committee.
    #[command(name = "add-validator")]
    AddValidator(AddValidator),
    // TODO: add more commands
    // - init (create committe file?)
    // - list validators (print peers)
    // - verify and sign (sign EL Genesis)
    // - finalize (todo)
}

impl GenesisArgs {
    /// Execute command
    pub async fn execute(&self) -> eyre::Result<()> {
        // // create datadir
        // let datadir = self.data_dir();
        // // creates a default config if none exists
        // let mut config = self.load_config()?;

        match &self.command {
            // add validator to the committee file
            CeremonySubcommand::AddValidator(args) => {
                args.execute().await?;
            }
        }

        Ok(())
    }
}
