//! Key command to generate all keys for running a full validator node.

mod generate;
use crate::{args::clap_genesis_parser, dirs::DataDirPath};
use clap::{value_parser, Args, Subcommand};
use eyre::{anyhow, Context};
use fastcrypto::traits::KeyPair as KeyPairTraits;
use generate::GenerateKeys;
use question::{Answer, Question};
use reth::dirs::{ChainPath, MaybePlatformPath};
use reth_primitives::ChainSpec;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tn_config::Config;
use tracing::{debug, info};
use self::generate::NodeType;

/// Generate keypairs and save them to a file.
#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct KeyArgs {
    /// Save an encoded keypair (Base64 encoded `privkey`) to file.
    /// - bls (bls12381)
    /// - network (ed25519)
    /// - execution (secp256k1)
    #[arg(long, value_name = "DATA_DIR", verbatim_doc_comment, default_value_t, global = true)]
    pub datadir: MaybePlatformPath<DataDirPath>,

    /// The path to the configuration file to use.
    #[arg(long, value_name = "FILE", verbatim_doc_comment)]
    pub config: Option<PathBuf>,

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
    pub read_or_write: KeySubcommand,

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
}

///Subcommand to either generate keys or read public keys.
#[derive(Debug, Clone, Subcommand)]
pub enum KeySubcommand {
    /// Generate keys and write to file.
    #[command(name = "generate")]
    Generate(GenerateKeys),
    /// Read public keys from file.
    #[command(name = "read")]
    Read,
}

/// Read arg that reads from file.
#[derive(Debug, Args)]
pub struct Read; // public keys for: bls, network, execution, address

impl KeyArgs {
    /// Execute command
    pub async fn execute(&self) -> eyre::Result<()> {
        // create datadir
        let datadir = self.data_dir();
        // creates a default config if none exists
        let mut config = self.load_config()?;

        match &self.read_or_write {
            // generate keys
            KeySubcommand::Generate(args) => {
                match &args.node_type {
                    NodeType::ValidatorKeys(args) => {
                        // initialize path
                        let path_name = format!("authority-keys-{}", self.instance);
                        let authority_key_path = datadir.as_ref().join(&path_name);
                        // init path warns users if overwriting keys
                        self.init_path(&authority_key_path, args.force)?;
                        // execute and store keypath
                        args.execute(&authority_key_path, &mut config).await?;
                        config.keypath = authority_key_path;
                        debug!("{config:?}");
                        // note: there is serialization issues with value not being emitted before
                        // tables serde skip_serializing seems to work, as
                        // well as putting maps last this is further
                        // complicated bc reth still uses default confy features,
                        // so toml it is for now
                        //
                        // see also https://github.com/paradigmxyz/reth/issues/805
                        confy::store_path(self.config_path(), config)?;
                    }
                }
            }

            // read public key from file
            KeySubcommand::Read => todo!(),
        }

        Ok(())
    }

    /// Ensure the path exists, and if not, create it.
    fn init_path<P: AsRef<Path>>(&self, path: P, force: bool) -> eyre::Result<()> {
        let rpath = path.as_ref();

        // create the dir if it doesn't exist or is empty
        if self.is_key_dir_empty(rpath) {
            // authority dir
            std::fs::create_dir_all(rpath).wrap_err_with(|| {
                format!("Could not create authority key directory {}", rpath.display())
            })?;
            // workers dir
            let worker_path = rpath.join("workers");
            std::fs::create_dir(&worker_path).wrap_err_with(|| {
                format!("Could not create worker key directory {}", worker_path.display())
            })?;
        } else if !force {
            // ask user if they want to continue generating new keys
            let answer =
                Question::new("Keys might already exist. Do you want to generate new keys? (y/n)")
                    .confirm();

            if answer != Answer::YES {
                // TODO: something better than panic here
                panic!("Abandoning new key generation.")
            }

            // double-check
            let answer = Question::new("Warning: this action is irreversable. Are you sure you want to overwrite authority keys? (y/n)")
                .confirm();

            if answer != Answer::YES {
                // TODO: something better than panic here
                panic!("Abandoning new key generation.")
            }
        }

        Ok(())
    }

    /// Check if key file directory is empty.
    fn is_key_dir_empty<P: AsRef<Path>>(&self, path: P) -> bool {
        let rpath = path.as_ref();

        if !rpath.exists() {
            true
        } else if let Ok(dir) = rpath.read_dir() {
            dir.count() == 0
        } else {
            true
        }
    }

    /// Read from file as Base64 encoded `privkey` and return a AuthorityKeyPair.
    ///
    /// TODO: when would this ever be used?
    pub fn read_authority_keypair_from_file<KP, Path>(&self, path: Path) -> eyre::Result<KP>
    where
        KP: KeyPairTraits,
        Path: AsRef<std::path::Path>,
    {
        let contents = std::fs::read_to_string(path)?;
        KP::decode_base64(contents.as_str().trim()).map_err(|e| anyhow!(e))
    }

    /// Returns the chain specific path to the data dir.
    fn data_dir(&self) -> ChainPath<DataDirPath> {
        self.datadir.unwrap_or_chain_default(self.chain.chain)
    }

    /// Returns the path to the config file.
    fn config_path(&self) -> PathBuf {
        self.config.clone().unwrap_or_else(|| self.data_dir().as_ref().join("telcoin-network.toml"))
    }

    /// Loads the reth config with the given datadir root
    fn load_config(&self) -> eyre::Result<Config> {
        debug!("loading config...");
        let config_path = self.config_path();
        debug!(?config_path);
        let config = confy::load_path::<Config>(&config_path).unwrap_or_default();
        debug!("{:?}", config);

        info!(target: "tn::cli", path = ?config_path, "Configuration loaded");

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use crate::cli::Cli;
    use clap::Parser;
    use tempfile::tempdir;
    use tn_config::Config;

    /// Test that generate keys command works.
    /// This test also ensures that confy is able to
    /// load the default config.toml, update the file,
    /// and save it.
    ///
    /// TODO: better unit test for arg methods.
    #[test]
    fn test_generate_keypairs() {
        // use tempdir
        let tempdir = tempdir().expect("tempdir created").into_path();
        let tn = Cli::<()>::try_parse_from([
            "telcoin-network",
            "keytool",
            "generate",
            "validator",
            "--workers",
            "1",
            "--datadir",
            tempdir.to_str().expect("tempdir path clean"),
        ])
        .expect("cli parsed");

        tn.run().expect("generate keys command successful");

        confy::load_path::<Config>(tempdir.as_path()).expect("confy loaded toml okay");
    }
}
