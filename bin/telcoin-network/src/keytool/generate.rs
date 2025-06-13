//! Generate subcommand

use crate::args::clap_address_parser;
use clap::{value_parser, Args, Subcommand};
use tn_config::{Config, ConfigFmt, ConfigTrait as _, KeyConfig, NodeInfo, TelcoinDirs};
use tn_types::{get_available_udp_port, Address};
use tracing::info;

/// Generate keypairs and save them to a file.
#[derive(Debug, Clone, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct GenerateKeys {
    /// Generate command that creates keypairs and writes to file.
    #[command(subcommand)]
    pub node_type: NodeType,
}

///Subcommand to generate keys for validator, primary, or worker.
#[derive(Debug, Clone, Subcommand)]
pub enum NodeType {
    /// Generate all validator keys and write them to file.
    #[command(name = "validator", alias = "all")]
    ValidatorKeys(KeygenArgs),
    /// Generate all observer (non-validator) keys and write them to file.
    #[command(name = "observer")]
    ObserverKeys(KeygenArgs),
}

#[derive(Debug, Clone, Args)]
pub struct KeygenArgs {
    /// The number of workers for the primary.
    #[arg(long, value_name = "workers", global = true, default_value_t = 1, value_parser = value_parser!(u16).range(..=4))]
    pub workers: u16,

    /// Overwrite existing keys, if present.
    ///
    /// Warning: Existing keys will be lost.
    #[arg(
        long = "force",
        alias = "overwrite",
        help_heading = "Overwrite existing keys. Warning: existing keys will be lost.",
        verbatim_doc_comment
    )]
    pub force: bool,

    /// The address for suggested fee recipient.
    ///
    /// The execution layer address, derived from `secp256k1` keypair.
    /// The validator uses this address when producing batches and blocks.
    /// Validators can pass "0" to use the zero address.
    /// Address doesn't have to start with "0x", but the CLI supports the "0x" format too.
    #[arg(
        long = "address",
        alias = "execution-address",
        help_heading = "The address that should receive block rewards. Pass `0` to use the zero address.",
        env = "EXECUTION_ADDRESS",
        value_parser = clap_address_parser,
        verbatim_doc_comment
    )]
    pub address: Address,

    /// The network host/ip for the primary libp2p network.
    /// If not set will default to 127.0.0.1 (localhost)- this is only useful for tests
    #[arg(long, value_name = "HOST", env = "TN_PRIMARY_HOST")]
    pub primary_host: Option<String>,

    /// The network host/ip for the worker network(s).
    /// If not set will default to 127.0.0.1 (localhost)- this is only useful for tests
    #[arg(long, value_name = "HOST", env = "TN_WORKER_HOST")]
    pub worker_host: Option<String>,

    /// The network port for the primary libp2p network.
    /// If not set will try to find an unused port.
    #[arg(long, value_name = "PORT", env = "TN_PRIMARY_PORT")]
    pub primary_port: Option<u16>,

    /// The base port for worker p2p network(s).
    /// If not set will try to find an unused port.
    #[arg(long, value_name = "PORT", env = "TN_WORKER_PORT")]
    pub worker_base_port: Option<u16>,
}

impl KeygenArgs {
    fn update_keys<TND: TelcoinDirs>(
        &self,
        node_info: &mut NodeInfo,
        tn_datadir: &TND,
        passphrase: Option<String>,
    ) -> eyre::Result<()> {
        let key_config = KeyConfig::generate_and_save(tn_datadir, passphrase)?;
        let proof = key_config.generate_proof_of_possession_bls(&self.address)?;
        node_info.bls_public_key = key_config.primary_public_key();
        node_info.proof_of_possession = proof;
        node_info.name = format!(
            "node-{}",
            bs58::encode(&node_info.bls_public_key.to_bytes()[0..8]).into_string()
        );

        // network keypair for authority
        let network_publickey = key_config.primary_network_public_key();
        node_info.p2p_info.network_key = network_publickey;
        let primary_host = self.primary_host.clone().unwrap_or("127.0.0.1".to_string());
        let primary_udp_port = self
            .primary_port
            .unwrap_or_else(|| get_available_udp_port(&primary_host).unwrap_or(49584));
        node_info.p2p_info.network_address =
            format!("/ip4/{primary_host}/udp/{primary_udp_port}/quic-v1").parse()?;

        // network keypair for workers
        let network_publickey = key_config.worker_network_public_key();
        let worker_host = self.primary_host.clone().unwrap_or("127.0.0.1".to_string());
        let mut worker_udp_port = self
            .worker_base_port
            .unwrap_or_else(|| get_available_udp_port(&worker_host).unwrap_or(49584));
        for worker in node_info.p2p_info.worker_index.0.iter_mut() {
            worker.name = network_publickey.clone();
            worker.worker_address =
                format!("/ip4/{worker_host}/udp/{worker_udp_port}/quic-v1").parse()?;
            worker_udp_port = if self.worker_base_port.is_none() {
                get_available_udp_port(&worker_host).unwrap_or(49584)
            } else {
                worker_udp_port + 1
            };
        }
        Ok(())
    }

    /// Create all necessary information needed for validator and save to file.
    pub fn execute<TND: TelcoinDirs>(
        &self,
        tn_datadir: &TND,
        passphrase: Option<String>,
    ) -> eyre::Result<()> {
        info!(target: "tn::generate_keys", "generating keys for full validator node");
        let mut node_info = NodeInfo::default();

        self.update_keys(&mut node_info, tn_datadir, passphrase)?;

        // add execution address
        node_info.execution_address = self.address;
        Config::write_to_path(tn_datadir.node_info_path(), &node_info, ConfigFmt::YAML)?;

        Ok(())
    }
}
