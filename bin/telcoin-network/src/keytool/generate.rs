//! Generate subcommand

use crate::dirs::DataDirPath;
use clap::{value_parser, Args, Subcommand};
use fastcrypto::traits::KeyPair as KeyPairTraits;
use rand::prelude::*;
use rand_chacha::ChaCha20Rng;
use reth::dirs::MaybePlatformPath;
use std::path::{Path, PathBuf};
use tn_config::Config;
use tn_types::{BlsKeypair, ExecutionKeypair, NetworkKeypair, WorkerInfo};
use tracing::{debug, info};

/// Generate keypairs and save them to a file.
#[derive(Debug, Clone, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct GenerateKeys {
    /// Save an encoded keypair (Base64 encoded `privkey`) to file.
    /// - bls (bls12381)
    /// - network (ed25519)
    /// - execution (secp256k1)
    #[arg(long, value_name = "DATA_DIR", verbatim_doc_comment, default_value_t, global = true)]
    pub datadir: MaybePlatformPath<DataDirPath>,

    /// Generate command that creates keypairs and writes to file.
    #[command(subcommand)]
    pub node_type: NodeType,
}

///Subcommand to generate keys for validator, primary, or worker.
#[derive(Debug, Clone, Subcommand)]
pub enum NodeType {
    /// Generate all validator keys and write them to file.
    #[command(name = "validator", alias = "all")]
    ValidatorKeys(ValidatorArgs),
    // primary keys
    // worker key
    // execution key
}

#[derive(Debug, Clone, Args)]
pub struct ValidatorArgs {
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
}

impl ValidatorArgs {
    /// Create all necessary information needed for validator and save to file.
    pub async fn execute(
        &self,
        authority_key_path: &PathBuf,
        config: &mut Config,
    ) -> eyre::Result<()> {
        info!(target: "tn::generate_keys", "generating keys for full validator node");

        // bls keypair for consensus - drop after write to zeroize memory
        let bls_keypair = self.generate_keypair_from_rng::<BlsKeypair>()?;
        self.write_keypair_to_file(&bls_keypair, &authority_key_path.join("bls.key"))?;
        config.update_protocol_key(bls_keypair.public().clone())?;
        drop(bls_keypair); // calls zeroize() for OnceCell containing private key

        // network keypair for authority
        let network_keypair = self.generate_keypair_from_rng::<NetworkKeypair>()?;
        self.write_keypair_to_file(&network_keypair, &authority_key_path.join("network.key"))?;
        config.update_network_key(network_keypair.public().clone())?;
        drop(network_keypair); // calls zeroize() for OnceCell containing private key

        // secp keypair for execution
        let execution_keypair = self.generate_keypair_from_rng::<ExecutionKeypair>()?;
        self.write_keypair_to_file(&execution_keypair, &authority_key_path.join("execution.key"))?;
        config.update_execution_key(execution_keypair.public().clone())?;
        drop(execution_keypair); // calls zeroize() for OnceCell containing private key

        let workers_dir = authority_key_path.join("workers");
        for worker in 0..self.workers {
            // TODO: better way to make this path the same between here and parent command's init
            // method
            debug!("worker: {worker:?}");
            let worker_path = format!("worker-{worker}.key");

            // network keypair for worker
            let network_keypair = self.generate_keypair_from_rng::<NetworkKeypair>()?;
            self.write_keypair_to_file(&network_keypair, &workers_dir.join(worker_path))?;
            let worker_info =
                WorkerInfo { name: network_keypair.public().clone(), ..Default::default() };
            debug!(?worker_info);
            config.workers.insert(worker, worker_info);
            drop(network_keypair); // calls zeroize() for OnceCell containing private key
        }

        Ok(())
    }

    /// Generate a keypair from a ChaCha20 RNG.
    ///
    /// based on: https://rust-random.github.io/book/guide-seeding.html
    ///
    /// TODO: discuss with @Utku
    pub fn generate_keypair_from_rng<KP>(&self) -> eyre::Result<KP>
    where
        // R: rand::CryptoRng + rand::RngCore,
        KP: KeyPairTraits,
        // <KP as KeyPairTraits>::PubKey: SuiPublicKey,
    {
        // this is expensive
        let rng = ChaCha20Rng::from_entropy();
        // note: StdRng uses ChaCha12
        let kp = KP::generate(&mut StdRng::from_rng(rng)?);
        Ok(kp)
    }

    /// Write Base64 encoded `privkey` to file.
    pub fn write_keypair_to_file<KP, P>(&self, keypair: &KP, path: P) -> eyre::Result<()>
    where
        KP: KeyPairTraits,
        P: AsRef<Path>,
    {
        let contents = keypair.encode_base64();
        std::fs::write(path, contents)?;
        Ok(())
    }
}
