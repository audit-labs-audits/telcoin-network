//! Key command to generate all keys for running a full validator node.

use std::path::Path;

use crate::dirs::DataDirPath;
use clap::Parser;
use eyre::{anyhow, Context};
use fastcrypto::traits::KeyPair as KeyPairTraits;
use narwhal_types::{yukon_chain_spec, BlsKeypair, ExecutionKeypair, NetworkKeypair};
use question::{Answer, Question};
use rand::prelude::*;
use rand_chacha::ChaCha20Rng;
use reth::dirs::MaybePlatformPath;
use tracing::{debug, info};

/// Generate keypairs and save them to a file.
#[derive(Debug, Parser)]
pub struct Command {
    /// Save an encoded keypair (Base64 encoded `privkey`) to file.
    /// - bls (bls12381)
    /// - network (ed25519)
    /// - execution (secp256k1)
    #[arg(long, value_name = "DATA_DIR", verbatim_doc_comment, default_value_t)]
    pub datadir: MaybePlatformPath<DataDirPath>,
}

impl Command {
    /// Execute command
    pub async fn execute(&self) -> eyre::Result<()> {
        info!(target: "tn::generate_keys", "generating keys for full validator node");

        let chain = yukon_chain_spec();
        let datadir = self.datadir.unwrap_or_chain_default(chain.chain);

        let authority_key_path = datadir.as_ref().join("authority-keys");
        self.init_path(&authority_key_path)?;
        debug!("generating secrets at: {:?}", authority_key_path);

        // bls keypair for consensus - drop after write to zeroize memory
        let bls_keypair = self.generate_keypair_from_rng::<BlsKeypair>()?;
        self.write_authority_keypair_to_file(&bls_keypair, &authority_key_path.join("bls.key"))?;
        drop(bls_keypair); // calls zeroize() for OnceCell containing private key

        // let authority_keypair
        // network keypair for network
        let network_keypair = self.generate_keypair_from_rng::<NetworkKeypair>()?;
        self.write_authority_keypair_to_file(
            &network_keypair,
            &authority_key_path.join("network.key"),
        )?;
        drop(network_keypair); // calls zeroize() for OnceCell containing private key

        // secp keypair for execution
        let execution_keypair = self.generate_keypair_from_rng::<ExecutionKeypair>()?;
        self.write_authority_keypair_to_file(
            &execution_keypair,
            &authority_key_path.join("execution.key"),
        )?;
        drop(execution_keypair); // calls zeroize() for OnceCell containing private key

        Ok(())
    }

    /// Ensure the path exists, and if not, create it.
    fn init_path<P: AsRef<Path>>(&self, path: P) -> eyre::Result<()> {
        let rpath = path.as_ref();

        // create the dir if it doesn't exist or is empty
        if self.is_key_dir_empty(rpath) {
            std::fs::create_dir_all(rpath)
                .wrap_err_with(|| format!("Could not create key directory {}", rpath.display()))?;
        } else {
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

    // pub fn generate_keypair()

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
    pub fn write_authority_keypair_to_file<KP, P>(&self, keypair: &KP, path: P) -> eyre::Result<()>
    where
        KP: KeyPairTraits,
        P: AsRef<Path>,
    {
        let contents = keypair.encode_base64();
        std::fs::write(path, contents)?;
        Ok(())
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
}

#[cfg(test)]
mod tests {
    use crate::keys::Command;
    use clap::Parser;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_generate_keypairs() {
        // use tempdir
        let tempdir = tempdir().expect("tempdir created").into_path();
        let tn = Command::try_parse_from([
            "telcoin-network",
            // "generate-keys",
            "--datadir",
            tempdir.to_str().expect("tempdir path clean"),
        ])
        .expect("cli parsed");

        // create keys and assert dir is not empty
        assert!(tn.is_key_dir_empty(&tempdir));
        assert!(tn.execute().await.is_ok());
        assert!(!tn.is_key_dir_empty(&tempdir));
    }
}
