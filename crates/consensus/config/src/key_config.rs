use std::sync::Arc;

use tn_types::{
    read_validator_keypair_from_file,
    traits::{AllowedRng, KeyPair, Signer},
    BlsKeypair, BlsSignature, BlsSigner, NetworkKeypair, TelcoinDirs, BLS_KEYFILE,
    PRIMARY_NETWORK_KEYFILE, WORKER_NETWORK_KEYFILE,
};

#[derive(Debug)]
struct KeyConfigInner {
    bls_keypair: BlsKeypair,
    network_keypair: NetworkKeypair,
    worker_network_keypair: NetworkKeypair,
}

// TODO: audit the use of this struct for leaking private keys, etc...
#[derive(Debug, Clone)]
pub struct KeyConfig {
    inner: Arc<KeyConfigInner>,
}

impl KeyConfig {
    pub fn new<TND: TelcoinDirs>(tn_datadir: &TND) -> eyre::Result<Self> {
        // TODO: find a better way to manage keys
        //
        // load keys to start the primary
        let validator_keypath = tn_datadir.validator_keys_path();
        tracing::info!(target: "telcoin::consensus_config", "loading validator keys at {:?}", validator_keypath);
        let bls_keypair = read_validator_keypair_from_file(validator_keypath.join(BLS_KEYFILE))?;
        let network_keypair =
            read_validator_keypair_from_file(validator_keypath.join(PRIMARY_NETWORK_KEYFILE))?;
        let worker_network_keypair =
            read_validator_keypair_from_file(validator_keypath.join(WORKER_NETWORK_KEYFILE))?;
        Ok(Self {
            inner: Arc::new(KeyConfigInner {
                bls_keypair,
                network_keypair,
                worker_network_keypair,
            }),
        })
    }

    /// Generate random keys with provided RNG.
    ///
    /// Useful for testing.
    pub fn with_random<R: AllowedRng>(rng: &mut R) -> Self {
        let bls_keypair = BlsKeypair::generate(rng);
        let network_keypair = NetworkKeypair::generate(rng);
        let worker_network_keypair = NetworkKeypair::generate(rng);
        Self {
            inner: Arc::new(KeyConfigInner {
                bls_keypair,
                network_keypair,
                worker_network_keypair,
            }),
        }
    }

    pub fn bls_keypair(&self) -> &BlsKeypair {
        &self.inner.bls_keypair
    }

    pub fn network_keypair(&self) -> &NetworkKeypair {
        &self.inner.network_keypair
    }

    pub fn worker_network_keypair(&self) -> &NetworkKeypair {
        &self.inner.worker_network_keypair
    }
}

impl BlsSigner for KeyConfig {
    fn request_signature_direct(&self, msg: &[u8]) -> BlsSignature {
        self.inner.bls_keypair.sign(msg)
    }
}
