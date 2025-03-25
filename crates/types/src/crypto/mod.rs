//! Type aliases selecting the signature algorithm for the code base.
//!
//! Here we select the types that are used by default in the code base.
//!
//! Guidelines:
//! - refer to these aliases always (avoid using the individual scheme implementations)
//! - use generic schemes (avoid using the algo's `Struct`` impl functions)
//! - change type aliases to update codebase with new crypto

use std::future::Future;

use blake2::digest::consts::U32;
use libp2p::PeerId;
// This re-export allows using the trait-defined APIs
mod bls_keypair;
mod bls_public_key;
mod bls_signature;
mod intent;
mod network;

pub use bls_keypair::*;
pub use bls_public_key::*;
pub use bls_signature::*;
pub use intent::*;
pub use network::*;

/// Trait impl'd by a key/keypair that can create signatures.
pub trait Signer {
    /// Create a new signature over a message.
    fn sign(&self, msg: &[u8]) -> BlsSignature;
}

//
// EXECUTION
//
/// Public key used for signing transactions in the Execution Layer.
pub type ExecutionPublicKey = secp256k1::PublicKey;
/// Keypair used for signing transactions in the Execution Layer.
pub type ExecutionKeypair = secp256k1::Keypair;

/// Type alias selecting the default hash function for the code base.
pub type DefaultHashFunction = blake2::Blake2b<U32>;
pub const DIGEST_LENGTH: usize = 32;
pub const INTENT_MESSAGE_LENGTH: usize = INTENT_PREFIX_LENGTH + DIGEST_LENGTH;

/// Trait to implement Bls key signing.  This allows us to maintain private keys in a
/// secure enclave and provide a signing service.
pub trait BlsSigner: Clone + Send + Sync + Unpin + 'static {
    /// Sync version to sign something with a BLS private key.
    fn request_signature_direct(&self, msg: &[u8]) -> BlsSignature;

    /// Request a signature asyncronisly.
    /// Note: used the de-sugared signature here (instead of async fn request_signature...)
    /// due to current async trait limitations and the need for + Send.
    fn request_signature(&self, msg: Vec<u8>) -> impl Future<Output = BlsSignature> + Send {
        let this = self.clone();
        let handle = tokio::task::spawn_blocking(move || this.request_signature_direct(&msg));
        async move { handle.await.expect("Failed to receive signature from Signature Service") }
    }
}

/// Wrap a message in an intent message. Currently in Consensus, the scope is always
/// IntentScope::ConsensusDigest and the app id is AppId::Consensus.
pub fn to_intent_message<T>(value: T) -> IntentMessage<T> {
    IntentMessage::new(Intent::consensus(IntentScope::ConsensusDigest), value)
}

/// Convert an existing NetworkPublicKey into a libp2p PeerId.
pub fn network_public_key_to_libp2p(public_key: &NetworkPublicKey) -> PeerId {
    public_key.to_peer_id()
}

#[cfg(test)]
mod tests {
    use super::{generate_proof_of_possession_bls, verify_proof_of_possession_bls};
    use crate::{adiri_chain_spec_arc, adiri_genesis, BlsKeypair};
    use rand::{
        rngs::{OsRng, StdRng},
        SeedableRng,
    };

    #[test]
    fn test_proof_of_possession_success() {
        let keypair = BlsKeypair::generate(&mut StdRng::from_rng(OsRng).unwrap());
        let chain_spec = adiri_chain_spec_arc();
        let proof = generate_proof_of_possession_bls(&keypair, &chain_spec).unwrap();
        assert!(verify_proof_of_possession_bls(&proof, keypair.public(), &chain_spec).is_ok())
    }

    #[test]
    fn test_proof_of_possession_fails_wrong_signature() {
        let keypair = BlsKeypair::generate(&mut StdRng::from_rng(OsRng).unwrap());
        let malicious_key = BlsKeypair::generate(&mut StdRng::from_rng(OsRng).unwrap());
        let chain_spec = adiri_chain_spec_arc();
        let proof = generate_proof_of_possession_bls(&malicious_key, &chain_spec).unwrap();
        assert!(verify_proof_of_possession_bls(&proof, keypair.public(), &chain_spec).is_err())
    }

    #[test]
    fn test_proof_of_possession_fails_wrong_public_key() {
        let keypair = BlsKeypair::generate(&mut StdRng::from_rng(OsRng).unwrap());
        let malicious_key = BlsKeypair::generate(&mut StdRng::from_rng(OsRng).unwrap());
        let chain_spec = adiri_chain_spec_arc();
        let proof = generate_proof_of_possession_bls(&keypair, &chain_spec).unwrap();
        assert!(verify_proof_of_possession_bls(&proof, malicious_key.public(), &chain_spec).is_err())
    }

    #[test]
    fn test_proof_of_possession_fails_wrong_message() {
        let keypair = BlsKeypair::generate(&mut StdRng::from_rng(OsRng).unwrap());
        let chain_spec = adiri_chain_spec_arc();
        let mut wrong = adiri_genesis();
        wrong.timestamp = 0;
        let proof = generate_proof_of_possession_bls(&keypair, &wrong.into()).unwrap();
        assert!(verify_proof_of_possession_bls(&proof, keypair.public(), &chain_spec).is_err())
    }
}
