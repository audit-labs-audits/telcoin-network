use fastcrypto::traits::{KeyPair as _, ToFromBytes};
use crate::{execution::Address, consensus::error::CryptoError};
use super::{
    intent::{Intent, IntentMessage, IntentScope},
    KeyPair, Signature, DEFAULT_EPOCH_ID, PublicKey,
    NarwhalAuthoritySignature,
};

/// Creates a proof of that the authority account address is owned by the
/// holder of authority protocol key, and also ensures that the authority
/// protocol public key exists. A proof of possession is an authority
/// signature committed over the intent message `intent || message || epoch` (See
/// more at [struct IntentMessage] and [struct Intent]) where the message is
/// constructed as `authority_pubkey_bytes || authority_account_address`.
pub fn generate_proof_of_possession(
    keypair: &KeyPair,
    address: Address,
) -> Signature {
    let mut msg: Vec<u8> = Vec::new();
    msg.extend_from_slice(keypair.public().as_bytes());
    msg.extend_from_slice(address.as_ref());
    // Signature::new_secure(
    //     &IntentMessage::new(Intent::sui_app(IntentScope::ProofOfPossession), msg),
    //     &DEFAULT_EPOCH_ID,
    //     keypair,
    // )
    todo!("not sure we need this")
}

/// Verify proof of possession against the expected intent message,
/// consisting of the protocol pubkey and the authority account address.
pub fn verify_proof_of_possession(
    pop: &Signature,
    protocol_pubkey: &PublicKey,
    address: Address,
) -> Result<(), CryptoError> {
    protocol_pubkey
        .validate()
        .map_err(|_| CryptoError::InvalidSignature)?;
    let mut msg = protocol_pubkey.as_bytes().to_vec();
    msg.extend_from_slice(address.as_ref());
    // pop.verify_secure(
    //     &IntentMessage::new(Intent::sui_app(IntentScope::ProofOfPossession), msg),
    //     DEFAULT_EPOCH_ID,
    //     protocol_pubkey.into(),
    // )
    todo!("not sure we need this")
}
