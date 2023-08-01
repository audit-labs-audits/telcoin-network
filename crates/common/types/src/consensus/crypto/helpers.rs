// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

//! Helper functions for authority keys.
use super::{
    intent::{Intent, IntentMessage, IntentScope},
    AuthorityKeyPair, NarwhalAuthoritySignature, AuthorityPublicKey, AuthoritySignature, DEFAULT_EPOCH_ID, NetworkKeyPair,
};
use crate::{consensus::error::CryptoError, execution::Address};
use fastcrypto::traits::{KeyPair as KeypairTraits, ToFromBytes};
use rand::{rngs::StdRng, SeedableRng};

/// Creates a proof of that the authority account address is owned by the
/// holder of authority protocol key, and also ensures that the authority
/// protocol public key exists. A proof of possession is an authority
/// signature committed over the intent message `intent || message || epoch` (See
/// more at [struct IntentMessage] and [struct Intent]) where the message is
/// constructed as `authority_pubkey_bytes || authority_account_address`.
pub fn generate_proof_of_possession(keypair: &AuthorityKeyPair, address: Address) -> AuthoritySignature {
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
    pop: &AuthoritySignature,
    protocol_pubkey: &AuthorityPublicKey,
    address: Address,
) -> Result<(), CryptoError> {
    protocol_pubkey.validate().map_err(|_| CryptoError::InvalidSignature)?;
    let mut msg = protocol_pubkey.as_bytes().to_vec();
    msg.extend_from_slice(address.as_ref());
    // pop.verify_secure(
    //     &IntentMessage::new(Intent::sui_app(IntentScope::ProofOfPossession), msg),
    //     DEFAULT_EPOCH_ID,
    //     protocol_pubkey.into(),
    // )
    todo!("not sure we need this")
}

/// Generate a random keys needed to create an [Authority]
pub fn random_num_keys_for_authority(size: usize) -> Vec<(AuthorityKeyPair, NetworkKeyPair)> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..size)
        .map(|_| { (AuthorityKeyPair::generate(&mut rng), NetworkKeyPair::generate(&mut rng)) }).collect()
}
