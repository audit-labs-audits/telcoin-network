// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
pub mod helpers;
pub mod intent;

use fastcrypto::{
    bls12381::min_sig::{
        BLS12381AggregateSignature, BLS12381AggregateSignatureAsBytes, BLS12381KeyPair,
        BLS12381PrivateKey, BLS12381PublicKey, BLS12381PublicKeyAsBytes, BLS12381Signature,
    },
    ed25519::{Ed25519KeyPair, Ed25519PublicKey},
    error::FastCryptoError,
    hash::{Blake2b256, HashFunction},
    traits::{AggregateAuthenticator, Signer, VerifyingKey},
};

// This re-export allows using the trait-defined APIs
use self::intent::{Intent, IntentMessage, IntentScope, INTENT_PREFIX_LENGTH};
pub use fastcrypto::traits;
use serde::Serialize;

////////////////////////////////////////////////////////////////////////
/// Type aliases selecting the signature algorithm for the code base.
////////////////////////////////////////////////////////////////////////
// Here we select the types that are used by default in the code base.
// The whole code base should only:
// - refer to those aliases and not use the individual scheme implementations
// - not use the schemes in a way that break genericity (e.g. using their Struct impl functions)
// - swap one of those aliases to point to another type if necessary
//
// Beware: if you change those aliases to point to another scheme implementation, you will have
// to change all four aliases to point to concrete types that work with each other. Failure to do
// so will result in a ton of compilation errors, and worse: it will not make sense!

/// Keypair for this authority.
pub type AuthorityKeyPair = BLS12381KeyPair;
/// Public key for this authority.
pub type AuthorityPublicKey = BLS12381PublicKey;
/// Private key for this authority.
pub type AuthorityPrivateKey = BLS12381PrivateKey;
/// Public key for this authority as bytes.
pub type AuthorityPublicKeyBytes = BLS12381PublicKeyAsBytes;
/// BLS signature.
pub type AuthoritySignature = BLS12381Signature;
/// BLS12381 aggregate signature.
pub type AggregateAuthoritySignature = BLS12381AggregateSignature;
/// BLS12381 aggregate signature bytes.
pub type AggregateAuthoritySignatureBytes = BLS12381AggregateSignatureAsBytes;
/// Public key for this authority's consensus messages between other nodes.
pub type NetworkPublicKey = Ed25519PublicKey;
/// Keypair for this authority's consensus messages between other nodes.
pub type NetworkKeyPair = Ed25519KeyPair;

////////////////////////////////////////////////////////////////////////

/// Type alias selecting the default hash function for the code base.
pub type DefaultHashFunction = Blake2b256;
/// Default hash expected output.
pub const DIGEST_LENGTH: usize = DefaultHashFunction::OUTPUT_SIZE;
/// TODO: Intent message length may not be needed.
pub const INTENT_MESSAGE_LENGTH: usize = INTENT_PREFIX_LENGTH + DIGEST_LENGTH;

// pub const DEFAULT_EPOCH_ID: EpochId = 0;
/// The default epoch id.
/// TODO: use EpochId type alias here
pub const DEFAULT_EPOCH_ID: u64 = 0;

/// A trait for sign and verify over an intent message, instead of the message itself. See more at
/// [struct IntentMessage].
pub trait NarwhalAuthoritySignature {
    /// Create a new signature over an intent message.
    fn new_secure<T>(value: &IntentMessage<T>, secret: &dyn Signer<Self>) -> Self
    where
        T: Serialize;

    /// Verify the signature on an intent message against the public key.
    fn verify_secure<T>(
        &self,
        value: &IntentMessage<T>,
        author: &AuthorityPublicKey,
    ) -> Result<(), FastCryptoError>
    where
        T: Serialize;
}

impl NarwhalAuthoritySignature for AuthoritySignature {
    fn new_secure<T>(value: &IntentMessage<T>, secret: &dyn Signer<Self>) -> Self
    where
        T: Serialize,
    {
        let message = bcs::to_bytes(&value).expect("Message serialization should not fail");
        secret.sign(&message)
    }

    fn verify_secure<T>(
        &self,
        value: &IntentMessage<T>,
        public_key: &AuthorityPublicKey,
    ) -> Result<(), FastCryptoError>
    where
        T: Serialize,
    {
        let message = bcs::to_bytes(&value).expect("Message serialization should not fail");
        public_key.verify(&message, self)
    }
}

/// Trait for verifying authority aggregate signature.
pub trait NarwhalAuthorityAggregateSignature {
    /// Verify the aggregate signatures are valid.
    fn verify_secure<T>(
        &self,
        value: &IntentMessage<T>,
        pks: &[AuthorityPublicKey],
    ) -> Result<(), FastCryptoError>
    where
        T: Serialize;
}

impl NarwhalAuthorityAggregateSignature for AggregateAuthoritySignature {
    fn verify_secure<T>(
        &self,
        value: &IntentMessage<T>,
        pks: &[AuthorityPublicKey],
    ) -> Result<(), FastCryptoError>
    where
        T: Serialize,
    {
        let message = bcs::to_bytes(&value).expect("Message serialization should not fail");
        self.verify(pks, &message)
    }
}

/// Wrap a message in an intent message. Currently in Narwhal, the scope is always
/// IntentScope::HeaderDigest and the app id is AppId::Narwhal.
pub fn to_intent_message<T>(value: T) -> IntentMessage<T> {
    IntentMessage::new(Intent::narwhal_app(IntentScope::HeaderDigest), value)
}
