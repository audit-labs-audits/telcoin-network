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

pub type PublicKey = BLS12381PublicKey;
pub type PublicKeyBytes = BLS12381PublicKeyAsBytes;
pub type Signature = BLS12381Signature;
pub type AggregateSignature = BLS12381AggregateSignature;
pub type AggregateSignatureBytes = BLS12381AggregateSignatureAsBytes;
pub type PrivateKey = BLS12381PrivateKey;
pub type KeyPair = BLS12381KeyPair;
pub type NetworkPublicKey = Ed25519PublicKey;
pub type NetworkKeyPair = Ed25519KeyPair;

////////////////////////////////////////////////////////////////////////

// Type alias selecting the default hash function for the code base.
pub type DefaultHashFunction = Blake2b256;
pub const DIGEST_LENGTH: usize = DefaultHashFunction::OUTPUT_SIZE;
pub const INTENT_MESSAGE_LENGTH: usize = INTENT_PREFIX_LENGTH + DIGEST_LENGTH;

// TODO: epoch here
// pub const DEFAULT_EPOCH_ID: EpochId = 0;
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
        author: &PublicKey,
    ) -> Result<(), FastCryptoError>
    where
        T: Serialize;
}

impl NarwhalAuthoritySignature for Signature {
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
        public_key: &PublicKey,
    ) -> Result<(), FastCryptoError>
    where
        T: Serialize,
    {
        let message = bcs::to_bytes(&value).expect("Message serialization should not fail");
        public_key.verify(&message, self)
    }
}

pub trait NarwhalAuthorityAggregateSignature {
    fn verify_secure<T>(
        &self,
        value: &IntentMessage<T>,
        pks: &[PublicKey],
    ) -> Result<(), FastCryptoError>
    where
        T: Serialize;
}

impl NarwhalAuthorityAggregateSignature for AggregateSignature {
    fn verify_secure<T>(
        &self,
        value: &IntentMessage<T>,
        pks: &[PublicKey],
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
