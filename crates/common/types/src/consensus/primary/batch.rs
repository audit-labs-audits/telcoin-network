use crate::consensus::{crypto, VersionedMetadata};
use consensus_util_mem::MallocSizeOf;
use enum_dispatch::enum_dispatch;
use fastcrypto::hash::{Digest, Hash, HashFunction};
#[cfg(any(test, feature = "arbitrary"))]
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Type that batches contain.
pub type Transaction = Vec<u8>;

/// The batch for workers to communicate for consensus.
#[cfg_attr(any(test, feature = "arbitrary"), derive(Arbitrary))]
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[enum_dispatch(BatchAPI)]
pub enum Batch {
    /// Version 1
    V1(BatchV1),
}

impl Batch {
    /// Create a new batch for testing only!
    ///
    /// This is not a valid batch for consensus. Metadata uses defaults.
    pub fn new(transactions: Vec<Transaction>) -> Self {
        Self::V1(BatchV1::new(transactions))
    }

    /// Size of the batch variant's inner data.
    pub fn size(&self) -> usize {
        match self {
            Batch::V1(data) => data.size(),
        }
    }
}

impl Hash<{ crypto::DIGEST_LENGTH }> for Batch {
    type TypedDigest = BatchDigest;

    fn digest(&self) -> BatchDigest {
        match self {
            Batch::V1(data) => data.digest(),
        }
    }
}

/// API for access data from versioned Batch variants.
///
/// TODO: update comments once EL data is finalized between Batch and VersionedMetadata
#[enum_dispatch]
pub trait BatchAPI {
    /// TODO
	fn transactions(&self) -> &Vec<Transaction>;
    /// TODO
	fn transactions_mut(&mut self) -> &mut Vec<Transaction>;
    /// TODO
	fn versioned_metadata(&self) -> &VersionedMetadata;
    /// TODO
	fn versioned_metadata_mut(&mut self) -> &mut VersionedMetadata;
    /// TODO
	fn owned_metadata(self) -> VersionedMetadata;
    /// TODO
	fn owned_transactions(&self) -> Vec<Transaction>;
}

/// The batch version.
///
/// akin to BatchV2 in sui
#[cfg_attr(any(test, feature = "arbitrary"), derive(Arbitrary))]
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct BatchV1 {
    /// List of transactions.
    /// 
    /// A batch `Transaction` is an encoded `TransactionSigned` from the EL.
    /// Recovering senders is CPU intensive, but so it network bandwidth. For this
    /// protocol, network bandwidth is more costly so only the signed version is sent.
    /// It is the responsibility of peers to recover signers and verify transactions.
    pub transactions: Vec<Transaction>,

    /// Metadata for batch.
    ///
    /// This field is not included as part of the batch digest
    pub versioned_metadata: VersionedMetadata,
}

impl BatchAPI for BatchV1 {
    fn transactions(&self) -> &Vec<Transaction> {
        &self.transactions
    }

    fn transactions_mut(&mut self) -> &mut Vec<Transaction> {
        &mut self.transactions
    }

    fn versioned_metadata(&self) -> &VersionedMetadata {
        &self.versioned_metadata
    }

    fn versioned_metadata_mut(&mut self) -> &mut VersionedMetadata {
        &mut self.versioned_metadata
    }

    fn owned_metadata(self) -> VersionedMetadata {
        self.versioned_metadata
    }

    fn owned_transactions(&self) -> Vec<Transaction> {
        self.transactions.clone()
    }
}

impl BatchV1 {
    /// Create a new BatchV1
    pub fn new(transactions: Vec<Transaction>) -> Self {
        Self {
            transactions,
            // Default metadata uses defaults for ExecutionPayload
            versioned_metadata: VersionedMetadata::default(),
        }
    }

    /// The size of the BatchV1 inner data
    pub fn size(&self) -> usize {
        self.transactions.iter().map(|t| t.len()).sum()
    }
}

/// Digest of the batch.
#[cfg_attr(any(test, feature = "arbitrary"), derive(Arbitrary))]
#[derive(
    Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq, Hash, PartialOrd, Ord, MallocSizeOf,
)]
pub struct BatchDigest(pub [u8; crypto::DIGEST_LENGTH]);

impl fmt::Debug for BatchDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", base64::encode(self.0))
    }
}

impl fmt::Display for BatchDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", base64::encode(self.0).get(0..16).ok_or(fmt::Error)?)
    }
}

impl From<BatchDigest> for Digest<{ crypto::DIGEST_LENGTH }> {
    fn from(digest: BatchDigest) -> Self {
        Digest::new(digest.0)
    }
}

impl BatchDigest {
    /// New BatchDigest
    pub fn new(val: [u8; crypto::DIGEST_LENGTH]) -> BatchDigest {
        BatchDigest(val)
    }
}

impl Hash<{ crypto::DIGEST_LENGTH }> for BatchV1 {
    type TypedDigest = BatchDigest;

    fn digest(&self) -> Self::TypedDigest {
        BatchDigest::new(
            crypto::DefaultHashFunction::digest_iterator(self.transactions.iter()).into(),
        )
    }
}
