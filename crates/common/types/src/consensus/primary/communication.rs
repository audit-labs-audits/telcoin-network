use crate::consensus::{
    crypto::NetworkPublicKey,
    Batch, BatchDigest, Certificate, CertificateDigest, Header, Round, VersionedMetadata, Vote, AuthorityIdentifier, WorkerId, WorkerInfo, Epoch,
};
use crate::execution::{H256, SealedHeader};
use indexmap::IndexMap;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fmt,
};
use tracing::warn;

use super::{TimestampSec, HeaderAPI};
// /// Used by the primary to request that the worker delete the specified batches.
// #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
// pub struct WorkerDeleteBatchesMessage {
//     pub digests: Vec<BatchDigest>,
// }

// #[derive(Clone, Debug, Eq, PartialEq)]
// pub struct BatchMessage {
//     // TODO: revisit including the digest here [see #188]
//     pub digest: BatchDigest,
//     pub batch: Batch,
// }

// #[derive(Debug, Clone, Eq, PartialEq)]
// pub enum BlockErrorKind {
//     BlockNotFound,
//     BatchTimeout,
//     BatchError,
// }

// pub type BlockResult<T> = Result<T, BlockError>;

// #[derive(Debug, Clone, Eq, PartialEq)]
// pub struct BlockError {
//     pub digest: CertificateDigest,
//     pub error: BlockErrorKind,
// }

// impl<T> From<BlockError> for BlockResult<T> {
//     fn from(error: BlockError) -> Self {
//         Err(error)
//     }
// }

// impl fmt::Display for BlockError {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(f, "block digest: {}, error type: {}", self.digest, self.error)
//     }
// }

// impl fmt::Display for BlockErrorKind {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(f, "{:?}", self)
//     }
// }
