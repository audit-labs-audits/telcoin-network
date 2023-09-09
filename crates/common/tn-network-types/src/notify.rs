//! Notification message types.
//! 
//! These messages are passed as unreliable send and
//! don't expect a response.
use tn_types::{
    consensus::{
        crypto::NetworkPublicKey,
        Batch, BatchDigest, Certificate, CertificateDigest, Header, Round, VersionedMetadata, Vote, AuthorityIdentifier, WorkerId, WorkerInfo, Epoch,
        TimestampMs, HeaderAPI,
    },
    execution::{H256, SealedHeader},
};
use indexmap::IndexMap;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fmt,
};
use tracing::warn;

/// Used by worker to inform primary it sealed a new batch.
#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct WorkerOwnBatchMessage {
    /// The batch digest.
    pub digest: BatchDigest,
    /// The worker's id.
    pub worker_id: WorkerId,
    /// The metadata for the sealed batch.
    pub metadata: VersionedMetadata,
}

/// Used by worker to inform primary it received a batch from another authority.
#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct WorkerOthersBatchMessage {
    /// The batch digest.
    pub digest: BatchDigest,
    /// The worker's id.
    pub worker_id: WorkerId,
}

/// Used by workers to send a new batch.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerBatchMessage {
    /// Batch
    pub batch: Batch,
}