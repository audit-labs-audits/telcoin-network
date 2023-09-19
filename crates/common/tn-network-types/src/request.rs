//! Request message types
use anemo::types::Version;
use tn_types::{
    consensus::{
        crypto::NetworkPublicKey,
        Batch, BatchDigest, Certificate, CertificateDigest, Header, Round, VersionedMetadata, Vote, AuthorityIdentifier, WorkerId, WorkerInfo, Epoch,
        TimestampSec, HeaderAPI,
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

/// Request for broadcasting certificates to peers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SendCertificateRequest {
    pub certificate: Certificate,
}

/// Used by the primary to request a vote from other primaries on newly produced headers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    pub header: Header,

    // Optional parent certificates provided by the requester, in case this primary doesn't yet
    // have them and requires them in order to offer a vote.
    pub parents: Vec<Certificate>,
}

/// Used by the primary to get specific certificates from other primaries.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetCertificatesRequest {
    pub digests: Vec<CertificateDigest>,
}

/// Used by the primary to fetch certificates from other primaries.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchCertificatesRequest {
    /// The exclusive lower bound is a round number where each primary should return certificates
    /// above that. This corresponds to the GC round at the requestor.
    pub exclusive_lower_bound: Round,
    /// This contains per authority serialized RoaringBitmap for the round diffs between
    /// - rounds of certificates to be skipped from the response and
    /// - the GC round.
    /// These rounds are skipped because the requestor already has them.
    pub skip_rounds: Vec<(AuthorityIdentifier, Vec<u8>)>,
    /// Maximum number of certificates that should be returned.
    pub max_items: usize,
}

impl FetchCertificatesRequest {
    #[allow(clippy::mutable_key_type)]
    pub fn get_bounds(&self) -> (Round, BTreeMap<AuthorityIdentifier, BTreeSet<Round>>) {
        let skip_rounds: BTreeMap<AuthorityIdentifier, BTreeSet<Round>> = self
            .skip_rounds
            .iter()
            .filter_map(|(k, serialized)| {
                match RoaringBitmap::deserialize_from(&mut &serialized[..]) {
                    Ok(bitmap) => {
                        let rounds: BTreeSet<Round> = bitmap
                            .into_iter()
                            .map(|r| self.exclusive_lower_bound + r as Round)
                            .collect();
                        Some((*k, rounds))
                    }
                    Err(e) => {
                        warn!("Failed to deserialize RoaringBitmap {e}");
                        None
                    }
                }
            })
            .collect();
        (self.exclusive_lower_bound, skip_rounds)
    }

    #[allow(clippy::mutable_key_type)]
    pub fn set_bounds(
        mut self,
        gc_round: Round,
        skip_rounds: BTreeMap<AuthorityIdentifier, BTreeSet<Round>>,
    ) -> Self {
        self.exclusive_lower_bound = gc_round;
        self.skip_rounds = skip_rounds
            .into_iter()
            .map(|(k, rounds)| {
                let mut serialized = Vec::new();
                rounds
                    .into_iter()
                    .map(|v| u32::try_from(v - gc_round).unwrap())
                    .collect::<RoaringBitmap>()
                    .serialize_into(&mut serialized)
                    .unwrap();
                (k, serialized)
            })
            .collect();
        self
    }

    pub fn set_max_items(mut self, max_items: usize) -> Self {
        self.max_items = max_items;
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct PayloadAvailabilityRequest {
    pub certificate_digests: Vec<CertificateDigest>,
}


/// Used by the primary to request that the worker sync the target missing batches.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerSynchronizeMessage {
    pub digests: Vec<BatchDigest>,
    pub target: AuthorityIdentifier,
    // Used to indicate to the worker that it does not need to fully validate
    // the batch it receives because it is part of a certificate. Only digest
    // verification is required.
    pub is_certified: bool,
}

/// Used by the primary to request that the worker fetch the missing batches and reply
/// with all of the content.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FetchBatchesRequest {
    pub digests: HashSet<BatchDigest>,
    pub known_workers: HashSet<NetworkPublicKey>,
}

/// Used by the Engine to request missing batches from the worker's store
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MissingBatchesRequest {
    pub digests: HashSet<BatchDigest>,
}

impl From<HashSet<BatchDigest>> for MissingBatchesRequest {
    fn from(digests: HashSet<BatchDigest>) -> Self {
        Self { digests }
    }
}

/// Message for engine to build the next header using the
/// batch digests.
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct BuildHeaderRequest {
    /// The round for this header.
    pub round: Round,
    /// The epoch for this header.
    pub epoch: Epoch,
    /// The timestamp for this header.
    pub created_at: TimestampSec,
    /// The ordered lists of batch digests and the worker responsible
    /// for pulling from from storage if transactions are missing.
    pub payload: IndexMap<BatchDigest, (WorkerId, TimestampSec)>,
    /// The parents for this block.
    pub parents: BTreeSet<CertificateDigest>,
}

//=== Workers

/// Used by primary to ask worker for the request.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestBatchRequest {
    /// Requested batch's digest
    pub batch: BatchDigest,
}

/// Used by primary to bulk request batches from workers local store.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestBatchesRequest {
    /// Vec of requested batches' digests
    pub batch_digests: Vec<BatchDigest>,
}

/// Used by primary to ask worker for the request.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BuildBatchRequest {
    /// The worker_id for the batch.
    pub worker_id: WorkerId,
}

impl From<WorkerId> for BuildBatchRequest {
    fn from(worker_id: WorkerId) -> Self {
        Self { worker_id }
    }
}

/// Engine to worker after a batch is built.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SealBatchRequest {
    /// Collection of transactions encoded as bytes.
    pub payload: Vec<Vec<u8>>,
    /// Execution data for validation.
    pub metadata: VersionedMetadata,
}

/// Used by workers to validate a peer's batch using EL.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ValidateBatchRequest {
    /// The peer's batch to validate.
    pub batch: Batch,
    /// The worker's id.
    /// 
    /// TODO: this is redundant because
    /// there is a method on anemo requests, but it returns
    /// an option which makes me wonder if it's reliably present.
    pub worker_id: WorkerId,
}

impl From<SealBatchRequest> for Batch {
    fn from(value: SealBatchRequest) -> Self {
        Batch::new_with_metadata(value.payload, value.metadata)
    }
}
