//! Response message types
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

/// Response from peers after receiving a certificate.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SendCertificateResponse {
    pub accepted: bool,
}
/// Used by the primary to reply to RequestVoteRequest.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub vote: Option<Vote>,

    // Indicates digests of missing certificates without which a vote cannot be provided.
    pub missing: Vec<CertificateDigest>,
}
/// Used by the primary to reply to GetCertificatesRequest.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetCertificatesResponse {
    pub certificates: Vec<Certificate>,
}
/// Used by the primary to reply to FetchCertificatesRequest.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FetchCertificatesResponse {
    /// Certificates sorted from lower to higher rounds.
    pub certificates: Vec<Certificate>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct PayloadAvailabilityResponse {
    pub payload_availability: Vec<(CertificateDigest, bool)>,
}

impl PayloadAvailabilityResponse {
    pub fn available_certificates(&self) -> Vec<CertificateDigest> {
        self.payload_availability
            .iter()
            .filter_map(|(digest, available)| available.then_some(*digest))
            .collect()
    }
}
/// All batches requested by the primary.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FetchBatchesResponse {
    pub batches: HashMap<BatchDigest, Batch>,
}

/// Information for the workers.
#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct WorkerInfoResponse {
    /// Map of workers' id and their network addresses.
    pub workers: BTreeMap<WorkerId, WorkerInfo>,
}

// TODO: need to refactor the organization of these types to avoid
// circular dependencies.
/// The workaround to prevent circular dependencies is to use the same
/// fields for this message as `HeaderPayload` and cast from one to 
/// another.
/// 
/// The [HeaderPayloadResponse] message needs to be in this crate
/// so proto can build, but the type also lives in lattice-payload-builder.
/// The types used in the lattice-payload-builder are in other crates that
/// also depend on this crate, causing circular dependency issues.
/// For instance, `TransactionId` for `BatchPayload`.
/// 
/// The engine's response with a built header payload.
#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct HeaderPayloadResponse {
    /// Data from from the EL.
    /// 
    /// Contains header and block hash.
    /// 
    /// TODO: SealedHeader contains some redundant info for the EL
    /// header. This should be another type used for just consensus,
    /// but consolidate things like round/block number.
    pub sealed_header: SealedHeader,
}

// /// Payload response from PayloadBuilder to Workers.
// #[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Debug)]
// pub struct BatchPayloadResponse {
//     /// Collection of transactions encoded as bytes.
//     pub payload: Vec<Vec<u8>>,
//     /// Transaction ids for updating the pool after the batch is sealed.
//     executed_txs: Vec<TransactionId>,
//     /// The metric indicating why the batch was sealed.
//     size_metric: BatchPayloadSizeMetric,
// }

//=== Worker

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestBatchResponse {
    /// Batch
    pub batch: Option<Batch>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestBatchesResponse {
    pub batches: Vec<Batch>,
    /// If true, the primary should request the batches from the workers again.
    /// This may not be something that can be trusted from a remote worker.
    pub is_size_limit_reached: bool,
}
