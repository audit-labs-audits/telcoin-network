use serde::{Deserialize, Serialize};
use tn_network_libp2p::TNMessage;
use tn_types::{Batch, BlockHash, SealedBatch};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestBatchesResponse {
    /// Requested batches.
    pub batches: Vec<Batch>,
    /// If true, the primary should request the batches from the workers again.
    /// This may not be something that can be trusted from a remote worker.
    pub is_size_limit_reached: bool,
}

/// Worker messages on the gossip network.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkerGossip {
    /// A new is available.
    Batch(BlockHash),
}

// impl TNMessage trait for types
impl TNMessage for WorkerRequest {}
impl TNMessage for WorkerResponse {}

/// Requests from Worker.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkerRequest {
    /// Send a new batch to a peer.
    ReportBatch { sealed_batch: SealedBatch },
    /// Request batches by digest from a peer.
    RequestBatches { batch_digests: Vec<BlockHash> },
}

//
//
//=== Response types
//
//

/// Response to worker requests.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkerResponse {
    ReportBatch,
    RequestBatches(RequestBatchesResponse),
    /// RPC error while handling request.
    ///
    /// This is an application-layer error response.
    Error(WorkerRPCError),
}

impl WorkerResponse {
    /// Helper method if the response is an error.
    pub fn is_err(&self) -> bool {
        matches!(self, WorkerResponse::Error(_))
    }
}

impl From<WorkerRPCError> for WorkerResponse {
    fn from(value: WorkerRPCError) -> Self {
        Self::Error(value)
    }
}

/// Application-specific error type while handling Worker request.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct WorkerRPCError(pub String);
