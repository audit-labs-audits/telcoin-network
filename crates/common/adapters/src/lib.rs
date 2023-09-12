//! Adapters for implementing cross-layer communication.
use lattice_payload_builder::LatticePayloadBuilderHandle;
use execution_lattice_consensus::LatticeConsensusEngineHandle;
use async_trait::async_trait;
use tn_network_types::{WorkerToEngine, BuildBatchRequest, BatchPayloadResponse};

/// Adapter to router local communication requests between consensus
/// and execution layers.
/// 
/// The [NetworkAdapter] impl traits and routes requests using handles
/// to build payloads and validate peer proposals.
pub struct NetworkAdapter {
    /// Handle for building payloads for this node to propose.
    payload_builder: LatticePayloadBuilderHandle,
    // /// Handle for validating payloads from other peers and 
    // /// update the blockchain tree.
    // consensus_engine: LatticeConsensusEngineHandle,
}

impl NetworkAdapter {
    /// Create a new instance of [Self].
    pub fn new(
        payload_builder: LatticePayloadBuilderHandle,
        // consensus_engine: LatticeConsensusEngineHandle,
    ) -> Self {
        Self { payload_builder }
        // Self { payload_builder, consensus_engine }
    }
}

/// Implement the receiving side of PrimaryToEngine trait for the 
/// handle to the payload builder service.
#[async_trait]
impl WorkerToEngine for NetworkAdapter {
    async fn build_batch(
        &self,
        _request: anemo::Request<BuildBatchRequest>,
    ) -> Result<anemo::Response<()>, anemo::rpc::Status> {
        // 1. call payload builder and await job to build the batch
        // 2. generator sends the batch to the quorum waiter
        // 3. return Ok(()) to batch_maker
        match self.payload_builder.new_batch().await {
            Ok(()) => Ok(anemo::Response::new(())),
            Err(e) => Err(anemo::rpc::Status::internal(e.to_string())),
        }
    }
}
