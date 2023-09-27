use lattice_payload_builder::BlockPayload;
use std::sync::Arc;
use tn_types::consensus::Batch;

/// Events emitted by [crate::LatticeConsensusEngine].
#[derive(Clone, Debug)]
pub enum LatticeConsensusEngineEvent {
    /// A block was added to the canonical chain.
    CanonicalBlockAdded(Arc<BlockPayload>),

    /// A batch was verified and added to the "seen" tx-pool.
    ///
    /// Akin to `ForkBlockAdded` in Beacon.
    BatchVerified(Batch),
}
