use crate::engine::forkchoice::ForkchoiceStatus;
use execution_interfaces::consensus::ForkchoiceState;
use std::sync::Arc;
use tn_types::{consensus::Batch, execution::SealedBlock};

/// Events emitted by [crate::LatticeConsensusEngine].
#[derive(Clone, Debug)]
pub enum LatticeConsensusEngineEvent {
    /// The fork choice state was updated.
    ForkchoiceUpdated(ForkchoiceState, ForkchoiceStatus),
    /// A block was added to the canonical chain.
    CanonicalBlockAdded(Arc<SealedBlock>),
    /// A block was added to the fork chain.
    ForkBlockAdded(Arc<SealedBlock>),

    /// A batch was verified and added to the "seen" tx-pool.
    ///
    /// Akin to `ForkBlockAdded` in Beacon.
    BatchVerified(Batch),

    /// A batch was sent to workers for consensus.
    ///
    /// Akin to CL requesting next payload - get_payload_v2()
    BatchCreated(Batch),
    // /// Consensus - akin to CanonicalBlockAdded / ForkchoiceUpdated
    // CertificateIssued(Certificate)
}
