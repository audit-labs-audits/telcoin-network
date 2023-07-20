use crate::engine::forkchoice::ForkchoiceStatus;
use execution_interfaces::consensus::ForkchoiceState;
use execution_primitives::SealedBlock;
use std::sync::Arc;

/// Events emitted by [crate::BeaconConsensusEngine].
#[derive(Clone, Debug)]
pub enum BeaconConsensusEngineEvent {
    /// The fork choice state was updated.
    ForkchoiceUpdated(ForkchoiceState, ForkchoiceStatus),
    /// A block was added to the canonical chain.
    CanonicalBlockAdded(Arc<SealedBlock>),
    /// A block was added to the fork chain.
    ForkBlockAdded(Arc<SealedBlock>),
}
