use crate::engine::forkchoice::ForkchoiceStatus;
use execution_interfaces::consensus::ForkchoiceState;
use std::sync::Arc;
use tn_types::execution::SealedBlock;

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
