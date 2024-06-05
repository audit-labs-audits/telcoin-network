//! The implementation for building TN payloads once consensus is reached.
//! 
//! This mod is inspired by reth's default_ethereum_payload_builder.
//!
//! Few adjustments:
//! - instead of `Pool`, transactions come from `ConsensusOutput`
//!   - payloads are built per batch in order of consensus
//! - no cached reads since the state is only accessed during this execution, unlike eth payload building
//! 
//! Optimization: save execution state and pass to next payload builder
//! while the block is being written to db.

use reth_provider::StateProviderBox;
use reth_revm::{database::StateProviderDatabase, State};

#[inline]
pub(super) fn handle_consensus_output(
    state_provider: StateProviderBox,
) -> eyre::Result<()> {
    let state = StateProviderDatabase::new(state_provider);
    // TODO: what does .with_bundle_update do? some sort of auto impl
    let mut db = State::builder().with_database(state).with_bundle_update().build();
    
    // implement payload builder attributes on ConsensusOutput

    
    todo!()
}
