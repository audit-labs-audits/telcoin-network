#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs, unreachable_pub, unused_crate_dependencies)]
#![deny(unused_must_use, rust_2018_idioms)]
#![doc(test(
    no_crate_inject,
    attr(deny(warnings, rust_2018_idioms), allow(dead_code, unused_variables))
))]
//! Lattice consensus implementation.

mod lattice_consensus;
pub use lattice_consensus::LatticeConsensus;

mod engine;
pub use engine::*;



// TODO: need to update sync state based on NW Consensus layer's status
//
// for now, do nothing
//
// See: NetworkHandle for real impl

/// Network sync adapter for Narwhal consensus
/// is stubbed out.
#[derive(Debug, Default)]
pub struct LatticeNetworkAdapter;
// TODO: link this with Narwhal?
//
// how can NW CL tell if we're syncing?
impl execution_interfaces::sync::NetworkSyncUpdater for LatticeNetworkAdapter {
    // do nothing
    fn update_sync_state(&self, _state: execution_interfaces::sync::SyncState) {}

    // do nothing
    fn update_status(&self, _head: execution_primitives::Head) {}
}
