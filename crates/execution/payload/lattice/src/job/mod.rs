//! Mod containing payload job specifics.

use std::sync::{Arc, atomic::AtomicBool};

mod batch;
mod header;
mod block;
pub use batch::BatchPayload;
pub(crate) use batch::{BatchPayloadConfig, BatchPayloadJob, BatchPayloadFuture};
pub(crate) use header::*;
pub use block::*;

/// A marker that can be used to cancel a job.
///
/// If dropped, it will set the `cancelled` flag to true.
#[derive(Default, Clone, Debug)]
pub(super) struct Cancelled(Arc<AtomicBool>);

// === impl Cancelled ===

impl Cancelled {
    /// Returns true if the job was cancelled.
    pub(super) fn _is_cancelled(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Drop for Cancelled {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}
