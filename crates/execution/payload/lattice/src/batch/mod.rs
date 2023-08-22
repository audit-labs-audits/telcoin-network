//! Batch payload builder.
//! 
//! Worker requests the next batch from the engine.
//!
//! The engine returns the best transactions in the pending pool.
pub mod job;
pub mod generator;
mod helpers;
mod error;
pub use error::BatchBuilderError;
mod service;
pub use service::{BatchBuilderServiceCommand, BatchBuilderService};
mod handle;
pub use handle::BatchBuilderHandle;
mod metrics;
mod traits;
pub use traits::BatchJobGenerator;
