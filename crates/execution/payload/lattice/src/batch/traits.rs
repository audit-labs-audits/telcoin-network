//! Traits for the batch builder.
//! 
//! Mostly used to reduce complexity when using generics in the batch job and generator.
use std::{future::Future, sync::Arc};
use tn_types::consensus::BatchDigest;
use super::{generator::BuiltBatch, BatchBuilderError};

/// A type that knows how to create new jobs for creating payloads.
pub trait BatchJobGenerator: Send + Sync {
    /// The type that manages the lifecycle of a payload.
    ///
    /// This type is a Stream that yields better payloads.
    type Job: Future<Output = Result<Arc<BuiltBatch>, BatchBuilderError>>
        + Send
        + Sync
        + 'static;

    /// Creates a batch from the best pending payloads.
    ///
    /// This is called when the CL requests a new batch.
    fn new_batch_job(
        &self,
        // attr: PayloadBuilderAttributes,
    ) -> Result<Self::Job, BatchBuilderError>;

    /// Update the pool after a batch is sealed.
    /// 
    /// TODO: this should be a part of the job?
    fn batch_sealed(
        &self,
        batch: Arc<BuiltBatch>,
        digest: BatchDigest,
    ) -> Result<(), BatchBuilderError>;
}
