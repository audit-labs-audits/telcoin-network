//! Traits for the batch builder.
//! 
//! Mostly used to reduce complexity when using generics in the batch job and generator.
use std::{future::Future, sync::Arc, collections::HashMap};
use indexmap::IndexMap;
use tn_types::consensus::{BatchDigest, CertificateDigest, WorkerId, TimestampSec, Batch, ConsensusOutput};
use tn_network_types::BuildHeaderRequest;
use tokio::sync::oneshot;
use crate::{HeaderPayload, BlockPayload};
use super::LatticePayloadBuilderError;

/// A type that knows how to create new jobs for creating payloads.
pub trait BatchPayloadJobGenerator: Send + Sync {
    /// The type that manages the lifecycle of a payload.
    ///
    /// This type is a Stream that yields better payloads.
    type Job: Future<Output = Result<(), LatticePayloadBuilderError>>
        + Send
        + Sync
        + 'static;

    /// Creates a batch from the best pending payloads.
    ///
    /// This is called when the CL requests a new batch.
    fn new_batch_job(
        &self,
        // attr: PayloadBuilderAttributes,
    ) -> Result<Self::Job, LatticePayloadBuilderError>;
}

/// A type that knows how to create new jobs for creating payloads.
pub trait HeaderPayloadJobGenerator: Send + Sync {
    /// The type that manages the lifecycle of a payload.
    ///
    /// This type is a Stream that yields better payloads.
    type Job: Future<Output = Result<Arc<HeaderPayload>, LatticePayloadBuilderError>>
        + Send
        + Sync
        + 'static;

    /// Creates a batch from the best pending payloads.
    ///
    /// This is called when the CL requests a new batch.
    fn new_header_job(
        &self,
        attr: BuildHeaderRequest,
    ) -> Result<Self::Job, LatticePayloadBuilderError>;

    /// Update the pool after a header is sealed.
    /// When a header is sealed, a certificate is issued.
    /// 
    /// TODO: this should be a part of the job?
    fn header_sealed(
        &self,
        header: Arc<HeaderPayload>,
        digest: CertificateDigest,
    ) -> Result<(), LatticePayloadBuilderError>;

    /// Verify the requested batches are in the sealed pool.
    /// 
    /// If a batch is missing or the calculated digest doesn't match
    /// the requested, then the builder requests the batch from the
    /// worker.
    /// 
    /// TODO: the missing batch is not added to the sealed pool. This
    /// can be done by taking the parent block from the batch and 
    /// calling `pool.add_transactions()` then `on_sealed_batch()`.
    ///
    /// But there may be a better approach and this isn't expected to
    /// happen under normal circumstances.
    fn verify_batches_present(
        &self,
        batch_digests: &IndexMap<BatchDigest, (WorkerId, TimestampSec)>
    ) -> Option<oneshot::Receiver<HashMap<BatchDigest, Batch>>>;
}

/// A type that creates the next canonical block from `ConsensusOutput`.
pub trait BlockPayloadJobGenerator: Send + Sync {
    /// The type that manages the lifecycle of a payload.
    ///
    /// This type is a Stream that yields better payloads.
    type Job: Future<Output = Result<Arc<BlockPayload>, LatticePayloadBuilderError>>
        + Send
        + Sync
        + 'static;

    /// Creates the next block for the canonical chain.
    ///
    /// This is called when the CL reaches consensus.
    fn new_canonical_block(
        &self,
        output: ConsensusOutput,
    ) -> Result<Self::Job, LatticePayloadBuilderError>;
}
