use futures_util::StreamExt;
use tn_types::consensus::{BatchDigest, CertificateDigest,};
use tn_network_types::BuildHeaderRequest;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll}, sync::Arc,
};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{error, warn, debug};

use crate::{BatchPayloadFuture, BatchPayload, BatchPayloadJobGenerator, LatticePayloadBuilderServiceMetrics, LatticePayloadBuilderHandle, HeaderPayloadJobGenerator, HeaderPayloadFuture, HeaderPayload};

/// A service that manages payload building tasks.
///
/// This type is an endless future that manages the building of payloads.
///
/// It tracks active payloads and their build jobs that run in the worker pool.
///
/// By design, this type relies entirely on the [LatticePayloadJobGenerator] to create new payloads and
/// knows nothing about how to build them, it just drives the payload jobs.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct LatticePayloadBuilderService<Gen>
where
    Gen: BatchPayloadJobGenerator + HeaderPayloadJobGenerator,
{
    /// The type that knows how to create new payloads.
    generator: Gen,
    /// Copy of the sender half, so new [`PayloadBuilderHandle`] can be created on demand.
    _service_tx: mpsc::UnboundedSender<LatticePayloadBuilderServiceCommand>,
    /// Receiver half of the command channel.
    command_rx: UnboundedReceiverStream<LatticePayloadBuilderServiceCommand>,
    /// metrics for the payload builder service
    metrics: LatticePayloadBuilderServiceMetrics,
}

// === impl PayloadBuilderService ===

impl<Gen> LatticePayloadBuilderService<Gen>
where
    Gen: BatchPayloadJobGenerator + HeaderPayloadJobGenerator,
{
    /// Creates a new payload builder service.
    pub fn new(generator: Gen) -> (Self, LatticePayloadBuilderHandle) {
        let (service_tx, command_rx) = mpsc::unbounded_channel();
        let service = Self {
            generator,
            _service_tx: service_tx.clone(),
            command_rx: UnboundedReceiverStream::new(command_rx),
            metrics: Default::default(),
        };
        let handle = LatticePayloadBuilderHandle { to_service: service_tx };
        (service, handle)
    }
}

impl<Gen> Future for LatticePayloadBuilderService<Gen>
where
    Gen: BatchPayloadJobGenerator + HeaderPayloadJobGenerator + Unpin + 'static,
    <Gen as BatchPayloadJobGenerator>::Job: Unpin + 'static,
    <Gen as HeaderPayloadJobGenerator>::Job: Unpin + 'static,
{
    type Output = ();

    // Note: this seems to pass the test, but I've left commented code out just in case I need to revisit this.
    //
    // originaly, this looped forever. I don't think that's necessary. This method wakes up when another message is sent,
    // so I'm removing the loop for now.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // loop {
            // marker for exit condition
            // TODO(mattsse): this could be optmized so we only poll new jobs
            // let mut new_job = false;

            // drain all requests
            while let Poll::Ready(Some(cmd)) = this.command_rx.poll_next_unpin(cx) {
                match cmd {
                    LatticePayloadBuilderServiceCommand::NewBatch(tx) => {
                        // TODO: should this error be handled differently?
                        match this.generator.new_batch_job() {
                            Ok(job) => {
                                let _ = tx.send(Box::pin(job));
                                this.metrics.inc_initiated_batch_jobs();
                            }
                            Err(e) => {
                                this.metrics.inc_failed_batch_jobs();
                                warn!("{e:?}");
                            },
                        }
                    }
                    LatticePayloadBuilderServiceCommand::BatchSealed { batch, digest } => {
                        // TODO: not sure how to handle this failure
                        // an error here would result in the duplicate transactions in the next batch
                        if let Err(e) = this.generator.batch_sealed(batch, digest) {
                            this.metrics.inc_failed_sealed_batches();
                            error!("{e:?}")
                        }
                        this.metrics.inc_sealed_batches();
                    }
                    LatticePayloadBuilderServiceCommand::NewHeader{ reply, attributes } => {
                        // add another function for generator to verify batches are all present
                        // if not, give the new_header_job(attributes, Some(missing))
                        // (tx, missing) = oneshot::channel()
                        //
                        // move get_and_validate_batch_transactions() to generator
                        //
                        // check all batches digests
                        //
                        // for the ones that don't 
                        match this.generator.new_header_job(attributes) {
                            Ok(job) => {
                                let _ = reply.send(Box::pin(job));
                                this.metrics.inc_initiated_header_jobs();
                            }
                            Err(e) => {
                                this.metrics.inc_failed_header_jobs();
                                warn!("{e:?}");
                            }
                        }
                    }
                    LatticePayloadBuilderServiceCommand::HeaderSealed { header, digest } => {
                        todo!()
                    }
                };
            }

            // if !new_job {
            //    return Poll::Pending
            // }

            Poll::Pending

        // }
    }
}

/// Command for the batch builder service.
pub enum LatticePayloadBuilderServiceCommand {
    /// Return a future job that builds a batch.
    NewBatch(oneshot::Sender<BatchPayloadFuture>),
    /// Update the transaction pool after a batch is sealed.
    BatchSealed{
        /// The reference to the built batch for updating the tx pool.
        batch: Arc<BatchPayload>,
        /// The digest of the sealed batch.
        digest: BatchDigest
    },
    /// Return a future job that builds a header.
    NewHeader {
        /// Channel to return the job future.
        reply: oneshot::Sender<HeaderPayloadFuture>,
        /// Attributes for the block to build.
        attributes: BuildHeaderRequest,
    },
    /// Update the transaction pool after a batch is sealed.
    HeaderSealed { // Certificate issued?
        /// The reference to the built batch for updating the tx pool.
        header: Arc<HeaderPayload>,
        /// The digest of the sealed batch.
        digest: CertificateDigest,
    },
}
