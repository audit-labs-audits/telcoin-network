use futures_util::StreamExt;
use tn_types::consensus::BatchDigest;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll}, sync::Arc,
};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{error, warn, debug};

use super::{job::BatchPayloadFuture, handle::BatchBuilderHandle, metrics::BatchBuilderServiceMetrics, traits::BatchJobGenerator, generator::BuiltBatch};

/// A service that manages payload building tasks.
///
/// This type is an endless future that manages the building of payloads.
///
/// It tracks active payloads and their build jobs that run in the worker pool.
///
/// By design, this type relies entirely on the [BatchJobGenerator] to create new payloads and
/// knows nothing about how to build them, it just drives the payload jobs.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct BatchBuilderService<Gen>
where
    Gen: BatchJobGenerator,
{
    /// The type that knows how to create new payloads.
    generator: Gen,
    /// Copy of the sender half, so new [`PayloadBuilderHandle`] can be created on demand.
    _service_tx: mpsc::UnboundedSender<BatchBuilderServiceCommand>,
    /// Receiver half of the command channel.
    command_rx: UnboundedReceiverStream<BatchBuilderServiceCommand>,
    /// metrics for the payload builder service
    metrics: BatchBuilderServiceMetrics,
}

// === impl PayloadBuilderService ===

impl<Gen> BatchBuilderService<Gen>
where
    Gen: BatchJobGenerator,
{
    /// Creates a new payload builder service.
    pub fn new(generator: Gen) -> (Self, BatchBuilderHandle) {
        let (service_tx, command_rx) = mpsc::unbounded_channel();
        let service = Self {
            generator,
            _service_tx: service_tx.clone(),
            command_rx: UnboundedReceiverStream::new(command_rx),
            metrics: Default::default(),
        };
        let handle = BatchBuilderHandle { to_service: service_tx };
        (service, handle)
    }
}

impl<Gen> Future for BatchBuilderService<Gen>
where
    Gen: BatchJobGenerator + Unpin + 'static,
    <Gen as BatchJobGenerator>::Job: Unpin + 'static,
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
                    BatchBuilderServiceCommand::NewPayload(tx) => {
                        // TODO: should this error be handled differently?
                        match this.generator.new_batch_job() {
                            Ok(job) => {
                                let _ = tx.send(Box::pin(job));
                                this.metrics.inc_initiated_jobs();
                            }
                            Err(e) => {
                                this.metrics.inc_failed_jobs();
                                warn!("{e:?}");
                            },
                        }
                    }
                    BatchBuilderServiceCommand::BatchSealed { batch, digest } => {
                        // TODO: not sure how to handle this failure
                        // an error here would result in the duplicate transactions in the next batch
                        if let Err(e) = this.generator.batch_sealed(batch, digest) {
                            this.metrics.inc_failed_sealed_batches();
                            error!("{e:?}")
                        }
                        this.metrics.inc_sealed_batches();
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
pub enum BatchBuilderServiceCommand {
    /// Return a future for the worker to wait.
    NewPayload(oneshot::Sender<BatchPayloadFuture>),
    /// Update the transaction pool after a batch is sealed.
    BatchSealed{
        /// The reference to the built batch for updating the tx pool.
        batch: Arc<BuiltBatch>,
        /// The digest of the sealed batch.
        digest: BatchDigest
    },
}
