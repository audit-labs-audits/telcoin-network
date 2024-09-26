// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{block_provider::MAX_PARALLEL_BLOCK, metrics::WorkerMetrics};
use consensus_metrics::{metered_channel::Receiver, monitored_future, spawn_logged_monitored_task};
use futures::stream::{futures_unordered::FuturesUnordered, StreamExt as _};
use narwhal_network::{CancelOnDropHandler, ReliableNetwork};
use narwhal_network_types::WorkerBlockMessage;
use std::{
    future::Future,
    pin::{pin, Pin},
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tn_types::{Authority, Committee, Noticer, Stake, WorkerBlock, WorkerCache, WorkerId};
use tokio::{
    sync::oneshot,
    task::JoinHandle,
    time::{error::Elapsed, timeout},
};
use tracing::{trace, warn};

#[cfg(test)]
#[path = "tests/quorum_waiter_tests.rs"]
pub mod quorum_waiter_tests;

/// Basically BoxFuture but without the unneeded lifetime.
type QMBoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// The results of a pipelined future.
type PipelinePayload =
    (WorkerBlock, Option<oneshot::Sender<()>>, FuturesUnordered<QMBoxFuture<u64>>);

/// The QuorumWaiter waits for 2f authorities to acknowledge reception of a batch.
pub struct QuorumWaiter {
    /// This authority.
    authority: Authority,
    /// The id of this worker.
    id: WorkerId,
    /// The committee information.
    committee: Committee,
    /// The worker information cache.
    worker_cache: WorkerCache,
    /// Receiver for shutdown.
    rx_shutdown: Noticer,
    /// Input Channel to receive commands.
    rx_quorum_waiter: Receiver<(WorkerBlock, oneshot::Sender<()>)>,
    /// A network sender to broadcast the batches to the other workers.
    network: anemo::Network,
    /// Record metrics for quorum waiter.
    metrics: Arc<WorkerMetrics>,
    /// Pipeline of in-flight quorum waiter commands.
    pipeline: FuturesUnordered<QMBoxFuture<PipelinePayload>>,
    /// In-flight left over commands after consensus was reached.
    best_effort_with_timeout: FuturesUnordered<QMBoxFuture<Result<(), Elapsed>>>,
}

impl QuorumWaiter {
    /// Spawn a new QuorumWaiter.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn spawn(
        authority: Authority,
        id: WorkerId,
        committee: Committee,
        worker_cache: WorkerCache,
        rx_shutdown: Noticer,
        rx_quorum_waiter: Receiver<(WorkerBlock, oneshot::Sender<()>)>,
        network: anemo::Network,
        metrics: Arc<WorkerMetrics>,
    ) -> JoinHandle<()> {
        spawn_logged_monitored_task!(
            async move {
                Self {
                    authority,
                    id,
                    committee,
                    worker_cache,
                    rx_shutdown,
                    rx_quorum_waiter,
                    network,
                    metrics,
                    pipeline: FuturesUnordered::new(),
                    best_effort_with_timeout: FuturesUnordered::new(),
                }
                .await;
            },
            "QuorumWaiterTask"
        )
    }

    /// Helper function. It waits for a future to complete and then delivers a value.
    async fn waiter(
        wait_for: CancelOnDropHandler<eyre::Result<anemo::Response<()>>>,
        deliver: Stake,
    ) -> Stake {
        let _ = wait_for.await;
        deliver
    }

    /// Process futures in the pipeline. They complete when we have sent to >2/3
    /// of other worker by stake, but after that we still try to send to the remaining
    /// on a best effort basis.
    fn run_pipeline(&mut self, cx: &mut Context<'_>) -> Result<(), ()> {
        while let Poll::Ready(Some((batch, opt_channel, mut remaining))) =
            self.pipeline.poll_next_unpin(cx)
        {
            // opt_channel is not consumed only when the worker is shutting down and
            // broadcast fails. TODO: switch to returning a status from pipeline.
            if opt_channel.is_some() {
                return Err(());
            }
            // Attempt to send messages to the remaining workers
            if !remaining.is_empty() {
                trace!(
                    "Best effort dissemination for batch {} for remaining {}",
                    batch.digest(),
                    remaining.len()
                );
                self.best_effort_with_timeout.push(Box::pin(async move {
                    // Bound the attempt to a few seconds to tolerate nodes that are
                    // offline and will never succeed.
                    //
                    // TODO: make the constant a config parameter.
                    timeout(Duration::from_secs(5), async move {
                        while remaining.next().await.is_some() {}
                    })
                    .await
                }));
            }
        }
        Ok(())
    }
}

impl Future for QuorumWaiter {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();

        // If we are shutting down then go ahead and end.
        if pin!(&this.rx_shutdown).poll(cx).is_ready() {
            return Poll::Ready(());
        }

        // Process the pipeline now to potentially make room.
        if this.run_pipeline(cx).is_err() {
            return Poll::Ready(());
        }

        // When a new batch is available, and the pipeline is not full, add a new
        // task to the pipeline to send this batch to workers.
        if this.pipeline.len() < MAX_PARALLEL_BLOCK {
            while let Poll::Ready(Some((batch, channel))) = this.rx_quorum_waiter.poll_recv(cx) {
                // Broadcast the batch to the other workers.
                let workers: Vec<_> = this
                    .worker_cache
                    .others_workers_by_id(this.authority.protocol_key(), &this.id)
                    .into_iter()
                    .map(|(name, info)| (name, info.name))
                    .collect();
                let (primary_names, worker_names): (Vec<_>, _) = workers.into_iter().unzip();
                let message = WorkerBlockMessage { worker_block: batch.clone() };
                let handlers = this.network.broadcast(worker_names, &message);
                let timer = this.metrics.block_broadcast_quorum_latency.start_timer();

                // Collect all the handlers to receive acknowledgements.
                let mut wait_for_quorum: FuturesUnordered<QMBoxFuture<u64>> =
                    FuturesUnordered::new();
                primary_names
                    .into_iter()
                    .zip(handlers.into_iter())
                    .map(|(name, handler)| {
                        let stake = this.committee.stake(&name);
                        Box::pin(monitored_future!(Self::waiter(handler, stake)))
                    })
                    .for_each(|f| wait_for_quorum.push(f));

                // Wait for the first 2f nodes to send back an Ack. Then we consider the batch
                // delivered and we send its digest to the primary (that will include it into
                // the dag). This should reduce the amount of syncing.
                let threshold = this.committee.quorum_threshold();
                let mut total_stake = this.authority.stake();

                this.pipeline.push(Box::pin(async move {
                    // Keep the timer until a quorum is reached.
                    let _timer = timer;
                    // A future that sends to 2/3 stake then returns. Also prints a warning
                    // if we terminate before we have managed to get to the full 2/3 stake.
                    let mut opt_channel = Some(channel);
                    loop {
                        if let Some(stake) = wait_for_quorum.next().await {
                            total_stake += stake;
                            if total_stake >= threshold {
                                // Notify anyone waiting for this.
                                let channel = opt_channel.take().unwrap();
                                if let Err(e) = channel.send(()) {
                                    warn!("Channel waiting for quorum response dropped: {:?}", e);
                                }
                                break;
                            }
                        } else {
                            // This should not happen unless shutting down, because
                            // `broadcast()` uses `send()` which keeps retrying on
                            // failed responses.
                            warn!("Batch dissemination ended without a quorum. Shutting down.");
                            break;
                        }
                    }
                    (batch, opt_channel, wait_for_quorum)
                }));
                if this.pipeline.len() >= MAX_PARALLEL_BLOCK {
                    // Breaking like this leaves us with no watch on rx_block_maker
                    // but that is good since we have to clear the block_pipeline
                    // first before we care...
                    break;
                }
            }
            // Need to do this here so we will be left with waker registered on the pipeline.
            if this.run_pipeline(cx).is_err() {
                return Poll::Ready(());
            }
        }

        // Drive the best effort send efforts which may update remaining workers
        // or timeout.
        while let Poll::Ready(Some(_)) = this.best_effort_with_timeout.poll_next_unpin(cx) {}

        Poll::Pending
    }
}
