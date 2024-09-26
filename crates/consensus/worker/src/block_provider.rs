// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The receiving side of the execution layer's `BlockProvider`.
//!
//! Consensus `BlockProvider` takes a block from the EL, stores it,
//! and sends it to the quorum waiter for broadcasting to peers.

use crate::metrics::WorkerMetrics;
use consensus_metrics::{
    metered_channel::{Receiver, Sender},
    monitored_scope, spawn_logged_monitored_task,
};
use futures::{stream::FuturesUnordered, StreamExt};
use narwhal_network::{client::NetworkClient, WorkerToPrimaryClient};
use narwhal_typed_store::{tables::WorkerBlocks, traits::Database};
use std::{
    future::Future,
    pin::{pin, Pin},
    sync::Arc,
    task::Poll,
};
use tn_types::{NewWorkerBlock, Noticer, WorkerId};

use narwhal_network_types::WorkerOwnBlockMessage;
use tn_types::WorkerBlock;
use tokio::{task::JoinHandle, time::Instant};
use tracing::{error, info, warn};

#[cfg(feature = "trace_transaction")]
use byteorder::{BigEndian, ReadBytesExt};

// The number of blocks to store / transmit in parallel.
pub const MAX_PARALLEL_BLOCK: usize = 100;

#[cfg(test)]
#[path = "tests/block_provider_tests.rs"]
pub mod block_provider_tests;

type BPBoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// Process blocks from EL into sealed blocks for CL.
pub struct BlockProvider<DB: Database> {
    /// Our worker's id.
    id: WorkerId,
    /// Receiver for shutdown.
    rx_shutdown: Noticer,
    /// Channel to receive transactions from the network.
    rx_block_maker: Receiver<NewWorkerBlock>,
    /// Output channel to deliver sealed blocks to the `QuorumWaiter`.
    tx_quorum_waiter: Sender<(WorkerBlock, tokio::sync::oneshot::Sender<()>)>,
    /// Metrics handler
    node_metrics: Arc<WorkerMetrics>,
    /// The timestamp of the block creation.
    /// Average resident time in the block would be ~ (block seal time - creation time) / 2
    block_start_timestamp: Instant,
    /// The network client to send our blocks to the primary.
    client: NetworkClient,
    /// The block store to store our own blocks.
    store: DB,
    /// collection of future sealed blocks
    block_pipeline: FuturesUnordered<BPBoxFuture<()>>,
}

impl<DB: Database + Clone + 'static> BlockProvider<DB> {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn spawn(
        id: WorkerId,
        rx_shutdown: Noticer,
        rx_block_maker: Receiver<NewWorkerBlock>,
        tx_quorum_waiter: Sender<(WorkerBlock, tokio::sync::oneshot::Sender<()>)>,
        node_metrics: Arc<WorkerMetrics>,
        client: NetworkClient,
        store: DB,
    ) -> JoinHandle<()> {
        let this = Self {
            id,
            rx_shutdown,
            rx_block_maker,
            tx_quorum_waiter,
            block_start_timestamp: Instant::now(),
            node_metrics,
            client,
            store,
            block_pipeline: FuturesUnordered::new(),
        };
        spawn_logged_monitored_task!(
            async move {
                info!(target: "worker::block_provider", "BlockProvider id {} has started successfully.", id);
                this.await;
                info!(target: "worker::block_provider", "BlockProvider id {} has shutdown.", id);
            },
            "BlockProviderTask"
        )
    }

    /// Seal and broadcast the current block.
    fn seal(&self, new_block: NewWorkerBlock) -> Option<BPBoxFuture<()>> {
        #[cfg(feature = "benchmark")]
        {
            let digest = new_block.block.digest();

            // Look for sample txs (they all start with 0) and gather their txs id (the next 8
            // bytes).
            let tx_ids: Vec<_> = new_block
                .block
                .transactions()
                .iter()
                .filter(|tx| tx.hash[0] == 0u8 && tx.hash.len() > 8)
                .filter_map(|tx| tx.hash[1..9].try_into().ok())
                .collect();

            let size = tx_ids.len();

            for id in tx_ids {
                // NOTE: This log entry is used to compute performance.
                tracing::info!(target: "worker::block_provider", "Block {:?} contains sample tx {}", digest, u64::from_be_bytes(id));
            }

            #[cfg(feature = "trace_transaction")]
            {
                // The first 8 bytes of each transaction message is reserved for an identifier
                // that's useful for debugging and tracking the lifetime of messages between
                // Narwhal and clients.
                let tracking_ids: Vec<_> = new_block
                    .block
                    .transactions()
                    .iter()
                    .map(|tx| {
                        let len = tx.hash.len();
                        if len >= 8 {
                            (&tx.hash[0..8]).read_u64::<BigEndian>().unwrap_or_default()
                        } else {
                            0
                        }
                    })
                    .collect();
                tracing::debug!(
                    target: "worker::block_provider",
                    "Tracking IDs of transactions in the Block {:?}: {:?}",
                    digest,
                    tracking_ids
                );
            }

            // NOTE: This log entry is used to compute performance.
            tracing::info!(target: "worker::block_provider", "Block {:?} contains {} B", digest, size);
        }

        let NewWorkerBlock { block, ack } = new_block;
        let size = block.size();

        // TODO: include timeout vs size_reached in `NewWorkerBlock`
        // let reason = if timeout { "timeout" } else { "size_reached" };
        let reason = "timeout";

        self.node_metrics.created_block_size.with_label_values(&[reason]).observe(size as f64);

        // Send the block through the deliver channel for further processing.
        let (notify_done, broadcasted_to_quorum) = tokio::sync::oneshot::channel();
        let tx_quorum_waiter = self.tx_quorum_waiter.clone();
        let block_clone = block.clone();
        tokio::spawn(async move {
            if let Err(e) = tx_quorum_waiter.send((block_clone, notify_done)).await {
                tracing::error!(target: "worker::block_provider", "Error sending block to quorum waiter: {e}");
            }
        });

        let block_creation_duration = self.block_start_timestamp.elapsed().as_secs_f64();

        tracing::debug!(target: "worker::block_provider",
            "Block {:?} took {} seconds to create due to {}",
            block.digest(),
            block_creation_duration,
            reason
        );

        // we are deliberately measuring this after the sending to the downstream
        // channel tx_quorum_waiter as the operation is blocking and affects any further
        // block creation.
        self.node_metrics
            .created_block_latency
            .with_label_values(&[reason])
            .observe(block_creation_duration);

        // Clone things to not capture self
        let client = self.client.clone();
        let store = self.store.clone();
        let worker_id = self.id;

        Some(Box::pin(async move {
            // Also wait quorum broadcast here.
            //
            // Error can only happen when the worker is shutting down.
            // All other errors, e.g. timeouts, failure responses from individual peers,
            // are retried indefinitely underneath.
            if broadcasted_to_quorum.await.is_err() {
                // Drop all response handlers to signal error.
                return;
            }

            // Now save it to disk
            let digest = block.digest();

            if let Err(e) = store.insert::<WorkerBlocks>(&digest, &block) {
                error!(target: "worker::block_provider", "Store failed with error: {:?}", e);
                return;
            }

            // Send the block to the primary.
            let message = WorkerOwnBlockMessage { digest, worker_id, worker_block: block.clone() };
            if let Err(e) = client.report_own_block(message).await {
                warn!(target: "worker::block_provider", "Failed to report our block: {}", e);
                // Drop all response handlers to signal error, since we
                // cannot ensure the primary has actually signaled the
                // block will eventually be sent.
                // The transaction submitter will see the error and retry.
                return;
            }

            // We now signal back to the execution layer's block provider
            // that the block is sealed.
            let _ = ack.send(digest);
        }))
    }
}

impl<DB: Database> Future for BlockProvider<DB> {
    // Errors are either loggable events or show stoppers so we don't return an error type.
    type Output = ();

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // If we are shutting down then go ahead and end.
        if pin!(&this.rx_shutdown).poll(cx).is_ready() {
            return Poll::Ready(());
        }

        // Clear space in the block_pipeline first to make room for new blocks.
        while let Poll::Ready(Some(_)) = this.block_pipeline.poll_next_unpin(cx) {
            this.node_metrics.parallel_worker_blocks.set(this.block_pipeline.len() as i64);
        }

        // Wait for the next block from the EL.
        //
        // Note that transactions are only consumed when the number of blocks
        // 'in-flight' are below a certain number (MAX_PARALLEL_BLOCK). This
        // condition will be met eventually if the store and network are functioning.
        if this.block_pipeline.len() < MAX_PARALLEL_BLOCK {
            while let Poll::Ready(Some(new_block)) = this.rx_block_maker.poll_recv(cx) {
                let _scope = monitored_scope("BlockProvider::recv");
                // try seal the block
                if let Some(seal) = this.seal(new_block) {
                    this.block_pipeline.push(seal);
                }

                this.node_metrics.parallel_worker_blocks.set(this.block_pipeline.len() as i64);
                this.block_start_timestamp = Instant::now();
                if this.block_pipeline.len() >= MAX_PARALLEL_BLOCK {
                    // Breaking like this leaves us with no watch on rx_block_maker
                    // but that is good since we have to clear the block_pipeline
                    // first before we care...
                    break;
                }

                // Yield once per size threshold to allow other tasks to run.
                //tokio::task::yield_now().await;
            }
            // This might look pointless but is important.  We need to do this here so that the
            // waiter will be activated when block_pipeline is ready.
            while let Poll::Ready(Some(_)) = this.block_pipeline.poll_next_unpin(cx) {
                this.node_metrics.parallel_worker_blocks.set(this.block_pipeline.len() as i64);
            }
        }

        Poll::Pending
    }
}
