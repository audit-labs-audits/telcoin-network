// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::metrics::WorkerMetrics;
use consensus_metrics::{
    metered_channel::{Receiver, Sender},
    monitored_scope, spawn_logged_monitored_task,
};
use fastcrypto::hash::Hash;
use futures::{future::BoxFuture, stream::FuturesUnordered, StreamExt};
use lattice_network::{client::NetworkClient, WorkerToPrimaryClient, WorkerToEngineClient};
use lattice_typed_store::{rocks::DBMap, Map};
use std::sync::Arc;
use tn_types::consensus::{
    WorkerId, error::DagError, now, Batch, BatchAPI, BatchDigest,
    ConditionalBroadcastReceiver, MetadataAPI, Transaction, TxResponse,
};
use tn_network_types::{WorkerOwnBatchMessage, SealedBatchResponse};
use tokio::{
    task::JoinHandle,
    time::{sleep, Duration, Instant},
};
use tracing::{error, warn, debug};

#[cfg(feature = "trace_transaction")]
use byteorder::{BigEndian, ReadBytesExt};
#[cfg(feature = "benchmark")]
use std::convert::TryInto;

// The number of batches to store / transmit in parallel.
pub const MAX_PARALLEL_BATCH: usize = 100;

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    // Our worker's id.
    id: WorkerId,
    /// The preferred batch size (in bytes).
    /// TODO: this is only used by the EL for building the batch.
    batch_size_limit: usize,
    /// The maximum delay after which to seal the batch.
    max_batch_delay: Duration,
    /// Receiver for shutdown.
    rx_shutdown: ConditionalBroadcastReceiver,
    /// Channel to receive transactions from the network.
    rx_batch_maker: Receiver<(Transaction, TxResponse)>,
    /// Metrics handler
    node_metrics: Arc<WorkerMetrics>,
    /// The timestamp of the batch creation.
    /// Average resident time in the batch would be ~ (batch seal time - creation time) / 2
    batch_start_timestamp: Instant,
    /// The network client to send our batches to the primary,
    /// and request the next batch from the EL.
    network_client: NetworkClient,
    /// The batch store to store our own batches.
    store: DBMap<BatchDigest, Batch>,
}

impl BatchMaker {
    #[must_use]
    pub fn spawn(
        id: WorkerId,
        batch_size_limit: usize,
        max_batch_delay: Duration,
        rx_shutdown: ConditionalBroadcastReceiver,
        rx_batch_maker: Receiver<(Transaction, TxResponse)>,
        node_metrics: Arc<WorkerMetrics>,
        network_client: NetworkClient,
        store: DBMap<BatchDigest, Batch>,
    ) -> JoinHandle<()> {
        spawn_logged_monitored_task!(
            async move {
                Self {
                    id,
                    batch_size_limit,
                    max_batch_delay,
                    rx_shutdown,
                    rx_batch_maker,
                    batch_start_timestamp: Instant::now(),
                    node_metrics,
                    network_client,
                    store,
                }
                .run()
                .await;
            },
            "BatchMakerTask"
        )
    }

    /// Main loop to trigger the EL to pull transactions from the pending tx pool,
    /// build a batch, and send it to the quorum waiter.
    /// 
    /// TODO: currently only produces batches based on timer
    ///  - this can be removed entirely
    ///  - logic should be on EL to monitor pending pool?
    ///  - should reimplement a pipeline or leave the pressure gauge inside quorum waiter pipeline?
    async fn run(&mut self) {
        let timer = sleep(self.max_batch_delay);
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // If the timer triggers, tell EL to build the next batch
                () = &mut timer => {
                    let _scope = monitored_scope("BatchMaker::timer");

                    // this returns an err if the batch is empty
                    if let Ok(()) = self.network_client.build_batch(self.id.clone()).await {
                        // if let Some(seal) = self.notify_primary(batch).await {
                        //     batch_pipeline.push(seal);
                        // }
                        self.node_metrics.parallel_worker_batches.set(1 as i64);
                        // current_responses = Vec::new();
                    }

                    timer.as_mut().reset(Instant::now() + self.max_batch_delay);
                    self.batch_start_timestamp = Instant::now();
                }

                _ = self.rx_shutdown.receiver.recv() => {
                    return
                }

                // // Process the pipeline of batches, this consumes items in the `batch_pipeline`
                // // list, and ensures the main loop in run will always be able to make progress
                // // by lowering it until condition batch_pipeline.len() < MAX_PARALLEL_BATCH is met.
                // _ = batch_pipeline.next(), if !batch_pipeline.is_empty() => {
                //     self.node_metrics.parallel_worker_batches.set(batch_pipeline.len() as i64);
                // }

            }
        }
    }
}
