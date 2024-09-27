// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The receiving side of the execution layer's `BlockProvider`.
//!
//! Consensus `BlockProvider` takes a block from the EL, stores it,
//! and sends it to the quorum waiter for broadcasting to peers.

use crate::{
    metrics::WorkerMetrics,
    quorum_waiter::{QuorumWaiter, QuorumWaiterError},
};
use narwhal_network::{client::NetworkClient, WorkerToPrimaryClient};
use narwhal_typed_store::{tables::WorkerBlocks, traits::Database};
use std::{sync::Arc, time::Duration};
use tn_types::{WorkerBlock, WorkerId};

use narwhal_network_types::WorkerOwnBlockMessage;
use tokio::{task::JoinHandle, time::Instant};
use tracing::{error, warn};

#[cfg(feature = "trace_transaction")]
use byteorder::{BigEndian, ReadBytesExt};

#[cfg(test)]
#[path = "tests/block_provider_tests.rs"]
pub mod block_provider_tests;

/// Process blocks from EL into sealed blocks for CL.
#[derive(Clone)]
pub struct BlockProvider<DB: Database> {
    /// Our worker's id.
    id: WorkerId,
    /// Use `QuorumWaiter` to attest to blocks.
    quorum_waiter: QuorumWaiter,
    /// Metrics handler
    node_metrics: Arc<WorkerMetrics>,
    /// The timestamp of the block creation.
    /// Average resident time in the block would be ~ (block seal time - creation time) / 2
    block_start_timestamp: Instant,
    /// The network client to send our blocks to the primary.
    client: NetworkClient,
    /// The block store to store our own blocks.
    store: DB,
}

impl<DB: Database> std::fmt::Debug for BlockProvider<DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BlockProvider for worker {}", self.id)
    }
}

impl<DB: Database + Clone + 'static> BlockProvider<DB> {
    pub fn new(
        id: WorkerId,
        quorum_waiter: QuorumWaiter,
        node_metrics: Arc<WorkerMetrics>,
        client: NetworkClient,
        store: DB,
    ) -> Self {
        Self {
            id,
            quorum_waiter,
            block_start_timestamp: Instant::now(),
            node_metrics,
            client,
            store,
        }
    }

    /// Seal and broadcast the current block.
    pub fn seal(
        &self,
        block: WorkerBlock,
        timeout: Duration,
    ) -> JoinHandle<Result<(), QuorumWaiterError>> {
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

        let size = block.size();

        let reason = "timeout";

        self.node_metrics.created_block_size.with_label_values(&[reason]).observe(size as f64);

        let block_attest_handle = self.quorum_waiter.attest_block(block.clone(), timeout);

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

        tokio::spawn(async move {
            // Wait for our block to reach quorum or fail to do so.
            match block_attest_handle.await {
                Ok(res) => {
                    match res {
                        Ok(_) => {} // Block reach quorum!
                        Err(e) => return Err(e), /*match e {
                                         crate::quorum_waiter::QuorumWaiterError::QuorumRejected => todo!(),
                                         crate::quorum_waiter::QuorumWaiterError::AntiQuorum => todo!(),
                                         crate::quorum_waiter::QuorumWaiterError::Timeout => todo!(),
                                         crate::quorum_waiter::QuorumWaiterError::Network => todo!(),
                                         crate::quorum_waiter::QuorumWaiterError::Rpc(status_code) => todo!(),
                                     },*/
                    }
                }
                Err(e) => {
                    error!("Join error attempting block quorum! {e}");
                    // XXXX proper error.
                    return Err(QuorumWaiterError::Timeout);
                }
            }

            // Now save it to disk
            let digest = block.digest();

            if let Err(e) = store.insert::<WorkerBlocks>(&digest, &block) {
                error!(target: "worker::block_provider", "Store failed with error: {:?}", e);
                // XXXX proper error.
                return Err(QuorumWaiterError::Timeout);
            }

            // Send the block to the primary.
            let message = WorkerOwnBlockMessage { digest, worker_id, worker_block: block.clone() };
            if let Err(e) = client.report_own_block(message).await {
                warn!(target: "worker::block_provider", "Failed to report our block: {}", e);
                // Drop all response handlers to signal error, since we
                // cannot ensure the primary has actually signaled the
                // block will eventually be sent.
                // The transaction submitter will see the error and retry.
                // XXXX proper error.
                return Err(QuorumWaiterError::Timeout);
            }
            Ok(())
        })
    }
}
