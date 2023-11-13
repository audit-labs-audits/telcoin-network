// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use async_trait::async_trait;
use narwhal_executor::ExecutionState;
use narwhal_types::{BatchAPI, ConsensusOutput};
use tokio::sync::mpsc::Sender;

/// A simple/dumb execution engine.
pub struct SimpleExecutionState {
    tx_transaction_confirmation: Sender<Vec<u8>>,
}

impl SimpleExecutionState {
    pub fn new(tx_transaction_confirmation: Sender<Vec<u8>>) -> Self {
        Self { tx_transaction_confirmation }
    }
}

#[async_trait]
impl ExecutionState for SimpleExecutionState {
    async fn handle_consensus_output(&mut self, consensus_output: ConsensusOutput) {
        for batches in consensus_output.batches {
            for batch in batches {
                for transaction in batch.transactions().iter() {
                    if let Err(err) =
                        self.tx_transaction_confirmation.send(transaction.clone()).await
                    {
                        eprintln!("Failed to send txn in SimpleExecutionState: {}", err);
                    }
                }
            }
        }
    }

    async fn last_executed_sub_dag_index(&self) -> u64 {
        0
    }
}
