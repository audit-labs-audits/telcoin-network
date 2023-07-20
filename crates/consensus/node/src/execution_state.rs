// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use async_trait::async_trait;
use lattice_executor::ExecutionState;
use tn_types::consensus::{BatchAPI, CertificateAPI, ConsensusOutput, HeaderAPI};
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
    async fn handle_consensus_output(&self, consensus_output: ConsensusOutput) {
        for (_, batches) in consensus_output.batches {
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

/// Client sender for passing completed certificates to the Execution Engine.
///
/// This is passed to the Node for Primary.start()
pub struct LatticeExecutionState {
    /// Sender channel to NarwhalEngine in the EL.
    tx_to_execution_engine: Sender<Vec<u8>>,
}

impl LatticeExecutionState {
    pub fn new(tx_to_execution_engine: Sender<Vec<u8>>) -> Self {
        Self { tx_to_execution_engine }
    }
}

#[async_trait]
impl ExecutionState for LatticeExecutionState {
    async fn handle_consensus_output(&self, consensus_output: ConsensusOutput) {
        let _batches = consensus_output.batches;
        let _round = consensus_output.sub_dag.leader_round();
        let _timestamp = consensus_output.sub_dag.commit_timestamp();
        let _leader_author = consensus_output.sub_dag.leader.header().author();

        // metrics

        // TODO: what to send here?
        // parse data here or in EL?
        //
        // prolly here so we don't mix business logic
        let _res = self.tx_to_execution_engine.send(vec![]);
    }

    async fn last_executed_sub_dag_index(&self) -> u64 {
        // what is this used for?
        //
        // sui_core checks the node's epoch store
        todo!()
    }
}
