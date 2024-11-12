// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

//! Responsible for persisting all consensus output to a persistant store for later retrieval.

use narwhal_primary::ConsensusBus;
use narwhal_typed_store::{
    tables::{ConsensusBlockNumbersByDigest, ConsensusBlocks, WorkerBlocks},
    traits::{Database, DbTxMut},
    ReDB,
};
use tn_types::{ConsensusHeader, TnReceiver, TnSender};

/// Implements writing consensus output to a peristant store.
pub struct PersistConsensus {
    db: ReDB,
}

impl PersistConsensus {
    /// Create a new PersistConsensus.  Will open a new ReDB database at path and save to that.
    pub fn new<P: AsRef<std::path::Path> + Send>(path: P) -> PersistConsensus {
        // Just pick the DB type and tables here, keep it simple.
        // Nothing else cares about this, eventually some RPC calls will need to use this module to
        // expose the DB.
        let db = ReDB::open(path).expect("Cannot open database");
        db.open_table::<WorkerBlocks>().expect("failed to open table!");
        db.open_table::<ConsensusBlocks>().expect("failed to open table!");
        db.open_table::<ConsensusBlockNumbersByDigest>().expect("failed to open table!");
        Self { db }
    }

    /// Spawns an async task that will pull from rx and save each ConsensusOutput to the DB.
    pub async fn start(&self, consensus_bus: ConsensusBus) {
        let db = self.db.clone();
        let mut last_block = if let Some((_, last_block)) = db.last_record::<ConsensusBlocks>() {
            last_block
        } else {
            ConsensusHeader::default()
        };
        // Normal thread here so we don't bog down the runtime with DB writes.
        tokio::spawn(async move {
            tracing::info!(target: "engine", "starting persistant consensus writer");
            let mut rx = consensus_bus.raw_consensus_output().subscribe();
            // When rx errors (closed sender) thread should end.
            while let Some(consensus_output) = rx.recv().await {
                match db.write_txn() {
                    Ok(mut txn) => {
                        let sub_dag = consensus_output.sub_dag.clone();
                        let number = last_block.number + 1;
                        let parent_hash = last_block.digest();
                        let header =
                            ConsensusHeader { parent_hash, sub_dag: (*sub_dag).clone(), number };
                        if let Err(e) = consensus_bus
                            .consensus_output()
                            .send((consensus_output.clone(), header.clone()))
                            .await
                        {
                            tracing::error!(target: "engine", ?e, "error sending a committed sub dag with header!")
                        }
                        for wb in consensus_output.blocks.iter().flatten() {
                            if let Err(e) = txn.insert::<WorkerBlocks>(&wb.digest(), wb) {
                                tracing::error!(target: "engine", ?e, "error saving a worker block to persistant storage!")
                            }
                        }
                        if let Err(e) = txn.insert::<ConsensusBlocks>(&number, &header) {
                            tracing::error!(target: "engine", ?e, "error saving a consensus header to persistant storage!")
                        }
                        if let Err(e) =
                            txn.insert::<ConsensusBlockNumbersByDigest>(&header.digest(), &number)
                        {
                            tracing::error!(target: "engine", ?e, "error saving a consensus header number to persistant storage!")
                        }
                        if let Err(e) = txn.commit() {
                            tracing::error!(target: "engine", ?e, "error saving committing to persistant storage!")
                        }
                        last_block = header;
                    }
                    Err(e) => {
                        tracing::error!(target: "engine", ?e, "error getting a transaction on persistant storage!")
                    }
                }
            }
            tracing::info!(target: "engine", "stopping persistant consensus writer");
        });
    }
}
