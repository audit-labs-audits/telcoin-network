// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

use crate::{primary::PrimaryNode, worker::WorkerNode};
use engine::{ExecutionNode, TnBuilder};
use futures::{future::try_join_all, stream::FuturesUnordered, StreamExt};
pub use narwhal_storage::NodeStorage;
use narwhal_typed_store::open_db;
use persist_consensus::PersistConsensus;
use reth_db::{
    database::Database,
    database_metrics::{DatabaseMetadata, DatabaseMetrics},
};
use reth_evm::{execute::BlockExecutorProvider, ConfigureEvm};
use reth_provider::CanonStateSubscriptions;
use tn_config::{ConsensusConfig, KeyConfig};
use tn_types::TelcoinDirs;
use tracing::{info, instrument};

pub mod dirs;
pub mod engine;
mod error;
pub mod metrics;
mod persist_consensus;
pub mod primary;
pub mod worker;

/// Launch all components for the node.
///
/// Worker, Primary, and Execution.
#[instrument(level = "info", skip_all)]
pub async fn launch_node<DB, Evm, CE, P>(
    mut builder: TnBuilder<DB>,
    executor: Evm,
    evm_config: CE,
    tn_datadir: P,
) -> eyre::Result<()>
where
    DB: Database + DatabaseMetadata + DatabaseMetrics + Clone + Unpin + 'static,
    Evm: BlockExecutorProvider + Clone + 'static,
    CE: ConfigureEvm,
    P: TelcoinDirs + 'static,
{
    // config for validator keys
    let config = builder.tn_config.clone();
    // adjust rpc instance ports
    builder.node_config.adjust_instance_ports();
    let engine = ExecutionNode::new(builder, executor, evm_config)?;

    info!(target: "telcoin::node", "execution engine created");

    let narwhal_db_path = tn_datadir.narwhal_db_path();
    let persist_db_path = narwhal_db_path.join("persist_consensus");
    let _ = std::fs::create_dir_all(&persist_db_path);
    let persist_consensus = PersistConsensus::new(persist_db_path);

    tracing::info!(target: "telcoin::cli", "opening node storage at {:?}", narwhal_db_path);

    // open storage for consensus
    // In case the DB dir does not yet exist.
    let _ = std::fs::create_dir_all(&narwhal_db_path);
    let db = open_db(&narwhal_db_path);
    let node_storage = NodeStorage::reopen(db);
    tracing::info!(target: "telcoin::cli", "node storage open");
    let key_config = KeyConfig::new(&tn_datadir)?;
    let consensus_config = ConsensusConfig::new(config, tn_datadir, node_storage, key_config)?;

    let (worker_id, _worker_info) = consensus_config.config().workers().first_worker()?;
    let worker = WorkerNode::new(*worker_id, consensus_config.clone());
    let primary = PrimaryNode::new(consensus_config.clone());
    // Start persist consensus output, do this before primary starts to be 100% sure of getting all
    // messages.
    persist_consensus.start(primary.consensus_bus().await).await;

    // XXXX- stream latest blocks
    let mut engine_state = engine.get_provider().await.canonical_state_stream();
    let eng_bus = primary.consensus_bus().await;
    // Spawn a task to update the consensus bus with new execution blocks as they are produced.
    tokio::spawn(async move {
        while let Some(latest) = engine_state.next().await {
            let latest_num_hash = latest.tip().block.num_hash();
            eng_bus.recent_blocks().send_modify(|blocks| blocks.push_latest(latest_num_hash));
        }
    });
    // XXXX Put the height and hash into a bus watch?

    // start the primary
    primary.start(&engine).await?;

    // start the worker
    worker.start(&engine).await?;

    // TODO: use value from CLI
    let terminate_early = false;

    if terminate_early {
        Ok(())
    } else {
        // The pipeline has finished downloading blocks up to `--debug.tip` or
        // `--debug.max-block`. Keep other node components alive for further usage.
        futures::future::pending().await
    }
}
