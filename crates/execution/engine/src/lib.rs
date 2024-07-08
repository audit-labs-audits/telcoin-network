//! Execute output from consensus layer to extend the canonical chain.
//!
//! The engine listens to a stream of output from consensus and constructs a new block.

#![doc(
    html_logo_url = "https://www.telco.in/logos/TEL.svg",
    html_favicon_url = "https://www.telco.in/logos/TEL.svg",
    issue_tracker_base_url = "https://github.com/telcoin-association/telcoin-network/issues/"
)]
#![warn(missing_debug_implementations, missing_docs, unreachable_pub, rustdoc::all)]
#![deny(unused_must_use, rust_2018_idioms)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

mod error;
mod handle;
use error::EngineResult;
use futures::{channel::mpsc::UnboundedSender, stream::BoxStream, StreamExt};
use futures_util::{future::BoxFuture, FutureExt};
use handle::TNEngineHandle;
use reth_blockchain_tree::BlockchainTreeEngine;
use reth_chainspec::ChainSpec;
use reth_db::database::Database;
use reth_errors::RethError;
use reth_primitives::{BlockNumber, B256};
use reth_provider::{
    BlockIdReader, BlockReader, BlockReaderIdExt, CanonChainTracker, CanonStateNotificationSender,
    Chain, ChainSpecProvider, StageCheckpointReader, StateProviderFactory,
};
use reth_stages::{Pipeline, PipelineEvent};
use reth_stages_api::StageId;
use reth_tasks::TaskSpawner;
use reth_tokio_util::EventStream;
use std::{collections::VecDeque, sync::Arc};
use tn_types::ConsensusOutput;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::BroadcastStream;
use tracing::{debug, error, warn};

/// The TN consensus engine is responsible executing state that has reached consensus.
#[derive(Debug)]
pub struct ExecutorEngine<BT> {
    /// The backlog of output from consensus that's ready to be executed.
    queued: VecDeque<ConsensusOutput>,
    // /// Used to notify consumers of new blocks
    // canon_state_notification: CanonStateNotificationSender,
    /// The type used to query both the database and the blockchain tree.
    blockchain: BT,
    /// Optional round of consensus to finish executing before then returning. The value is used to track the subdag index from consensus output. The index is included in executed blocks as the `nonce` value.
    ///
    /// note: this is used for debugging and testing
    max_block: Option<u64>,
    /// The pipeline events to listen on
    pipeline_events: Option<EventStream<PipelineEvent>>,
    /// Receiving end from CL's `Executor`. The `ConsensusOutput` is sent
    /// to the mining task here.
    consensus_output_stream: BroadcastStream<ConsensusOutput>,
}

impl<BT> ExecutorEngine<BT>
where
    BT: BlockchainTreeEngine
        + BlockReader
        + BlockIdReader
        + CanonChainTracker
        + StageCheckpointReader
        + ChainSpecProvider
        + 'static,
    // DB: Database + Unpin + 'static,
{
    /// Create a new instance of the [`ExecutorEngine`] using the given channel to configure
    /// the [`ConsensusOutput`] communication channel.
    ///
    /// By default the engine is started with idle pipeline.
    /// The pipeline can be launched immediately in one of the following ways descending in
    /// priority:
    /// - Explicit [`Option::Some`] target block hash provided via a constructor argument.
    /// - The process was previously interrupted amidst the pipeline run. This is checked by
    ///   comparing the checkpoints of the first ([`StageId::Headers`]) and last
    ///   ([`StageId::Finish`]) stages. In this case, the latest available header in the database is
    ///   used as the target.
    ///
    /// Propagates any database related error.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        // pipeline: Pipeline<DB>,
        blockchain: BT,
        task_spawner: Box<dyn TaskSpawner>,
        max_block: Option<BlockNumber>,
        target: Option<B256>,
        pipeline_run_threshold: u64,
        consensus_output_stream: BroadcastStream<ConsensusOutput>,
        // hooks: EngineHooks,
    ) -> EngineResult<Self> {
        // let event_sender = EventSender::default();
        // let handle = BeaconConsensusEngineHandle::new(to_engine, event_sender.clone());
        // let sync = EngineSyncController::new(
        //     pipeline,
        //     client,
        //     task_spawner.clone(),
        //     max_block,
        //     blockchain.chain_spec(),
        //     event_sender.clone(),
        // );

        Ok(Self {
            queued: Default::default(),
            blockchain,
            max_block,
            pipeline_events: None,
            consensus_output_stream,
        })

        // TODO: should pipeline consistency be checked here
        // or inside `start_engine` method for tn node?

        // let maybe_pipeline_target = match target {
        //     // Provided target always takes precedence.
        //     target @ Some(_) => target,
        //     None => this.check_pipeline_consistency()?,
        // };

        // if let Some(target) = maybe_pipeline_target {
        //     this.sync.set_pipeline_sync_target(target.into());
        // }

        // Ok((this, handle))
    }

    /// From: reth::consensus::beacon::engine::mod.rs
    /// Check if the pipeline is consistent (all stages have the checkpoint block numbers no less
    /// than the checkpoint of the first stage).
    ///
    /// This will return the pipeline target if:
    ///  * the pipeline was interrupted during its previous run
    ///  * a new stage was added
    ///  * stage data was dropped manually through `reth stage drop ...`
    ///
    /// # Returns
    ///
    /// A target block hash if the pipeline is inconsistent, otherwise `None`.
    ///
    ///
    ///
    ///
    /// TODO: pipeline consistency should check for completed consensus output, not single block.
    ///  - what to do if node crashes in the middle of the pipeline?
    ///  - ensure db would revert and request the consensus output from CL again using sub dag index
    ///  - reth pipeline implementation would request block from peers and start download, but this is not what TN should do
    fn check_pipeline_consistency(&self) -> EngineResult<Option<B256>> {
        // !!!!!!!
        //
        // TODO: read todo above - !!!!!!!!!!
        //
        // obnoxious comment !!!!!
        // do not merge until above is answered
        //
        // If no target was provided, check if the stages are congruent - check if the
        // checkpoint of the last stage matches the checkpoint of the first.
        let first_stage_checkpoint = self
            .blockchain
            .get_stage_checkpoint(*StageId::ALL.first().expect("first stage always set"))
            .map_err(RethError::from)?
            .unwrap_or_default()
            .block_number;

        // Skip the first stage since it's already retrieved and then compare all other checkpoints
        // against it.
        for stage_id in StageId::ALL.iter().skip(1) {
            let stage_checkpoint = self
                .blockchain
                .get_stage_checkpoint(*stage_id)
                .map_err(RethError::from)?
                .unwrap_or_default()
                .block_number;

            // If the checkpoint of any stage is less than the checkpoint of the first stage,
            // retrieve and return the block hash of the latest header and use it as the target.
            if stage_checkpoint < first_stage_checkpoint {
                debug!(
                    target: "consensus::engine",
                    first_stage_checkpoint,
                    inconsistent_stage_id = %stage_id,
                    inconsistent_stage_checkpoint = stage_checkpoint,
                    "Pipeline sync progress is inconsistent"
                );
                return Ok(self.blockchain.block_hash(first_stage_checkpoint)?);
            }
        }

        Ok(None)
    }
}

impl<BT> Future for ExecutorEngine<BT> where
    BT: BlockchainTreeEngine
        + BlockReader
        + BlockIdReader
        + CanonChainTracker
        + StageCheckpointReader
        + ChainSpecProvider
        + Unpin
        + 'static
{
}
