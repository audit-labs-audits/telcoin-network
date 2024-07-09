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
mod payload_builder;
use error::EngineResult;
use futures::{channel::mpsc::UnboundedSender, stream::BoxStream, Future, StreamExt};
use futures_util::{future::BoxFuture, FutureExt};
use handle::TNEngineHandle;
pub use payload_builder::execute_consensus_output;
use reth_blockchain_tree::BlockchainTreeEngine;
use reth_chainspec::ChainSpec;
use reth_db::database::Database;
use reth_errors::RethError;
use reth_evm::ConfigureEvm;
use reth_primitives::{BlockNumHash, BlockNumber, B256};
use reth_provider::{
    BlockIdReader, BlockReader, BlockReaderIdExt, CanonChainTracker, CanonStateNotificationSender,
    Chain, ChainSpecProvider, StageCheckpointReader, StateProviderFactory,
};
use reth_stages::{Pipeline, PipelineEvent};
use reth_stages_api::StageId;
use reth_tasks::TaskSpawner;
use reth_tokio_util::EventStream;
use std::{
    collections::VecDeque,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tn_types::{BuildArguments, ConsensusOutput};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::BroadcastStream;
use tracing::{debug, error, info, warn};

/// The TN consensus engine is responsible executing state that has reached consensus.
pub struct ExecutorEngine<BT, CE> {
    /// The backlog of output from consensus that's ready to be executed.
    queued: VecDeque<ConsensusOutput>,
    /// Single active future that inserts a new block into `storage`
    // insert_task: Option<BoxFuture<'static, Option<EventStream<PipelineEvent>>>>,
    insert_task: Option<BoxFuture<'static, EngineResult<BlockNumHash>>>,
    // /// Used to notify consumers of new blocks
    // canon_state_notification: CanonStateNotificationSender,
    /// The type used to query both the database and the blockchain tree.
    blockchain: BT,
    /// EVM configuration for executing transactions and building blocks.
    evm_config: CE,
    /// Optional round of consensus to finish executing before then returning. The value is used to
    /// track the subdag index from consensus output. The index is included in executed blocks as
    /// the `nonce` value.
    ///
    /// note: this is used for debugging and testing
    max_block: Option<u64>,
    /// The pipeline events to listen on
    pipeline_events: Option<EventStream<PipelineEvent>>,
    /// Receiving end from CL's `Executor`. The `ConsensusOutput` is sent
    /// to the mining task here.
    consensus_output_stream: BroadcastStream<ConsensusOutput>,
    /// The [BlockNumHash] of the last fully-executed block.
    ///
    /// This information is reflects the current finalized block number and hash.
    parent: BlockNumHash,
}

impl<BT, CE> ExecutorEngine<BT, CE>
where
    BT: BlockchainTreeEngine
        + BlockReader
        + BlockIdReader
        + CanonChainTracker
        + StageCheckpointReader
        + ChainSpecProvider
        + 'static,
    CE: ConfigureEvm,
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
        blockchain: BT,
        evm_config: CE,
        task_spawner: Box<dyn TaskSpawner>,
        max_block: Option<BlockNumber>,
        consensus_output_stream: BroadcastStream<ConsensusOutput>,
        parent: BlockNumHash,
        // hooks: EngineHooks,
    ) -> EngineResult<Self> {
        // let event_sender = EventSender::default();
        // let handle = BeaconConsensusEngineHandle::new(to_engine, event_sender.clone());
        Ok(Self {
            queued: Default::default(),
            insert_task: None,
            blockchain,
            evm_config,
            max_block,
            pipeline_events: None,
            consensus_output_stream,
            parent,
        })
    }
}

impl<BT, CE> Future for ExecutorEngine<BT, CE>
where
    BT: BlockchainTreeEngine
        + BlockReader
        + BlockIdReader
        + CanonChainTracker
        + StageCheckpointReader
        + StateProviderFactory
        + ChainSpecProvider
        + Clone
        + Unpin
        + 'static,
    CE: ConfigureEvm,
{
    type Output = EngineResult<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // this executes output from consensus
        'main: loop {
            // check if output is available from consensus
            match this.consensus_output_stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(output))) => {
                    // queue the output for local execution
                    this.queued.push_back(output)
                }
                Poll::Ready(Some(Err(e))) => {
                    error!(target: "execution::executor", ?e, "for consensus output stream");
                }
                Poll::Ready(None) => {
                    // stream has ended
                    //
                    // TODO: should this be an error?
                    // Primary shuts down engine this way?
                    info!(target: "tn::engine", "ConsensusOutput channel closed. Shutting down...");

                    // if the engine received output before channel closed,
                    // execute final output then return
                    if let Some(mut fut) = this.insert_task.take() {
                        info!(target: "tn::engine", "attempting to execute final output...");

                        // TODO: test that this works as expected
                        //
                        // loop for executing the last output
                        'last_output: loop {
                            match fut.poll_unpin(cx) {
                                Poll::Ready(final_num_hash) => {
                                    info!(target: "tn::engine", ?final_num_hash, "engine completed execution");
                                    // this.pipeline_events = events;
                                    //
                                    // TODO: broadcast tip?
                                    //
                                    // ensure no errors then continue
                                    this.parent = final_num_hash?;

                                    // break last_output loop to return Ok()
                                    break 'last_output;
                                }
                                Poll::Pending => {
                                    this.insert_task = Some(fut);
                                    // break main loop to return Poll::Pending
                                    break 'main;
                                }
                            }
                        }
                    }

                    // TODO: try take insert_task to finish executing last output before returning
                    return Poll::Ready(Ok(()));
                }
                Poll::Pending => { /* nothing to do */ }
            }

            // only insert task if there is none
            //
            // note: it's important that the previous consensus output finishes executing before inserting
            // the next task to ensure the parent numhash is finalized
            if this.insert_task.is_none() {
                if this.queued.is_empty() {
                    // nothing to insert
                    break;
                }

                // ready to queue executing next round of consensus
                let output = this.queued.pop_front().expect("not empty");
                let provider = this.blockchain.clone();
                let evm_config = this.evm_config.clone();
                let parent = this.parent; // Copy
                let build_args = BuildArguments::new(provider, output, parent);

                // TODO: should this be on a blocking thread?
                //
                // execute the consensus output
                this.insert_task = Some(Box::pin(async move {
                    let finalized_block_num_hash = execute_consensus_output(evm_config, build_args);
                    finalized_block_num_hash
                }));
            }

            if let Some(mut fut) = this.insert_task.take() {
                match fut.poll_unpin(cx) {
                    Poll::Ready(final_num_hash) => {
                        // this.pipeline_events = events;
                        //
                        // TODO: broadcast tip?
                        //
                        // ensure no errors then continue
                        this.parent = final_num_hash?;
                        // loop again to check for next output
                        continue;
                    }
                    Poll::Pending => {
                        this.insert_task = Some(fut);
                        break;
                    }
                }
            }
        }

        // all output executed, yield back to runtime
        Poll::Pending
    }
}
