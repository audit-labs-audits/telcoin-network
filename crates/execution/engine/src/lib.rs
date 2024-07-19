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
use futures::{Future, StreamExt};
use futures_util::{future::BoxFuture, FutureExt};
pub use payload_builder::execute_consensus_output;
use reth_blockchain_tree::BlockchainTreeEngine;
use reth_evm::ConfigureEvm;
use reth_primitives::{BlockNumber, SealedHeader};
use reth_provider::{
    BlockIdReader, BlockReader, CanonChainTracker, ChainSpecProvider, StageCheckpointReader,
    StateProviderFactory,
};
use reth_stages::PipelineEvent;
use reth_tasks::TaskSpawner;
use reth_tokio_util::EventStream;
use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
};
use tn_types::{BuildArguments, ConsensusOutput};
use tokio::sync::oneshot;
use tokio_stream::wrappers::BroadcastStream;
use tracing::{error, info, trace, warn};

/// Type alias for the blocking task that executes consensus output and returns the finalized `SealedHeader`.
type PendingExecutionTask = oneshot::Receiver<EngineResult<SealedHeader>>;

/// The TN consensus engine is responsible executing state that has reached consensus.
pub struct ExecutorEngine<BT, CE, Tasks> {
    /// The backlog of output from consensus that's ready to be executed.
    queued: VecDeque<ConsensusOutput>,
    /// Single active future that inserts a new block into `storage`
    // insert_task: Option<BoxFuture<'static, Option<EventStream<PipelineEvent>>>>,
    // insert_task: Option<BoxFuture<'static, EngineResult<SealedHeader>>>,
    insert_task: Option<PendingExecutionTask>,
    // /// Used to notify consumers of new blocks
    // canon_state_notification: CanonStateNotificationSender,
    /// The type used to query both the database and the blockchain tree.
    blockchain: BT,
    /// EVM configuration for executing transactions and building blocks.
    evm_config: CE,
    /// The task executor to spawn new builds.
    executor: Tasks,
    /// Optional round of consensus to finish executing before then returning. The value is used to
    /// track the subdag index from consensus output. The index is included in executed blocks as
    /// the `nonce` value.
    ///
    /// note: this is used for debugging and testing
    max_round: Option<u64>,
    /// The pipeline events to listen on
    pipeline_events: Option<EventStream<PipelineEvent>>,
    /// Receiving end from CL's `Executor`. The `ConsensusOutput` is sent
    /// to the mining task here.
    consensus_output_stream: BroadcastStream<ConsensusOutput>,
    /// The [SealedHeader] of the last fully-executed block.
    ///
    /// This information is reflects the current finalized block number and hash.
    parent_header: SealedHeader,
}

impl<BT, CE, Tasks> ExecutorEngine<BT, CE, Tasks>
where
    BT: BlockchainTreeEngine
        + BlockReader
        + BlockIdReader
        + CanonChainTracker
        + StageCheckpointReader
        + ChainSpecProvider
        + 'static,
    CE: ConfigureEvm,
    Tasks: TaskSpawner + Clone + Unpin + 'static,
{
    /// Create a new instance of the [`ExecutorEngine`] using the given channel to configure
    /// the [`ConsensusOutput`] communication channel.
    ///
    /// The engine waits for CL to broadcast output then tries to execute.
    ///
    /// Propagates any database related error.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        blockchain: BT,
        evm_config: CE,
        executor: Tasks,
        max_round: Option<u64>,
        consensus_output_stream: BroadcastStream<ConsensusOutput>,
        parent_header: SealedHeader,
        // hooks: EngineHooks,
    ) -> Self {
        // let event_sender = EventSender::default();
        // let handle = BeaconConsensusEngineHandle::new(to_engine, event_sender.clone());
        Self {
            queued: Default::default(),
            insert_task: None,
            blockchain,
            evm_config,
            executor,
            max_round,
            pipeline_events: None,
            consensus_output_stream,
            parent_header,
        }
    }

    /// Spawns a blocking task to execute consensus output.
    fn spawn_execution_task(&mut self) -> PendingExecutionTask
    where
        BT: StateProviderFactory
            + ChainSpecProvider
            + BlockchainTreeEngine
            + CanonChainTracker
            + Clone,
    {
        let output = self.queued.pop_front().expect("not empty");
        let provider = self.blockchain.clone();
        let evm_config = self.evm_config.clone();
        let parent = self.parent_header.clone();
        let build_args = BuildArguments::new(provider, output, parent);
        let (tx, rx) = oneshot::channel();

        // spawn blocking task and return oneshot receiver
        self.executor.spawn_blocking(Box::pin(async move {
            let result = execute_consensus_output(evm_config, build_args);
            match tx.send(result) {
                Ok(()) => (),
                Err(e) => error!(target: "engine", ?e, "error sending result from execute_consensus_output"),
            }
        }));

        // oneshot receiver for execution result
        rx
    }

    /// Check if the engine has reached max round of consensus as specified by `max_round` parameter.
    ///
    /// Note: this is mainly for debugging purposes.
    fn has_reached_max_round(&self, progress: u64) -> bool {
        let has_reached_max_round =
            self.max_round.map(|target| progress >= target).unwrap_or_default();
        if has_reached_max_round {
            trace!(
                target: "engine",
                ?progress,
                max_round = ?self.max_round,
                "Consensus engine reached max round for consensus"
            );
        }
        has_reached_max_round
    }
}

/// The [ExecutorEngine] is a future that loops through the following:
/// - receive messages from consensus
/// - add these messages to a queue
/// - pull from queue to start next execution task if idle
/// - poll any pending tasks that are currently being executed
///
/// If a task completes, the loop continues to poll for any new output from consensus then begins executing the next task.
///
/// If the broadcast stream is closed, the engine will attempt to execute all remaining tasks and output that is queued.
impl<BT, CE, Tasks> Future for ExecutorEngine<BT, CE, Tasks>
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
    Tasks: TaskSpawner + Clone + Unpin + 'static,
{
    type Output = EngineResult<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            // check if output is available from consensus to keep broadcast stream from "lagging"
            match this.consensus_output_stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(output))) => {
                    // queue the output for local execution
                    this.queued.push_back(output)
                }
                Poll::Ready(Some(Err(e))) => {
                    error!(target: "engine", ?e, "for consensus output stream");
                }
                Poll::Ready(None) => {
                    // the stream has ended
                    //
                    // this could indicate an error but it's also how the Primary signals engine to shutdown
                    info!(target: "engine", "ConsensusOutput channel closed. Shutting down...");

                    // only return if there are no current tasks and the queue is empty
                    // otherwise, let the loop continue so any remaining tasks and queued output is executed
                    if this.insert_task.is_none() && this.queued.is_empty() {
                        return Poll::Ready(Ok(()));
                    }
                }

                Poll::Pending => { /* nothing to do */ }
            }

            // only insert task if there is none
            //
            // note: it's important that the previous consensus output finishes executing before
            // inserting the next task to ensure the parent sealed header is finalized
            if this.insert_task.is_none() {
                if this.queued.is_empty() {
                    // nothing to insert
                    break;
                }

                // ready to begin executing next round of consensus
                this.insert_task = Some(this.spawn_execution_task());
            }

            // poll receiver that returns output execution result
            if let Some(mut receiver) = this.insert_task.take() {
                match receiver.poll_unpin(cx) {
                    Poll::Ready(res) => {
                        let finalized_header = res.map_err(Into::into).and_then(|res| res);
                        // this.pipeline_events = events;
                        //
                        // TODO: broadcast tip?
                        //
                        // ensure no errors and store last executed header
                        this.parent_header = finalized_header?;

                        // check max_round
                        if this.has_reached_max_round(this.parent_header.nonce) {
                            // terminate early if the specified max consensus round is reached
                            return Poll::Ready(Ok(()));
                        }

                        // continue loop to poll broadcast stream for next output
                    }
                    Poll::Pending => {
                        this.insert_task = Some(receiver);
                        // break loop and return Poll::Pending
                        break;
                    }
                }
            }
        }

        // all output executed, yield back to runtime
        Poll::Pending
    }
}

impl<BT, CE, Tasks> std::fmt::Debug for ExecutorEngine<BT, CE, Tasks> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutorEngine")
            .field("queued", &self.queued.len())
            .field("insert_task", &self.insert_task.is_some())
            .field("max_round", &self.max_round)
            .field("parent_header", &self.parent_header)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use crate::ExecutorEngine;
    use fastcrypto::hash::Hash as _;
    use narwhal_test_utils::default_test_execution_node;
    use reth_blockchain_tree::BlockchainTreeViewer;
    use reth_chainspec::ChainSpec;
    use reth_node_ethereum::{EthEvmConfig, EthExecutorProvider};
    use reth_primitives::{
        constants::MIN_PROTOCOL_BASE_FEE, Address, GenesisAccount, TransactionSigned, U256,
    };
    use reth_provider::{BlockIdReader, BlockNumReader, BlockReader};
    use reth_tasks::TaskManager;
    use reth_tracing::init_test_tracing;
    use std::{borrow::BorrowMut, str::FromStr as _, sync::Arc, time::Duration};
    use tn_types::{
        adiri_chain_spec_arc, adiri_genesis, now,
        test_utils::{
            execute_test_batch, seeded_genesis_from_random_batches, OptionalTestBatchParams,
        },
        BatchAPI as _, Certificate, CertificateAPI, CommittedSubDag, ConsensusOutput,
        ReputationScores,
    };
    use tokio::{sync::oneshot, time::timeout};
    use tokio_stream::wrappers::BroadcastStream;
    use tracing::debug;

    #[tokio::test]
    async fn test_empty_output_executes() -> eyre::Result<()> {
        init_test_tracing();
        //=== Consensus
        //
        // create consensus output bc transactions in batches
        // are randomly generated
        //
        // for each tx, seed address with funds in genesis
        //
        // TODO: this does not use a "real" `ConsensusOutput`
        //
        // refactor with valid data once test util helpers are in place
        let leader = Certificate::default();
        let sub_dag_index = 1;
        let reputation_scores = ReputationScores::default();
        let previous_sub_dag = None;
        let beneficiary = Address::from_str("0x0000002cbd23b783741e8d7fcf51e459b497e4a6")
            .expect("beneficiary address from str");
        let consensus_output = ConsensusOutput {
            sub_dag: CommittedSubDag::new(
                vec![Certificate::default()],
                leader,
                sub_dag_index,
                reputation_scores,
                previous_sub_dag,
            )
            .into(),
            batches: Default::default(),
            beneficiary,
            batch_digests: Default::default(),
        };

        let chain = adiri_chain_spec_arc();

        // execution node components
        let manager = TaskManager::current();
        let executor = manager.executor();
        let execution_node =
            default_test_execution_node(Some(chain.clone()), None, executor.clone())?;

        let (to_engine, from_consensus) = tokio::sync::broadcast::channel(1);
        let consensus_output_stream = BroadcastStream::from(from_consensus);
        let blockchain = execution_node.get_provider().await;
        let evm_config = EthEvmConfig::default();
        let max_round = None;
        let parent = chain.sealed_genesis_header();

        let engine = ExecutorEngine::new(
            blockchain.clone(),
            evm_config,
            executor.clone(),
            max_round,
            consensus_output_stream,
            parent,
        );

        // send output
        let broadcast_result = to_engine.send(consensus_output);
        assert!(broadcast_result.is_ok());

        // drop sending channel
        drop(to_engine);

        let (tx, rx) = oneshot::channel();

        // spawn engine task
        executor.spawn_blocking(async move {
            let res = engine.await;
            let _ = tx.send(res);
        });

        let engine_task = timeout(Duration::from_secs(10), rx).await?;
        assert!(engine_task.is_ok());

        let last_block_num = blockchain.last_block_number()?;
        let canonical_tip = blockchain.canonical_tip();
        let final_block = blockchain.finalized_block_num_hash()?.expect("finalized block");

        debug!("last block num {last_block_num:?}");
        debug!("canonical tip: {canonical_tip:?}");
        debug!("final block num {final_block:?}");

        let chain_info = blockchain.chain_info()?;
        debug!("chain info:\n{chain_info:?}");

        assert_eq!(canonical_tip, final_block);

        Ok(())
    }

    /// Test the engine shuts down after the sending half of the broadcast channel is closed.
    ///
    /// One output is queued (simulating already received) in the engine and another is sent on the channel.
    /// Then, the sender is dropped and the engine task is started.
    ///
    /// Expected result:
    /// - engine receives last broadcast
    /// - engine processes queued output first
    /// - engine processes last broadcast second
    /// - engine has no more output in queue and gracefully shuts down
    #[tokio::test]
    async fn test_queued_output_executes_after_sending_channel_closed() -> eyre::Result<()> {
        init_test_tracing();
        // create batches for consensus output
        let mut batches_1 = tn_types::test_utils::batches(4); // create 4 batches
        let mut batches_2 = tn_types::test_utils::batches(4); // create 4 batches

        // use default genesis and seed accounts to execute batches
        let genesis = adiri_genesis();
        // seed genesis for batches and track txs/signers for each group
        // let (genesis, txs_1, signers_1) =
        //     seeded_genesis_from_random_batches(genesis, batches_1.iter());
        // let (genesis, txs_2, signers_2) =
        //     seeded_genesis_from_random_batches(genesis, batches_2.iter());
        let all_batches = [batches_1.clone(), batches_2.clone()].concat();
        let (genesis, txs_by_block, signers_by_block) =
            seeded_genesis_from_random_batches(genesis, all_batches.iter());
        let chain: Arc<ChainSpec> = Arc::new(genesis.into());

        // create execution node components
        let manager = TaskManager::current();
        let executor = manager.executor();
        let execution_node =
            default_test_execution_node(Some(chain.clone()), None, executor.clone())?;
        let provider = execution_node.get_provider().await;
        let block_executor = EthExecutorProvider::new(Arc::clone(&chain), EthEvmConfig::default());
        let parent = chain.sealed_genesis_header();

        // node authorities
        let beneficiary_1 = Address::from_str("0x1111111111111111111111111111111111111111")
            .expect("beneficiary address from str");
        let beneficiary_2 = Address::from_str("0x2222222222222222222222222222222222222222")
            .expect("beneficiary address from str");

        // execute batches to update headers with valid data
        let initial_base_fee = MIN_PROTOCOL_BASE_FEE;

        // update first round
        for (idx, batch) in batches_1.iter_mut().enumerate() {
            let optional_params = OptionalTestBatchParams {
                beneficiary_opt: Some(beneficiary_1.clone()),
                withdrawals_opt: None,
                timestamp_opt: None,
                mix_hash_opt: None,
                base_fee_per_gas_opt: Some(initial_base_fee + idx as u64),
            };
            execute_test_batch(batch, &parent, optional_params, &provider, &block_executor);
            debug!("{idx}\n{:?}\n", batch);
        }

        // update second round
        for (idx, batch) in batches_2.iter_mut().enumerate() {
            let optional_params = OptionalTestBatchParams {
                beneficiary_opt: Some(beneficiary_2.clone()),
                withdrawals_opt: None,
                timestamp_opt: None,
                mix_hash_opt: None,
                base_fee_per_gas_opt: Some(initial_base_fee + idx as u64),
            };
            execute_test_batch(batch, &parent, optional_params, &provider, &block_executor);
            debug!("{idx}\n{:?}\n", batch);
        }

        //=== Consensus
        //
        // create consensus output bc transactions in batches
        // are randomly generated
        //
        // for each tx, seed address with funds in genesis
        //
        // TODO: this does not use a "real" `ConsensusOutput`
        let timestamp = now();
        let mut leader_1 = Certificate::default();
        // update timestamp
        leader_1.update_created_at(timestamp);
        let sub_dag_index = 1;
        let reputation_scores = ReputationScores::default();
        let previous_sub_dag = None;
        let batch_digests = batches_1.iter().map(|b| b.digest()).collect();
        let subdag_1 = Arc::new(CommittedSubDag::new(
            vec![Certificate::default()],
            leader_1,
            sub_dag_index,
            reputation_scores,
            previous_sub_dag,
        ));
        let consensus_output_1 = ConsensusOutput {
            sub_dag: subdag_1.clone(),
            batches: vec![batches_1],
            beneficiary: beneficiary_1,
            batch_digests,
        };

        // create second output
        let mut leader_2 = Certificate::default();
        // update timestamp
        leader_2.update_created_at(timestamp + 2);
        let sub_dag_index = 2;
        let reputation_scores = ReputationScores::default();
        let previous_sub_dag = Some(subdag_1.as_ref());
        let batch_digests = batches_2.iter().map(|b| b.digest()).collect();
        let subdag_2 = CommittedSubDag::new(
            vec![Certificate::default()],
            leader_2,
            sub_dag_index,
            reputation_scores,
            previous_sub_dag,
        )
        .into();
        let consensus_output_2 = ConsensusOutput {
            sub_dag: subdag_2,
            batches: vec![batches_2],
            beneficiary: beneficiary_2,
            batch_digests,
        };

        //=== Execution

        let (to_engine, from_consensus) = tokio::sync::broadcast::channel(1);
        let consensus_output_stream = BroadcastStream::from(from_consensus);
        let blockchain = execution_node.get_provider().await;
        let evm_config = EthEvmConfig::default();
        let max_round = None;
        let parent = chain.sealed_genesis_header();

        let mut engine = ExecutorEngine::new(
            blockchain.clone(),
            evm_config,
            executor.clone(),
            max_round,
            consensus_output_stream,
            parent,
        );

        // queue the first output - simulate already received from channel
        engine.queued.push_back(consensus_output_1);

        // send second output
        let broadcast_result = to_engine.send(consensus_output_2);
        assert!(broadcast_result.is_ok());

        // drop sending channel before received
        drop(to_engine);

        // channels for engine shutting down
        let (tx, rx) = oneshot::channel();

        // spawn engine task
        //
        // one output already queued up, one output waiting in broadcast stream
        executor.spawn_blocking(async move {
            let res = engine.await;
            let _ = tx.send(res);
        });

        let engine_task = timeout(Duration::from_secs(10), rx).await?;
        assert!(engine_task.is_ok());

        let last_block_num = blockchain.last_block_number()?;
        let canonical_tip = blockchain.canonical_tip();
        let final_block = blockchain.finalized_block_num_hash()?.expect("finalized block");

        debug!("last block num {last_block_num:?}");
        debug!("canonical tip: {canonical_tip:?}");
        debug!("final block num {final_block:?}");

        let chain_info = blockchain.chain_info()?;
        debug!("chain info:\n{chain_info:?}");

        // assert all 4 batches were executed
        assert_eq!(last_block_num, 8);
        // assert canonical tip and finalized block are equal
        assert_eq!(canonical_tip, final_block);

        // assert blocks are executed as expected
        for (idx, txs) in txs_by_block.iter().enumerate() {
            let signers = &signers_by_block[idx];
            let corresponding_canonical_block = provider.block_by_number(1 + idx as u64);
        }

        Ok(())
    }
}
