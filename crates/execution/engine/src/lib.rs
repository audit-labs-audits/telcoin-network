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

use futures::StreamExt;
use futures_util::{future::BoxFuture, FutureExt};
use reth_blockchain_tree::BlockchainTreeEngine;
use reth_chainspec::ChainSpec;
use reth_provider::{
    BlockIdReader, BlockReader, BlockReaderIdExt, CanonChainTracker, CanonStateNotificationSender,
    Chain, ChainSpecProvider, StageCheckpointReader, StateProviderFactory,
};
use reth_stages::PipelineEvent;
use reth_tokio_util::EventStream;
use std::{collections::VecDeque, sync::Arc};
use tn_types::ConsensusOutput;
use tokio_stream::wrappers::BroadcastStream;
use tracing::{debug, error, warn};

/// The TN consensus engine is responsible executing state that has reached consensus.
#[derive(Debug)]
pub struct ExecutorEngine<BT> {
    /// The configured chain spec
    chain_spec: Arc<ChainSpec>,
    /// The backlog of output from consensus that's ready to be executed.
    queued: VecDeque<ConsensusOutput>,
    /// Used to notify consumers of new blocks
    canon_state_notification: CanonStateNotificationSender,
    /// The type used to query both the database and the blockchain tree.
    blockchain: BT,
    /// The pipeline events to listen on
    pipeline_events: Option<EventStream<PipelineEvent>>,
    /// Receiving end from CL's `Executor`. The `ConsensusOutput` is sent
    /// to the mining task here.
    consensus_output_stream: BroadcastStream<ConsensusOutput>,
}
