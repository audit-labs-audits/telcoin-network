use crate::Storage;
use consensus_metrics::metered_channel::Receiver;
use futures_util::{future::BoxFuture, FutureExt};

use narwhal_types::ConsensusOutput;
use reth_beacon_consensus::{BeaconEngineMessage, ForkchoiceStatus};
use reth_interfaces::consensus::ForkchoiceState;
use reth_primitives::ChainSpec;
use reth_provider::{
    BlockReaderIdExt, CanonChainTracker, CanonStateNotificationSender, Chain, StateProviderFactory,
};
use reth_stages::PipelineEvent;

use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::{mpsc::UnboundedSender, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug, error, warn};

/// A Future that listens for new ready transactions and puts new blocks into storage
pub struct MiningTask<Client> {
    /// The configured chain spec
    chain_spec: Arc<ChainSpec>,
    /// The client used to interact with the state
    client: Client,
    /// Single active future that inserts a new block into `storage`
    insert_task: Option<BoxFuture<'static, Option<UnboundedReceiverStream<PipelineEvent>>>>,
    /// Shared storage to insert new blocks
    storage: Storage,
    /// The backlog of output from consensus that's ready to be executed.
    queued: VecDeque<ConsensusOutput>,
    /// TODO: ideally this would just be a sender of hashes
    to_engine: UnboundedSender<BeaconEngineMessage>,
    /// Used to notify consumers of new blocks
    canon_state_notification: CanonStateNotificationSender,
    /// The pipeline events to listen on
    pipe_line_events: Option<UnboundedReceiverStream<PipelineEvent>>,
    /// Receiving end from CL's `Executor`. The `ConsensusOutput` is sent
    /// to the mining task here.
    from_consensus: Receiver<ConsensusOutput>,
}

// === impl MiningTask ===

impl<Client> MiningTask<Client> {
    /// Creates a new instance of the task
    pub(crate) fn new(
        chain_spec: Arc<ChainSpec>,
        to_engine: UnboundedSender<BeaconEngineMessage>,
        canon_state_notification: CanonStateNotificationSender,
        storage: Storage,
        client: Client,
        from_consensus: Receiver<ConsensusOutput>,
    ) -> Self {
        Self {
            chain_spec,
            client,
            insert_task: None,
            storage,
            to_engine,
            canon_state_notification,
            queued: Default::default(),
            pipe_line_events: None,
            from_consensus,
        }
    }

    /// Sets the pipeline events to listen on.
    pub fn set_pipeline_events(&mut self, events: UnboundedReceiverStream<PipelineEvent>) {
        self.pipe_line_events = Some(events);
    }
}

impl<Client> Future for MiningTask<Client>
where
    Client: StateProviderFactory + CanonChainTracker + BlockReaderIdExt + Clone + Unpin + 'static,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // this executes output from consensus
        loop {
            // check if output is available from consensus
            if let Poll::Ready(Some(output)) = this.from_consensus.poll_recv(cx) {
                // consensus returned output that needs to be executed
                this.queued.push_back(output)
            }

            if this.insert_task.is_none() {
                if this.queued.is_empty() {
                    // nothing to insert
                    break
                }

                // ready to queue in new insert task
                let storage = this.storage.clone();
                let output = this.queued.pop_front().expect("not empty");

                let to_engine = this.to_engine.clone();
                let client = this.client.clone();
                let chain_spec = Arc::clone(&this.chain_spec);
                // let pool = this.pool.clone();
                let events = this.pipe_line_events.take();
                let canon_state_notification = this.canon_state_notification.clone();

                // Create the mining future that creates a block, notifies the engine that drives
                // the pipeline
                this.insert_task = Some(Box::pin(async move {
                    let mut storage = storage.write().await;

                    match storage.build_and_execute(output, &client, chain_spec) {
                        Ok((sealed_block_with_senders, bundle_state)) => {
                            // send sealed forkchoice update to engine
                            let new_header_hash = sealed_block_with_senders.hash();

                            let state = ForkchoiceState {
                                head_block_hash: new_header_hash,
                                finalized_block_hash: new_header_hash,
                                safe_block_hash: new_header_hash,
                            };

                            drop(storage);

                            // TODO: make this a future
                            // await the fcu call rx for SYNCING, then wait for a VALID response
                            loop {
                                // send the new update to the engine, this will trigger the engine
                                // to download and execute the block we just inserted
                                let (tx, rx) = oneshot::channel();
                                let _ = to_engine.send(BeaconEngineMessage::ForkchoiceUpdated {
                                    state,
                                    payload_attrs: None,
                                    tx,
                                });

                                debug!(target: "execution::executor", ?state, "Sent forkchoice update");

                                match rx.await.unwrap() {
                                    Ok(fcu_response) => {
                                        debug!(target: "execution::executor", ?fcu_response);
                                        match fcu_response.forkchoice_status() {
                                            ForkchoiceStatus::Valid => break,
                                            ForkchoiceStatus::Invalid => {
                                                error!(target: "execution::executor", ?fcu_response, "Forkchoice update returned invalid response");
                                                return None
                                            }
                                            ForkchoiceStatus::Syncing => {
                                                debug!(target: "execution::executor", ?fcu_response, "Forkchoice update returned SYNCING, waiting for VALID");
                                                // wait for the next fork choice update
                                                continue
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        error!(target: "execution::executor", ?err, "Autoseal fork choice update failed");
                                        return None
                                    }
                                }
                            }

                            // update canon chain for rpc
                            let new_header = sealed_block_with_senders.header.clone();
                            client.set_canonical_head(new_header.clone());
                            client.set_safe(new_header.clone());
                            client.set_finalized(new_header);

                            debug!(target: "execution::executor", header=?sealed_block_with_senders.hash(), "sending block notification");

                            let chain =
                                Arc::new(Chain::new(vec![sealed_block_with_senders], bundle_state));

                            // send block notification
                            let _ = canon_state_notification
                                .send(reth_provider::CanonStateNotification::Commit { new: chain });
                        }
                        Err(err) => {
                            warn!(target: "execution::executor", ?err, "failed to execute block")
                        }
                    }

                    events
                }));
            }

            if let Some(mut fut) = this.insert_task.take() {
                match fut.poll_unpin(cx) {
                    Poll::Ready(events) => {
                        this.pipe_line_events = events;
                    }
                    Poll::Pending => {
                        this.insert_task = Some(fut);
                        break
                    }
                }
            }
        }

        Poll::Pending
    }
}

impl<Client> std::fmt::Debug for MiningTask<Client> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MiningTask").finish_non_exhaustive()
    }
}
