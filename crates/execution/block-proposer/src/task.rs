use crate::{mode::MiningMode, Storage};
use futures_util::{future::BoxFuture, FutureExt};
use narwhal_typed_store::traits::Database;
use narwhal_worker::{quorum_waiter::QuorumWaiterError, BlockProvider};
use reth_chainspec::ChainSpec;
use reth_evm::execute::BlockExecutorProvider;
use reth_primitives::{IntoRecoveredTransaction, Withdrawals};
use reth_provider::{BlockReaderIdExt, CanonChainTracker, StateProviderFactory};
use reth_transaction_pool::{TransactionPool, ValidPoolTransaction};
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tn_types::{PendingWorkerBlock, WorkerBlock};
use tokio::sync::watch;
use tracing::{debug, error, warn};

/// A Future that listens for new ready transactions and puts new blocks into storage
pub struct MiningTask<Client, Pool: TransactionPool, BlockExecutor, DB: Database> {
    /// The configured chain spec
    chain_spec: Arc<ChainSpec>,
    /// The client used to interact with the state
    client: Client,
    /// The active miner
    miner: MiningMode,
    /// Single active future that inserts a new block into `storage`
    insert_task: Option<BoxFuture<'static, Result<(), QuorumWaiterError>>>,
    /// Shared storage to insert new blocks
    storage: Storage,
    /// Pool where transactions are stored
    pool: Pool,
    /// backlog of sets of transactions ready to be mined
    queued: VecDeque<Vec<Arc<ValidPoolTransaction<<Pool as TransactionPool>::Transaction>>>>,
    /// The type used for block execution
    block_executor: BlockExecutor,
    /// The watch channel that shares the current pending worker block.
    watch_tx: watch::Sender<PendingWorkerBlock>,
    /// Provider for sealing blocks.
    block_provider: BlockProvider<DB>,
}

// === impl MiningTask ===

impl<Client, Pool: TransactionPool, BlockExecutor, DB: Database>
    MiningTask<Client, Pool, BlockExecutor, DB>
{
    /// Creates a new instance of the task
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        chain_spec: Arc<ChainSpec>,
        miner: MiningMode,
        storage: Storage,
        client: Client,
        pool: Pool,
        block_executor: BlockExecutor,
        watch_tx: watch::Sender<PendingWorkerBlock>,
        block_provider: BlockProvider<DB>,
    ) -> Self {
        Self {
            chain_spec,
            client,
            miner,
            insert_task: None,
            storage,
            pool,
            // canon_state_notification,
            queued: Default::default(),
            block_executor,
            watch_tx,
            block_provider,
        }
    }
}

impl<BlockExecutor, Client, Pool, DB> Future for MiningTask<Client, Pool, BlockExecutor, DB>
where
    BlockExecutor: BlockExecutorProvider,
    Client: StateProviderFactory + CanonChainTracker + BlockReaderIdExt + Clone + Unpin + 'static,
    Pool: TransactionPool + Unpin + 'static,
    DB: Database,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // loop to poll the tx miner and send the next block to Worker's `BlockProvider`
        loop {
            if let Poll::Ready(transactions) = this.miner.poll(&this.pool, cx) {
                // miner returned a set of transaction that we feed to the producer
                this.queued.push_back(transactions);
            }

            if this.insert_task.is_none() {
                if this.queued.is_empty() {
                    // nothing to insert
                    break;
                }

                // ready to queue in new insert task
                let storage = this.storage.clone();
                let transactions = this.queued.pop_front().expect("not empty");

                let block_provider = this.block_provider.clone();
                let client = this.client.clone();
                let chain_spec = Arc::clone(&this.chain_spec);
                let pool = this.pool.clone();
                let block_executor = this.block_executor.clone();
                let worker_update = this.watch_tx.clone();

                // Create the mining future that creates a block and sends it to the CL
                this.insert_task = Some(Box::pin(async move {
                    let mut storage = storage.write().await;

                    let txns: Vec<_> = transactions
                        .iter()
                        .map(|tx| tx.to_recovered_transaction().into_signed())
                        .collect();

                    // TODO: support withdrawals
                    let withdrawals = Some(Withdrawals::default());

                    match storage.build_and_execute(
                        txns.clone(),
                        withdrawals,
                        &client,
                        chain_spec,
                        &block_executor,
                    ) {
                        Ok((new_header, state)) => {
                            let block = WorkerBlock::new(
                                // TODO: make block `TransactionSigned` then convert to
                                // bytes in `.digest` impl
                                // NOTE: a `WorkerBlock` is a `SealedBlock`
                                // convert txs to bytes
                                txns, // versioned metadata for peer validation
                                new_header,
                            );
                            let digest = block.digest();
                            let seal_handle = block_provider.seal(block, Duration::from_secs(10));

                            match seal_handle.await {
                                Ok(res) => match res {
                                    Ok(()) => {
                                        debug!(target: "execution::block_provider", ?digest, "Block sealed:");
                                    }
                                    Err(e) => return Err(e),
                                },
                                Err(err) => {
                                    error!(target: "execution::block_provider", ?err, "Execution's BlockProvider Ack Failed:");
                                    // XXXX Proper error
                                    return Err(QuorumWaiterError::Timeout);
                                }
                            }

                            // TODO: leaving this here in case `WorkerBlock` -> `SealedBlock`

                            // // seal the block
                            // let block = Block {
                            //     header: new_header.clone().unseal(),
                            //     body: transactions,
                            //     ommers: vec![],
                            //     withdrawals: None,
                            // };
                            // let sealed_block = block.seal_slow();

                            // let sealed_block_with_senders =
                            //     SealedBlockWithSenders::new(sealed_block, senders)
                            //         .expect("senders are valid");

                            // debug!(target: "execution::block_provider",
                            // header=?sealed_block_with_senders.hash(), "sending block
                            // notification");

                            // let chain =
                            //     Arc::new(Chain::new(vec![sealed_block_with_senders],
                            // bundle_state));

                            // // send block notification
                            // let _ = canon_state_notification
                            //     .send(reth_provider::CanonStateNotification::Commit { new: chain
                            // });

                            // update execution state on watch channel
                            let _ = worker_update.send(PendingWorkerBlock::new(Some(state)));

                            // TODO: is this the best place to remove transactions?
                            // should the miner poll this like payload builder?

                            // TODO: this comment says dependent txs are also removed?
                            // might need to extend the trait onto another pool impl
                            //
                            // clear all transactions from pool once block is sealed
                            pool.remove_transactions(
                                transactions.iter().map(|tx| *(tx.hash())).collect(),
                            );

                            drop(storage);
                        }
                        Err(err) => {
                            warn!(target: "execution::block_provider", ?err, "failed to execute block");
                            // XXXX proper error
                            return Err(QuorumWaiterError::Timeout);
                        }
                    }

                    Ok(())
                }));
            }

            if let Some(mut fut) = this.insert_task.take() {
                match fut.poll_unpin(cx) {
                    Poll::Ready(res) => match res {
                        Ok(()) => {} // Block accepted!
                        Err(e) => match e {
                            // XXXX Use an error type at this level that has more meaning.
                            QuorumWaiterError::QuorumRejected => {} // Block has been rejected by peers don't try it again...
                            QuorumWaiterError::AntiQuorum => {} // Rejected but may work later (?)
                            QuorumWaiterError::Timeout => {} // Timeout, maybe not enough peers up?
                            QuorumWaiterError::Network => {} // Net failure
                            QuorumWaiterError::Rpc(_status_code) => {} // RPC error talking to a peer, should not come back
                        },
                    },
                    Poll::Pending => {
                        this.insert_task = Some(fut);
                        break;
                    }
                }
            }
        }

        Poll::Pending
    }
}

impl<EvmConfig, Client, Pool: TransactionPool, DB: Database> std::fmt::Debug
    for MiningTask<Client, Pool, EvmConfig, DB>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MiningTask").finish_non_exhaustive()
    }
}
