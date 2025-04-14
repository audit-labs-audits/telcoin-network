use std::{sync::Arc, time::Instant};

use futures::StreamExt as _;
use reth::transaction_pool::{
    blobstore::DiskFileBlobStore, BlockInfo as RethBlockInfo, EthTransactionPool,
    TransactionValidationTaskExecutor,
};
use reth_chainspec::ChainSpec;
use reth_node_builder::{NodeConfig, RethTransactionPoolConfig};
use reth_provider::{
    providers::BlockchainProvider, CanonStateNotification, CanonStateSubscriptions as _, Chain,
    ChangedAccount, ProviderFactory,
};
use reth_transaction_pool::{
    error::InvalidPoolTransactionError, identifier::TransactionId, BestTransactions,
    CanonicalStateUpdate, EthPooledTransaction, PoolSize, PoolUpdateKind, TransactionEvents,
    TransactionOrigin, TransactionPool as _, TransactionPoolExt as _, ValidPoolTransaction,
};
use tn_types::{
    Address, EnvKzgSettings, RecoveredTx, SealedBlock, TaskManager, TxHash, MIN_PROTOCOL_BASE_FEE,
};
use tracing::{debug, info, trace};

use crate::traits::TelcoinNode;

pub type PoolTxnId = TransactionId;
pub type PoolTxn = ValidPoolTransaction<EthPooledTransaction>;
pub type RecoveredPoolTxn = RecoveredTx<EthPooledTransaction>;

pub use reth_primitives_traits::InMemorySize as TxnSize;

pub fn new_pool_txn(transaction: EthPooledTransaction, transaction_id: PoolTxnId) -> PoolTxn {
    ValidPoolTransaction {
        transaction,
        transaction_id,
        propagate: false,
        timestamp: Instant::now(),
        origin: TransactionOrigin::External,
    }
}

pub trait WorkerTxBest {
    fn best_transactions(&self) -> BestTxns;
}

#[derive(Clone, Debug)]
pub struct WorkerTxPool(EthTransactionPool<BlockchainProvider<TelcoinNode>, DiskFileBlobStore>);

impl From<WorkerTxPool> for EthTransactionPool<BlockchainProvider<TelcoinNode>, DiskFileBlobStore> {
    fn from(value: WorkerTxPool) -> Self {
        value.0
    }
}

impl WorkerTxPool {
    pub(crate) fn new(
        node_config: &NodeConfig<ChainSpec>,
        task_manager: &TaskManager,
        provider_factory: &ProviderFactory<TelcoinNode>,
        blockchain_provider: &BlockchainProvider<TelcoinNode>,
    ) -> eyre::Result<Self> {
        let head = node_config.lookup_head(provider_factory)?;
        // inspired by reth's default eth tx pool:
        // - `EthereumPoolBuilder::default()`
        // - `components_builder.build_components()`
        // - `pool_builder.build_pool(&ctx)`
        let data_dir = node_config.datadir();
        let pool_config = node_config.txpool.pool_config();
        let blob_store = DiskFileBlobStore::open(data_dir.blobstore(), Default::default())?;
        let validator = TransactionValidationTaskExecutor::eth_builder(node_config.chain.clone())
            .with_head_timestamp(head.timestamp)
            .kzg_settings(EnvKzgSettings::Default)
            .with_local_transactions_config(pool_config.local_transactions_config.clone())
            .with_additional_tasks(node_config.txpool.additional_validation_tasks)
            .build_with_tasks(
                blockchain_provider.clone(),
                task_manager.get_spawner(),
                blob_store.clone(),
            );

        let transaction_pool =
            reth_transaction_pool::Pool::eth_pool(validator, blob_store, pool_config);

        info!(target: "tn::execution", "Transaction pool initialized");

        /* TODO: replace this functionality to save and load the txn pool on start/stop
           The reth function backup_local_tranractions_task's shutdown param can not be easily created.
           The internal functions are not easy to just copy.
           Basically this interface does not work when using your own TaskManager.  Best solution may be to
           open a PR with Reth to fix this.
        let transactions_path = data_dir.txpool_transactions();
        let transactions_backup_config =
            reth_transaction_pool::maintain::LocalTransactionBackupConfig::with_local_txs_backup(transactions_path);

        // spawn task to backup local transaction pool in case of restarts
        ctx.task_executor().spawn_critical_with_graceful_shutdown_signal(
            "local transactions backup task",
            |shutdown| {
                reth_transaction_pool::maintain::backup_local_transactions_task(
                    shutdown,
                    transaction_pool.clone(),
                    transactions_backup_config,
                )
            },
        );
        */

        let mut state_stream = blockchain_provider.canonical_state_stream();
        let this = Self(transaction_pool);
        let txn_pool_clone = this.clone();
        // Update the txn pool as the canonical tip changes.
        task_manager.spawn_task("canonical txn pool", async move {
            while let Some(update) = state_stream.next().await {
                match update {
                    CanonStateNotification::Commit { new } => {
                        txn_pool_clone.process_canon_state_update(new);
                    }
                    _ => unreachable!("TN reorgs are impossible"),
                }
            }
        });
        Ok(this)
    }

    // update pool to remove mined transactions
    pub fn update_canonical_state(
        &self,
        new_tip: &SealedBlock,
        pending_block_base_fee: u64,
        pending_block_blob_fee: Option<u128>,
        mined_transactions: Vec<TxHash>,
        changed_accounts: Vec<ChangedAccount>,
    ) {
        // create canonical state update
        let update = CanonicalStateUpdate {
            new_tip,
            pending_block_base_fee,
            pending_block_blob_fee,
            changed_accounts,
            mined_transactions,
            update_kind: PoolUpdateKind::Commit,
        };

        // TODO: should this be a spawned blocking task?
        //
        // update pool to remove mined transactions
        self.0.on_canonical_state_change(update);
    }

    pub fn get_pending_base_fee(&self) -> u64 {
        // TODO: calculate the next basefee HERE for the entire round
        //
        // for now, always use lowest base fee possible
        MIN_PROTOCOL_BASE_FEE
    }

    pub fn pending_transactions(&self) -> Vec<Arc<PoolTxn>> {
        self.0.pending_transactions()
    }

    pub fn queued_transactions(&self) -> Vec<Arc<PoolTxn>> {
        self.0.queued_transactions()
    }

    /// This method is called when a canonical state update is received.
    /// This method is called when a canonical state update is received.
    /// This method is called when a canonical state update is received.
    ///
    /// Trigger the maintenance task to update pool before building the next block.
    fn process_canon_state_update(&self, update: Arc<Chain>) {
        trace!(target: "worker::block-builder", ?update, "canon state update from engine");

        // update pool based with canonical tip update
        let (blocks, state) = update.inner();
        let tip = blocks.tip();

        // collect all accounts that changed in last round of consensus
        let changed_accounts: Vec<ChangedAccount> = state
            .accounts_iter()
            .filter_map(|(addr, acc)| acc.map(|acc| (addr, acc)))
            .map(|(address, acc)| ChangedAccount {
                address,
                nonce: acc.nonce,
                balance: acc.balance,
            })
            .collect();

        debug!(target: "block-builder", ?changed_accounts);

        // collect tx hashes to remove any transactions from this pool that were mined
        let mined_transactions: Vec<TxHash> = blocks.transaction_hashes().collect();

        debug!(target: "block-builder", ?mined_transactions);

        // sync fn so self will block until all pool updates are complete
        self.update_canonical_state(
            &tip.block,
            self.get_pending_base_fee(),
            None,
            mined_transactions,
            changed_accounts,
        );
    }

    pub fn block_info(&self) -> BlockInfo {
        self.0.block_info()
    }

    pub fn set_block_info(&self, block_info: BlockInfo) {
        self.0.set_block_info(block_info);
    }

    pub fn get_transactions_by_sender(&self, address: Address) -> Vec<Arc<PoolTxn>> {
        self.0.get_transactions_by_sender(address)
    }

    pub async fn add_transaction(
        &self,
        recovered: EthPooledTransaction,
    ) -> Result<TxHash, crate::PoolError> {
        self.0.add_transaction(TransactionOrigin::Local, recovered).await
    }

    pub async fn add_transaction_and_subscribe(
        &self,
        recovered: EthPooledTransaction,
    ) -> Result<TransactionEvents, crate::EthApiError> {
        Ok(self.0.add_transaction_and_subscribe(TransactionOrigin::Local, recovered).await?)
    }

    pub fn get(&self, tx: &TxHash) -> Option<Arc<PoolTxn>> {
        self.0.get(tx)
    }

    pub fn pool_size(&self) -> PoolSize {
        self.0.pool_size()
    }
}

pub type BlockInfo = RethBlockInfo;

impl WorkerTxBest for WorkerTxPool {
    fn best_transactions(&self) -> BestTxns {
        BestTxns { inner: self.0.best_transactions() }
    }
}

pub struct BestTxns {
    inner: Box<dyn BestTransactions<Item = Arc<PoolTxn>>>,
}

impl BestTxns {
    pub fn new_for_test(inner: Box<dyn BestTransactions<Item = Arc<PoolTxn>>>) -> Self {
        Self { inner }
    }
}

impl BestTxns {
    pub fn exceeds_gas_limit(&mut self, pool_tx: &Arc<PoolTxn>, gas_limit: u64) {
        self.inner.mark_invalid(
            pool_tx,
            InvalidPoolTransactionError::ExceedsGasLimit(pool_tx.gas_limit(), gas_limit),
        );
    }

    pub fn max_batch_size(&mut self, pool_tx: &Arc<PoolTxn>, tx_size: usize, max_size: usize) {
        self.inner
            .mark_invalid(pool_tx, InvalidPoolTransactionError::OversizedData(tx_size, max_size));
    }
}

impl Iterator for BestTxns {
    type Item = Arc<PoolTxn>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
