use execution_rlp::Decodable;
use futures_util::future::Pending;
use tn_types::{consensus::{BatchDigest, Batch, BatchAPI}, execution::TransactionSigned};

use crate::{
    identifier::TransactionId,
    pool:: size::SizeTracker,
    TransactionOrdering, ValidPoolTransaction, PoolResult, error::PoolError, PoolTransaction, PooledTransaction,
};

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use super::pending::{PendingTransactionRef, PendingPool};

/// A pool of transactions that were sealed in a worker's batch.
/// 
/// The transactions in this pool were promoted from `pending` pool after
/// being sealed, and stored by the CL.
/// 
/// By the time transactions are moved to this pool, they are stored and guaranteed
/// to be delivered by a worker's quorum waiter. However, the batch may fail to reach
/// quorum. Or it may reach quorum, but the primary's proposed block fails.
///
/// Transactions in this pool are not finalized, but have been successfully sent to
/// the consensus layer for broadcasting.
#[derive(Clone)]
pub(crate) struct SealedPool<T: TransactionOrdering + Clone> {
    /// How to order transactions.
    ordering: T,
    // /// Keeps track of transactions inserted in the pool.
    // ///
    // /// This way we can determine when transactions where submitted to the pool.
    // submission_id: u64,
    // /// _All_ Transactions that are currently inside the pool grouped by their identifier.
    // by_id: BTreeMap<TransactionId, Arc<SealedTransaction<T>>>,
    // /// _All_ Transactions that are currently inside the pool grouped by the batch digest.
    // /// 
    // /// Stored as Arc<ValidPoolTransaction<T::Transaction>>> because that is how they need to
    // /// be retrieved.
    // by_batch_digest: BTreeMap<Arc<BatchDigest>, Vec<Arc<ValidPoolTransaction<T::Transaction>>>>,
    // /// _All_ transactions sorted by priority
    // all: BTreeSet<SealedTransactionRef<T>>,
    /// Keeps track of the size of this pool.
    ///
    /// See also [`PoolTransaction::size`](crate::traits::PoolTransaction::size).
    size_of: SizeTracker,

    /// Sub-pool by batch for block proposal.
    // batches: BTreeMap<Arc<BatchDigest>, BatchPool<T>>,
    by_batch_digest: BTreeMap<Arc<BatchDigest>, PendingPool<T>>,
}

// === impl SealedPool ===

impl<T: TransactionOrdering + Clone> SealedPool<T> {
    /// Create a new pool instance.
    pub(crate) fn new(ordering: T) -> Self {
        Self {
            ordering,
            // submission_id: 0,
            // by_id: Default::default(),
            // all: Default::default(),
            size_of: Default::default(),
            by_batch_digest: Default::default(),
        }
    }

    /// Returns an iterator over all transactions in the pool
    pub(crate) fn all(
        &self,
    ) -> impl Iterator<Item = Arc<ValidPoolTransaction<T::Transaction>>> + '_ {
        self.by_batch_digest.values().flat_map(|pool| pool.all())
    }

    /// Adds a new transactions to the pending queue.
    ///
    /// # Panics
    ///
    /// if the transaction is already included
    pub(crate) fn add_transaction(
        &mut self,
        batch_digest: Arc<BatchDigest>,
        tx: Arc<ValidPoolTransaction<T::Transaction>>,
    ) {
        let tx_size = tx.size();
        // add by batch
        self.by_batch_digest
            .entry(batch_digest.clone())
            .and_modify(|pool| pool.add_transaction(tx.clone()))
            .or_insert_with(|| {
                let mut pool = PendingPool::new(self.ordering.clone());
                pool.add_transaction(tx);
                pool
            });

        // add to pool size
        self.size_of += tx_size;
    }

    /// Remove a _mined_ batch from the pool
    fn prune_batch(
        &mut self,
        batch_digest: &Arc<BatchDigest>,
    ) -> Option<Vec<Arc<ValidPoolTransaction<T::Transaction>>>> {
        // similar to prune transaction, but at the batch level

        // this method should remove all transactions associated with a batch digest
        // let transactions_to_prune = self.by_batch_digest.remove(&batch_digest);
        // if let Some(txs) = self.by_batch_digest.remove(&batch_digest) {
        //     let tx = txs.iter().map(|id| self.remove_transaction(id)).collect();
        // }
        let txs = self.by_batch_digest.remove(batch_digest)?;
        // let res = txs.iter().map(|tx| self.remove_transaction(tx.id())).collect();
        // res 
        todo!()
    }

    /// Get the pending pool for each batch
    pub(crate) fn get_batch_pool(&self, batch_digest: &BatchDigest) -> PoolResult<&PendingPool<T>> {
        self.by_batch_digest
            .get(batch_digest)
            .ok_or(PoolError::BatchMissing(batch_digest.to_owned()))
    }

    /// Try to add missing batches to the sealed pool.
    /// 
    /// TODO: figure something else out
    pub(crate) fn add_missing_batches(
        &self,
        mut missing_batches: HashMap<BatchDigest, Batch>,
    ) -> Option<HashMap<BatchDigest, Batch>> {
        let mut remaining_batches = missing_batches.clone();
        'batches: for (digest, batch) in missing_batches.iter_mut() {
            // Arc will change
            let batch_digest = Arc::new(digest);
            '_txs: for tx in batch.transactions_mut().iter_mut() {
                if let Ok(transaction) = TransactionSigned::decode(&mut tx.as_ref()) {
                    // convert to valid pooltransaction

                    // let tx = ValidPoolTransaction {
                    //     transaction,
                    //     transaction_id: TransactionId::new(),
                    //     propagate: todo!(),
                    //     timestamp: todo!(),
                    //     origin: todo!(),
                    //     encoded_length: todo!(),
                    // }

                    // add transaction to this pool
                    // self.add_transaction(batch_digest, tx);
                }
                // if error, continue loop for batches
                // leaving the batch with broken tx in
                // `remaining_batches` hashmap
                continue 'batches
                // return Some(missing_batches)
            }
            // remove the digest from the map after all txs processed
            remaining_batches.remove(digest);
        }
        None
    }


    // /// Get a collection of transactions by batch digest.
    // pub(crate) fn get_batches(
    //     &self,
    //     digests: Vec<BatchDigest>,
    // ) -> BTreeMap<BatchDigest, Vec<Arc<ValidPoolTransaction<T::Transaction>>>> {
    //     // TODO: optimize this?
    //     let mut map = BTreeMap::new();
    //     for digest in digests {
    //         let txs = self.by_batch_digest
    //             .get(&digest)
    //             .cloned()
    //             .unwrap_or(vec![]);
    //             // .iter()
    //             // .map(|arc| arc.transaction())
    //             // .collect();
    //         map.insert(digest, txs);
    //     }
    //     map
    // }

    // /// Removes a _mined_ transaction from the pool.
    // ///
    // /// If the transaction has a descendant transaction it will advance it to the best queue.
    // pub(crate) fn prune_transaction(
    //     &mut self,
    //     id: &TransactionId,
    // ) -> Option<Arc<ValidPoolTransaction<T::Transaction>>> {
    //     // mark the next as independent if it exists
    //     if let Some(unlocked) = self.by_id.get(&id.descendant()) {
    //         self.independent_transactions.insert(unlocked.transaction.clone());
    //     };
    //     self.remove_transaction(id)
    // }

    // /// Removes the transaction from the pool.
    // ///
    // /// Note: this only removes the given transaction.
    // pub(crate) fn remove_transaction(
    //     &mut self,
    //     id: &TransactionId,
    // ) -> Option<Arc<ValidPoolTransaction<T::Transaction>>> {
    //     let tx = self.by_id.remove(id)?;
    //     // self.all.remove(&tx.transaction);
    //     self.size_of -= tx.transaction.transaction.size();
    //     // self.independent_transactions.remove(&tx.transaction);
    //     Some(tx.transaction.transaction.clone())
    // }

    // fn next_id(&mut self) -> u64 {
    //     let id = self.submission_id;
    //     self.submission_id = self.submission_id.wrapping_add(1);
    //     id
    // }

    // /// Removes the worst transaction from this pool.
    // pub(crate) fn pop_worst(&mut self) -> Option<Arc<ValidPoolTransaction<T::Transaction>>> {
    //     let worst = self.all.iter().next_back().map(|tx| *tx.transaction.id())?;
    //     self.remove_transaction(&worst)
    // }

    /// The reported size of all transactions in this pool.
    pub(crate) fn size(&self) -> usize {
        self.size_of.into()
    }

    /// Number of transactions in the entire pool
    pub(crate) fn len(&self) -> usize {
        // get the size of each batch's pool
        let mut len = 0;
        for (_, pool) in self.by_batch_digest.iter() {
            len += pool.len()
        }
        len
    }

    // /// Whether the pool is empty
    // #[cfg(test)]
    // pub(crate) fn is_empty(&self) -> bool {
    //     self.by_id.is_empty()
    // }
}

/// A transaction that is included in a sealed batch.
pub(crate) struct SealedTransaction<T: TransactionOrdering> {
    /// Reference to the actual transaction.
    pub(crate) transaction: SealedTransactionRef<T>,
}

// impl<T: TransactionOrdering> SealedTransaction<T> {
//     fn id(&self) -> &TransactionId {
//         self.transaction.transaction.id()
//     }

//     fn transaction(&self) -> Arc<ValidPoolTransaction<T::Transaction>> {
//         self.transaction.transaction()
//     }
// }

impl<T: TransactionOrdering> Clone for SealedTransaction<T> {
    fn clone(&self) -> Self {
        Self { transaction: self.transaction.clone() }
    }
}

/// A transaction that is included in a sealed batch.
pub(crate) struct SealedTransactionRef<T: TransactionOrdering> {
    /// Identifier that tags when transaction was submitted in the pool.
    pub(crate) submission_id: u64,
    /// Actual transaction.
    pub(crate) transaction: Arc<ValidPoolTransaction<T::Transaction>>,
    /// The priority value assigned by the used `Ordering` function.
    pub(crate) priority: T::Priority,
    /// The batch 
    pub(crate) batch_digest: Arc<BatchDigest>,
}

impl<T: TransactionOrdering> SealedTransactionRef<T> {
    /// The next transaction of the sender: `nonce + 1`
    pub(crate) fn unlocks(&self) -> TransactionId {
        self.transaction.transaction_id.descendant()
    }

    // /// The reference to the ValidPoolTransaction
    // fn transaction(&self) -> Arc<ValidPoolTransaction<T::Transaction>> {
    //     self.transaction.clone()
    // }
}

impl<T: TransactionOrdering> Clone for SealedTransactionRef<T> {
    fn clone(&self) -> Self {
        Self {
            submission_id: self.submission_id,
            transaction: Arc::clone(&self.transaction),
            priority: self.priority.clone(),
            batch_digest: Arc::clone(&self.batch_digest),
        }
    }
}

impl<T: TransactionOrdering> Eq for SealedTransactionRef<T> {}

impl<T: TransactionOrdering> PartialEq<Self> for SealedTransactionRef<T> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<T: TransactionOrdering> PartialOrd<Self> for SealedTransactionRef<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: TransactionOrdering> Ord for SealedTransactionRef<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // This compares by `priority` and only if two tx have the exact same priority this compares
        // the unique `submission_id`. This ensures that transactions with same priority are not
        // equal, so they're not replaced in the set
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.submission_id.cmp(&self.submission_id))
    }
}

#[cfg(test)]
mod tests {
    use tn_types::{consensus::Batch, execution::IntoRecoveredTransaction};
    use fastcrypto::hash::Hash;
    use super::*;
    use crate::test_utils::{MockOrdering, MockTransaction, MockTransactionFactory};

    #[test]
    fn test_add_transactions() {
        let mut f = MockTransactionFactory::default();
        let mut pool = SealedPool::new(MockOrdering::default());
        let tx1 = f.validated_arc(MockTransaction::eip1559().inc_price());
        let tx1_bytes = tx1.to_recovered_transaction().into_signed().envelope_encoded().into();
        let tx2 = f.validated_arc(MockTransaction::eip1559().inc_price());
        let tx2_bytes = tx2.to_recovered_transaction().into_signed().envelope_encoded().into();

        // create a batch and digest
        let batch = Batch::new(vec![tx1_bytes, tx2_bytes]);
        let batch_digest = Arc::new(batch.digest());
        pool.add_transaction(batch_digest.clone(), tx1.clone());

        let batch_pool = pool.get_batch_pool(&batch_digest).unwrap();
        assert_eq!(batch_pool.all().next().unwrap().id(), tx1.id());
        assert_eq!(pool.len(), 1);

        // add a second tx
        pool.add_transaction(batch_digest.clone(), tx2.clone());

        let batch_pool = pool.get_batch_pool(&batch_digest).unwrap();
        assert_eq!(pool.len(), 2);
        assert_eq!(batch_pool.all().last().unwrap().id(), tx2.id());
    }

    #[test]
    fn test_missing_batch_err() {
        let mut f = MockTransactionFactory::default();
        let mut pool = SealedPool::new(MockOrdering::default());
        let tx1 = f.validated_arc(MockTransaction::eip1559().inc_price());

        // create a dummy batch digest
        let batch_digest = Arc::new(BatchDigest::new([1_u8; 32]));
        pool.add_transaction(batch_digest.clone(), tx1.clone());

        let batch_pool = pool.get_batch_pool(&batch_digest).unwrap();
        assert_eq!(batch_pool.all().next().unwrap().id(), tx1.id());
        assert_eq!(pool.len(), 1);

        let missing_batch_digest = BatchDigest::new([3_u8; 32]);
        assert!(pool.get_batch_pool(&missing_batch_digest).is_err())
    }

    #[test]
    fn test_prune_batch() {
        // let mut f = MockTransactionFactory::default();
        // let mut pool = SealedPool::new(MockOrdering::default());
        // let tx1 = f.validated_arc(MockTransaction::eip1559().inc_price());

        // let batch = Batch::new(vec![vec![1]]);
        // let batch_digest = Arc::new(batch.digest());
        // pool.add_transaction(batch_digest.clone(), tx1.clone());

        // assert!(pool.by_id.contains_key(tx1.id()));
        // assert_eq!(pool.len(), 1);
        // assert_eq!(pool.by_batch_digest.get(&batch_digest).unwrap().len(), 1);

        // pool.prune_batch(&batch_digest);

        // assert!(!pool.by_id.contains_key(tx1.id()));
        // assert_eq!(pool.len(), 0);
        // assert!(pool.by_batch_digest.get(&batch_digest).is_none());
        todo!()
    }

}
