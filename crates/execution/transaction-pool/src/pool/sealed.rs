use tn_types::consensus::BatchDigest;

use crate::{
    identifier::TransactionId,
    pool:: size::SizeTracker,
    TransactionOrdering, ValidPoolTransaction,
};

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

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
pub(crate) struct SealedPool<T: TransactionOrdering> {
    /// How to order transactions.
    ordering: T,
    /// Keeps track of transactions inserted in the pool.
    ///
    /// This way we can determine when transactions where submitted to the pool.
    submission_id: u64,
    /// _All_ Transactions that are currently inside the pool grouped by their identifier.
    by_id: BTreeMap<TransactionId, Arc<SealedTransaction<T>>>,
    /// _All_ Transactions that are currently inside the pool grouped by the batch digest.
    by_batch_digest: BTreeMap<Arc<BatchDigest>, Vec<TransactionId>>,
    /// _All_ transactions sorted by priority
    all: BTreeSet<SealedTransactionRef<T>>,
    /// Keeps track of the size of this pool.
    ///
    /// See also [`PoolTransaction::size`](crate::traits::PoolTransaction::size).
    size_of: SizeTracker,
}

// === impl SealedPool ===

impl<T: TransactionOrdering> SealedPool<T> {
    /// Create a new pool instance.
    pub(crate) fn new(ordering: T) -> Self {
        Self {
            ordering,
            submission_id: 0,
            by_id: Default::default(),
            by_batch_digest: Default::default(),
            all: Default::default(),
            size_of: Default::default(),
        }
    }

    /// Returns an iterator over all transactions in the pool
    pub(crate) fn all(
        &self,
    ) -> impl Iterator<Item = Arc<ValidPoolTransaction<T::Transaction>>> + '_ {
        self.by_id.values().map(|tx| tx.transaction.transaction.clone())
    }

    /// Returns the ancestor the given transaction, the transaction with `nonce - 1`.
    ///
    /// Note: for a transaction with nonce higher than the current on chain nonce this will always
    /// return an ancestor since all transaction in this pool are gapless.
    fn ancestor(&self, id: &TransactionId) -> Option<&Arc<SealedTransaction<T>>> {
        self.by_id.get(&id.unchecked_ancestor()?)
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
        assert!(
            !self.by_id.contains_key(tx.id()),
            "transaction already included {:?}",
            self.by_id.contains_key(tx.id())
        );

        let tx_id = *tx.id();
        let submission_id = self.next_id();

        let priority = self.ordering.priority(&tx.transaction);

        // keep track of size
        self.size_of += tx.size();

        let transaction = SealedTransactionRef { submission_id, transaction: tx, priority, batch_digest: batch_digest.clone() };

        self.all.insert(transaction.clone());

        let transaction = Arc::new(SealedTransaction { transaction });

        self.by_id.insert(tx_id.clone(), transaction);

        self.by_batch_digest
            .entry(batch_digest)
            .and_modify(|vec| vec.push(tx_id))
            .or_insert(vec![tx_id]);
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
        let res = txs.iter().map(|id| self.remove_transaction(id)).collect();
        res 
    }

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

    /// Removes the transaction from the pool.
    ///
    /// Note: this only removes the given transaction.
    pub(crate) fn remove_transaction(
        &mut self,
        id: &TransactionId,
    ) -> Option<Arc<ValidPoolTransaction<T::Transaction>>> {
        let tx = self.by_id.remove(id)?;
        self.all.remove(&tx.transaction);
        self.size_of -= tx.transaction.transaction.size();
        // self.independent_transactions.remove(&tx.transaction);
        Some(tx.transaction.transaction.clone())
    }

    fn next_id(&mut self) -> u64 {
        let id = self.submission_id;
        self.submission_id = self.submission_id.wrapping_add(1);
        id
    }

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
        self.by_id.len()
    }

    /// Whether the pool is empty
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }
}

/// A transaction that is included in a sealed batch.
pub(crate) struct SealedTransaction<T: TransactionOrdering> {
    /// Reference to the actual transaction.
    pub(crate) transaction: SealedTransactionRef<T>,
}

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
    use tn_types::consensus::Batch;
    use fastcrypto::hash::Hash;
    use super::*;
    use crate::test_utils::{MockOrdering, MockTransaction, MockTransactionFactory};

    #[test]
    fn test_add_transactions() {
        let mut f = MockTransactionFactory::default();
        let mut pool = SealedPool::new(MockOrdering::default());
        let tx1 = f.validated_arc(MockTransaction::eip1559().inc_price());

        let batch = Batch::new(vec![vec![1]]);
        let batch_digest = Arc::new(batch.digest());
        pool.add_transaction(batch_digest.clone(), tx1.clone());

        assert!(pool.by_id.contains_key(tx1.id()));
        assert_eq!(pool.len(), 1);

        let txs_in_batch_digest = pool.by_batch_digest.get(&batch_digest).unwrap();
        assert_eq!(txs_in_batch_digest.len(), 1);
        assert_eq!(txs_in_batch_digest.first().unwrap(), tx1.id());

        // add a second tx
        let tx2 = f.validated_arc(MockTransaction::eip1559().inc_price());
        pool.add_transaction(batch_digest.clone(), tx2.clone());

        assert!(pool.by_id.contains_key(tx1.id()));
        assert_eq!(pool.len(), 2);

        let txs_in_batch_digest = pool.by_batch_digest.get(&batch_digest).unwrap();
        assert_eq!(txs_in_batch_digest.len(), 2);
        assert_eq!(txs_in_batch_digest.first().unwrap(), tx1.id());
        assert_eq!(txs_in_batch_digest.last().unwrap(), tx2.id());
    }

    #[test]
    fn test_prune_batch() {
        let mut f = MockTransactionFactory::default();
        let mut pool = SealedPool::new(MockOrdering::default());
        let tx1 = f.validated_arc(MockTransaction::eip1559().inc_price());

        let batch = Batch::new(vec![vec![1]]);
        let batch_digest = Arc::new(batch.digest());
        pool.add_transaction(batch_digest.clone(), tx1.clone());

        assert!(pool.by_id.contains_key(tx1.id()));
        assert_eq!(pool.len(), 1);
        assert_eq!(pool.by_batch_digest.get(&batch_digest).unwrap().len(), 1);

        pool.prune_batch(&batch_digest);

        assert!(!pool.by_id.contains_key(tx1.id()));
        assert_eq!(pool.len(), 0);
        assert!(pool.by_batch_digest.get(&batch_digest).is_none());
    }

}
