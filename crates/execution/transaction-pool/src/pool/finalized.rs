use super::{size::SizeTracker, best::FinalizedTransactions};
use crate::{
    identifier::TransactionId, TransactionOrdering,
    ValidPoolTransaction, error::PoolError, PoolResult,
};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// A pool containing transactions from consensus.
///
/// All transactions in this pool are finalize. The pool sorts transactions in the order they
/// arrive from the committed subdag (consensus output). Transations are validated and ready
/// for execution on the current state.
///
/// This pool distinguishes between `independent` transactions and pending transactions. A
/// transaction is `independent`, if it is in the pending pool, and it has the current on chain
/// nonce of the sender. Meaning `independent` transactions can be executed right away, other
/// pending transactions depend on at least one `independent` transaction.
///
/// TODO: sort transactions by sender/nonce as well
#[derive(Clone)]
pub struct FinalizedPool<T: TransactionOrdering> {
    /// How to order transactions.
    ordering: T,
    /// Keeps track of transactions inserted in the pool.
    ///
    /// This way we can determine when transactions where submitted to the pool.
    submission_id: u64,
    /// _All_ Transactions that are currently inside the pool grouped by their identifier.
    by_id: BTreeMap<TransactionId, Arc<FinalizedTransaction<T>>>,
    /// _All_ transactions sorted by priority
    all: BTreeSet<FinalizedTransactionRef<T>>,
    /// Independent transactions that can be included directly and don't require other
    /// transactions.
    ///
    /// Sorted by their scoring value.
    independent_transactions: BTreeSet<FinalizedTransactionRef<T>>,
    /// Keeps track of the size of this pool.
    ///
    /// See also [`PoolTransaction::size`](crate::traits::PoolTransaction::size).
    size_of: SizeTracker,
}

impl<T: TransactionOrdering> std::fmt::Debug for FinalizedPool<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TODO: may want a better debug impl
        f.debug_struct("FinalizedPool")
            .field("submission_id", &self.submission_id)
            .field("size_of", &self.size())
            .finish()
    }
}

// === impl FinalizedPool ===

impl<T: TransactionOrdering> FinalizedPool<T> {
    /// Create a new instance of Self
    pub(crate) fn new(ordering: T) -> Self {
        Self {
            ordering,
            submission_id: 0,
            by_id: Default::default(),
            all: Default::default(),
            independent_transactions: Default::default(),
            size_of: Default::default(),
        }
    }

    /// Add transactions to the pool.
    ///
    /// The primary purpose of this fn is to ensure workers did not include duplicate
    /// transactions.
    ///
    /// TODO: how can we check for duplicates prior to consensus?
    pub(super) fn add_transaction(
        &mut self,
        tx: ValidPoolTransaction<T::Transaction>
    ) -> PoolResult<()> {
            // ensure a transaction is only added once
            // instead of panicking, just ignore
            // assert!(
            //     !self.by_id.contains_key(tx.id()),
            //     "transaction already included {:?}",
            //     self.by_id.contains_key(tx.id())
            // );
            let hash = tx.hash();
            
            // TODO: there should be a penalty associated with this
            //
            // or, check during batch voting
            if self.by_id.contains_key(tx.id()) {
                // TODO: add metric here
                // self.metrics.inc_duplicate_transaction();
                // continue
                return Err(PoolError::AlreadyImported(*hash))
            }

            let tx_id = *tx.id();
            let submission_id = self.next_id();

            let priority = self.ordering.priority(&tx.transaction);

            // TODO: is this the only relevant "size"?
            self.size_of += tx.size();

            let transaction = FinalizedTransactionRef { submission_id, transaction: Arc::new(tx), priority };

            // // TODO: is this redundant?
            // // - batches already "execute" a block to verify txs
            // // - can the subdag produce a tx order that has nonce issues?
            // // - for now, leave it as is
            // //
            // // If there's __no__ ancestor in the pool, then this transaction is independent, this is
            // // guaranteed because this pool is gapless.
            // if self.ancestor(&tx_id).is_none() {
            //     self.independent_transactions.insert(transaction.clone());
            // }

            self.all.insert(transaction.clone());

            let transaction = Arc::new(FinalizedTransaction { transaction });

            self.by_id.insert(tx_id, transaction);

            Ok(())
        // }
    }

    // /// Returns the ancestor the given transaction, the transaction with `nonce - 1`.
    // ///
    // /// Note: for a transaction with nonce higher than the current on chain nonce this will always
    /// return an ancestor since all transaction in this pool are gapless.
    fn ancestor(&self, id: &TransactionId) -> Option<&Arc<FinalizedTransaction<T>>> {
        self.by_id.get(&id.unchecked_ancestor()?)
    }

    /// Counter for submission ids.
    fn next_id(&mut self) -> u64 {
        let id = self.submission_id;
        self.submission_id = self.submission_id.wrapping_add(1);
        id
    }

    /// The size of all transactions in the pool.
    fn size(&self) -> usize {
        // TODO: do we want to keep track of bytes as well?
        self.size_of.into()
    }

    /// Number of transactions in the pool.
    fn len(&self) -> usize {
        self.by_id.len()
    }

    /// Returns all transactions for building the next canonical block.
    /// 
    /// Akin to pending_pool.best()
    pub(super) fn all(&self) -> FinalizedTransactions<T> {
        FinalizedTransactions {
            all: self.by_id.clone(),
            independent: self.independent_transactions.clone(),
            invalid: Default::default(),
        }
    }
}


// Use Pool validate_all()
// impl TryFrom<Vec<Bytes>> for FinalizedPool<GasCostOrdering<PooledTransaction>> {
//     // TODO: should this be a different error?
//     type Error = PoolError;
//
//     fn try_from(value: Vec<Bytes>) -> Result<Self, Self::Error> {
//         // TODO: consider implement custom ordering.
//         // for now, just prioritize based on gas
//         let mut empty_pool = Self::new(GasCostOrdering::default());
//         let transactions = value
//             .iter()
//             .map(|tx| TransactionSigned::decode(&mut tx.as_ref()))
//             .collect::<Result<Vec<_>, _>>()?;
//         let transactions = transactions
//             .iter()
//             .map(|tx| tx.into_ecrecovered())
//             .collect::<Vec<_>>();
//         let pool = empty_pool.add_transactions(transactions);
//
//         // todo!()
//         Ok(empty_pool)
//     }
// }

/// A transaction that has reached consensus.
pub(crate) struct FinalizedTransaction<T: TransactionOrdering> {
    /// Reference to the actual transaction.
    pub(crate) transaction: FinalizedTransactionRef<T>,
}

impl<T: TransactionOrdering> Clone for FinalizedTransaction<T> {
    fn clone(&self) -> Self {
        Self { transaction: self.transaction.clone() }
    }
}

/// A reference to a transaction that has reached consensus.
pub(crate) struct FinalizedTransactionRef<T: TransactionOrdering> {
    /// Identifier that tags when transaction was submitted in the pool.
    pub(crate) submission_id: u64,
    /// Actual transaction.
    pub(crate) transaction: Arc<ValidPoolTransaction<T::Transaction>>,
    /// The priority value assigned by the used `Ordering` function.
    pub(crate) priority: T::Priority,
}

impl<T: TransactionOrdering> FinalizedTransactionRef<T> {
    /// The next transaction of the sender: `nonce + 1`
    pub(crate) fn unlocks(&self) -> TransactionId {
        self.transaction.transaction_id.descendant()
    }
}

impl<T: TransactionOrdering> Clone for FinalizedTransactionRef<T> {
    fn clone(&self) -> Self {
        Self {
            submission_id: self.submission_id,
            transaction: Arc::clone(&self.transaction),
            priority: self.priority.clone(),
        }
    }
}

impl<T: TransactionOrdering> Eq for FinalizedTransactionRef<T> {}

impl<T: TransactionOrdering> PartialEq<Self> for FinalizedTransactionRef<T> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<T: TransactionOrdering> PartialOrd<Self> for FinalizedTransactionRef<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: TransactionOrdering> Ord for FinalizedTransactionRef<T> {
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
    use super::*;
    use crate::test_utils::{MockOrdering, MockTransaction, MockTransactionFactory};

    #[test]
    fn test_() {
        // let mut f = MockTransactionFactory::default();
        // let mut pool = FinalizedPool::new(MockOrdering::default());
        // let tx = f.validated_arc(MockTransaction::eip1559().inc_price());
        // pool.add_transactions(vec![tx.clone()]);

        // assert!(pool.by_id.contains_key(tx.id()));
        // assert_eq!(pool.len(), 1);

        todo!()
    }

    #[test]
    fn test_enforce_basefee_descendant() {
        // let mut f = MockTransactionFactory::default();
        // let mut pool = PendingPool::new(MockOrdering::default());
        // let t = MockTransaction::eip1559().inc_price_by(10);
        // let root_tx = f.validated_arc(t.clone());
        // pool.add_transaction(root_tx.clone());

        // let descendant_tx = f.validated_arc(t.inc_nonce().decr_price());
        // pool.add_transaction(descendant_tx.clone());

        // assert!(pool.by_id.contains_key(root_tx.id()));
        // assert!(pool.by_id.contains_key(descendant_tx.id()));
        // assert_eq!(pool.len(), 2);

        // assert_eq!(pool.independent_transactions.len(), 1);

        // let removed = pool.enforce_basefee(0);
        // assert!(removed.is_empty());

        // // two dependent tx in the pool with decreasing fee

        {
            // let mut pool2 = pool.clone();
            // let removed = pool2.enforce_basefee(descendant_tx.max_fee_per_gas() + 1);
            // assert_eq!(removed.len(), 1);
            // assert_eq!(pool2.len(), 1);
            // // descendant got popped
            // assert!(pool2.by_id.contains_key(root_tx.id()));
            // assert!(!pool2.by_id.contains_key(descendant_tx.id()));
        }

        // // remove root transaction via fee
        // let removed = pool.enforce_basefee(root_tx.max_fee_per_gas() + 1);
        // assert_eq!(removed.len(), 2);
        // assert!(pool.is_empty());

        todo!()
    }
}
