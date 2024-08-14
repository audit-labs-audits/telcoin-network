// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use serde::{de::DeserializeOwned, Serialize};
use std::{borrow::Borrow, fmt::Debug};

pub trait KeyT: Serialize + DeserializeOwned + Send + Sync + Ord + Clone + Debug + 'static {}
pub trait ValueT: Serialize + DeserializeOwned + Send + Sync + Clone + Debug + 'static {}

impl<K: Serialize + DeserializeOwned + Send + Sync + Ord + Clone + Debug + 'static> KeyT for K {}
impl<V: Serialize + DeserializeOwned + Send + Sync + Clone + Debug + 'static> ValueT for V {}

pub trait Table: Send + Sync + Debug + 'static {
    type Key: KeyT;
    type Value: ValueT;

    const NAME: &'static str;
}

/// Interface to a DB read transaction.
pub trait DbTx {
    /// Returns true if the map contains a value for the specified key.
    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool>;

    /// Returns the value for the given key from the map, if it exists.
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>>;

    /// Is the this table empty?
    fn is_empty<T: crate::traits::Table>(&self) -> eyre::Result<bool>;
}

/// Interface to a DB write transaction.
pub trait DbTxMut {
    /// Returns the value for the given key from the map, if it exists.
    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) -> eyre::Result<()>;

    /// Removes the entry for the given key from the map.
    fn remove<T: Table>(&mut self, key: &T::Key) -> eyre::Result<()>;

    /// Removes every key-value pair from the table.
    fn clear_table<T: Table>(&mut self) -> eyre::Result<()>;

    /// Commit data to durable storage.
    fn commit(self) -> eyre::Result<()>;
}

pub trait Database: Send + Sync {
    type TX: DbTx + Send + Sync + Debug + 'static;
    type TXMut: DbTxMut + Send + Sync + Debug + 'static;

    /// Return a read txn object.
    fn read_txn(&self) -> eyre::Result<Self::TX>;

    /// Return a write txn object.
    fn write_txn(&self) -> eyre::Result<Self::TXMut>;

    /// Returns true if the map contains a value for the specified key.
    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool>;

    /// Returns the value for the given key from the map, if it exists.
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>>;

    /// Inserts the given key-value pair into the map.
    fn insert<T: Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()>;

    /// Removes the entry for the given key from the map.
    fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()>;

    /// Removes every key-value pair from the map.
    fn clear_table<T: Table>(&self) -> eyre::Result<()>;

    /// Returns true if the map is empty, otherwise false.
    fn is_empty<T: Table>(&self) -> bool;

    /// Returns an unbounded iterator visiting each key-value pair in the map.
    /// If this is backed by storage an underlying error will most likely end the iterator early.
    fn iter<T: Table>(&self) -> Box<dyn Iterator<Item = (T::Key, T::Value)> + '_>;

    /// Skips all the elements that are smaller than the given key,
    /// and either lands on the key or the first one greater than
    /// the key.
    fn skip_to<T: Table>(
        &self,
        key: &T::Key,
    ) -> eyre::Result<Box<dyn Iterator<Item = (T::Key, T::Value)> + '_>>;

    /// Iterates over all the keys in reverse.
    fn reverse_iter<T: Table>(&self) -> Box<dyn Iterator<Item = (T::Key, T::Value)> + '_>;

    /// Returns the record prior to key if it exists or the first record that is sorted before if it
    /// does not exist.
    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)>;

    /// Returns the last (key, value) in the database.
    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)>;

    /// Returns a vector of values corresponding to the keys provided, non-atomically.
    fn multi_get<'a, T: Table>(
        &'a self,
        keys: impl IntoIterator<Item = &'a T::Key>,
    ) -> eyre::Result<Vec<Option<T::Value>>> {
        let tx = self.read_txn()?;
        keys.into_iter().map(|key| tx.get::<T>(key.borrow())).collect()
    }
}

/// Interface that defines a consesus DB.
/// Properly implemented this will allow something to be used as a store for consensus.
pub trait DBMap<K, V>: Send + Sync
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
{
    /// Returns true if the map contains a value for the specified key.
    fn contains_key(&self, key: &K) -> eyre::Result<bool>;

    /// Returns the value for the given key from the map, if it exists.
    fn get(&self, key: &K) -> eyre::Result<Option<V>>;

    /// Inserts the given key-value pair into the map.
    fn insert(&self, key: &K, value: &V) -> eyre::Result<()>;

    /// Removes the entry for the given key from the map.
    fn remove(&self, key: &K) -> eyre::Result<()>;

    /// Removes every key-value pair from the map.
    fn clear(&self) -> eyre::Result<()>;

    /// Returns true if the map is empty, otherwise false.
    fn is_empty(&self) -> bool;

    /// Returns an unbounded iterator visiting each key-value pair in the map.
    /// If this is backed by storage an underlying error will most likely end the iterator early.
    fn iter(&self) -> Box<dyn Iterator<Item = (K, V)> + '_>;

    /// Skips all the elements that are smaller than the given key,
    /// and either lands on the key or the first one greater than
    /// the key.
    fn skip_to(&self, key: &K) -> eyre::Result<Box<dyn Iterator<Item = (K, V)> + '_>>;

    /// Iterates over all the keys in reverse.
    fn reverse_iter(&self) -> Box<dyn Iterator<Item = (K, V)> + '_>;

    /// Returns the record prior to key if it exists or the first record that is sorted before if it
    /// does not exist.
    fn record_prior_to(&self, key: &K) -> Option<(K, V)>;

    /// Returns the last (key, value) in the database.
    fn last_record(&self) -> Option<(K, V)>;

    /// Commit data to durable storage.
    fn commit(&self) -> eyre::Result<()>;
}

// These multi-operations are functions so they can have their own generics without severly limiting
// how Map can be used. TODO: Will need to add some hints/optional functions to Map at some point to
// make these more efficient (i.e. use native DB batching/transactions).

/// Inserts key-value pairs, non-atomically.
pub fn multi_insert<K, V, J, U>(
    db: &dyn DBMap<K, V>,
    key_val_pairs: impl IntoIterator<Item = (J, U)>,
) -> eyre::Result<()>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    J: Borrow<K>,
    U: Borrow<V>,
{
    //let mut txn = db.write_txn()?;
    {
        //let rtxn = &mut txn;
        key_val_pairs
            .into_iter()
            .try_for_each(|(key, value)| db.insert(key.borrow(), value.borrow()))?;
    }
    //txn.commit();
    Ok(())
}

/// Removes keys, non-atomically.
pub fn multi_remove<K, V, J>(
    db: &dyn DBMap<K, V>,
    keys: impl IntoIterator<Item = J>,
) -> eyre::Result<()>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    J: Borrow<K>,
{
    //let mut txn = db.write_txn()?;
    keys.into_iter().try_for_each(|key| db.remove(key.borrow()))
}

/// Returns a vector of values corresponding to the keys provided, non-atomically.
pub fn multi_get<K, V, J>(
    db: &dyn DBMap<K, V>,
    keys: impl IntoIterator<Item = J>,
) -> eyre::Result<Vec<Option<V>>>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    J: Borrow<K>,
{
    //let read_txn = db.read_txn()?;
    keys.into_iter().map(|key| db.get(key.borrow())).collect()
}
