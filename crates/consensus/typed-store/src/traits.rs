// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::TypedStoreError;
use serde::{de::DeserializeOwned, Serialize};
use std::{borrow::Borrow, collections::BTreeMap};

// TODO- need to expose some sort of transaction interface.

pub trait Map<K, V>: Send + Sync
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
{
    /// Returns true if the map contains a value for the specified key.
    fn contains_key(&self, key: &K) -> Result<bool, TypedStoreError>;

    /// Returns the value for the given key from the map, if it exists.
    fn get(&self, key: &K) -> Result<Option<V>, TypedStoreError>;

    /// Inserts the given key-value pair into the map.
    fn insert(&self, key: &K, value: &V) -> Result<(), TypedStoreError>;

    /// Removes the entry for the given key from the map.
    fn remove(&self, key: &K) -> Result<(), TypedStoreError>;

    /// Removes every key-value pair from the map.
    fn clear(&self) -> Result<(), TypedStoreError>;

    /// Returns true if the map is empty, otherwise false.
    fn is_empty(&self) -> bool;

    /// Returns an unbounded iterator visiting each key-value pair in the map.
    /// If this is backed by storage an underlying error will most likely end the iterator early.
    fn iter(&self) -> Box<dyn Iterator<Item = (K, V)> + '_>;

    /// Skips all the elements that are smaller than the given key,
    /// and either lands on the key or the first one greater than
    /// the key.
    fn skip_to(&self, key: &K) -> Result<Box<dyn Iterator<Item = (K, V)> + '_>, TypedStoreError>;

    /// Iterates over all the keys in reverse.
    fn reverse_iter(&self) -> Box<dyn Iterator<Item = (K, V)> + '_>;

    /// Returns the record prior to key if it exists or the first record that is sorted before if it
    /// does not exist.
    fn record_prior_to(&self, key: &K) -> Option<(K, V)>;

    /// Returns the last (key, value) in the database.
    fn last_record(&self) -> Option<(K, V)>;
}

// These multi-operations are functions so they can have their own generics without severly limiting
// how Map can be used. TODO: Will need to add some hints/optional functions to Map at some point to
// make these more efficient (i.e. use native DB batching/transactions).

/// Inserts key-value pairs, non-atomically.
pub fn multi_insert<K, V, J, U>(
    db: &dyn Map<K, V>,
    key_val_pairs: impl IntoIterator<Item = (J, U)>,
) -> Result<(), TypedStoreError>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    J: Borrow<K>,
    U: Borrow<V>,
{
    key_val_pairs.into_iter().try_for_each(|(key, value)| db.insert(key.borrow(), value.borrow()))
}

/// Removes keys, non-atomically.
pub fn multi_remove<K, V, J>(
    db: &dyn Map<K, V>,
    keys: impl IntoIterator<Item = J>,
) -> Result<(), TypedStoreError>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    J: Borrow<K>,
{
    keys.into_iter().try_for_each(|key| db.remove(key.borrow()))
}

/// Returns a vector of values corresponding to the keys provided, non-atomically.
pub fn multi_get<K, V, J>(
    db: &dyn Map<K, V>,
    keys: impl IntoIterator<Item = J>,
) -> Result<Vec<Option<V>>, TypedStoreError>
where
    K: Serialize + DeserializeOwned + Send + Sync,
    V: Serialize + DeserializeOwned + Send + Sync,
    J: Borrow<K>,
{
    keys.into_iter().map(|key| db.get(key.borrow())).collect()
}

pub struct TableSummary {
    pub num_keys: u64,
    pub key_bytes_total: usize,
    pub value_bytes_total: usize,
    pub key_hist: hdrhistogram::Histogram<u64>,
    pub value_hist: hdrhistogram::Histogram<u64>,
}

pub trait TypedStoreDebug {
    /// Dump a DB table with pagination
    fn dump_table(
        &self,
        table_name: String,
        page_size: u16,
        page_number: usize,
    ) -> eyre::Result<BTreeMap<String, String>>;

    /// Get the name of the DB. This is simply the name of the struct
    fn primary_db_name(&self) -> String;

    /// Get a map of table names to key-value types
    fn describe_all_tables(&self) -> BTreeMap<String, (String, String)>;

    /// Count the entries in the table
    fn count_table_keys(&self, table_name: String) -> eyre::Result<usize>;

    /// Return table summary of the input table
    fn table_summary(&self, table_name: String) -> eyre::Result<TableSummary>;
}
