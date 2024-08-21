// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, fmt::Debug, marker::PhantomData, sync::Arc};

use bincode::Options;
use dashmap::DashMap;
use ouroboros::self_referencing;
use parking_lot::{RwLock, RwLockReadGuard};
use serde::{Deserialize, Serialize};

use crate::traits::{DBIter, Database, DbTx, DbTxMut, Table};

type StoreType = DashMap<&'static str, Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>>;

fn get<T: crate::traits::Table>(store: &StoreType, key: &T::Key) -> eyre::Result<Option<T::Value>> {
    if let Some(table) = store.get(T::NAME) {
        let key_bytes = encode(key);
        if let Some(val_bytes) = table.read().get(&key_bytes) {
            let val = decode(val_bytes);
            return Ok(Some(val));
        }
    }
    Ok(None)
}

#[derive(Clone, Debug)]
pub struct MemDbTx {
    store: StoreType,
}

impl DbTx for MemDbTx {
    fn get<T: crate::traits::Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        get::<T>(&self.store, key)
    }
}

#[derive(Clone, Debug)]
pub struct MemDbTxMut {
    store: StoreType,
}

impl DbTx for MemDbTxMut {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        get::<T>(&self.store, key)
    }
}

impl DbTxMut for MemDbTxMut {
    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        if let Some(table) = self.store.get(T::NAME) {
            let key_bytes = encode(key);
            let value_bytes = encode(value);
            table.write().insert(key_bytes, value_bytes);
        }
        Ok(())
    }

    fn remove<T: Table>(&mut self, key: &T::Key) -> eyre::Result<()> {
        if let Some(table) = self.store.get(T::NAME) {
            let key_bytes = encode(key);
            table.write().remove(&key_bytes);
        }
        Ok(())
    }

    fn clear_table<T: Table>(&mut self) -> eyre::Result<()> {
        if let Some(table) = self.store.get(T::NAME) {
            table.write().clear();
        }
        Ok(())
    }

    fn commit(self) -> eyre::Result<()> {
        // We are already "committed"...
        Ok(())
    }
}

/// Implement the Database trait with an in-memory store.
/// This means no persistance.
/// This DB also plays loose with transactions, but since it is in-memory and we do not do
/// roll-backs this should be fine.
#[derive(Clone, Debug)]
pub struct MemDatabase {
    store: StoreType,
}

impl MemDatabase {
    pub fn new() -> Self {
        Self { store: DashMap::new() }
    }

    pub fn open_table<T: Table>(&self) {
        self.store.insert(T::NAME, Arc::new(RwLock::new(BTreeMap::new())));
    }
}

impl Default for MemDatabase {
    fn default() -> Self {
        Self::new()
    }
}

impl Database for MemDatabase {
    type TX<'txn> = MemDbTx
    where
        Self: 'txn;

    type TXMut<'txn> = MemDbTxMut
    where
        Self: 'txn;

    fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
        Ok(MemDbTx { store: self.store.clone() })
    }

    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
        Ok(MemDbTxMut { store: self.store.clone() })
    }

    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        if let Some(table) = self.store.get(T::NAME) {
            let key_bytes = encode(key);
            return Ok(table.read().contains_key(&key_bytes));
        }
        Ok(false)
    }

    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        get::<T>(&self.store, key)
    }

    fn insert<T: Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        if let Some(table) = self.store.get(T::NAME) {
            let key_bytes = encode(key);
            let value_bytes = encode(value);
            table.write().insert(key_bytes, value_bytes);
        }
        Ok(())
    }

    fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()> {
        if let Some(table) = self.store.get(T::NAME) {
            let key_bytes = encode(key);
            table.write().remove(&key_bytes);
        }
        Ok(())
    }

    fn clear_table<T: Table>(&self) -> eyre::Result<()> {
        if let Some(table) = self.store.get(T::NAME) {
            table.write().clear();
        }
        Ok(())
    }

    fn is_empty<T: Table>(&self) -> bool {
        if let Some(table) = self.store.get(T::NAME) {
            table.read().is_empty()
        } else {
            false
        }
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        if let Some(table) = self.store.get(T::NAME) {
            Box::new(
                MemDBIterBuilder {
                    table: TabAndGuardBuilder {
                        table: table.clone(),
                        guard_builder: |table| table.read(),
                        casper: PhantomData::<T>,
                    }
                    .build(),
                    iter_builder: |table: &'_ TabAndGuard<T>| {
                        table.with(|fields| {
                            let iter = Box::new(fields.guard.iter());
                            iter
                        })
                    },
                    casper: PhantomData::<T>,
                }
                .build(),
            )
        } else {
            panic!("Invalid table {}", T::NAME);
        }
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        if let Some(table) = self.store.get(T::NAME) {
            Ok(Box::new(
                MemDBIterBuilder {
                    table: TabAndGuardBuilder {
                        table: table.clone(),
                        guard_builder: |table| table.read(),
                        casper: PhantomData::<T>,
                    }
                    .build(),
                    iter_builder: |table: &'_ TabAndGuard<T>| {
                        table.with(|fields| {
                            let key_bytes = encode(key);
                            let iter = Box::new(
                                fields.guard.iter().skip_while(move |(k, _)| **k < key_bytes),
                            );
                            iter
                        })
                    },
                    casper: PhantomData::<T>,
                }
                .build(),
            ))
        } else {
            Err(eyre::eyre!("Invalid table {}", T::NAME))
        }
    }

    fn reverse_iter<T: Table>(&self) -> crate::traits::DBIter<'_, T> {
        if let Some(table) = self.store.get(T::NAME) {
            Box::new(
                MemDBIterBuilder {
                    table: TabAndGuardBuilder {
                        table: table.clone(),
                        guard_builder: |table| table.read(),
                        casper: PhantomData::<T>,
                    }
                    .build(),
                    iter_builder: |table: &'_ TabAndGuard<T>| {
                        table.with(|fields| {
                            let iter = Box::new(fields.guard.iter().rev());
                            iter
                        })
                    },
                    casper: PhantomData::<T>,
                }
                .build(),
            )
        } else {
            panic!("Invalid table {}", T::NAME);
        }
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        if let Some(table) = self.store.get(T::NAME) {
            let key_bytes = encode(key);
            let mut last = None;
            let guard = table.read();
            for (k, v) in guard.iter() {
                if k >= &key_bytes {
                    break;
                }
                last = Some((k, v));
            }
            last.map(|(key_bytes, value_bytes)| {
                let key = decode(key_bytes);
                let value = decode(value_bytes);
                (key, value)
            })
        } else {
            None
        }
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        if let Some(table) = self.store.get(T::NAME) {
            table.read().last_key_value().map(|(key_bytes, value_bytes)| {
                let key = decode(key_bytes);
                let value = decode(value_bytes);
                (key, value)
            })
        } else {
            None
        }
    }
}

fn decode<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> T {
    bincode::DefaultOptions::new()
        .with_big_endian()
        .with_fixint_encoding()
        .deserialize(bytes)
        .expect("Invalid bytes!")
}

fn encode<T: Serialize>(obj: &T) -> Vec<u8> {
    bincode::DefaultOptions::new()
        .with_big_endian()
        .with_fixint_encoding()
        .serialize(obj)
        .expect("Can not serialize!")
}

#[self_referencing]
struct TabAndGuard<T>
where
    T: Table,
{
    casper: PhantomData<T>,
    table: Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>,
    #[borrows(table)]
    #[covariant]
    guard: RwLockReadGuard<'this, BTreeMap<Vec<u8>, Vec<u8>>>,
}

#[self_referencing]
pub struct MemDBIter<T>
where
    T: Table,
{
    casper: PhantomData<T>,
    table: TabAndGuard<T>,
    #[borrows(table)]
    #[not_covariant]
    iter: Box<dyn Iterator<Item = (&'this Vec<u8>, &'this Vec<u8>)> + 'this>,
}

impl<T: Table> Iterator for MemDBIter<T> {
    type Item = (T::Key, T::Value);

    fn next(&mut self) -> Option<Self::Item> {
        self.with_mut(|fields| {
            fields.iter.next().map(|(key_bytes, value_bytes)| {
                let key = bincode::DefaultOptions::new()
                    .with_big_endian()
                    .with_fixint_encoding()
                    .deserialize(key_bytes)
                    .expect("Invalid bytes!");
                let value = bincode::DefaultOptions::new()
                    .with_big_endian()
                    .with_fixint_encoding()
                    .deserialize(value_bytes)
                    .expect("Invalid bytes!");
                (key, value)
            })
        })
    }
}

#[cfg(test)]
mod test {
    use crate::{
        mem_db::MemDatabase,
        traits::{Database, DbTxMut, Table},
    };

    #[derive(Debug)]
    struct TestTable {}
    impl Table for TestTable {
        type Key = u64;
        type Value = String;

        const NAME: &'static str = "TestTable";
    }

    fn open_db() -> MemDatabase {
        let db = MemDatabase::new();
        db.open_table::<TestTable>();
        db
    }

    #[test]
    fn test_memdb_contains_key() {
        let db = open_db();
        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");
        assert!(db.contains_key::<TestTable>(&123456789).expect("Failed to call contains key"));
        assert!(!db.contains_key::<TestTable>(&000000000).expect("Failed to call contains key"));
    }

    #[test]
    fn test_memdb_get() {
        let db = open_db();
        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");
        assert_eq!(
            Some("123456789".to_string()),
            db.get::<TestTable>(&123456789).expect("Failed to get")
        );
        assert_eq!(None, db.get::<TestTable>(&000000000).expect("Failed to get"));
    }

    #[test]
    fn test_memdb_multi_get() {
        let db = open_db();
        db.insert::<TestTable>(&123, &"123".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&456, &"456".to_string()).expect("Failed to insert");

        let result = db.multi_get::<TestTable>([&123, &456, &789]).expect("Failed to multi get");

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Some("123".to_string()));
        assert_eq!(result[1], Some("456".to_string()));
        assert_eq!(result[2], None);
    }

    #[test]
    fn test_memdb_skip() {
        let db = open_db();

        db.insert::<TestTable>(&123, &"123".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&456, &"456".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&789, &"789".to_string()).expect("Failed to insert");

        // Skip all smaller
        let key_vals: Vec<_> = db.skip_to::<TestTable>(&456).expect("Seek failed").collect();
        assert_eq!(key_vals.len(), 2);
        assert_eq!(key_vals[0], (456, "456".to_string()));
        assert_eq!(key_vals[1], (789, "789".to_string()));

        // Skip to the end
        assert_eq!(db.skip_to::<TestTable>(&999).expect("Seek failed").count(), 0);

        // Skip to last
        assert_eq!(db.last_record::<TestTable>(), Some((789, "789".to_string())));

        // Skip to successor of first value
        assert_eq!(db.skip_to::<TestTable>(&000).expect("Skip failed").count(), 3);
    }

    #[test]
    fn test_memdb_skip_to_previous_simple() {
        let db = open_db();

        let mut txn = db.write_txn().unwrap();
        txn.insert::<TestTable>(&123, &"123".to_string()).expect("Failed to insert");
        txn.insert::<TestTable>(&456, &"456".to_string()).expect("Failed to insert");
        txn.insert::<TestTable>(&789, &"789".to_string()).expect("Failed to insert");
        txn.commit().unwrap();

        // Skip to the one before the end
        let key_val = db.record_prior_to::<TestTable>(&999).expect("Seek failed");
        assert_eq!(key_val, (789, "789".to_string()));

        // Skip to prior of first value
        // Note: returns an empty iterator!
        assert!(db.record_prior_to::<TestTable>(&000).is_none());
    }

    #[test]
    fn test_memdb_iter_skip_to_previous_gap() {
        let db = open_db();

        let mut txn = db.write_txn().unwrap();
        for i in 1..100 {
            if i != 50 {
                txn.insert::<TestTable>(&i, &i.to_string()).unwrap();
            }
        }
        txn.commit().unwrap();

        // Skip prior to will return an iterator starting with an "unexpected" key if the sought one
        // is not in the table
        let val = db.record_prior_to::<TestTable>(&50).map(|(k, _)| k).unwrap();
        assert_eq!(49, val);
    }

    #[test]
    fn test_memdb_remove() {
        let db = open_db();
        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");
        assert!(db.get::<TestTable>(&123456789).expect("Failed to get").is_some());

        db.remove::<TestTable>(&123456789).expect("Failed to remove");
        assert!(db.get::<TestTable>(&123456789).expect("Failed to get").is_none());
    }

    #[test]
    fn test_memdb_iter() {
        let db = open_db();
        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");

        let mut iter = db.iter::<TestTable>();
        assert_eq!(Some((123456789, "123456789".to_string())), iter.next());
        assert_eq!(None, iter.next());
    }

    #[test]
    fn test_memdb_iter_reverse() {
        let db = open_db();
        db.insert::<TestTable>(&1, &"1".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&2, &"2".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&3, &"3".to_string()).expect("Failed to insert");
        let mut iter = db.iter::<TestTable>();

        assert_eq!(Some((1, "1".to_string())), iter.next());
        assert_eq!(Some((2, "2".to_string())), iter.next());
        assert_eq!(Some((3, "3".to_string())), iter.next());
        assert_eq!(None, iter.next());
    }

    #[test]
    fn test_memdb_clear() {
        let db = open_db();

        // Test clear of empty map
        let _ = db.clear_table::<TestTable>();

        let mut txn = db.write_txn().unwrap();
        for (key, val) in (0..101).map(|i| (i, i.to_string())) {
            txn.insert::<TestTable>(&key, &val).expect("Failed to batch insert");
        }
        txn.commit().unwrap();

        // Check we have multiple entries
        assert!(db.iter::<TestTable>().count() > 1);
        let _ = db.clear_table::<TestTable>();
        assert_eq!(db.iter::<TestTable>().count(), 0);
        // Clear again to ensure safety when clearing empty map
        let _ = db.clear_table::<TestTable>();
        assert_eq!(db.iter::<TestTable>().count(), 0);
        // Clear with one item
        let _ = db.insert::<TestTable>(&1, &"e".to_string());
        assert_eq!(db.iter::<TestTable>().count(), 1);
        let _ = db.clear_table::<TestTable>();
        assert_eq!(db.iter::<TestTable>().count(), 0);
    }

    #[test]
    fn test_memdb_is_empty() {
        let db = open_db();

        // Test empty map is truly empty
        assert!(db.is_empty::<TestTable>());
        let _ = db.clear_table::<TestTable>();
        assert!(db.is_empty::<TestTable>());

        let mut txn = db.write_txn().unwrap();
        for (key, val) in (0..101).map(|i| (i, i.to_string())) {
            txn.insert::<TestTable>(&key, &val).expect("Failed to batch insert");
        }
        txn.commit().unwrap();

        // Check we have multiple entries and not empty
        assert!(db.iter::<TestTable>().count() > 1);
        assert!(!db.is_empty::<TestTable>());

        // Clear again to ensure empty works after clearing
        let _ = db.clear_table::<TestTable>();
        assert_eq!(db.iter::<TestTable>().count(), 0);
        assert!(db.is_empty::<TestTable>());
    }

    #[test]
    fn test_memdb_multi_insert() {
        // Init a DB
        let db = open_db();

        let mut txn = db.write_txn().unwrap();
        for (key, val) in (0..101).map(|i| (i, i.to_string())) {
            txn.insert::<TestTable>(&key, &val).expect("Failed to batch insert");
        }
        txn.commit().unwrap();

        for (k, v) in (0..101).map(|i| (i, i.to_string())) {
            let val = db.get::<TestTable>(&k).expect("Failed to get inserted key");
            assert_eq!(Some(v), val);
        }
    }

    #[test]
    fn test_memdb_multi_remove() {
        // Init a DB
        let db = open_db();

        // Create kv pairs
        let mut txn = db.write_txn().unwrap();
        for (key, val) in (0..101).map(|i| (i, i.to_string())) {
            txn.insert::<TestTable>(&key, &val).expect("Failed to batch insert");
        }
        txn.commit().unwrap();

        // Check insertion
        for (k, v) in (0..101).map(|i| (i, i.to_string())) {
            let val = db.get::<TestTable>(&k).expect("Failed to get inserted key");
            assert_eq!(Some(v), val);
        }

        // Remove 50 items
        let mut txn = db.write_txn().unwrap();
        for (key, _val) in (0..101).map(|i| (i, i.to_string())).take(50) {
            txn.remove::<TestTable>(&key).expect("Failed to batch remove");
        }
        txn.commit().unwrap();
        assert_eq!(db.iter::<TestTable>().count(), 101 - 50);

        // Check that the remaining are present
        for (k, v) in (0..101).map(|i| (i, i.to_string())).skip(50) {
            let val = db.get::<TestTable>(&k).expect("Failed to get inserted key");
            assert_eq!(Some(v), val);
        }
    }
}
