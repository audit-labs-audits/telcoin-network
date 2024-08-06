// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock, RwLockReadGuard},
};

use ouroboros::self_referencing;
use rand::distributions::{Alphanumeric, DistString};
use serde::{de::DeserializeOwned, Serialize};

use crate::Map;

/// An interface to a btree map database. This is mainly intended
/// for tests and performing benchmark comparisons or anywhere where an ephemeral database is useful.
#[derive(Clone, Debug)]
pub struct MemDB<K, V> {
    pub db: Arc<RwLock<BTreeMap<K, V>>>,
    pub name: String,
}

impl<K, V> MemDB<K, V> {
    pub fn open() -> Self {
        MemDB {
            db: Arc::new(RwLock::new(BTreeMap::new())),
            name: Alphanumeric.sample_string(&mut rand::thread_rng(), 16),
        }
    }
}

impl<K, V> Map<K, V> for MemDB<K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone + Send + Sync,
    V: Serialize + DeserializeOwned + Clone + Send + Sync,
{
    fn contains_key(&self, key: &K) -> eyre::Result<bool> {
        Ok(self.db.read().expect("Poisoned read lock!").contains_key(key))
    }

    fn get(&self, key: &K) -> eyre::Result<Option<V>> {
        Ok(self.db.read().expect("Poisoned read lock!").get(key).cloned())
    }

    fn insert(&self, key: &K, value: &V) -> eyre::Result<()> {
        self.db.write().expect("Poisoned write lock!").insert(key.clone(), value.clone());
        Ok(())
    }

    fn remove(&self, key: &K) -> eyre::Result<()> {
        self.db.write().expect("Poisoned write lock!").remove(key);
        Ok(())
    }

    fn clear(&self) -> eyre::Result<()> {
        self.db.write().expect("Poisoned write lock!").clear();
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.db.read().expect("Poisoned read lock!").is_empty()
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (K, V)> + '_> {
        Box::new(
            MemDBIterBuilder {
                guard: self.db.read().expect("Poisoned read lock!"),
                iter_builder: |guard: &mut RwLockReadGuard<'_, BTreeMap<K, V>>| {
                    Box::new(guard.iter().map(|(k, v)| (k.clone(), v.clone())))
                },
            }
            .build(),
        )
    }

    fn skip_to(&self, key: &K) -> eyre::Result<Box<dyn Iterator<Item = (K, V)> + '_>> {
        let key = key.clone();
        Ok(Box::new(
            MemDBIterBuilder {
                guard: self.db.read().expect("Poisoned read lock!"),
                iter_builder: |guard: &mut RwLockReadGuard<'_, BTreeMap<K, V>>| {
                    Box::new(
                        guard
                            .iter()
                            .skip_while(move |(k, _)| **k < key)
                            .map(|(k, v)| (k.clone(), v.clone())),
                    )
                },
            }
            .build(),
        ))
    }

    fn reverse_iter(&self) -> Box<dyn Iterator<Item = (K, V)> + '_> {
        Box::new(
            MemDBIterBuilder {
                guard: self.db.read().expect("Poisoned read lock!"),
                iter_builder: |guard: &mut RwLockReadGuard<'_, BTreeMap<K, V>>| {
                    Box::new(guard.iter().rev().map(|(k, v)| (k.clone(), v.clone())))
                },
            }
            .build(),
        )
    }

    fn record_prior_to(&self, key: &K) -> Option<(K, V)> {
        let mut last = None;
        let guard = self.db.read().expect("Poisoned read lock");
        for (k, v) in guard.iter() {
            if k >= key {
                break;
            }
            last = Some((k, v));
        }
        last.map(|(k, v)| (k.clone(), v.clone()))
    }

    fn last_record(&self) -> Option<(K, V)> {
        self.db
            .read()
            .expect("Poisoned read lock")
            .last_key_value()
            .map(|(k, v)| (k.clone(), v.clone()))
    }
}

#[self_referencing(pub_extras)]
pub struct MemDBIter<'a, K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone + Send + Sync,
    V: Serialize + DeserializeOwned + Clone + Send + Sync,
{
    guard: RwLockReadGuard<'a, BTreeMap<K, V>>,
    #[borrows(mut guard)]
    #[covariant]
    iter: Box<dyn Iterator<Item = (K, V)> + 'this>,
    //iter: std::collections::btree_map::Iter<'this, K, V>,
}

impl<'a, K, V> Iterator for MemDBIter<'a, K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone + Send + Sync,
    V: Serialize + DeserializeOwned + Clone + Send + Sync,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.with_mut(|fields| fields.iter.next())
    }
}

#[cfg(test)]
mod test {
    use crate::{
        mem_db::MemDB,
        traits::{multi_get, multi_insert, multi_remove},
        Map,
    };

    #[test]
    fn test_contains_key() {
        let db = MemDB::open();
        db.insert(&123456789, &"123456789".to_string()).expect("Failed to insert");
        assert!(db.contains_key(&123456789).expect("Failed to call contains key"));
        assert!(!db.contains_key(&000000000).expect("Failed to call contains key"));
    }

    #[test]
    fn test_get() {
        let db = MemDB::open();
        db.insert(&123456789, &"123456789".to_string()).expect("Failed to insert");
        assert_eq!(Some("123456789".to_string()), db.get(&123456789).expect("Failed to get"));
        assert_eq!(None, db.get(&000000000).expect("Failed to get"));
    }

    #[test]
    fn test_multi_get() {
        let db = MemDB::open();
        db.insert(&123, &"123".to_string()).expect("Failed to insert");
        db.insert(&456, &"456".to_string()).expect("Failed to insert");

        let result = multi_get(&db, [123, 456, 789]).expect("Failed to multi get");

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Some("123".to_string()));
        assert_eq!(result[1], Some("456".to_string()));
        assert_eq!(result[2], None);
    }

    #[test]
    fn test_remove() {
        let db = MemDB::open();
        db.insert(&123456789, &"123456789".to_string()).expect("Failed to insert");
        assert!(db.get(&123456789).expect("Failed to get").is_some());

        db.remove(&123456789).expect("Failed to remove");
        assert!(db.get(&123456789).expect("Failed to get").is_none());
    }

    #[test]
    fn test_iter() {
        let db = MemDB::open();
        db.insert(&123456789, &"123456789".to_string()).expect("Failed to insert");

        let mut iter = db.iter();
        assert_eq!(Some((123456789, "123456789".to_string())), iter.next());
        assert_eq!(None, iter.next());
    }

    #[test]
    fn test_iter_reverse() {
        let db = MemDB::open();
        db.insert(&1, &"1".to_string()).expect("Failed to insert");
        db.insert(&2, &"2".to_string()).expect("Failed to insert");
        db.insert(&3, &"3".to_string()).expect("Failed to insert");
        let mut iter = db.iter();

        assert_eq!(Some((1, "1".to_string())), iter.next());
        assert_eq!(Some((2, "2".to_string())), iter.next());
        assert_eq!(Some((3, "3".to_string())), iter.next());
        assert_eq!(None, iter.next());
    }

    #[test]
    fn test_insert_batch() {
        let db = MemDB::open();
        let keys_vals = (1..100).map(|i| (i, i.to_string()));
        multi_insert(&db, keys_vals.clone()).expect("Failed to batch insert");
        for (k, v) in keys_vals {
            let val = db.get(&k).expect("Failed to get inserted key");
            assert_eq!(Some(v), val);
        }
    }

    #[test]
    fn test_insert_batch_across_cf() {
        let db_cf_1 = MemDB::open();
        let keys_vals_1 = (1..100).map(|i| (i, i.to_string()));

        let db_cf_2 = MemDB::open();
        let keys_vals_2 = (1000..1100).map(|i| (i, i.to_string()));

        multi_insert(&db_cf_1, keys_vals_1.clone()).expect("Failed to batch insert");
        multi_insert(&db_cf_2, keys_vals_2.clone()).expect("Failed to batch insert");
        for (k, v) in keys_vals_1 {
            let val = db_cf_1.get(&k).expect("Failed to get inserted key");
            assert_eq!(Some(v), val);
        }

        for (k, v) in keys_vals_2 {
            let val = db_cf_2.get(&k).expect("Failed to get inserted key");
            assert_eq!(Some(v), val);
        }
    }

    #[test]
    fn test_delete_batch() {
        let db: MemDB<i32, String> = MemDB::open();

        let keys_vals = (1..100).map(|i| (i, i.to_string()));
        multi_insert(&db, keys_vals).expect("Failed to batch insert");

        // delete the odd-index keys
        let deletion_keys = (1..100).step_by(2);
        multi_remove(&db, deletion_keys).expect("Failed to batch delete");

        for (k, _v) in db.iter() {
            assert_eq!(k % 2, 0);
        }
    }

    #[test]
    fn test_delete_range() {
        let db: MemDB<i32, String> = MemDB::open();

        // Note that the last element is (100, "100".to_owned()) here
        let keys_vals = (0..101).map(|i| (i, i.to_string()));
        multi_insert(&db, keys_vals).expect("Failed to batch insert");

        multi_remove(&db, 50..100).expect("Failed to delete range");

        for k in 0..50 {
            assert!(db.contains_key(&k).expect("Failed to query legal key"),);
        }
        for k in 50..100 {
            assert!(!db.contains_key(&k).expect("Failed to query legal key"));
        }

        // range operator is not inclusive of to
        assert!(db.contains_key(&100).expect("Failed to query legel key"));
    }

    #[test]
    fn test_clear() {
        let db: MemDB<i32, String> = MemDB::open();

        // Test clear of empty map
        let _ = db.clear();

        let keys_vals = (0..101).map(|i| (i, i.to_string()));
        multi_insert(&db, keys_vals).expect("Failed to batch insert");

        // Check we have multiple entries
        assert!(db.iter().count() > 1);
        let _ = db.clear();
        assert_eq!(db.iter().count(), 0);
        // Clear again to ensure safety when clearing empty map
        let _ = db.clear();
        assert_eq!(db.iter().count(), 0);
        // Clear with one item
        let _ = db.insert(&1, &"e".to_string());
        assert_eq!(db.iter().count(), 1);
        let _ = db.clear();
        assert_eq!(db.iter().count(), 0);
    }

    #[test]
    fn test_is_empty() {
        let db: MemDB<i32, String> = MemDB::open();

        // Test empty map is truly empty
        assert!(db.is_empty());
        let _ = db.clear();
        assert!(db.is_empty());

        let keys_vals = (0..101).map(|i| (i, i.to_string()));
        multi_insert(&db, keys_vals).expect("Failed to batch insert");

        // Check we have multiple entries and not empty
        assert!(db.iter().count() > 1);
        assert!(!db.is_empty());

        // Clear again to ensure empty works after clearing
        let _ = db.clear();
        assert_eq!(db.iter().count(), 0);
        assert!(db.is_empty());
    }

    #[test]
    fn test_multi_insert() {
        // Init a DB
        let db: MemDB<i32, String> = MemDB::open();

        // Create kv pairs
        let keys_vals = (0..101).map(|i| (i, i.to_string()));

        multi_insert(&db, keys_vals.clone()).expect("Failed to multi-insert");

        for (k, v) in keys_vals {
            let val = db.get(&k).expect("Failed to get inserted key");
            assert_eq!(Some(v), val);
        }
    }

    #[test]
    fn test_multi_remove() {
        // Init a DB
        let db: MemDB<i32, String> = MemDB::open();

        // Create kv pairs
        let keys_vals = (0..101).map(|i| (i, i.to_string()));

        multi_insert(&db, keys_vals.clone()).expect("Failed to multi-insert");

        // Check insertion
        for (k, v) in keys_vals.clone() {
            let val = db.get(&k).expect("Failed to get inserted key");
            assert_eq!(Some(v), val);
        }

        // Remove 50 items
        multi_remove(&db, keys_vals.clone().map(|kv| kv.0).take(50))
            .expect("Failed to multi-remove");
        assert_eq!(db.iter().count(), 101 - 50);

        // Check that the remaining are present
        for (k, v) in keys_vals.skip(50) {
            let val = db.get(&k).expect("Failed to get inserted key");
            assert_eq!(Some(v), val);
        }
    }
}
