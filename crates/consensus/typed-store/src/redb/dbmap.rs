// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt::Debug,
    path::Path,
    sync::{Arc, RwLock, RwLockReadGuard},
};

use ouroboros::self_referencing;
use redb::{
    Database, ReadOnlyTable, ReadTransaction, ReadableTable, ReadableTableMetadata,
    TableDefinition, WriteTransaction,
};
use serde::{de::DeserializeOwned, Serialize};

use crate::DBMap;

use super::wraps::{KeyWrap, ValWrap};

#[macro_export]
macro_rules! reopen_redb {
    ( $db:expr, $($cf:expr;<$K:ty, $V:ty>),*) => {
        (
            $(
                Arc::new($crate::redb::dbmap::ReDBMap::<$K, $V>::reopen($db.clone(), $crate::redb::TableDefinition::<$crate::redb::wraps::KeyWrap<$K>, $crate::redb::wraps::ValWrap<$V>>::new($cf)).expect("can not open database"))
            ),*
        )
    };
}

/// An interface to a btree map database. This is mainly intended
/// for tests and performing benchmark comparisons or anywhere where an ephemeral database is
/// useful.
pub struct ReDBMap<'a, K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone + Send + Sync + Debug + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static,
{
    db: Arc<RwLock<Database>>,
    table_def: TableDefinition<'a, KeyWrap<K>, ValWrap<V>>,
}

pub fn open_redb<P: AsRef<Path>>(path: P) -> eyre::Result<Arc<RwLock<Database>>> {
    Ok(Arc::new(RwLock::new(Database::create(path.as_ref().join("redb"))?)))
}

impl<'a, K, V> ReDBMap<'a, K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone + Send + Sync + Debug,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug,
{
    pub fn reopen(
        db: Arc<RwLock<Database>>,
        table_def: TableDefinition<'a, KeyWrap<K>, ValWrap<V>>,
    ) -> eyre::Result<Self> {
        let txn = db.read().expect("poisoned lock").begin_write()?;
        txn.open_table(table_def)?;
        txn.commit()?;
        Ok(Self { db, table_def })
    }

    fn read_txn(&self) -> eyre::Result<ReadTransaction> {
        Ok(self.db.read().expect("Poisoned lock!").begin_read()?)
    }

    fn read_table(&self) -> eyre::Result<ReadOnlyTable<KeyWrap<K>, ValWrap<V>>> {
        Ok(self.read_txn()?.open_table(self.table_def)?)
    }

    fn write_txn(&self) -> eyre::Result<WriteTransaction> {
        Ok(self.db.write().expect("Poisoned lock!").begin_write()?)
    }
}

impl<'a, K, V> DBMap<K, V> for ReDBMap<'a, K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone + Send + Sync + Debug,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug,
{
    fn contains_key(&self, key: &K) -> eyre::Result<bool> {
        Ok(self.read_table()?.get(key)?.map(|_| true).unwrap_or_default())
    }

    fn get(&self, key: &K) -> eyre::Result<Option<V>> {
        Ok(self.read_table()?.get(key)?.map(|v| v.value().clone()))
    }

    fn insert(&self, key: &K, value: &V) -> eyre::Result<()> {
        let txn = self.write_txn()?;
        {
            let mut table = txn.open_table(self.table_def)?;
            table.insert(key, value)?;
        }
        txn.commit()?;
        Ok(())
    }

    fn remove(&self, key: &K) -> eyre::Result<()> {
        let txn = self.write_txn()?;
        {
            let mut table = txn.open_table(self.table_def)?;
            table.remove(key)?;
        }
        txn.commit()?;
        Ok(())
    }

    fn clear(&self) -> eyre::Result<()> {
        let txn = self.write_txn()?;
        {
            let mut table = txn.open_table(self.table_def)?;
            table.retain(|_, _| false)?;
        }
        txn.commit()?;
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.read_table().map(|t| t.is_empty().unwrap_or(true)).unwrap_or(true)
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (K, V)> + '_> {
        let guard = self.db.read().expect("Poisoned lock!");
        Box::new(
            ReDBIterBuilder {
                guard,
                table_builder: |guard: &mut RwLockReadGuard<'_, Database>| {
                    guard
                        .begin_read()
                        .expect("XXXX ")
                        .open_table(self.table_def)
                        .expect("XXXX No read table")
                },
                iter_builder: |table: &ReadOnlyTable<KeyWrap<K>, ValWrap<V>>| {
                    Box::new(table.iter().expect("XXXX no iter").filter(|r| r.is_ok()).map(|r| {
                        let (k, v) = r.unwrap();
                        (k.value().clone(), v.value().clone())
                    }))
                },
            }
            .build(),
        )
    }

    fn skip_to(&self, key: &K) -> eyre::Result<Box<dyn Iterator<Item = (K, V)> + '_>> {
        let guard = self.db.read().expect("Poisoned lock!");
        let key = key.clone();
        Ok(Box::new(
            ReDBIterBuilder {
                guard,
                table_builder: |guard: &mut RwLockReadGuard<'_, Database>| {
                    guard
                        .begin_read()
                        .expect("XXXX ")
                        .open_table(self.table_def)
                        .expect("XXXX No read table")
                },
                iter_builder: |table: &ReadOnlyTable<KeyWrap<K>, ValWrap<V>>| {
                    Box::new(
                        table
                            .iter()
                            .expect("XXXX no iter")
                            .filter(|r| r.is_ok())
                            .map(|r| {
                                let (k, v) = r.unwrap();
                                (k.value().clone(), v.value().clone())
                            })
                            .skip_while(move |(k, _)| k < &key),
                    )
                },
            }
            .build(),
        ))
    }

    fn reverse_iter(&self) -> Box<dyn Iterator<Item = (K, V)> + '_> {
        let guard = self.db.read().expect("Poisoned lock!");
        Box::new(
            ReDBIterBuilder {
                guard,
                table_builder: |guard: &mut RwLockReadGuard<'_, Database>| {
                    guard
                        .begin_read()
                        .expect("XXXX ")
                        .open_table(self.table_def)
                        .expect("XXXX No read table")
                },
                iter_builder: |table: &ReadOnlyTable<KeyWrap<K>, ValWrap<V>>| {
                    Box::new(table.iter().expect("XXXX no iter").rev().filter(|r| r.is_ok()).map(
                        |r| {
                            let (k, v) = r.unwrap();
                            (k.value().clone(), v.value().clone())
                        },
                    ))
                },
            }
            .build(),
        )
    }

    fn record_prior_to(&self, key: &K) -> Option<(K, V)> {
        let mut last = None;
        for (k, v) in self.read_table().ok()?.iter().ok()?.flatten() {
            let (k, v) = (k.value().clone(), v.value().clone());
            if &k >= key {
                break;
            }
            last = Some((k, v));
        }
        last.map(|(k, v)| (k.clone(), v.clone()))
    }

    fn last_record(&self) -> Option<(K, V)> {
        self.read_table()
            .map(|t| t.last().ok().flatten().map(|(k, v)| (k.value().clone(), v.value().clone())))
            .ok()
            .flatten()
    }

    fn commit(&self) -> eyre::Result<()> {
        Ok(())
    }
}

#[self_referencing(pub_extras)]
pub struct ReDBIter<'a, K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone + Send + Sync + Debug + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static,
{
    guard: RwLockReadGuard<'a, Database>,
    #[borrows(mut guard)]
    table: ReadOnlyTable<KeyWrap<K>, ValWrap<V>>,
    #[borrows(table)]
    #[covariant]
    iter: Box<dyn Iterator<Item = (K, V)> + 'this>,
}

impl<'a, K, V> Iterator for ReDBIter<'a, K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone + Send + Sync + Debug + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.with_mut(|fields| fields.iter.next())
    }
}
