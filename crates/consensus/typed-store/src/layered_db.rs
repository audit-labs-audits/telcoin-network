// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt::Debug,
    marker::PhantomData,
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    thread::JoinHandle,
};

use crate::{
    mem_db::MemDatabase,
    traits::{DBIter, Database, DbTx, DbTxMut, Table},
};

#[derive(Clone, Debug)]
pub struct LayeredDbTx {
    mem_db: MemDatabase,
}

impl DbTx for LayeredDbTx {
    fn get<T: crate::traits::Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        self.mem_db.get::<T>(key)
    }
}

#[derive(Clone)]
pub struct LayeredDbTxMut<DB: Database> {
    mem_db: MemDatabase,
    tx: Sender<DBMessage<DB>>,
}

impl<DB: Database> Debug for LayeredDbTxMut<DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LayeredDbTxMut")
    }
}

impl<DB: Database> DbTx for LayeredDbTxMut<DB> {
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        self.mem_db.get::<T>(key)
    }
}

impl<DB: Database> DbTxMut for LayeredDbTxMut<DB> {
    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        self.mem_db.insert::<T>(key, value)?;
        let ins = Box::new(KeyValueInsert::<T> { key: key.clone(), value: value.clone() });
        self.tx.send(DBMessage::Insert(ins)).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }

    fn remove<T: Table>(&mut self, key: &T::Key) -> eyre::Result<()> {
        self.mem_db.remove::<T>(key)?;
        let rm = Box::new(KeyRemove::<T> { key: key.clone() });
        self.tx.send(DBMessage::Remove(rm)).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }

    fn clear_table<T: Table>(&mut self) -> eyre::Result<()> {
        self.mem_db.clear_table::<T>()?;
        let clr = Box::new(ClearTable::<T> { _casper: PhantomData });
        self.tx.send(DBMessage::Clear(clr)).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }

    fn commit(self) -> eyre::Result<()> {
        self.tx.send(DBMessage::CommitTxn).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }
}

/// Run the thread to manage the persistant DB in the background.
fn db_run<DB: Database>(db: DB, rx: Receiver<DBMessage<DB>>) {
    let mut txn = None;
    while let Ok(msg) = rx.recv() {
        match msg {
            DBMessage::StartTxn => match db.write_txn() {
                Ok(ntxn) => txn = Some(ntxn),
                Err(e) => tracing::error!("DB ERROR getting write txn (background): {e}"),
            },
            DBMessage::CommitTxn => {
                if let Some(txn) = txn.take() {
                    if let Err(e) = txn.commit() {
                        tracing::error!("DB TXN Commit: {e}")
                    }
                }
            }
            DBMessage::Insert(ins) => {
                if let Some(txn) = &mut txn {
                    if let Err(e) = ins.insert_txn(txn) {
                        tracing::error!("DB TXN Insert: {e}")
                    }
                } else if let Err(e) = ins.insert(&db) {
                    tracing::error!("DB Insert: {e}")
                }
            }
            DBMessage::Remove(rm) => {
                if let Some(txn) = &mut txn {
                    if let Err(e) = rm.remove_txn(txn) {
                        tracing::error!("DB TXN Remove: {e}")
                    }
                } else if let Err(e) = rm.remove(&db) {
                    tracing::error!("DB Remove: {e}")
                }
            }
            DBMessage::Clear(clr) => {
                if let Some(txn) = &mut txn {
                    if let Err(e) = clr.clear_table_txn(txn) {
                        tracing::error!("DB TXN Clear table: {e}")
                    }
                } else if let Err(e) = clr.clear_table(&db) {
                    tracing::error!("DB Clear: {e}")
                }
            }
            DBMessage::Shutdown => break,
        }
    }
}

/// Implement the Database trait with an in-memory store.
/// This means no persistance.
/// This DB also plays loose with transactions, but since it is in-memory and we do not do
/// roll-backs this should be fine.
#[derive(Clone, Debug)]
pub struct LayeredDatabase<DB: Database> {
    mem_db: MemDatabase,
    db: DB,
    tx: Sender<DBMessage<DB>>,
    thread: Option<Arc<JoinHandle<()>>>, /* Use as a ref count for shuting down the background
                                          * thread and it's handle. */
}

impl<DB: Database> Drop for LayeredDatabase<DB> {
    fn drop(&mut self) {
        if Arc::strong_count(self.thread.as_ref().expect("no db thread!")) <= 1 {
            tracing::info!("LayeredDatabase Dropping, shutting down DB thread");
            if let Err(e) = self.tx.send(DBMessage::Shutdown) {
                tracing::error!("Error while trying to send shutdown to layered DB thread {e}");
            }
            // We can not be here without a thread handle so unwraps OK.
            if let Err(e) = Arc::into_inner(self.thread.take().unwrap()).unwrap().join() {
                tracing::error!("Error while waiting for shutdown of layered DB thread {e:?}");
            }
        }
    }
}

impl<DB: Database> LayeredDatabase<DB> {
    pub fn open(db: DB) -> Self {
        let (tx, rx) = mpsc::channel();
        let db_cloned = db.clone();
        let thread = Some(Arc::new(std::thread::spawn(move || db_run(db_cloned, rx))));
        Self { mem_db: MemDatabase::default(), db, tx, thread }
    }

    pub fn open_table<T: Table>(&self) {
        self.mem_db.open_table::<T>();
        for (key, value) in self.db.iter::<T>() {
            // mem db insert should not fail.
            let _ = self.mem_db.insert::<T>(&key, &value);
        }
    }
}

impl<DB: Database> Database for LayeredDatabase<DB> {
    type TX<'txn> = LayeredDbTx
    where
        Self: 'txn;

    type TXMut<'txn> = LayeredDbTxMut<DB>
    where
        Self: 'txn;

    fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
        Ok(LayeredDbTx { mem_db: self.mem_db.clone() })
    }

    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
        self.tx.send(DBMessage::StartTxn).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(LayeredDbTxMut { mem_db: self.mem_db.clone(), tx: self.tx.clone() })
    }

    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        self.mem_db.contains_key::<T>(key)
    }

    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        self.mem_db.get::<T>(key)
    }

    fn insert<T: Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        self.mem_db.insert::<T>(key, value)?;
        let ins = Box::new(KeyValueInsert::<T> { key: key.clone(), value: value.clone() });
        self.tx.send(DBMessage::Insert(ins)).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }

    fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()> {
        self.mem_db.remove::<T>(key)?;
        let rm = Box::new(KeyRemove::<T> { key: key.clone() });
        self.tx.send(DBMessage::Remove(rm)).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }

    fn clear_table<T: Table>(&self) -> eyre::Result<()> {
        self.mem_db.clear_table::<T>()?;
        let clr = Box::new(ClearTable::<T> { _casper: PhantomData });
        self.tx.send(DBMessage::Clear(clr)).map_err(|_| eyre::eyre!("DB thread gone, FATAL!"))?;
        Ok(())
    }

    fn is_empty<T: Table>(&self) -> bool {
        self.mem_db.is_empty::<T>()
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        self.mem_db.iter::<T>()
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        self.mem_db.skip_to::<T>(key)
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        self.mem_db.reverse_iter::<T>()
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        self.mem_db.record_prior_to::<T>(key)
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        self.mem_db.last_record::<T>()
    }
}

trait InsertTrait<DB: Database>: Send + 'static {
    fn insert(&self, db: &DB) -> eyre::Result<()>;
    fn insert_txn(&self, txn: &mut DB::TXMut<'_>) -> eyre::Result<()>;
}

trait RemoveTrait<DB: Database>: Send + 'static {
    fn remove(&self, db: &DB) -> eyre::Result<()>;
    fn remove_txn(&self, txn: &mut DB::TXMut<'_>) -> eyre::Result<()>;
}

trait ClearTrait<DB: Database>: Send + 'static {
    fn clear_table(&self, db: &DB) -> eyre::Result<()>;
    fn clear_table_txn(&self, txn: &mut DB::TXMut<'_>) -> eyre::Result<()>;
}

struct KeyValueInsert<T: Table> {
    key: T::Key,
    value: T::Value,
}

struct KeyRemove<T: Table> {
    key: T::Key,
}

struct ClearTable<T: Table> {
    _casper: PhantomData<T>,
}

impl<T: Table, DB: Database> InsertTrait<DB> for KeyValueInsert<T> {
    fn insert(&self, db: &DB) -> eyre::Result<()> {
        db.insert::<T>(&self.key, &self.value)
    }
    fn insert_txn(&self, txn: &mut DB::TXMut<'_>) -> eyre::Result<()> {
        txn.insert::<T>(&self.key, &self.value)
    }
}

impl<T: Table, DB: Database> RemoveTrait<DB> for KeyRemove<T> {
    fn remove(&self, db: &DB) -> eyre::Result<()> {
        db.remove::<T>(&self.key)
    }

    fn remove_txn(&self, txn: &mut <DB as Database>::TXMut<'_>) -> eyre::Result<()> {
        txn.remove::<T>(&self.key)
    }
}

impl<T: Table, DB: Database> ClearTrait<DB> for ClearTable<T> {
    fn clear_table(&self, db: &DB) -> eyre::Result<()> {
        db.clear_table::<T>()
    }

    fn clear_table_txn(&self, txn: &mut <DB as Database>::TXMut<'_>) -> eyre::Result<()> {
        txn.clear_table::<T>()
    }
}

enum DBMessage<DB: Database> {
    StartTxn,
    CommitTxn,
    Insert(Box<dyn InsertTrait<DB>>),
    Remove(Box<dyn RemoveTrait<DB>>),
    Clear(Box<dyn ClearTrait<DB>>),
    Shutdown,
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use tempfile::tempdir;

    use crate::{
        redb::ReDB,
        traits::{Database, DbTxMut, Table},
    };

    use super::LayeredDatabase;

    #[derive(Debug)]
    struct TestTable {}
    impl Table for TestTable {
        type Key = u64;
        type Value = String;

        const NAME: &'static str = "TestTable";
    }

    fn open_db(path: &Path) -> LayeredDatabase<ReDB> {
        let db = ReDB::open(path).expect("Cannot open database");
        db.open_table::<TestTable>().expect("failed to open table!");
        let db = LayeredDatabase::open(db);
        db.open_table::<TestTable>();
        db
    }

    #[test]
    fn test_layereddb_contains_key() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");
        assert!(db.contains_key::<TestTable>(&123456789).expect("Failed to call contains key"));
        assert!(!db.contains_key::<TestTable>(&000000000).expect("Failed to call contains key"));
    }

    #[test]
    fn test_layereddb_get() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");
        assert_eq!(
            Some("123456789".to_string()),
            db.get::<TestTable>(&123456789).expect("Failed to get")
        );
        assert_eq!(None, db.get::<TestTable>(&000000000).expect("Failed to get"));
    }

    #[test]
    fn test_layereddb_multi_get() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        db.insert::<TestTable>(&123, &"123".to_string()).expect("Failed to insert");
        db.insert::<TestTable>(&456, &"456".to_string()).expect("Failed to insert");

        let result = db.multi_get::<TestTable>([&123, &456, &789]).expect("Failed to multi get");

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Some("123".to_string()));
        assert_eq!(result[1], Some("456".to_string()));
        assert_eq!(result[2], None);
    }

    #[test]
    fn test_layereddb_skip() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

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
    fn test_layereddb_skip_to_previous_simple() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

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
    fn test_layereddb_iter_skip_to_previous_gap() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

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
    fn test_layereddb_remove() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");
        assert!(db.get::<TestTable>(&123456789).expect("Failed to get").is_some());

        db.remove::<TestTable>(&123456789).expect("Failed to remove");
        assert!(db.get::<TestTable>(&123456789).expect("Failed to get").is_none());
    }

    #[test]
    fn test_layereddb_iter() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
        db.insert::<TestTable>(&123456789, &"123456789".to_string()).expect("Failed to insert");

        let mut iter = db.iter::<TestTable>();
        assert_eq!(Some((123456789, "123456789".to_string())), iter.next());
        assert_eq!(None, iter.next());
    }

    #[test]
    fn test_layereddb_iter_reverse() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());
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
    fn test_layereddb_clear() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

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
    fn test_layereddb_is_empty() {
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

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
    fn test_layereddb_multi_insert() {
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

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
    fn test_layereddb_multi_remove() {
        // Init a DB
        let temp_dir = tempdir().expect("failed to create temp dir");
        let db = open_db(temp_dir.path());

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
