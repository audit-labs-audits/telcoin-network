use super::{HashedAccountCursor, HashedCursorFactory, HashedStorageCursor};
use execution_db::{
    cursor::{DbCursorRO, DbDupCursorRO},
    tables,
    transaction::{DbTx, DbTxGAT},
};
use execution_primitives::{Account, StorageEntry, H256};

impl<'a, 'tx, TX: DbTx<'tx>> HashedCursorFactory<'a> for TX {
    type AccountCursor = <TX as DbTxGAT<'a>>::Cursor<tables::HashedAccount> where Self: 'a;
    type StorageCursor = <TX as DbTxGAT<'a>>::DupCursor<tables::HashedStorage> where Self: 'a;

    fn hashed_account_cursor(&'a self) -> Result<Self::AccountCursor, execution_db::DatabaseError> {
        self.cursor_read::<tables::HashedAccount>()
    }

    fn hashed_storage_cursor(&'a self) -> Result<Self::StorageCursor, execution_db::DatabaseError> {
        self.cursor_dup_read::<tables::HashedStorage>()
    }
}

impl<'tx, C> HashedAccountCursor for C
where
    C: DbCursorRO<'tx, tables::HashedAccount>,
{
    fn seek(&mut self, key: H256) -> Result<Option<(H256, Account)>, execution_db::DatabaseError> {
        self.seek(key)
    }

    fn next(&mut self) -> Result<Option<(H256, Account)>, execution_db::DatabaseError> {
        self.next()
    }
}

impl<'tx, C> HashedStorageCursor for C
where
    C: DbCursorRO<'tx, tables::HashedStorage> + DbDupCursorRO<'tx, tables::HashedStorage>,
{
    fn is_storage_empty(&mut self, key: H256) -> Result<bool, execution_db::DatabaseError> {
        Ok(self.seek_exact(key)?.is_none())
    }

    fn seek(
        &mut self,
        key: H256,
        subkey: H256,
    ) -> Result<Option<StorageEntry>, execution_db::DatabaseError> {
        self.seek_by_key_subkey(key, subkey)
    }

    fn next(&mut self) -> Result<Option<StorageEntry>, execution_db::DatabaseError> {
        self.next_dup_val()
    }
}
