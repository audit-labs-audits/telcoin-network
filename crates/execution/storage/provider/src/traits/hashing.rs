use auto_impl::auto_impl;
use execution_db::models::BlockNumberAddress;
use execution_interfaces::Result;
use std::ops::{Range, RangeInclusive};
use tn_types::execution::{Account, Address, BlockNumber, StorageEntry, H256};

/// Hashing Writer
#[auto_impl(&, Arc, Box)]
pub trait HashingWriter: Send + Sync {
    /// Unwind and clear account hashing
    fn unwind_account_hashing(&self, range: RangeInclusive<BlockNumber>) -> Result<()>;

    /// Inserts all accounts into [execution_db::tables::AccountHistory] table.
    fn insert_account_for_hashing(
        &self,
        accounts: impl IntoIterator<Item = (Address, Option<Account>)>,
    ) -> Result<()>;

    /// Unwind and clear storage hashing
    fn unwind_storage_hashing(&self, range: Range<BlockNumberAddress>) -> Result<()>;

    /// iterate over storages and insert them to hashing table
    fn insert_storage_for_hashing(
        &self,
        storages: impl IntoIterator<Item = (Address, impl IntoIterator<Item = StorageEntry>)>,
    ) -> Result<()>;

    /// Calculate the hashes of all changed accounts and storages, and finally calculate the state
    /// root.
    ///
    /// The hashes are calculated from `fork_block_number + 1` to `current_block_number`.
    ///
    /// The resulting state root is compared with `expected_state_root`.
    fn insert_hashes(
        &self,
        range: RangeInclusive<BlockNumber>,
        end_block_hash: H256,
        expected_state_root: H256,
    ) -> Result<()>;
}
