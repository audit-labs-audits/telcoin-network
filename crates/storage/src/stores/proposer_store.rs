//! NOTE: tests for this module are in test-utils storage_tests.rs to avoid circular dependancies.

use crate::{tables::LastProposed, ProposerKey, StoreResult};
use tn_types::{Database, Header};
use tn_utils::fail_point;

pub const LAST_PROPOSAL_KEY: ProposerKey = 0;

pub trait ProposerStore {
    /// Inserts a proposed header into the store
    fn write_last_proposed(&self, header: &Header) -> StoreResult<()>;

    /// Get the last header
    fn get_last_proposed(&self) -> StoreResult<Option<Header>>;
}

impl<DB: Database> ProposerStore for DB {
    #[allow(clippy::let_and_return)]
    fn write_last_proposed(&self, header: &Header) -> StoreResult<()> {
        fail_point!("proposer-store-before-write");

        let result = self.insert::<LastProposed>(&LAST_PROPOSAL_KEY, header);

        fail_point!("proposer-store-after-write");
        result
    }

    fn get_last_proposed(&self) -> StoreResult<Option<Header>> {
        self.get::<LastProposed>(&LAST_PROPOSAL_KEY)
    }
}

// NOTE: tests for this module are in test-utils storage_tests.rs to avoid circular dependancies.
