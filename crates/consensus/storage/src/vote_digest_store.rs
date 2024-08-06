// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use narwhal_typed_store::{mem_db::MemDB, Map};
use telcoin_macros::fail_point;
use tn_types::{AuthorityIdentifier, Vote, VoteAPI, VoteInfo};

/// The storage for the last votes digests per authority
#[derive(Clone)]
pub struct VoteDigestStore {
    store: Arc<dyn Map<AuthorityIdentifier, VoteInfo>>,
}

impl VoteDigestStore {
    pub fn new(store: Arc<dyn Map<AuthorityIdentifier, VoteInfo>>) -> VoteDigestStore {
        Self { store }
    }

    pub fn new_for_tests() -> VoteDigestStore {
        VoteDigestStore::new(Arc::new(MemDB::open()))
    }

    /// Insert the vote's basic details into the database for the corresponding
    /// header author key.
    #[allow(clippy::let_and_return)]
    pub fn write(&self, vote: &Vote) -> eyre::Result<()> {
        fail_point!("narwhal-store-before-write");

        let result = self.store.insert(&vote.origin(), &vote.into());

        fail_point!("narwhal-store-after-write");
        result
    }

    /// Read the vote info based on the provided corresponding header author key
    pub fn read(&self, header_author: &AuthorityIdentifier) -> eyre::Result<Option<VoteInfo>> {
        self.store.get(header_author)
    }
}
