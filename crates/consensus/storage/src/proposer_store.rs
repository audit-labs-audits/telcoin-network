// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use crate::StoreResult;
use narwhal_typed_store::Map;
use telcoin_macros::fail_point;
use tn_types::Header;

pub type ProposerKey = u32;

pub const LAST_PROPOSAL_KEY: ProposerKey = 0;

/// The storage for the proposer
#[derive(Clone)]
pub struct ProposerStore {
    /// Holds the Last Header that was proposed by the Proposer.
    last_proposed: Arc<dyn Map<ProposerKey, Header>>,
}

impl ProposerStore {
    pub fn new(last_proposed: Arc<dyn Map<ProposerKey, Header>>) -> ProposerStore {
        Self { last_proposed }
    }

    /// Inserts a proposed header into the store
    #[allow(clippy::let_and_return)]
    pub fn write_last_proposed(&self, header: &Header) -> StoreResult<()> {
        fail_point!("narwhal-store-before-write");

        let result = self.last_proposed.insert(&LAST_PROPOSAL_KEY, header);

        fail_point!("narwhal-store-after-write");
        result
    }

    /// Get the last header
    pub fn get_last_proposed(&self) -> StoreResult<Option<Header>> {
        self.last_proposed.get(&LAST_PROPOSAL_KEY)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::LAST_PROPOSAL_KEY;
    use narwhal_typed_store::test_db::TestDB;
    use tn_types::{
        test_utils::{fixture_batch_with_transactions, CommitteeFixture},
        CertificateDigest, Header, HeaderV1Builder, Round,
    };

    use super::ProposerStore;

    pub fn new_proposer_for_tests() -> ProposerStore {
        let db = TestDB::open();
        ProposerStore::new(Arc::new(db))
    }

    pub fn create_header_for_round(round: Round) -> Header {
        let builder = HeaderV1Builder::default();
        let fixture = CommitteeFixture::builder().randomize_ports(true).build();
        let primary = fixture.authorities().next().unwrap();
        let id = primary.id();
        let header = builder
            .author(id)
            .round(round)
            .epoch(fixture.committee().epoch())
            .parents([CertificateDigest::default()].iter().cloned().collect())
            .with_payload_batch(fixture_batch_with_transactions(10), 0, 0)
            .build()
            .unwrap();
        Header::V1(header)
    }

    #[tokio::test]
    async fn test_writes() {
        let store = new_proposer_for_tests();
        let header_1 = create_header_for_round(1);

        let out = store.write_last_proposed(&header_1);
        assert!(out.is_ok());

        let result = store.last_proposed.get(&LAST_PROPOSAL_KEY).unwrap();
        assert_eq!(result.unwrap(), header_1);

        let header_2 = create_header_for_round(2);
        let out = store.write_last_proposed(&header_2);
        assert!(out.is_ok());

        let should_exist = store.last_proposed.get(&LAST_PROPOSAL_KEY).unwrap();
        assert_eq!(should_exist.unwrap(), header_2);
    }

    #[tokio::test]
    async fn test_reads() {
        let store = new_proposer_for_tests();

        let should_not_exist = store.get_last_proposed().unwrap();
        assert_eq!(should_not_exist, None);

        let header_1 = create_header_for_round(1);
        let out = store.write_last_proposed(&header_1);
        assert!(out.is_ok());

        let should_exist = store.get_last_proposed().unwrap();
        assert_eq!(should_exist.unwrap(), header_1);
    }
}
