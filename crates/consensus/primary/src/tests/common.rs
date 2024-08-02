use std::{num::NonZeroUsize, sync::Arc};
// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use narwhal_storage::{CertificateStore, CertificateStoreCache, PayloadStore};
use narwhal_typed_store::test_db::TestDB;

pub fn create_db_stores() -> (CertificateStore, PayloadStore) {
    let (
        certificate_map,
        certificate_digest_by_round_map,
        certificate_digest_by_origin_map,
        payload_map,
    ) = (TestDB::open(), TestDB::open(), TestDB::open(), TestDB::open());
    (
        CertificateStore::new(
            Arc::new(certificate_map),
            Arc::new(certificate_digest_by_round_map),
            Arc::new(certificate_digest_by_origin_map),
            CertificateStoreCache::new(NonZeroUsize::new(100).unwrap(), None),
        ),
        PayloadStore::new(Arc::new(payload_map)),
    )
}
