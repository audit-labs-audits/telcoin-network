// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use narwhal_storage::{CertificateStore, CertificateStoreCache, ConsensusStore};
use narwhal_typed_store::mem_db::MemDB;
use std::{num::NonZeroUsize, sync::Arc};

pub const NUM_SUB_DAGS_PER_SCHEDULE: u64 = 100;

pub fn make_consensus_store(_store_path: &std::path::Path) -> Arc<ConsensusStore> {
    Arc::new(ConsensusStore::new(Arc::new(MemDB::open()), Arc::new(MemDB::open())))
}

pub fn make_certificate_store(_store_path: &std::path::Path) -> CertificateStore {
    CertificateStore::new(
        Arc::new(MemDB::open()),
        Arc::new(MemDB::open()),
        Arc::new(MemDB::open()),
        CertificateStoreCache::new(NonZeroUsize::new(100).unwrap(), None),
    )
}
