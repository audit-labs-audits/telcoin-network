// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    payload_store::PayloadStore, vote_digest_store::VoteDigestStore, CertificateStore,
    CertificateStoreCache, CertificateStoreCacheMetrics, ConsensusStore, ProposerStore,
};
use narwhal_typed_store::{open_node_dbs, DBMap};
use std::{num::NonZeroUsize, sync::Arc /* , time::Duration */};
use tn_types::{Batch, BatchDigest};

// A type alias marking the "payload" tokens sent by workers to their primary as batch
// acknowledgements
pub use narwhal_typed_store::PayloadToken;

/// All the data stores of the node.
#[derive(Clone)]
pub struct NodeStorage {
    pub proposer_store: ProposerStore,
    pub vote_digest_store: VoteDigestStore,
    pub certificate_store: CertificateStore<CertificateStoreCache>,
    pub payload_store: PayloadStore,
    pub batch_store: Arc<dyn DBMap<BatchDigest, Batch>>,
    pub consensus_store: Arc<ConsensusStore>,
}

impl NodeStorage {
    /// Cache size for certificate store.
    ///
    /// Reasoning: 100 nodes * 60 rounds (assuming 1 round/sec)
    /// The cache should hold data for the last ~1 minute
    /// which should be more than enough for advancing the protocol
    /// and also help other nodes advance.
    ///
    /// TODO: take into account committee size instead of having fixed 100.
    pub(crate) const CERTIFICATE_STORE_CACHE_SIZE: usize = 100 * 60;

    /// Open or reopen all the storage of the node.
    pub fn reopen<P: AsRef<std::path::Path> + Send + 'static>(
        store_path: P,
        certificate_store_cache_metrics: Option<CertificateStoreCacheMetrics>,
    ) -> Self {
        // In case the DB dir does not yet exist.
        let _ = std::fs::create_dir_all(&store_path);
        let (
            last_proposed_map,
            votes_map,
            certificate_map,
            certificate_digest_by_round_map,
            certificate_digest_by_origin_map,
            payload_map,
            batch_map,
            last_committed_map,
            // table `sub_dag` is deprecated in favor of `committed_sub_dag`.
            // This can be removed when RocksDBMap supports removing tables.
            // _sub_dag_index_map,
            committed_sub_dag_map,
        ) = open_node_dbs(store_path);

        let proposer_store = ProposerStore::new(last_proposed_map);
        let vote_digest_store = VoteDigestStore::new(votes_map);

        let certificate_store_cache = CertificateStoreCache::new(
            NonZeroUsize::new(Self::CERTIFICATE_STORE_CACHE_SIZE).unwrap(),
            certificate_store_cache_metrics,
        );
        let certificate_store = CertificateStore::<CertificateStoreCache>::new(
            certificate_map,
            certificate_digest_by_round_map,
            certificate_digest_by_origin_map,
            certificate_store_cache,
        );
        let payload_store = PayloadStore::new(payload_map);
        let batch_store = batch_map;
        let consensus_store =
            Arc::new(ConsensusStore::new(last_committed_map, committed_sub_dag_map));

        Self {
            proposer_store,
            vote_digest_store,
            certificate_store,
            payload_store,
            batch_store,
            consensus_store,
        }
    }
}
