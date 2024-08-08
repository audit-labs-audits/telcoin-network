// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    payload_store::PayloadStore, proposer_store::ProposerKey, vote_digest_store::VoteDigestStore,
    CertificateStore, CertificateStoreCache, CertificateStoreCacheMetrics, ConsensusStore,
    ProposerStore,
};
use narwhal_typed_store::{
    mem_db::MemDB,
    redb::dbmap::{open_redb, ReDBMap},
    /* reopen, */ reopen_redb,
    /*rocks::{
        default_db_options, metrics::SamplingInterval, open_cf_opts, MetricConf, ReadWriteOptions,
        RocksDBMap,
    },*/
    DBMap,
};
use std::{num::NonZeroUsize, sync::Arc /* , time::Duration */};
use tn_types::{
    AuthorityIdentifier, Batch, BatchDigest, Certificate, CertificateDigest, ConsensusCommit,
    Header, Round, SequenceNumber, VoteInfo, WorkerId,
};

// A type alias marking the "payload" tokens sent by workers to their primary as batch
// acknowledgements
pub type PayloadToken = u8;

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
    /// The datastore column family names.
    pub(crate) const LAST_PROPOSED_CF: &'static str = "last_proposed";
    pub(crate) const VOTES_CF: &'static str = "votes";
    pub(crate) const CERTIFICATES_CF: &'static str = "certificates";
    pub(crate) const CERTIFICATE_DIGEST_BY_ROUND_CF: &'static str = "certificate_digest_by_round";
    pub(crate) const CERTIFICATE_DIGEST_BY_ORIGIN_CF: &'static str = "certificate_digest_by_origin";
    pub(crate) const PAYLOAD_CF: &'static str = "payload";
    pub(crate) const BATCHES_CF: &'static str = "batches";
    pub(crate) const LAST_COMMITTED_CF: &'static str = "last_committed";
    pub(crate) const COMMITTED_SUB_DAG_INDEX_CF: &'static str = "committed_sub_dag";

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
    pub fn reopen<Path: AsRef<std::path::Path> + Send>(
        store_path: Path,
        certificate_store_cache_metrics: Option<CertificateStoreCacheMetrics>,
    ) -> Self {
        // In case the DB dir does not yet exist.
        let _ = std::fs::create_dir_all(&store_path);
        //NodeStorage::reopen_rocks(store_path, certificate_store_cache_metrics)
        NodeStorage::reopen_redb(store_path, certificate_store_cache_metrics)
    }

    /*
    /// Open or reopen all the storage of the node backed by rocks DB.
    fn reopen_rocks<Path: AsRef<std::path::Path> + Send>(
        store_path: Path,
        certificate_store_cache_metrics: Option<CertificateStoreCacheMetrics>,
    ) -> Self {
        let db_options = default_db_options().optimize_db_for_write_throughput(2);
        let mut metrics_conf = MetricConf::with_db_name("consensus_epoch");
        metrics_conf.read_sample_interval = SamplingInterval::new(Duration::from_secs(60), 0);
        let cf_options = db_options.options.clone();
        let column_family_options = vec![
            (Self::LAST_PROPOSED_CF, cf_options.clone()),
            (Self::VOTES_CF, cf_options.clone()),
            (
                Self::CERTIFICATES_CF,
                default_db_options()
                    .optimize_for_write_throughput()
                    .optimize_for_large_values_no_scan(1 << 10)
                    .options,
            ),
            (Self::CERTIFICATE_DIGEST_BY_ROUND_CF, cf_options.clone()),
            (Self::CERTIFICATE_DIGEST_BY_ORIGIN_CF, cf_options.clone()),
            (Self::PAYLOAD_CF, cf_options.clone()),
            (
                Self::BATCHES_CF,
                default_db_options()
                    .optimize_for_write_throughput()
                    .optimize_for_large_values_no_scan(1 << 10)
                    .options,
            ),
            (Self::LAST_COMMITTED_CF, cf_options.clone()),
            (Self::COMMITTED_SUB_DAG_INDEX_CF, cf_options),
        ];
        let rocksdb = open_cf_opts(
            store_path,
            Some(db_options.options),
            metrics_conf,
            &column_family_options,
        )
        .expect("Cannot open database");

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
        ) = reopen!(&rocksdb,
            Self::LAST_PROPOSED_CF;<ProposerKey, Header>,
            Self::VOTES_CF;<AuthorityIdentifier, VoteInfo>,
            Self::CERTIFICATES_CF;<CertificateDigest, Certificate>,
            Self::CERTIFICATE_DIGEST_BY_ROUND_CF;<(Round, AuthorityIdentifier), CertificateDigest>,
            Self::CERTIFICATE_DIGEST_BY_ORIGIN_CF;<(AuthorityIdentifier, Round), CertificateDigest>,
            Self::PAYLOAD_CF;<(BatchDigest, WorkerId), PayloadToken>,
            Self::BATCHES_CF;<BatchDigest, Batch>,
            Self::LAST_COMMITTED_CF;<AuthorityIdentifier, Round>,
            Self::COMMITTED_SUB_DAG_INDEX_CF;<SequenceNumber, ConsensusCommit>
        );

        let proposer_store = ProposerStore::new(Arc::new(last_proposed_map));
        let vote_digest_store = VoteDigestStore::new(Arc::new(votes_map));

        let certificate_store_cache = CertificateStoreCache::new(
            NonZeroUsize::new(Self::CERTIFICATE_STORE_CACHE_SIZE).unwrap(),
            certificate_store_cache_metrics,
        );
        let certificate_store = CertificateStore::<CertificateStoreCache>::new(
            Arc::new(certificate_map),
            Arc::new(certificate_digest_by_round_map),
            Arc::new(certificate_digest_by_origin_map),
            certificate_store_cache,
        );
        let payload_store = PayloadStore::new(Arc::new(payload_map));
        let batch_store = Arc::new(batch_map);
        let consensus_store = Arc::new(ConsensusStore::new(
            Arc::new(last_committed_map),
            Arc::new(committed_sub_dag_map),
        ));

        Self {
            proposer_store,
            vote_digest_store,
            certificate_store,
            payload_store,
            batch_store,
            consensus_store,
        }
    }
    */

    /// Open or reopen all the storage of the node backed by redb.
    fn reopen_redb<Path: AsRef<std::path::Path> + Send>(
        store_path: Path,
        certificate_store_cache_metrics: Option<CertificateStoreCacheMetrics>,
    ) -> Self {
        let redb = open_redb(store_path).expect("Cannot open database");

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
        ) = reopen_redb!(redb,
            Self::LAST_PROPOSED_CF;<ProposerKey, Header>,
            Self::VOTES_CF;<AuthorityIdentifier, VoteInfo>,
            Self::CERTIFICATES_CF;<CertificateDigest, Certificate>,
            Self::CERTIFICATE_DIGEST_BY_ROUND_CF;<(Round, AuthorityIdentifier), CertificateDigest>,
            Self::CERTIFICATE_DIGEST_BY_ORIGIN_CF;<(AuthorityIdentifier, Round), CertificateDigest>,
            Self::PAYLOAD_CF;<(BatchDigest, WorkerId), PayloadToken>,
            Self::BATCHES_CF;<BatchDigest, Batch>,
            Self::LAST_COMMITTED_CF;<AuthorityIdentifier, Round>,
            Self::COMMITTED_SUB_DAG_INDEX_CF;<SequenceNumber, ConsensusCommit>
        );

        let proposer_store = ProposerStore::new(Arc::new(last_proposed_map));
        let vote_digest_store = VoteDigestStore::new(Arc::new(votes_map));

        let certificate_store_cache = CertificateStoreCache::new(
            NonZeroUsize::new(Self::CERTIFICATE_STORE_CACHE_SIZE).unwrap(),
            certificate_store_cache_metrics,
        );
        let certificate_store = CertificateStore::<CertificateStoreCache>::new(
            Arc::new(certificate_map),
            Arc::new(certificate_digest_by_round_map),
            Arc::new(certificate_digest_by_origin_map),
            certificate_store_cache,
        );
        let payload_store = PayloadStore::new(Arc::new(payload_map));
        let batch_store = Arc::new(batch_map);
        let consensus_store = Arc::new(ConsensusStore::new(
            Arc::new(last_committed_map),
            Arc::new(committed_sub_dag_map),
        ));

        Self {
            proposer_store,
            vote_digest_store,
            certificate_store,
            payload_store,
            batch_store,
            consensus_store,
        }
    }

    /// Open or reopen all the storage of the node backed by a memory DB
    fn _reopen_mem<Path: AsRef<std::path::Path> + Send>(
        _store_path: Path,
        certificate_store_cache_metrics: Option<CertificateStoreCacheMetrics>,
    ) -> Self {
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
            // This can be removed when DBMap supports removing tables.
            // _sub_dag_index_map,
            committed_sub_dag_map,
        ) = (
            MemDB::<ProposerKey, Header>::open(),
            MemDB::<AuthorityIdentifier, VoteInfo>::open(),
            MemDB::<CertificateDigest, Certificate>::open(),
            MemDB::<(Round, AuthorityIdentifier), CertificateDigest>::open(),
            MemDB::<(AuthorityIdentifier, Round), CertificateDigest>::open(),
            MemDB::<(BatchDigest, WorkerId), PayloadToken>::open(),
            MemDB::<BatchDigest, Batch>::open(),
            MemDB::<AuthorityIdentifier, Round>::open(),
            MemDB::<SequenceNumber, ConsensusCommit>::open(),
        );

        let proposer_store = ProposerStore::new(Arc::new(last_proposed_map));
        let vote_digest_store = VoteDigestStore::new(Arc::new(votes_map));

        let certificate_store_cache = CertificateStoreCache::new(
            NonZeroUsize::new(Self::CERTIFICATE_STORE_CACHE_SIZE).unwrap(),
            certificate_store_cache_metrics,
        );
        let certificate_store = CertificateStore::<CertificateStoreCache>::new(
            Arc::new(certificate_map),
            Arc::new(certificate_digest_by_round_map),
            Arc::new(certificate_digest_by_origin_map),
            certificate_store_cache,
        );
        let payload_store = PayloadStore::new(Arc::new(payload_map));
        let batch_store = Arc::new(batch_map);
        let consensus_store = Arc::new(ConsensusStore::new(
            Arc::new(last_committed_map),
            Arc::new(committed_sub_dag_map),
        ));

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
