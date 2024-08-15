// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![warn(future_incompatible, nonstandard_style, rust_2018_idioms, rust_2021_compatibility)]

pub mod traits;

#[cfg(feature = "rocksdb")]
use std::time::Duration;

use redb::dbmap::{open_redatabase, ReDB};
#[cfg(feature = "rocksdb")]
use rocks::{default_db_options, metrics::SamplingInterval, open_cf_opts, MetricConf};
use tables::{
    Batches, CertificateDigestByOrigin, CertificateDigestByRound, Certificates, CommittedSubDag,
    LastCommitted, LastProposed, Payload, Votes,
};
pub mod redb;
#[cfg(feature = "rocksdb")]
pub mod rocks;

pub use tn_types::error::StoreError;

pub type ProposerKey = u32;
// A type alias marking the "payload" tokens sent by workers to their primary as batch
// acknowledgements
pub type PayloadToken = u8;

/// The datastore column family names.
const LAST_PROPOSED_CF: &str = "last_proposed";
const VOTES_CF: &str = "votes";
const CERTIFICATES_CF: &str = "certificates";
const CERTIFICATE_DIGEST_BY_ROUND_CF: &str = "certificate_digest_by_round";
const CERTIFICATE_DIGEST_BY_ORIGIN_CF: &str = "certificate_digest_by_origin";
const PAYLOAD_CF: &str = "payload";
const BATCHES_CF: &str = "batches";
const LAST_COMMITTED_CF: &str = "last_committed";
const COMMITTED_SUB_DAG_INDEX_CF: &str = "committed_sub_dag";

macro_rules! tables {
    ( $($table:ident;$name:expr;<$K:ty, $V:ty>),*) => {
            $(
                #[derive(Debug)]
                pub struct $table {}
                impl $crate::traits::Table for $table {
                    type Key = $K;
                    type Value = $V;

                    const NAME: &'static str = $name;
                }
            )*
    };
}

pub mod tables {
    use super::{PayloadToken, ProposerKey};
    use tn_types::{
        AuthorityIdentifier, Batch, BatchDigest, Certificate, CertificateDigest, ConsensusCommit,
        Header, Round, SequenceNumber, VoteInfo, WorkerId,
    };

    tables!(
        LastProposed;crate::LAST_PROPOSED_CF;<ProposerKey, Header>,
        Votes;crate::VOTES_CF;<AuthorityIdentifier, VoteInfo>,
        Certificates;crate::CERTIFICATES_CF;<CertificateDigest, Certificate>,
        CertificateDigestByRound;crate::CERTIFICATE_DIGEST_BY_ROUND_CF;<(Round, AuthorityIdentifier), CertificateDigest>,
        CertificateDigestByOrigin;crate::CERTIFICATE_DIGEST_BY_ORIGIN_CF;<(AuthorityIdentifier, Round), CertificateDigest>,
        Payload;crate::PAYLOAD_CF;<(BatchDigest, WorkerId), PayloadToken>,
        Batches;crate::BATCHES_CF;<BatchDigest, Batch>,
        LastCommitted;crate::LAST_COMMITTED_CF;<AuthorityIdentifier, Round>,
        CommittedSubDag;crate::COMMITTED_SUB_DAG_INDEX_CF;<SequenceNumber, ConsensusCommit>
    );
}

//#[cfg(feature = "rocksdb")]
//pub type DatabaseType = ReDB;
#[cfg(feature = "redb")]
pub type DatabaseType = ReDB;

/// Open the configured DB with the required tables.
pub fn open_db<Path: AsRef<std::path::Path> + Send>(store_path: Path) -> DatabaseType {
    let db = open_redatabase(store_path).expect("Cannot open database");
    db.open_table::<LastProposed>().expect("failed to open table!");
    db.open_table::<Votes>().expect("failed to open table!");
    db.open_table::<Certificates>().expect("failed to open table!");
    db.open_table::<CertificateDigestByRound>().expect("failed to open table!");
    db.open_table::<CertificateDigestByOrigin>().expect("failed to open table!");
    db.open_table::<Payload>().expect("failed to open table!");
    db.open_table::<Batches>().expect("failed to open table!");
    db.open_table::<LastCommitted>().expect("failed to open table!");
    db.open_table::<CommittedSubDag>().expect("failed to open table!");
    db
}
/*
/// Open or reopen all the storage of the node backed by rocks DB.
#[allow(unreachable_code)]
fn reopen_rocks<P: AsRef<std::path::Path> + Send>(_store_path: P) -> NodeDBs {
    #[cfg(feature = "rocksdb")]
    return {
        let db_options = default_db_options().optimize_db_for_write_throughput(2);
        let mut metrics_conf = MetricConf::with_db_name("consensus_epoch");
        metrics_conf.read_sample_interval = SamplingInterval::new(Duration::from_secs(60), 0);
        let cf_options = db_options.options.clone();
        let column_family_options = vec![
            (LAST_PROPOSED_CF, cf_options.clone()),
            (VOTES_CF, cf_options.clone()),
            (
                CERTIFICATES_CF,
                default_db_options()
                    .optimize_for_write_throughput()
                    .optimize_for_large_values_no_scan(1 << 10)
                    .options,
            ),
            (CERTIFICATE_DIGEST_BY_ROUND_CF, cf_options.clone()),
            (CERTIFICATE_DIGEST_BY_ORIGIN_CF, cf_options.clone()),
            (PAYLOAD_CF, cf_options.clone()),
            (
                BATCHES_CF,
                default_db_options()
                    .optimize_for_write_throughput()
                    .optimize_for_large_values_no_scan(1 << 10)
                    .options,
            ),
            (LAST_COMMITTED_CF, cf_options.clone()),
            (COMMITTED_SUB_DAG_INDEX_CF, cf_options),
        ];
        let rocksdb = open_cf_opts(
            _store_path,
            Some(db_options.options),
            metrics_conf,
            &column_family_options,
        )
        .expect("Cannot open database");

        reopen!(&rocksdb,
            LAST_PROPOSED_CF;<ProposerKey, Header>,
            VOTES_CF;<AuthorityIdentifier, VoteInfo>,
            CERTIFICATES_CF;<CertificateDigest, Certificate>,
            CERTIFICATE_DIGEST_BY_ROUND_CF;<(Round, AuthorityIdentifier), CertificateDigest>,
            CERTIFICATE_DIGEST_BY_ORIGIN_CF;<(AuthorityIdentifier, Round), CertificateDigest>,
            PAYLOAD_CF;<(BatchDigest, WorkerId), PayloadToken>,
            BATCHES_CF;<BatchDigest, Batch>,
            LAST_COMMITTED_CF;<AuthorityIdentifier, Round>,
            COMMITTED_SUB_DAG_INDEX_CF;<SequenceNumber, ConsensusCommit>
        )
    };
    panic!("Can't use rocks with the rocksdb feature!");
}
*/
