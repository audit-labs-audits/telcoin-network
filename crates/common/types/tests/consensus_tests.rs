// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use tn_types::consensus::{
    AuthorityIdentifier, Certificate, CommittedSubDag, Header, HeaderV1Builder,
    ReputationScores,
};
use indexmap::IndexMap;
use lattice_test_utils::CommitteeFixture;
use std::collections::BTreeSet;

#[test]
fn test_zero_timestamp_in_sub_dag() {
    let fixture = CommitteeFixture::builder().build();
    let committee = fixture.committee();

    let header_builder = HeaderV1Builder::default();
    let header = header_builder
        .author(AuthorityIdentifier(1u16))
        .round(2)
        .epoch(0)
        .created_at(50)
        .payload(IndexMap::new())
        .parents(BTreeSet::new())
        .build()
        .unwrap();

    let certificate =
        Certificate::new_unsigned(&committee, Header::V1(header), Vec::new()).unwrap();

    // AND we initialise the sub dag via the "restore" way
    let sub_dag_round = CommittedSubDag::new(
        vec![certificate.clone()],
        certificate,
        1,
        ReputationScores::default(),
        None,
    );

    // TODO: ensure using ::new() is still accurate
    //
    // the previous unit test initialized struct, but
    // the commit_timestamp is a private field.
    // had to move this to integration test because
    // test-utils & tn-types depend on each other.
    //
    // let sub_dag_round = CommittedSubDag {
    //     certificates: vec![certificate.clone()],
    //     leader: certificate,
    //     sub_dag_index: 1,
    //     reputation_score: ReputationScores::default(),
    //     commit_timestamp: 0,
    // };

    // AND commit timestamp is the leader's timestamp
    assert_eq!(sub_dag_round.commit_timestamp(), 50);
}

#[test]
fn test_monotonically_incremented_commit_timestamps() {
    // Create a certificate (leader) of round 2 with a high timestamp
    let newer_timestamp = 100;
    let older_timestamp = 50;

    let fixture = CommitteeFixture::builder().build();
    let committee = fixture.committee();

    let header_builder = HeaderV1Builder::default();
    let header = header_builder
        .author(AuthorityIdentifier(1u16))
        .round(2)
        .epoch(0)
        .created_at(newer_timestamp)
        .payload(IndexMap::new())
        .parents(BTreeSet::new())
        .build()
        .unwrap();

    let certificate =
        Certificate::new_unsigned(&committee, Header::V1(header), Vec::new()).unwrap();

    // AND
    let sub_dag_round_2 = CommittedSubDag::new(
        vec![certificate.clone()],
        certificate,
        1,
        ReputationScores::default(),
        None,
    );

    // AND commit timestamp is the leader's timestamp
    assert_eq!(sub_dag_round_2.commit_timestamp(), newer_timestamp);

    // Now create the leader of round 4 with the older timestamp
    let header_builder = HeaderV1Builder::default();
    let header = header_builder
        .author(AuthorityIdentifier(1u16))
        .round(4)
        .epoch(0)
        .created_at(older_timestamp)
        .payload(IndexMap::new())
        .parents(BTreeSet::new())
        .build()
        .unwrap();

    let certificate =
        Certificate::new_unsigned(&committee, Header::V1(header), Vec::new()).unwrap();

    // WHEN create the sub dag based on the "previously committed" sub dag.
    let sub_dag_round_4 = CommittedSubDag::new(
        vec![certificate.clone()],
        certificate,
        2,
        ReputationScores::default(),
        Some(&sub_dag_round_2),
    );

    // THEN the latest sub dag should have the highest committed timestamp - basically the
    // same as the previous commit round
    assert_eq!(sub_dag_round_4.commit_timestamp(), sub_dag_round_2.commit_timestamp());
}
