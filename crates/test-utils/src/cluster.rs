//! Cluster fixture to represent a local network.

use crate::{authority::AuthorityDetails, default_test_execution_node, CommitteeFixture};
use itertools::Itertools;
use std::collections::HashMap;
use tn_storage::traits::Database;
use tn_types::Committee;
use tracing::info;

/// Test fixture that holds all information needed to run a local network.
pub struct Cluster<DB> {
    fixture: CommitteeFixture<DB>,
    authorities: HashMap<usize, AuthorityDetails<DB>>,
    pub committee: Committee,
}

impl<DB> Cluster<DB>
where
    DB: Database,
{
    /// Initialises a new cluster by the provided parameters. The cluster will
    /// create all the authorities (primaries & workers) that are defined under
    /// the committee structure, but none of them will be started.
    ///
    /// Fields passed in via Parameters will be used, expect specified ports which have to be
    /// different for each instance. If None, the default Parameters will be used.
    pub fn new<F>(new_db: F) -> Self
    where
        F: Fn() -> DB,
    {
        let fixture = CommitteeFixture::builder(new_db).randomize_ports(true).build();
        let committee = fixture.committee();

        info!("###### Creating new cluster ######");
        info!("Validator keys:");
        let mut nodes = HashMap::new();

        for (id, authority_fixture) in fixture.authorities().enumerate() {
            info!("Key {id} -> {}", authority_fixture.primary_public_key());

            let authority_id = authority_fixture.id();
            let authority_execution_address = authority_fixture.execution_address();

            let engine = default_test_execution_node(
                None, // default: adiri chain
                Some(authority_execution_address),
            )
            .expect("default test execution node");

            let consensus_config = authority_fixture.consensus_config().clone();

            let authority =
                AuthorityDetails::new(id, authority_id, consensus_config.clone(), engine);
            nodes.insert(id, authority);
        }

        Self { fixture, authorities: nodes, committee }
    }

    /// Returns all the authorities (running or not).
    pub async fn authorities(&self) -> Vec<AuthorityDetails<DB>> {
        let mut result = Vec::new();

        for authority in self.authorities.values() {
            result.push(authority.clone());
        }

        result
    }

    /// Returns the authority identified by the provided id. Will panic if the
    /// authority with the id is not found. The returned authority can be freely
    /// cloned and managed without having the need to fetch again.
    pub fn authority(&self, id: usize) -> AuthorityDetails<DB> {
        self.authorities
            .get(&id)
            .unwrap_or_else(|| panic!("Authority with id {} not found", id))
            .clone()
    }

    /// This method asserts the progress of the cluster.
    /// `expected_nodes`: Nodes expected to have made progress. Any number different than that
    /// will make the assertion fail.
    /// `commit_threshold`: The acceptable threshold between the minimum and maximum reported
    /// commit value from the nodes.
    pub async fn assert_progress(
        &self,
        expected_nodes: u64,
        commit_threshold: u64,
    ) -> HashMap<usize, u64> {
        let r = self.authorities_latest_commit_round().await;
        let rounds: HashMap<usize, u64> =
            r.into_iter().map(|(key, value)| (key, value as u64)).collect();

        assert_eq!(
            rounds.len(),
            expected_nodes as usize,
            "Expected to have received commit metrics from {expected_nodes} nodes"
        );
        assert!(rounds.values().all(|v| v > &1), "All nodes are available so all should have made progress and committed at least after the first round");

        if expected_nodes == 0 {
            return HashMap::new();
        }

        let (min, max) = rounds.values().minmax().into_option().unwrap();
        assert!(max - min <= commit_threshold, "Nodes shouldn't be that behind");

        rounds
    }

    async fn authorities_latest_commit_round(&self) -> HashMap<usize, f64> {
        let mut authorities_latest_commit = HashMap::new();

        for authority in self.authorities().await {
            let primary = authority.primary().await;
            let value =
                primary.consensus_metrics().await.last_committed_round.with_label_values(&[]).get()
                    as f64;

            authorities_latest_commit.insert(primary.id, value);

            info!("[Node {}] Metric tn_primary_last_committed_round -> {value}", primary.id);
        }

        authorities_latest_commit
    }

    pub fn fixture(&self) -> &CommitteeFixture<DB> {
        &self.fixture
    }
}
