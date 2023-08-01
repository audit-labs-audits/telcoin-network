// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use std::{collections::{HashMap, BTreeMap, BTreeSet}, sync::Arc, cmp::max};
use fastcrypto::hash::Hash;
use tn_types::consensus::{Round, CommittedSubDag, AuthorityIdentifier, ConsensusCommit, Certificate, CertificateAPI, Timestamp, SequenceNumber, CertificateDigest, HeaderAPI};
use tracing::{debug, info, instrument};
use lattice_storage::CertificateStore;
use crate::{ConsensusRound, dag::Dag, metrics::ConsensusMetrics, ConsensusError};

/// The state that needs to be persisted for crash-recovery.
pub struct ConsensusState {
    /// The information about the last committed round and corresponding GC round.
    pub last_round: ConsensusRound,
    /// The chosen gc_depth
    pub gc_depth: Round,
    /// Keeps the last committed round for each authority. This map is used to clean up the dag and
    /// ensure we don't commit twice the same certificate.
    pub last_committed: HashMap<AuthorityIdentifier, Round>,
    /// The last committed sub dag. If value is None, it means that we haven't committed any sub
    /// dag yet.
    pub last_committed_sub_dag: Option<CommittedSubDag>,
    /// Keeps the latest committed certificate (and its parents) for every authority in memory. Anything
    /// older must be regularly cleaned up through the function `update`.
    pub dag: Dag,
    /// Metrics handler
    pub metrics: Arc<ConsensusMetrics>,
}

impl ConsensusState {
    /// Create a new instance of `Self`.
    pub fn new(metrics: Arc<ConsensusMetrics>, gc_depth: Round) -> Self {
        Self {
            last_round: ConsensusRound::default(),
            gc_depth,
            last_committed: Default::default(),
            dag: Default::default(),
            last_committed_sub_dag: None,
            metrics,
        }
    }

    /// Create a new instance of `Self` based on persisted data in the `CertificateStore`.
    pub fn new_from_store(
        metrics: Arc<ConsensusMetrics>,
        last_committed_round: Round,
        gc_depth: Round,
        recovered_last_committed: HashMap<AuthorityIdentifier, Round>,
        latest_sub_dag: Option<ConsensusCommit>,
        cert_store: CertificateStore,
    ) -> Self {
        let last_round = ConsensusRound::new_with_gc_depth(last_committed_round, gc_depth);

        let dag = Self::construct_dag_from_cert_store(
            &cert_store,
            &recovered_last_committed,
            last_round.gc_round,
        )
        .expect("error when recovering DAG from store");
        metrics.recovered_consensus_state.inc();

        let last_committed_sub_dag = if let Some(latest_sub_dag) = latest_sub_dag.as_ref() {
            let certificates = latest_sub_dag
                .certificates()
                .iter()
                .map(|s| {
                    cert_store.read(*s).unwrap().expect("Certificate should be found in database")
                })
                .collect();

            let leader = cert_store
                .read(latest_sub_dag.leader())
                .unwrap()
                .expect("Certificate should be found in database");

            Some(CommittedSubDag::from_commit(latest_sub_dag.clone(), certificates, leader))
        } else {
            None
        };

        Self {
            gc_depth,
            last_round,
            last_committed: recovered_last_committed,
            last_committed_sub_dag,
            dag,
            metrics,
        }
    }

    /// Recreate the Dag used for consensus using the `CertificateStore`.
    #[instrument(level = "info", skip_all)]
    pub fn construct_dag_from_cert_store(
        cert_store: &CertificateStore,
        last_committed: &HashMap<AuthorityIdentifier, Round>,
        gc_round: Round,
    ) -> Result<Dag, ConsensusError> {
        let mut dag: Dag = BTreeMap::new();

        info!("Recreating dag from last GC round: {}", gc_round);

        // get all certificates at rounds > gc_round
        let certificates = cert_store.after_round(gc_round + 1).unwrap();

        let mut num_certs = 0;
        for cert in &certificates {
            if Self::try_insert_in_dag(&mut dag, last_committed, gc_round, cert)? {
                info!("Inserted certificate: {:?}", cert);
                num_certs += 1;
            }
        }
        info!("Dag is restored and contains {} certs for {} rounds", num_certs, dag.len());

        Ok(dag)
    }

    /// Returns true if certificate is inserted in the dag.
    pub fn try_insert(&mut self, certificate: &Certificate) -> Result<bool, ConsensusError> {
        Self::try_insert_in_dag(
            &mut self.dag,
            &self.last_committed,
            self.last_round.gc_round,
            certificate,
        )
    }

    /// Returns true if certificate is inserted in the dag.
    fn try_insert_in_dag(
        dag: &mut Dag,
        last_committed: &HashMap<AuthorityIdentifier, Round>,
        gc_round: Round,
        certificate: &Certificate,
    ) -> Result<bool, ConsensusError> {
        if certificate.round() <= gc_round {
            debug!(
                "Ignoring certificate {:?} as it is at or before gc round {}",
                certificate, gc_round
            );
            return Ok(false)
        }
        Self::check_parents(certificate, dag, gc_round);

        // Always insert the certificate even if it is below last committed round of its origin,
        // to allow verifying parent existence.
        if let Some((_, existing_certificate)) = dag
            .entry(certificate.round())
            .or_default()
            .insert(certificate.origin(), (certificate.digest(), certificate.clone()))
        {
            // we want to error only if we try to insert a different certificate in the dag
            if existing_certificate.digest() != certificate.digest() {
                return Err(ConsensusError::CertificateEquivocation(
                    certificate.clone(),
                    existing_certificate,
                ))
            }
        }

        Ok(certificate.round() >
            last_committed.get(&certificate.origin()).cloned().unwrap_or_default())
    }

    /// Update and clean up internal state after committing a certificate.
    pub fn update(&mut self, certificate: &Certificate) {
        self.last_committed
            .entry(certificate.origin())
            .and_modify(|r| *r = max(*r, certificate.round()))
            .or_insert_with(|| certificate.round());
        self.last_round = self.last_round.update(certificate.round(), self.gc_depth);

        self.metrics
            .last_committed_round
            .with_label_values(&[])
            .set(self.last_round.committed_round as i64);
        let elapsed = certificate.created_at().elapsed().as_secs_f64();
        self.metrics
            .certificate_commit_latency
            .observe(certificate.created_at().elapsed().as_secs_f64());

        // NOTE: This log entry is used to compute performance.
        tracing::debug!(
            "Certificate {:?} took {} seconds to be committed at round {}",
            certificate.digest(),
            elapsed,
            certificate.round(),
        );

        // Purge all certificates past the gc depth.
        self.dag.retain(|r, _| *r > self.last_round.gc_round);
    }

    /// Checks that the provided certificate's parents exist, otherwise crashes.
    fn check_parents(certificate: &Certificate, dag: &Dag, gc_round: Round) {
        let round = certificate.round();
        // Skip checking parents if they are GC'ed.
        // Also not checking genesis parents for simplicity.
        if round <= gc_round + 1 {
            return
        }
        if let Some(round_table) = dag.get(&(round - 1)) {
            let store_parents: BTreeSet<&CertificateDigest> =
                round_table.iter().map(|(_, (digest, _))| digest).collect();
            for parent_digest in certificate.header().parents() {
                if !store_parents.contains(parent_digest) {
                    panic!("Parent digest {parent_digest:?} not found in DAG for {certificate:?}!");
                }
            }
        } else {
            panic!("Parent round not found in DAG for {certificate:?}!");
        }
    }

    /// Provides the next index to be used for the next produced sub dag
    pub fn next_sub_dag_index(&self) -> SequenceNumber {
        self.last_committed_sub_dag.as_ref().map(|s| s.sub_dag_index).unwrap_or_default() + 1
    }
}