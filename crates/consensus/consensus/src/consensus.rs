// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::mutable_key_type)]

use crate::{
    bullshark::Bullshark, metrics::ConsensusMetrics, ConsensusError,
    ConsensusRound, ConsensusState,
};
use consensus_metrics::{metered_channel, spawn_logged_monitored_task};
use lattice_storage::{CertificateStore, ConsensusStore};
use std::{
    cmp::Ordering,
    sync::Arc,
};
use tn_types::consensus::{
    Committee, Round,
    Certificate, CertificateAPI, CommittedSubDag, ConditionalBroadcastReceiver,
};
use tokio::{sync::watch, task::JoinHandle};
use tracing::debug;

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;


/// Container for reaching finality in the consensus layer.
pub struct Consensus {
    /// The committee information.
    committee: Committee,
    /// Receiver for shutdown.
    rx_shutdown: ConditionalBroadcastReceiver,
    /// Receives new certificates from the primary. The primary should send us new certificates
    /// only if it already sent us its whole history.
    rx_new_certificates: metered_channel::Receiver<Certificate>,
    /// Outputs the sequence of ordered certificates to the primary (for cleanup and feedback).
    tx_committed_certificates: metered_channel::Sender<(Round, Vec<Certificate>)>,
    /// Outputs the highest committed round & corresponding gc_round in the consensus.
    tx_consensus_round_updates: watch::Sender<ConsensusRound>,
    /// Outputs the sequence of ordered certificates to the application layer.
    tx_sequence: metered_channel::Sender<CommittedSubDag>,

    /// The consensus protocol to run.
    protocol: Bullshark,

    /// Metrics handler
    metrics: Arc<ConsensusMetrics>,

    /// Inner state
    state: ConsensusState,
}

impl Consensus {
    #[must_use]
    pub fn spawn(
        committee: Committee,
        gc_depth: Round,
        store: Arc<ConsensusStore>,
        cert_store: CertificateStore,
        rx_shutdown: ConditionalBroadcastReceiver,
        rx_new_certificates: metered_channel::Receiver<Certificate>,
        tx_committed_certificates: metered_channel::Sender<(Round, Vec<Certificate>)>,
        tx_consensus_round_updates: watch::Sender<ConsensusRound>,
        tx_sequence: metered_channel::Sender<CommittedSubDag>,
        protocol: Bullshark,
        metrics: Arc<ConsensusMetrics>,
    ) -> JoinHandle<()> {
        // The consensus state (everything else is immutable).
        let recovered_last_committed = store.read_last_committed();
        let last_committed_round = recovered_last_committed
            .iter()
            .max_by(|a, b| a.1.cmp(b.1))
            .map(|(_k, v)| *v)
            .unwrap_or_else(|| 0);
        let latest_sub_dag = store.get_latest_sub_dag();
        if let Some(sub_dag) = &latest_sub_dag {
            assert_eq!(
                sub_dag.leader_round(),
                last_committed_round,
                "Last subdag leader round {} is not equal to the last committed round {}!",
                sub_dag.leader_round(),
                last_committed_round,
            );
        }

        let state = ConsensusState::new_from_store(
            metrics.clone(),
            last_committed_round,
            gc_depth,
            recovered_last_committed,
            latest_sub_dag,
            cert_store,
        );

        tx_consensus_round_updates
            .send(state.last_round)
            .expect("Failed to send last_committed_round on initialization!");

        let s = Self {
            committee,
            rx_shutdown,
            rx_new_certificates,
            tx_committed_certificates,
            tx_consensus_round_updates,
            tx_sequence,
            protocol,
            metrics,
            state,
        };

        spawn_logged_monitored_task!(s.run(), "Consensus", INFO)
    }

    async fn run(self) {
        match self.run_inner().await {
            Ok(_) => {}
            Err(err @ ConsensusError::ShuttingDown) => {
                debug!("{:?}", err)
            }
            Err(err) => panic!("Failed to run consensus: {:?}", err),
        }
    }

    async fn run_inner(mut self) -> Result<(), ConsensusError> {
        // Listen to incoming certificates.
        'main: loop {
            tokio::select! {

                _ = self.rx_shutdown.receiver.recv() => {
                    return Ok(())
                }

                Some(certificate) = self.rx_new_certificates.recv() => {
                    match certificate.epoch().cmp(&self.committee.epoch()) {
                        Ordering::Equal => {
                            // we can proceed.
                        }
                        _ => {
                            tracing::debug!("Already moved to the next epoch");
                            continue 'main;
                        }
                    }

                    // Process the certificate using the selected consensus protocol.
                    let (_, committed_sub_dags) = self.protocol.process_certificate(&mut self.state, certificate)?;

                    // We extract a list of headers from this specific validator that
                    // have been agreed upon, and signal this back to the narwhal sub-system
                    // to be used to re-send batches that have not made it to a commit.
                    let mut committed_certificates = Vec::new();

                    // Output the sequence in the right order.
                    let mut i = 0;
                    for committed_sub_dag in committed_sub_dags {
                         tracing::debug!("Commit in Sequence {:?}", committed_sub_dag.sub_dag_index);

                        for certificate in &committed_sub_dag.certificates {
                            i+=1;

                            if i % 5_000 == 0 {
                                #[cfg(not(feature = "benchmark"))]
                                tracing::debug!("Committed {}", certificate.header());
                            }

                            #[cfg(feature = "benchmark")]
                            for digest in certificate.header().payload().keys() {
                                // NOTE: This log entry is used to compute performance.
                                tracing::info!("Committed {} -> {:?}", certificate.header(), digest);
                            }

                            committed_certificates.push(certificate.clone());
                        }

                        // NOTE: The size of the sub-dag can be arbitrarily large (depending on the network condition
                        // and Byzantine leaders).
                        self.tx_sequence.send(committed_sub_dag).await.map_err(|_|ConsensusError::ShuttingDown)?;
                    }

                    if !committed_certificates.is_empty(){
                        // Highest committed certificate round is the leader round / commit round
                        // expected by primary.
                        let leader_commit_round = committed_certificates.iter().map(|c| c.round()).max().unwrap();

                        self.tx_committed_certificates
                        .send((leader_commit_round, committed_certificates))
                        .await
                        .map_err(|_|ConsensusError::ShuttingDown)?;

                        assert_eq!(self.state.last_round.committed_round, leader_commit_round);

                        self.tx_consensus_round_updates.send(self.state.last_round)
                        .map_err(|_|ConsensusError::ShuttingDown)?;
                    }

                    self.metrics
                        .consensus_dag_rounds
                        .with_label_values(&[])
                        .set(self.state.dag.len() as i64);
                },

            }
        }
    }
}
