// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

//! Implement a container for channels used internally by consensus.
//! This allows easier examination of message flow and avoids excessives channel passing as
//! arguments.

use std::sync::Arc;

use consensus_metrics::metered_channel::{self, channel_with_total_sender, Sender};
use narwhal_primary_metrics::{ConsensusMetrics, Metrics};
use tn_types::{Certificate, Header, Round, TnSender, CHANNEL_CAPACITY};
use tokio::sync::watch;

use crate::{
    certificate_fetcher::CertificateFetcherCommand, consensus::ConsensusRound,
    proposer::OurDigestMessage,
};

#[derive(Clone, Debug)]
pub struct ConsensusBus {
    /// Receives new certificates from the primary. The primary should send us new certificates
    /// only if it already sent us its whole history.
    new_certificates: Sender<Certificate>,
    /// Outputs the sequence of ordered certificates to the primary (for cleanup and feedback).
    committed_certificates: Sender<(Round, Vec<Certificate>)>,
    /// Outputs the highest committed round & corresponding gc_round in the consensus.
    tx_consensus_round_updates: watch::Sender<ConsensusRound>,
    /// Hold onto a receiver to keep it "open".
    _rx_consensus_round_updates: watch::Receiver<ConsensusRound>,

    /// Send missing certificates to the `CertificateFetcher`.
    /// Receives certificates with missing parents from the `Synchronizer`.
    certificate_fetcher: Sender<CertificateFetcherCommand>,
    // Send valid a quorum of certificates' ids to the `Proposer` (along with their round).
    /// Receives the parents to include in the next header (along with their round number) from
    /// `Synchronizer`.
    parents: Sender<(Vec<Certificate>, Round)>,
    /// Receives the batches' digests from our workers.
    our_digests: Sender<OurDigestMessage>,
    /// Sends newly created headers to the `Certifier`.
    headers: Sender<Header>,
    /// Receiver for updates when Self's headers were committed by consensus.
    ///
    /// NOTE: this does not mean the header was executed yet.
    committed_own_headers: Sender<(Round, Vec<Round>)>,

    /// Hold onto the consensus_metrics (mostly for testing)
    consensus_metrics: Arc<ConsensusMetrics>,
    /// Hold onto the primary metrics (allow early creation)
    primary_metrics: Arc<Metrics>,

    /// Signals a new narwhal round
    tx_narwhal_round_updates: watch::Sender<Round>,
    /// Hold onto the primary metrics (allow early creation)
    _rx_narwhal_round_updates: watch::Receiver<Round>,
}

impl ConsensusBus {
    pub fn new() -> Self {
        let consensus_metrics = Arc::new(ConsensusMetrics::default());
        let primary_metrics = Arc::new(Metrics::default()); // Initialize the metrics
        let new_certificates = metered_channel::channel_sender(
            CHANNEL_CAPACITY,
            &primary_metrics.primary_channel_metrics.tx_new_certificates,
        );

        let committed_certificates = metered_channel::channel_sender(
            CHANNEL_CAPACITY,
            &primary_metrics.primary_channel_metrics.tx_committed_certificates,
        );

        let (tx_consensus_round_updates, _rx_consensus_round_updates) =
            watch::channel(ConsensusRound::new(0, 0));

        // XXXX
        let our_digests = channel_with_total_sender(
            CHANNEL_CAPACITY,
            &primary_metrics.primary_channel_metrics.tx_our_digests,
            &primary_metrics.primary_channel_metrics.tx_our_digests_total,
        );
        let parents = channel_with_total_sender(
            CHANNEL_CAPACITY,
            &primary_metrics.primary_channel_metrics.tx_parents,
            &primary_metrics.primary_channel_metrics.tx_parents_total,
        );
        let headers = channel_with_total_sender(
            CHANNEL_CAPACITY,
            &primary_metrics.primary_channel_metrics.tx_headers,
            &primary_metrics.primary_channel_metrics.tx_headers_total,
        );
        let certificate_fetcher = channel_with_total_sender(
            CHANNEL_CAPACITY,
            &primary_metrics.primary_channel_metrics.tx_certificate_fetcher,
            &primary_metrics.primary_channel_metrics.tx_certificate_fetcher_total,
        );
        let committed_own_headers = channel_with_total_sender(
            CHANNEL_CAPACITY,
            &primary_metrics.primary_channel_metrics.tx_committed_own_headers,
            &primary_metrics.primary_channel_metrics.tx_committed_own_headers_total,
        );

        let (tx_narwhal_round_updates, _rx_narwhal_round_updates) = watch::channel(0u64);

        Self {
            new_certificates,
            committed_certificates,
            tx_consensus_round_updates,
            _rx_consensus_round_updates,
            certificate_fetcher,
            parents,
            our_digests,
            headers,
            committed_own_headers,

            tx_narwhal_round_updates,
            _rx_narwhal_round_updates,
            consensus_metrics,
            primary_metrics,
        }
    }

    pub fn new_certificates(&self) -> &impl TnSender<Certificate> {
        &self.new_certificates
    }

    pub fn committed_certificates(&self) -> &impl TnSender<(Round, Vec<Certificate>)> {
        &self.committed_certificates
    }

    pub fn certificate_fetcher(&self) -> &impl TnSender<CertificateFetcherCommand> {
        &self.certificate_fetcher
    }

    pub fn parents(&self) -> &impl TnSender<(Vec<Certificate>, Round)> {
        &self.parents
    }

    pub fn consensus_round_updates(&self) -> &watch::Sender<ConsensusRound> {
        &self.tx_consensus_round_updates
    }

    pub fn narwhal_round_updates(&self) -> &watch::Sender<u64> {
        &self.tx_narwhal_round_updates
    }

    pub fn our_digests(&self) -> &impl TnSender<OurDigestMessage> {
        &self.our_digests
    }

    pub fn headers(&self) -> &impl TnSender<Header> {
        &self.headers
    }

    pub fn committed_own_headers(&self) -> &impl TnSender<(Round, Vec<Round>)> {
        &self.committed_own_headers
    }

    pub fn consensus_metrics(&self) -> Arc<ConsensusMetrics> {
        self.consensus_metrics.clone()
    }

    pub fn primary_metrics(&self) -> Arc<Metrics> {
        self.primary_metrics.clone()
    }
}
