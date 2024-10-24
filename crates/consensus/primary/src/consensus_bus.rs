// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

//! Implement a container for channels used internally by consensus.
//! This allows easier examination of message flow and avoids excessives channel passing as
//! arguments.

use std::sync::Arc;

use consensus_metrics::metered_channel;
use parking_lot::Mutex;
use tn_types::{Certificate, Round, CHANNEL_CAPACITY};
use tokio::sync::watch;

use crate::consensus::ConsensusRound;

#[derive(Clone, Debug)]
pub struct ConsensusBus {
    //new_certificates: Arc<dyn TnSender<Certificate>>,
    rx_new_certificates: Arc<Mutex<Option<metered_channel::Receiver<Certificate>>>>,
    tx_committed_certificates: metered_channel::Sender<(Round, Vec<Certificate>)>,
    tx_consensus_round_updates: watch::Sender<ConsensusRound>,
}

impl ConsensusBus {
    pub fn new(primary_metrics: narwhal_primary_metrics::Metrics) -> Self {
        let (tx_new_certificates, rx_new_certificates) = metered_channel::channel(
            CHANNEL_CAPACITY,
            &primary_metrics.primary_channel_metrics.tx_new_certificates,
        );

        let (tx_committed_certificates, rx_committed_certificates) = metered_channel::channel(
            CHANNEL_CAPACITY,
            &primary_metrics.primary_channel_metrics.tx_committed_certificates,
        );

        let (tx_consensus_round_updates, _rx_consensus_round_updates) =
            watch::channel(ConsensusRound::new(0, 0));
        Self {
            //tx_new_certificates,
            rx_new_certificates: Arc::new(Mutex::new(Some(rx_new_certificates))),
            tx_committed_certificates,
            tx_consensus_round_updates,
        }
    }

    //pub fn new_certificates(&self) -> &metered_channel::Sender<Certificate> {
    //    &self.tx_new_certificates
    //}

    pub fn subscribe_new_certificates(&self) -> metered_channel::Receiver<Certificate> {
        self.rx_new_certificates
            .lock()
            .take()
            .expect("Can only subscribe to new certificates once (underlying mpsc channel)")
    }

    pub fn consesus_round_updates(&self) -> &watch::Sender<ConsensusRound> {
        &self.tx_consensus_round_updates
    }
}
