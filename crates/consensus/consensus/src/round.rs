// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use std::cmp::max;
use crate::utils::gc_round;
use tn_types::consensus::Round;

/// Holds information about a committed round in consensus. When a certificate gets committed then
/// the corresponding certificate's round is considered a "committed" round. It bears both the
/// committed round and the corresponding garbage collection round.
#[derive(Debug, Default, Copy, Clone)]
pub struct ConsensusRound {
    /// The last round to be committed.
    pub committed_round: Round,
    /// The last round for garbage collection.
    pub gc_round: Round,
}

impl ConsensusRound {
    /// Create a new instance of Self.
    pub fn new(committed_round: Round, gc_round: Round) -> Self {
        Self { committed_round, gc_round }
    }

    /// Create a new instance of Self by calculating the GC round given a commit round and the gc_depth.
    /// 
    /// This method is used to recreate `ConsensusState` from the `CertificateStore`.
    pub fn new_with_gc_depth(committed_round: Round, gc_depth: Round) -> Self {
        let gc_round = gc_round(committed_round, gc_depth);

        Self { committed_round, gc_round }
    }

    /// Calculates the latest CommittedRound by providing a new committed round and the gc_depth.
    /// The method will compare against the existing committed round and return
    /// the updated instance.
    pub(super) fn update(&self, new_committed_round: Round, gc_depth: Round) -> Self {
        let last_committed_round = max(self.committed_round, new_committed_round);
        let last_gc_round = gc_round(last_committed_round, gc_depth);

        ConsensusRound { committed_round: last_committed_round, gc_round: last_gc_round }
    }
}
