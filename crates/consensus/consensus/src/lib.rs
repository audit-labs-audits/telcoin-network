// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
#![warn(future_incompatible, nonstandard_style, rust_2018_idioms, rust_2021_compatibility)]

pub mod bullshark;
pub mod consensus;
pub use consensus::Consensus;
#[cfg(test)]
#[path = "tests/consensus_utils.rs"]
pub mod consensus_utils;
pub mod dag;
pub mod metrics;
pub mod utils;
mod error;
pub use error::{ConsensusError, ValidatorDagError};
mod state;
pub use state::ConsensusState;
mod round;
pub use round::ConsensusRound;

/// The default channel size used in the consensus and subscriber logic.
pub const DEFAULT_CHANNEL_SIZE: usize = 1_000;

/// The number of shutdown receivers to create on startup. We need one per component loop.
pub const NUM_SHUTDOWN_RECEIVERS: u64 = 25;

/// Possible outcomes from processing a certificate for a round.
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Certificate is not processed, since it's below the latest committed round for its origin.
    CertificateBelowCommitRound,

    /// Certificate processed is of an even round, so the previous one is an odd round and
    /// no leader election takes process.
    NoLeaderElectedForOddRound,

    /// Leader has been elected, but it's below the latest commit round, so commit happens.
    LeaderBelowCommitRound,

    /// Tried to do a leader election, but leader was not found for the round, not commit will
    /// take place.
    LeaderNotFound,

    /// Leader has been found,  but there was no enough support from the children nodes, so leader
    /// can't be used to commit.
    NotEnoughSupportForLeader,

    /// Processed Certificate triggered a commit.
    Commit,
}
