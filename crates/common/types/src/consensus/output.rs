// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
#![allow(clippy::mutable_key_type)]

use crate::{consensus::{
    Batch, Certificate, CertificateAPI, CertificateDigest, HeaderAPI, Round, TimestampSec,
    Committee, AuthorityIdentifier,
}, execution::keccak256};
use enum_dispatch::enum_dispatch;
use fastcrypto::hash::Hash;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc;
use tracing::warn;
use crate::execution::H256;

/// A global sequence number assigned to every CommittedSubDag.
pub type SequenceNumber = u64;

/// The output of Consensus, which includes all the batches for each certificate in the sub-dag.
/// It is sent to the the ExecutionState `handle_consensus_output()` method.
#[derive(Clone, Debug)]
pub struct ConsensusOutput {
    /// The committed sub-dag representing the latest leader for a new round.
    pub sub_dag: Arc<CommittedSubDag>,
    /// The batches contained within each certificate from the last round of consensus.
    pub batches: Vec<(Certificate, Vec<Batch>)>,
}

impl ConsensusOutput {
    /// Get the round from the leader of the [CommittedSubDag].
    pub fn leader_round(&self) -> Round {
        self.sub_dag.leader_round()
    }

    /// The leader of the subdag.
    pub fn leader(&self) -> &Certificate {
        &self.sub_dag.leader
    }

    /// Parent block hash.
    pub fn parent_hash(&self) -> &H256 {
        self.leader().parent_hash()
    }

    /// Commit timestamp for when the subdag was committed.
    pub fn committed_at(&self) -> TimestampSec {
        self.sub_dag.commit_timestamp()
    }

    /// TODO: keccak hash of the leader's certificate's roaring bitmap
    pub fn prevrandao(&self) -> H256 {
        let signature_bytes = self.leader().aggregated_signature();
        keccak256(signature_bytes.0)
    }
}

/// The result of committing a new leader for a round during consensus.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct CommittedSubDag {
    /// The sequence of committed certificates.
    ///
    /// TODO: how is this used in EL?
    pub certificates: Vec<Certificate>,
    /// The leader certificate responsible of committing this sub-dag.
    /// 
    /// The leader earns PoS rewards for the block in EL.
    pub leader: Certificate,
    /// The index associated with this CommittedSubDag.
    /// 
    /// Akin to the EL's BlockHeight.
    pub sub_dag_index: SequenceNumber,
    /// The so far calculated reputation score for nodes
    /// 
    /// TODO: how is this used in the EL?
    pub reputation_score: ReputationScores,
    /// The timestamp that should identify this commit. This is guaranteed to be monotonically
    /// incremented. This is not necessarily the leader's timestamp. We compare the leader's
    /// timestamp with the previously committed sud dag timestamp and we always keep the max.
    /// Property is explicitly private so the method commit_timestamp() should be used instead
    /// which bears additional resolution logic.
    commit_timestamp: TimestampSec,
}

impl CommittedSubDag {
    /// Create a new instance of Self.
    pub fn new(
        certificates: Vec<Certificate>,
        leader: Certificate,
        sub_dag_index: SequenceNumber,
        reputation_score: ReputationScores,
        previous_sub_dag: Option<&CommittedSubDag>,
    ) -> Self {
        // Narwhal enforces some invariants on the header.created_at, so we can use it as a
        // timestamp.
        let previous_sub_dag_ts = previous_sub_dag.map(|s| s.commit_timestamp).unwrap_or_default();
        let commit_timestamp = previous_sub_dag_ts.max(*leader.header().created_at());

        if previous_sub_dag_ts > *leader.header().created_at() {
            warn!(sub_dag_index = ?sub_dag_index, "Leader timestamp {} is older than previously committed sub dag timestamp {}. Auto-correcting to max {}.",
            leader.header().created_at(), previous_sub_dag_ts, commit_timestamp);
        }

        Self { certificates, leader, sub_dag_index, reputation_score, commit_timestamp }
    }

    /// Create a new instance of `Self` from committed round of consensus.
    pub fn from_commit(
        commit: ConsensusCommit,
        certificates: Vec<Certificate>,
        leader: Certificate,
    ) -> Self {
        Self {
            certificates,
            leader,
            sub_dag_index: commit.sub_dag_index(),
            reputation_score: commit.reputation_score(),
            commit_timestamp: commit.commit_timestamp(),
        }
    }

    /// The number of certificates in the committed sub-dag.
    pub fn len(&self) -> usize {
        self.certificates.len()
    }

    /// Indicates if there are no certificates in the committed sub-dag.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The total number of batches within all certficates of the committed sub-dag.
    pub fn num_batches(&self) -> usize {
        self.certificates.iter().map(|x| x.header().payload().len()).sum()
    }

    /// Indicates if this passed certificate is the last in the committed sub-dag.
    pub fn is_last(&self, output: &Certificate) -> bool {
        self.certificates.iter().last().map_or_else(|| false, |x| x == output)
    }

    /// The round of the leader certificate responsible for committing this sub-dag.
    pub fn leader_round(&self) -> Round {
        self.leader.round()
    }

    /// The timestamp for when this sub-dag was committed.
    pub fn commit_timestamp(&self) -> TimestampSec {
        // If commit_timestamp is zero, then safely assume that this is an upgraded node that is
        // replaying this commit and field is never initialised. It's safe to fallback on leader's
        // timestamp.
        if self.commit_timestamp == 0 {
            return *self.leader.header().created_at()
        }
        self.commit_timestamp
    }
}

/// The relative score of each authority participating in consensus.
#[derive(Serialize, Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct ReputationScores {
    /// Holds the score for every authority. If an authority is not amongst
    /// the records of the map then we assume that its score is zero.
    pub scores_per_authority: HashMap<AuthorityIdentifier, u64>,
    /// When true it notifies us that those scores will be the last updated scores of the
    /// current schedule before they get reset for the next schedule and start
    /// scoring from the beginning. In practice we can leverage this information to
    /// use the scores during the next schedule until the next final ones are calculated.
    pub final_of_schedule: bool,
}

impl ReputationScores {
    /// Creating a new ReputationScores instance pre-populating the authorities entries with
    /// zero score value.
    pub fn new(committee: &Committee) -> Self {
        let scores_per_authority = committee.authorities().map(|a| (a.id(), 0_u64)).collect();

        Self { scores_per_authority, ..Default::default() }
    }

    /// Adds the provided `score` to the existing score for the provided `authority`
    pub fn add_score(&mut self, authority: AuthorityIdentifier, score: u64) {
        self.scores_per_authority
            .entry(authority)
            .and_modify(|value| *value += score)
            .or_insert(score);
    }

    /// The number of authorities with reputation scores.
    pub fn total_authorities(&self) -> u64 {
        self.scores_per_authority.len() as u64
    }

    /// The authorities with reputation scores of 0.
    /// 
    /// Note: If an authority is not amongst the records of the map then we assume
    /// that its score is also zero.
    pub fn all_zero(&self) -> bool {
        !self.scores_per_authority.values().any(|e| *e > 0)
    }
}

#[enum_dispatch(ConsensusCommitAPI)]
trait ConsensusCommitAPI {
    /// Retrieve all certificate digests from the sub-dag for the committed round.
    fn certificates(&self) -> Vec<CertificateDigest>;
    /// The [CertificateDigest] for the certificate that is responsible for committing this round.
    fn leader(&self) -> CertificateDigest;
    /// The round of the leader certificate responsible for committing this sub-dag.
    fn leader_round(&self) -> Round;
    /// The sequence number for the committed sub-dag.
    fn sub_dag_index(&self) -> SequenceNumber;
    /// The relative score of each authority in the committed sub-dag.
    fn reputation_score(&self) -> ReputationScores;
    /// The time the sub-dag was committed.
    fn commit_timestamp(&self) -> TimestampSec;
}

/// Versioned commit from consensus.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ConsensusCommitV1 {
    /// The sequence of committed certificates' digests.
    pub certificates: Vec<CertificateDigest>,
    /// The leader certificate's digest responsible of committing this sub-dag.
    pub leader: CertificateDigest,
    /// The round of the leader
    pub leader_round: Round,
    /// Sequence number of the CommittedSubDag
    pub sub_dag_index: SequenceNumber,
    /// The so far calculated reputation score for nodes
    pub reputation_score: ReputationScores,
    /// The timestamp that should identify this commit. This is guaranteed to be monotonically
    /// incremented
    pub commit_timestamp: TimestampSec,
}

impl ConsensusCommitV1 {
    /// Create an instance of `Self` from a referance to `CommittedSubDag`.
    pub fn from_sub_dag(sub_dag: &CommittedSubDag) -> Self {
        Self {
            certificates: sub_dag.certificates.iter().map(|x| x.digest()).collect(),
            leader: sub_dag.leader.digest(),
            leader_round: sub_dag.leader.round(),
            sub_dag_index: sub_dag.sub_dag_index,
            reputation_score: sub_dag.reputation_score.clone(),
            commit_timestamp: sub_dag.commit_timestamp,
        }
    }
}

impl ConsensusCommitAPI for ConsensusCommitV1 {
    fn certificates(&self) -> Vec<CertificateDigest> {
        self.certificates.clone()
    }

    fn leader(&self) -> CertificateDigest {
        self.leader
    }

    fn leader_round(&self) -> Round {
        self.leader_round
    }

    fn sub_dag_index(&self) -> SequenceNumber {
        self.sub_dag_index
    }

    fn reputation_score(&self) -> ReputationScores {
        self.reputation_score.clone()
    }

    fn commit_timestamp(&self) -> TimestampSec {
        self.commit_timestamp
    }
}

/// Versioned representation of finalized output from consensus.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[enum_dispatch(ConsensusCommitAPI)]
pub enum ConsensusCommit {
    /// Version 1
    V1(ConsensusCommitV1),
}

impl ConsensusCommit {
    /// Retrieve all certificate digests from the sub-dag for the committed round.
    pub fn certificates(&self) -> Vec<CertificateDigest> {
        match self {
            ConsensusCommit::V1(sub_dag) => sub_dag.certificates(),
        }
    }

    /// The CertificateDigest for the certificate that is responsible for committing this round.
    pub fn leader(&self) -> CertificateDigest {
        match self {
            ConsensusCommit::V1(sub_dag) => sub_dag.leader(),
        }
    }

    /// The round of the leader certificate responsible for committing this sub-dag.
    pub fn leader_round(&self) -> Round {
        match self {
            ConsensusCommit::V1(sub_dag) => sub_dag.leader_round(),
        }
    }

    /// The sequence number for the committed sub-dag.
    pub fn sub_dag_index(&self) -> SequenceNumber {
        match self {
            ConsensusCommit::V1(sub_dag) => sub_dag.sub_dag_index(),
        }
    }

    /// The so far calculated reputation score for nodes.
    pub fn reputation_score(&self) -> ReputationScores {
        match self {
            ConsensusCommit::V1(sub_dag) => sub_dag.reputation_score(),
        }
    }

    /// The timestamp that should identify this commit. This is guaranteed to be monotonically
    /// incremented.
    pub fn commit_timestamp(&self) -> TimestampSec {
        match self {
            ConsensusCommit::V1(sub_dag) => sub_dag.commit_timestamp(),
        }
    }
}

/// Shutdown token dropped when a task is properly shut down.
pub type ShutdownToken = mpsc::Sender<()>;
