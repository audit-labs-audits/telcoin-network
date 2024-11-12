//! Define the block header for the Telcoin "Consensus Chain"
//!
//! This is a very simple (data only) chain that records consesus output.
//! It can be used to validate the execution chain, catch up with consesus,
//! introduce a new validator to participate in consensus (either as a voter
//! or observer) or any task that requires realtime or historic consesus data
//! if not directly participating in consesus.

use reth_primitives::{BlockHash, B256};
use serde::{Deserialize, Serialize};

use crate::{crypto, Certificate};
use fastcrypto::hash::{Hash, HashFunction};

use super::CommittedSubDag;

/// Header for the consensus chain.
///
/// The consensus chain records consensus output used to extend the execution chain.
/// All hashes are Keccak 256.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ConsensusHeader {
    /// The hash of the previous ConsesusHeader in the chain.
    pub parent_hash: B256,

    /// This is the committed sub dag used to extend the execution chain.
    pub sub_dag: CommittedSubDag,

    /// A scalar value equal to the number of ancestor blocks. The genesis block has a number of
    /// zero.
    pub number: u64,
}

impl ConsensusHeader {
    pub fn digest(&self) -> BlockHash {
        let mut hasher = crypto::DefaultHashFunction::new();
        hasher.update(self.parent_hash);
        hasher.update(self.sub_dag.digest());
        hasher.update(self.number.to_le_bytes());
        BlockHash::from_slice(&hasher.finalize().digest)
    }
}

impl Default for ConsensusHeader {
    fn default() -> Self {
        let sub_dag = CommittedSubDag::new(
            vec![],
            Certificate::default(),
            0,
            crate::ReputationScores::default(),
            None,
        );
        Self { parent_hash: B256::default(), sub_dag, number: 0 }
    }
}
