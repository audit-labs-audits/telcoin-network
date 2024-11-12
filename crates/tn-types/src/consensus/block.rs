//! Define the block header for the Telcoin "Consensus Chain"
//!
//! This is a very simple (data only) chain that records consesus output.
//! It can be used to validate the execution chain, catch up with consesus,
//! introduce a new validator to participate in consensus (either as a voter
//! or observer) or any task that requires realtime or historic consesus data
//! if not directly participating in consesus.

use reth_primitives::{BlockHash, B256};
use serde::{Deserialize, Serialize};

use crate::crypto;
use fastcrypto::hash::HashFunction;

/// Header for the consensus chain.
///
/// The consensus chain records consensus output used to extend the execution chain.
/// All hashes are Keccak 256.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ConsensusHeader {
    /// The hash of the previous ConsesusHeader in the chain.
    pub parent_hash: B256,

    /// This is the hash of the committed sub dag used to extend the execution chain.
    pub sub_dag_hash: B256,
    /// A scalar value equal to the number of ancestor blocks. The genesis block has a number of
    /// zero.
    pub number: u64,
}

impl ConsensusHeader {
    pub fn digest(&self) -> BlockHash {
        let mut hasher = crypto::DefaultHashFunction::new();
        hasher.update(self.parent_hash);
        hasher.update(self.sub_dag_hash);
        hasher.update(self.number.to_le_bytes());
        BlockHash::from_slice(&hasher.finalize().digest)
    }
}
