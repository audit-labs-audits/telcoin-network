//! RPC types for Lattice Consensus

use serde::{Deserialize, Serialize};
use tn_types::execution::{Address, Bytes, H256, U64};

// TODO: is payload id useful here for syncing with all other CL nodes?
/// This struct is used for executing batches from the consensus layer (CL).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatticePayload {
    /// Hash of the last finalized block that these transactions were verified against.
    pub parent_hash: H256,

    /// The hash of the sealed batch.
    pub batch_hash: H256,

    /// The round for the batch.
    pub round: U64,

    /// The timestamp the batch was created by a peer.
    pub timestamp: U64,

    /// The peer's primary.
    pub fee_recipient: Address,

    /// The list of transactions.
    pub transactions: Vec<Bytes>,
    // TODO: include these?
    // - gas_limit
    // - gas_used
    // - batch_size
}
