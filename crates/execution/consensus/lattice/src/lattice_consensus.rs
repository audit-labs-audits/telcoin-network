//! Consensus for ethereum network
use execution_consensus_common::validation;
use execution_interfaces::consensus::{Consensus, ConsensusError};
use std::sync::Arc;
use tn_types::execution::{
    constants::MAXIMUM_EXTRA_DATA_SIZE, ChainSpec, Header, SealedBlock, SealedHeader, U256,
};

/// Ethereum lattice consensus
///
/// This consensus engine does basic checks as outlined in the execution specs.
#[derive(Debug)]
pub struct LatticeConsensus {
    /// Configuration
    chain_spec: Arc<ChainSpec>,
}

impl LatticeConsensus {
    /// Create a new instance of [LatticeConsensus]
    pub fn new(chain_spec: Arc<ChainSpec>) -> Self {
        Self { chain_spec }
    }
}

impl Consensus for LatticeConsensus {
    fn validate_header(&self, _header: &SealedHeader) -> Result<(), ConsensusError> {
        // stub out for now
        Ok(())
    }

    fn validate_header_against_parent(
        &self,
        _header: &SealedHeader,
        _parent: &SealedHeader,
    ) -> Result<(), ConsensusError> {
        // stub out for now
        Ok(())
    }

    fn validate_header_with_total_difficulty(
        &self,
        _header: &Header,
        _total_difficulty: U256,
    ) -> Result<(), ConsensusError> {
        // stub out for now
        Ok(())
    }

    fn validate_block(&self, block: &SealedBlock) -> Result<(), ConsensusError> {
        validation::validate_block_standalone(block, &self.chain_spec)
    }
}

/// Validates the header's extradata according to the lattice consensus rules.
///
/// From yellow paper: extraData: An arbitrary byte array containing data relevant to this block.
/// This must be 32 bytes or fewer; formally Hx.
fn _validate_header_extradata(header: &Header) -> Result<(), ConsensusError> {
    if header.extra_data.len() > MAXIMUM_EXTRA_DATA_SIZE {
        Err(ConsensusError::ExtraDataExceedsMax { len: header.extra_data.len() })
    } else {
        Ok(())
    }
}
