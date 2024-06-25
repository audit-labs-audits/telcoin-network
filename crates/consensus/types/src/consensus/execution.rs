//! Recreated `AutoSealConsensus` to reduce the amount of imports from reth.

use crate::{Batch, BatchAPI, ConsensusOutput, MetadataAPI};

use super::{Consensus, ConsensusError};
use reth_chainspec::ChainSpec;
use reth_consensus::PostExecutionInput;
use reth_engine_primitives::PayloadBuilderAttributes;
use reth_primitives::{
    constants::EIP1559_INITIAL_BASE_FEE, revm::config::revm_spec_by_timestamp_after_merge, Address,
    BlockWithSenders, ChainSpec, Hardfork, Header, SealedBlock, SealedBlockWithSenders,
    SealedHeader, Withdrawals, B256, U256,
};
use reth_revm::primitives::{
    BlobExcessGasAndPrice, BlockEnv, CfgEnv, CfgEnvWithHandlerCfg, SpecId,
};
use reth_primitives::{BlockWithSenders, Header, SealedBlock, SealedHeader, U256};
use reth_rpc_types::{engine::PayloadId, Withdrawal};
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, sync::Arc};

/// A consensus implementation that validates everything.
///
/// Taken from reth's `AutoSealConsensus`.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct AutoSealConsensus {
    /// Configuration
    chain_spec: Arc<ChainSpec>,
}

impl AutoSealConsensus {
    /// Create a new instance of [AutoSealConsensus]
    pub fn new(chain_spec: Arc<ChainSpec>) -> Self {
        Self { chain_spec }
    }

    /// Clone copy of `ChainSpec`.
    pub fn chain_spec(&self) -> &Arc<ChainSpec> {
        &self.chain_spec
    }
}

impl Consensus for AutoSealConsensus {
    fn validate_header(&self, _header: &SealedHeader) -> Result<(), ConsensusError> {
        Ok(())
    }

    fn validate_header_against_parent(
        &self,
        _header: &SealedHeader,
        _parent: &SealedHeader,
    ) -> Result<(), ConsensusError> {
        Ok(())
    }

    fn validate_header_with_total_difficulty(
        &self,
        _header: &Header,
        _total_difficulty: U256,
    ) -> Result<(), ConsensusError> {
        Ok(())
    }

    fn validate_block_pre_execution(&self, _block: &SealedBlock) -> Result<(), ConsensusError> {
        Ok(())
    }

    fn validate_block_post_execution(
        &self,
        _block: &BlockWithSenders,
        _input: PostExecutionInput<'_>,
    ) -> Result<(), ConsensusError> {
        Ok(())
    }
}

/// The type for building blocks that extend the canonical tip.
#[derive(Debug)]
pub struct BuildArguments<Provider> {
    /// State provider.
    pub provider: Provider,
    /// Output from consensus that contains all the transactions to execute.
    pub output: ConsensusOutput,
    /// Last executed block from the previous consensus output.
    pub parent_block: SealedBlock,
    /// The chain spec.
    pub chain_spec: Arc<ChainSpec>,
}

/// The type used to build the next canonical block.
#[derive(Debug)]
pub struct TNPayload<'a> {
    //
    // this is the concept of the block with additional information from consensus output needed for execution
    //
    /// The hash of the last block executed from the previous round of consensus.
    pub parent: B256,
    /// Attributes to use when building the payload.
    pub attributes: TNPayloadAttributes<'a>,
}

impl<'a> TNPayload<'a> {
    /// Create a new instance of [Self].
    pub fn new(parent: B256, attributes: TNPayloadAttributes<'a>) -> Self {
        Self { parent, attributes }
    }
}

/// The type used to construct a [TNPayload].
///
/// It contains all the attributes required to initiate a payload build process.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TNPayloadAttributes<'a> {
    /// Value for the `timestamp` field of the new payload
    pub timestamp: u64,
    /// Value for the `prevRandao` field of the new payload
    pub prev_randao: B256,
    /// Array of [`Withdrawal`].
    pub withdrawals: Withdrawals,
    /// Root of the parent beacon block?
    pub parent_beacon_block_root: Option<B256>,
    /// The block to build this payload from.
    pub block: &'a SealedBlockWithSenders,
    /// The index of the block within the entire output from consensus.
    pub block_index: u64,
    /// The beneficiary from the round of consensus.
    pub beneficiary: Address,
    /// The previous canonical block.
    pub parent_block: &'a SealedBlock,
}

impl<'a> TNPayloadAttributes<'a> {
    /// Create a new instance of [Self].
    pub fn new(
        output: &ConsensusOutput,
        block: &SealedBlockWithSenders,
        block_index: u64,
        parent_block: &SealedBlock,
    ) -> Self {
        Self {
            timestamp: output.committed_at(),
            prev_randao: todo!(),
            withdrawals: todo!(),
            parent_beacon_block_root: todo!(),
            block,
            block_index,
            beneficiary: output.beneficiary(),
            parent_block,
        }
    }
}

/// Implement [PayloadBuilderAttributes] for extending the canonical tip after consensus is reached.
impl<'a> PayloadBuilderAttributes for TNPayload<'a> {
    type RpcPayloadAttributes = TNPayloadAttributes<'a>;

    // TODO: use actual error here
    type Error = Infallible;

    fn try_new(parent: B256, attributes: Self::RpcPayloadAttributes) -> Result<Self, Self::Error> {
        Ok(Self::new(parent, attributes))
    }

    fn payload_id(&self) -> PayloadId {
        // construct the payload id from the block's index
        // guaranteed to always be unique within each output
        PayloadId::new(self.attributes.block_index.to_le_bytes())
    }

    fn parent(&self) -> B256 {
        self.parent
    }

    fn timestamp(&self) -> u64 {
        self.attributes.timestamp
    }

    fn parent_beacon_block_root(&self) -> Option<B256> {
        self.attributes.parent_beacon_block_root
    }

    fn suggested_fee_recipient(&self) -> Address {
        self.attributes.beneficiary
    }

    /// This is used by TN to indicate the [Batch]'s hash.
    fn prev_randao(&self) -> B256 {
        todo!()
    }

    fn withdrawals(&self) -> &Withdrawals {
        // self.attributes.withdrawals.unwrap_or_default().into()
        &self.attributes.withdrawals
    }

    fn cfg_and_block_env(
        &self,
        chain_spec: &ChainSpec,
        _parent: &Header,
    ) -> (CfgEnvWithHandlerCfg, BlockEnv) {
        // configure evm env based on parent block
        let cfg = CfgEnv::default().with_chain_id(chain_spec.chain().id());

        // ensure we're not missing any timestamp based hardforks
        let spec_id = revm_spec_by_timestamp_after_merge(chain_spec, self.timestamp());

        // TODO: support blob_excess_gas_and_price for Batch
        // let blob_excess_gas_and_price = parent
        //     .next_block_excess_blob_gas()
        //     .or_else(|| {
        //         if spec_id == SpecId::CANCUN {
        //             // default excess blob gas is zero
        //             Some(0)
        //         } else {
        //             None
        //         }
        //     })
        //     .map(BlobExcessGasAndPrice::new);
        let blob_excess_gas_and_price = Some(BlobExcessGasAndPrice::new(0));

        // use the block's sealed header for "parent" values
        let block = self.attributes.block.header();

        // TODO: is this the correct value for basefee?
        let basefee = block.base_fee_per_gas;
        // parent.next_block_base_fee(chain_spec.base_fee_params_at_timestamp(self.timestamp()));

        // ensure gas_limit enforced during block validation
        let gas_limit = U256::from(block.gas_limit);

        // TODO: DELETE ME
        // basefee is always included, leaving this here for now since basefee is still a question
        //
        //
        // // If we are on the London fork boundary, we need to multiply the parent's gas limit by the
        // // elasticity multiplier to get the new gas limit.
        // if chain_spec.fork(Hardfork::London).transitions_at_block(parent.number + 1) {
        //     let elasticity_multiplier =
        //         chain_spec.base_fee_params_at_timestamp(self.timestamp()).elasticity_multiplier;

        //     // multiply the gas limit by the elasticity multiplier
        //     gas_limit *= U256::from(elasticity_multiplier);

        //     // set the base fee to the initial base fee from the EIP-1559 spec
        //     basefee = Some(EIP1559_INITIAL_BASE_FEE)
        // }

        let block_env = BlockEnv {
            number: U256::from(self.attributes.parent_block.number + 1),
            coinbase: self.suggested_fee_recipient(),
            timestamp: U256::from(self.timestamp()),
            difficulty: U256::ZERO,
            prevrandao: Some(self.prev_randao()),
            gas_limit,
            // calculate basefee based on parent block's gas usage
            basefee: basefee.map(U256::from).unwrap_or_default(),
            // calculate excess gas based on parent block's blob gas usage
            blob_excess_gas_and_price,
        };

        (CfgEnvWithHandlerCfg::new_with_spec_id(cfg, spec_id), block_env)
    }
}
