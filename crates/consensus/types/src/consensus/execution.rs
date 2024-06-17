//! Recreated `AutoSealConsensus` to reduce the amount of imports from reth.

use crate::ConsensusOutput;

use super::{Consensus, ConsensusError};
use reth_chainspec::ChainSpec;
use reth_consensus::PostExecutionInput;
use reth_engine_primitives::PayloadBuilderAttributes;
use reth_primitives::{
    Address, BlockWithSenders, ChainSpec, Header, SealedBlock, SealedHeader, B256, U256,
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
pub struct TNPayload;

/// The type constructed from a [Batch] and the [ConsensusOutput] that includes it for execution.
///
/// It contains all the attributes required to initiate a payload build process.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TNPayloadAttributes {
    /// Value for the `timestamp` field of the new payload
    pub timestamp: u64,
    /// Value for the `prevRandao` field of the new payload
    pub prev_randao: B256,
    /// Suggested value for the `feeRecipient` field of the new payload
    pub suggested_fee_recipient: Address,
    /// Array of [`Withdrawal`] enabled with V2
    /// See <https://github.com/ethereum/execution-apis/blob/6452a6b194d7db269bf1dbd087a267251d3cc7f8/src/engine/shanghai.md#payloadattributesv2>
    #[serde(skip_serializing_if = "Option::is_none")]
    pub withdrawals: Option<Vec<Withdrawal>>,
    /// Root of the parent beacon block enabled with V3.
    ///
    /// See also <https://github.com/ethereum/execution-apis/blob/main/src/engine/cancun.md#payloadattributesv3>
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_beacon_block_root: Option<B256>,
}

/// Implement [PayloadBuilderAttributes] for extending the canonical tip after consensus is reached.
impl PayloadBuilderAttributes for TNPayload {
    type RpcPayloadAttributes = TNPayloadAttributes;

    // TODO: use actual error here
    type Error = Infallible;

    fn try_new(
        parent: reth_primitives::B256,
        rpc_payload_attributes: Self::RpcPayloadAttributes,
    ) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        todo!()
    }

    fn payload_id(&self) -> PayloadId {
        todo!()
    }

    fn parent(&self) -> reth_primitives::B256 {
        todo!()
    }

    fn timestamp(&self) -> u64 {
        todo!()
    }

    fn parent_beacon_block_root(&self) -> Option<reth_primitives::B256> {
        todo!()
    }

    fn suggested_fee_recipient(&self) -> reth_primitives::Address {
        todo!()
    }

    fn prev_randao(&self) -> reth_primitives::B256 {
        todo!()
    }

    fn withdrawals(&self) -> &reth_primitives::Withdrawals {
        todo!()
    }

    fn cfg_and_block_env(
        &self,
        chain_spec: &ChainSpec,
        parent: &Header,
    ) -> (reth_revm::primitives::CfgEnvWithHandlerCfg, reth_revm::primitives::BlockEnv) {
        todo!()
    }
}
