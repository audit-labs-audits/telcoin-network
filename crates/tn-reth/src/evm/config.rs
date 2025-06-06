//! Provide config and implement traits to bridge protocol extensions to Reth.
//!
//! Inspired by: crates/ethereum/evm/src/lib.rs

use super::{TNBlockAssembler, TNBlockExecutionCtx, TNBlockExecutorFactory, TNEvmFactory};
use crate::{error::TnRethError, payload::TNPayload, traits::TNPrimitives};
use alloy::eips::{eip1559::INITIAL_BASE_FEE, eip7840::BlobParams};
use reth_chainspec::{ChainSpec, EthChainSpec as _, EthereumHardfork};
use reth_evm::{
    ConfigureEvm, EthEvmFactory, EvmEnv, EvmEnvFor, EvmFor, ExecutionCtxFor, NextBlockEnvAttributes,
};
use reth_evm_ethereum::{EthEvmConfig, RethReceiptBuilder};
use reth_primitives::{BlockTy, HeaderTy};
use reth_revm::{
    context::{BlockEnv, CfgEnv},
    context_interface::block::BlobExcessGasAndPrice,
    primitives::hardfork::SpecId,
    Database,
};
use std::{borrow::Cow, convert::Infallible, sync::Arc};
use tn_types::{
    Address, BlockHeader as _, Bytes, EthPrimitives, SealedBlock, SealedHeader, B256, U256,
};

/// TN-related EVM configuration.
///
/// TODO: consider constructing this each time with the `ConsensusOutput`?????
/// ??!!!!!
///
///
/// ??
#[derive(Debug, Clone)]
pub struct TnEvmConfig {
    /// Inner [`TNBlockExecutorFactory`].
    pub executor_factory: TNBlockExecutorFactory<RethReceiptBuilder, Arc<ChainSpec>, TNEvmFactory>,
    /// Ethereum block assembler.
    pub block_assembler: TNBlockAssembler<ChainSpec>,
}

impl TnEvmConfig {
    /// Creates a new TN EVM configuration with the given chain spec.
    pub fn new(chain_spec: Arc<ChainSpec>) -> Self {
        Self {
            block_assembler: TNBlockAssembler::new(chain_spec.clone()),
            executor_factory: TNBlockExecutorFactory::new(
                RethReceiptBuilder::default(),
                chain_spec,
                TNEvmFactory::default(),
            ),
        }
    }

    /// Returns the chain spec associated with this configuration.
    pub const fn chain_spec(&self) -> &Arc<ChainSpec> {
        self.executor_factory.spec()
    }

    // TODO: remove this after compile
    //
    /// Returns blob params by hard fork as specified in chain spec.
    /// Blob params are in format `(spec id, target blob count, max blob count)`.
    pub fn blob_max_and_target_count_by_hardfork(&self) -> Vec<(SpecId, u64, u64)> {
        let cancun = self.chain_spec().blob_params.cancun();
        let prague = self.chain_spec().blob_params.prague();
        let osaka = self.chain_spec().blob_params.osaka();
        Vec::from([
            (SpecId::CANCUN, cancun.target_blob_count, cancun.max_blob_count),
            (SpecId::PRAGUE, prague.target_blob_count, prague.max_blob_count),
            (SpecId::OSAKA, osaka.target_blob_count, osaka.max_blob_count),
        ])
    }
}

// impl ConfigureEvmEnv for TnEvmConfig {
//     type Header = ExecHeader;
//     type Transaction = TransactionSigned;
//     type Error = Infallible;

//     fn fill_tx_env(&self, tx_env: &mut TxEnv, transaction: &Self::Transaction, sender: Address) {
//         self.inner.fill_tx_env(tx_env, transaction, sender)
//     }

//     fn fill_tx_env_system_contract_call(
//         &self,
//         env: &mut Env,
//         caller: Address,
//         contract: Address,
//         data: Bytes,
//     ) {
//         self.inner.fill_tx_env_system_contract_call(env, caller, contract, data)
//     }

//     fn fill_cfg_env(&self, cfg_env: &mut CfgEnvWithHandlerCfg, header: &Self::Header) {
//         self.inner.fill_cfg_env(cfg_env, header)
//     }

//     fn next_cfg_and_block_env(
//         &self,
//         parent: &Self::Header,
//         attributes: NextBlockEnvAttributes,
//     ) -> Result<EvmEnv, Self::Error> {
//         self.inner.next_cfg_and_block_env(parent, attributes)
//     }
// }

// reth-evm
impl ConfigureEvm for TnEvmConfig {
    type Primitives = TNPrimitives;

    type Error = TnRethError;

    type NextBlockEnvCtx = TNPayload;

    type BlockExecutorFactory =
        TNBlockExecutorFactory<RethReceiptBuilder, Arc<ChainSpec>, TNEvmFactory>;

    type BlockAssembler = TNBlockAssembler<ChainSpec>;

    fn block_executor_factory(&self) -> &Self::BlockExecutorFactory {
        &self.executor_factory
    }

    fn block_assembler(&self) -> &Self::BlockAssembler {
        &self.block_assembler
    }

    fn evm_env(&self, header: &HeaderTy<Self::Primitives>) -> EvmEnvFor<Self> {
        let spec = reth_evm_ethereum::revm_spec(self.chain_spec(), header);

        // configure evm env based on parent block
        let mut cfg_env =
            CfgEnv::new().with_chain_id(self.chain_spec().chain().id()).with_spec(spec);

        let blob_params = self.chain_spec().blob_params_at_timestamp(header.timestamp);
        if let Some(blob_params) = &blob_params {
            cfg_env.set_blob_max_count(blob_params.max_blob_count);
        }

        // derive the EIP-4844 blob fees from the header's `excess_blob_gas` and the current
        // blobparams
        let blob_excess_gas_and_price = header
            .excess_blob_gas
            .zip(self.chain_spec().blob_params_at_timestamp(header.timestamp))
            .map(|(excess_blob_gas, params)| {
                let blob_gasprice = params.calc_blob_fee(excess_blob_gas);
                BlobExcessGasAndPrice { excess_blob_gas, blob_gasprice }
            });

        let block_env = BlockEnv {
            number: header.number(),
            beneficiary: header.beneficiary(),
            timestamp: header.timestamp(),
            difficulty: if spec >= SpecId::MERGE { U256::ZERO } else { header.difficulty() },
            prevrandao: if spec >= SpecId::MERGE { header.mix_hash() } else { None },
            gas_limit: header.gas_limit(),
            basefee: header.base_fee_per_gas().unwrap_or_default(),
            blob_excess_gas_and_price,
        };

        EvmEnv { cfg_env, block_env }
    }

    fn next_evm_env(
        &self,
        parent: &HeaderTy<Self::Primitives>,
        payload: &Self::NextBlockEnvCtx,
    ) -> Result<EvmEnvFor<Self>, Self::Error> {
        // ensure we're not missing any timestamp based hardforks
        let spec_id = reth_evm_ethereum::revm_spec_by_timestamp_and_block_number(
            self.chain_spec(),
            payload.timestamp,
            parent.number() + 1,
        );

        // configure evm env based on parent block
        let mut cfg =
            CfgEnv::new().with_chain_id(self.chain_spec().chain().id()).with_spec(spec_id);

        let blob_params = self.chain_spec().blob_params_at_timestamp(payload.timestamp);
        if let Some(blob_params) = &blob_params {
            cfg.set_blob_max_count(blob_params.max_blob_count);
        }
        // TODO: keep this logic or trash???
        //
        // @!!!!!!!111!!!!!!!
        // !
        //
        //
        // if the parent block did not have excess blob gas (i.e. it was pre-cancun), but it is
        // cancun now, we need to set the excess blob gas to the default value(0)
        let blob_excess_gas_and_price = parent
            .maybe_next_block_excess_blob_gas(blob_params)
            .or_else(|| (spec_id == SpecId::CANCUN).then_some(0))
            .map(|excess_blob_gas| {
                let blob_gasprice =
                    blob_params.unwrap_or_else(BlobParams::cancun).calc_blob_fee(excess_blob_gas);
                BlobExcessGasAndPrice { excess_blob_gas, blob_gasprice }
            });

        let block_env = BlockEnv {
            number: parent.number + 1,
            beneficiary: payload.beneficiary,
            timestamp: payload.timestamp,
            // difficulty is useful for post-execution, but executed with ZERO
            // difficulty: U256::ZERO,
            difficulty: U256::from(payload.batch_index),
            prevrandao: Some(payload.prev_randao()),
            gas_limit: payload.gas_limit,
            basefee: payload.base_fee_per_gas,
            blob_excess_gas_and_price,
        };

        let evm_env = EvmEnv::new(cfg, block_env);

        Ok(evm_env)
    }

    fn context_for_block<'a>(
        &self,
        block: &'a SealedBlock<BlockTy<Self::Primitives>>,
    ) -> ExecutionCtxFor<'a, Self> {
        // extra data is default otherwise it contains the hashed bls signature
        let close_epoch = if block.extra_data == Bytes::default() {
            None
        } else {
            Some(B256::from_slice(block.extra_data.as_ref()))
        };

        TNBlockExecutionCtx {
            parent_hash: block.header().parent_hash,
            parent_beacon_block_root: block.header().parent_beacon_block_root,
            nonce: block.nonce.into(),
            requests_hash: block.requests_hash,
            close_epoch,
        }
    }

    fn context_for_next_block(
        &self,
        parent: &SealedHeader<HeaderTy<Self::Primitives>>,
        payload: Self::NextBlockEnvCtx,
    ) -> ExecutionCtxFor<'_, Self> {
        TNBlockExecutionCtx {
            parent_hash: parent.hash(),
            parent_beacon_block_root: payload.parent_beacon_block_root(),
            nonce: payload.nonce,
            requests_hash: payload.batch_digest,
            close_epoch: payload.close_epoch,
        }
    }

    // type DefaultExternalContext<'a> = ();

    // fn evm<DB: Database>(&self, db: DB) -> Evm<'_, Self::DefaultExternalContext<'_>, DB> {
    //     let mut evm = self.inner.evm(db);
    //     self.set_base_fee_handler(&mut evm);
    //     evm
    // }

    // fn evm_with_env<DB: Database>(
    //     &self,
    //     db: DB,
    //     env: EnvWithHandlerCfg,
    // ) -> Evm<'_, Self::DefaultExternalContext<'_>, DB> {
    //     let mut evm = self.evm(db);
    //     evm.modify_spec_id(env.spec_id());
    //     evm.context.evm.env = env.env;
    //     self.set_base_fee_handler(&mut evm);
    //     evm
    // }

    // fn default_external_context<'a>(&self) -> Self::DefaultExternalContext<'a> {
    //     self.inner.default_external_context()
    // }
}
