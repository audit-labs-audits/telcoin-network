//! Payload builder function for extending the canonical tip using the output from consensus.
//!
//! This approach heavily inspired by reth's `default_ethereum_payload_builder`.

use std::sync::Arc;

use reth_evm::ConfigureEvm;
use reth_node_api::PayloadBuilderAttributes as _;
use reth_payload_builder::error::PayloadBuilderError;
use reth_primitives::{ChainSpec, SealedBlock, TransactionSigned, U256};
use reth_provider::StateProviderFactory;
use reth_revm::{database::StateProviderDatabase, State};
use tn_types::{Batch, BatchAPI as _, BuildArguments, TNPayload, TNPayloadAttributes};
use tracing::{debug, error};

use crate::error::ExecutorError;

/// Constructs an Ethereum transaction payload using the best transactions from the pool.
///
/// Given build arguments including an Ethereum client, transaction pool,
/// and configuration, this function creates a transaction payload. Returns
/// a result indicating success with the payload or an error in case of failure.
#[inline]
fn execute_consensus_output<EvmConfig, Provider>(
    evm_config: EvmConfig,
    args: BuildArguments<Provider>,
) -> eyre::Result<()>
where
    EvmConfig: ConfigureEvm,
    Provider: StateProviderFactory,
{
    let BuildArguments { provider, output, mut parent_block, chain_spec } = args;

    //
    //
    // TODO: ensure this is called after the previous ConsensusOutput is complete.
    // to avoid race condition of executing next round before previous round is complete.
    //
    //

    // get the latest state from the last executed batch of the previous consensus output
    //
    // TODO: in order to absolutely ensure execution completed:
    // - search provider for all blocks with previous consensus output hash
    // - ensure ommers and nonce match up
    //      - ommers includes all other batches
    //      - ommers hash ensures all batches accounted for
    //      - ommers length used to get the last block in the output by nonce

    let flat_batches: Vec<Batch> = output.clone().batches.into_iter().flatten().collect();

    // TODO: use flat_map() here?
    for (batch_index, batch) in flat_batches.iter().enumerate() {
        let payload_attributes =
            TNPayloadAttributes::new(&output, batch, batch_index, &parent_block);
        let payload = TNPayload::try_new(parent_block.hash(), payload_attributes)?;

        build_block_from_batch_payload(
            &evm_config,
            payload,
            &parent_block,
            &provider,
            chain_spec.clone(),
        )?;
        // let block_gas_limit: u64 = initialized_block_env.gas_limit.try_into().unwrap_or(u64::MAX);
        // let base_fee = initialized_block_env.basefee.to::<u64>();

        // let mut executed_txs = Vec::new();
    }

    Ok(())
}

#[inline]
fn build_block_from_batch_payload<'a, EvmConfig, Provider>(
    evm_config: &EvmConfig,
    payload: TNPayload<'a>,
    parent_block: &SealedBlock,
    provider: &Provider,
    chain_spec: Arc<ChainSpec>,
) -> eyre::Result<()>
where
    EvmConfig: ConfigureEvm,
    Provider: StateProviderFactory,
{
    let state_provider = provider.state_by_block_hash(parent_block.hash())?;
    let state = StateProviderDatabase::new(state_provider);
    let mut db = State::builder().with_database(state).with_bundle_update().build();

    debug!(target: "payload_builder", parent_hash = ?parent_block.hash(), parent_number = parent_block.number, "building new payload");
    let mut total_gas_used = 0;
    let mut cumulative_gas_used = 0;
    let mut sum_blob_gas_used = 0;

    let (cfg, block_env) = payload.cfg_and_block_env(chain_spec.as_ref(), parent_block.header());
    let block_gas_limit: u64 = block_env.gas_limit.try_into().unwrap_or(u64::MAX);
    let base_fee = block_env.basefee.to::<u64>();

    // let mut executed_txs = Vec::new();

    let mut total_fees = U256::ZERO;

    let block_number = block_env.number.to::<u64>();

    // // apply eip-4788 pre block contract call
    // pre_block_beacon_root_contract_call(
    //     &mut db,
    //     &chain_spec,
    //     block_number,
    //     &initialized_cfg,
    //     &initialized_block_env,
    //     &attributes,
    // )?;

    // // apply eip-2935 blockhashes update
    // apply_blockhashes_update(
    //     &mut db,
    //     &chain_spec,
    //     initialized_block_env.timestamp.to::<u64>(),
    //     block_number,
    //     parent_block.hash(),
    // )
    // .map_err(|err| PayloadBuilderError::Internal(err.into()))?;

    // let mut receipts = Vec::new();

    // TODO: parallelize tx recovery when it's worth it (see reth blockchain_tree)

    let batch_txs: Result<Vec<TransactionSigned>, _> = payload
        .attributes
        .batch
        .transactions_owned()
        .map(|tx_bytes| {
            // batches must be validated by this point,
            // so encoding and decoding has already happened
            // and is not expected to fail
            TransactionSigned::decode_enveloped(&mut tx_bytes.as_ref()).map_err(|e| {
                error!(target: "execution::executor", "Failed to decode enveloped tx: {tx_bytes:?}");
                ExecutorError::DecodeTransaction(e)
            })
        })
        .collect();

    Ok(())
}
