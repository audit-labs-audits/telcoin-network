//! Payload builder function for extending the canonical tip using the output from consensus.
//!
//! This approach heavily inspired by reth's `default_ethereum_payload_builder`.

use reth_evm::ConfigureEvm;
use reth_payload_builder::error::PayloadBuilderError;
use reth_provider::StateProviderFactory;
use reth_revm::{database::StateProviderDatabase, State};
use tn_types::{BuildArguments, TNPayload, TNPayloadAttributes};
use tracing::debug;

/// Constructs an Ethereum transaction payload using the best transactions from the pool.
///
/// Given build arguments including an Ethereum client, transaction pool,
/// and configuration, this function creates a transaction payload. Returns
/// a result indicating success with the payload or an error in case of failure.
#[inline]
fn execute_consensus_output<EvmConfig, Provider>(
    evm_config: EvmConfig,
    args: BuildArguments<Provider>,
) -> Result<(), PayloadBuilderError>
where
    EvmConfig: ConfigureEvm,
    Provider: StateProviderFactory,
{
    let BuildArguments { provider, output, parent_block, chain_spec } = args;

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
    let state_provider = provider.state_by_block_hash(parent_block.hash())?;
    let state = StateProviderDatabase::new(state_provider);
    let mut db = State::builder().with_database(state).with_bundle_update().build();

    // TODO: use extra data for consensus output hash?
    // let extra_data = config.extra_data();
    // let PayloadConfig {
    //     initialized_block_env,
    //     initialized_cfg,
    //     parent_block,
    //     attributes,
    //     chain_spec,
    //     ..
    // } = config;

    debug!(target: "payload_builder", parent_hash = ?parent_block.hash(), parent_number = parent_block.number, "building new payload");
    let mut total_gas_used = 0;
    let mut cumulative_gas_used = 0;
    let mut sum_blob_gas_used = 0;

    // TODO: use flat_map() here?
    for (batch_index, batch) in output.batches.flatten().iter().enumerate() {
        let payload_attributes =
            TNPayloadAttributes::new(&output, batch, batch_index, parent_block);
        let payload = TNPayload::new();

        // let block_gas_limit: u64 = initialized_block_env.gas_limit.try_into().unwrap_or(u64::MAX);
        // let base_fee = initialized_block_env.basefee.to::<u64>();

        // let mut executed_txs = Vec::new();
    }

    Ok(())
}
