//! Payload builder function for extending the canonical tip using the output from consensus.
//!
//! This approach heavily inspired by reth's `default_ethereum_payload_builder`.

use reth_evm::ConfigureEvm;
use reth_node_api::PayloadBuilderAttributes as _;
use reth_payload_builder::{database::CachedReads, error::PayloadBuilderError};
use reth_primitives::{
    constants::EMPTY_WITHDRAWALS, proofs, revm::env::tx_env_with_recovered, Block, ChainSpec,
    Header, Receipt, Receipts, SealedBlock, SealedBlockWithSenders, TransactionSigned,
    TransactionSignedEcRecovered, EMPTY_OMMER_ROOT_HASH, U256,
};
use reth_provider::{BundleStateWithReceipts, StateProviderFactory};
use reth_revm::{
    database::StateProviderDatabase,
    db::states::bundle_state::BundleRetention,
    primitives::{EVMError, EnvWithHandlerCfg, ResultAndState},
    DatabaseCommit, State,
};
use std::sync::Arc;
use tn_types::{Batch, BatchAPI as _, BuildArguments, MetadataAPI, TNPayload, TNPayloadAttributes};
use tracing::{debug, error, warn};

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

    // let flat_batches: Vec<Batch> = output.clone().batches.into_iter().flatten().collect();

    // create ommers while converting Batch to SealedBlockWithSenders
    let mut ommers = Vec::new();
    let output_digest = output.digest();

    // TODO: add this as a method on ConsensusOutput and parallelize
    let sealed_blocks_with_senders: Result<Vec<SealedBlockWithSenders>, _> = output
        .batches
        .iter()
        .flat_map(|batches| {
            // try convert batch to sealed block
            //
            // this should never fail since batches are validated
            batches.iter().map(|batch| {
                // collect headers for ommers while looping through each batch
                //
                // TODO: is there a better way to do this?
                ommers.push(batch.versioned_metadata().sealed_header().header().clone());
                SealedBlockWithSenders::try_from(batch)
            })
        })
        .collect();

    // calculate ommers hash
    let ommers_root = proofs::calculate_ommers_root(&ommers);

    for (block_index, block) in sealed_blocks_with_senders?.into_iter().enumerate() {
        let payload_attributes = TNPayloadAttributes::new(
            &output,
            block,
            block_index as u64,
            &parent_block,
            ommers.clone(),
            ommers_root,
        );
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
    let mut db = State::builder()
        .with_database_ref(CachedReads::default().as_db(state))
        .with_bundle_update()
        .build();

    debug!(target: "payload_builder", parent_hash = ?parent_block.hash(), parent_number = parent_block.number, "building new payload");
    let mut total_gas_used = 0;
    let mut cumulative_gas_used = 0;
    let mut sum_blob_gas_used = 0;

    let (cfg, block_env) = payload.cfg_and_block_env(chain_spec.as_ref(), parent_block.header());
    let block_gas_limit: u64 = block_env.gas_limit.try_into().unwrap_or(u64::MAX);
    let base_fee = block_env.basefee.to::<u64>();

    let mut executed_txs = Vec::new();

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

    let mut receipts = Vec::new();

    // TODO: parallelize tx recovery when it's worth it (see TransactionSigned::recover_signers())

    // let batch_txs: Result<Vec<TransactionSignedEcRecovered>, _> = payload
    //     .attributes
    //     .batch
    //     .transactions_owned()
    //     .map(|tx_bytes| {
    //         // batches must be validated by this point,
    //         // so encoding and decoding has already happened
    //         // and is not expected to fail
    //         TransactionSigned::decode_enveloped(&mut tx_bytes.as_ref()).map_err(|e| {
    //             error!(target: "execution::executor", "Failed to decode enveloped tx: {tx_bytes:?}");
    //             ExecutorError::DecodeTransaction(e)
    //         })
    //         .expect("batch already validated")
    //         .try_into_ecrecovered()
    //     })
    //     .collect();

    // let batch_txs = batch_txs.expect("batch valid");
    // let recovered_batch_txs = TransactionSigned::recover_signers(&batch_txs, batch_txs.len());

    let txs = payload.attributes.block.clone().into_transactions_ecrecovered();

    for tx in txs {
        // TODO: support blob gas
        //
        // // There's only limited amount of blob space available per block, so we need to check if
        // // the EIP-4844 can still fit in the block
        // if let Some(blob_tx) = tx.as_eip4844() {
        //     let tx_blob_gas = blob_tx.blob_gas();
        //     if sum_blob_gas_used + tx_blob_gas > MAX_DATA_GAS_PER_BLOCK {
        //         // we can't fit this _blob_ transaction into the block, so we mark it as
        //         // invalid, which removes its dependent transactions from
        //         // the iterator. This is similar to the gas limit condition
        //         // for regular transactions above.
        //         trace!(target: "payload_builder", tx=?tx.hash, ?sum_blob_gas_used, ?tx_blob_gas, "skipping blob transaction because it would exceed the max data gas per block");
        //         best_txs.mark_invalid(&pool_tx);
        //         continue;
        //     }
        // }

        //
        let env = EnvWithHandlerCfg::new_with_cfg_env(
            cfg.clone(),
            block_env.clone(),
            tx_env_with_recovered(&tx),
        );

        // Configure the environment for the block.
        let mut evm = evm_config.evm_with_env(&mut db, env);

        let ResultAndState { result, state } = match evm.transact() {
            Ok(res) => res,
            Err(err) => {
                match err {
                    EVMError::Transaction(err) => {
                        warn!(target: "execution::executor", tx_hash=?tx.hash(), ?err);

                        continue;
                    }
                    err => {
                        // this is an error that we should treat as fatal for this attempt
                        // - invalid header resulting from misconfigured BlockEnv
                        // - Database error
                        // - custom error (unsure)
                        return Err(err.into());
                    }
                }
            }
        };
        // drop evm so db is released.
        drop(evm);
        // commit changes
        db.commit(state);

        // // add to the total blob gas used if the transaction successfully executed
        // if let Some(blob_tx) = tx.transaction.as_eip4844() {
        //     let tx_blob_gas = blob_tx.blob_gas();
        //     sum_blob_gas_used += tx_blob_gas;

        //     // if we've reached the max data gas per block, we can skip blob txs entirely
        //     if sum_blob_gas_used == MAX_DATA_GAS_PER_BLOCK {
        //         best_txs.skip_blobs();
        //     }
        // }

        let gas_used = result.gas_used();

        // add gas used by the transaction to cumulative gas used, before creating the receipt
        cumulative_gas_used += gas_used;

        // Push transaction changeset and calculate header bloom filter for receipt.
        receipts.push(Some(Receipt {
            tx_type: tx.tx_type(),
            success: result.is_success(),
            cumulative_gas_used,
            logs: result.into_logs().into_iter().map(Into::into).collect(),
        }));

        // update add to total fees
        let miner_fee = tx
            .effective_tip_per_gas(Some(base_fee))
            .expect("fee is always valid; execution succeeded");
        total_fees += U256::from(miner_fee) * U256::from(gas_used);

        // append transaction to the list of executed transactions
        executed_txs.push(tx.into_signed());
    }

    // TODO: logic for requests, withdrawals

    // merge all transitions into bundle state, this would apply the withdrawal balance changes
    // and 4788 contract call
    db.merge_transitions(BundleRetention::PlainState);

    let bundle = BundleStateWithReceipts::new(
        db.take_bundle(),
        Receipts::from_vec(vec![receipts]),
        block_number,
    );
    let receipts_root = bundle.receipts_root_slow(block_number).expect("Number is in range");
    let logs_bloom = bundle.block_logs_bloom(block_number).expect("Number is in range");

    // calculate the state root
    let state_root = {
        let state_provider = db.database.0.inner.borrow_mut();
        state_provider.db.state_root(bundle.state())?
    };

    // create the block header
    let transactions_root = reth_primitives::proofs::calculate_transaction_root(&executed_txs);

    // initialize empty blob sidecars at first. If cancun is active then this will
    let mut blob_sidecars = Vec::new();
    let mut excess_blob_gas = None;
    let mut blob_gas_used = None;

    // // only determine cancun fields when active
    // if chain_spec.is_cancun_active_at_timestamp(payload.attributes.timestamp) {
    //     // grab the blob sidecars from the executed txs
    //     blob_sidecars = pool.get_all_blobs_exact(
    //         executed_txs.iter().filter(|tx| tx.is_eip4844()).map(|tx| tx.hash).collect(),
    //     )?;

    //     excess_blob_gas = if chain_spec.is_cancun_active_at_timestamp(parent_block.timestamp) {
    //         let parent_excess_blob_gas = parent_block.excess_blob_gas.unwrap_or_default();
    //         let parent_blob_gas_used = parent_block.blob_gas_used.unwrap_or_default();
    //         Some(calculate_excess_blob_gas(parent_excess_blob_gas, parent_blob_gas_used))
    //     } else {
    //         // for the first post-fork block, both parent.blob_gas_used and
    //         // parent.excess_blob_gas are evaluated as 0
    //         Some(calculate_excess_blob_gas(0, 0))
    //     };

    //     blob_gas_used = Some(sum_blob_gas_used);
    // }

    let header = Header {
        parent_hash: parent_block.hash(),
        ommers_hash: EMPTY_OMMER_ROOT_HASH,
        beneficiary: block_env.coinbase,
        state_root,
        transactions_root,
        receipts_root,
        withdrawals_root: Some(EMPTY_WITHDRAWALS),
        logs_bloom,
        timestamp: payload.attributes.timestamp,
        mix_hash: todo!(),
        nonce: payload.attributes.block_index,
        base_fee_per_gas: Some(base_fee),
        number: parent_block.number + 1,
        gas_limit: block_gas_limit,
        difficulty: U256::ZERO,
        gas_used: cumulative_gas_used,
        extra_data: todo!(),
        parent_beacon_block_root: todo!(),
        blob_gas_used,
        excess_blob_gas,
        requests_root: None,
    };

    // seal the block
    let block = Block { header, body: executed_txs, ommers: vec![], withdrawals, requests };

    let sealed_block = block.seal_slow();
    debug!(target: "payload_builder", ?sealed_block, "sealed built block");

    Ok(())
}
