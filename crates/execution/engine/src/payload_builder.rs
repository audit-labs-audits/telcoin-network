//! Payload builder function for extending the canonical tip using the output from consensus.
//!
//! This approach heavily inspired by reth's `default_ethereum_payload_builder`.

use fastcrypto::hash::Hash as _;
use reth_blockchain_tree::{BlockValidationKind, BlockchainTreeEngine};
use reth_chainspec::ChainSpec;
use reth_evm::ConfigureEvm;
use reth_execution_types::ExecutionOutcome;
use reth_node_api::PayloadBuilderAttributes as _;
use reth_payload_builder::{database::CachedReads, error::PayloadBuilderError};
use reth_primitives::{
    constants::{eip4844::MAX_DATA_GAS_PER_BLOCK, EMPTY_WITHDRAWALS},
    proofs,
    revm::env::tx_env_with_recovered,
    Block, BlockNumHash, Bytes, Header, Receipt, Receipts, SealedBlock, SealedBlockWithSenders,
    SealedHeader, TransactionSigned, TransactionSignedEcRecovered, EMPTY_OMMER_ROOT_HASH, U256,
};
use reth_provider::{CanonChainTracker, ChainSpecProvider, StateProviderFactory};
use reth_revm::{
    database::StateProviderDatabase,
    db::states::bundle_state::BundleRetention,
    primitives::{EVMError, EnvWithHandlerCfg, ResultAndState},
    DatabaseCommit, State,
};
use std::{collections::VecDeque, sync::Arc};
use tn_types::{
    Batch, BatchAPI as _, BatchDigest, BuildArguments, MetadataAPI, TNPayload, TNPayloadAttributes,
};
use tracing::{debug, error, warn};

use crate::error::{EngineResult, TnEngineError};

/// Constructs an Ethereum transaction payload using the best transactions from the pool.
///
/// Given build arguments including an Ethereum client, transaction pool,
/// and configuration, this function creates a transaction payload. Returns
/// a result indicating success with the payload or an error in case of failure.
#[inline]
pub fn execute_consensus_output<EvmConfig, Provider>(
    evm_config: EvmConfig,
    args: BuildArguments<Provider>,
) -> EngineResult<BlockNumHash>
where
    EvmConfig: ConfigureEvm,
    Provider: StateProviderFactory + ChainSpecProvider + BlockchainTreeEngine + CanonChainTracker,
{
    let BuildArguments { provider, mut output, mut parent_block } = args;

    // TODO: explore "batch-execution" concept in reth: BlockExecutorProvider trait
    //
    // TODO: ensure this is called after the previous ConsensusOutput is complete.
    // to avoid race condition of executing next round before previous round is complete.
    //
    //

    // get the latest state from the last executed batch of the previous consensus output
    //
    // TODO: in order to absolutely ensure execution completed:
    // - search provider for all blocks with previous consensus output hash
    //      - use timestamp?
    // - ensure ommers and nonce match up
    //      - ommers includes all other batches
    //      - ommers hash ensures all batches accounted for
    //      - ommers length used to get the last block in the output by nonce

    // TODO: create "sealed consensus" type that contains hash and output
    //
    // create ommers while converting Batch to SealedBlockWithSenders
    let output_digest = output.digest();

    // TODO: add this as a method on ConsensusOutput and parallelize
    let sealed_blocks_with_senders_result: Result<Vec<SealedBlockWithSenders>, _> = output
        .batches
        .iter()
        .flat_map(|batches| {
            batches.iter().map(|batch| {
                // create sealed block from batch for execution this should never fail since batches
                // are validated
                SealedBlockWithSenders::try_from(batch)
            })
        })
        .collect();

    // TODO: include this information when parallelizing?
    let ommers: Vec<Header> = output
        .batches
        .iter()
        .flat_map(|batches| {
            batches.iter().map(|batch| batch.versioned_metadata().sealed_header().header().clone())
        })
        .collect();

    // calculate ommers hash
    let ommers_root = proofs::calculate_ommers_root(&ommers);

    // unwrap result
    let sealed_blocks_with_senders = sealed_blocks_with_senders_result?;

    // assert vecs match
    assert_eq!(sealed_blocks_with_senders.len(), output.batch_digests.len());

    // use default block - updated during loop - used to update chain info after loop
    let mut next_canonical_header = SealedHeader::default();

    for (block_index, block) in sealed_blocks_with_senders.into_iter().enumerate() {
        let batch_digest =
            output.next_batch_digest().ok_or(TnEngineError::NextBatchDigestMissing)?.into();
        let payload_attributes = TNPayloadAttributes::new(
            parent_block,
            ommers.clone(),
            ommers_root,
            block_index as u64,
            batch_digest,
            &output,
            output_digest.into(),
            block,
        );
        let payload = TNPayload::new(payload_attributes);

        // execute
        let next_canonical_block =
            build_block_from_batch_payload(&evm_config, payload, &provider, provider.chain_spec())?;

        debug!(target: "execution::executor", ?next_canonical_block);

        // next steps:
        // - save block to db
        // - possible to reuse state to prevent extra call to db?
        // - set this block as parent_block
        // - handle end of loop

        // update parent for next block execution in loop
        parent_block = BlockNumHash::new(next_canonical_block.number, next_canonical_block.hash());

        // update sealed canonical header
        next_canonical_header = next_canonical_block.header.clone();

        // add block to the tree and skip state root validation
        provider
            .insert_block(next_canonical_block, BlockValidationKind::SkipStateRootValidation)?;
    }

    // make all blocks canonical, commit them to the database, and broadcast on `canon_state_notification_sender`
    provider.make_canonical(parent_block.hash)?;

    //
    // see: reth/crates/consensus/beacon/src/engine/mod.rs:update_canon_chain
    //
    // set last executed header as the tracked header
    provider.set_canonical_head(next_canonical_header.clone());

    // finalize the last block executed from consensus output and update chain info
    //
    // this removes canonical blocks from the tree, but still need to set_finalized
    debug!("setting finalized block number...{:?}", parent_block.number);
    provider.finalize_block(parent_block.number)?;
    provider.set_finalized(next_canonical_header.clone());
    debug!("finalized block successful: {:?}", provider.finalized_block_num_hash());

    // update safe block
    provider.set_safe(next_canonical_header);

    // return parent num hash for next engine task
    Ok(parent_block)
}

#[inline]
fn build_block_from_batch_payload<'a, EvmConfig, Provider>(
    evm_config: &EvmConfig,
    payload: TNPayload,
    provider: &Provider,
    chain_spec: Arc<ChainSpec>,
) -> EngineResult<SealedBlockWithSenders>
where
    EvmConfig: ConfigureEvm,
    Provider: StateProviderFactory,
{
    let state_provider = provider.state_by_block_hash(payload.attributes.parent_block.hash)?;
    let state = StateProviderDatabase::new(state_provider);

    // TODO: using same apprach as reth here bc I can't find the State::builder()'s methods
    // I'm not sure what `with_bundle_update` does, and using `CachedReads` is the only way
    // I can get the state root section below to compile
    let mut cached_reads = CachedReads::default();
    let mut db =
        State::builder().with_database_ref(cached_reads.as_db(state)).with_bundle_update().build();

    debug!(target: "payload_builder", parent_hash = ?payload.attributes.parent_block.hash, parent_number = payload.attributes.parent_block.number, "building new payload");
    // collect these totals to report at the end
    let mut total_gas_used = 0;
    let mut cumulative_gas_used = 0;
    let mut total_fees = U256::ZERO;
    let mut executed_txs = Vec::new();
    let mut senders = Vec::new();
    let mut receipts = Vec::new();
    // let mut sum_blob_gas_used = 0;

    // initialize values for execution from block env
    // note: use the batch's sealed header for "parent" values
    let (cfg, block_env) =
        payload.cfg_and_block_env(chain_spec.as_ref(), payload.attributes.batch_block.header());
    let block_gas_limit: u64 = block_env.gas_limit.try_into().unwrap_or(u64::MAX);
    let base_fee = block_env.basefee.to::<u64>();
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

    // // TODO: TN needs to support this
    // // apply eip-2935 blockhashes update
    // apply_blockhashes_update(
    //     &mut db,
    //     &chain_spec,
    //     initialized_block_env.timestamp.to::<u64>(),
    //     block_number,
    //     parent_block.hash(),
    // )
    // .map_err(|err| PayloadBuilderError::Internal(err.into()))?;

    // TODO: parallelize tx recovery when it's worth it (see TransactionSigned::recover_signers())

    let txs = payload.attributes.batch_block.clone().into_transactions_ecrecovered();

    for tx in txs {
        // // TODO: support blob gas with cancun genesis hardfork
        // //
        // // There's only limited amount of blob space available per block, so we need to check if
        // // the EIP-4844 can still fit in the block
        // //
        // // note: this should never be a problem
        // if let Some(blob_tx) = tx.as_eip4844() {
        //     let tx_blob_gas = blob_tx.blob_gas();
        //     if sum_blob_gas_used + tx_blob_gas > MAX_DATA_GAS_PER_BLOCK {
        //         // this should never happen and is considered an error
        //         error!(target: "payload_builder", tx=?tx.hash, ?sum_blob_gas_used, ?tx_blob_gas,
        // "skipping blob transaction because it would exceed the max data gas per block");
        //         // TODO: should this break the process?
        //         continue;
        //     }
        // }

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
                    // allow transaction errors (ie - duplicates)
                    //
                    // it's possible that another worker's batch included this transaction
                    EVMError::Transaction(err) => {
                        warn!(target: "execution::executor", tx_hash=?tx.hash(), ?err);

                        // TODO: collect metrics here

                        continue;
                    }
                    err => {
                        // this is an error that we should treat as fatal
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

        //     // TODO: this is important for worker's batch payload builder
        //     // // if we've reached the max data gas per block, we can skip blob txs entirely
        //     // if sum_blob_gas_used == MAX_DATA_GAS_PER_BLOCK {
        //     //     best_txs.skip_blobs();
        //     // }
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

        // append transaction to the list of executed transactions and keep signers
        senders.push(tx.signer());
        executed_txs.push(tx.into_signed());
    }

    // TODO: logic for withdrawals
    //
    // let WithdrawalsOutcome { withdrawals_root, withdrawals } =
    //     commit_withdrawals(&mut db, &chain_spec, attributes.timestamp, attributes.withdrawals)?;

    // merge all transitions into bundle state, this would apply the withdrawal balance changes
    // and 4788 contract call
    db.merge_transitions(BundleRetention::PlainState);

    let execution_outcome = ExecutionOutcome::new(
        db.take_bundle(),
        vec![receipts].into(),
        block_number,
        vec![], // TODO: support requests
    );
    let receipts_root =
        execution_outcome.receipts_root_slow(block_number).expect("Number is in range");
    let logs_bloom = execution_outcome.block_logs_bloom(block_number).expect("Number is in range");

    // calculate the state root
    let state_root = {
        let state_provider = db.database.0.inner.borrow_mut();
        state_provider.db.state_root(execution_outcome.state())?
    };

    // create the block header
    let transactions_root = reth_primitives::proofs::calculate_transaction_root(&executed_txs);

    // // initialize empty blob sidecars at first. If cancun is active then this will
    // let mut blob_sidecars = Vec::new();
    // let mut excess_blob_gas = None;
    // let mut blob_gas_used = None;

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
        parent_hash: payload.parent(),
        ommers_hash: payload.attributes.ommers_root,
        beneficiary: block_env.coinbase,
        state_root,
        transactions_root,
        receipts_root,
        withdrawals_root: Some(EMPTY_WITHDRAWALS),
        logs_bloom,
        timestamp: payload.timestamp(),
        mix_hash: payload.prev_randao(),
        nonce: payload.attributes.batch_index,
        base_fee_per_gas: Some(base_fee),
        number: payload.attributes.parent_block.number + 1, // ensure this matches the block env
        gas_limit: block_gas_limit,
        difficulty: U256::from(payload.attributes.batch_index),
        gas_used: cumulative_gas_used,
        extra_data: payload.attributes.batch_digest.into(),
        parent_beacon_block_root: payload.parent_beacon_block_root(),
        blob_gas_used: None,   // TODO: support blobs
        excess_blob_gas: None, // TODO: support blobs
        requests_root: None,
    };

    // seal the block
    let withdrawals = Some(payload.withdrawals().clone());
    let block = Block {
        header,
        body: executed_txs,
        ommers: payload.attributes.ommers,
        withdrawals,
        requests: None,
    };

    let sealed_block = block.seal_slow();
    debug!(target: "payload_builder", ?sealed_block, "sealed built block");

    let sealed_block_with_senders = SealedBlockWithSenders::new(sealed_block, senders)
        .ok_or(TnEngineError::SealBlockWithSenders)?;

    Ok(sealed_block_with_senders)
}
