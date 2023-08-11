use std::sync::Arc;

use execution_payload_builder::{BuiltPayload, database::CachedReads, error::PayloadBuilderError};
use execution_provider::{PostState, StateProviderFactory};
use execution_revm::{executor::{increment_account_balance, post_block_withdrawals_balance_increments, commit_state_changes}, database::State, into_execution_log, env::tx_env_with_recovered};
use execution_transaction_pool::TransactionPool;
use revm::{db::{CacheDB, DatabaseRef}, primitives::{ResultAndState, InvalidTransaction, EVMError, Env}};
use tn_types::execution::{
    proofs, ChainSpec, Withdrawal, U256, Receipt, IntoRecoveredTransaction,
};
use tokio::sync::oneshot;
use tracing::{debug, trace};

use super::job::{WithdrawalsOutcome, PayloadConfig, Cancelled, BuildOutcome};

/// Builds the next batch by iterating over the best pending transactions.
pub(super) fn build_payload<Pool, Client>(
    client: Client,
    pool: Pool,
    cached_reads: CachedReads,
    config: PayloadConfig,
    cancel: Cancelled,
    best_payload: Option<Arc<BuiltPayload>>,
    to_job: oneshot::Sender<Result<BuildOutcome, PayloadBuilderError>>,
) where
    Client: StateProviderFactory,
    Pool: TransactionPool,
{
    #[inline(always)]
    fn try_build<Pool, Client>(
        client: Client,
        pool: Pool,
        mut cached_reads: CachedReads,
        config: PayloadConfig,
        cancel: Cancelled,
        best_payload: Option<Arc<BuiltPayload>>,
    ) -> Result<BuildOutcome, PayloadBuilderError>
    where
        Client: StateProviderFactory,
        Pool: TransactionPool,
    {
        let PayloadConfig {
            initialized_block_env,
            initialized_cfg,
            parent_block,
            extra_data,
            attributes,
            chain_spec,
        } = config;

        debug!(parent_hash=?parent_block.hash, parent_number=parent_block.number, "building new payload");

        let state = State::new(client.state_by_block_hash(parent_block.hash)?);
        let mut db = CacheDB::new(cached_reads.as_db(&state));
        let mut post_state = PostState::default();

        let mut cumulative_gas_used = 0;
        let block_gas_limit: u64 = initialized_block_env.gas_limit.try_into().unwrap_or(u64::MAX);

        let mut executed_txs = Vec::new();
        let mut best_txs = pool.best_transactions();

        let mut total_fees = U256::ZERO;
        let base_fee = initialized_block_env.basefee.to::<u64>();

        let block_number = initialized_block_env.number.to::<u64>();

        while let Some(pool_tx) = best_txs.next() {
            // ensure we still have capacity for this transaction
            if cumulative_gas_used + pool_tx.gas_limit() > block_gas_limit {
                // we can't fit this transaction into the block, so we need to mark it as invalid
                // which also removes all dependent transaction from the iterator before we can
                // continue
                best_txs.mark_invalid(&pool_tx);
                continue
            }

            // check if the job was cancelled, if so we can exit early
            if cancel.is_cancelled() {
                return Ok(BuildOutcome::Cancelled)
            }

            // convert tx to a signed transaction
            let tx = pool_tx.to_recovered_transaction();

            // Configure the environment for the block.
            let env = Env {
                cfg: initialized_cfg.clone(),
                block: initialized_block_env.clone(),
                tx: tx_env_with_recovered(&tx),
            };

            let mut evm = revm::EVM::with_env(env);
            evm.database(&mut db);

            let ResultAndState { result, state } = match evm.transact() {
                Ok(res) => res,
                Err(err) => {
                    match err {
                        EVMError::Transaction(err) => {
                            if matches!(err, InvalidTransaction::NonceTooLow { .. }) {
                                // if the nonce is too low, we can skip this transaction
                                trace!(?err, ?tx, "skipping nonce too low transaction");
                            } else {
                                // if the transaction is invalid, we can skip it and all of its
                                // descendants
                                trace!(
                                    ?err,
                                    ?tx,
                                    "skipping invalid transaction and its descendants"
                                );
                                best_txs.mark_invalid(&pool_tx);
                            }
                            continue
                        }
                        err => {
                            // this is an error that we should treat as fatal for this attempt
                            return Err(PayloadBuilderError::EvmExecutionError(err))
                        }
                    }
                }
            };

            let gas_used = result.gas_used();

            // commit changes
            commit_state_changes(&mut db, &mut post_state, block_number, state, true);

            // add gas used by the transaction to cumulative gas used, before creating the receipt
            cumulative_gas_used += gas_used;

            // Push transaction changeset and calculate header bloom filter for receipt.
            post_state.add_receipt(
                block_number,
                Receipt {
                    tx_type: tx.tx_type(),
                    success: result.is_success(),
                    cumulative_gas_used,
                    logs: result.logs().into_iter().map(into_execution_log).collect(),
                },
            );

            // update add to total fees
            let miner_fee = tx
                .effective_tip_per_gas(base_fee)
                .expect("fee is always valid; execution succeeded");
            total_fees += U256::from(miner_fee) * U256::from(gas_used);

            // append transaction to the list of executed transactions
            executed_txs.push(tx.into_signed());
        }

        // check if we have a better block
        if !is_better_payload(best_payload.as_deref(), total_fees) {
            // can skip building the block
            return Ok(BuildOutcome::Aborted { fees: total_fees, cached_reads })
        }

        let WithdrawalsOutcome { withdrawals_root, withdrawals } = commit_withdrawals(
            &mut db,
            &mut post_state,
            &chain_spec,
            block_number,
            attributes.timestamp,
            attributes.withdrawals,
        )?;

        let receipts_root = post_state.receipts_root(block_number);
        let logs_bloom = post_state.logs_bloom(block_number);

        // calculate the state root
        let state_root = state.state().state_root(post_state)?;

        // create the block header
        let transactions_root = proofs::calculate_transaction_root(&executed_txs);

        // TODO: don't return a header.
        // the batch only needs Vec<Bytes>
        //
        // let header = Header {
        //     parent_hash: parent_block.hash,
        //     ommers_hash: EMPTY_OMMER_ROOT,
        //     beneficiary: initialized_block_env.coinbase,
        //     state_root,
        //     transactions_root,
        //     receipts_root,
        //     withdrawals_root,
        //     logs_bloom,
        //     timestamp: attributes.timestamp,
        //     mix_hash: attributes.prev_randao,
        //     nonce: BEACON_NONCE,
        //     base_fee_per_gas: Some(base_fee),
        //     number: parent_block.number + 1,
        //     gas_limit: block_gas_limit,
        //     difficulty: U256::ZERO,
        //     gas_used: cumulative_gas_used,
        //     extra_data: extra_data.into(),
        // };

        // seal the block
        // let block = Block { header, body: executed_txs, ommers: vec![], withdrawals };

        // let sealed_block = block.seal_slow();
        // Ok(BuildOutcome::Better {
        //     payload: BuiltPayload::new(attributes.id, sealed_block, total_fees),
        //     cached_reads,
        // })
        todo!()
    }

    let _ = to_job.send(try_build(client, pool, cached_reads, config, cancel, best_payload));
}

/// Executes the withdrawals and commits them to the _runtime_ Database and PostState.
///
/// Returns the withdrawals root.
///
/// Returns `None` values pre shanghai
#[allow(clippy::too_many_arguments)]
pub(super) fn commit_withdrawals<DB>(
    db: &mut CacheDB<DB>,
    post_state: &mut PostState,
    chain_spec: &ChainSpec,
    block_number: u64,
    timestamp: u64,
    withdrawals: Vec<Withdrawal>,
) -> Result<WithdrawalsOutcome, <DB as DatabaseRef>::Error>
where
    DB: DatabaseRef,
{
    if !chain_spec.is_shanghai_activated_at_timestamp(timestamp) {
        return Ok(WithdrawalsOutcome::pre_shanghai())
    }

    if withdrawals.is_empty() {
        return Ok(WithdrawalsOutcome::empty())
    }

    let balance_increments =
        post_block_withdrawals_balance_increments(chain_spec, timestamp, &withdrawals);

    for (address, increment) in balance_increments {
        increment_account_balance(db, post_state, block_number, address, increment)?;
    }

    let withdrawals_root = proofs::calculate_withdrawals_root(&withdrawals);

    // calculate withdrawals root
    Ok(WithdrawalsOutcome {
        withdrawals: Some(withdrawals),
        withdrawals_root: Some(withdrawals_root),
    })
}

/// Checks if the new payload is better than the current best.
///
/// This compares the total fees of the blocks, higher is better.
#[inline(always)]
pub(super) fn is_better_payload(best_payload: Option<&BuiltPayload>, new_fees: U256) -> bool {
    if let Some(best_payload) = best_payload {
        new_fees > best_payload.fees()
    } else {
        true
    }
}
