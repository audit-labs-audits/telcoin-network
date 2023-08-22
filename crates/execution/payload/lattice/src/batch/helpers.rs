use std::task::Waker;
use execution_payload_builder::database::CachedReads;
use execution_provider::{PostState, StateProviderFactory};
use execution_revm::{executor::commit_state_changes, database::State, into_execution_log, env::tx_env_with_recovered};
use execution_transaction_pool::TransactionPool;
use revm::{db::CacheDB, primitives::{ResultAndState, InvalidTransaction, EVMError, Env}};
use tn_types::execution::{
    U256, Receipt, IntoRecoveredTransaction,
};
use tokio::sync::oneshot;
use tracing::{debug, warn};
use crate::batch::{BatchBuilderError, metrics::PayloadSizeMetric};

use super::{job::{BatchPayloadConfig, Cancelled}, generator::BuiltBatch};

/// Builds the next batch by iterating over the best pending transactions.
pub(super) fn create_batch<Pool, Client>(
    client: Client,
    pool: Pool,
    cached_reads: CachedReads,
    config: BatchPayloadConfig,
    cancel: Cancelled,
    to_job: oneshot::Sender<Result<BuiltBatch, BatchBuilderError>>,
    waker: Waker,
) //-> Result<(Vec<TransactionId>, Vec<Vec<u8>>), PayloadBuilderError>
where
    Client: StateProviderFactory,
    Pool: TransactionPool,
{
    #[inline(always)]
    fn try_build<Pool, Client>(
        client: Client,
        pool: Pool,
        mut cached_reads: CachedReads,
        config: BatchPayloadConfig,
        _cancel: Cancelled, // TODO: can cancel be used to prevent batches while processing consensus output?
    ) -> Result<BuiltBatch, BatchBuilderError>
    where
        Client: StateProviderFactory,
        Pool: TransactionPool,
    {
        let BatchPayloadConfig {
            initialized_block_env,
            initialized_cfg,
            parent_block,
            max_batch_size,
        } = config;

        debug!(parent_hash=?parent_block.hash, parent_number=parent_block.number, "building new payload");

        let state = State::new(client.state_by_block_hash(parent_block.hash)?);
        let mut db = CacheDB::new(cached_reads.as_db(&state));
        let mut post_state = PostState::default();

        let mut cumulative_gas_used = 0;
        let mut payload_size = 0;
        let mut size_metric = PayloadSizeMetric::default();
        let block_gas_limit: u64 = initialized_block_env.gas_limit.try_into().unwrap_or(u64::MAX);

        let mut executed_txs = Vec::new();
        let mut batch = Vec::new();

        let mut best_txs = pool.best_transactions();
        let mut total_fees = U256::ZERO;

        // TODO: where should the base fee come from?
        let base_fee = initialized_block_env.basefee.to::<u64>();

        let block_number = initialized_block_env.number.to::<u64>();

        while let Some(pool_tx) = best_txs.next() {
            // TODO: is gas or batch size more likely to be reached first?
            // whichever it tends to be should be the first "size" checked.

            // ensure we still have gas capacity for this transaction
            if cumulative_gas_used + pool_tx.gas_limit() > block_gas_limit {
                // we can't fit this transaction into the block, so we need to mark it as invalid
                // which also removes all dependent transaction from the iterator before we can
                // continue
                best_txs.mark_invalid(&pool_tx);
                size_metric = PayloadSizeMetric::GasLimit;
                continue
            }

            let tx_size = pool_tx.size();
            // ensure we have memory capacity for the transaction
            if payload_size + tx_size > max_batch_size {
                best_txs.mark_invalid(&pool_tx);
                size_metric = PayloadSizeMetric::MaxBatchSize;
                continue
            } 

            let tx_id = pool_tx.transaction_id;

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
                                warn!(?err, ?tx, "skipping nonce too low transaction");
                            } else {
                                // if the transaction is invalid, we can skip it and all of its
                                // descendants
                                warn!(
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
                            warn!("EVM Fatal error - returning empty batch.");
                            return Err(BatchBuilderError::EvmExecutionError(err))
                        }
                    }
                }
            };

            let gas_used = result.gas_used();

            // commit changes
            commit_state_changes(&mut db, &mut post_state, block_number, state, true);

            // update payload's size
            payload_size += tx_size;
            // add gas used by the transaction to cumulative gas used, before creating the receipt
            cumulative_gas_used += gas_used;

            // TODO: this may not be needed to verify transaction validity for batches
            //
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

            // // update add to total fees
            // let miner_fee = tx
            //     .effective_tip_per_gas(base_fee)
            //     .expect("fee is always valid; execution succeeded");
            // total_fees += U256::from(miner_fee) * U256::from(gas_used);

            // append transaction to the list of executed transactions
            let tx_bytes: Vec<u8> = tx.into_signed().envelope_encoded().into();
            executed_txs.push(tx_id);
            // TODO: does this need to be ordered?
            batch.push(tx_bytes);
        }
        // check withdrawals at block level, not batch

        // return an error if the batch is empty so the worker doesn't seal
        // an empty batch
        if batch.is_empty() {
            return Err(BatchBuilderError::EmptyBatch)
        }

        Ok(BuiltBatch::new(batch, executed_txs, size_metric))
    }

    // return the result to the batch building job
    let _ = to_job.send(try_build(client, pool, cached_reads, config, cancel));

    // match to_job.send(try_build(client, pool, cached_reads, config, cancel)) {
    //     Ok(_) => debug!("\n\n~~~~ create_batch() is finished!!\n"),
    //     Err(_) => debug!("\n\n~~~~ ERROR ~~~~ in create_batch() sending\n"),
    // }

    // call wake() to poll job again
    waker.wake();
}
