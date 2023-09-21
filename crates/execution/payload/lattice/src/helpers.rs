use std::{task::Waker, sync::Arc, collections::HashMap};
use anemo::types::Version;
use execution_payload_builder::database::CachedReads;
use execution_provider::{PostState, StateProviderFactory};
use execution_revm::{executor::{commit_state_changes, post_block_withdrawals_balance_increments, increment_account_balance}, database::State, into_execution_log, env::tx_env_with_recovered};
use execution_rlp::Decodable;
use execution_tasks::TaskSpawner;
use execution_transaction_pool::{TransactionPool, ValidPoolTransaction};
use fastcrypto::hash::Hash;
use indexmap::IndexMap;
use lattice_network::EngineToWorkerClient;
use revm::{db::{CacheDB, DatabaseRef}, primitives::{ResultAndState, InvalidTransaction, EVMError, Env}};
use tn_network_types::{SealBatchRequest, SealedBatchResponse};
use tn_types::{execution::{
    U256, Receipt, IntoRecoveredTransaction, Withdrawal, H256, constants::{EMPTY_WITHDRAWALS, BEACON_NONCE}, ChainSpec, proofs::{self, EMPTY_ROOT}, EMPTY_OMMER_ROOT, Header, Bytes, TransactionSigned, Block,
}, consensus::{WorkerId, TimestampSec, BatchDigest, Batch, VersionedMetadata, now, BatchAPI, ConsensusOutput}};
use tokio::sync::oneshot;
use tracing::{debug, warn, error};
use execution_transaction_pool::BestTransactions;
use crate::{BatchPayloadConfig, HeaderPayloadConfig, Cancelled, BatchPayload, LatticePayloadBuilderError, BatchPayloadSizeMetric, HeaderPayload, BlockPayloadConfig, BlockPayload};

/// Share the built batch with the quorum waiter for broadcasting to all peers.
pub(super) async fn seal_batch<Network>(
    network: Network,
    payload: Vec<Vec<u8>>,
    metadata: VersionedMetadata,
    tx: oneshot::Sender<Result<SealedBatchResponse, LatticePayloadBuilderError>>,
    _cancel: Cancelled,
    waker: Waker,
)
where
    Network: EngineToWorkerClient + Clone + Unpin + Send + Sync + 'static,
{
    let worker_id = 0;
    let request = SealBatchRequest { payload, metadata };
    let res = network.seal_batch(worker_id, request).await;
    let _ = tx.send(res.map_err(Into::into));
    waker.wake();
}

/// Builds the next batch by iterating over the best pending transactions.
pub(super) fn create_batch<Pool, Client>(
    client: Client,
    pool: Pool,
    cached_reads: CachedReads,
    config: BatchPayloadConfig,
    cancel: Cancelled,
    to_job: oneshot::Sender<Result<BatchPayload, LatticePayloadBuilderError>>,
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
    ) -> Result<BatchPayload, LatticePayloadBuilderError>
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
        let mut size_metric = BatchPayloadSizeMetric::default();
        let block_gas_limit: u64 = initialized_block_env.gas_limit.try_into().unwrap_or(u64::MAX);

        let mut executed_tx_ids = Vec::new();
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
                size_metric = BatchPayloadSizeMetric::GasLimit;
                continue
            }

            let tx_size = pool_tx.size();
            // ensure we have memory capacity for the transaction
            if payload_size + tx_size > max_batch_size {
                best_txs.mark_invalid(&pool_tx);
                size_metric = BatchPayloadSizeMetric::MaxBatchSize;
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
                            return Err(LatticePayloadBuilderError::EvmExecutionError(err))
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

            // TODO:
            // Peers verify reciepts when executing the batch, however this may not be
            // strictly necessary.
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

            // update add to total fees
            let miner_fee = tx
                .effective_tip_per_gas(base_fee)
                .expect("fee is always valid; execution succeeded");
            total_fees += U256::from(miner_fee) * U256::from(gas_used);

            // append transaction to the list of executed transactions
            executed_txs.push(tx.clone().into_signed());
            let tx_bytes: Vec<u8> = tx.into_signed().envelope_encoded().into();
            executed_tx_ids.push(tx_id);
            // TODO: does this need to be ordered?
            batch.push(tx_bytes);
        }
        // check withdrawals at block level, not batch

        // TODO: is this better to check on the worker's side?
        //
        // return an error if the batch is empty so the worker doesn't seal
        // an empty batch
        if batch.is_empty() {
            return Err(LatticePayloadBuilderError::EmptyBatch)
        }

        // calculate roots for peers to validate
        //
        // NOTE: this may not be strictly necessary, but it's a lot
        // easier to include this verbose data for now than it is to rewrite
        // a lot of the engine/executor validation code.
        let receipts_root = post_state.receipts_root(block_number);
        let logs_bloom = post_state.logs_bloom(block_number);

        // calculate the state root
        let state_root = state.state().state_root(post_state)?;

        // create the block header
        let transactions_root = proofs::calculate_transaction_root(&executed_txs);

        let metadata = Header {
            parent_hash: parent_block.hash,
            ommers_hash: EMPTY_OMMER_ROOT,
            beneficiary: initialized_block_env.coinbase,
            state_root,
            transactions_root,
            receipts_root,
            withdrawals_root: Some(EMPTY_ROOT),
            logs_bloom,
            timestamp: now(),
            mix_hash: H256::zero(),
            nonce: BEACON_NONCE,
            base_fee_per_gas: Some(base_fee),
            number: parent_block.number + 1,
            gas_limit: block_gas_limit,
            difficulty: U256::ZERO,
            gas_used: cumulative_gas_used,
            extra_data: Bytes::default(),
        }
        .seal_slow()
        .into();

        Ok(BatchPayload::new(batch, metadata, executed_tx_ids, size_metric))
    }

    // return the result to the batch building job
    let _ = to_job.send(try_build(client, pool, cached_reads, config, cancel));

    // call wake() to poll job again
    waker.wake();
}

/// Builds a header for the round based on the requested `BatchDigest`.
pub(super) fn create_header<Pool, Client>(
    client: Client,
    pool: Pool,
    cached_reads: CachedReads,
    config: HeaderPayloadConfig,
    cancel: Cancelled,
    to_job: oneshot::Sender<Result<HeaderPayload, LatticePayloadBuilderError>>,
    waker: Waker,
    digests: IndexMap<BatchDigest, (WorkerId, TimestampSec)>,
    missing_batches: Option<HashMap<BatchDigest, Batch>>,
)
where
    Client: StateProviderFactory,
    Pool: TransactionPool,
{
    #[inline(always)]
    fn try_build<Pool, Client>(
        client: Client,
        pool: Pool,
        mut cached_reads: CachedReads,
        config: HeaderPayloadConfig,
        _cancel: Cancelled, // TODO: can cancel be used to prevent batches while processing consensus output?
        digests: IndexMap<BatchDigest, (WorkerId, TimestampSec)>,
        missing_batches: Option<HashMap<BatchDigest, Batch>>,
    ) -> Result<HeaderPayload, LatticePayloadBuilderError>
    where
        Client: StateProviderFactory,
        Pool: TransactionPool,
    {
        let HeaderPayloadConfig {
            initialized_block_env,
            initialized_cfg,
            parent_block,
            max_header_size,
            chain_spec,
            attributes,
        } = config;

        debug!(parent_hash=?parent_block.hash, parent_number=parent_block.number, "building new payload");

        let state = State::new(client.state_by_block_hash(parent_block.hash)?);
        let mut db = CacheDB::new(cached_reads.as_db(&state));
        let mut post_state = PostState::default();

        let mut cumulative_gas_used = 0;
        let block_gas_limit: u64 = initialized_block_env.gas_limit.try_into().unwrap_or(u64::MAX);

        let mut executed_txs = Vec::new();
        let mut total_fees = U256::ZERO;

        // TODO: where should the base fee come from?
        let base_fee = initialized_block_env.basefee.to::<u64>();

        // TODO: should this be the round from primary?
        let block_number = initialized_block_env.number.to::<u64>();

        for (digest, (_worker_id, _timestamp)) in digests {
            // if batch is missing, return error for now
            let mut batch_best_txs = pool.get_batch_transactions(&digest)?;

            // execute all transactions in the batch
            while let Some(pool_tx) = batch_best_txs.next() {
                // header size contraints are managed at the batch level
                // and by the number of batches in the primary's proposed header
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
                                    batch_best_txs.mark_invalid(&pool_tx);
                                }
                                continue
                            }
                            err => {
                                // this is an error that we should treat as fatal for this attempt
                                warn!("EVM Fatal error - returning empty header.");
                                return Err(LatticePayloadBuilderError::EvmExecutionError(err))
                            }
                        }
                    }
                };

                let gas_used = result.gas_used();

                // commit changes
                commit_state_changes(&mut db, &mut post_state, block_number, state, true);

                // update payload's size
                // payload_size += tx_size;
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

                // TODO: how do we want to distribute fees for lattice?
                let miner_fee = tx
                    .effective_tip_per_gas(base_fee)
                    .expect("fee is always valid; execution succeeded");
                total_fees += U256::from(miner_fee) * U256::from(gas_used);

                // append to executed transactions
                executed_txs.push(tx.into_signed());
            }
        }

        // TODO: support withdrawals.
        let WithdrawalsOutcome { withdrawals_root, withdrawals } = WithdrawalsOutcome::empty();

        // commit_withdrawals(
        //     &mut db,
        //     &mut post_state,
        //     &chain_spec,
        //     block_number,
        //     attributes.timestamp,
        //     attributes.withdrawals,
        // )?;

        let receipts_root = post_state.receipts_root(block_number);
        let logs_bloom = post_state.logs_bloom(block_number);

        // calculate the state root
        let state_root = state.state().state_root(post_state)?;

        // create the block header
        let transactions_root = proofs::calculate_transaction_root(&executed_txs);

        let header = Header {
            parent_hash: parent_block.hash,
            ommers_hash: EMPTY_OMMER_ROOT,
            beneficiary: initialized_block_env.coinbase,
            state_root,
            transactions_root,
            receipts_root,
            withdrawals_root,
            logs_bloom,
            timestamp: attributes.created_at,
            mix_hash: H256::from_low_u64_be(33), // TODO: hash of cert parents?
            nonce: BEACON_NONCE,
            base_fee_per_gas: Some(base_fee),
            number: parent_block.number + 1,
            gas_limit: block_gas_limit,
            difficulty: U256::ZERO,
            gas_used: cumulative_gas_used,
            extra_data: Bytes::default(), // TODO: empty bytes
        };

        Ok(HeaderPayload::new(header))
    }

    // return the result to the header building job
    let _ = to_job.send(try_build(client, pool, cached_reads, config, cancel, digests, missing_batches));

    // call wake() to poll job again
    waker.wake();
}

/// Represents the outcome of committing withdrawals to the runtime database and post state.
/// Pre-shanghai these are `None` values.
struct WithdrawalsOutcome {
    withdrawals: Option<Vec<Withdrawal>>,
    withdrawals_root: Option<H256>,
}

impl WithdrawalsOutcome {
    // /// No withdrawals pre shanghai
    // fn pre_shanghai() -> Self {
    //     Self { withdrawals: None, withdrawals_root: None }
    // }

    fn empty() -> Self {
        Self { withdrawals: Some(vec![]), withdrawals_root: Some(EMPTY_WITHDRAWALS) }
    }
}

/// Executes the withdrawals and commits them to the _runtime_ Database and PostState.
///
/// Returns the withdrawals root.
///
/// Returns `None` values pre shanghai
#[allow(clippy::too_many_arguments)]
fn commit_withdrawals<DB>(
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
    // if !chain_spec.is_shanghai_activated_at_timestamp(timestamp) {
    //     return Ok(WithdrawalsOutcome::pre_shanghai())
    // }

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

pub(crate) fn create_block<Client, Pool>(
    client: Client,
    pool: Pool,
    cached_reads: CachedReads,
    config: BlockPayloadConfig,
    cancel: Cancelled,
    to_job: oneshot::Sender<Result<BlockPayload, LatticePayloadBuilderError>>,
    waker: Waker,
)
where
    Client: StateProviderFactory,
    Pool: TransactionPool,
{
    #[inline(always)]
    fn try_build<Pool, Client>(
        client: Client,
        pool: Pool,
        mut cached_reads: CachedReads,
        config: BlockPayloadConfig,
        _cancel: Cancelled, // TODO: can cancel be used to prevent batches while processing consensus output?
    ) -> Result<BlockPayload, LatticePayloadBuilderError>
    where
        Client: StateProviderFactory,
        Pool: TransactionPool,
    {
        let BlockPayloadConfig {
            initialized_cfg,
            initialized_block_env,
            parent_block,
            chain_spec,
            output,
        } = config;

        debug!(parent_hash=?parent_block.hash, parent_number=parent_block.number, "building new canonical block");

        let state = State::new(client.state_by_block_hash(parent_block.hash)?);
        let mut db = CacheDB::new(cached_reads.as_db(&state));
        let mut post_state = PostState::default();

        let mut cumulative_gas_used = 0;
        let block_gas_limit: u64 = initialized_block_env.gas_limit.try_into().unwrap_or(u64::MAX);

        let mut executed_txs = Vec::new();
        let mut total_fees = U256::ZERO;
        let base_fee = initialized_block_env.basefee.to::<u64>();

        let block_number = initialized_block_env.number.to::<u64>();

        // execute every transaction in the output
        for (_, batches) in output.batches {
            for batch in batches {
                for transaction in batch.transactions().iter() {
                    let signed_tx = TransactionSigned::decode(&mut transaction.as_slice())?;
                    // ensure block has gas left
                    if cumulative_gas_used + signed_tx.gas_limit() > block_gas_limit {
                        error!("Unexpected overflow of gas used executing consensus output");
                        // TODO: return metric that gas ran out? 
                        // this shouldn't happen bc batches are capped at 30mil
                        // and the gas limit for this block = (num_batches * 30mil)
                        todo!()
                    }

                    let tx = signed_tx.clone().into_ecrecovered().ok_or_else(||
                        LatticePayloadBuilderError::RecoverSignature(signed_tx)
                    )?;
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
                                            "skipping invalid transaction"
                                        );
                                        // batch_best_txs.mark_invalid(&pool_tx);
                                        
                                        // TODO: add metric here

                                        // TODO: does it break consensus/DAG ordering to add consensus output
                                        // to a pool first, reordered, remove duplicates, and then process?

                                        // worse case without using pool: 
                                        // duplicate transaction included:
                                        //      inefficient execution of duplicate transactions that will fail
                                        // duplicate tx nonce:
                                        //      the first one processed will be executed

                                        // TODO: need tests for these scenarios

                                        // TODO: should we search the sealed pool for these transactions?
                                        // TODO: how to cleanup pool after canonical block executed?
                                    }
                                    continue
                                }
                                err => {
                                    // this is an error that we should treat as fatal for this attempt
                                    warn!("EVM Fatal error - returning empty header.");
                                    return Err(LatticePayloadBuilderError::EvmExecutionError(err))
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

                    let miner_fee = tx
                        .effective_tip_per_gas(base_fee)
                        .expect("fee is always valid; execution succeeded");
                    total_fees += U256::from(miner_fee) * U256::from(gas_used);

                    // append to executed transactions
                    executed_txs.push(tx.into_signed());
                }
            }
        }

        // TODO: support withdrawals.
        let WithdrawalsOutcome { withdrawals_root, withdrawals } = WithdrawalsOutcome::empty();
        // commit_withdrawals(
        //     &mut db,
        //     &mut post_state,
        //     &chain_spec,
        //     block_number,
        //     attributes.timestamp,
        //     attributes.withdrawals,
        // )?;

        let receipts_root = post_state.receipts_root(block_number);
        let logs_bloom = post_state.logs_bloom(block_number);

        // calculate the state root
        let state_root = state.state().state_root(post_state)?;

        // create the block header
        let transactions_root = proofs::calculate_transaction_root(&executed_txs);

        let header = Header {
            parent_hash: parent_block.hash,
            ommers_hash: EMPTY_OMMER_ROOT,
            beneficiary: initialized_block_env.coinbase,
            state_root,
            transactions_root,
            receipts_root,
            withdrawals_root,
            logs_bloom,
            timestamp: initialized_block_env.timestamp.to::<u64>(),
            mix_hash: initialized_block_env.prevrandao.unwrap_or_default(),
            nonce: BEACON_NONCE,
            base_fee_per_gas: Some(base_fee),
            number: parent_block.number + 1,
            gas_limit: block_gas_limit,
            difficulty: U256::ZERO,
            gas_used: cumulative_gas_used,
            extra_data: Bytes::default(), // TODO: empty bytes
        };

        let block = Block { header, body: executed_txs, ommers: vec![], withdrawals}.seal_slow();

        Ok(BlockPayload::new(block, total_fees))
    }

    // return the result to the block building job
    let _ = to_job.send(try_build(client, pool, cached_reads, config, cancel));

    // call wake() to poll job again
    waker.wake();
}

#[cfg(test)]
mod test {
    use std::time::Instant;
    use rayon::prelude::*;
    use execution_transaction_pool::{test_utils::{testing_pool, MockTransaction, MockTransactionFactory}, TransactionOrigin, BatchInfo};
    use tn_types::execution::TxHash;
    use super::*;

    // Rayon takes longer than standard iter()
    #[tokio::test]
    async fn test_rayon_speed() {
        let pool = testing_pool();
        let tx = MockTransaction::eip1559();
        // gapless transactions
        let mock_txs = (0..100).map(|nonce|{
            tx.clone().rng_hash().with_nonce(nonce)
        }).collect();

        // add to pending pool
        let _tx_hashes = pool.add_transactions(TransactionOrigin::Local, mock_txs)
            .await
            .unwrap()
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        println!("pool size: {:?}", pool.pool_size());
        assert_eq!(pool.pool_size().pending, 100);

        // seal the txs
        // let tx_hashes = tx_hashes.map(|res| res.unwrap()).collect();
        let digest = BatchDigest::new([0_u8; 32]);
        let batch_info = BatchInfo::new(digest, pool.pending_transactions().iter().map(|tx| tx.transaction_id).collect(), 0);
        pool.on_sealed_batch(batch_info);
        println!("pool size: {:?}", pool.pool_size());
        assert_eq!(pool.pool_size().sealed, 100);

        let now = Instant::now();
        {
            let batch_pool = pool.get_batch_transactions(&digest).unwrap();
            let batch_txs = batch_pool.all_transactions();
            assert_eq!(batch_txs.len(), 100);

            // create batch
            let batch: Batch = batch_txs
                .par_iter()
                .map(|tx| tx.to_recovered_transaction().into_signed().envelope_encoded().into())
                .collect::<Vec<Vec<u8>>>()
                .into();

            assert_ne!(&batch.digest(), &digest);
        }
        let elapsed = now.elapsed();
        println!("rayon par_iter() time: {elapsed:.2?}");

        let new_now = Instant::now();
        {
            let batch_pool = pool.get_batch_transactions(&digest).unwrap();
            let batch_txs = batch_pool.all_transactions();
            assert_eq!(batch_txs.len(), 100);

            // create batch
            let batch: Batch = batch_txs
                .iter()
                .map(|tx| tx.to_recovered_transaction().into_signed().envelope_encoded().into())
                .collect::<Vec<Vec<u8>>>()
                .into();

            assert_ne!(&batch.digest(), &digest);
        }
        let elapsed = new_now.elapsed();
        println!("standard iter() time: {elapsed:.2?}");

    }
}
