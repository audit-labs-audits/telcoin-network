//! After consensus, there is no other opportunity for building a "better" payload.
//! 
//! The consensus_output modules is responsible for building the next block based
//! on the output provided by the consensus layer.
//! 
//! Unlike beacon consensus, the pending transaction pool is finalized. The execution
//! layer is responsible for building the next canonical block and sharing the hash
//! with the next batch.

use std::sync::Arc;
use execution_payload_builder::database::CachedReads;
use execution_payload_builder::error::PayloadBuilderError;
use execution_provider::{BlockReaderIdExt, StateProviderFactory, BlockSource, PostState};
use execution_revm::database::State;
use execution_revm::env::tx_env_with_recovered;
use execution_revm::executor::commit_state_changes;
use execution_revm::into_execution_log;
use execution_tasks::TaskSpawner;
use execution_transaction_pool::{TransactionPool, TransactionOrdering};
use execution_transaction_pool::pool::FinalizedPool;
use revm::db::CacheDB;
use revm::primitives::{BlockEnv, CfgEnv, Env, ResultAndState, EVMError, InvalidTransaction};
use tn_types::execution::{ChainSpec, BlockNumberOrTag, H256, SealedBlock, Transaction, TransactionSigned, U256, IntoRecoveredTransaction, Receipt};
use tn_types::consensus::OutputAttributes;
use tracing::trace;

/// The [PayloadGenerator] that creates [BatchPayloadJob]s.
pub struct CanonicalBlockGenerator<Client, Pool, Tasks> {
    /// The client that can interact with the chain.
    client: Client,
    /// Txpool
    pool: Pool,
    /// How to spawn building tasks
    executor: Tasks,
    /// The chain spec.
    chain_spec: Arc<ChainSpec>,
}

// === impl CanonicalBlockGenerator ===

impl<Client, Pool, Tasks> CanonicalBlockGenerator<Client, Pool, Tasks> {
    /// Creates a new [CanonicalBlockGenerator] with the given config.
    pub fn new(
        client: Client,
        pool: Pool,
        executor: Tasks,
        chain_spec: Arc<ChainSpec>,
    ) -> Self {
        Self {
            client,
            pool,
            executor,
            chain_spec,
        }
    }
}

// TODO: impl something like `PayloadJobGenerator` -> but don't use the trait
impl<Client, Pool, Tasks> CanonicalBlockGenerator<Client, Pool, Tasks>
where
    Client: StateProviderFactory + BlockReaderIdExt + Clone + Unpin + 'static,
    Pool: TransactionPool + Unpin + 'static,
    Tasks: TaskSpawner + Clone + Unpin + 'static,
{
    /// Build a new canonical block from the consensus output.
    fn new_canonical_block(
        &self,
        output: OutputAttributes,
    ) -> Result<(), PayloadBuilderError> {
        let parent_block = if output.parent.is_zero() {
            // use latest block if parent is zero: genesis block
            self.client
                .block_by_number_or_tag(BlockNumberOrTag::Latest)?
                .ok_or_else(|| PayloadBuilderError::MissingParentBlock(output.parent))?
                .seal_slow()
        } else {
            let block = self
                .client
                // only search for committed blocks in the database (skip `Pending`)
                .find_block_by_hash(output.parent, BlockSource::Database)?
                .ok_or_else(|| PayloadBuilderError::MissingParentBlock(output.parent))?;

            // we already know the hash, so we can seal it
            block.seal(output.parent)
        };

        // previous: configure evm env based on parent block
        // instead, configure base fee based on consensus value
        // note: possible to use features (ie-"optional-block-gas-limit")
        // to disable in CfgEnv.

        // TODO: don't use default - placeholder for compiler
        let block_config = LatticeBlockConfig {
            initialized_block_env: BlockEnv::default(),
            initialized_cfg: CfgEnv::default(),
            parent_block: Arc::new(parent_block),
            chain_spec: Arc::clone(&self.chain_spec),
            body: vec![],
        };

        // build the next block for the primary to propose
        // akin to fn build_payload() in basic
        let _ = build_lattice_block(
            self.client.clone(),
            self.pool.clone(),
            block_config,
        );

        
        
        Ok(())
    }
}

/// Builds a block for lattice consensus to propose.
/// 
/// The block contains information that other validators will use
/// to verify a peer's execution layer results.
/// 
/// Once a certificate is issued for this block,
/// the block becomes a part of the tree. The chosen leader's block
/// becomes the next canonical tip.
fn build_lattice_block<Pool, Client>(
    client: Client,
    pool: Pool,
    config: LatticeBlockConfig,
) -> Result<(), PayloadBuilderError>
where
    Client: StateProviderFactory,
    Pool: TransactionPool,
{
    // info obtained from OutputAttributes
    let LatticeBlockConfig {
        initialized_block_env,
        initialized_cfg,
        parent_block,
        ..
    } = config;

    // create cache
    // TODO: this is optimized for building and optimizing payloads,
    // but this payload should only be built once.
    //
    // for now, just use default cached db (baed on BasicPayloadJob::poll())
    let mut cache = CachedReads::default();
    let state = State::new(client.state_by_block_hash(parent_block.hash)?);
    let mut db = CacheDB::new(cache.as_db(&state));
    let mut post_state = PostState::default();

    let mut cumulative_gas_used = 0;
    // TODO: what is the block's gas limit?
    let block_gas_limit = u64::MAX;

    let mut executed_txs = Vec::new();
    // TODO: do we want to mark txs as invalid here?
    //
    // if a tx makes it to through consensus, it must be submitted and gas taken
    let mut finalized_transactions = pool.all_finalized_transactions();

    let mut total_fees = U256::ZERO;
    let base_fee = initialized_block_env.basefee.to::<u64>();

    let block_number = initialized_block_env.number.to::<u64>();


    // TODO: pool.best_transaction() returns an ordered iterator
    // see transaction-pool/src/pool/pending.rs
    //
    // TODO: if the dag is totally ordered, then is this a violation?
    // no bc
    // dag order affects the order of how txs are added to the queue, ie) `SubmissionId`
    // which ultimately affect's their priority

    while let Some(finalized_tx) = finalized_transactions.next() {
        // recover transaction
        let tx = finalized_tx.to_recovered_transaction();

        // configure environment for block
        let env = Env {
            cfg: initialized_cfg.clone(),
            block: initialized_block_env.clone(),
            tx: tx_env_with_recovered(&tx),
        };

        // create EVM from env + in-memory db
        let mut evm = revm::EVM::with_env(env);
        evm.database(&mut db);

        // TODO: what happens if a tx fails?
        //
        // Happy path, the tx won't fail because quorum checked this already.
        //
        // - ensure gas is still spent
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
                            // TODO: what is the best way to handle this?
                            // best_txs.mark_invalid(&tx);
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

        // commit state changes
        commit_state_changes(&mut db, &mut post_state, block_number, state, true);

        // track gas used
        cumulative_gas_used += gas_used;

        // Push transaction changeset and calculate header bloom filter for receipt.
        post_state.add_receipt(
            block_number,
            Receipt {
                tx_type: tx.tx_type(),
                success: result.is_success(),
                cumulative_gas_used,
                logs: result.logs().into_iter().map(into_execution_log).collect(),
            }
        );

        // update and add to toal fees
        //
        // TODO: miner fees should be based on validator whos batch collected the tx
        //
        // issuance goes to the round leader for PoS
        let miner_fee = tx
            .effective_tip_per_gas(base_fee)
            .expect("fee is always valid; execution succeeded");
    
        total_fees += U256::from(miner_fee) * U256::from(gas_used);

        // append tx to executed transactions
        executed_txs.push(tx.into_signed());
    }

    // let WithdrawalsOutcome {withdrawals_root, withdrawals } = 

    Ok(())
}

/// Configuration for building the next canonical block.
struct LatticeBlockConfig {
    /// Pre-configured block environment.
    initialized_block_env: BlockEnv,
    /// Configuration for the environment.
    initialized_cfg: CfgEnv,
    /// Parent block.
    parent_block: Arc<SealedBlock>,
    /// The chain spec.
    chain_spec: Arc<ChainSpec>,
    /// Body of transactions from consensus.
    ///
    /// The order of this matters.
    body: Vec<TransactionSigned>,
}









// /// Settings for the [BatchPayloadJobGenerator].
// #[derive(Debug, Clone)]
// pub struct CanonicalBlockGeneratorConfig {
//     /// Data to include in the block's extra data field.
//     extradata: Bytes,
//     /// Target gas ceiling for built blocks, defaults to [ETHEREUM_BLOCK_GAS_LIMIT] gas.
//     max_gas_limit: u64,
//     /// The interval at which the job should build a new payload after the last.
//     interval: Duration,
//     /// The deadline for when the payload builder job should resolve.
//     deadline: Duration,
//     /// Maximum number of tasks to spawn for building a payload.
//     max_payload_tasks: usize,
// }

// // === impl CanonicalBlockGeneratorConfig ===

// impl CanonicalBlockGeneratorConfig {
//     /// Sets the interval at which the job should build a new payload after the last.
//     pub fn interval(mut self, interval: Duration) -> Self {
//         self.interval = interval;
//         self
//     }

//     /// Sets the deadline when this job should resolve.
//     pub fn deadline(mut self, deadline: Duration) -> Self {
//         self.deadline = deadline;
//         self
//     }

//     /// Sets the maximum number of tasks to spawn for building a payload(s).
//     ///
//     /// # Panics
//     ///
//     /// If `max_payload_tasks` is 0.
//     pub fn max_payload_tasks(mut self, max_payload_tasks: usize) -> Self {
//         assert!(max_payload_tasks > 0, "max_payload_tasks must be greater than 0");
//         self.max_payload_tasks = max_payload_tasks;
//         self
//     }

//     /// Sets the data to include in the block's extra data field.
//     ///
//     /// Defaults to the current client version: `rlp(EXECUTION_CLIENT_VERSION)`.
//     pub fn extradata(mut self, extradata: Bytes) -> Self {
//         self.extradata = extradata;
//         self
//     }

//     /// Sets the target gas ceiling for mined blocks.
//     ///
//     /// Defaults to [ETHEREUM_BLOCK_GAS_LIMIT] gas.
//     pub fn max_gas_limit(mut self, max_gas_limit: u64) -> Self {
//         self.max_gas_limit = max_gas_limit;
//         self
//     }
// }

// impl Default for CanonicalBlockGeneratorConfig {
//     fn default() -> Self {
//         let mut extradata = BytesMut::new();
//         EXECUTION_CLIENT_VERSION.as_bytes().encode(&mut extradata);
//         Self {
//             extradata: extradata.freeze(),
//             max_gas_limit: ETHEREUM_BLOCK_GAS_LIMIT,
//             interval: Duration::from_secs(1),
//             // 12s slot time
//             deadline: SLOT_DURATION,
//             max_payload_tasks: 3,
//         }
//     }
// }
