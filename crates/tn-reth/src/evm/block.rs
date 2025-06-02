//! The types that build blocks for EVM execution.

use alloy::{
    consensus::{proofs, Block, BlockBody, Transaction, TxReceipt},
    eips::{eip7685::Requests, eip7840, merge::BEACON_NONCE},
};
use alloy_evm::{Database, Evm};
use reth_chainspec::{EthChainSpec, EthereumHardfork, EthereumHardforks};
use reth_errors::{BlockExecutionError, BlockValidationError};
use reth_evm::{
    block::{
        BlockExecutor, BlockExecutorFactory, BlockExecutorFor, ExecutableTx,
        StateChangePostBlockSource, StateChangeSource, SystemCaller,
    },
    eth::{
        receipt_builder::{AlloyReceiptBuilder, ReceiptBuilder, ReceiptBuilderCtx},
        spec::{EthExecutorSpec, EthSpec},
    },
    execute::{BlockAssembler, BlockAssemblerInput},
    state_change::balance_increment_state,
    EthEvmFactory, EvmFactory, FromRecoveredTx, FromTxWithEncoded, OnStateHook,
};
use reth_primitives::{logs_bloom, Log};
use reth_provider::BlockExecutionResult;
use reth_revm::{
    context::result::{ExecutionResult, ResultAndState},
    DatabaseCommit as _, Inspector, State,
};
use std::{borrow::Cow, sync::Arc};
use tn_types::{
    BlsSignature, Bytes, Encodable2718, ExecHeader, Receipt, TransactionSigned, Withdrawals, B256,
    EMPTY_OMMER_ROOT_HASH, EMPTY_WITHDRAWALS,
};

/// Context for TN block execution.
#[derive(Debug, Clone)]
pub struct TNBlockExecutionCtx {
    /// Parent block hash.
    pub parent_hash: B256,
    /// Parent beacon block root.
    pub parent_beacon_block_root: Option<B256>,
    /// The index for the batch.
    pub nonce: u64,
    /// Keccak hash of the bls signature for the leader certificate.
    /// Indicates executor to make closing epoch system call.
    pub close_epoch: Option<B256>,
    // // /// Block ommers
    // // pub ommers: &'a [ExecHeader],
    // // /// Block withdrawals.
    // // pub withdrawals: Option<Cow<'a, Withdrawals>>,
}

/// Block executor for Ethereum.
#[derive(Debug)]
pub struct TNBlockExecutor<Evm, Spec, R: ReceiptBuilder> {
    /// Reference to the specification object.
    spec: Spec,
    /// Context for block execution.
    pub ctx: TNBlockExecutionCtx,
    /// Inner EVM.
    evm: Evm,
    /// Utility to call system smart contracts.
    system_caller: SystemCaller<Spec>,
    /// Receipt builder.
    receipt_builder: R,

    /// Receipts of executed transactions.
    receipts: Vec<R::Receipt>,
    /// Total gas used by transactions in this block.
    gas_used: u64,
}

impl<Evm, Spec, R> TNBlockExecutor<Evm, Spec, R>
where
    Spec: Clone,
    R: ReceiptBuilder,
{
    /// Creates a new [`TNBlockExecutor`]
    pub fn new(evm: Evm, ctx: TNBlockExecutionCtx, spec: Spec, receipt_builder: R) -> Self {
        Self {
            evm,
            ctx,
            receipts: Vec::new(),
            gas_used: 0,
            system_caller: SystemCaller::new(spec.clone()),
            spec,
            receipt_builder,
        }
    }
}

impl<'db, DB, E, Spec, R> BlockExecutor for TNBlockExecutor<E, Spec, R>
where
    DB: Database + 'db,
    E: Evm<
        DB = &'db mut State<DB>,
        Tx: FromRecoveredTx<R::Transaction> + FromTxWithEncoded<R::Transaction>,
    >,
    Spec: EthExecutorSpec,
    R: ReceiptBuilder<Transaction: Transaction + Encodable2718, Receipt: TxReceipt<Log = Log>>,
{
    type Transaction = R::Transaction;
    type Receipt = R::Receipt;
    type Evm = E;

    fn apply_pre_execution_changes(&mut self) -> Result<(), BlockExecutionError> {
        // Set state clear flag if the block is after the Spurious Dragon hardfork.
        let state_clear_flag =
            self.spec.is_spurious_dragon_active_at_block(self.evm.block().number);
        self.evm.db_mut().set_state_clear_flag(state_clear_flag);

        // TODO: requires prague fork
        self.system_caller.apply_blockhashes_contract_call(self.ctx.parent_hash, &mut self.evm)?;
        // TODO: requires cancun
        self.system_caller
            .apply_beacon_root_contract_call(self.ctx.parent_beacon_block_root, &mut self.evm)?;

        Ok(())
    }

    fn execute_transaction_with_result_closure(
        &mut self,
        tx: impl ExecutableTx<Self>,
        f: impl FnOnce(&ExecutionResult<<Self::Evm as Evm>::HaltReason>),
    ) -> Result<u64, BlockExecutionError> {
        // The sum of the transaction's gas limit, Tg, and the gas utilized in this block prior,
        // must be no greater than the block's gasLimit.
        let block_available_gas = self.evm.block().gas_limit - self.gas_used;

        if tx.tx().gas_limit() > block_available_gas {
            return Err(BlockValidationError::TransactionGasLimitMoreThanAvailableBlockGas {
                transaction_gas_limit: tx.tx().gas_limit(),
                block_available_gas,
            }
            .into());
        }

        // Execute transaction.
        let result_and_state = self
            .evm
            .transact(tx)
            .map_err(|err| BlockExecutionError::evm(err, tx.tx().trie_hash()))?;
        self.system_caller
            .on_state(StateChangeSource::Transaction(self.receipts.len()), &result_and_state.state);
        let ResultAndState { result, state } = result_and_state;

        f(&result);

        let gas_used = result.gas_used();

        // append gas used
        self.gas_used += gas_used;

        // Push transaction changeset and calculate header bloom filter for receipt.
        self.receipts.push(self.receipt_builder.build_receipt(ReceiptBuilderCtx {
            tx: tx.tx(),
            evm: &self.evm,
            result,
            state: &state,
            cumulative_gas_used: self.gas_used,
        }));

        // Commit the state changes.
        self.evm.db_mut().commit(state);

        Ok(gas_used)
    }

    fn finish(
        mut self,
    ) -> Result<(Self::Evm, BlockExecutionResult<R::Receipt>), BlockExecutionError> {
        // let requests = if self.spec.is_prague_active_at_timestamp(self.evm.block().timestamp) {
        //     // Collect all EIP-6110 deposits
        //     let deposit_requests =
        //         eip6110::parse_deposits_from_receipts(&self.spec, &self.receipts)?;

        //     let mut requests = Requests::default();

        //     if !deposit_requests.is_empty() {
        //         requests.push_request_with_type(eip6110::DEPOSIT_REQUEST_TYPE, deposit_requests);
        //     }

        //     requests.extend(self.system_caller.apply_post_execution_changes(&mut self.evm)?);
        //     requests
        // } else {
        //     Requests::default()
        // };

        // don't support prague deposit requests
        let requests = Requests::default();

        // don't support block rewards for ommers
        //
        // let mut balance_increments = post_block_balance_increments(
        //     &self.spec,
        //     self.evm.block(),
        //     self.ctx.ommers,
        //     self.ctx.withdrawals.as_deref(),
        // );

        // don't support eth dao hardfork
        //
        // // irregular state change at Ethereum DAO hardfork
        // if self
        //     .spec
        //     .ethereum_fork_activation(EthereumHardfork::Dao)
        //     .transitions_at_block(self.evm.block().number)
        // {
        //     // drain balances from hardcoded addresses.
        //     let drained_balance: u128 = self
        //         .evm
        //         .db_mut()
        //         .drain_balances(dao_fork::DAO_HARDFORK_ACCOUNTS)
        //         .map_err(|_| BlockValidationError::IncrementBalanceFailed)?
        //         .into_iter()
        //         .sum();

        //     // return balance to DAO beneficiary.
        //     *balance_increments.entry(dao_fork::DAO_HARDFORK_BENEFICIARY).or_default() +=
        //         drained_balance;
        // }

        if self.ctx.close_epoch.is_some() {
            // TODO: apply epoch close here
            todo!()
        }

        // // close epoch using leader's aggregate signature if conditions are met
        // if let Some(res) = payload
        //     .attributes
        //     .close_epoch
        //     .map(|sig| self.apply_closing_epoch_contract_call(&mut evm, sig))
        // {
        //     // add logs if epoch closed
        //     let logs = res?;
        //     receipts.push(Some(Receipt {
        //         // no better tx type
        //         tx_type: TxType::Legacy,
        //         success: true,
        //         cumulative_gas_used: 0,
        //         logs,
        //     }));
        // }

        let balance_increments = alloy::primitives::map::foldhash::HashMap::default();

        // TODO: apply rewards for beneficiary
        //
        // increment balances for consensus leader on first batch of consensus output
        self.evm
            .db_mut()
            .increment_balances(balance_increments.clone())
            .map_err(|_| BlockValidationError::IncrementBalanceFailed)?;

        // call state hook with changes due to balance increments.
        self.system_caller.try_on_state_with(|| {
            balance_increment_state(&balance_increments, self.evm.db_mut()).map(|state| {
                (
                    StateChangeSource::PostBlock(StateChangePostBlockSource::BalanceIncrements),
                    Cow::Owned(state),
                )
            })
        })?;

        Ok((
            self.evm,
            BlockExecutionResult { receipts: self.receipts, requests, gas_used: self.gas_used },
        ))
    }

    fn set_state_hook(&mut self, hook: Option<Box<dyn OnStateHook>>) {
        self.system_caller.with_state_hook(hook);
    }

    fn evm_mut(&mut self) -> &mut Self::Evm {
        &mut self.evm
    }

    fn evm(&self) -> &Self::Evm {
        &self.evm
    }
}

/// Block builder for TN.
#[derive(Debug, Clone)]
pub struct TNBlockAssembler<ChainSpec = reth_chainspec::ChainSpec> {
    /// The chainspec.
    pub chain_spec: Arc<ChainSpec>,
    /// Extra data to use for the blocks.
    pub extra_data: Bytes,
}

impl<ChainSpec> TNBlockAssembler<ChainSpec> {
    /// Creates a new [`TNBlockAssembler`].
    pub fn new(chain_spec: Arc<ChainSpec>) -> Self {
        Self { chain_spec, extra_data: Default::default() }
    }
}

impl<F, ChainSpec> BlockAssembler<F> for TNBlockAssembler<ChainSpec>
where
    F: for<'a> BlockExecutorFactory<
        ExecutionCtx<'a> = TNBlockExecutionCtx,
        Transaction = TransactionSigned,
        Receipt = Receipt,
    >,
    ChainSpec: EthChainSpec + EthereumHardforks,
{
    type Block = Block<TransactionSigned>;

    fn assemble_block(
        &self,
        input: BlockAssemblerInput<'_, '_, F>,
    ) -> Result<Block<TransactionSigned>, BlockExecutionError> {
        let BlockAssemblerInput {
            evm_env,
            execution_ctx: ctx,
            parent,
            transactions,
            output: BlockExecutionResult { receipts, requests, gas_used },
            state_root,
            ..
        } = input;

        let timestamp = evm_env.block_env.timestamp;
        let transactions_root = proofs::calculate_transaction_root(&transactions);
        let receipts_root = Receipt::calculate_receipt_root_no_memo(receipts);
        let logs_bloom = logs_bloom(receipts.iter().flat_map(|r| r.logs()));

        let withdrawals = Some(Withdrawals::default()); //self
                                                        //     .chain_spec
                                                        //     .is_shanghai_active_at_timestamp(timestamp)
                                                        //     .then(|| ctx.withdrawals.map(|w| w.into_owned()).unwrap_or_default());

        let withdrawals_root = Some(EMPTY_WITHDRAWALS);
        // withdrawals.as_deref().map(|w| proofs::calculate_withdrawals_root(w));
        let requests_hash = self
            .chain_spec
            .is_prague_active_at_timestamp(timestamp)
            .then(|| requests.requests_hash());

        // cancun isn't active
        let excess_blob_gas = None;
        let blob_gas_used = None;

        // TODO: delete this
        //
        // // only determine cancun fields when active
        // if self.chain_spec.is_cancun_active_at_timestamp(timestamp) {
        //     blob_gas_used =
        //         Some(transactions.iter().map(|tx| tx.blob_gas_used().unwrap_or_default()).sum());
        //     excess_blob_gas = if self.chain_spec.is_cancun_active_at_timestamp(parent.timestamp) {
        //         parent.maybe_next_block_excess_blob_gas(
        //             self.chain_spec.blob_params_at_timestamp(timestamp),
        //         )
        //     } else {
        //         // for the first post-fork block, both parent.blob_gas_used and
        //         // parent.excess_blob_gas are evaluated as 0
        //         Some(eip7840::BlobParams::cancun().next_block_excess_blob_gas(0, 0))
        //     };
        // }

        let header = ExecHeader {
            parent_hash: ctx.parent_hash,
            ommers_hash: EMPTY_OMMER_ROOT_HASH,
            beneficiary: evm_env.block_env.beneficiary,
            state_root,
            transactions_root,
            receipts_root,
            withdrawals_root,
            logs_bloom,
            timestamp,
            mix_hash: evm_env.block_env.prevrandao.unwrap_or_default(),

            // ((self.epoch as u64) << 32) | self.round as u64
            nonce: ctx.nonce.into(), // output nonce - subdag leader nonce -> (epoch | round)

            base_fee_per_gas: Some(evm_env.block_env.basefee),
            number: evm_env.block_env.number,
            gas_limit: evm_env.block_env.gas_limit,

            // batch index?
            difficulty: evm_env.block_env.difficulty,

            gas_used: *gas_used,

            // USE THE RANDOMNESS hashed bls sig
            extra_data: ctx.close_epoch.map(|hash| hash.to_vec().into()).unwrap_or_default(),

            parent_beacon_block_root: ctx.parent_beacon_block_root,
            blob_gas_used,
            excess_blob_gas,

            // batch digests??
            requests_hash,
        };

        Ok(Block {
            header,
            body: BlockBody { transactions, ommers: Default::default(), withdrawals },
        })
    }
}
