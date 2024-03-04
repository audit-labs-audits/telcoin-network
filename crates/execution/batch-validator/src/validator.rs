//! Batch validator

use crate::error::BatchValidationError;
use reth_blockchain_tree::BundleStateDataRef;
use reth_db::database::Database;
use reth_interfaces::{
    blockchain_tree::{error::BlockchainTreeError, BlockchainTreeViewer},
    consensus::Consensus,
    executor::BlockValidationError,
};
use reth_primitives::{GotExpected, Hardfork, SealedBlockWithSenders, U256};
use reth_provider::{
    providers::{BlockchainProvider, BundleStateProvider},
    BlockchainTreePendingStateProvider, BundleStateDataProvider, BundleStateWithReceipts,
    ChainSpecProvider, ExecutorFactory, HeaderProvider, StateProviderFactory, StateRootProvider,
};
use reth_revm::EvmProcessorFactory;
use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
    sync::Arc,
};
use tn_types::Batch;
use tracing::debug;

/// Batch validator
#[derive(Clone)]
pub struct BatchValidator<DB, Tree> {
    /// Validation methods for beacon consensus.
    ///
    /// Required to remain fully compatible with Ethereum.
    consensus: Arc<dyn Consensus>,
    /// Database provider to encompass tree and provider factory.
    blockchain_db: BlockchainProvider<DB, Tree>,
    /// The executor factory to execute blocks with.
    executor_factory: EvmProcessorFactory,
}

/// Defines the validation procedure for receiving either a new single transaction (from a client)
/// of a batch of transactions (from another validator). Invalid transactions will not receive
/// further processing.
#[async_trait::async_trait]
pub trait BatchValidation: Clone + Send + Sync + 'static {
    type Error: Display + Debug + Send + Sync + 'static;
    // /// Determines if a transaction valid for the worker to consider putting in a batch
    // fn validate(&self, t: &[u8]) -> Result<(), Self::Error>;
    /// Determines if this batch can be voted on
    async fn validate_batch(&self, b: &Batch) -> Result<(), Self::Error>;
}

#[async_trait::async_trait]
// impl<DB, EF> TransactionValidator for BatchValidator<DB, EF>
impl<DB, Tree> BatchValidation for BatchValidator<DB, Tree>
where
    DB: Database + Sized + Clone + 'static,
    Tree: BlockchainTreePendingStateProvider + BlockchainTreeViewer + Clone + Send + Sync + 'static,
    // Self: Clone + Send + Sync + 'static,
{
    /// Error type for batch validation
    type Error = BatchValidationError;

    // /// TODO: remove this method
    // fn validate(&self, _tx: &[u8]) -> Result<(), Self::Error> {
    //     unimplemented!("txs are validated in exeuction layer")
    // }

    /// Execute the transactions within the batch
    ///
    /// akin to `on_new_payload()` for `BeaconEngine`.
    ///
    /// BlockchainTree has several useful methods, but they are private. The publicly exposed
    /// methods would result in canonicalizing batches, which is undesireable. It is possible to
    /// append then revert the batch, but this is also very inefficient.
    async fn validate_batch(&self, batch: &Batch) -> Result<(), Self::Error> {
        // check sui + reth
        //
        // ensure well-formed batch
        // verify receipts
        // ensure timestamps are valid
        // parent
        // ensure

        // try recover senders/txs
        // create sealed block with senders
        // BT: try_insert_validated_block
        // beacon consensus checks - timestamps, forks, etc.
        // ensure parent number +1 is batch's sealed header number
        // parent should be canonical - lookup in db

        // the following is taken from BlockchainTree::try_append_canonical_chain()

        // the main reason for porting this code is bc batches may or may not
        // extend the canonical tip, but state root still needs to be validated

        // in reth, this is only done when canonical head is extended
        // but batches may be behind canonical tip, which should still
        // be considered potentially valid

        // moving this code here prevents having to revert the tree after
        // validating the batch because `on_new_payload` results in appending
        // the execution payload to either a fork or the canonical tree

        // all other methods are private

        // try to recover signed transactions
        let block: SealedBlockWithSenders = batch.try_into()?;

        let parent = block.parent_num_hash();
        let block_num_hash = block.num_hash();
        debug!(target: "batch_validator", head = ?block_num_hash.hash, ?parent, "Appending block to canonical chain");

        // retrieve latest db provider
        // let provider = self.provider_factory.provider()?;

        // Validate that the block is post merge
        let parent_td = self
            .blockchain_db
            .header_td(&block.parent_hash)?
            .ok_or_else(|| BlockchainTreeError::CanonicalChain { block_hash: block.parent_hash })?;

        // Pass the parent total difficulty to short-circuit unnecessary calculations.
        if !self
            .blockchain_db
            .chain_spec()
            .fork(Hardfork::Paris)
            .active_at_ttd(parent_td, U256::ZERO)
        {
            return Err(BlockValidationError::BlockPreMerge { hash: block.hash })?
        }

        // retrieve parent header from provider
        let parent_header = self
            .blockchain_db
            .header(&block.parent_hash)?
            .ok_or_else(|| BlockchainTreeError::CanonicalChain { block_hash: block.parent_hash })?
            .seal(block.parent_hash);

        // read from canonical tree - updated by `Executor` and engine
        //
        // same return as BlockchainTree::canonical_chain()
        let canonical_block_hashes = self.blockchain_db.canonical_blocks();

        // from AppendableChain::new_canonical_fork() but with state root validation added
        let state = BundleStateWithReceipts::default();
        let empty = BTreeMap::new();

        // get the bundle state provider.
        let bundle_state_data_provider = BundleStateDataRef {
            state: &state,
            sidechain_block_hashes: &empty,
            canonical_block_hashes: &canonical_block_hashes,
            canonical_fork: parent,
        };

        // from AppendableChain::validate_and_execute() - private method
        //
        // ported here to prevent redundant creation of bundle state provider
        // just to check state root

        self.consensus.validate_header_against_parent(&block, &parent_header)?;

        let (block, senders) = block.into_components();
        let block = block.unseal();

        let canonical_fork = bundle_state_data_provider.canonical_fork();
        let state_provider = self.blockchain_db.history_by_block_number(canonical_fork.number)?;

        let provider = BundleStateProvider::new(state_provider, bundle_state_data_provider);

        let mut executor = self.executor_factory.with_state(&provider);
        executor.execute_and_verify_receipt(&block, U256::MAX, Some(senders))?;
        let bundle_state = executor.take_output_state();

        // check state root
        let state_root = provider.state_root(&bundle_state)?;
        if block.state_root != state_root {
            return Err(BatchValidationError::BodyStateRootDiff(GotExpected {
                got: state_root,
                expected: block.state_root,
            }))
        }

        Ok(())
    }
}

impl<DB, Tree> BatchValidator<DB, Tree> {
    /// Create a new instance of [Self]
    pub fn new(
        consensus: Arc<dyn Consensus>,
        executor_factory: EvmProcessorFactory,
        blockchain_db: BlockchainProvider<DB, Tree>,
    ) -> Self {
        Self { consensus, blockchain_db, executor_factory }
    }
}

#[cfg(any(test, feature = "test-utils"))]
/// Noop validation struct that validates any batch.
#[derive(Default, Clone)]
pub struct NoopBatchValidator;

#[cfg(any(test, feature = "test-utils"))]
#[async_trait::async_trait]
impl BatchValidation for NoopBatchValidator {
    type Error = BatchValidationError;

    async fn validate_batch(&self, _batch: &Batch) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth::{init::init_genesis, revm::EvmProcessorFactory};
    use reth_beacon_consensus::BeaconConsensus;
    use reth_blockchain_tree::{
        BlockchainTree, BlockchainTreeConfig, ShareableBlockchainTree, TreeExternals,
    };
    use reth_db::test_utils::create_test_rw_db;
    use reth_primitives::{
        hex, Address, Bloom, Bytes, ChainSpec, GenesisAccount, Header, SealedHeader, B256,
        EMPTY_OMMER_ROOT_HASH,
    };
    use reth_provider::ProviderFactory;
    use reth_tracing::init_test_tracing;
    use std::str::FromStr;
    use tn_types::{
        adiri_genesis,
        test_utils::{get_gas_price, TransactionFactory},
        VersionedMetadata,
    };

    #[tokio::test]
    async fn test_valid_batch() {
        init_test_tracing();
        let genesis = adiri_genesis();
        let mut tx_factory = TransactionFactory::new();
        let factory_address = tx_factory.address();
        debug!("seeding factory address: {factory_address:?}");

        // fund factory with 99mil TEL
        let account = vec![(
            factory_address,
            GenesisAccount::default().with_balance(
                U256::from_str("0x51E410C0F93FE543000000").expect("account balance is parsed"),
            ),
        )];

        let genesis = genesis.extend_accounts(account);
        debug!("seeded genesis: {genesis:?}");
        let chain: Arc<ChainSpec> = Arc::new(genesis.into());

        // init genesis
        let db = create_test_rw_db();
        let genesis_hash = init_genesis(db.clone(), chain.clone()).expect("init genesis");

        debug!("genesis hash: {genesis_hash:?}");
        // configure blockchain tree
        let provider_factory = ProviderFactory::new(Arc::clone(&db), Arc::clone(&chain));
        let consensus: Arc<dyn Consensus> = Arc::new(BeaconConsensus::new(chain.clone()));

        let tree_externals = TreeExternals::new(
            provider_factory.clone(),
            Arc::clone(&consensus),
            EvmProcessorFactory::new(chain.clone()),
        );
        let tree_config = BlockchainTreeConfig::default();
        let tree = BlockchainTree::new(
            tree_externals,
            tree_config,
            None, // prune config
        )
        .expect("blockchain tree is valid");

        let blockchain_tree = ShareableBlockchainTree::new(tree);

        // provider
        let blockchain_db =
            BlockchainProvider::new(provider_factory.clone(), blockchain_tree.clone())
                .expect("blockchain db valid");

        // get gas price before passing db
        let gas_price = get_gas_price(&blockchain_db);

        // batch validator
        let batch_validator = BatchValidator::new(
            Arc::clone(&consensus),
            EvmProcessorFactory::new(chain.clone()),
            blockchain_db,
        );

        // tx factory - [0; 32] seed address - nonce 0-2
        //
        // transactions are deterministic bc the factory is seeded with [0; 32]

        // tx1: TransactionSigned {
        //          hash: 0x1afbd86fd04c89d06778f8fbcc17d46d6f4869cef70ea02919202dbb943db636,
        //          signature: Signature {
        //              r: 0x83f87b2af6f42e7315f94e3c6278c1ea8fab3d45e739bdd706440c5b22961d1c_U256,
        //              s: 0x12f7d46ca56fdb9bacfcbb6f9a621742728faa80dc84c1a0ab078e542bb23d08_U256,
        //              odd_y_parity: true,
        //          },
        //          transaction: Eip1559(TxEip1559 {
        //              chain_id: 2600,
        //              nonce: 0,
        //              gas_limit: 1000000,
        //              max_fee_per_gas: 875000000,
        //              max_priority_fee_per_gas: 0,
        //              to: Call(0x0000000000000000000000000000000000000000),
        //              value:
        // TxValue(0x0000000000000000000000000000000000000000000000000de0b6b3a7640000_U256),
        //              access_list: AccessList([]),
        //              input: Bytes(0x),
        //          })
        //      }
        // let raw_tx1 =
        // Bytes::from_str("
        // 0x02f871820a28808084342770c0830f4240940000000000000000000000000000000000000000880de0b6b3a764000080c001a083f87b2af6f42e7315f94e3c6278c1ea8fab3d45e739bdd706440c5b22961d1ca012f7d46ca56fdb9bacfcbb6f9a621742728faa80dc84c1a0ab078e542bb23d08"
        // ).expect("tx1 valid bytes");

        // tx2: TransactionSigned {
        //          hash: 0x7d27749eeebc64d3385fab2301b619a84c78fc8d9ba4efd7e7d4f9fe8a59a6d3,
        //          signature: Signature {
        //              r: 0x934803978ef6e920072232c5f1c50b690722a7385bcd155879218bb2e2f67b2f_U256,
        //              s: 0x6c12751b4acd898b2c0935e183a7f5d324eed939106f87a6f2cd744f7ef3b39b_U256,
        //              odd_y_parity: true,
        //          },
        //          transaction: Eip1559(TxEip1559 {
        //              chain_id: 2600,
        //              nonce: 1,
        //              gas_limit: 1000000,
        //              max_fee_per_gas: 875000000,
        //              max_priority_fee_per_gas: 0,
        //              to: Call(0x0000000000000000000000000000000000000000),
        //              value:
        // TxValue(0x0000000000000000000000000000000000000000000000000de0b6b3a7640000_U256),
        //              access_list: AccessList([]),
        //              input: Bytes(0x),
        //          })
        //      }
        // let raw_tx2 =
        // Bytes::from_str("
        // 0x02f871820a28018084342770c0830f4240940000000000000000000000000000000000000000880de0b6b3a764000080c001a0934803978ef6e920072232c5f1c50b690722a7385bcd155879218bb2e2f67b2fa06c12751b4acd898b2c0935e183a7f5d324eed939106f87a6f2cd744f7ef3b39b"
        // ).expect("tx2 valid bytes");

        // tx3: TransactionSigned {
        //          hash: 0xf6bb571157b2553543f055ab0a24ad0fc1faf4c907c7faaabce6c40026ef462f,
        //          signature: Signature {
        //              r: 0x6b323119aefa01f11b805394e332b405db72a7d090d9ea73f69e0df7794b59d1_U256,
        //              s: 0x4b29e1b127058630e5f9adbe77c2841421b7d6ece8c833e4fd4513ce612c89fe_U256,
        //              odd_y_parity: true,
        //          },
        //          transaction: Eip1559(TxEip1559 {
        //              chain_id: 2600,
        //              nonce: 2,
        //              gas_limit: 1000000,
        //              max_fee_per_gas: 875000000,
        //              max_priority_fee_per_gas: 0,
        //              to: Call(0x0000000000000000000000000000000000000000),
        //              value:
        // TxValue(0x0000000000000000000000000000000000000000000000000de0b6b3a7640000_U256),
        //              access_list: AccessList([]),
        //              input: Bytes(0x),
        //          })
        //      }
        // let raw_tx3 =
        // Bytes::from_str("
        // 0x02f871820a28028084342770c0830f4240940000000000000000000000000000000000000000880de0b6b3a764000080c001a06b323119aefa01f11b805394e332b405db72a7d090d9ea73f69e0df7794b59d1a04b29e1b127058630e5f9adbe77c2841421b7d6ece8c833e4fd4513ce612c89fe"
        // ).expect("tx3 valid bytes");
        //
        let value =
            U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256").into();

        // create 3 transactions
        let transaction1 = tx_factory.create_eip1559(
            chain.clone(),
            gas_price,
            Address::ZERO,
            value, // 1 TEL
        );
        debug!("transaction 1: {transaction1:?}");

        let transaction2 = tx_factory.create_eip1559(
            chain.clone(),
            gas_price,
            Address::ZERO,
            value, // 1 TEL
        );
        debug!("transaction 2: {transaction2:?}");

        let transaction3 = tx_factory.create_eip1559(
            chain.clone(),
            gas_price,
            Address::ZERO,
            value, // 1 TEL
        );

        // sealed header
        let sealed_header = SealedHeader {
            header: Header {
                parent_hash: hex!(
                    "9ca9e62a3599c89955a93e0c76130973221f6056af2afd738d8514a66a900ebb"
                )
                .into(),
                ommers_hash: EMPTY_OMMER_ROOT_HASH,
                beneficiary: hex!("0000000000000000000000000000000000000000").into(),
                state_root: hex!(
                    "1e6751bd30803af71e305b61cf2da9bb3fa1a6841bc333f49c7422e1f8b0d013"
                )
                .into(),
                transactions_root: hex!(
                    "73b96de5ae50ad0100ad668270d6c1ce790c9a8d74835c315d5abecf851a8c74"
                )
                .into(),
                receipts_root: hex!(
                    "25e6b7af647c519a27cc13276a1e6abc46154b51414d174b072698df1f6c19df"
                )
                .into(),
                withdrawals_root: None,
                logs_bloom: Bloom::default(),
                difficulty: U256::ZERO,
                number: 1,
                gas_limit: 30000000,
                gas_used: 63000,
                timestamp: 1701790139,
                mix_hash: B256::ZERO,
                nonce: 0,
                base_fee_per_gas: Some(875000000),
                blob_gas_used: None,
                excess_blob_gas: None,
                parent_beacon_block_root: None,
                extra_data: Bytes::default(),
            },
            hash: hex!("ed9242a844ec144e25b58c085184c3c4ae8709226771659badf7e45cdd415c58").into(),
        };
        let transactions = vec![
            transaction1.envelope_encoded().into(),
            transaction2.envelope_encoded().into(),
            transaction3.envelope_encoded().into(),
        ];
        let metadata = VersionedMetadata::new(sealed_header.clone());
        let batch = Batch::new_with_metadata(transactions, metadata);

        let result = batch_validator.validate_batch(&batch).await;
        println!("result: {result:?}");

        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn test_invalid_batch() {
        todo!()
    }
}
