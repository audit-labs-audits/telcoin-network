//! Utils for testing purposes.
//!
//! TODOs for testing:
//! 
//! generator:
//!     - batch does not exceed config gas/batch_size
//! 
//! handle:
//!     - worker responds with batch digest & txpool updates ids
use execution_provider::test_utils::{MockEthProvider, ExtendedAccount};
use execution_provider::test_utils::blocks::BlockChainTestData;
use execution_provider::{StateProviderFactory, BlockReaderIdExt};
use execution_tasks::{TaskSpawner, TokioTaskExecutor};
use execution_transaction_pool::noop::NoopTransactionValidator;
use execution_transaction_pool::{TransactionPool, Pool, TransactionOrigin};
use execution_transaction_pool::test_utils::{testing_pool, MockTransaction, MockOrdering, TestPool};
use lattice_network::client::NetworkClient;
use lattice_payload_builder::{LatticePayloadBuilderService, LatticePayloadJobGenerator, LatticePayloadJobGeneratorConfig, LatticePayloadBuilderHandle};
use rand::Rng;
use telcoin_network::args::utils::genesis_value_parser;
use tn_types::consensus::MockEngineToWorker;
use tn_types::execution::{Address, U256};
use tokio::sync::oneshot;
use tracing::debug;

/// Creates a new [BatchBuilderService] and spawns it in the background.
pub async fn spawn_test_payload_service() -> LatticePayloadBuilderHandle {
    let (service, handle) = test_batch_builder_service().await;
    tokio::spawn(service);
    handle
}

/// Test generator uses:
/// 
/// Client: [MockEthProvider]
/// Pool: type [TestPool](crates/execution/transaction-pool/src/test_utils/mod.rs)
/// Tasks: [TokioTaskExecutor]
type TestGenerator = LatticePayloadJobGenerator<MockEthProvider, Pool<NoopTransactionValidator<MockTransaction>, MockOrdering>, TokioTaskExecutor, NetworkClient>;

/// Creates a [BatchBuilderService] and [BatchBuilderHandle]
pub async fn test_batch_builder_service() -> (LatticePayloadBuilderService<TestGenerator>, LatticePayloadBuilderHandle)
{
    let senders = vec![Address::from_low_u64_be(3), Address::from_low_u64_be(33), Address::from_low_u64_be(333)];
    // let client = MockEthProvider::default();
    let client = mock_lattice_provider(senders.clone());
    let pool = mock_lattice_pool(senders).await;
    let tasks = TokioTaskExecutor::default();
    let config = LatticePayloadJobGeneratorConfig::default();
    let chain_spec = genesis_value_parser("lattice").unwrap();
    // let generator = LatticePayloadJobGenerator::new(
    //     client,
    //     pool,
    //     tasks,
    //     config,
    //     chain_spec,
    // );

    // LatticePayloadBuilderService::new(generator)
    todo!()
}

/// Create a client for the test batch builder service that has some blocks.
/// 
/// TODO: create a lattice-specific one.
pub fn mock_lattice_provider(accounts_to_seed: Vec<Address>) -> MockEthProvider {
    let provider = MockEthProvider::default();
    // add funds to seed accounts
    for address in accounts_to_seed {
        let account = ExtendedAccount::new(0, U256::MAX);
        provider.add_account(address, account);
    }
    let data = BlockChainTestData::default();
    // add genesis
    provider.add_block(data.genesis.hash, data.genesis.into());
    // let block_1 = data.blocks.first().unwrap().to_owned();
    // provider.add_block(block_1.0.hash, block_1.0.block.into());
    provider
}

/// Create a transaction pool with some pending transactions
pub async fn mock_lattice_pool(accounts_with_funds: Vec<Address>) -> TestPool {
    let pool = testing_pool();
    let transactions = {
        let mut txs = vec![];
        for _ in 0..10 {
            // choose a random account for next transaction
            let mut rng = rand::thread_rng();
            let max = accounts_with_funds.len() - 1;
            let account_index = rng.gen_range(0..max);
            txs.push(MockTransaction::eip1559()
                    .set_sender(accounts_with_funds[account_index])
                    .to_owned()
                )
        }
        txs
    };

    pool.add_transactions(TransactionOrigin::Local, transactions).await.unwrap();
    assert!(pool.pool_size().pending > 0);
    pool
}
