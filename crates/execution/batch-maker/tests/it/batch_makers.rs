//! Batch maker (EL) collects transactions
//! and creates a batch.
//!
//! Batch maker (CL) receives the batch from EL
//! and forwards it to the Quorum Waiter.

use assert_matches::assert_matches;
use fastcrypto::hash::Hash;
use narwhal_network::client::NetworkClient;
use narwhal_network_types::MockWorkerToPrimary;
use narwhal_typed_store::Map;
use narwhal_worker::{metrics::WorkerMetrics, BatchMaker, NUM_SHUTDOWN_RECEIVERS};
use prometheus::Registry;
use reth::{init::init_genesis, tasks::TokioTaskExecutor};
use reth_blockchain_tree::noop::NoopBlockchainTree;
use reth_db::test_utils::create_test_rw_db;
use reth_interfaces::p2p::{
    headers::client::{HeadersClient, HeadersRequest},
    priority::Priority,
};
use reth_primitives::{
    alloy_primitives::U160, Address, ChainSpec, GenesisAccount, HeadersDirection,
    TransactionSigned, U256,
};
use reth_provider::{providers::BlockchainProvider, ProviderFactory};
use reth_tracing::init_test_tracing;
use reth_transaction_pool::{
    blobstore::InMemoryBlobStore, PoolConfig, TransactionPool, TransactionValidationTaskExecutor,
};
use std::{str::FromStr, sync::Arc, time::Duration};
use tn_batch_maker::{BatchMakerBuilder, MiningMode};
use tn_types::{
    adiri_genesis,
    test_utils::{create_batch_store, get_gas_price, TransactionFactory},
    Batch, BatchAPI, MetadataAPI, PreSubscribedBroadcastSender,
};
use tokio::time::timeout;
use tracing::debug;

#[tokio::test]
async fn test_make_batch_el_to_cl() {
    init_test_tracing();

    // worker channel
    let (to_worker, rx_batch_maker) = tn_types::test_channel!(1);

    //
    //=== Consensus Layer
    //

    let network_client = NetworkClient::new_with_empty_id();
    let store = create_batch_store();
    let mut tx_shutdown = PreSubscribedBroadcastSender::new(NUM_SHUTDOWN_RECEIVERS);
    let (tx_quorum_waiter, mut rx_quorum_waiter) = tn_types::test_channel!(1);
    let node_metrics = WorkerMetrics::new(&Registry::new());

    // Mock the primary client to always succeed.
    let mut mock_server = MockWorkerToPrimary::new();
    mock_server.expect_report_own_batch().returning(|_| Ok(anemo::Response::new(())));
    network_client.set_worker_to_primary_local_handler(Arc::new(mock_server));

    // Spawn a `BatchMaker` instance.
    let id = 0;
    let _batch_maker_handle = BatchMaker::spawn(
        id,
        /* max_batch_size */ 200,
        /* max_batch_delay */
        Duration::from_millis(1_000_000), // Ensure the timer is not triggered.
        tx_shutdown.subscribe(),
        rx_batch_maker,
        tx_quorum_waiter,
        Arc::new(node_metrics),
        network_client,
        store.clone(),
    );

    // worker's batch maker takes a long time to start
    tokio::task::yield_now().await;

    //
    //=== Execution Layer
    //

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
    let head_timestamp = genesis.timestamp;
    let chain: Arc<ChainSpec> = Arc::new(genesis.into());

    // init genesis
    let db = create_test_rw_db();
    let genesis_hash = init_genesis(db.clone(), chain.clone()).expect("init genesis");

    debug!("genesis hash: {genesis_hash:?}");

    // provider
    let factory = ProviderFactory::new(Arc::clone(&db), Arc::clone(&chain));
    let blockchain_db = BlockchainProvider::new(factory, NoopBlockchainTree::default())
        .expect("test blockchain provider");

    let task_executor = TokioTaskExecutor::default();

    // txpool
    let blob_store = InMemoryBlobStore::default();
    let validator = TransactionValidationTaskExecutor::eth_builder(Arc::clone(&chain))
        .with_head_timestamp(head_timestamp)
        .with_additional_tasks(1)
        .build_with_tasks(blockchain_db.clone(), task_executor, blob_store.clone());

    let txpool =
        reth_transaction_pool::Pool::eth_pool(validator, blob_store, PoolConfig::default());
    let max_transactions = 1;
    let mining_mode = MiningMode::instant(max_transactions, txpool.pending_transactions_listener());
    let address = Address::from(U160::from(333));

    // build execution batch maker
    let (_, client, task) = BatchMakerBuilder::new(
        Arc::clone(&chain),
        blockchain_db.clone(),
        txpool.clone(),
        to_worker,
        mining_mode,
        address,
    )
    .build();

    let gas_price = get_gas_price(blockchain_db.clone());
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
    debug!("transaction 3: {transaction3:?}");

    let added_result = tx_factory.submit_tx_to_pool(transaction1.clone(), txpool.clone()).await;
    assert_matches!(added_result, hash if hash == transaction1.hash());

    let added_result = tx_factory.submit_tx_to_pool(transaction2.clone(), txpool.clone()).await;
    assert_matches!(added_result, hash if hash == transaction2.hash());

    let added_result = tx_factory.submit_tx_to_pool(transaction3.clone(), txpool.clone()).await;
    assert_matches!(added_result, hash if hash == transaction3.hash());

    // txpool size
    let pending_pool_len = txpool.pool_size().pending;
    debug!("pool_size(): {:?}", txpool.pool_size());
    assert_eq!(pending_pool_len, 3);

    // spawn mining task once worker is ready
    let _mining_task = tokio::spawn(Box::pin(task));

    //
    //=== Test batch flow
    //

    // wait for quorum waiter's channel to recv batch
    let too_long = Duration::from_secs(5);
    let (batch, resp) = timeout(too_long, rx_quorum_waiter.recv())
        .await
        .expect("new batch created within time")
        .expect("new batch is Some()");

    // ensure expected transaction is in batch
    let expected_batch = Batch::new(vec![transaction1.envelope_encoded().into()]);
    let batch_txs = batch.transactions();
    assert_eq!(batch_txs, expected_batch.transactions());

    // ack to CL batch maker
    assert!(resp.send(()).is_ok());

    // ensure batch transaction decodes correctly
    let batch_tx_bytes = batch_txs.first().cloned().expect("one tx in batch");
    let decoded_batch_tx = TransactionSigned::decode_enveloped(&mut batch_tx_bytes.as_ref())
        .expect("tx bytes are uncorrupted");
    assert_eq!(decoded_batch_tx, transaction1);

    // retrieve block number 1 from storage
    //
    // at this point, we know the task has completed because
    // the task's Storage write lock must be dropped for the
    // read lock to be available here
    let storage_header = client
        .get_headers_with_priority(
            HeadersRequest { start: 1.into(), limit: 1, direction: HeadersDirection::Rising },
            Priority::Normal,
        )
        .await
        .expect("header is available from storage")
        .into_data()
        .first()
        .expect("header included")
        .to_owned();

    debug!("awaited first reply from storage header");

    let storage_sealed_header = storage_header.seal_slow();

    debug!("storage sealed header: {storage_sealed_header:?}");

    // TODO: this isn't the right thing to test bc storage should be removed
    //
    assert_eq!(batch.versioned_metadata().sealed_header(), &storage_sealed_header);
    assert_eq!(storage_sealed_header.beneficiary, address);

    // Ensure the batch is stored
    let batch_from_store = store
        .get(&expected_batch.digest())
        .expect("store searched for batch")
        .expect("batch in store");
    let sealed_header_from_batch_store = batch_from_store.versioned_metadata().sealed_header();
    assert_eq!(sealed_header_from_batch_store.beneficiary, address);

    // txpool size after mining
    let pending_pool_len = txpool.pool_size().pending;
    debug!("pool_size(): {:?}", txpool.pool_size());
    assert_eq!(pending_pool_len, 2);

    // ensure tx1 is removed
    assert!(!txpool.contains(transaction1.hash_ref()));
    // ensure tx2 & tx3 are in the pool still
    assert!(txpool.contains(transaction2.hash_ref()));
    assert!(txpool.contains(transaction3.hash_ref()));
}
