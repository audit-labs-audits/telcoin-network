//! Executor (CL) reaches consensus and
//! and forwards the output to the EL.
//!
//! Executor (EL) receives the output
//! and executes the next block.


use fastcrypto::hash::Hash;
use narwhal_primary::{
    consensus::{
        make_certificate_store, make_consensus_store, Bullshark, ConsensusMetrics, ConsensusRound,
        LeaderSchedule, LeaderSwapTable, NUM_SUB_DAGS_PER_SCHEDULE,
    },
    NUM_SHUTDOWN_RECEIVERS,
};
use narwhal_types::{
    test_utils::{
        make_optimal_certificates, mock_certificate, CommitteeFixture,
        TransactionFactory,
    },
    yukon_genesis, Certificate, CommittedSubDag,
    PreSubscribedBroadcastSender,
};
use prometheus::Registry;
use reth::{init::init_genesis, tasks::TokioTaskExecutor};
use reth_blockchain_tree::{
    BlockchainTree, BlockchainTreeConfig, ShareableBlockchainTree,
    TreeExternals,
};
use reth_db::test_utils::create_test_rw_db;
use reth_interfaces::{
    consensus::Consensus,
};
use reth_primitives::{ChainSpec, GenesisAccount, U256};
use reth_provider::{providers::BlockchainProvider, CanonStateSubscriptions, ProviderFactory};
use reth_revm::Factory;
use reth_tracing::init_test_tracing;

use std::{collections::BTreeSet, str::FromStr, sync::Arc};
use tn_executor::{AutoSealConsensus, Executor};
use tokio::{
    sync::{mpsc::unbounded_channel, watch},
};
use tracing::debug;

#[tokio::test]
async fn test_execute_consensus_output() {
    init_test_tracing();
    let genesis = yukon_genesis();
    let tx_factory = TransactionFactory::new();
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
    let _head_timestamp = genesis.timestamp;
    let chain: Arc<ChainSpec> = Arc::new(genesis.into());

    // init genesis
    let db = create_test_rw_db();
    let genesis_hash = init_genesis(db.clone(), chain.clone()).expect("init genesis");

    debug!("genesis hash: {genesis_hash:?}");

    let consensus: Arc<dyn Consensus> = Arc::new(AutoSealConsensus::new(Arc::clone(&chain)));

    // configure blockchain tree
    let tree_externals = TreeExternals::new(
        Arc::clone(&db),
        Arc::clone(&consensus),
        Factory::new(chain.clone()),
        Arc::clone(&chain),
    );

    // TODO: add prune config for full node
    let tree = BlockchainTree::new(
        tree_externals,
        BlockchainTreeConfig::default(), // default is more than enough
        None,                            // TODO: prune config
    )
    .expect("blockchain tree is valid");

    let _canon_state_notification_sender = tree.canon_state_notification_sender();
    let blockchain_tree = ShareableBlockchainTree::new(tree);

    // provider
    let factory = ProviderFactory::new(Arc::clone(&db), Arc::clone(&chain));
    let blockchain_db =
        BlockchainProvider::new(factory, blockchain_tree).expect("blockchain provider is valid");

    let _task_executor = TokioTaskExecutor::default();

    // // txpool
    // let blob_store = InMemoryBlobStore::default();
    // let validator = TransactionValidationTaskExecutor::eth_builder(Arc::clone(&chain))
    //     .with_head_timestamp(head_timestamp)
    //     .with_additional_tasks(1)
    //     .build_with_tasks(blockchain_db.clone(), task_executor, blob_store.clone());

    // let txpool =
    //     reth_transaction_pool::Pool::eth_pool(validator, blob_store, PoolConfig::default());
    // let max_transactions = 1;
    // let mining_mode =
    //     MiningMode::instant(max_transactions, txpool.pending_transactions_listener());

    // worker channel
    let (_to_consensus, from_consensus) = narwhal_types::test_channel!(1);
    let (to_engine, _from_engine) = unbounded_channel();
    let _canon_state_notification = blockchain_db.subscribe_to_canonical_state();

    // note on canon state stream:
    // SENDERS:
    //  - BlockchainTree::make_canonical() sends broadcast
    //      - engine calls this method
    //
    // RECEIVERS:
    //  - rpc (stream)
    //  - txpool maintenance task
    //      - stream events inside loop calling `.next()`
    //  - event handler
    //
    // Provider's CanonChainTracker:
    //  - rpc uses this to get chain info
    //

    // set capacity to `2` - chain cannot reorg
    let (canon_state_notification_sender, _receiver) = tokio::sync::broadcast::channel(2);

    // build batch maker
    let (_, _client, _task) = Executor::new(
        Arc::clone(&chain),
        blockchain_db.clone(),
        from_consensus,
        to_engine,
        canon_state_notification_sender,
        // txpool.clone(),
        // mining_mode,
    )
    .build();

    //=== Consensus

    let fixture = CommitteeFixture::builder().build();
    let committee = fixture.committee();

    // Make certificates for rounds 1 and 2.
    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();
    // TODO: add transactions to certificates
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    // TODO: add transactions to certificates
    let (mut certificates, next_parents) =
        make_optimal_certificates(&committee, 1..=2, &genesis, &ids);

    // Make two certificate (f+1) with round 3 to trigger the commits.
    let (_, certificate1) = mock_certificate(&committee, ids[0], 3, next_parents.clone());
    certificates.push_back(certificate1);

    // TODO: add transactions to certificates
    let (_, certificate2) = mock_certificate(&committee, ids[1], 3, next_parents);
    certificates.push_back(certificate2);

    // TODO: add transactions to certificates

    // Spawn the consensus engine and sink the primary channel.
    let (tx_new_certificates, rx_new_certificates) = narwhal_types::test_channel!(1);
    let (tx_primary, mut rx_primary) = narwhal_types::test_channel!(1);
    let (tx_output, mut rx_output) = narwhal_types::test_channel!(1);
    let (tx_consensus_round_updates, _rx_consensus_round_updates) =
        watch::channel(ConsensusRound::new(0, 0));

    let mut tx_shutdown = PreSubscribedBroadcastSender::new(NUM_SHUTDOWN_RECEIVERS);

    let store = make_consensus_store(&narwhal_types::test_utils::temp_dir());
    let cert_store = make_certificate_store(&narwhal_types::test_utils::temp_dir());
    let gc_depth = 50;
    let metrics = Arc::new(ConsensusMetrics::new(&Registry::new()));

    let _bad_nodes_stake_threshold = 0;
    let bullshark = Bullshark::new(
        committee.clone(),
        store.clone(),
        metrics.clone(),
        NUM_SUB_DAGS_PER_SCHEDULE,
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
        0,
    );

    let _consensus_handle = narwhal_primary::consensus::Consensus::spawn(
        committee,
        gc_depth,
        store,
        cert_store,
        tx_shutdown.subscribe(),
        rx_new_certificates,
        tx_primary,
        tx_consensus_round_updates,
        tx_output,
        bullshark,
        metrics,
    );
    tokio::spawn(async move { while rx_primary.recv().await.is_some() {} });

    // Feed all certificates to the consensus. Only the last certificate should trigger
    // commits, so the task should not block.
    while let Some(certificate) = certificates.pop_front() {
        tx_new_certificates.send(certificate).await.unwrap();
    }

    // Ensure the first 4 ordered certificates are from round 1 (they are the parents of the
    // committed leader); then the leader's certificate should be committed.
    let committed_sub_dag: CommittedSubDag = rx_output.recv().await.unwrap();
    let mut sequence = committed_sub_dag.certificates.into_iter();
    for _ in 1..=4 {
        let output = sequence.next().unwrap();
        assert_eq!(output.round(), 1);
    }
    let output = sequence.next().unwrap();
    assert_eq!(output.round(), 2);

    // AND the reputation scores have not been updated
    assert_eq!(committed_sub_dag.reputation_score.total_authorities(), 4);
    assert!(committed_sub_dag.reputation_score.all_zero());

    // let gas_price = get_gas_price(blockchain_db.clone());
    // let value =
    //     U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256").into();

    // // create 3 transactions
    // let transaction1 = tx_factory.create_eip1559(
    //     chain.clone(),
    //     gas_price,
    //     Address::ZERO,
    //     value, // 1 TEL
    // );
    // debug!("transaction 1: {transaction1:?}");

    // let transaction2 = tx_factory.create_eip1559(
    //     chain.clone(),
    //     gas_price,
    //     Address::ZERO,
    //     value, // 1 TEL
    // );
    // debug!("transaction 2: {transaction2:?}");

    // let transaction3 = tx_factory.create_eip1559(
    //     chain.clone(),
    //     gas_price,
    //     Address::ZERO,
    //     value, // 1 TEL
    // );
    // debug!("transaction 3: {transaction3:?}");

    // let added_result = tx_factory.submit_tx_to_pool(transaction1.clone(), txpool.clone()).await;
    // assert_matches!(added_result, hash if hash == transaction1.hash());

    // let added_result = tx_factory.submit_tx_to_pool(transaction2.clone(), txpool.clone()).await;
    // assert_matches!(added_result, hash if hash == transaction2.hash());

    // let added_result = tx_factory.submit_tx_to_pool(transaction3.clone(), txpool.clone()).await;
    // assert_matches!(added_result, hash if hash == transaction3.hash());

    // // txpool size
    // let pending_pool_len = txpool.pool_size().pending;
    // debug!("pool_size(): {:?}", txpool.pool_size());
    // assert_eq!(pending_pool_len, 3);

    // // spawn mining task
    // let _mining_task = tokio::spawn(Box::pin(task));

    // // wait for new batch
    // let too_long = Duration::from_secs(5);
    // let new_batch = timeout(too_long, worker_rx.recv())
    //     .await
    //     .expect("new batch created within time")
    //     .expect("new batch is Some()");

    // debug!("new batch: {new_batch:?}");
    // // number of transactions in the batch
    // let batch_txs = new_batch.batch.transactions();

    // // check max tx for task matches num of transactions in batch
    // let num_batch_txs = batch_txs.len();
    // assert_eq!(max_transactions, num_batch_txs);

    // // ensure decoded batch transaction is transaction1
    // let batch_tx_bytes = batch_txs.first().cloned().expect("one tx in batch");
    // let decoded_batch_tx = TransactionSigned::decode_enveloped(batch_tx_bytes.into())
    //     .expect("tx bytes are uncorrupted");
    // assert_eq!(decoded_batch_tx, transaction1);

    // // send the worker's ack to task
    // let digest = new_batch.batch.digest();
    // let _ack = new_batch.ack.send(digest);

    // // retrieve block number 1 from storage
    // //
    // // at this point, we know the task has completed because
    // // the task's Storage write lock must be dropped for the
    // // read lock to be available here
    // let storage_header = client
    //     .get_headers_with_priority(
    //         HeadersRequest { start: 1.into(), limit: 1, direction: HeadersDirection::Rising },
    //         Priority::Normal,
    //     )
    //     .await
    //     .expect("header is available from storage")
    //     .into_data()
    //     .first()
    //     .expect("header included")
    //     .to_owned();

    // debug!("awaited first reply from storage header");

    // let storage_sealed_header = storage_header.seal_slow();

    // debug!("storage sealed header: {storage_sealed_header:?}");

    // // TODO: this isn't the right thing to test bc storage should be removed
    // //
    // assert_eq!(new_batch.batch.versioned_metadata().sealed_header(), &storage_sealed_header,);

    // // txpool size after mining
    // let pending_pool_len = txpool.pool_size().pending;
    // debug!("pool_size(): {:?}", txpool.pool_size());
    // assert_eq!(pending_pool_len, 2);

    // // ensure tx1 is removed
    // assert!(!txpool.contains(transaction1.hash_ref()));
    // // ensure tx2 & tx3 are in the pool still
    // assert!(txpool.contains(transaction2.hash_ref()));
    // assert!(txpool.contains(transaction3.hash_ref()));
}
