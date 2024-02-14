//! Executor (CL) reaches consensus and
//! and forwards the output to the EL.
//!
//! Executor (EL) receives the output
//! and executes the next block.
use anemo::Response;
use assert_matches::assert_matches;
use consensus_metrics::metered_channel;
use fastcrypto::{hash::Hash, traits::KeyPair as _};
use narwhal_network::client::NetworkClient;
use narwhal_network_types::{FetchBatchesResponse, MockPrimaryToWorker};
use narwhal_primary::{
    consensus::{
        make_certificate_store, make_consensus_store, Bullshark, ConsensusMetrics, ConsensusRound,
        LeaderSchedule, LeaderSwapTable, NUM_SUB_DAGS_PER_SCHEDULE,
    },
    NUM_SHUTDOWN_RECEIVERS,
};
use prometheus::Registry;
use reth::{cli::components::RethNodeComponentsImpl, init::init_genesis};
use reth_auto_seal_consensus::AutoSealConsensus;
use reth_beacon_consensus::{
    hooks::EngineHooks, BeaconConsensusEngine, MIN_BLOCKS_FOR_PIPELINE_RUN,
};
use reth_blockchain_tree::{
    BlockchainTree, BlockchainTreeConfig, ShareableBlockchainTree, TreeExternals,
};
use reth_config::Config;
use reth_db::test_utils::create_test_rw_db;
use reth_interfaces::{
    consensus::Consensus,
    p2p::{
        headers::client::{HeadersClient, HeadersRequest},
        priority::Priority,
    },
};
use reth_primitives::{ChainSpec, GenesisAccount, Head, HeadersDirection, TransactionSigned, U256};
use reth_provider::{
    providers::BlockchainProvider, BlockReaderIdExt, CanonStateNotification,
    CanonStateSubscriptions, ProviderFactory,
};
use reth_revm::EvmProcessorFactory;
use reth_rpc_types::engine::ForkchoiceState;
use reth_tasks::TaskManager;
use reth_tracing::init_test_tracing;
use reth_transaction_pool::noop::NoopTransactionPool;
use std::{
    collections::{BTreeSet, HashMap},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tn_executor::{
    build_network, build_networked_pipeline, lookup_head, spawn_payload_builder_service, Executor,
};
use tn_types::{
    adiri_genesis, execution_args,
    test_utils::{batch, CommitteeFixture},
    BatchAPI, Certificate, ConsensusOutput, PreSubscribedBroadcastSender,
};
use tokio::{
    runtime::Handle,
    sync::{mpsc::unbounded_channel, oneshot, watch},
    time::timeout,
};
use tracing::{debug, info};

/// Helper to run consensus to the point of committing one leader.
///
/// Based on `bullshark_tests::commit_one()`.
async fn commit_one() -> ConsensusOutput {
    let fixture = CommitteeFixture::builder().build();
    let committee = fixture.committee();
    let registry = Registry::new();
    // Make certificates for rounds 1 and 2.
    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let (mut certificates, next_parents) =
        tn_types::test_utils::make_optimal_certificates(&committee, 1..=2, &genesis, &ids);

    // Make two certificate (f+1) with round 3 to trigger the commits.
    let (_, certificate) =
        tn_types::test_utils::mock_certificate(&committee, ids[0], 3, next_parents.clone());
    certificates.push_back(certificate);
    let (_, certificate) =
        tn_types::test_utils::mock_certificate(&committee, ids[1], 3, next_parents);
    certificates.push_back(certificate);

    // Spawn the consensus engine and sink the primary channel.
    let (tx_new_certificates, rx_new_certificates) = tn_types::test_channel!(1);
    let (tx_primary, mut rx_primary) = tn_types::test_channel!(1);
    let (tx_sequence, rx_sequence) = tn_types::test_channel!(1);
    let (tx_consensus_round_updates, _rx_consensus_round_updates) =
        watch::channel(ConsensusRound::new(0, 0));

    let mut tx_shutdown = PreSubscribedBroadcastSender::new(NUM_SHUTDOWN_RECEIVERS);

    let store = make_consensus_store(&tn_types::test_utils::temp_dir());
    let cert_store = make_certificate_store(&tn_types::test_utils::temp_dir());
    let gc_depth = 50;
    let metrics = Arc::new(ConsensusMetrics::new(&registry));

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
        committee.clone(),
        gc_depth,
        store,
        cert_store,
        tx_shutdown.subscribe(),
        rx_new_certificates,
        tx_primary,
        tx_consensus_round_updates,
        tx_sequence,
        bullshark,
        metrics,
    );
    tokio::spawn(async move { while rx_primary.recv().await.is_some() {} });

    // Feed all certificates to the consensus. Only the last certificate should trigger
    // commits, so the task should not block.
    while let Some(certificate) = certificates.pop_front() {
        tx_new_certificates.send(certificate).await.unwrap();
    }

    // // Ensure the first 4 ordered certificates are from round 1 (they are the parents of the
    // // committed leader); then the leader's certificate should be committed.
    // let committed_sub_dag: CommittedSubDag = rx_output.recv().await.unwrap();
    // let mut sequence = committed_sub_dag.certificates.into_iter();
    // for _ in 1..=4 {
    //     let output = sequence.next().unwrap();
    //     assert_eq!(output.round(), 1);
    // }
    // let output = sequence.next().unwrap();
    // assert_eq!(output.round(), 2);

    // // AND the reputation scores have not been updated
    // assert_eq!(committed_sub_dag.reputation_score.total_authorities(), 4);
    // assert!(committed_sub_dag.reputation_score.all_zero());

    let primary = fixture.authorities().next().expect("first authority in fixture");
    let authority_id = primary.id();

    // mock worker
    let worker_id = 0;
    let worker = primary.worker(worker_id);
    let worker_peer_id = anemo::PeerId(worker.keypair().public().0.to_bytes());
    let mut mock_server = MockPrimaryToWorker::new();
    // create new batch here since the test is only for sealing whatever comes out of consensus
    mock_server.expect_fetch_batches().returning(move |request| {
        let digests = request.into_inner().digests;
        let mut batches = HashMap::new();
        for digest in digests {
            // this creates a new batch that's different than
            // the one created with the Certificate
            //
            // althought these are different, it is sufficient for mock server
            // bc the test is just executing whatever comes out of consensus
            let _ = batches.insert(digest, batch());
        }
        let response = FetchBatchesResponse { batches };
        Ok(Response::new(response))
    });

    let client = NetworkClient::new_from_keypair(&primary.network_keypair());
    client.set_primary_to_worker_local_handler(worker_peer_id, Arc::new(mock_server));
    // let execution_state = SimpleState
    let metrics = narwhal_executor::ExecutorMetrics::new(&registry);

    // TODO: this is what connects the EL and CL
    let (tx_notifier, mut rx_notifier) =
        metered_channel::channel(narwhal_primary::CHANNEL_CAPACITY, &metrics.tx_notifier);

    // Spawn the client executing the transactions. It can also synchronize with the
    // subscriber handler if it missed some transactions.
    let _executor_handles = narwhal_executor::Executor::spawn(
        authority_id,
        fixture.worker_cache(),
        committee,
        client,
        // execution_state,
        // vec![tx_shutdown.subscribe(), tx_shutdown.subscribe()], // this is dumb
        tx_shutdown.subscribe(),
        rx_sequence,
        vec![], // restored_consensus_output, // pass empty vec for now
        tx_notifier,
        metrics, // TODO: kinda sloppy
    )
    .expect("executor handles spawned successfully");

    // Wait till other services have been able to start up
    tokio::task::yield_now().await;

    rx_notifier.recv().await.expect("output received")
}

/// Unit test at this time is very complicated.
///
/// Transactions need to be valid, but the helper functions to create certificates
/// are not
#[tokio::test]
async fn test_execute_consensus_output() {
    init_test_tracing();

    //=== Consensus
    //
    // create consensus output bc transactions in batches
    // are randomly generated
    //
    // for each tx, seed address with funds in genesis
    //
    // TODO: this does not use a "real" `ConsensusOutput`
    //
    // refactor with valid data once test util helpers are in place
    let consensus_output = commit_one().await;
    let expected_beneficiary = consensus_output.beneficiary();

    //=== Execution

    let genesis = adiri_genesis();
    let args = execution_args();

    // collect txs and addresses for later assertions
    let mut txs_in_output = vec![];
    let mut senders_in_output = vec![];

    let mut accounts_to_seed = Vec::new();
    // loop through output
    for batches in consensus_output.clone().batches {
        for batch in batches.into_iter() {
            for tx in batch.transactions_owned() {
                let tx_signed = TransactionSigned::decode_enveloped(&mut tx.as_ref())
                    .expect("decode tx signed");
                let address = tx_signed.recover_signer().expect("signer recoverable");
                txs_in_output.push(tx_signed);
                senders_in_output.push(address);
                // fund account with 99mil TEL
                let account = (
                    address,
                    GenesisAccount::default().with_balance(
                        U256::from_str("0x51E410C0F93FE543000000")
                            .expect("account balance is parsed"),
                    ),
                );
                accounts_to_seed.push(account);
            }
        }
    }
    debug!("accounts to seed: {accounts_to_seed:?}");

    // genesis
    let genesis = genesis.extend_accounts(accounts_to_seed);
    let chain: Arc<ChainSpec> = Arc::new(genesis.into());
    let db = create_test_rw_db();
    let genesis_hash = init_genesis(db.clone(), chain.clone()).expect("init genesis");

    debug!("genesis hash: {genesis_hash:?}");

    let consensus: Arc<dyn Consensus> = Arc::new(AutoSealConsensus::new(Arc::clone(&chain)));
    let provider_factory = ProviderFactory::new(Arc::clone(&db), Arc::clone(&chain));

    // configure blockchain tree
    let tree_externals = TreeExternals::new(
        provider_factory.clone(),
        Arc::clone(&consensus),
        EvmProcessorFactory::new(chain.clone()),
    );

    // TODO: add prune config for full node
    let tree = BlockchainTree::new(
        tree_externals,
        BlockchainTreeConfig::default(), // default is more than enough
        None,                            // TODO: prune config
    )
    .expect("blockchain tree is valid");

    let canon_state_notification_sender = tree.canon_state_notification_sender();
    let blockchain_tree = ShareableBlockchainTree::new(tree);

    // provider
    let blockchain_db = BlockchainProvider::new(provider_factory.clone(), blockchain_tree)
        .expect("blockchain provider is valid");

    // skip txpool

    // task executor
    let manager = TaskManager::new(Handle::current());
    let task_executor = manager.executor();

    let head: Head = lookup_head(provider_factory.clone()).expect("lookup head successful");

    // network
    let network = build_network(
        chain.clone(),
        task_executor.clone(),
        provider_factory.clone(),
        head,
        NoopTransactionPool::default(),
        &args.network,
    )
    .await
    .expect("build network successful with no peers");

    // engine channel
    let (to_executor, from_consensus) = tn_types::test_channel!(1);
    let (to_engine, from_engine) = unbounded_channel();
    let mut canon_state_notification_receiver = blockchain_db.subscribe_to_canonical_state();

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

    // build executor
    let (_, client, mut task) = Executor::new(
        Arc::clone(&chain),
        blockchain_db.clone(),
        from_consensus,
        to_engine.clone(),
        canon_state_notification_sender,
    )
    .build();

    let config = Config::default();
    let (metrics_tx, _sync_metrics_rx) = unbounded_channel();

    let mut pipeline = build_networked_pipeline(
        &config,
        client.clone(),
        consensus,
        provider_factory.clone(),
        &task_executor,
        metrics_tx,
        chain,
    )
    .await
    .expect("networked pipeline build was successful");

    // TODO: is this necessary?
    let pipeline_events = pipeline.events();
    task.set_pipeline_events(pipeline_events);

    // spawn task to execute consensus output
    task_executor.spawn(Box::pin(task));

    // TODO: incompatible with tempdir db
    //
    // capture pipeline events one more time for events stream
    // before passing pipeline to beacon engine
    // let pipeline_events = pipeline.events();

    // spawn engine
    let hooks = EngineHooks::new();
    let components = RethNodeComponentsImpl {
        provider: blockchain_db.clone(),
        pool: NoopTransactionPool::default(),
        network: network.clone(),
        task_executor: task_executor.clone(),
        events: blockchain_db.clone(),
    };
    let payload_builder =
        spawn_payload_builder_service(components, &args.builder).expect("payload builder service");
    let (beacon_consensus_engine, beacon_engine_handle) = BeaconConsensusEngine::with_channel(
        client.clone(),
        pipeline,
        blockchain_db.clone(),
        Box::new(task_executor.clone()),
        Box::new(network.clone()),
        None,  // max block
        false, // self.debug.continuous,
        payload_builder.clone(),
        None, // initial_target
        MIN_BLOCKS_FOR_PIPELINE_RUN,
        to_engine,
        from_engine,
        hooks,
    )
    .expect("beacon consensus engine spawned");

    // TODO: events handler isn't compatible with temp db
    //
    // stream events
    // let events = stream_select!(
    //     network.event_listener().map(Into::into),
    //     beacon_engine_handle.event_listener().map(Into::into),
    //     pipeline_events.map(Into::into),
    //     ConsensusLayerHealthEvents::new(Box::new(blockchain_db.clone())).map(Into::into),
    // );

    // // monitor and print events
    // task_executor.spawn_critical(
    //     "events task",
    //     events::handle_events(Some(network.clone()), Some(head.number), events),
    // );

    let mut handles = vec![];

    // Run consensus engine to completion
    let (tx, _rx) = oneshot::channel();
    info!(target: "reth::cli", "Starting consensus engine");
    let engine_handle = task_executor.spawn_critical_blocking("consensus engine", async move {
        let res = beacon_consensus_engine.await;
        let _ = tx.send(res);
    });

    handles.push(engine_handle);

    // finalize genesis
    let genesis_state = ForkchoiceState {
        head_block_hash: genesis_hash,
        finalized_block_hash: genesis_hash,
        safe_block_hash: genesis_hash,
    };

    let _res = beacon_engine_handle
        .fork_choice_updated(genesis_state, None)
        .await
        .expect("fork choice updated to finalize genesis");
    tracing::debug!("genesis finalized :D");

    //=== Testing begins

    // send output to executor
    let res = to_executor.send(consensus_output).await;
    assert!(res.is_ok());

    // wait for next canonical block
    let too_long = Duration::from_secs(5);
    let canon_update = timeout(too_long, canon_state_notification_receiver.recv())
        .await
        .expect("next canonical block created within time")
        .expect("canon update is Some()");

    assert_matches!(canon_update, CanonStateNotification::Commit { .. });
    let canonical_tip = canon_update.tip();
    debug!("canon update: {:?}", canonical_tip);
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

    // ensure canonical tip is the next block
    assert_eq!(canonical_tip.header, storage_sealed_header);

    // ensure database and provider are updated
    let canonical_hash = canonical_tip.hash();

    // wait for forkchoice to finish updating
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut current_finalized_header = blockchain_db
            .finalized_header()
            .expect("blockchain db has some finalized header 1")
            .expect("some finalized header 2");
        while canonical_hash != current_finalized_header.hash() {
            // sleep - then look up finalized in db again
            println!("\nwaiting for engine to complete forkchoice update...\n");
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            current_finalized_header = blockchain_db
                .finalized_header()
                .expect("blockchain db has some finalized header 1")
                .expect("some finalized header 2");
        }

        tx.send(current_finalized_header)
    });

    let current_finalized_header = timeout(too_long, rx.recv())
        .await
        .expect("next canonical block created within time")
        .expect("finalized block retrieved from db");

    debug!("update completed...");
    assert_eq!(canonical_hash, current_finalized_header.hash());

    // assert canonical tip contains all txs and senders in batches
    assert_eq!(canonical_tip.block.beneficiary, expected_beneficiary);
    assert_eq!(canonical_tip.block.body, txs_in_output);
    assert_eq!(canonical_tip.senders, senders_in_output);
}
