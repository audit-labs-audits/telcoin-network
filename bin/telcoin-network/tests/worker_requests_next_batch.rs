// Copyright (c) Telcoin, LLC
use std::{sync::Arc, num::NonZeroUsize, time::Duration};
use execution_blockchain_tree::{TreeExternals, BlockchainTreeConfig, ShareableBlockchainTree, BlockchainTree};
use execution_db::init_db;
use execution_interfaces::{consensus::Consensus, blockchain_tree::BlockchainTreeEngine};
use execution_lattice_consensus::LatticeConsensus;
use execution_provider::{ProviderFactory, providers::BlockchainProvider, BlockReaderIdExt};
use execution_revm::Factory;
use execution_rlp::Decodable;
use execution_tasks::{TokioTaskExecutor, TaskSpawner};
use execution_transaction_pool::{EthTransactionValidator, TransactionPool, TransactionOrigin, TransactionEvent};
use futures::StreamExt;
use lattice_node::worker_node::WorkerNodes;
use lattice_payload_builder::batch::{generator::{BatchPayloadJobGenerator, BatchPayloadJobGeneratorConfig}, BatchBuilderService};
use lattice_test_utils::{CommitteeFixture, temp_dir, WorkerToWorkerMockServer};
use lattice_worker::TrivialTransactionValidator;
use telcoin_network::{dirs::{MaybePlatformPath, DataDirPath}, args::utils::genesis_value_parser, init::init_genesis};
use lattice_storage::NodeStorage;
use tn_tracing::init_test_tracing;
use tn_types::{execution::{LATTICE_GENESIS, TransactionSigned}, consensus::{Batch, Parameters, BatchAPI, crypto::traits::KeyPair, MockWorkerToPrimary}};
use execution_rpc_types::engine::ExecutionPayload;
use lattice_network::client::NetworkClient;
use consensus_metrics::RegistryService;
use prometheus::Registry;
use tokio::time::sleep;
use tracing::debug;
use lattice_typed_store::traits::Map;
use fastcrypto::hash::Hash;
mod common;
use crate::common::{tx_signed_from_raw, pool_transaction_from_raw, bob_raw_tx1, bob_raw_tx3, bob_raw_tx2};


// TODO: this is a good test for simulating consensus in next PR
// //=== Consensus Layer
// following along with crates/consensus/executor/tests/consensus_integration_tests.rs

#[tokio::test]
async fn test_single_worker_requests_next_batch() -> eyre::Result<(), eyre::Error> {
    // TODO: consolidate tracing fns:
    // let _guard = setup_tracing();
    // telemetry_subscribers::init_for_testing();
    init_test_tracing();

    //=== Execution Layer

    // default chain
    let chain = genesis_value_parser("lattice").unwrap();
    let platform_path = MaybePlatformPath::<DataDirPath>::default();
    let data_dir = platform_path.temp_chain(chain.chain.clone());
    
    // db for EL
    let db = Arc::new(init_db(&data_dir.db_path(), None)?);
    let chain_genesis = chain.genesis();
    debug!("genesis: \n\n{chain_genesis:?}\n\n");

    // initialize genesis
    let genesis_hash = init_genesis(db.clone(), chain.clone())?;
    assert_eq!(genesis_hash, LATTICE_GENESIS);
    // hash for forkchoice-updated?
    
    let consensus_type: Arc<dyn Consensus> = Arc::new(LatticeConsensus::new(chain.clone()));

    // configure blockchain tree
    let tree_externals = TreeExternals::new(
        db.clone(),
        Arc::clone(&consensus_type),
        Factory::new(chain.clone()),
        Arc::clone(&chain),
    );

    // default tree config for now
    let tree_config = BlockchainTreeConfig::default();

    // TODO: probably don't need to reconfigure tree since consensus is final
    // so, remove the canon_state_notification_sender (could still be useful for impl epochs?)
    //
    // original from reth:
    // The size of the broadcast is twice the maximum reorg depth, because at maximum reorg
    // depth at least N blocks must be sent at once.
    let (canon_state_notification_sender, _receiver) =
        tokio::sync::broadcast::channel(tree_config.max_reorg_depth() as usize * 2);

    // used to retrieve genesis block
    let blockchain_tree = ShareableBlockchainTree::new(BlockchainTree::new(
        tree_externals,
        canon_state_notification_sender.clone(),
        tree_config,
    )?);

    // make genesis block canonical so transactions are valid when added to the pool
    let canonical_outcome = blockchain_tree.make_canonical(&genesis_hash).unwrap();
    debug!("canonical outcome: \n{canonical_outcome:?}\n");
    

    // setup the blockchain provider - main struct for interacting with the blockchain
    let factory = ProviderFactory::new(Arc::clone(&db), Arc::clone(&chain));
    let blockchain_db = BlockchainProvider::new(factory, blockchain_tree.clone())?;

    debug!("Genesis hash: {:?}", genesis_hash);
    // let genesis_block_by_hash = blockchain_db.block_by_id(BlockId::Hash(genesis_hash.into())).unwrap();
    // let genesis_block_by_num = blockchain_db.block_by_id(BlockId::Number(0.into())).unwrap();
    // let genesis_block_by_finalized_tag = blockchain_db.block_by_number_or_tag(tn_types::execution::BlockNumberOrTag::Finalized).unwrap();
    let genesis_block_by_earliest_tag = blockchain_db.block_by_number_or_tag(tn_types::execution::BlockNumberOrTag::Earliest)?.unwrap();

    // assert_eq!(genesis_block_by_hash, genesis_block_by_num);
    // assert_eq!(genesis_block_by_hash, genesis_block_by_earliest_tag);
    // assert_ne!(genesis_block_by_hash, genesis_block_by_finalized_tag);

    // let sealed_genesis_header_by_earliest_tag = blockchain_db.sealed_header_by_number_or_tag(tn_types::execution::BlockNumberOrTag::Earliest)?.unwrap();
    debug!("Genesis block: {:?}", genesis_block_by_earliest_tag);
    let sealed_genesis_block = genesis_block_by_earliest_tag.seal_slow();
    debug!("\nsealed genesis block: {:?}", sealed_genesis_block);
    let genesis_payload: ExecutionPayload = sealed_genesis_block.into();
    debug!("\ngenesis payload: {:?}", genesis_payload);
    let genesis_batch: Batch = genesis_payload.into();
    debug!("\ngenesis batch: {:?}", genesis_batch);

    let task_executor = TokioTaskExecutor::default();

    // create transaction pool and batch payload generator
    let transaction_pool = execution_transaction_pool::Pool::eth_pool(
        EthTransactionValidator::with_additional_tasks(
            blockchain_db.clone(),
            Arc::clone(&chain),
            task_executor.clone(),
            1,
        ),
        Default::default(),
    );

    // add some transactions - bob is the only account with a seed balance
    let tx_1 = pool_transaction_from_raw(&transaction_pool, bob_raw_tx1());
    let tx_2 = pool_transaction_from_raw(&transaction_pool, bob_raw_tx2());
    let tx_3 = pool_transaction_from_raw(&transaction_pool, bob_raw_tx3());
    let mut tx1_events = transaction_pool.add_transaction_and_subscribe(TransactionOrigin::Local, tx_1).await?;
    let mut tx2_events = transaction_pool.add_transaction_and_subscribe(TransactionOrigin::Local, tx_2).await?;
    let mut tx3_events = transaction_pool.add_transaction_and_subscribe(TransactionOrigin::Local, tx_3).await?;
    assert_eq!(transaction_pool.pending_transactions().len(), 3);

    let batch_generator = BatchPayloadJobGenerator::new(
        blockchain_db.clone(),
        transaction_pool.clone(),
        task_executor.clone(),
        BatchPayloadJobGeneratorConfig::default(),
        Arc::clone(&chain),
    );

    let (batch_builder_service, batch_builder_handle) = BatchBuilderService::new(batch_generator);
    // TODO: why do I have to Box::pin() the payload service here,
    // when the `PayloadBuilderService` in the cli doesn't?
    task_executor.spawn_critical("batch-builder-service", Box::pin(batch_builder_service));


    //=== Consensus Layer for requesting the next batch
    // GIVEN
    // - one primary
    // - one worker
    let parameters = Parameters::default();
    let registry_service = RegistryService::new(Registry::new());
    let fixture = CommitteeFixture::builder()
        .number_of_workers(NonZeroUsize::new(1).unwrap())
        .randomize_ports(true)
        .build();
    let committee = fixture.committee();
    let worker_cache = fixture.worker_cache();

    let authority = fixture.authorities().next().unwrap();
    let key_pair = authority.keypair();
    let network_key_pair = authority.network_keypair();
    let client = NetworkClient::new_from_keypair(&network_key_pair);

    let store = NodeStorage::reopen(temp_dir(), None);

    // let (tx_confirmation, _rx_confirmation) = channel(10);
    // let execution_state = Arc::new(SimpleExecutionState::new(tx_confirmation));

    // // WHEN
    // let primary = PrimaryNode::new(parameters.clone(), true, registry_service.clone());

    // // channel for proposer and EL
    // let (el_sender, mut el_receiver) = tokio::sync::mpsc::channel(1);

    // primary
    //     .start(
    //         key_pair.copy(),
    //         network_key_pair.copy(),
    //         committee.clone(),
    //         worker_cache.clone(),
    //         client.clone(),
    //         &store,
    //         execution_state,
    //         el_sender,
    //     )
    //     .await
    //     .unwrap();

    // // spawn a task to respond to primary header requests
    // tokio::spawn(async move {
    //     while let Some((_header, reply)) = el_receiver.recv().await {
    //         debug!("replying to Primary...");
    //         let _ = reply.send(());
    //     }
    // });

    // AND
    let worker_id = 0;
    let workers = WorkerNodes::new(registry_service, parameters.clone());

    // TODO: update this method to take a Vec<Option<BatchBuilderHandle>>
    // - right now, I think it's passing the same `Sender` to every worker
    workers
        .start(
            key_pair.public().clone(),
            vec![(worker_id, authority.worker(worker_id).keypair().copy())],
            committee,
            worker_cache,
            client.clone(),
            &store,
            TrivialTransactionValidator::default(),
            Some(batch_builder_handle),
        )
        .await
        .unwrap();

    // doesn't work
    let (tx_await_batch, mut rx_await_batch) = lattice_test_utils::test_channel!(1000);
    let mut mock_primary_server = MockWorkerToPrimary::new();
    mock_primary_server
        .expect_report_own_batch()
        .withf(move |request| {
            let message = request.body();

            message.worker_id == worker_id
        })
        .times(1)
        .returning(move |_| {
            tx_await_batch.try_send(()).unwrap();
            Ok(anemo::Response::new(()))
        });
    client.set_worker_to_primary_local_handler(Arc::new(mock_primary_server));

    // spawn enough receivers to acknowledge the proposed batch
    let mut listener_handles = Vec::new();
    for worker in fixture.authorities().skip(1).map(|a| a.worker(0)) {
        let handle =
            WorkerToWorkerMockServer::spawn(worker.keypair(), worker.info().worker_address.clone());
        listener_handles.push(handle);
    }

    tokio::task::yield_now().await;

    sleep(Duration::from_secs(2)).await;

    assert!(!store.batch_store.is_empty());
    
    let mut batch = store.batch_store.values().next().unwrap().unwrap();
    let batch_transactions = batch.transactions_mut();
    assert_eq!(batch_transactions.len(), 3);

    let batch_tx1 = TransactionSigned::decode(&mut batch_transactions[0].as_ref()).unwrap();
    let batch_tx2 = TransactionSigned::decode(&mut batch_transactions[1].as_ref()).unwrap();
    let batch_tx3 = TransactionSigned::decode(&mut batch_transactions[2].as_ref()).unwrap();
    let expected_tx1 = tx_signed_from_raw(bob_raw_tx1());
    let expected_tx2 = tx_signed_from_raw(bob_raw_tx2());
    let expected_tx3 = tx_signed_from_raw(bob_raw_tx3());
    assert_eq!(batch_tx1, expected_tx1);
    assert_eq!(batch_tx2, expected_tx2);
    assert_eq!(batch_tx3, expected_tx3);

    sleep(Duration::from_secs(2)).await;

    let batch_digest = batch.digest();

    // ensure transactions were sealed with the correct batch info
    while let Some(event) = tx1_events.next().await {
        match event {
            TransactionEvent::Pending => (),
            TransactionEvent::Sealed(digest) => {
                assert_eq!(digest, batch_digest);
                break
            }
            _ => unreachable!("tx should only be pending and sealed")
        }
    }

    while let Some(event) = tx2_events.next().await {
        match event {
            TransactionEvent::Pending => (),
            TransactionEvent::Sealed(digest) => {
                assert_eq!(digest, batch_digest);
                break
            }
            _ => unreachable!("tx should only be pending and sealed")
        }
    }

    while let Some(event) = tx3_events.next().await {
        match event {
            TransactionEvent::Pending => (),
            TransactionEvent::Sealed(digest) => {
                assert_eq!(digest, batch_digest);
                break
            }
            _ => unreachable!("tx should only be pending and sealed")
        }
    }

    // assert tx pool updated
    assert_eq!(transaction_pool.pool_size().sealed, 3);

    // ensure primary received the batch's digest
    rx_await_batch.recv().await.unwrap();
    workers.shutdown().await;
    // primary.shutdown().await;
    // TODO:
    // -refactor Batch metadata for validation purposes
    //      - batch validator checks tree for parent hash and executes batch from that?
    //          - what if parent is old?
    // - check subscribers
    //      - rpc needs them
    //      - prevent batches from going out if canonical state change is happening?




    Ok(())
}
