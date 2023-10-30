// Copyright (c) Telcoin, LLC
use std::{sync::Arc, num::NonZeroUsize, time::Duration, collections::BTreeSet};
use anemo::Request;
use execution_blockchain_tree::{TreeExternals, BlockchainTreeConfig, ShareableBlockchainTree, BlockchainTree};
use execution_db::{init_db, test_utils::create_test_rw_db};
use execution_interfaces::{consensus::Consensus, blockchain_tree::BlockchainTreeEngine, test_utils::NoopFullBlockClient};
use execution_lattice_consensus::{LatticeConsensus, LatticeConsensusEngine};
use execution_provider::{ProviderFactory, providers::BlockchainProvider, BlockReaderIdExt};
use execution_revm::Factory;
use execution_rlp::Decodable;
use execution_tasks::{TokioTaskExecutor, TaskSpawner};
use execution_transaction_pool::{EthTransactionValidator, TransactionPool, TransactionOrigin, TransactionEvent};
use futures::StreamExt;
use lattice_node::{worker_node::WorkerNodes, primary_node::PrimaryNode, execution_state::LatticeExecutionState};
use lattice_payload_builder::{LatticePayloadJobGenerator, LatticePayloadJobGeneratorConfig, LatticePayloadBuilderService};
use lattice_test_utils::{CommitteeFixture, temp_dir, WorkerToWorkerMockServer, test_network, PrimaryToPrimaryMockServer, make_optimal_signed_certificates};
use lattice_worker::LatticeTransactionValidator;
use telcoin_network::{dirs::{MaybePlatformPath, DataDirPath}, args::utils::genesis_value_parser, init::init_genesis};
use lattice_storage::NodeStorage;
use tn_adapters::NetworkAdapter;
use tn_network_types::{MockWorkerToPrimary, MockEngineToWorker, PrimaryToPrimary, PrimaryToPrimaryClient, SendCertificateRequest};
use tn_tracing::init_test_tracing;
use tn_types::{
    execution::{LATTICE_GENESIS, TransactionSigned, Signature},
    consensus::{Batch, Parameters, BatchAPI, crypto::traits::KeyPair, Certificate, CertificateAPI, HeaderAPI}
};
use execution_rpc_types::engine::{ExecutionPayload, BatchExecutionPayload};
use lattice_network::{client::NetworkClient, anemo_ext::NetworkExt};
use consensus_metrics::RegistryService;
use prometheus::Registry;
use tokio::time::{sleep, timeout};
use tracing::{debug, info};
use lattice_typed_store::traits::Map;
use fastcrypto::{hash::Hash, signature_service::SignatureService, secp256k1::Secp256k1KeyPair, ed25519::{Ed25519KeyPair, Ed25519Signature}, traits::Signer};
mod common;
use crate::common::{tx_signed_from_raw, pool_transaction_from_raw, bob_raw_tx1, bob_raw_tx3, bob_raw_tx2};

// Tests:
// - happy path
// - fees
// - missing batches for proposed header?
// - invalid batch/block
// - node joins the network & catches up

#[tokio::test]
async fn test_happy_path_from_submit_tx_to_block() -> eyre::Result<(), eyre::Error> {
    // TODO: consolidate tracing fns:
    // let _guard = setup_tracing();
    // telemetry_subscribers::init_for_testing();
    init_test_tracing();

    //=== Execution Layer

    // lattice chain
    let chain = genesis_value_parser("lattice").unwrap();
    let db = create_test_rw_db();
    let chain_genesis = chain.genesis();
    debug!("genesis: \n\n{chain_genesis:?}\n\n");

    // initialize genesis before creating tree
    let genesis_hash = init_genesis(db.clone(), chain.clone())?;
    assert_eq!(genesis_hash, LATTICE_GENESIS);

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

    // original from reth:
    // The size of the broadcast is twice the maximum reorg depth, because at maximum reorg
    // depth at least N blocks must be sent at once.
    let (canon_state_notification_sender, mut canon_state_notification_receiver) =
        tokio::sync::broadcast::channel(tree_config.max_reorg_depth() as usize * 2);

    // create tree to validate batches and track canonical tip
    let blockchain_tree = ShareableBlockchainTree::new(BlockchainTree::new(
        tree_externals,
        canon_state_notification_sender.clone(),
        tree_config,
    )?);

    // TODO: this doesn't work
    blockchain_tree.make_canonical(&genesis_hash).unwrap();
    blockchain_tree.finalize_block(1);

    // setup the blockchain provider - main struct for interacting with the blockchain
    let factory = ProviderFactory::new(Arc::clone(&db), Arc::clone(&chain));
    let blockchain_db = BlockchainProvider::new(factory, blockchain_tree.clone())?;

    let (consensus_engine, engine_handle) = LatticeConsensusEngine::new(
        blockchain_db.clone(),
        db.clone(),
        chain.clone(),
    )?;

    // let genesis_block_by_hash = blockchain_db.block_by_id(tn_types::execution::BlockId::Hash(genesis_hash.into())).unwrap();
    // let genesis_block_by_num = blockchain_db.block_by_id(BlockId::Number(0.into())).unwrap();
    let genesis_block_by_finalized_tag = blockchain_db.block_by_number_or_tag(tn_types::execution::BlockNumberOrTag::Finalized).unwrap();
    debug!("\ngenesis block by finalized tag: {genesis_block_by_finalized_tag:?}\n");
    // let genesis_block_by_earliest_tag = blockchain_db.block_by_number_or_tag(tn_types::execution::BlockNumberOrTag::Earliest)?.unwrap();

    let task_executor = TokioTaskExecutor::default();

    // spawn engine task
    // TODO: remove result - engine should never return?
    task_executor.spawn_critical("execution-engine", Box::pin(async move {
        // TODO: oneshot channel here?
        let _res = consensus_engine.await;
    }));

    let transaction_pool = execution_transaction_pool::Pool::eth_pool(
        EthTransactionValidator::with_additional_tasks(
            blockchain_db.clone(),
            Arc::clone(&chain),
            task_executor.clone(),
            1,
        ),
        Default::default(),
    );

    // TODO: tx pool maintenance on canon chain update
    // - what other tasks need to update with canon chain?
    //
    // E2E - launch rpc server

    // add some transactions - bob is the only account with a seed balance
    let tx_1 = pool_transaction_from_raw(&transaction_pool, bob_raw_tx1());
    let tx_2 = pool_transaction_from_raw(&transaction_pool, bob_raw_tx2());
    let tx_3 = pool_transaction_from_raw(&transaction_pool, bob_raw_tx3());
    let mut tx1_events = transaction_pool.add_transaction_and_subscribe(TransactionOrigin::Local, tx_1).await?;
    let mut tx2_events = transaction_pool.add_transaction_and_subscribe(TransactionOrigin::Local, tx_2).await?;
    let mut tx3_events = transaction_pool.add_transaction_and_subscribe(TransactionOrigin::Local, tx_3).await?;
    assert_eq!(transaction_pool.pending_transactions().len(), 3);

    //=== Consensus Layer for requesting the next batch
    // GIVEN
    // - one primary
    // - one worker
    let parameters = Parameters::default();
    let registry_service = RegistryService::new(Registry::new());
    let fixture = CommitteeFixture::builder()
        .number_of_workers(NonZeroUsize::new(1).unwrap())
        .randomize_ports(true)
        .stake_distribution((5..9).collect()) // give each authority 2 stake
        .build();
    let committee = fixture.committee();
    let worker_cache = fixture.worker_cache();
    let authority = fixture.authorities().next().unwrap();
    let key_pair = authority.keypair();
    let network_key_pair = authority.network_keypair();
    let store = NodeStorage::reopen(temp_dir(), None);
    let network_client = NetworkClient::new_from_keypair(&authority.network_keypair(), &authority.engine_network_keypair().public());






    // create signed mock certificates
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let ids: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (certificates, _next_parents) =
        make_optimal_signed_certificates(1..=1, &genesis, &committee, ids.as_slice());
    let all_certificates: Vec<_> = certificates.into_iter().collect();
    debug!("all certs length: {:?}", all_certificates.len());
    for (num, cert) in all_certificates.iter().enumerate() {
        // let Certificate::V1( certificate ) = cert;
        let header = cert.header();
        let digest = cert.digest();
        let author = header.author();
        let round  = header.round();
        debug!("\ncert {num}:\ndigest:{digest:?}\nauthor:{author:?}\nround:{round:?}\n");
    }
    let round_1_certs = all_certificates[1..].to_vec();
    let (round_2_certs, _parents) = 
        make_optimal_signed_certificates(2..=2, &round_1_certs.iter().map(|x| x.digest()).collect::<BTreeSet<_>>(), &committee, ids.as_slice());

    let round_2_certs = round_2_certs.into_iter().collect::<Vec<_>>();
    let round_2_certs = round_2_certs[1..].to_vec();

    // let round_2_certs = all_certificates[6..].to_vec();
    for cert in round_1_certs {
        store.certificate_store.write(cert.clone()).unwrap();
    }
    for cert in round_2_certs {
        store.certificate_store.write(cert.clone()).unwrap();
    }








    let batch_generator = LatticePayloadJobGenerator::new(
        blockchain_db.clone(),
        transaction_pool.clone(),
        task_executor.clone(),
        LatticePayloadJobGeneratorConfig::default(),
        Arc::clone(&chain),
        network_client.clone(),
    );

    let (batch_builder_service, batch_builder_handle) = LatticePayloadBuilderService::new(batch_generator);
    task_executor.spawn_critical("batch-builder-service", Box::pin(batch_builder_service));

    // TODO: better name for adapter
    let network_adapter = Arc::new(NetworkAdapter::new(batch_builder_handle, engine_handle.clone()));
    network_client.set_worker_to_engine_local_handler(network_adapter.clone());
    network_client.set_primary_to_engine_local_handler(network_adapter.clone());

    let execution_state = Arc::new(LatticeExecutionState::new(network_adapter));

    // spawn primary
    let primary = PrimaryNode::new(parameters.clone(), registry_service.clone());
    primary
        .start(
            key_pair.copy(),
            network_key_pair.copy(),
            committee.clone(),
            worker_cache.clone(),
            network_client.clone(),
            &store,
            execution_state,
        )
        .await
        .unwrap();

    let worker_id = 0;
    let workers = WorkerNodes::new(registry_service, parameters.clone());

    // spawn worker
    workers
        .start(
            key_pair.public().clone(),
            vec![(worker_id, authority.worker(worker_id).keypair().copy())],
            committee.clone(),
            worker_cache,
            network_client.clone(),
            &store,
            LatticeTransactionValidator::new(engine_handle), // temp solution
        )
        .await
        .unwrap();

    // spawn enough receivers to acknowledge the proposed batch
    let mut worker_listener_handles = Vec::new();
    for worker in fixture.authorities().skip(1).map(|a| a.worker(worker_id)) {
        let handle =
            WorkerToWorkerMockServer::spawn(worker.keypair(), worker.info().worker_address.clone());
        worker_listener_handles.push(handle);
    }
    debug!("worker listener handles spawned");

    tokio::task::yield_now().await;

    // spawn enough receivers to acknowledge the proposed header
    let mut primary_network_handles = Vec::new();
    for primary in fixture.authorities().skip(1) {
        let signature_service = SignatureService::new(primary.keypair().copy());
        let (rx, network) = PrimaryToPrimaryMockServer::spawn(
            primary.network_keypair(),
            primary.address().clone(),
            primary.id(),
            signature_service,
        );

        // construct PrimaryToPrimaryClient
        let peer_id = anemo::PeerId(primary.network_public_key().0.to_bytes());
        let peer = network.waiting_peer(peer_id);
        let client = PrimaryToPrimaryClient::new(peer);

        primary_network_handles.push((rx, network, client));
    }

    // // skip 2 so threshold isn't reached until this primary proposes header
    // for network_handle in primary_network_handles.iter().skip(2) {
    //     // primary.1.client
    //     // TODO: make client and call client.send_certificate(request)
    //     // from synchronizer:L802
    //     let (_rx, _network, client) = network_handle;
    //     let request = Request::new(SendCertificateRequest { certificate });
    //     let res = client.send_certificate(request).await;
    //     debug!("client res: {res:?}");
    // }

    debug!("primary listener handles spawned");

    tokio::task::yield_now().await;

    // TODO: better approach? less time?
    // check parameter config
    //
    // wait for batch timer
    debug!("sleeping for 1 sec...");
    sleep(Duration::from_secs(1)).await;

    debug!("sleep over! asserting batch store");

    assert!(!store.batch_store.is_empty());
    
    let mut batch = store.batch_store.values().next().unwrap().unwrap();
    let batch_transactions = batch.transactions_mut();
    assert_eq!(batch_transactions.len(), 3);

    debug!("\n\nbatch length looks good\n\n");

    let batch_tx1 = TransactionSigned::decode(&mut batch_transactions[0].as_ref()).unwrap();
    let batch_tx2 = TransactionSigned::decode(&mut batch_transactions[1].as_ref()).unwrap();
    let batch_tx3 = TransactionSigned::decode(&mut batch_transactions[2].as_ref()).unwrap();
    let expected_tx1 = tx_signed_from_raw(bob_raw_tx1());
    let expected_tx2 = tx_signed_from_raw(bob_raw_tx2());
    let expected_tx3 = tx_signed_from_raw(bob_raw_tx3());
    assert_eq!(batch_tx1, expected_tx1);
    assert_eq!(batch_tx2, expected_tx2);
    assert_eq!(batch_tx3, expected_tx3);
    debug!("\n\nbatch transactions look good\n\n");

    let batch_digest = batch.digest();

    debug!("awaiting tx events...");
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

    debug!("asserting tx pool updated");
    // assert tx pool updated
    assert_eq!(transaction_pool.pool_size().sealed, 3);

    // TODO: check these
    // canon state tracker update
    // execution db
    // primary db
    // certificate db
    // tx pool size

    // wait for canon state change or timeout
    debug!("\n\nwaiting for canon state notification receiver\n\n");
    let duration = Duration::from_secs(10);
    let update = timeout(duration, canon_state_notification_receiver.recv()).await;
    debug!("update: {update:?}");
    assert!(update.is_ok());
    primary.shutdown().await;
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
