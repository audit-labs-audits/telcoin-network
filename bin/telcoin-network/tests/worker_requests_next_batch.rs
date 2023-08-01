// Copyright (c) Telcoin, LLC

use std::sync::Arc;
use execution_blockchain_tree::{TreeExternals, BlockchainTreeConfig, ShareableBlockchainTree, BlockchainTree};
use execution_db::init_db;
use execution_interfaces::{consensus::Consensus, blockchain_tree::BlockchainTreeViewer};
use execution_lattice_consensus::LatticeConsensus;
use execution_provider::{ProviderFactory, providers::BlockchainProvider, BlockReaderIdExt};
use execution_revm::Factory;
use lattice_test_utils::{CommitteeFixture, temp_dir, setup_tracing};
use telcoin_network::{dirs::{MaybePlatformPath, DataDirPath}, args::utils::genesis_value_parser, init::init_genesis};
use lattice_storage::NodeStorage;
use tn_types::{execution::{BlockId, LATTICE_GENESIS, SealedBlock}, consensus::{Batch, Certificate, BatchAPI, MetadataAPI}};
use execution_rpc_types::engine::ExecutionPayload;

#[tokio::test]
async fn test_single_worker_requests_next_batch() -> eyre::Result<(), eyre::Error> {
    let _guard = setup_tracing();
    //=== Execution Layer

    // default chain
    let chain = genesis_value_parser("lattice").unwrap();
    let platform_path = MaybePlatformPath::<DataDirPath>::default();
    let data_dir = platform_path.temp_chain(chain.chain.clone());
    
    // db for EL
    let db = Arc::new(init_db(&data_dir.db_path(), None)?);

    // metrics? - leave separate for now

    // TODO: what genesis info is needed for CL?

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

    // setup the blockchain provider - main struct for interacting with the blockchain
    let factory = ProviderFactory::new(Arc::clone(&db), Arc::clone(&chain));
    let blockchain_db = BlockchainProvider::new(factory, blockchain_tree.clone())?;

    println!("Genesis hash: {:?}", genesis_hash);
    // let genesis_block_by_hash = blockchain_db.block_by_id(BlockId::Hash(genesis_hash.into())).unwrap();
    // let genesis_block_by_num = blockchain_db.block_by_id(BlockId::Number(0.into())).unwrap();
    // let genesis_block_by_finalized_tag = blockchain_db.block_by_number_or_tag(tn_types::execution::BlockNumberOrTag::Finalized).unwrap();
    let genesis_block_by_earliest_tag = blockchain_db.block_by_number_or_tag(tn_types::execution::BlockNumberOrTag::Earliest)?.unwrap();

    // assert_eq!(genesis_block_by_hash, genesis_block_by_num);
    // assert_eq!(genesis_block_by_hash, genesis_block_by_earliest_tag);
    // assert_ne!(genesis_block_by_hash, genesis_block_by_finalized_tag);

    // let sealed_genesis_header_by_earliest_tag = blockchain_db.sealed_header_by_number_or_tag(tn_types::execution::BlockNumberOrTag::Earliest)?.unwrap();
    println!("Genesis block: {:?}", genesis_block_by_earliest_tag);
    let sealed_genesis_block = genesis_block_by_earliest_tag.seal_slow();
    println!("\nsealed genesis block: {:?}", sealed_genesis_block);
    let genesis_payload: ExecutionPayload = sealed_genesis_block.into();
    println!("\ngenesis payload: {:?}", genesis_payload);
    let genesis_batch: Batch = genesis_payload.into();
    println!("\ngenesis batch: {:?}", genesis_batch);

    //=== Consensus Layer
    // following along with crates/consensus/executor/tests/consensus_integration_tests.rs

    // storage for CL
    let storage = NodeStorage::reopen(temp_dir(), None);

    let consensus_store = storage.consensus_store;
    let certificate_store = storage.certificate_store;

    // create 4-node committee
    let fixture = CommitteeFixture::builder()
        .randomize_ports(true)
        .build();
    let committee = fixture.committee();

    // let genesis_certificates = Certificate::genesis_with_payload(&committee, genesis_batch);
    // for (i, c) in genesis_certificates.iter().enumerate() {
    //     println!("certificate {i:?}: \n{c:?}");
    // }

    // should genesis be a separate message type for engine?
    // ex)
    // - EngineMessage::genesis(certificate)
    // - this message type calls init_genesis() for EL in match statement
    // similar to ForkChoiceUpdated to make it canonical
    //
    // returns the payload builder sync receiver so worker waits for the first
    // payload to build ?
    // 
    // I think genesis message might make the most sense since the genesis struct 
    // is unique.

    // for now, just use empty cert
    let genesis_certificate = Certificate::genesis(&committee);

    // see: crates/consensus/primary/src/tests/block_remover_tests.rs for `populate_genesis()` helper method



    Ok(())
}
