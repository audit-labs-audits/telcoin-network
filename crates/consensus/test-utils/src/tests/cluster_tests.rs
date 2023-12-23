// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::cluster::Cluster;
use reth::providers::BlockReader;
use std::time::Duration;
use tn_types::test_utils::ensure_test_environment;

#[tokio::test]
async fn basic_cluster_setup() {
    ensure_test_environment();
    reth_tracing::init_test_tracing();
    let mut cluster = Cluster::new(None);

    // start the cluster will all the possible nodes
    cluster.start(Some(4), Some(1), None).await;

    // give some time for nodes to bootstrap
    tokio::time::sleep(Duration::from_secs(2)).await;

    // fetch all the running authorities
    let authorities = cluster.authorities().await;

    assert_eq!(authorities.len(), 4);

    let mut block_vec = Vec::new();

    // fetch their workers transactions address
    for authority in cluster.authorities().await {
        assert_eq!(authority.worker_transaction_addresses().await.len(), 1);
        // collect first 3 executed blocks
        let el = authority.execution_components().await.expect("execution layer running");
        let db = el.get_provider().await;
        let blocks = db.block_range(0..=3).expect("at least 3 blocks");
        block_vec.push(blocks.clone());
    }

    // assert first three blocks are the same for all authorities
    assert!(block_vec.windows(2).all(|w| w[0] == w[1]));

    // now stop all authorities
    for id in 0..4 {
        cluster.stop_node(id).await;
    }

    println!("sleeping...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // No authority should still run
    assert!(cluster.authorities().await.is_empty());
}
