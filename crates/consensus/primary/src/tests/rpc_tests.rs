//! RPC tests

use std::time::Duration;
use tn_network::WorkerRpc;
use tn_network_types::RequestBatchesRequest;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::cluster::Cluster;
use tn_types::AuthorityIdentifier;

#[tokio::test]
async fn test_server_authorizations() {
    // Set up primaries and workers with a committee.
    let mut test_cluster = Cluster::new(MemDatabase::default);
    test_cluster.start(Some(4), Some(1), None).await;
    // allow enough time for peers to establish connections
    tokio::time::sleep(Duration::from_secs(3)).await;

    let this_authority_id = test_cluster.authorities().await.first().unwrap().name;
    let worker_network =
        test_cluster.authorities().await.first().unwrap().worker_network(0).await.unwrap();
    let test_committee = test_cluster.committee.clone();
    let test_worker_cache = test_cluster.fixture().worker_cache().clone();

    // Reachable to primaries in the same committee.
    {
        // ensures this authority isn't `1`
        //
        // this test occasionally failed because this primary was assigned id `1`
        let target = AuthorityIdentifier(1);
        let target_id = if this_authority_id == target { AuthorityIdentifier(2) } else { target };

        // retrieve peer's network key
        let target_authority = test_committee.authority(&target_id).unwrap();
        let worker_target_name = test_worker_cache
            .workers
            .get(target_authority.protocol_key())
            .unwrap()
            .0
            .get(&0)
            .unwrap()
            .name
            .clone();
        let request = anemo::Request::new(RequestBatchesRequest::default())
            .with_timeout(Duration::from_secs(5));
        worker_network.request_batches(&worker_target_name, request).await.unwrap();
    }

    // Set up primaries and workers with a another committee.
    let mut unreachable_cluster = Cluster::new(MemDatabase::default);
    unreachable_cluster.start(Some(4), Some(1), None).await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    // test_client should not reach unreachable_authority.
    {
        let unreachable_committee = unreachable_cluster.committee.clone();
        let unreachable_worker_cache = unreachable_cluster.fixture().worker_cache().clone();

        let unreachable_authority =
            unreachable_committee.authority(&AuthorityIdentifier(0)).unwrap();

        let worker_target_name = unreachable_worker_cache
            .workers
            .get(unreachable_authority.protocol_key())
            .unwrap()
            .0
            .get(&0)
            .unwrap()
            .name
            .clone();
        let request = anemo::Request::new(RequestBatchesRequest::default())
            .with_timeout(Duration::from_secs(5));
        // Removing the AllowedPeers RequireAuthorizationLayer for workers should make this succeed.
        assert!(worker_network.request_batches(&worker_target_name, request).await.is_err());
    }
}
