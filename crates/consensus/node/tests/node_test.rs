// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use consensus_metrics::RegistryService;
use fastcrypto::traits::KeyPair;
use lattice_network::client::NetworkClient;
use lattice_node::{
    execution_state::SimpleExecutionState, primary_node::PrimaryNode, worker_node::WorkerNodes,
};
use lattice_storage::NodeStorage;
use lattice_test_utils::{temp_dir, CommitteeFixture};
use lattice_worker::TrivialTransactionValidator;
use prometheus::Registry;
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use tn_types::consensus::Parameters;
use tokio::{sync::mpsc::channel, time::sleep};

#[tokio::test]
async fn simple_primary_worker_node_start_stop() {
    telemetry_subscribers::init_for_testing();

    // GIVEN
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

    let (tx_confirmation, _rx_confirmation) = channel(10);
    let execution_state = Arc::new(SimpleExecutionState::new(tx_confirmation));

    // WHEN
    let primary_node = PrimaryNode::new(parameters.clone(), true, registry_service.clone());

    // channel for proposer and EL
    let (el_sender, mut el_receiver) = tokio::sync::mpsc::channel(1);

    primary_node
        .start(
            key_pair.copy(),
            network_key_pair.copy(),
            committee.clone(),
            worker_cache.clone(),
            client.clone(),
            &store,
            execution_state,
            el_sender,
        )
        .await
        .unwrap();



    // AND
    let workers = WorkerNodes::new(registry_service, parameters.clone());

    workers
        .start(
            key_pair.public().clone(),
            vec![(0, authority.worker(0).keypair().copy())],
            committee,
            worker_cache,
            client,
            &store,
            TrivialTransactionValidator::default(),
        )
        .await
        .unwrap();

    tokio::task::yield_now().await;

    sleep(Duration::from_secs(2)).await;

    // THEN
    // unfortunately we don't have strong signal to check whether a node is up and running complete,
    // so just use the admin endpoint to check it's running
    let client = reqwest::Client::new();
    let response = client
        .get(format!(
            "http://127.0.0.1:{}/known_peers",
            parameters.network_admin_server.worker_network_admin_server_base_port
        ))
        .send()
        .await
        .unwrap();
    let result = response.text().await.unwrap();

    assert_ne!(result, "");

    // simulate el - clear channel before shutdown
    let (_h, reply) = el_receiver.recv().await.expect("channel still open");
    reply.send(()).expect("channel open");

    // AND
    primary_node.shutdown().await;
    workers.shutdown().await;
}

#[tokio::test]
async fn primary_node_restart() {
    telemetry_subscribers::init_for_testing();

    // GIVEN
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

    let (tx_confirmation, _rx_confirmation) = channel(10);
    let execution_state = Arc::new(SimpleExecutionState::new(tx_confirmation));

    // AND
    let primary_node = PrimaryNode::new(parameters.clone(), true, registry_service.clone());
    // channel for proposer and EL
    let (el_sender, mut el_receiver) = tokio::sync::mpsc::channel(1);

    primary_node
        .start(
            key_pair.copy(),
            network_key_pair.copy(),
            committee.clone(),
            worker_cache.clone(),
            client.clone(),
            &store,
            execution_state.clone(),
            el_sender.clone(),
        )
        .await
        .unwrap();

    tokio::task::yield_now().await;

    sleep(Duration::from_secs(2)).await;

    // simulate el - clear channel before shutdown
    let (_h, reply) = el_receiver.recv().await.expect("channel still open");
    reply.send(()).expect("channel open");

    // WHEN
    primary_node.shutdown().await;

    // AND start again the node
    primary_node
        .start(
            key_pair.copy(),
            network_key_pair.copy(),
            committee.clone(),
            worker_cache.clone(),
            client.clone(),
            &store,
            execution_state,
            el_sender,
        )
        .await
        .unwrap();

    tokio::task::yield_now().await;

    sleep(Duration::from_secs(2)).await;

    // THEN can query/confirm that node is running
    let client = reqwest::Client::new();
    let response = client
        .get(format!(
            "http://127.0.0.1:{}/known_peers",
            parameters.network_admin_server.primary_network_admin_server_port
        ))
        .send()
        .await
        .unwrap();
    let result = response.text().await.unwrap();

    assert_ne!(result, "");
}
