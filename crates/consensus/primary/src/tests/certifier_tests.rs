//! Certifier tests

use super::*;
use crate::ConsensusBus;
use fastcrypto::traits::KeyPair;
use rand::{rngs::StdRng, SeedableRng};
use std::{collections::HashMap, num::NonZeroUsize};
use tn_network_libp2p::types::NetworkCommand;
use tn_network_types::{MockPrimaryToPrimary, PrimaryToPrimaryServer, RequestVoteResponse};
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::CommitteeFixture;
use tn_types::{BlsKeypair, Notifier, SignatureVerificationState, TnSender};
use tokio::sync::mpsc;

#[tokio::test(flavor = "current_thread")]
async fn propose_header_to_form_certificate() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().last().unwrap();
    let id = primary.id();

    // Create a fake header.
    let proposed_header = primary.header(&committee);

    // Set up network handle- this is all we need to simulate then network for the certifier.
    let (sender, mut network_rx) = mpsc::channel(100);
    let network: NetworkHandle<PrimaryRequest, PrimaryResponse> = NetworkHandle::new(sender);

    // Set up remote primaries responding with votes.
    let mut peer_votes = HashMap::new();
    for peer in fixture.authorities().filter(|a| a.id() != id) {
        let name = peer.id();
        let vote = Vote::new(&proposed_header, &name, peer.consensus_config().key_config()).await;
        let id = primary.consensus_config().peer_id_for_authority(&name).unwrap();
        peer_votes.insert(id, vote);
    }

    let cb = ConsensusBus::new();
    let mut rx_new_certificates = cb.new_certificates().subscribe();
    // Spawn the core.
    let synchronizer = Arc::new(Synchronizer::new(primary.consensus_config(), &cb));

    let task_manager = TaskManager::default();
    synchronizer.spawn(&task_manager);
    Certifier::spawn(
        primary.consensus_config(),
        cb.clone(),
        synchronizer,
        network.clone(),
        &task_manager,
    );

    // Propose header and ensure that a certificate is formed by pulling it out of the
    // consensus channel.
    let proposed_digest = proposed_header.digest();
    cb.headers().send(proposed_header).await.unwrap();
    // Wait for the vote requests and send the votes back.
    while let Some(req) = network_rx.recv().await {
        match req {
            NetworkCommand::SendRequest { peer, request, reply } => match request {
                PrimaryRequest::Vote { header: _, parents: _ } => {
                    if let Some(vote) = peer_votes.remove(&peer) {
                        reply.send(Ok(PrimaryResponse::Vote(vote))).unwrap();
                    }
                }
                _ => {}
            },
            _ => {}
        }
        if peer_votes.is_empty() {
            break;
        }
    }
    let certificate = tokio::time::timeout(Duration::from_secs(10), rx_new_certificates.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(certificate.header().digest(), proposed_digest);
    assert!(matches!(
        certificate.signature_verification_state(),
        SignatureVerificationState::VerifiedDirectly(_)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn propose_header_failure() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().last().unwrap();

    // Create a fake header.
    let proposed_header = primary.header(&committee);

    // Set up network handle- this is all we need to simulate then network for the certifier.
    let (sender, mut network_rx) = mpsc::channel(100);
    let network: NetworkHandle<PrimaryRequest, PrimaryResponse> = NetworkHandle::new(sender);

    let cb = ConsensusBus::new();
    let mut rx_new_certificates = cb.new_certificates().subscribe();
    // Spawn the core.
    let synchronizer = Arc::new(Synchronizer::new(primary.consensus_config(), &cb));

    let task_manager = TaskManager::default();
    synchronizer.spawn(&task_manager);
    Certifier::spawn(
        primary.consensus_config(),
        cb.clone(),
        synchronizer,
        network.clone(),
        &task_manager,
    );

    // Propose header and verify we get no certificate back.
    cb.headers().send(proposed_header).await.unwrap();

    // Wait for the vote requests and send back errors.
    let mut i = 0;
    while let Some(req) = network_rx.recv().await {
        match req {
            NetworkCommand::SendRequest { peer: _, request, reply } => match request {
                PrimaryRequest::Vote { header: _, parents: _ } => {
                    reply.send(Err(NetworkError::RPCError("bad vote".to_string()))).unwrap();
                }
                _ => {}
            },
            _ => {}
        }
        i += 1;
        if i >= 3 {
            break;
        }
    }

    if let Ok(result) =
        tokio::time::timeout(Duration::from_secs(5), rx_new_certificates.recv()).await
    {
        panic!("expected no certificate to form; got {result:?}");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn propose_header_scenario_with_bad_sigs() {
    // expect cert if less than 2 byzantines, otherwise no cert
    run_vote_aggregator_with_param(6, 0, true).await;
    run_vote_aggregator_with_param(6, 1, true).await;
    run_vote_aggregator_with_param(6, 2, false).await;

    // expect cert if less than 2 byzantines, otherwise no cert
    run_vote_aggregator_with_param(4, 0, true).await;
    run_vote_aggregator_with_param(4, 1, true).await;
    run_vote_aggregator_with_param(4, 2, false).await;
}

async fn run_vote_aggregator_with_param(
    committee_size: usize,
    num_byzantine: usize,
    expect_cert: bool,
) {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .committee_size(NonZeroUsize::new(committee_size).unwrap())
        .randomize_ports(true)
        .build();

    let committee = fixture.committee();
    let primary = fixture.authorities().last().unwrap();
    let id: AuthorityIdentifier = primary.id();

    // Create a fake header.
    let proposed_header = primary.header(&committee);

    // Set up network.
    let network = primary.new_network(anemo::Router::new());

    // Set up remote primaries responding with votes.
    let mut peer_networks = Vec::new();
    for (i, peer) in fixture.authorities().filter(|a| a.id() != id).enumerate() {
        let address = committee.primary(&peer.primary_public_key()).unwrap();
        let name = peer.id();
        // Create bad signature for a number of byzantines.
        let vote = if i < num_byzantine {
            let bad_key = BlsKeypair::generate(&mut StdRng::from_seed([0; 32]));
            Vote::new_with_signer(&proposed_header, &name, &bad_key)
        } else {
            Vote::new(&proposed_header, &name, peer.consensus_config().key_config()).await
        };
        let mut mock_server = MockPrimaryToPrimary::new();
        let mut mock_seq = mockall::Sequence::new();
        mock_server.expect_request_vote().times(1).in_sequence(&mut mock_seq).return_once(
            move |_request| {
                Ok(anemo::Response::new(RequestVoteResponse {
                    vote: Some(vote),
                    missing: Vec::new(),
                }))
            },
        );
        let routes = anemo::Router::new().add_rpc_service(PrimaryToPrimaryServer::new(mock_server));
        peer_networks.push(peer.new_network(routes));

        let address = address.to_anemo_address().unwrap();
        let peer_id = anemo::PeerId(peer.primary_network_keypair().public().0.to_bytes());
        network.connect_with_peer_id(address, peer_id).await.unwrap();
    }

    let cb = ConsensusBus::new();
    let mut rx_new_certificates = cb.new_certificates().subscribe();
    // Spawn the core.
    let synchronizer = Arc::new(Synchronizer::new(primary.consensus_config(), &cb));
    let task_manager = TaskManager::default();
    synchronizer.spawn(&task_manager);
    Certifier::spawn(
        primary.consensus_config(),
        cb.clone(),
        synchronizer,
        NetworkHandle::new_for_test(),
        &task_manager,
    );

    // Send a proposed header.
    let proposed_digest = proposed_header.digest();
    cb.headers().send(proposed_header).await.unwrap();

    if expect_cert {
        // A cert is expected, checks that the header digest matches.
        let certificate = tokio::time::timeout(Duration::from_secs(5), rx_new_certificates.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(certificate.header().digest(), proposed_digest);
    } else {
        // A cert is not expected, checks that it times out without forming the cert.
        assert!(tokio::time::timeout(Duration::from_secs(5), rx_new_certificates.recv())
            .await
            .is_err());
    }
}

#[tokio::test]
async fn test_shutdown_core() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let primary = fixture.authorities().next().unwrap();

    let cb = ConsensusBus::new();
    // Make a synchronizer for the core.
    let synchronizer = Arc::new(Synchronizer::new(primary.consensus_config(), &cb));

    // Spawn the core.
    let mut task_manager = TaskManager::default();
    synchronizer.spawn(&task_manager);
    Certifier::spawn(
        primary.consensus_config(),
        cb.clone(),
        synchronizer.clone(),
        NetworkHandle::new_for_test(),
        &task_manager,
    );

    // Shutdown the core.
    fixture.notify_shutdown();
    task_manager.join(Notifier::default()).await;
}
