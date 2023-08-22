// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{
    block_synchronizer::{handler::BlockSynchronizerHandler, BlockSynchronizer},
    block_waiter::BlockWaiter,
    certificate_fetcher::CertificateFetcher,
    certifier::Certifier,
    grpc_server::ConsensusAPIGrpc,
    metrics::{initialise_metrics, PrimaryMetrics},
    proposer::{OurDigestMessage, Proposer},
    state_handler::StateHandler,
    synchronizer::Synchronizer,
    BlockRemover,
};
use anemo::{
    codegen::InboundRequestLayer,
    types::{Address, PeerInfo},
    Network, PeerId,
};
use anemo_tower::{
    auth::{AllowedPeers, RequireAuthorizationLayer},
    callback::CallbackLayer,
    inflight_limit, rate_limit,
    set_header::{SetRequestHeaderLayer, SetResponseHeaderLayer},
    trace::{DefaultMakeSpan, DefaultOnFailure, TraceLayer},
};
use async_trait::async_trait;
use consensus_metrics::{
    metered_channel::{channel_with_total, Receiver, Sender},
    monitored_scope, spawn_monitored_task,
};
use consensus_network::{multiaddr::Protocol, Multiaddr};
use fastcrypto::{
    hash::Hash,
    signature_service::SignatureService,
    traits::{KeyPair as _, ToFromBytes},
};
use lattice_consensus::{ConsensusRound, dag::DagHandle};
use lattice_network::{
    client::NetworkClient,
    epoch_filter::{AllowedEpoch, EPOCH_HEADER_KEY},
    failpoints::FailpointsMakeCallbackHandler,
    metrics::MetricsMakeCallbackHandler,
};
use lattice_storage::{
    CertificateStore, HeaderStore, PayloadStore, ProposerStore, VoteDigestStore,
};
use parking_lot::Mutex;
use prometheus::Registry;
use std::{
    cmp::Reverse,
    collections::{btree_map::Entry, BTreeMap, BTreeSet, BinaryHeap, HashMap},
    net::Ipv4Addr,
    sync::Arc,
    thread::sleep,
    time::Duration,
};
use tn_types::{
    consensus::{
        Authority, AuthorityIdentifier, Committee, Parameters, WorkerCache,
        crypto,
        crypto::{
            traits::EncodeDecodeBase64, AuthorityKeyPair, NetworkKeyPair, NetworkPublicKey, AuthoritySignature,
        },
        error::{DagError, DagResult},
        now, Certificate, CertificateAPI, CertificateDigest, FetchCertificatesRequest,
        FetchCertificatesResponse, GetCertificatesRequest, GetCertificatesResponse, Header,
        HeaderAPI, MetadataAPI, PayloadAvailabilityRequest, PayloadAvailabilityResponse,
        PreSubscribedBroadcastSender, PrimaryToPrimary, PrimaryToPrimaryServer, RequestVoteRequest,
        RequestVoteResponse, Round, SendCertificateRequest, SendCertificateResponse, Vote,
        VoteInfoAPI, WorkerOthersBatchMessage, WorkerOwnBatchMessage, WorkerToPrimary,
        WorkerToPrimaryServer,
    },
    ensure,
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::Instant,
};
use tower::ServiceBuilder;
use tracing::{debug, error, info, instrument, warn};

/// The default channel capacity for each channel of the primary.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// The number of shutdown receivers to create on startup. We need one per component loop.
pub const NUM_SHUTDOWN_RECEIVERS: u64 = 27;

/// Maximum duration to fetch certificates from local storage.
const FETCH_CERTIFICATES_MAX_HANDLER_TIME: Duration = Duration::from_secs(10);

pub struct Primary;

impl Primary {
    // Spawns the primary and returns the JoinHandles of its tasks, as well as a metered receiver
    // for the Consensus.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        authority: Authority,
        signer: AuthorityKeyPair,
        network_signer: NetworkKeyPair,
        committee: Committee,
        worker_cache: WorkerCache,
        parameters: Parameters,
        client: NetworkClient,
        header_store: HeaderStore,
        certificate_store: CertificateStore,
        proposer_store: ProposerStore,
        payload_store: PayloadStore,
        vote_digest_store: VoteDigestStore,
        tx_new_certificates: Sender<Certificate>,
        rx_committed_certificates: Receiver<(Round, Vec<Certificate>)>,
        rx_consensus_round_updates: watch::Receiver<ConsensusRound>,
        dag: Option<Arc<DagHandle>>,
        tx_shutdown: &mut PreSubscribedBroadcastSender,
        tx_committed_certificates: Sender<(Round, Vec<Certificate>)>,
        registry: &Registry,
        tx_execute_header: mpsc::Sender<(Header, oneshot::Sender<()>)>,
    ) -> Vec<JoinHandle<()>> {
        // Write the parameters to the logs.
        parameters.tracing();

        // Some info statements
        let own_peer_id = PeerId(network_signer.public().0.to_bytes());
        info!(
            "Boot primary node with peer id {} and public key {}",
            own_peer_id,
            authority.protocol_key().encode_base64(),
        );

        // Initialize the metrics
        let metrics = initialise_metrics(registry);
        let endpoint_metrics = metrics.endpoint_metrics.unwrap();
        let mut primary_channel_metrics = metrics.primary_channel_metrics.unwrap();
        let inbound_network_metrics = Arc::new(metrics.inbound_network_metrics.unwrap());
        let outbound_network_metrics = Arc::new(metrics.outbound_network_metrics.unwrap());
        let node_metrics = Arc::new(metrics.node_metrics.unwrap());
        let network_connection_metrics = metrics.network_connection_metrics.unwrap();

        let (tx_our_digests, rx_our_digests) = channel_with_total(
            CHANNEL_CAPACITY,
            &primary_channel_metrics.tx_our_digests,
            &primary_channel_metrics.tx_our_digests_total,
        );
        let (tx_parents, rx_parents) = channel_with_total(
            CHANNEL_CAPACITY,
            &primary_channel_metrics.tx_parents,
            &primary_channel_metrics.tx_parents_total,
        );
        let (tx_headers, rx_headers) = channel_with_total(
            CHANNEL_CAPACITY,
            &primary_channel_metrics.tx_headers,
            &primary_channel_metrics.tx_headers_total,
        );
        let (tx_certificate_fetcher, rx_certificate_fetcher) = channel_with_total(
            CHANNEL_CAPACITY,
            &primary_channel_metrics.tx_certificate_fetcher,
            &primary_channel_metrics.tx_certificate_fetcher_total,
        );
        let (tx_block_synchronizer_commands, rx_block_synchronizer_commands) = channel_with_total(
            CHANNEL_CAPACITY,
            &primary_channel_metrics.tx_block_synchronizer_commands,
            &primary_channel_metrics.tx_block_synchronizer_commands_total,
        );
        let (tx_committed_own_headers, rx_committed_own_headers) = channel_with_total(
            CHANNEL_CAPACITY,
            &primary_channel_metrics.tx_committed_own_headers,
            &primary_channel_metrics.tx_committed_own_headers_total,
        );

        // we need to hack the gauge from this consensus channel into the primary registry
        // This avoids a cyclic dependency in the initialization of consensus and primary
        let committed_certificates_gauge = tx_committed_certificates.gauge().clone();
        primary_channel_metrics.replace_registered_committed_certificates_metric(
            registry,
            Box::new(committed_certificates_gauge),
        );

        let new_certificates_gauge = tx_new_certificates.gauge().clone();
        primary_channel_metrics
            .replace_registered_new_certificates_metric(registry, Box::new(new_certificates_gauge));

        let (tx_narwhal_round_updates, rx_narwhal_round_updates) = watch::channel(0u64);
        let (tx_synchronizer_network, rx_synchronizer_network) = oneshot::channel();

        let synchronizer = Arc::new(Synchronizer::new(
            authority.id(),
            committee.clone(),
            worker_cache.clone(),
            parameters.gc_depth,
            client.clone(),
            certificate_store.clone(),
            payload_store.clone(),
            tx_certificate_fetcher,
            tx_new_certificates,
            tx_parents,
            rx_consensus_round_updates.clone(),
            rx_synchronizer_network,
            dag.clone(),
            node_metrics.clone(),
            &primary_channel_metrics,
        ));

        let signature_service = SignatureService::new(signer);

        // Spawn the network receiver listening to messages from the other primaries.
        let address = authority.primary_address();
        let address =
            address.replace(0, |_protocol| Some(Protocol::Ip4(Ipv4Addr::UNSPECIFIED))).unwrap();
        let mut primary_service = PrimaryToPrimaryServer::new(PrimaryReceiverHandler {
            authority_id: authority.id(),
            committee: committee.clone(),
            worker_cache: worker_cache.clone(),
            synchronizer: synchronizer.clone(),
            signature_service: signature_service.clone(),
            header_store: header_store.clone(),
            certificate_store: certificate_store.clone(),
            payload_store: payload_store.clone(),
            vote_digest_store,
            rx_narwhal_round_updates,
            parent_digests: Default::default(),
            metrics: node_metrics.clone(),
        })
        // Allow only one inflight RequestVote RPC at a time per peer.
        // This is required for correctness.
        .add_layer_for_request_vote(InboundRequestLayer::new(
            inflight_limit::InflightLimitLayer::new(1, inflight_limit::WaitMode::ReturnError),
        ))
        // Allow only one inflight FetchCertificates RPC at a time per peer.
        // These are already a batch request; an individual peer should never need more than one.
        .add_layer_for_fetch_certificates(InboundRequestLayer::new(
            inflight_limit::InflightLimitLayer::new(1, inflight_limit::WaitMode::ReturnError),
        ));

        // Apply other rate limits from configuration as needed.
        if let Some(limit) = parameters.anemo.send_certificate_rate_limit {
            primary_service = primary_service.add_layer_for_send_certificate(
                InboundRequestLayer::new(rate_limit::RateLimitLayer::new(
                    governor::Quota::per_second(limit),
                    rate_limit::WaitMode::Block,
                )),
            );
        }
        if let Some(limit) = parameters.anemo.get_payload_availability_rate_limit {
            primary_service = primary_service.add_layer_for_get_payload_availability(
                InboundRequestLayer::new(rate_limit::RateLimitLayer::new(
                    governor::Quota::per_second(limit),
                    rate_limit::WaitMode::Block,
                )),
            );
        }
        if let Some(limit) = parameters.anemo.get_certificates_rate_limit {
            primary_service = primary_service.add_layer_for_get_certificates(
                InboundRequestLayer::new(rate_limit::RateLimitLayer::new(
                    governor::Quota::per_second(limit),
                    rate_limit::WaitMode::Block,
                )),
            );
        }

        let worker_receiver_handler =
            WorkerReceiverHandler { tx_our_digests, payload_store: payload_store.clone() };

        client.set_worker_to_primary_local_handler(Arc::new(worker_receiver_handler.clone()));

        let worker_service = WorkerToPrimaryServer::new(worker_receiver_handler);

        let addr = address.to_anemo_address().unwrap();

        let epoch_string: String = committee.epoch().to_string();

        let our_worker_peer_ids = worker_cache
            .our_workers(authority.protocol_key())
            .unwrap()
            .into_iter()
            .map(|worker_info| PeerId(worker_info.name.0.to_bytes()));
        let worker_to_primary_router = anemo::Router::new()
            .add_rpc_service(worker_service)
            // Add an Authorization Layer to ensure that we only service requests from our workers
            .route_layer(RequireAuthorizationLayer::new(AllowedPeers::new(our_worker_peer_ids)))
            .route_layer(RequireAuthorizationLayer::new(AllowedEpoch::new(epoch_string.clone())));

        let routes = anemo::Router::new()
            .add_rpc_service(primary_service)
            .route_layer(RequireAuthorizationLayer::new(AllowedEpoch::new(epoch_string.clone())))
            .merge(worker_to_primary_router);

        let service = ServiceBuilder::new()
            .layer(
                TraceLayer::new_for_server_errors()
                    .make_span_with(DefaultMakeSpan::new().level(tracing::Level::INFO))
                    .on_failure(DefaultOnFailure::new().level(tracing::Level::WARN)),
            )
            .layer(CallbackLayer::new(MetricsMakeCallbackHandler::new(
                inbound_network_metrics,
                parameters.anemo.excessive_message_size(),
            )))
            .layer(CallbackLayer::new(FailpointsMakeCallbackHandler::new()))
            .layer(SetResponseHeaderLayer::overriding(
                EPOCH_HEADER_KEY.parse().unwrap(),
                epoch_string.clone(),
            ))
            .service(routes);

        let outbound_layer = ServiceBuilder::new()
            .layer(
                TraceLayer::new_for_client_and_server_errors()
                    .make_span_with(DefaultMakeSpan::new().level(tracing::Level::INFO))
                    .on_failure(DefaultOnFailure::new().level(tracing::Level::WARN)),
            )
            .layer(CallbackLayer::new(MetricsMakeCallbackHandler::new(
                outbound_network_metrics,
                parameters.anemo.excessive_message_size(),
            )))
            .layer(CallbackLayer::new(FailpointsMakeCallbackHandler::new()))
            .layer(SetRequestHeaderLayer::overriding(
                EPOCH_HEADER_KEY.parse().unwrap(),
                epoch_string,
            ))
            .into_inner();

        let anemo_config = {
            let mut quic_config = anemo::QuicConfig::default();
            // Allow more concurrent streams for burst activity.
            quic_config.max_concurrent_bidi_streams = Some(10_000);
            // Increase send and receive buffer sizes on the primary, since the primary also
            // needs to fetch payloads.
            // With 200MiB buffer size and ~500ms RTT, the max throughput ~400MiB/s.
            quic_config.stream_receive_window = Some(100 << 20);
            quic_config.receive_window = Some(200 << 20);
            quic_config.send_window = Some(200 << 20);
            quic_config.crypto_buffer_size = Some(1 << 20);
            quic_config.socket_receive_buffer_size = Some(20 << 20);
            quic_config.socket_send_buffer_size = Some(20 << 20);
            quic_config.allow_failed_socket_buffer_size_setting = true;
            quic_config.max_idle_timeout_ms = Some(30_000);
            // Enable keep alives every 5s
            quic_config.keep_alive_interval_ms = Some(5_000);
            let mut config = anemo::Config::default();
            config.quic = Some(quic_config);
            // Set the max_frame_size to be 2 GB to work around the issue of there being too many
            // delegation events in the epoch change txn.
            config.max_frame_size = Some(2 << 30);
            // Set a default timeout of 300s for all RPC requests
            config.inbound_request_timeout_ms = Some(300_000);
            config.outbound_request_timeout_ms = Some(300_000);
            config.shutdown_idle_timeout_ms = Some(1_000);
            config.connectivity_check_interval_ms = Some(2_000);
            config.connection_backoff_ms = Some(1_000);
            config.max_connection_backoff_ms = Some(20_000);
            config
        };

        let network;
        let mut retries_left = 90;

        loop {
            let network_result = anemo::Network::bind(addr.clone())
                .server_name("lattice")
                .private_key(network_signer.copy().private().0.to_bytes())
                .config(anemo_config.clone())
                .outbound_request_layer(outbound_layer.clone())
                .start(service.clone());
            match network_result {
                Ok(n) => {
                    network = n;
                    break
                }
                Err(_) => {
                    retries_left -= 1;

                    if retries_left <= 0 {
                        panic!("Failed to initialize Network!");
                    }
                    error!(
                        "Address {} should be available for the primary Narwhal service, retrying in one second",
                        addr
                    );
                    sleep(Duration::from_secs(1));
                }
            }
        }
        if tx_synchronizer_network.send(network.clone()).is_err() {
            panic!("Failed to send Network to Synchronizer!");
        }

        info!("Primary {} listening on {}", authority.id(), address);

        let mut peer_types = HashMap::new();

        // Add my workers
        for worker in worker_cache.our_workers(authority.protocol_key()).unwrap() {
            let (peer_id, address) =
                Self::add_peer_in_network(&network, worker.name, &worker.worker_address);
            peer_types.insert(peer_id, "our_worker".to_string());
            info!("Adding our worker with peer id {} and address {}", peer_id, address);
        }

        // Add others workers
        for (_, worker) in worker_cache.others_workers(authority.protocol_key()) {
            let (peer_id, address) =
                Self::add_peer_in_network(&network, worker.name, &worker.worker_address);
            peer_types.insert(peer_id, "other_worker".to_string());
            info!("Adding others worker with peer id {} and address {}", peer_id, address);
        }

        // Add other primaries
        let primaries = committee
            .others_primaries_by_id(authority.id())
            .into_iter()
            .map(|(_, address, network_key)| (network_key, address));

        for (public_key, address) in primaries {
            let (peer_id, address) = Self::add_peer_in_network(&network, public_key, &address);
            peer_types.insert(peer_id, "other_primary".to_string());
            info!("Adding others primaries with peer id {} and address {}", peer_id, address);
        }

        let (connection_monitor_handle, _) =
            lattice_network::connectivity::ConnectionMonitor::spawn(
                network.downgrade(),
                network_connection_metrics,
                peer_types,
                Some(tx_shutdown.subscribe()),
            );

        info!(
            "Primary {} listening to network admin messages on 127.0.0.1:{}",
            authority.id(),
            parameters.network_admin_server.primary_network_admin_server_port
        );

        let admin_handles = lattice_network::admin::start_admin_server(
            parameters.network_admin_server.primary_network_admin_server_port,
            network.clone(),
            tx_shutdown.subscribe(),
        );

        let core_handle = Certifier::spawn(
            authority.id(),
            committee.clone(),
            header_store.clone(),
            certificate_store.clone(),
            synchronizer.clone(),
            signature_service,
            tx_shutdown.subscribe(),
            rx_headers,
            node_metrics.clone(),
            network.clone(),
        );

        // The `CertificateFetcher` waits to receive all the ancestors of a certificate before
        // looping it back to the `Synchronizer` for further processing.
        let certificate_fetcher_handle = CertificateFetcher::spawn(
            authority.id(),
            committee.clone(),
            network.clone(),
            certificate_store.clone(),
            rx_consensus_round_updates,
            tx_shutdown.subscribe(),
            rx_certificate_fetcher,
            synchronizer.clone(),
            node_metrics.clone(),
        );

        // When the `Synchronizer` collects enough parent certificates, the `Proposer` generates
        // a new header with new batch digests from our workers and sends it to the `Certifier`.
        let proposer_handle = Proposer::spawn(
            authority.id(),
            committee.clone(),
            proposer_store,
            parameters.header_num_of_batches_threshold,
            parameters.max_header_num_of_batches,
            parameters.max_header_delay,
            parameters.min_header_delay,
            None,
            tx_shutdown.subscribe(),
            rx_parents,
            rx_our_digests,
            tx_headers,
            tx_narwhal_round_updates,
            rx_committed_own_headers,
            node_metrics,
            tx_execute_header,
        );

        let mut handles = vec![
            core_handle,
            certificate_fetcher_handle,
            proposer_handle,
            connection_monitor_handle,
        ];
        handles.extend(admin_handles);

        // If a DAG component is present then we are not using the internal consensus (Bullshark)
        // but rather an external one and we are leveraging a pure DAG structure, and more
        // components need to get initialised.
        if dag.is_some() {
            let (tx_certificate_synchronizer, mut rx_certificate_synchronizer) =
                mpsc::channel(CHANNEL_CAPACITY);
            spawn_monitored_task!(async move {
                while let Some(cert) = rx_certificate_synchronizer.recv().await {
                    // Ok to ignore error including Suspended,
                    // because fetching would be kicked off.
                    let _ = synchronizer.try_accept_certificate(cert).await;
                }
                // BlockSynchronizer has shut down.
            });

            let block_synchronizer_handler = Arc::new(BlockSynchronizerHandler::new(
                tx_block_synchronizer_commands,
                tx_certificate_synchronizer,
                certificate_store.clone(),
                parameters.block_synchronizer.handler_certificate_deliver_timeout,
            ));

            // Responsible for finding missing blocks (certificates) and fetching
            // them from the primary peers by synchronizing also their batches.
            let block_synchronizer_handle = BlockSynchronizer::spawn(
                authority.id(),
                committee.clone(),
                worker_cache.clone(),
                tx_shutdown.subscribe(),
                rx_block_synchronizer_commands,
                network.clone(),
                payload_store.clone(),
                certificate_store.clone(),
                parameters.clone(),
            );

            // Retrieves a block's data by contacting the worker nodes that contain the
            // underlying batches and their transactions.
            // TODO: (Laura) pass shutdown signal here
            let block_waiter = BlockWaiter::new(
                authority.id(),
                committee.clone(),
                worker_cache.clone(),
                network.clone(),
                block_synchronizer_handler.clone(),
            );

            // Orchestrates the removal of blocks across the primary and worker nodes.
            // TODO: (Laura) pass shutdown signal here
            let block_remover = BlockRemover::new(
                authority.id(),
                committee.clone(),
                worker_cache,
                certificate_store,
                header_store,
                payload_store,
                dag.clone(),
                network.clone(),
                tx_committed_certificates,
            );

            // Spawn a grpc server to accept requests from external consensus layer.
            let consensus_api_handle = ConsensusAPIGrpc::spawn(
                authority.id(),
                parameters.consensus_api_grpc.socket_addr,
                block_waiter,
                block_remover,
                parameters.consensus_api_grpc.get_collections_timeout,
                parameters.consensus_api_grpc.remove_collections_timeout,
                block_synchronizer_handler,
                dag,
                committee,
                endpoint_metrics,
                tx_shutdown.subscribe(),
            );

            handles.extend(vec![block_synchronizer_handle, consensus_api_handle]);
        }

        // Keeps track of the latest consensus round and allows other tasks to clean up their
        // internal state
        let state_handler_handle = StateHandler::spawn(
            authority.id(),
            rx_committed_certificates,
            tx_shutdown.subscribe(),
            Some(tx_committed_own_headers),
            network,
        );
        handles.push(state_handler_handle);

        // NOTE: This log entry is used to compute performance.
        info!("Primary {} successfully booted on {}", authority.id(), authority.primary_address());

        handles
    }

    fn add_peer_in_network(
        network: &Network,
        peer_name: NetworkPublicKey,
        address: &Multiaddr,
    ) -> (PeerId, Address) {
        let peer_id = PeerId(peer_name.0.to_bytes());
        let address = address.to_anemo_address().unwrap();
        let peer_info = PeerInfo {
            peer_id,
            affinity: anemo::types::PeerAffinity::High,
            address: vec![address.clone()],
        };
        network.known_peers().insert(peer_info);

        (peer_id, address)
    }
}

/// Defines how the network receiver handles incoming primary messages.
#[derive(Clone)]
struct PrimaryReceiverHandler {
    /// The id of this primary.
    authority_id: AuthorityIdentifier,
    committee: Committee,
    worker_cache: WorkerCache,
    synchronizer: Arc<Synchronizer>,
    /// Service to sign headers.
    signature_service: SignatureService<AuthoritySignature, { crypto::INTENT_MESSAGE_LENGTH }>,
    header_store: HeaderStore,
    certificate_store: CertificateStore,
    payload_store: PayloadStore,
    /// The store to persist the last voted round per authority, used to ensure idempotence.
    vote_digest_store: VoteDigestStore,
    /// Get a signal when the round changes.
    rx_narwhal_round_updates: watch::Receiver<Round>,
    /// Known parent digests that are being fetched from header proposers.
    /// Values are where the digests are first known from.
    /// TODO: consider limiting maximum number of digests from one authority, allow timeout
    /// and retries from other authorities.
    parent_digests: Arc<Mutex<BTreeMap<(Round, CertificateDigest), AuthorityIdentifier>>>,
    metrics: Arc<PrimaryMetrics>,
}

#[allow(clippy::result_large_err)]
impl PrimaryReceiverHandler {
    fn find_next_round(
        &self,
        origin: AuthorityIdentifier,
        current_round: Round,
        skip_rounds: &BTreeSet<Round>,
    ) -> Result<Option<Round>, anemo::rpc::Status> {
        let mut current_round = current_round;
        while let Some(round) = self
            .certificate_store
            .next_round_number(origin, current_round)
            .map_err(|e| anemo::rpc::Status::from_error(Box::new(e)))?
        {
            if !skip_rounds.contains(&round) {
                return Ok(Some(round))
            }
            current_round = round;
        }
        Ok(None)
    }

    #[allow(clippy::mutable_key_type)]
    async fn process_request_vote(
        &self,
        request: anemo::Request<RequestVoteRequest>,
    ) -> DagResult<RequestVoteResponse> {
        let header = &request.body().header;
        let committee = self.committee.clone();
        header.validate(&committee, &self.worker_cache)?;

        let num_parents = request.body().parents.len();
        ensure!(
            num_parents <= committee.size(),
            DagError::TooManyParents(num_parents, committee.size())
        );
        self.metrics.certificates_in_votes.inc_by(num_parents as u64);

        // Vote request must come from the Header's author.
        let peer_id = request
            .peer_id()
            .ok_or_else(|| DagError::NetworkError("Unable to access remote peer ID".to_owned()))?;
        let peer_network_key = NetworkPublicKey::from_bytes(&peer_id.0).map_err(|e| {
            DagError::NetworkError(format!(
                "Unable to interpret remote peer ID {peer_id:?} as a NetworkPublicKey: {e:?}"
            ))
        })?;
        let peer_authority =
            committee.authority_by_network_key(&peer_network_key).ok_or_else(|| {
                DagError::NetworkError(format!(
                    "Unable to find authority with network key {peer_network_key:?}"
                ))
            })?;
        ensure!(
            header.author() == peer_authority.id(),
            DagError::NetworkError(format!(
                "Header author {:?} must match requesting peer {peer_authority:?}",
                header.author()
            ))
        );

        debug!("Processing vote request for {:?} round:{:?}", header, header.round());

        // Request missing parent certificates from the header proposer, to reduce voting latency
        // when some certificates are not broadcasted to many primaries.
        // This is only a latency optimization, and not required for liveness.
        let parents = request.body().parents.clone();
        if parents.is_empty() {
            // If any parent is still unknown, ask the header proposer to include them with another
            // vote request.
            let unknown_digests = self.get_unknown_parent_digests(header).await?;
            if !unknown_digests.is_empty() {
                debug!(
                    "Received vote request for {:?} with unknown parents {:?}",
                    header, unknown_digests
                );
                return Ok(RequestVoteResponse { vote: None, missing: unknown_digests })
            }
        } else {
            // If requester has provided parent certificates, try to accept them.
            // It is ok to not check for additional unknown digests, because certificates can
            // become available asynchronously from broadcast or certificate fetching.
            self.try_accept_unknown_parents(header, parents).await?;
        }

        // Ensure the header has all parents accepted. If some are missing, waits until they become
        // available from broadcast or certificate fetching. If no certificate becomes available
        // for a digest, this request will time out or get cancelled by the requestor eventually.
        // This check is necessary for correctness.
        let parents = self.synchronizer.notify_read_parent_certificates(header).await?;

        // Check the parent certificates. Ensure the parents:
        // - form a quorum
        // - are all from the previous round
        // - are from unique authorities
        let mut parent_authorities = BTreeSet::new();
        let mut stake = 0;
        for parent in parents.iter() {
            ensure!(
                parent.round() + 1 == header.round(),
                DagError::HeaderHasInvalidParentRoundNumbers(header.digest())
            );
            ensure!(
                header.created_at() >= parent.header().created_at(),
                DagError::HeaderHasInvalidParentTimestamp(header.digest())
            );
            ensure!(
                parent_authorities.insert(parent.header().author()),
                DagError::HeaderHasDuplicateParentAuthorities(header.digest())
            );
            stake += committee.stake_by_id(parent.origin());
        }
        ensure!(
            stake >= committee.quorum_threshold(),
            DagError::HeaderRequiresQuorum(header.digest())
        );

        // Synchronize all batches referenced in the header.
        self.synchronizer.sync_header_batches(header, /* max_age */ 0).await?;

        // Check that the time of the header is smaller than the current time. If not but the
        // difference is small, just wait. Otherwise reject with an error.
        const TOLERANCE_MS: u64 = 1_000;
        let current_time = now();
        if current_time < *header.created_at() {
            if *header.created_at() - current_time < TOLERANCE_MS {
                // for a small difference we simply wait
                tokio::time::sleep(Duration::from_millis(*header.created_at() - current_time))
                    .await;
            } else {
                // For larger differences return an error, and log it
                warn!(
                    "Rejected header {:?} due to timestamp {} newer than {current_time}",
                    header,
                    *header.created_at()
                );
                return Err(DagError::InvalidTimestamp {
                    created_time: *header.created_at(),
                    local_time: current_time,
                })
            }
        }

        // Store the header.
        self.header_store.write(header).map_err(DagError::StoreError)?;

        // Check if we can vote for this header.
        // Send the vote when:
        // 1. when there is no existing vote for this publicKey & epoch/round
        // 2. when there is a vote for this publicKey & epoch/round, and the vote is the same
        // Taking the inverse of these two, the only time we don't want to vote is when:
        // there is a digest for the publicKey & epoch/round, and it does not match the digest
        // of the vote we create for this header.
        // Also when the header is older than one we've already voted for, it is useless to vote,
        // so we don't.
        let result = self.vote_digest_store.read(&header.author()).map_err(DagError::StoreError)?;

        if let Some(vote_info) = result {
            ensure!(
                header.epoch() == vote_info.epoch(),
                DagError::InvalidEpoch { expected: header.epoch(), received: vote_info.epoch() }
            );
            ensure!(
                header.round() >= vote_info.round(),
                DagError::AlreadyVotedNewerHeader(
                    header.digest(),
                    header.round(),
                    vote_info.round(),
                )
            );
            if header.round() == vote_info.round() {
                // Make sure we don't vote twice for the same authority in the same epoch/round.
                let vote = Vote::new(header, &self.authority_id, &self.signature_service).await;
                if vote.digest() != vote_info.vote_digest() {
                    warn!(
                        "Authority {} submitted different header {:?} for voting",
                        header.author(),
                        header,
                    );
                    self.metrics.votes_dropped_equivocation_protection.inc();
                    return Err(DagError::AlreadyVoted(
                        vote_info.vote_digest(),
                        header.digest(),
                        header.round(),
                    ))
                }
                debug!("Resending vote {vote:?} for {} at round {}", header, header.round());
                return Ok(RequestVoteResponse { vote: Some(vote), missing: Vec::new() })
            }
        }

        // Make a vote and send it to the header's creator.
        let vote = Vote::new(header, &self.authority_id, &self.signature_service).await;
        debug!("Created vote {vote:?} for {} at round {}", header, header.round());

        // Update the vote digest store with the vote we just sent.
        self.vote_digest_store.write(&vote)?;

        Ok(RequestVoteResponse { vote: Some(vote), missing: Vec::new() })
    }

    // Tries to accept certificates if they have been requested from the header author.
    // The filtering is to avoid overload from unrequested certificates. It is ok that this
    // filter may result in a certificate never arriving via header proposals, because
    // liveness is guaranteed by certificate fetching.
    async fn try_accept_unknown_parents(
        &self,
        header: &Header,
        mut parents: Vec<Certificate>,
    ) -> DagResult<()> {
        {
            let parent_digests = self.parent_digests.lock();
            parents.retain(|cert| {
                let Some(from) = parent_digests.get(&(cert.round(), cert.digest())) else {
                    return false;
                };
                // Only process a certificate from the primary where it is first known.
                *from == header.author()
            });
        }
        for parent in parents {
            self.synchronizer.try_accept_certificate(parent).await?;
        }
        Ok(())
    }

    /// Gets parent certificate digests not known before, in storage, among suspended certificates,
    /// or being requested from other header proposers.
    async fn get_unknown_parent_digests(
        &self,
        header: &Header,
    ) -> DagResult<Vec<CertificateDigest>> {
        // Get digests not known by the synchronizer, in storage or among suspended certificates.
        let mut digests = self.synchronizer.get_unknown_parent_digests(header).await?;

        // Maximum header age is chosen to strike a balance between allowing for slightly older
        // certificates to still have a chance to be included in the DAG while not wasting
        // resources on very old vote requests. This value affects performance but not correctness
        // of the algorithm.
        const HEADER_AGE_LIMIT: Round = 3;

        // Lock to ensure consistency between limit_round and where parent_digests are gc'ed.
        let mut parent_digests = self.parent_digests.lock();

        // Check that the header is not too old.
        let narwhal_round = *self.rx_narwhal_round_updates.borrow();
        let limit_round = narwhal_round.saturating_sub(HEADER_AGE_LIMIT);
        ensure!(
            limit_round <= header.round(),
            DagError::TooOld(header.digest().into(), header.round(), narwhal_round)
        );

        // Drop old entries from parent_digests.
        while let Some(((round, _digest), _authority)) = parent_digests.first_key_value() {
            // Minimum header round is limit_round, so minimum parent round is limit_round - 1.
            if *round < limit_round.saturating_sub(1) {
                parent_digests.pop_first();
            } else {
                break
            }
        }

        // Filter out digests that are already requested from other header proposers.
        digests.retain(|digest| match parent_digests.entry((header.round() - 1, *digest)) {
            Entry::Occupied(_) => false,
            Entry::Vacant(v) => {
                v.insert(header.author());
                true
            }
        });

        Ok(digests)
    }
}

#[async_trait]
impl PrimaryToPrimary for PrimaryReceiverHandler {
    async fn send_certificate(
        &self,
        request: anemo::Request<SendCertificateRequest>,
    ) -> Result<anemo::Response<SendCertificateResponse>, anemo::rpc::Status> {
        let _scope = monitored_scope("PrimaryReceiverHandler::send_certificate");
        let certificate = request.into_body().certificate;
        match self.synchronizer.try_accept_certificate(certificate).await {
            Ok(()) => Ok(anemo::Response::new(SendCertificateResponse { accepted: true })),
            Err(DagError::Suspended(_)) => {
                Ok(anemo::Response::new(SendCertificateResponse { accepted: false }))
            }
            Err(e) => Err(anemo::rpc::Status::internal(e.to_string())),
        }
    }

    async fn request_vote(
        &self,
        request: anemo::Request<RequestVoteRequest>,
    ) -> Result<anemo::Response<RequestVoteResponse>, anemo::rpc::Status> {
        self.process_request_vote(request).await.map(anemo::Response::new).map_err(|e| {
            anemo::rpc::Status::new_with_message(
                match e {
                    // Report unretriable errors as 400 Bad Request.
                    DagError::InvalidSignature |
                    DagError::InvalidEpoch { .. } |
                    DagError::InvalidHeaderDigest |
                    DagError::HeaderHasBadWorkerIds(_) |
                    DagError::HeaderHasInvalidParentRoundNumbers(_) |
                    DagError::HeaderHasDuplicateParentAuthorities(_) |
                    DagError::AlreadyVoted(_, _, _) |
                    DagError::AlreadyVotedNewerHeader(_, _, _) |
                    DagError::HeaderRequiresQuorum(_) |
                    DagError::TooOld(_, _, _) => anemo::types::response::StatusCode::BadRequest,
                    // All other errors are retriable.
                    _ => anemo::types::response::StatusCode::Unknown,
                },
                format!("{e:?}"),
            )
        })
    }

    async fn get_certificates(
        &self,
        request: anemo::Request<GetCertificatesRequest>,
    ) -> Result<anemo::Response<GetCertificatesResponse>, anemo::rpc::Status> {
        let digests = request.into_body().digests;
        if digests.is_empty() {
            return Ok(anemo::Response::new(GetCertificatesResponse { certificates: Vec::new() }))
        }

        // TODO [issue #195]: Do some accounting to prevent bad nodes from monopolizing our
        // resources.
        let certificates = self.certificate_store.read_all(digests).map_err(|e| {
            anemo::rpc::Status::internal(format!("error while retrieving certificates: {e}"))
        })?;
        Ok(anemo::Response::new(GetCertificatesResponse {
            certificates: certificates.into_iter().flatten().collect(),
        }))
    }

    #[instrument(level = "debug", skip_all, peer = ?request.peer_id())]
    async fn fetch_certificates(
        &self,
        request: anemo::Request<FetchCertificatesRequest>,
    ) -> Result<anemo::Response<FetchCertificatesResponse>, anemo::rpc::Status> {
        let time_start = Instant::now();
        let peer =
            request.peer_id().map_or_else(|| "None".to_string(), |peer_id| format!("{}", peer_id));
        let request = request.into_body();
        let mut response = FetchCertificatesResponse { certificates: Vec::new() };
        if request.max_items == 0 {
            return Ok(anemo::Response::new(response))
        }

        // Use a min-queue for (round, authority) to keep track of the next certificate to fetch.
        //
        // Compared to fetching certificates iteratatively round by round, using a heap is simpler,
        // and avoids the pathological case of iterating through many missing rounds of a downed
        // authority.
        let (lower_bound, skip_rounds) = request.get_bounds();
        debug!(
            "Fetching certificates after round {lower_bound} for peer {:?}, elapsed = {}ms",
            peer,
            time_start.elapsed().as_millis(),
        );

        let mut fetch_queue = BinaryHeap::new();
        const MAX_SKIP_ROUNDS: usize = 1000;
        for (origin, rounds) in &skip_rounds {
            if rounds.len() > MAX_SKIP_ROUNDS {
                warn!(
                    "Peer has sent {} rounds to skip on origin {}, indicating peer's problem with \
                    committing or keeping track of GC rounds. elapsed = {}ms",
                    rounds.len(),
                    origin,
                    time_start.elapsed().as_millis(),
                );
            }
            let next_round = self.find_next_round(*origin, lower_bound, rounds)?;
            if let Some(r) = next_round {
                fetch_queue.push(Reverse((r, origin)));
            }
        }
        debug!(
            "Initialized origins and rounds to fetch, elapsed = {}ms",
            time_start.elapsed().as_millis(),
        );

        // Iteratively pop the next smallest (Round, Authority) pair, and push to min-heap the next
        // higher round of the same authority that should not be skipped.
        // The process ends when there are no more pairs in the min-heap.
        while let Some(Reverse((round, origin))) = fetch_queue.pop() {
            // Allow the request handler to be stopped after timeout.
            tokio::task::yield_now().await;
            match self
                .certificate_store
                .read_by_index(*origin, round)
                .map_err(|e| anemo::rpc::Status::from_error(Box::new(e)))?
            {
                Some(cert) => {
                    response.certificates.push(cert);
                    let next_round =
                        self.find_next_round(*origin, round, skip_rounds.get(origin).unwrap())?;
                    if let Some(r) = next_round {
                        fetch_queue.push(Reverse((r, origin)));
                    }
                }
                None => continue,
            };
            if response.certificates.len() == request.max_items {
                debug!(
                    "Collected enough certificates (num={}, elapsed={}ms), returning.",
                    response.certificates.len(),
                    time_start.elapsed().as_millis(),
                );
                break
            }
            if time_start.elapsed() >= FETCH_CERTIFICATES_MAX_HANDLER_TIME {
                debug!(
                    "Spent enough time reading certificates (num={}, elapsed={}ms), returning.",
                    response.certificates.len(),
                    time_start.elapsed().as_millis(),
                );
                break
            }
            assert!(response.certificates.len() < request.max_items);
        }

        // The requestor should be able to process certificates returned in this order without
        // any missing parents.
        Ok(anemo::Response::new(response))
    }

    async fn get_payload_availability(
        &self,
        request: anemo::Request<PayloadAvailabilityRequest>,
    ) -> Result<anemo::Response<PayloadAvailabilityResponse>, anemo::rpc::Status> {
        let digests = request.into_body().certificate_digests;
        let certificates = self.certificate_store.read_all(digests.to_owned()).map_err(|e| {
            anemo::rpc::Status::internal(format!("error reading certificates: {e:?}"))
        })?;

        let mut result: Vec<(CertificateDigest, bool)> = Vec::new();
        for (id, certificate_option) in digests.into_iter().zip(certificates) {
            // Find batches only for certificates that exist.
            if let Some(certificate) = certificate_option {
                let payload_available = match self.payload_store.read_all(
                    certificate
                        .header()
                        .payload()
                        .iter()
                        .map(|(batch, (worker_id, _))| (*batch, *worker_id)),
                ) {
                    Ok(payload_result) => payload_result.into_iter().all(|x| x.is_some()),
                    Err(err) => {
                        // Assume that we don't have the payloads available,
                        // otherwise an error response should be sent back.
                        error!("Error while retrieving payloads: {err}");
                        false
                    }
                };
                result.push((id, payload_available));
            } else {
                // We don't have the certificate available in first place,
                // so we can't even look up the batches.
                result.push((id, false));
            }
        }

        Ok(anemo::Response::new(PayloadAvailabilityResponse { payload_availability: result }))
    }
}

/// Defines how the network receiver handles incoming workers messages.
#[derive(Clone)]
struct WorkerReceiverHandler {
    tx_our_digests: Sender<OurDigestMessage>,
    payload_store: PayloadStore,
}

#[async_trait]
impl WorkerToPrimary for WorkerReceiverHandler {
    async fn report_own_batch(
        &self,
        request: anemo::Request<WorkerOwnBatchMessage>,
    ) -> Result<anemo::Response<()>, anemo::rpc::Status> {
        let message = request.into_body();

        let (tx_ack, rx_ack) = oneshot::channel();
        let response = self
            .tx_our_digests
            .send(OurDigestMessage {
                digest: message.digest,
                worker_id: message.worker_id,
                timestamp: *message.metadata.created_at(),
                ack_channel: Some(tx_ack),
            })
            .await
            .map(|_| anemo::Response::new(()))
            .map_err(|e| anemo::rpc::Status::internal(e.to_string()))?;

        // If we are ok, then wait for the ack
        rx_ack.await.map_err(|e| anemo::rpc::Status::internal(e.to_string()))?;

        Ok(response)
    }

    async fn report_others_batch(
        &self,
        request: anemo::Request<WorkerOthersBatchMessage>,
    ) -> Result<anemo::Response<()>, anemo::rpc::Status> {
        let message = request.into_body();
        self.payload_store
            .write(&message.digest, &message.worker_id)
            .map_err(|e| anemo::rpc::Status::internal(e.to_string()))?;
        Ok(anemo::Response::new(()))
    }
}

#[cfg(test)]
mod test {
    use super::{Primary, PrimaryReceiverHandler, CHANNEL_CAPACITY};
    use crate::{
        common::create_db_stores,
        metrics::{PrimaryChannelMetrics, PrimaryMetrics},
        synchronizer::Synchronizer,
        NUM_SHUTDOWN_RECEIVERS,
    };
    use bincode::Options;
    use fastcrypto::{
        encoding::{Encoding, Hex},
        hash::Hash,
        signature_service::SignatureService,
        traits::KeyPair,
    };
    use itertools::Itertools;
    use lattice_consensus::{ConsensusRound, dag::DagHandle, metrics::ConsensusMetrics};
    use lattice_network::client::NetworkClient;
    use lattice_storage::{
        CertificateStore, CertificateStoreCache, NodeStorage, PayloadStore, PayloadToken,
        VoteDigestStore,
    };
    use lattice_test_utils::{make_optimal_signed_certificates, temp_dir, CommitteeFixture};
    use lattice_typed_store::rocks::{DBMap, MetricConf, ReadWriteOptions};
    use lattice_worker::{metrics::initialise_metrics, TrivialTransactionValidator, Worker};
    use prometheus::Registry;
    use std::{
        borrow::Borrow,
        collections::{BTreeSet, HashMap, HashSet},
        num::NonZeroUsize,
        sync::Arc,
        time::Duration,
    };
    use tn_types::consensus::{
        AuthorityIdentifier, Committee, Parameters, WorkerId,
        now, BatchDigest, Certificate, CertificateAPI, CertificateDigest, FetchCertificatesRequest,
        Header, HeaderAPI, MockPrimaryToWorker, PayloadAvailabilityRequest,
        PreSubscribedBroadcastSender, PrimaryToPrimary, RequestVoteRequest, Round,
    };
    use tokio::{
        sync::{oneshot, watch},
        time::timeout,
    };

    #[tokio::test]
    async fn get_network_peers_from_admin_server() {
        // telemetry_subscribers::init_for_testing();
        let primary_1_parameters = Parameters {
            batch_size: 200, // Two transactions.
            ..Parameters::default()
        };
        let fixture = CommitteeFixture::builder().randomize_ports(true).build();
        let committee = fixture.committee();
        let worker_cache = fixture.worker_cache();
        let authority_1 = fixture.authorities().next().unwrap();
        let signer_1 = authority_1.keypair().copy();

        let worker_id = 0;
        let worker_1_keypair = authority_1.worker(worker_id).keypair().copy();

        // Make the data store.
        let store = NodeStorage::reopen(temp_dir(), None);
        let client_1 = NetworkClient::new_from_keypair(&authority_1.network_keypair());

        let (tx_new_certificates, rx_new_certificates) = consensus_metrics::metered_channel::channel(
            CHANNEL_CAPACITY,
            &prometheus::IntGauge::new(
                PrimaryChannelMetrics::NAME_NEW_CERTS,
                PrimaryChannelMetrics::DESC_NEW_CERTS,
            )
            .unwrap(),
        );
        let (tx_feedback, rx_feedback) = consensus_metrics::metered_channel::channel(
            CHANNEL_CAPACITY,
            &prometheus::IntGauge::new(
                PrimaryChannelMetrics::NAME_COMMITTED_CERTS,
                PrimaryChannelMetrics::DESC_COMMITTED_CERTS,
            )
            .unwrap(),
        );
        let (_tx_consensus_round_updates, rx_consensus_round_updates) =
            watch::channel(ConsensusRound::default());

        let mut tx_shutdown = PreSubscribedBroadcastSender::new(NUM_SHUTDOWN_RECEIVERS);
        let consensus_metrics = Arc::new(ConsensusMetrics::new(&Registry::new()));
        // channel for proposer and EL
        let (el_sender, _el_receiver) = tokio::sync::mpsc::channel(1);

        // Spawn Primary 1
        Primary::spawn(
            authority_1.authority().clone(),
            signer_1,
            authority_1.network_keypair().copy(),
            committee.clone(),
            worker_cache.clone(),
            primary_1_parameters.clone(),
            client_1.clone(),
            store.header_store.clone(),
            store.certificate_store.clone(),
            store.proposer_store.clone(),
            store.payload_store.clone(),
            store.vote_digest_store.clone(),
            tx_new_certificates,
            rx_feedback,
            rx_consensus_round_updates,
            /* dag */
            Some(Arc::new(
                DagHandle::new(&committee, rx_new_certificates, consensus_metrics, tx_shutdown.subscribe()).1,
            )),
            &mut tx_shutdown,
            tx_feedback,
            &Registry::new(),
            el_sender,
        );

        // Wait for tasks to start
        tokio::time::sleep(Duration::from_secs(1)).await;

        let registry_1 = Registry::new();
        let metrics_1 = initialise_metrics(&registry_1);

        let worker_1_parameters = Parameters {
            batch_size: 200, // Two transactions.
            ..Parameters::default()
        };

        let mut tx_shutdown_worker = PreSubscribedBroadcastSender::new(NUM_SHUTDOWN_RECEIVERS);

        // Spawn a `Worker` instance for primary 1.
        Worker::spawn(
            authority_1.authority().clone(),
            worker_1_keypair.copy(),
            worker_id,
            committee.clone(),
            worker_cache.clone(),
            worker_1_parameters.clone(),
            TrivialTransactionValidator::default(),
            client_1,
            store.batch_store,
            metrics_1,
            &mut tx_shutdown_worker,
            None,
        );

        // Test getting all known peers for primary 1
        let resp = reqwest::get(format!(
            "http://127.0.0.1:{}/known_peers",
            primary_1_parameters.network_admin_server.primary_network_admin_server_port
        ))
        .await
        .unwrap()
        .json::<Vec<String>>()
        .await
        .unwrap();

        // Assert we returned 19 peers (3 other primaries + 4 workers + 4*3 other workers)
        assert_eq!(19, resp.len());

        // Test getting all connected peers for primary 1
        let resp = reqwest::get(format!(
            "http://127.0.0.1:{}/peers",
            primary_1_parameters.network_admin_server.primary_network_admin_server_port
        ))
        .await
        .unwrap()
        .json::<Vec<String>>()
        .await
        .unwrap();

        // Assert we returned 1 peers (only 1 worker spawned)
        assert_eq!(1, resp.len());

        let authority_2 = fixture.authorities().nth(1).unwrap();
        let signer_2 = authority_2.keypair().copy();
        let client_2 = NetworkClient::new_from_keypair(&authority_2.network_keypair());

        let primary_2_parameters = Parameters {
            batch_size: 200, // Two transactions.
            ..Parameters::default()
        };

        // TODO: Rework test-utils so that macro can be used for the channels below.
        let (tx_new_certificates_2, rx_new_certificates_2) =
            consensus_metrics::metered_channel::channel(
                CHANNEL_CAPACITY,
                &prometheus::IntGauge::new(
                    PrimaryChannelMetrics::NAME_NEW_CERTS,
                    PrimaryChannelMetrics::DESC_NEW_CERTS,
                )
                .unwrap(),
            );
        let (tx_feedback_2, rx_feedback_2) = consensus_metrics::metered_channel::channel(
            CHANNEL_CAPACITY,
            &prometheus::IntGauge::new(
                PrimaryChannelMetrics::NAME_COMMITTED_CERTS,
                PrimaryChannelMetrics::DESC_COMMITTED_CERTS,
            )
            .unwrap(),
        );
        let (_tx_consensus_round_updates, rx_consensus_round_updates) =
            watch::channel(ConsensusRound::default());
        let mut tx_shutdown_2 = PreSubscribedBroadcastSender::new(NUM_SHUTDOWN_RECEIVERS);
        let consensus_metrics = Arc::new(ConsensusMetrics::new(&Registry::new()));
        // channel for proposer and EL
        let (el_sender2, _el_receiver2) = tokio::sync::mpsc::channel(1);

        // Spawn Primary 2
        Primary::spawn(
            authority_2.authority().clone(),
            signer_2,
            authority_2.network_keypair().copy(),
            committee.clone(),
            worker_cache.clone(),
            primary_2_parameters.clone(),
            client_2.clone(),
            store.header_store.clone(),
            store.certificate_store.clone(),
            store.proposer_store.clone(),
            store.payload_store.clone(),
            store.vote_digest_store.clone(),
            /* tx_consensus */ tx_new_certificates_2,
            /* rx_consensus */ rx_feedback_2,
            rx_consensus_round_updates,
            /* dag */
            Some(Arc::new(
                DagHandle::new(&committee, rx_new_certificates_2, consensus_metrics, tx_shutdown.subscribe())
                    .1,
            )),
            &mut tx_shutdown_2,
            tx_feedback_2,
            &Registry::new(),
            el_sender2,
        );

        // Wait for tasks to start
        tokio::time::sleep(Duration::from_secs(1)).await;

        let primary_1_peer_id = Hex::encode(authority_1.network_keypair().copy().public().0.as_bytes());
        let primary_2_peer_id = Hex::encode(authority_2.network_keypair().copy().public().0.as_bytes());
        let worker_1_peer_id = Hex::encode(worker_1_keypair.copy().public().0.as_bytes());

        // Test getting all connected peers for primary 1
        let resp = reqwest::get(format!(
            "http://127.0.0.1:{}/peers",
            primary_1_parameters.network_admin_server.primary_network_admin_server_port
        ))
        .await
        .unwrap()
        .json::<Vec<String>>()
        .await
        .unwrap();

        // Assert we returned 2 peers (1 other primary spawned + 1 worker spawned)
        assert_eq!(2, resp.len());

        // Assert peer ids are correct
        let expected_peer_ids = vec![&primary_2_peer_id, &worker_1_peer_id];
        assert!(expected_peer_ids.iter().all(|e| resp.contains(e)));

        // Test getting all connected peers for primary 2
        let resp = reqwest::get(format!(
            "http://127.0.0.1:{}/peers",
            primary_2_parameters.network_admin_server.primary_network_admin_server_port
        ))
        .await
        .unwrap()
        .json::<Vec<String>>()
        .await
        .unwrap();

        // Assert we returned 2 peers (1 other primary spawned + 1 other worker)
        assert_eq!(2, resp.len());

        // Assert peer ids are correct
        let expected_peer_ids = vec![&primary_1_peer_id, &worker_1_peer_id];
        assert!(expected_peer_ids.iter().all(|e| resp.contains(e)));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_request_vote_has_missing_parents() {
        telemetry_subscribers::init_for_testing();
        const NUM_PARENTS: usize = 10;
        let fixture = CommitteeFixture::builder()
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(NUM_PARENTS).unwrap())
            .build();
        let target = fixture.authorities().next().unwrap();
        let author = fixture.authorities().nth(2).unwrap();
        let target_id = target.id();
        let author_id = author.id();
        let worker_cache = fixture.worker_cache();
        let signature_service = SignatureService::new(target.keypair().copy());
        let metrics = Arc::new(PrimaryMetrics::new(&Registry::new()));
        let primary_channel_metrics = PrimaryChannelMetrics::new(&Registry::new());
        let network = lattice_test_utils::test_network(target.network_keypair(), target.address());
        let client = NetworkClient::new_from_keypair(&target.network_keypair());

        let (header_store, certificate_store, payload_store) = create_db_stores();
        let (tx_certificate_fetcher, _rx_certificate_fetcher) = lattice_test_utils::test_channel!(1);
        let (tx_new_certificates, _rx_new_certificates) = lattice_test_utils::test_channel!(100);
        let (tx_parents, _rx_parents) = lattice_test_utils::test_channel!(100);
        let (_tx_consensus_round_updates, rx_consensus_round_updates) =
            watch::channel(ConsensusRound::new(1, 0));
        let (tx_narwhal_round_updates, rx_narwhal_round_updates) = watch::channel(1u64);
        let (_tx_synchronizer_network, rx_synchronizer_network) = oneshot::channel();

        let synchronizer = Arc::new(Synchronizer::new(
            target_id,
            fixture.committee(),
            worker_cache.clone(),
            /* gc_depth */ 50,
            client,
            certificate_store.clone(),
            payload_store.clone(),
            tx_certificate_fetcher,
            tx_new_certificates,
            tx_parents,
            rx_consensus_round_updates,
            rx_synchronizer_network,
            None,
            metrics.clone(),
            &primary_channel_metrics,
        ));
        let handler = PrimaryReceiverHandler {
            authority_id: target_id,
            committee: fixture.committee(),
            worker_cache: worker_cache.clone(),
            synchronizer: synchronizer.clone(),
            signature_service,
            header_store: header_store.clone(),
            certificate_store: certificate_store.clone(),
            payload_store: payload_store.clone(),
            vote_digest_store: VoteDigestStore::new_for_tests(),
            rx_narwhal_round_updates,
            parent_digests: Default::default(),
            metrics: metrics.clone(),
        };

        // Make some mock certificates that are parents of our new header.
        let committee: Committee = fixture.committee();
        let genesis =
            Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
        let ids: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
        let (certificates, _next_parents) =
            make_optimal_signed_certificates(1..=3, &genesis, &committee, ids.as_slice());
        let all_certificates = certificates.into_iter().collect_vec();
        let round_2_certs = all_certificates[NUM_PARENTS..(NUM_PARENTS * 2)].to_vec();
        let round_2_parents = round_2_certs[..(NUM_PARENTS / 2)].to_vec();
        let round_2_missing = round_2_certs[(NUM_PARENTS / 2)..].to_vec();

        // Create a test header.
        let test_header = Header::V1(
            author
                .header_builder(&fixture.committee())
                .author(author_id)
                .round(3)
                .parents(round_2_certs.iter().map(|c| c.digest()).collect())
                .with_payload_batch(lattice_test_utils::fixture_batch_with_transactions(10), 0, 0)
                .build()
                .unwrap(),
        );

        // Write some certificates from round 2 into the store, and leave out the rest to test
        // headers with some parents but not all available. Round 1 certificates should be written
        // into the storage as parents of round 2 certificates. But to test phase 2 they are left out.
        for cert in round_2_parents {
            for (digest, (worker_id, _)) in cert.header().payload() {
                payload_store.write(digest, worker_id).unwrap();
            }
            certificate_store.write(cert.clone()).unwrap();
        }

        // TEST PHASE 1: Handler should report missing parent certificates to caller.
        let mut request = anemo::Request::new(RequestVoteRequest {
            header: test_header.clone(),
            parents: Vec::new(),
        });
        assert!(request.extensions_mut().insert(network.downgrade()).is_none());
        assert!(request
            .extensions_mut()
            .insert(anemo::PeerId(author.network_public_key().0.to_bytes()))
            .is_none());
        let result = handler.request_vote(request).await;

        let expected_missing: HashSet<_> = round_2_missing.iter().map(|c| c.digest()).collect();
        let received_missing: HashSet<_> = result.unwrap().into_body().missing.into_iter().collect();
        assert_eq!(expected_missing, received_missing);

        // TEST PHASE 2: Handler should not return additional unknown digests.
        let mut request = anemo::Request::new(RequestVoteRequest {
            header: test_header.clone(),
            parents: Vec::new(),
        });
        assert!(request.extensions_mut().insert(network.downgrade()).is_none());
        assert!(request
            .extensions_mut()
            .insert(anemo::PeerId(author.network_public_key().0.to_bytes()))
            .is_none());
        // No additional missing parents will be requested.
        let result = timeout(Duration::from_secs(5), handler.request_vote(request)).await;
        assert!(result.is_err(), "{:?}", result);

        // TEST PHASE 3: Handler should return error if header is too old.
        // Increase round threshold.
        let _ = tx_narwhal_round_updates.send(100);
        let mut request = anemo::Request::new(RequestVoteRequest {
            header: test_header.clone(),
            parents: Vec::new(),
        });
        assert!(request.extensions_mut().insert(network.downgrade()).is_none());
        assert!(request
            .extensions_mut()
            .insert(anemo::PeerId(author.network_public_key().0.to_bytes()))
            .is_none());
        // Because round 1 certificates are not in store, the missing parents will not be accepted yet.
        let result = handler.request_vote(request).await;
        assert!(result.is_err(), "{:?}", result);
        assert_eq!(
            // Returned error should be unretriable.
            anemo::types::response::StatusCode::BadRequest,
            result.err().unwrap().status()
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_request_vote_accept_missing_parents() {
        telemetry_subscribers::init_for_testing();
        const NUM_PARENTS: usize = 10;
        let fixture = CommitteeFixture::builder()
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(NUM_PARENTS).unwrap())
            .build();
        let target = fixture.authorities().next().unwrap();
        let author = fixture.authorities().nth(2).unwrap();
        let target_id = target.id();
        let author_id = author.id();
        let worker_cache = fixture.worker_cache();
        let signature_service = SignatureService::new(target.keypair().copy());
        let metrics = Arc::new(PrimaryMetrics::new(&Registry::new()));
        let primary_channel_metrics = PrimaryChannelMetrics::new(&Registry::new());
        let network = lattice_test_utils::test_network(target.network_keypair(), target.address());
        let client = NetworkClient::new_from_keypair(&target.network_keypair());

        let (header_store, certificate_store, payload_store) = create_db_stores();
        let (tx_certificate_fetcher, _rx_certificate_fetcher) = lattice_test_utils::test_channel!(1);
        let (tx_new_certificates, _rx_new_certificates) = lattice_test_utils::test_channel!(100);
        let (tx_parents, _rx_parents) = lattice_test_utils::test_channel!(100);
        let (_tx_consensus_round_updates, rx_consensus_round_updates) =
            watch::channel(ConsensusRound::new(1, 0));
        let (tx_narwhal_round_updates, rx_narwhal_round_updates) = watch::channel(1u64);
        let (_tx_synchronizer_network, rx_synchronizer_network) = oneshot::channel();

        let synchronizer = Arc::new(Synchronizer::new(
            target_id,
            fixture.committee(),
            worker_cache.clone(),
            /* gc_depth */ 50,
            client,
            certificate_store.clone(),
            payload_store.clone(),
            tx_certificate_fetcher,
            tx_new_certificates,
            tx_parents,
            rx_consensus_round_updates,
            rx_synchronizer_network,
            None,
            metrics.clone(),
            &primary_channel_metrics,
        ));
        let handler = PrimaryReceiverHandler {
            authority_id: target_id,
            committee: fixture.committee(),
            worker_cache: worker_cache.clone(),
            synchronizer: synchronizer.clone(),
            signature_service,
            header_store: header_store.clone(),
            certificate_store: certificate_store.clone(),
            payload_store: payload_store.clone(),
            vote_digest_store: VoteDigestStore::new_for_tests(),
            rx_narwhal_round_updates,
            parent_digests: Default::default(),
            metrics: metrics.clone(),
        };

        // Make some mock certificates that are parents of our new header.
        let committee: Committee = fixture.committee();
        let genesis =
            Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
        let ids: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
        let (certificates, _next_parents) =
            make_optimal_signed_certificates(1..=3, &genesis, &committee, ids.as_slice());
        let all_certificates = certificates.into_iter().collect_vec();
        let round_1_certs = all_certificates[..NUM_PARENTS].to_vec();
        let round_2_certs = all_certificates[NUM_PARENTS..(NUM_PARENTS * 2)].to_vec();
        let round_2_parents = round_2_certs[..(NUM_PARENTS / 2)].to_vec();
        let round_2_missing = round_2_certs[(NUM_PARENTS / 2)..].to_vec();

        // Create a test header.
        let test_header = Header::V1(
            author
                .header_builder(&fixture.committee())
                .author(author_id)
                .round(3)
                .parents(round_2_certs.iter().map(|c| c.digest()).collect())
                .with_payload_batch(lattice_test_utils::fixture_batch_with_transactions(10), 0, 0)
                .build()
                .unwrap(),
        );

        // Populate all round 1 certificates and some round 2 certificates into the storage.
        // The new header will have some round 2 certificates missing as parents, but these parents
        // should be able to get accepted.
        for cert in round_1_certs {
            for (digest, (worker_id, _)) in cert.header().payload() {
                payload_store.write(digest, worker_id).unwrap();
            }
            certificate_store.write(cert.clone()).unwrap();
        }
        for cert in round_2_parents {
            for (digest, (worker_id, _)) in cert.header().payload() {
                payload_store.write(digest, worker_id).unwrap();
            }
            certificate_store.write(cert.clone()).unwrap();
        }
        // Populate new header payload so they don't have to be retrieved.
        for (digest, (worker_id, _)) in test_header.payload() {
            payload_store.write(digest, worker_id).unwrap();
        }

        // TEST PHASE 1: Handler should report missing parent certificates to caller.
        let mut request = anemo::Request::new(RequestVoteRequest {
            header: test_header.clone(),
            parents: Vec::new(),
        });
        assert!(request.extensions_mut().insert(network.downgrade()).is_none());
        assert!(request
            .extensions_mut()
            .insert(anemo::PeerId(author.network_public_key().0.to_bytes()))
            .is_none());
        let result = handler.request_vote(request).await;

        let expected_missing: HashSet<_> = round_2_missing.iter().map(|c| c.digest()).collect();
        let received_missing: HashSet<_> = result.unwrap().into_body().missing.into_iter().collect();
        assert_eq!(expected_missing, received_missing);

        // TEST PHASE 2: Handler should process missing parent certificates and succeed.
        let _ = tx_narwhal_round_updates.send(1);
        let mut request = anemo::Request::new(RequestVoteRequest {
            header: test_header,
            parents: round_2_missing.clone(),
        });
        assert!(request.extensions_mut().insert(network.downgrade()).is_none());
        assert!(request
            .extensions_mut()
            .insert(anemo::PeerId(author.network_public_key().0.to_bytes()))
            .is_none());

        let result = timeout(Duration::from_secs(5), handler.request_vote(request)).await.unwrap();
        assert!(result.is_ok(), "{:?}", result);
    }

    #[tokio::test]
    async fn test_request_vote_missing_batches() {
        telemetry_subscribers::init_for_testing();
        let fixture = CommitteeFixture::builder()
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(4).unwrap())
            .build();
        let worker_cache = fixture.worker_cache();
        let primary = fixture.authorities().next().unwrap();
        let authority_id = primary.id();
        let author = fixture.authorities().nth(2).unwrap();
        let signature_service = SignatureService::new(primary.keypair().copy());
        let metrics = Arc::new(PrimaryMetrics::new(&Registry::new()));
        let primary_channel_metrics = PrimaryChannelMetrics::new(&Registry::new());
        let network = lattice_test_utils::test_network(primary.network_keypair(), primary.address());
        let client = NetworkClient::new_from_keypair(&primary.network_keypair());

        let (header_store, certificate_store, payload_store) = create_db_stores();
        let (tx_certificate_fetcher, _rx_certificate_fetcher) = lattice_test_utils::test_channel!(1);
        let (tx_new_certificates, _rx_new_certificates) = lattice_test_utils::test_channel!(100);
        let (tx_parents, _rx_parents) = lattice_test_utils::test_channel!(100);
        let (_tx_consensus_round_updates, rx_consensus_round_updates) =
            watch::channel(ConsensusRound::new(1, 0));
        let (_tx_narwhal_round_updates, rx_narwhal_round_updates) = watch::channel(1u64);
        let (_tx_synchronizer_network, rx_synchronizer_network) = oneshot::channel();

        let synchronizer = Arc::new(Synchronizer::new(
            authority_id,
            fixture.committee(),
            worker_cache.clone(),
            /* gc_depth */ 50,
            client.clone(),
            certificate_store.clone(),
            payload_store.clone(),
            tx_certificate_fetcher,
            tx_new_certificates,
            tx_parents,
            rx_consensus_round_updates,
            rx_synchronizer_network,
            None,
            metrics.clone(),
            &primary_channel_metrics,
        ));
        let handler = PrimaryReceiverHandler {
            authority_id,
            committee: fixture.committee(),
            worker_cache: worker_cache.clone(),
            synchronizer: synchronizer.clone(),
            signature_service,
            header_store: header_store.clone(),
            certificate_store: certificate_store.clone(),
            payload_store: payload_store.clone(),
            vote_digest_store: VoteDigestStore::new_for_tests(),
            rx_narwhal_round_updates,
            parent_digests: Default::default(),
            metrics: metrics.clone(),
        };

        // Make some mock certificates that are parents of our new header.
        let mut certificates = HashMap::new();
        for primary in fixture.authorities().filter(|a| a.id() != authority_id) {
            let header = Header::V1(
                primary
                    .header_builder(&fixture.committee())
                    .with_payload_batch(lattice_test_utils::fixture_batch_with_transactions(10), 0, 0)
                    .build()
                    .unwrap(),
            );

            let certificate = fixture.certificate(&header);
            let digest = certificate.clone().digest();

            certificates.insert(digest, certificate.clone());
            certificate_store.write(certificate.clone()).unwrap();
            for (digest, (worker_id, _)) in certificate.header().payload() {
                payload_store.write(digest, worker_id).unwrap();
            }
        }
        let test_header = Header::V1(
            author
                .header_builder(&fixture.committee())
                .round(2)
                .parents(certificates.keys().cloned().collect())
                .with_payload_batch(lattice_test_utils::fixture_batch_with_transactions(10), 1, 0)
                .build()
                .unwrap(),
        );
        let test_digests: HashSet<_> =
            test_header.payload().iter().map(|(digest, _)| digest).cloned().collect();

        // Set up mock worker.
        let author_id = author.id();
        let worker = primary.worker(1);
        let worker_address = &worker.info().worker_address;
        let worker_peer_id = anemo::PeerId(worker.keypair().public().0.to_bytes());
        let mut mock_server = MockPrimaryToWorker::new();
        mock_server
            .expect_synchronize()
            .withf(move |request| {
                let digests: HashSet<_> = request.body().digests.iter().cloned().collect();
                digests == test_digests && request.body().target == author_id
            })
            .times(1)
            .return_once(|_| Ok(anemo::Response::new(())));

        client.set_primary_to_worker_local_handler(worker_peer_id, Arc::new(mock_server));

        let _worker_network = worker.new_network(anemo::Router::new());
        let address = worker_address.to_anemo_address().unwrap();
        network.connect_with_peer_id(address, worker_peer_id).await.unwrap();

        // Verify Handler synchronizes missing batches and generates a Vote.
        let mut request = anemo::Request::new(RequestVoteRequest {
            header: test_header.clone(),
            parents: Vec::new(),
        });
        assert!(request.extensions_mut().insert(network.downgrade()).is_none());
        assert!(request
            .extensions_mut()
            .insert(anemo::PeerId(author.network_public_key().0.to_bytes()))
            .is_none());

        let response = handler.request_vote(request).await.unwrap();
        assert!(response.body().vote.is_some());
    }

    #[tokio::test]
    async fn test_request_vote_already_voted() {
        telemetry_subscribers::init_for_testing();
        let fixture = CommitteeFixture::builder()
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(4).unwrap())
            .build();
        let worker_cache = fixture.worker_cache();
        let primary = fixture.authorities().next().unwrap();
        let id = primary.id();
        let author = fixture.authorities().nth(2).unwrap();
        let signature_service = SignatureService::new(primary.keypair().copy());
        let metrics = Arc::new(PrimaryMetrics::new(&Registry::new()));
        let primary_channel_metrics = PrimaryChannelMetrics::new(&Registry::new());
        let network = lattice_test_utils::test_network(primary.network_keypair(), primary.address());
        let client = NetworkClient::new_from_keypair(&primary.network_keypair());

        let (header_store, certificate_store, payload_store) = create_db_stores();
        let (tx_certificate_fetcher, _rx_certificate_fetcher) = lattice_test_utils::test_channel!(1);
        let (tx_new_certificates, _rx_new_certificates) = lattice_test_utils::test_channel!(100);
        let (tx_parents, _rx_parents) = lattice_test_utils::test_channel!(100);
        let (_tx_consensus_round_updates, rx_consensus_round_updates) =
            watch::channel(ConsensusRound::new(1, 0));
        let (_tx_narwhal_round_updates, rx_narwhal_round_updates) = watch::channel(1u64);
        let (_tx_synchronizer_network, rx_synchronizer_network) = oneshot::channel();

        let synchronizer = Arc::new(Synchronizer::new(
            id,
            fixture.committee(),
            worker_cache.clone(),
            /* gc_depth */ 50,
            client.clone(),
            certificate_store.clone(),
            payload_store.clone(),
            tx_certificate_fetcher,
            tx_new_certificates,
            tx_parents,
            rx_consensus_round_updates,
            rx_synchronizer_network,
            None,
            metrics.clone(),
            &primary_channel_metrics,
        ));

        let handler = PrimaryReceiverHandler {
            authority_id: id,
            committee: fixture.committee(),
            worker_cache: worker_cache.clone(),
            synchronizer: synchronizer.clone(),
            signature_service,
            header_store: header_store.clone(),
            certificate_store: certificate_store.clone(),
            payload_store: payload_store.clone(),
            vote_digest_store: VoteDigestStore::new_for_tests(),
            rx_narwhal_round_updates,
            parent_digests: Default::default(),
            metrics: metrics.clone(),
        };

        // Make some mock certificates that are parents of our new header.
        let mut certificates = HashMap::new();
        for primary in fixture.authorities().filter(|a| a.id() != id) {
            let header = Header::V1(
                primary
                    .header_builder(&fixture.committee())
                    .with_payload_batch(lattice_test_utils::fixture_batch_with_transactions(10), 0, 0)
                    .build()
                    .unwrap(),
            );

            let certificate = fixture.certificate(&header);
            let digest = certificate.clone().digest();

            certificates.insert(digest, certificate.clone());
            certificate_store.write(certificate.clone()).unwrap();
            for (digest, (worker_id, _)) in certificate.header().payload() {
                payload_store.write(digest, worker_id).unwrap();
            }
        }

        // Set up mock worker.
        let worker = primary.worker(1);
        let worker_address = &worker.info().worker_address;
        let worker_peer_id = anemo::PeerId(worker.keypair().public().0.to_bytes());
        let mut mock_server = MockPrimaryToWorker::new();
        // Always Synchronize successfully.
        mock_server.expect_synchronize().returning(|_| Ok(anemo::Response::new(())));

        client.set_primary_to_worker_local_handler(worker_peer_id, Arc::new(mock_server));

        let _worker_network = worker.new_network(anemo::Router::new());
        let address = worker_address.to_anemo_address().unwrap();
        network.connect_with_peer_id(address, worker_peer_id).await.unwrap();

        // Verify Handler generates a Vote.
        let test_header = Header::V1(
            author
                .header_builder(&fixture.committee())
                .round(2)
                .parents(certificates.keys().cloned().collect())
                .with_payload_batch(lattice_test_utils::fixture_batch_with_transactions(10), 1, 0)
                .build()
                .unwrap(),
        );
        let mut request = anemo::Request::new(RequestVoteRequest {
            header: test_header.clone(),
            parents: Vec::new(),
        });
        assert!(request.extensions_mut().insert(network.downgrade()).is_none());
        assert!(request
            .extensions_mut()
            .insert(anemo::PeerId(author.network_public_key().0.to_bytes()))
            .is_none());

        let response = handler.request_vote(request).await.unwrap();
        assert!(response.body().vote.is_some());
        let vote = response.into_body().vote.unwrap();

        // Verify the same request gets the same vote back successfully.
        let mut request = anemo::Request::new(RequestVoteRequest {
            header: test_header.clone(),
            parents: Vec::new(),
        });
        assert!(request.extensions_mut().insert(network.downgrade()).is_none());
        assert!(request
            .extensions_mut()
            .insert(anemo::PeerId(author.network_public_key().0.to_bytes()))
            .is_none());

        let response = handler.request_vote(request).await.unwrap();
        assert!(response.body().vote.is_some());
        assert_eq!(vote.digest(), response.into_body().vote.unwrap().digest());

        // Verify a different request for the same round receives an error.
        let test_header = Header::V1(
            author
                .header_builder(&fixture.committee())
                .round(2)
                .parents(certificates.keys().cloned().collect())
                .with_payload_batch(lattice_test_utils::fixture_batch_with_transactions(10), 1, 0)
                .build()
                .unwrap(),
        );
        let mut request = anemo::Request::new(RequestVoteRequest {
            header: test_header.clone(),
            parents: Vec::new(),
        });
        assert!(request.extensions_mut().insert(network.downgrade()).is_none());
        assert!(request
            .extensions_mut()
            .insert(anemo::PeerId(author.network_public_key().0.to_bytes()))
            .is_none());

        let response = handler.request_vote(request).await;
        assert_eq!(
            // Returned error should not be retriable.
            anemo::types::response::StatusCode::BadRequest,
            response.err().unwrap().status()
        );
    }

    #[tokio::test]
    async fn test_fetch_certificates_handler() {
        let fixture = CommitteeFixture::builder()
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(4).unwrap())
            .build();
        let id = fixture.authorities().next().unwrap().id();
        let worker_cache = fixture.worker_cache();
        let primary = fixture.authorities().next().unwrap();
        let signature_service = SignatureService::new(primary.keypair().copy());
        let metrics = Arc::new(PrimaryMetrics::new(&Registry::new()));
        let primary_channel_metrics = PrimaryChannelMetrics::new(&Registry::new());
        let client = NetworkClient::new_from_keypair(&primary.network_keypair());

        let (header_store, certificate_store, payload_store) = create_db_stores();
        let (tx_certificate_fetcher, _rx_certificate_fetcher) = lattice_test_utils::test_channel!(1);
        let (tx_new_certificates, _rx_new_certificates) = lattice_test_utils::test_channel!(100);
        let (tx_parents, _rx_parents) = lattice_test_utils::test_channel!(100);
        let (_tx_consensus_round_updates, rx_consensus_round_updates) =
            watch::channel(ConsensusRound::default());
        let (_tx_narwhal_round_updates, rx_narwhal_round_updates) = watch::channel(1u64);
        let (_tx_synchronizer_network, rx_synchronizer_network) = oneshot::channel();

        let synchronizer = Arc::new(Synchronizer::new(
            id,
            fixture.committee(),
            worker_cache.clone(),
            /* gc_depth */ 50,
            client,
            certificate_store.clone(),
            payload_store.clone(),
            tx_certificate_fetcher,
            tx_new_certificates,
            tx_parents,
            rx_consensus_round_updates.clone(),
            rx_synchronizer_network,
            None,
            metrics.clone(),
            &primary_channel_metrics,
        ));
        let handler = PrimaryReceiverHandler {
            authority_id: id,
            committee: fixture.committee(),
            worker_cache: worker_cache.clone(),
            synchronizer: synchronizer.clone(),
            signature_service,
            header_store: header_store.clone(),
            certificate_store: certificate_store.clone(),
            payload_store: payload_store.clone(),
            vote_digest_store: VoteDigestStore::new_for_tests(),
            rx_narwhal_round_updates,
            parent_digests: Default::default(),
            metrics: metrics.clone(),
        };

        let mut current_round: Vec<_> = Certificate::genesis(&fixture.committee())
            .into_iter()
            .map(|cert| cert.header().clone())
            .collect();
        let mut headers = vec![];
        let total_rounds = 4;
        for i in 0..total_rounds {
            let parents: BTreeSet<_> =
                current_round.into_iter().map(|header| fixture.certificate(&header).digest()).collect();
            (_, current_round) = fixture.headers_round(i, &parents);
            headers.extend(current_round.clone());
        }

        let total_authorities = fixture.authorities().count();
        let total_certificates = total_authorities * total_rounds as usize;
        // Create certificates test data.
        let mut certificates = vec![];
        for header in headers.into_iter() {
            certificates.push(fixture.certificate(&header));
        }
        assert_eq!(certificates.len(), total_certificates);
        assert_eq!(16, total_certificates);

        // Populate certificate store such that each authority has the following rounds:
        // Authority 0: 1
        // Authority 1: 1 2
        // Authority 2: 1 2 3
        // Authority 3: 1 2 3 4
        // This is unrealistic because in practice a certificate can only be stored with 2f+1 parents
        // already in store. But this does not matter for testing here.
        let mut authorities = Vec::<AuthorityIdentifier>::new();
        for i in 0..total_authorities {
            authorities.push(certificates[i].header().author());
            for j in 0..=i {
                let cert = certificates[i + j * total_authorities].clone();
                assert_eq!(&cert.header().author(), authorities.last().unwrap());
                certificate_store.write(cert).expect("Writing certificate to store failed");
            }
        }

        // Each test case contains (lower bound round, skip rounds, max items, expected output).
        let test_cases = vec![
            (0, vec![vec![], vec![], vec![], vec![]], 20, vec![1, 1, 1, 1, 2, 2, 2, 3, 3, 4]),
            (0, vec![vec![1u64], vec![1], vec![], vec![]], 20, vec![1, 1, 2, 2, 2, 3, 3, 4]),
            (0, vec![vec![], vec![], vec![1], vec![1]], 20, vec![1, 1, 2, 2, 2, 3, 3, 4]),
            (1, vec![vec![], vec![], vec![2], vec![2]], 4, vec![2, 3, 3, 4]),
            (1, vec![vec![], vec![], vec![2], vec![2]], 2, vec![2, 3]),
            (0, vec![vec![1], vec![1], vec![1, 2, 3], vec![1, 2, 3]], 2, vec![2, 4]),
            (2, vec![vec![], vec![], vec![], vec![]], 3, vec![3, 3, 4]),
            (2, vec![vec![], vec![], vec![], vec![]], 2, vec![3, 3]),
            // Check that round 2 and 4 are fetched for the last authority, skipping round 3.
            (1, vec![vec![], vec![], vec![3], vec![3]], 5, vec![2, 2, 2, 4]),
        ];
        for (lower_bound_round, skip_rounds_vec, max_items, expected_rounds) in test_cases {
            let req = FetchCertificatesRequest::default()
                .set_bounds(
                    lower_bound_round,
                    authorities
                        .clone()
                        .into_iter()
                        .zip(skip_rounds_vec.into_iter().map(|rounds| rounds.into_iter().collect()))
                        .collect(),
                )
                .set_max_items(max_items);
            let resp =
                handler.fetch_certificates(anemo::Request::new(req.clone())).await.unwrap().into_body();
            assert_eq!(
                resp.certificates.iter().map(|cert| cert.round()).collect_vec(),
                expected_rounds
            );
        }
    }

    #[tokio::test]
    async fn test_process_payload_availability_success() {
        let fixture = CommitteeFixture::builder()
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(4).unwrap())
            .build();
        let author = fixture.authorities().next().unwrap();
        let id = author.id();
        let worker_cache = fixture.worker_cache();
        let primary = fixture.authorities().next().unwrap();
        let signature_service = SignatureService::new(primary.keypair().copy());
        let metrics = Arc::new(PrimaryMetrics::new(&Registry::new()));
        let primary_channel_metrics = PrimaryChannelMetrics::new(&Registry::new());
        let client = NetworkClient::new_from_keypair(&primary.network_keypair());

        let (header_store, certificate_store, payload_store) = create_db_stores();
        let (tx_certificate_fetcher, _rx_certificate_fetcher) = lattice_test_utils::test_channel!(1);
        let (tx_new_certificates, _rx_new_certificates) = lattice_test_utils::test_channel!(100);
        let (tx_parents, _rx_parents) = lattice_test_utils::test_channel!(100);
        let (_tx_consensus_round_updates, rx_consensus_round_updates) =
            watch::channel(ConsensusRound::default());
        let (_tx_narwhal_round_updates, rx_narwhal_round_updates) = watch::channel(1u64);
        let (_tx_synchronizer_network, rx_synchronizer_network) = oneshot::channel();

        let synchronizer = Arc::new(Synchronizer::new(
            id,
            fixture.committee(),
            worker_cache.clone(),
            /* gc_depth */ 50,
            client,
            certificate_store.clone(),
            payload_store.clone(),
            tx_certificate_fetcher,
            tx_new_certificates,
            tx_parents,
            rx_consensus_round_updates,
            rx_synchronizer_network,
            None,
            metrics.clone(),
            &primary_channel_metrics,
        ));
        let handler = PrimaryReceiverHandler {
            authority_id: id,
            committee: fixture.committee(),
            worker_cache: worker_cache.clone(),
            synchronizer: synchronizer.clone(),
            signature_service,
            header_store: header_store.clone(),
            certificate_store: certificate_store.clone(),
            payload_store: payload_store.clone(),
            vote_digest_store: VoteDigestStore::new_for_tests(),
            rx_narwhal_round_updates,
            parent_digests: Default::default(),
            metrics: metrics.clone(),
        };

        // GIVEN some mock certificates
        let mut certificates = HashMap::new();
        let mut missing_certificates = HashSet::new();

        for i in 0..10 {
            let header = Header::V1(
                author
                    .header_builder(&fixture.committee())
                    .with_payload_batch(lattice_test_utils::fixture_batch_with_transactions(10), 0, 0)
                    .build()
                    .unwrap(),
            );

            let certificate = fixture.certificate(&header);
            let digest = certificate.clone().digest();

            certificates.insert(digest, certificate.clone());

            // We want to simulate the scenario of both having some certificates
            // found and some non found. Store only the half. The other half
            // should be returned back as non found.
            if i < 7 {
                // write the certificate
                certificate_store.write(certificate.clone()).unwrap();

                for (digest, (worker_id, _)) in certificate.header().payload() {
                    payload_store.write(digest, worker_id).unwrap();
                }
            } else {
                missing_certificates.insert(digest);
            }
        }

        // WHEN requesting the payload availability for all the certificates
        let request = anemo::Request::new(PayloadAvailabilityRequest {
            certificate_digests: certificates.keys().copied().collect(),
        });
        let response = handler.get_payload_availability(request).await.unwrap();
        let result_digests: HashSet<CertificateDigest> =
            response.body().payload_availability.iter().map(|(digest, _)| *digest).collect();

        assert_eq!(
            result_digests.len(),
            certificates.len(),
            "Returned unique number of certificates don't match the expected"
        );

        // ensure that we have no payload availability for some
        let availability_map = response.into_body().payload_availability.into_iter().counts_by(|c| c.1);

        for (available, found) in availability_map {
            if available {
                assert_eq!(found, 7, "Expected to have available payloads");
            } else {
                assert_eq!(found, 3, "Expected to have non available payloads");
            }
        }
    }

    #[tokio::test]
    async fn test_process_payload_availability_when_failures() {
        // GIVEN
        // We initialise the test stores manually to allow us
        // inject some wrongly serialised values to cause data store errors.
        let rocksdb = lattice_typed_store::rocks::open_cf(
            temp_dir(),
            None,
            MetricConf::default(),
            &[
                lattice_test_utils::CERTIFICATES_CF,
                lattice_test_utils::CERTIFICATE_DIGEST_BY_ROUND_CF,
                lattice_test_utils::CERTIFICATE_DIGEST_BY_ORIGIN_CF,
                lattice_test_utils::PAYLOAD_CF,
            ],
        )
        .expect("Failed creating database");

        let (
            certificate_map,
            certificate_digest_by_round_map,
            certificate_digest_by_origin_map,
            payload_map,
        ) = lattice_typed_store::reopen!(&rocksdb,
            lattice_test_utils::CERTIFICATES_CF;<CertificateDigest, Certificate>,
            lattice_test_utils::CERTIFICATE_DIGEST_BY_ROUND_CF;<(Round, AuthorityIdentifier), CertificateDigest>,
            lattice_test_utils::CERTIFICATE_DIGEST_BY_ORIGIN_CF;<(AuthorityIdentifier, Round), CertificateDigest>,
            lattice_test_utils::PAYLOAD_CF;<(BatchDigest, WorkerId), PayloadToken>);

        let certificate_store = CertificateStore::new(
            certificate_map,
            certificate_digest_by_round_map,
            certificate_digest_by_origin_map,
            CertificateStoreCache::new(NonZeroUsize::new(100).unwrap(), None),
        );
        let payload_store = PayloadStore::new(payload_map);

        let fixture = CommitteeFixture::builder()
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(4).unwrap())
            .build();
        let committee = fixture.committee();
        let author = fixture.authorities().next().unwrap();
        let id = author.id();
        let worker_cache = fixture.worker_cache();
        let primary = fixture.authorities().next().unwrap();
        let signature_service = SignatureService::new(primary.keypair().copy());
        let metrics = Arc::new(PrimaryMetrics::new(&Registry::new()));
        let primary_channel_metrics = PrimaryChannelMetrics::new(&Registry::new());
        let client = NetworkClient::new_from_keypair(&primary.network_keypair());

        let (header_store, _, _) = create_db_stores();
        let (tx_certificate_fetcher, _rx_certificate_fetcher) = lattice_test_utils::test_channel!(1);
        let (tx_new_certificates, _rx_new_certificates) = lattice_test_utils::test_channel!(100);
        let (tx_parents, _rx_parents) = lattice_test_utils::test_channel!(100);
        let (_tx_consensus_round_updates, rx_consensus_round_updates) =
            watch::channel(ConsensusRound::default());
        let (_tx_narwhal_round_updates, rx_narwhal_round_updates) = watch::channel(1u64);
        let (_tx_synchronizer_network, rx_synchronizer_network) = oneshot::channel();

        let synchronizer = Arc::new(Synchronizer::new(
            id,
            fixture.committee(),
            worker_cache.clone(),
            /* gc_depth */ 50,
            client,
            certificate_store.clone(),
            payload_store.clone(),
            tx_certificate_fetcher,
            tx_new_certificates,
            tx_parents,
            rx_consensus_round_updates,
            rx_synchronizer_network,
            None,
            metrics.clone(),
            &primary_channel_metrics,
        ));
        let handler = PrimaryReceiverHandler {
            authority_id: id,
            committee: fixture.committee(),
            worker_cache: worker_cache.clone(),
            synchronizer: synchronizer.clone(),
            signature_service,
            header_store: header_store.clone(),
            certificate_store: certificate_store.clone(),
            payload_store: payload_store.clone(),
            vote_digest_store: VoteDigestStore::new_for_tests(),
            rx_narwhal_round_updates,
            parent_digests: Default::default(),
            metrics: metrics.clone(),
        };

        // AND some mock certificates
        let mut certificate_digests = Vec::new();
        for _ in 0..10 {
            let header = Header::V1(
                author
                    .header_builder(&committee)
                    .with_payload_batch(lattice_test_utils::fixture_batch_with_transactions(10), 0, 0)
                    .build()
                    .unwrap(),
            );

            let certificate = fixture.certificate(&header);
            let digest = certificate.clone().digest();

            // In order to test an error scenario that is coming from the data store,
            // we are going to store for the provided certificate digests some unexpected
            // payload in order to blow up the deserialisation.
            let serialised_key = bincode::DefaultOptions::new()
                .with_big_endian()
                .with_fixint_encoding()
                .serialize(&digest.borrow())
                .expect("Couldn't serialise key");

            // Just serialise the "false" value
            let dummy_value = bcs::to_bytes(false.borrow()).expect("Couldn't serialise value");

            rocksdb
                .put_cf(
                    &rocksdb
                        .cf_handle(lattice_test_utils::CERTIFICATES_CF)
                        .expect("Couldn't find column family"),
                    serialised_key,
                    dummy_value,
                    &ReadWriteOptions::default().writeopts(),
                )
                .expect("Couldn't insert value");

            certificate_digests.push(digest);
        }

        // WHEN requesting the payload availability for all the certificates
        let request = anemo::Request::new(PayloadAvailabilityRequest { certificate_digests });
        let result = handler.get_payload_availability(request).await;
        assert!(result.is_err(), "expected error reading certificates");
    }

    #[tokio::test]
    async fn test_request_vote_created_at_in_future() {
        telemetry_subscribers::init_for_testing();
        let fixture = CommitteeFixture::builder()
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(4).unwrap())
            .build();
        let worker_cache = fixture.worker_cache();
        let primary = fixture.authorities().next().unwrap();
        let id = primary.id();
        let author = fixture.authorities().nth(2).unwrap();
        let signature_service = SignatureService::new(primary.keypair().copy());
        let metrics = Arc::new(PrimaryMetrics::new(&Registry::new()));
        let primary_channel_metrics = PrimaryChannelMetrics::new(&Registry::new());
        let network = lattice_test_utils::test_network(primary.network_keypair(), primary.address());
        let client = NetworkClient::new_from_keypair(&primary.network_keypair());

        let (header_store, certificate_store, payload_store) = create_db_stores();
        let (tx_certificate_fetcher, _rx_certificate_fetcher) = lattice_test_utils::test_channel!(1);
        let (tx_new_certificates, _rx_new_certificates) = lattice_test_utils::test_channel!(100);
        let (tx_parents, _rx_parents) = lattice_test_utils::test_channel!(100);
        let (_tx_consensus_round_updates, rx_consensus_round_updates) =
            watch::channel(ConsensusRound::new(1, 0));
        let (_tx_narwhal_round_updates, rx_narwhal_round_updates) = watch::channel(1u64);
        let (_tx_synchronizer_network, rx_synchronizer_network) = oneshot::channel();

        let synchronizer = Arc::new(Synchronizer::new(
            id,
            fixture.committee(),
            worker_cache.clone(),
            /* gc_depth */ 50,
            client.clone(),
            certificate_store.clone(),
            payload_store.clone(),
            tx_certificate_fetcher,
            tx_new_certificates,
            tx_parents,
            rx_consensus_round_updates,
            rx_synchronizer_network,
            None,
            metrics.clone(),
            &primary_channel_metrics,
        ));
        let handler = PrimaryReceiverHandler {
            authority_id: id,
            committee: fixture.committee(),
            worker_cache: worker_cache.clone(),
            synchronizer: synchronizer.clone(),
            signature_service,
            header_store: header_store.clone(),
            certificate_store: certificate_store.clone(),
            payload_store: payload_store.clone(),
            vote_digest_store: VoteDigestStore::new_for_tests(),
            rx_narwhal_round_updates,
            parent_digests: Default::default(),
            metrics: metrics.clone(),
        };

        // Make some mock certificates that are parents of our new header.
        let mut certificates = HashMap::new();
        for primary in fixture.authorities().filter(|a| a.id() != id) {
            let header = Header::V1(
                primary
                    .header_builder(&fixture.committee())
                    .with_payload_batch(lattice_test_utils::fixture_batch_with_transactions(10), 0, 0)
                    .build()
                    .unwrap(),
            );

            let certificate = fixture.certificate(&header);
            let digest = certificate.clone().digest();

            certificates.insert(digest, certificate.clone());
            certificate_store.write(certificate.clone()).unwrap();
            for (digest, (worker_id, _)) in certificate.header().payload() {
                payload_store.write(digest, worker_id).unwrap();
            }
        }

        // Set up mock worker.
        let worker = primary.worker(1);
        let worker_address = &worker.info().worker_address;
        let worker_peer_id = anemo::PeerId(worker.keypair().public().0.to_bytes());
        let mut mock_server = MockPrimaryToWorker::new();
        // Always Synchronize successfully.
        mock_server.expect_synchronize().returning(|_| Ok(anemo::Response::new(())));

        client.set_primary_to_worker_local_handler(worker_peer_id, Arc::new(mock_server));

        let _worker_network = worker.new_network(anemo::Router::new());
        let address = worker_address.to_anemo_address().unwrap();
        network.connect_with_peer_id(address, worker_peer_id).await.unwrap();

        // Verify Handler generates a Vote.

        // Set the creation time to be deep in the future (an hour)
        let created_at = now() + 60 * 60 * 1000;

        let test_header = Header::V1(
            author
                .header_builder(&fixture.committee())
                .round(2)
                .parents(certificates.keys().cloned().collect())
                .with_payload_batch(lattice_test_utils::fixture_batch_with_transactions(10), 1, 0)
                .created_at(created_at)
                .build()
                .unwrap(),
        );

        let mut request = anemo::Request::new(RequestVoteRequest {
            header: test_header.clone(),
            parents: Vec::new(),
        });
        assert!(request.extensions_mut().insert(network.downgrade()).is_none());
        assert!(request
            .extensions_mut()
            .insert(anemo::PeerId(author.network_public_key().0.to_bytes()))
            .is_none());

        // For such a future header we get back an error
        assert!(handler.request_vote(request).await.is_err());

        // Verify Handler generates a Vote.

        // Set the creation time to be a bit in the future (500 ms)
        let created_at = now() + 500;

        let test_header = author
            .header_builder(&fixture.committee())
            .round(2)
            .parents(certificates.keys().cloned().collect())
            .with_payload_batch(lattice_test_utils::fixture_batch_with_transactions(10), 1, 0)
            .created_at(created_at)
            .build()
            .unwrap();

        let mut request = anemo::Request::new(RequestVoteRequest {
            header: Header::V1(test_header.clone()),
            parents: Vec::new(),
        });
        assert!(request.extensions_mut().insert(network.downgrade()).is_none());
        assert!(request
            .extensions_mut()
            .insert(anemo::PeerId(author.network_public_key().0.to_bytes()))
            .is_none());

        let response = handler.request_vote(request).await.unwrap();
        assert!(response.body().vote.is_some());

        // We are now later
        assert!(created_at < now());
    }

}