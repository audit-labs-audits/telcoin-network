// Copyright (c) Telcoin, LLC
//! How the primary's network should handle incoming messages from workers.
//! Primary and workers only communicate locally for now.
use crate::{
    metrics::{PrimaryMetrics},
    synchronizer::Synchronizer, primary::FETCH_CERTIFICATES_MAX_HANDLER_TIME, error::{PrimaryResult, PrimaryError},
};
use anemo::{
    codegen::InboundRequestLayer,
    types::{Address, PeerInfo},
    Network, PeerId,
};
use async_trait::async_trait;
use consensus_metrics::{
    metered_channel::{channel_with_total, Receiver, Sender},
    monitored_scope,
};
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
        crypto::{
            self, traits::EncodeDecodeBase64, AuthorityKeyPair, NetworkKeyPair, NetworkPublicKey, AuthoritySignature,
        },
        Header, HeaderAPI, Round, Vote, PreSubscribedBroadcastSender, VoteInfoAPI, 
        now, Certificate, CertificateAPI, CertificateDigest,
    },
    ensure,
};
use tn_network_types::{
        FetchCertificatesRequest,
        FetchCertificatesResponse, GetCertificatesRequest, GetCertificatesResponse,
        PayloadAvailabilityRequest, PayloadAvailabilityResponse,
        PrimaryToPrimary, PrimaryToPrimaryServer, RequestVoteRequest,
        RequestVoteResponse, SendCertificateRequest, SendCertificateResponse,
        WorkerToPrimaryServer,
};
use tokio::{
    sync::{oneshot, watch},
    task::JoinHandle,
    time::Instant,
};
use tower::ServiceBuilder;
use tracing::{debug, error, info, instrument, warn};

/// Defines how the network receiver handles incoming primary messages.
#[derive(Clone)]
pub(crate) struct PrimaryToPrimaryHandler {
    /// The id of this primary.
    pub (crate) authority_id: AuthorityIdentifier,
    pub (crate) committee: Committee,
    pub (crate) worker_cache: WorkerCache,
    pub (crate) synchronizer: Arc<Synchronizer>,
    /// Service to sign headers.
    pub (crate) signature_service: SignatureService<AuthoritySignature, { crypto::INTENT_MESSAGE_LENGTH }>,
    pub (crate) header_store: HeaderStore,
    pub (crate) certificate_store: CertificateStore,
    pub (crate) payload_store: PayloadStore,
    /// The store to persist the last voted round per authority, used to ensure idempotence.
    pub (crate) vote_digest_store: VoteDigestStore,
    /// Get a signal when the round changes.
    pub (crate) rx_narwhal_round_updates: watch::Receiver<Round>,
    /// Known parent digests that are being fetched from header proposers.
    /// Values are where the digests are first known from.
    /// TODO: consider limiting maximum number of digests from one authority, allow timeout
    /// and retries from other authorities.
    pub (crate) parent_digests: Arc<Mutex<BTreeMap<(Round, CertificateDigest), AuthorityIdentifier>>>,
    pub (crate) metrics: Arc<PrimaryMetrics>,
}

#[allow(clippy::result_large_err)]
impl PrimaryToPrimaryHandler {
    pub(crate) fn new(
        authority_id: AuthorityIdentifier,
        committee: Committee,
        worker_cache: WorkerCache,
        synchronizer: Arc<Synchronizer>,
        signature_service: SignatureService<AuthoritySignature, { crypto::INTENT_MESSAGE_LENGTH }>,
        header_store: HeaderStore,
        certificate_store: CertificateStore,
        payload_store: PayloadStore,
        vote_digest_store: VoteDigestStore,
        rx_narwhal_round_updates: watch::Receiver<Round>,
        parent_digests: Arc<Mutex<BTreeMap<(Round, CertificateDigest), AuthorityIdentifier>>>,
        metrics: Arc<PrimaryMetrics>,
    ) -> Self {
        Self {
            authority_id,
            committee,
            worker_cache,
            synchronizer,
            signature_service,
            header_store,
            certificate_store,
            payload_store,
            vote_digest_store,
            rx_narwhal_round_updates,
            parent_digests,
            metrics,
        }
    }

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
    ) -> PrimaryResult<RequestVoteResponse> {
        let header = &request.body().header;
        let committee = self.committee.clone();
        header.validate(&committee, &self.worker_cache)?;

        let num_parents = request.body().parents.len();
        ensure!(
            num_parents <= committee.size(),
            PrimaryError::TooManyParents(num_parents, committee.size())
        );
        self.metrics.certificates_in_votes.inc_by(num_parents as u64);

        // Vote request must come from the Header's author.
        let peer_id = request
            .peer_id()
            .ok_or_else(|| PrimaryError::NetworkError("Unable to access remote peer ID".to_owned()))?;
        let peer_network_key = NetworkPublicKey::from_bytes(&peer_id.0).map_err(|e| {
            PrimaryError::NetworkError(format!(
                "Unable to interpret remote peer ID {peer_id:?} as a NetworkPublicKey: {e:?}"
            ))
        })?;
        let peer_authority =
            committee.authority_by_network_key(&peer_network_key).ok_or_else(|| {
                PrimaryError::NetworkError(format!(
                    "Unable to find authority with network key {peer_network_key:?}"
                ))
            })?;
        ensure!(
            header.author() == peer_authority.id(),
            PrimaryError::NetworkError(format!(
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
                PrimaryError::HeaderHasInvalidParentRoundNumbers(header.digest())
            );
            ensure!(
                header.created_at() >= parent.header().created_at(),
                PrimaryError::HeaderHasInvalidParentTimestamp(header.digest())
            );
            ensure!(
                parent_authorities.insert(parent.header().author()),
                PrimaryError::HeaderHasDuplicateParentAuthorities(header.digest())
            );
            stake += committee.stake_by_id(parent.origin());
        }
        ensure!(
            stake >= committee.quorum_threshold(),
            PrimaryError::HeaderRequiresQuorum(header.digest())
        );

        // Synchronize all batches referenced in the header.
        self.synchronizer.sync_header_batches(header, /* max_age */ 0).await?;

        // Check that the time of the header is smaller than the current time. If not but the
        // difference is small, just wait. Otherwise reject with an error.
        const TOLERANCE_SEC: u64 = 1;
        let current_time = now();
        if current_time < *header.created_at() {
            if *header.created_at() - current_time < TOLERANCE_SEC {
                // for a small difference we simply wait
                tokio::time::sleep(Duration::from_secs(*header.created_at() - current_time))
                    .await;
            } else {
                // For larger differences return an error, and log it
                warn!(
                    "Rejected header {:?} due to timestamp {} newer than {current_time}",
                    header,
                    *header.created_at()
                );
                return Err(PrimaryError::InvalidTimestamp {
                    created_time: *header.created_at(),
                    local_time: current_time,
                })
            }
        }

        // Store the header.
        self.header_store.write(header).map_err(PrimaryError::StoreError)?;

        // Check if we can vote for this header.
        // Send the vote when:
        // 1. when there is no existing vote for this publicKey & epoch/round
        // 2. when there is a vote for this publicKey & epoch/round, and the vote is the same
        // Taking the inverse of these two, the only time we don't want to vote is when:
        // there is a digest for the publicKey & epoch/round, and it does not match the digest
        // of the vote we create for this header.
        // Also when the header is older than one we've already voted for, it is useless to vote,
        // so we don't.
        let result = self.vote_digest_store.read(&header.author()).map_err(PrimaryError::StoreError)?;

        if let Some(vote_info) = result {
            ensure!(
                header.epoch() == vote_info.epoch(),
                PrimaryError::InvalidEpoch { expected: header.epoch(), received: vote_info.epoch() }
            );
            ensure!(
                header.round() >= vote_info.round(),
                PrimaryError::AlreadyVotedNewerHeader(
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
                    return Err(PrimaryError::AlreadyVoted(
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
    ) -> PrimaryResult<()> {
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
    ) -> PrimaryResult<Vec<CertificateDigest>> {
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
            PrimaryError::TooOld(header.digest().into(), header.round(), narwhal_round)
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
impl PrimaryToPrimary for PrimaryToPrimaryHandler {
    async fn send_certificate(
        &self,
        request: anemo::Request<SendCertificateRequest>,
    ) -> Result<anemo::Response<SendCertificateResponse>, anemo::rpc::Status> {
        let _scope = monitored_scope("PrimaryReceiverHandler::send_certificate");
        let certificate = request.into_body().certificate;
        match self.synchronizer.try_accept_certificate(certificate).await {
            Ok(()) => Ok(anemo::Response::new(SendCertificateResponse { accepted: true })),
            Err(PrimaryError::Suspended(_)) => {
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
                    PrimaryError::InvalidSignature |
                    PrimaryError::InvalidEpoch { .. } |
                    PrimaryError::InvalidHeaderDigest |
                    PrimaryError::HeaderHasBadWorkerIds(_) |
                    PrimaryError::HeaderHasInvalidParentRoundNumbers(_) |
                    PrimaryError::HeaderHasDuplicateParentAuthorities(_) |
                    PrimaryError::AlreadyVoted(_, _, _) |
                    PrimaryError::AlreadyVotedNewerHeader(_, _, _) |
                    PrimaryError::HeaderRequiresQuorum(_) |
                    PrimaryError::TooOld(_, _, _) => anemo::types::response::StatusCode::BadRequest,
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

