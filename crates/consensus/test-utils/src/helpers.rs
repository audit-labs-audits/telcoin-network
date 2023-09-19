// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper methods for test-utils.
use crate::BATCHES_CF;
use tn_types::consensus::{Multiaddr, VersionedMetadata};
use execution_transaction_pool::{TransactionId, SenderId};
use fastcrypto::{
    hash::Hash as _,
    traits::KeyPair as _,
};
use indexmap::IndexMap;
use lattice_payload_builder::{BatchPayload, LatticePayloadBuilderError, LatticePayloadBuilderHandle, LatticePayloadBuilderServiceCommand};
use lattice_typed_store::rocks::{DBMap, MetricConf, ReadWriteOptions};
use rand::{
    distributions::{Bernoulli, Distribution},
    rngs::{StdRng, OsRng},
    thread_rng, Rng, RngCore, SeedableRng,
};
use telemetry_subscribers::TelemetryGuards;
use tokio::sync::mpsc::{Receiver, UnboundedReceiver};
use std::{
    collections::{BTreeSet, VecDeque},
    ops::RangeInclusive, future::Future, pin::Pin, task::{Poll, Context},
};
use tn_types::consensus::{
    crypto::{
        to_intent_message, AuthorityKeyPair, NarwhalAuthoritySignature,
        AuthoritySignature, NetworkKeyPair,
    },
    AuthorityIdentifier, Committee, Epoch, Stake, WorkerId,
    Batch, BatchDigest, Certificate, CertificateAPI, CertificateDigest,
    Header, HeaderAPI, HeaderV1Builder, Round, TimestampSec, Transaction
};
use std::sync::Arc;

pub fn temp_dir() -> std::path::PathBuf {
    tempfile::tempdir().expect("Failed to open temporary directory").into_path()
}

pub fn ensure_test_environment() {
    // One common issue when running tests on Mac is that the default ulimit is too low,
    // leading to I/O errors such as "Too many open files". Raising fdlimit to bypass it.
    // Also we can't do this in Windows, apparently.
    #[cfg(not(target_os = "windows"))]
    fdlimit::raise_fd_limit().expect("Could not raise ulimit");
}

#[macro_export]
macro_rules! test_channel {
    ($e:expr) => {
        consensus_metrics::metered_channel::channel(
            $e,
            &prometheus::IntGauge::new("TEST_COUNTER", "test counter").unwrap(),
        );
    };
}

// Note: use the following macros to initialize your Primary / Consensus channels
// if your test is spawning a primary and you encounter an `AllReg` error.
//
// Rationale:
// The primary initialization will try to edit a specific metric in its registry
// for its new_certificates and committeed_certificates channel. The gauge situated
// in the channel you're passing as an argument to the primary initialization is
// the replacement. If that gauge is a dummy gauge, such as the one above, the
// initialization of the primary will panic (to protect the production code against
// an erroneous mistake in editing this bootstrap logic).
#[macro_export]
macro_rules! test_committed_certificates_channel {
    ($e:expr) => {
        consensus_metrics::metered_channel::channel(
            $e,
            &prometheus::IntGauge::new(
                lattice_primary::PrimaryChannelMetrics::NAME_COMMITTED_CERTS,
                lattice_primary::PrimaryChannelMetrics::DESC_COMMITTED_CERTS,
            )
            .unwrap(),
        );
    };
}

#[macro_export]
macro_rules! test_new_certificates_channel {
    ($e:expr) => {
        consensus_metrics::metered_channel::channel(
            $e,
            &prometheus::IntGauge::new(
                lattice_primary::PrimaryChannelMetrics::NAME_NEW_CERTS,
                lattice_primary::PrimaryChannelMetrics::DESC_NEW_CERTS,
            )
            .unwrap(),
        );
    };
}

////////////////////////////////////////////////////////////////
/// Keys, Committee
////////////////////////////////////////////////////////////////

/// Generate a new [AuthorityKeyPair].
pub fn random_key() -> AuthorityKeyPair {
    AuthorityKeyPair::generate(&mut thread_rng())
}

////////////////////////////////////////////////////////////////
/// Headers, Votes, Certificates
////////////////////////////////////////////////////////////////
pub fn fixture_payload(number_of_batches: u8) -> IndexMap<BatchDigest, (WorkerId, TimestampSec)> {
    let mut payload: IndexMap<BatchDigest, (WorkerId, TimestampSec)> = IndexMap::new();

    for _ in 0..number_of_batches {
        let batch_digest = batch().digest();

        payload.insert(batch_digest, (0, 0));
    }

    payload
}

/// Creates an invalid batch with randomly formed transactions
/// dictated by the parameter number_of_transactions
/// 
/// The metadata is invalid.
/// TODO: consolidate this with execution/interfaces/test-utils/generators
pub fn fixture_batch_with_transactions(number_of_transactions: u32) -> Batch {
    let transactions = (0..number_of_transactions).map(|_v| transaction()).collect();

    Batch::new(transactions)
}

pub fn fixture_payload_with_rand<R: Rng + ?Sized>(
    number_of_batches: u8,
    rand: &mut R,
) -> IndexMap<BatchDigest, (WorkerId, TimestampSec)> {
    let mut payload: IndexMap<BatchDigest, (WorkerId, TimestampSec)> = IndexMap::new();

    for _ in 0..number_of_batches {
        let batch_digest = batch_with_rand(rand).digest();

        payload.insert(batch_digest, (0, 0));
    }

    payload
}

pub fn transaction_with_rand<R: Rng + ?Sized>(rand: &mut R) -> Transaction {
    // generate random value transactions, but the length will be always 100 bytes
    (0..100).map(|_v| rand.gen_range(u8::MIN..=u8::MAX)).collect()
}

pub fn batch_with_rand<R: Rng + ?Sized>(rand: &mut R) -> Batch {
    Batch::new(vec![transaction_with_rand(rand), transaction_with_rand(rand)])
}

/// Generate random value transactions, but the length will be always 100 bytes
pub fn transaction() -> Transaction {
    (0..100).map(|_v| rand::random::<u8>()).collect()
}

/// Generate a known value for transaction.
pub fn known_transaction_1() -> Transaction {
    (0..100).collect()
}

////////////////////////////////////////////////////////////////
/// Batches
////////////////////////////////////////////////////////////////

// Fixture
pub fn batch() -> Batch {
    Batch::new(vec![transaction(), transaction()])
}

/// Built payload from the EL
/// 
/// Returns a BatchPayload with known values.
pub fn build_batch() -> Result<Arc<BatchPayload>, LatticePayloadBuilderError> {
   Ok(Arc::new(
        BatchPayload::new(
            vec![known_transaction_1()],
            Default::default(),
            vec![
                TransactionId {
                    sender: SenderId::from(3),
                    nonce: 0
                }
            ],
            Default::default(),
        )
    ))
}

/// Mock representation of a job that returns a Future built batch payload resutl.
#[derive(Default, Clone)]
pub struct MockBatchBuildJob;
impl Future for MockBatchBuildJob {
    type Output = Result<Arc<BatchPayload>, LatticePayloadBuilderError>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(build_batch())
    }
}

/// generate multiple fixture batches. The number of generated batches
/// are dictated by the parameter num_of_batches.
pub fn batches(num_of_batches: usize) -> Vec<Batch> {
    let mut batches = Vec::new();

    for i in 1..num_of_batches + 1 {
        batches.push(batch_with_transactions(i));
    }

    batches
}

pub fn batch_with_transactions(num_of_transactions: usize) -> Batch {
    let mut transactions = Vec::new();

    for _ in 0..num_of_transactions {
        transactions.push(transaction());
    }

    Batch::new(transactions)
}

pub fn create_batch_store() -> DBMap<BatchDigest, Batch> {
    DBMap::<BatchDigest, Batch>::open(
        temp_dir(),
        MetricConf::default(),
        None,
        Some(BATCHES_CF),
        &ReadWriteOptions::default(),
    )
    .unwrap()
}

/// Creates one certificate per authority starting and finishing at the specified rounds (inclusive).
/// Outputs a VecDeque of certificates (the certificate with higher round is on the front) and a set
/// of digests to be used as parents for the certificates of the next round.
/// Note : the certificates are unsigned
pub fn make_optimal_certificates(
    committee: &Committee,
    range: RangeInclusive<Round>,
    initial_parents: &BTreeSet<CertificateDigest>,
    ids: &[AuthorityIdentifier],
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>) {
    make_certificates(committee, range, initial_parents, ids, 0.0)
}

/// Outputs rounds worth of certificates with optimal parents, signed
pub fn make_optimal_signed_certificates(
    range: RangeInclusive<Round>,
    initial_parents: &BTreeSet<CertificateDigest>,
    committee: &Committee,
    keys: &[(AuthorityIdentifier, AuthorityKeyPair)],
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>) {
    make_signed_certificates(range, initial_parents, committee, keys, 0.0)
}

/// Bernoulli-samples from a set of ancestors passed as a argument,
fn this_cert_parents(
    ancestors: &BTreeSet<CertificateDigest>,
    failure_prob: f64,
) -> BTreeSet<CertificateDigest> {
    std::iter::from_fn(|| {
        let f: f64 = rand::thread_rng().gen();
        Some(f > failure_prob)
    })
    .take(ancestors.len())
    .zip(ancestors)
    .flat_map(|(parenthood, parent)| parenthood.then_some(*parent))
    .collect::<BTreeSet<_>>()
}

/// Utility for making several rounds worth of certificates through iterated parenthood sampling.
/// The making of individual certificates once parents are figured out is delegated to the
/// `make_one_certificate` argument
fn rounds_of_certificates(
    range: RangeInclusive<Round>,
    initial_parents: &BTreeSet<CertificateDigest>,
    ids: &[AuthorityIdentifier],
    failure_probability: f64,
    make_one_certificate: impl Fn(
        AuthorityIdentifier,
        Round,
        BTreeSet<CertificateDigest>,
    ) -> (CertificateDigest, Certificate),
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>) {
    let mut certificates = VecDeque::new();
    let mut parents = initial_parents.iter().cloned().collect::<BTreeSet<_>>();
    let mut next_parents = BTreeSet::new();

    for round in range {
        next_parents.clear();
        for id in ids {
            let this_cert_parents = this_cert_parents(&parents, failure_probability);

            let (digest, certificate) = make_one_certificate(*id, round, this_cert_parents);
            certificates.push_back(certificate);
            next_parents.insert(digest);
        }
        parents = next_parents.clone();
    }
    (certificates, next_parents)
}

/// make rounds worth of unsigned certificates with the sampled number of parents
pub fn make_certificates(
    committee: &Committee,
    range: RangeInclusive<Round>,
    initial_parents: &BTreeSet<CertificateDigest>,
    ids: &[AuthorityIdentifier],
    failure_probability: f64,
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>) {
    let generator = |pk, round, parents| mock_certificate(committee, pk, round, parents);

    rounds_of_certificates(range, initial_parents, ids, failure_probability, generator)
}

/// Creates certificates for the provided rounds but also having slow nodes.
/// - `range`: the rounds for which we intend to create the certificates for
/// - `initial_parents`: the parents to use when start creating the certificates
/// - `keys`: the authorities for which it will create certificates for
/// - `slow_nodes`: the authorities which are considered slow.
/// Slow authorities still create certificates for each round,
/// but no other authority from a higher round will refer to their certificates.
/// 
/// Note: The number (by stake) of slow_nodes can not be > f , as
/// otherwise no valid graph will be produced.
pub fn make_certificates_with_slow_nodes(
    committee: &Committee,
    range: RangeInclusive<Round>,
    initial_parents: Vec<Certificate>,
    names: &[AuthorityIdentifier],
    slow_nodes: &[(AuthorityIdentifier, f64)],
) -> (VecDeque<Certificate>, Vec<Certificate>) {
    let mut rand = StdRng::seed_from_u64(1);

    // ensure provided slow nodes do not account > f
    let slow_nodes_stake: Stake =
        slow_nodes.iter().map(|(key, _)| committee.authority(key).unwrap().stake()).sum();

    assert!(slow_nodes_stake < committee.validity_threshold());

    let mut certificates = VecDeque::new();
    let mut parents = initial_parents;
    let mut next_parents = Vec::new();

    for round in range {
        next_parents.clear();
        for name in names {
            let this_cert_parents = this_cert_parents_with_slow_nodes(
                name,
                parents.clone(),
                slow_nodes,
                &mut rand,
                committee,
            );

            let (_, certificate) = mock_certificate(committee, *name, round, this_cert_parents);
            certificates.push_back(certificate.clone());
            next_parents.push(certificate);
        }
        parents = next_parents.clone();
    }
    (certificates, next_parents)
}

/// Returns the parents that should be used as part of a newly created certificate.
/// 
/// The `slow_nodes` parameter is used to dictate which parents to exclude and not use. The slow
/// node will not be used under some probability which is provided as part of the tuple.
/// 
/// If probability to use it is 0.0, then the parent node will NEVER be used.
/// 
/// If probability to use it is 1.0, then the parent node will ALWAYS be used.
/// 
/// We always make sure to include our "own" certificate, thus the `name` property is needed.
pub fn this_cert_parents_with_slow_nodes(
    authority_id: &AuthorityIdentifier,
    ancestors: Vec<Certificate>,
    slow_nodes: &[(AuthorityIdentifier, f64)],
    rand: &mut StdRng,
    committee: &Committee,
) -> BTreeSet<CertificateDigest> {
    let mut parents = BTreeSet::new();
    let mut not_included = Vec::new();
    let mut total_stake = 0;

    for parent in ancestors {
        let authority = committee.authority(&parent.origin()).unwrap();

        // Identify if the parent is within the slow nodes - and is not the same author as the
        // one we want to create the certificate for.
        if let Some((_, inclusion_probability)) = slow_nodes
            .iter()
            .find(|(id, _)| *id != *authority_id && *id == parent.header().author())
        {
            let b = Bernoulli::new(*inclusion_probability).unwrap();
            let should_include = b.sample(rand);

            if should_include {
                parents.insert(parent.digest());
                total_stake += authority.stake();
            } else {
                not_included.push(parent);
            }
        } else {
            // just add it directly as it is not within the slow nodes or we are the
            // same author.
            parents.insert(parent.digest());
            total_stake += authority.stake();
        }
    }

    // ensure we'll have enough parents (2f + 1)
    while total_stake < committee.quorum_threshold() {
        let parent = not_included.pop().unwrap();
        let authority = committee.authority(&parent.origin()).unwrap();

        total_stake += authority.stake();

        parents.insert(parent.digest());
    }

    assert!(
        committee.reached_quorum(total_stake),
        "Not enough parents by stake provided. Expected at least {} but instead got {}",
        committee.quorum_threshold(),
        total_stake
    );

    parents
}

/// make rounds worth of unsigned certificates with the sampled number of parents
pub fn make_certificates_with_epoch(
    committee: &Committee,
    range: RangeInclusive<Round>,
    epoch: Epoch,
    initial_parents: &BTreeSet<CertificateDigest>,
    keys: &[AuthorityIdentifier],
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>) {
    let mut certificates = VecDeque::new();
    let mut parents = initial_parents.iter().cloned().collect::<BTreeSet<_>>();
    let mut next_parents = BTreeSet::new();

    for round in range {
        next_parents.clear();
        for name in keys {
            let (digest, certificate) =
                mock_certificate_with_epoch(committee, *name, round, epoch, parents.clone());
            certificates.push_back(certificate);
            next_parents.insert(digest);
        }
        parents = next_parents.clone();
    }
    (certificates, next_parents)
}

/// make rounds worth of signed certificates with the sampled number of parents
pub fn make_signed_certificates(
    range: RangeInclusive<Round>,
    initial_parents: &BTreeSet<CertificateDigest>,
    committee: &Committee,
    keys: &[(AuthorityIdentifier, AuthorityKeyPair)],
    failure_probability: f64,
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>) {
    let ids = keys.iter().map(|(authority, _)| *authority).collect::<Vec<_>>();
    let generator =
        |pk, round, parents| mock_signed_certificate(keys, pk, round, parents, committee);

    rounds_of_certificates(range, initial_parents, &ids[..], failure_probability, generator)
}

pub fn mock_certificate_with_rand<R: RngCore + ?Sized>(
    committee: &Committee,
    origin: AuthorityIdentifier,
    round: Round,
    parents: BTreeSet<CertificateDigest>,
    rand: &mut R,
) -> (CertificateDigest, Certificate) {
    let header_builder = HeaderV1Builder::default();
    let header = header_builder
        .author(origin)
        .round(round)
        .epoch(0)
        .parents(parents)
        .payload(fixture_payload_with_rand(1, rand))
        .build()
        .unwrap();
    let certificate = Certificate::new_unsigned(committee, Header::V1(header), Vec::new()).unwrap();
    (certificate.digest(), certificate)
}

/// Creates a badly signed certificate from its given round, origin and parents,
/// Note: the certificate is signed by a random key rather than its author
pub fn mock_certificate(
    committee: &Committee,
    origin: AuthorityIdentifier,
    round: Round,
    parents: BTreeSet<CertificateDigest>,
) -> (CertificateDigest, Certificate) {
    mock_certificate_with_epoch(committee, origin, round, 0, parents)
}

/// Creates a badly signed certificate from its given round, epoch, origin, and parents,
/// Note: the certificate is signed by a random key rather than its author
pub fn mock_certificate_with_epoch(
    committee: &Committee,
    origin: AuthorityIdentifier,
    round: Round,
    epoch: Epoch,
    parents: BTreeSet<CertificateDigest>,
) -> (CertificateDigest, Certificate) {
    let header_builder = HeaderV1Builder::default();
    let header = header_builder
        .author(origin)
        .round(round)
        .epoch(epoch)
        .parents(parents)
        .payload(fixture_payload(1))
        .build()
        .unwrap();
    let certificate = Certificate::new_unsigned(committee, Header::V1(header), Vec::new()).unwrap();
    (certificate.digest(), certificate)
}

/// Creates one signed certificate from a set of signers - the signers must include the origin
pub fn mock_signed_certificate(
    signers: &[(AuthorityIdentifier, AuthorityKeyPair)],
    origin: AuthorityIdentifier,
    round: Round,
    parents: BTreeSet<CertificateDigest>,
    committee: &Committee,
) -> (CertificateDigest, Certificate) {
    let header_builder = HeaderV1Builder::default()
        .author(origin)
        .payload(fixture_payload(1))
        .round(round)
        .epoch(0)
        .parents(parents);

    let header = header_builder.build().unwrap();

    let cert =
        Certificate::new_unsigned(committee, Header::V1(header.clone()), Vec::new()).unwrap();

    let mut votes = Vec::new();
    for (name, signer) in signers {
        let sig = AuthoritySignature::new_secure(&to_intent_message(cert.header().digest()), signer);
        votes.push((*name, sig))
    }
    let cert = Certificate::new_unverified(committee, Header::V1(header), votes).unwrap();
    (cert.digest(), cert)
}

pub fn test_network(keypair: NetworkKeyPair, address: &Multiaddr) -> anemo::Network {
    let address = address.to_anemo_address().unwrap();
    let network_key = keypair.private().0.to_bytes();
    anemo::Network::bind(address)
        .server_name("lattice")
        .private_key(network_key)
        .start(anemo::Router::new())
        .unwrap()
}

/// Create a network with a new network key and wildcard port.
pub fn random_network() -> anemo::Network {
    let network_key = NetworkKeyPair::generate(&mut StdRng::from_rng(OsRng).unwrap());
    let address = "/ip4/127.0.0.1/udp/0".parse().unwrap();
    test_network(network_key, &address)
}

/// Start tracing for tests
pub fn setup_tracing() -> TelemetryGuards {
    // Setup tracing
    let tracing_level = "debug";
    let network_tracing_level = "info";

    let log_filter = format!("{tracing_level},h2={network_tracing_level},tower={network_tracing_level},hyper={network_tracing_level},tonic::transport={network_tracing_level},quinn={network_tracing_level}");

    telemetry_subscribers::TelemetryConfig::new()
        // load env variables
        .with_env()
        // load special log filter
        .with_log_level(&log_filter)
        .init()
        .0
}

/// Create a handle for a payload builder.
pub fn payload_builder() -> (UnboundedReceiver<LatticePayloadBuilderServiceCommand>, LatticePayloadBuilderHandle) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let handle = LatticePayloadBuilderHandle::new(tx);
    (rx, handle)
}
