use crate::consensus::{
    crypto::{
        self, to_intent_message, AggregateAuthoritySignature, AggregateAuthoritySignatureBytes,
        NarwhalAuthorityAggregateSignature, AuthorityPublicKey, AuthoritySignature,
    },
    dag::node_dag::Affiliated,
    error::{DagError, DagResult},
    now,
    serde::NarwhalBitmap,
    CertificateDigestProto, Header, HeaderAPI, HeaderV1, Round, TimestampMs, WorkerCache, config::Stake, AuthorityIdentifier, Epoch, Committee,
};
use bytes::Bytes;
use consensus_util_mem::MallocSizeOf;
use enum_dispatch::enum_dispatch;
use fastcrypto::{
    hash::{Digest, Hash},
    traits::AggregateAuthenticator,
};
use indexmap::IndexMap;
#[cfg(any(test, feature = "arbitrary"))]
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::{collections::VecDeque, fmt};

use super::{Batch, BatchAPI, MetadataAPI};

/// Versioned certificate. Certificates are the output of consensus.
#[derive(Clone, Serialize, Deserialize, MallocSizeOf)]
#[enum_dispatch(CertificateAPI)]
pub enum Certificate {
    /// V1
    V1(CertificateV1),
}

// TODO: Revisit if we should not impl Default for Certificate
impl Default for Certificate {
    fn default() -> Self {
        Self::V1(CertificateV1::default())
    }
}

impl Certificate {
    /// TODO: Add version number and match on that
    pub fn genesis(committee: &Committee) -> Vec<Self> {
        CertificateV1::genesis(committee).into_iter().map(Self::V1).collect()
    }

    /// Create genesis with header payload for [CertificateV1]
    pub fn genesis_with_payload(
        committee: &Committee,
        batch: Batch,
    ) -> Vec<Self> {
        CertificateV1::genesis_with_payload(committee, batch).into_iter().map(Self::V1).collect()
    }

    /// Create a new, unsafe certificate that checks stake, but does not verify authority signatures.
    pub fn new_unverified(
        committee: &Committee,
        header: Header,
        votes: Vec<(AuthorityIdentifier, AuthoritySignature)>,
    ) -> DagResult<Certificate> {
        CertificateV1::new_unverified(committee, header, votes)
    }

    /// Create a new, unsafe certificate without verifying authority signatures or stake.
    pub fn new_unsigned(
        committee: &Committee,
        header: Header,
        votes: Vec<(AuthorityIdentifier, AuthoritySignature)>,
    ) -> DagResult<Certificate> {
        CertificateV1::new_unsigned(committee, header, votes)
    }

    /// Return the group of authorities that signed this certificate.
    /// 
    /// This function requires that certificate was verified against given committee
    pub fn signed_authorities(&self, committee: &Committee) -> Vec<AuthorityPublicKey> {
        match self {
            Certificate::V1(certificate) => certificate.signed_authorities(committee),
        }
    }

    /// Return the total stake and group of authorities that formed the committee for this certificate.
    pub fn signed_by(&self, committee: &Committee) -> (Stake, Vec<AuthorityPublicKey>) {
        match self {
            Certificate::V1(certificate) => certificate.signed_by(committee),
        }
    }

    /// Verify the certificate's authority signatures.
    pub fn verify(&self, committee: &Committee, worker_cache: &WorkerCache) -> DagResult<()> {
        match self {
            Certificate::V1(certificate) => certificate.verify(committee, worker_cache),
        }
    }

    /// The certificate's round.
    pub fn round(&self) -> Round {
        match self {
            Certificate::V1(certificate) => certificate.round(),
        }
    }

    /// The certificate's epoch.
    pub fn epoch(&self) -> Epoch {
        match self {
            Certificate::V1(certificate) => certificate.epoch(),
        }
    }

    /// The author of the certificate.
    pub fn origin(&self) -> AuthorityIdentifier {
        match self {
            Certificate::V1(certificate) => certificate.origin(),
        }
    }
}

impl Hash<{ crypto::DIGEST_LENGTH }> for Certificate {
    type TypedDigest = CertificateDigest;

    fn digest(&self) -> CertificateDigest {
        match self {
            Certificate::V1(data) => data.digest(),
        }
    }
}

/// API for certrificates based on version.
#[enum_dispatch]
pub trait CertificateAPI {
    /// The header for the certificate.
    fn header(&self) -> &Header;

    /// The aggregate signature for the certriciate.
    fn aggregated_signature(&self) -> &AggregateAuthoritySignatureBytes;

    /// The bitmap of signed authorities for the certificate.
    /// 
    /// This is the aggregate signature of all authrotities for the certificate.
    fn signed_authorities(&self) -> &roaring::RoaringBitmap;

    /// The time (ms) when the certificate was created.
    fn created_at(&self) -> &TimestampMs;

    /// Only Used for testing.
    /// Change the certificate's header.
    fn update_header(&mut self, header: Header);

    /// Return a mutable reference to the header.
    fn header_mut(&mut self) -> &mut Header;
}

/// The certificate issued after a successful round of consensus.
#[serde_as]
#[derive(Clone, Serialize, Deserialize, Default, MallocSizeOf, Debug)]
pub struct CertificateV1 {
    /// Certificate's header.
    pub header: Header,
    /// The aggregate signatures of validating authorities.
    pub aggregated_signature: AggregateAuthoritySignatureBytes,
    /// what is this??
    #[serde_as(as = "NarwhalBitmap")]
    signed_authorities: roaring::RoaringBitmap,
    /// Timestamp for certificate
    pub created_at: TimestampMs,
}

impl CertificateAPI for CertificateV1 {
    fn header(&self) -> &Header {
        &self.header
    }

    fn aggregated_signature(&self) -> &AggregateAuthoritySignatureBytes {
        &self.aggregated_signature
    }

    fn signed_authorities(&self) -> &roaring::RoaringBitmap {
        &self.signed_authorities
    }

    fn created_at(&self) -> &TimestampMs {
        &self.created_at
    }

    // Used for testing.
    fn update_header(&mut self, header: Header) {
        self.header = header;
    }

    fn header_mut(&mut self) -> &mut Header {
        &mut self.header
    }
}

impl CertificateV1 {
    /// Create a genesis certificate with empty payload.
    pub fn genesis(committee: &Committee) -> Vec<Self> {
        committee
            .authorities()
            .map(|authority| Self {
                header: Header::V1(HeaderV1 {
                    author: authority.id(),
                    epoch: committee.epoch(),
                    digest: Default::default(),
                    ..Default::default()
                }),
                ..Self::default()
            })
            .collect()
    }

    /// Create genesis with header payload
    pub fn genesis_with_payload(
        committee: &Committee,
        batch: Batch,
    ) -> Vec<Self> {
        let timestamp = batch.versioned_metadata().created_at().clone();
        let mut payload = IndexMap::default();
        payload.insert(batch.clone().digest(), (0, timestamp));
        committee
            .authorities()
            .map(|authority| {
                // let header_builder = HeaderV1Builder::default();
                // let header = header_builder
                //     .author(authority.id())
                //     .epoch(committee.epoch())
                //     .with_payload_batch(batch.clone(), 0, timestamp)
                //     .build()
                //     // TODO: remove this unwrap
                //     // starting with removing the Result in .build()
                //     //
                //     // build() also uses .unwrap()
                //     .unwrap();
                Self {
                    // header: Header::V1(header),
                    header: Header::V1(HeaderV1 {
                        author: authority.id(),
                        epoch: committee.epoch(),
                        digest: Default::default(),
                        payload: payload.clone(), // add genesis transactions here?
                        ..HeaderV1::default()
                    }),
                    ..Self::default()
                }
            })
            .collect()
    }

    /// Create a new, unsafe certificate that checks stake.
    pub fn new_unverified(
        committee: &Committee,
        header: Header,
        votes: Vec<(AuthorityIdentifier, AuthoritySignature)>,
    ) -> DagResult<Certificate> {
        Self::new_unsafe(committee, header, votes, true)
    }

    /// Create a new, unsafe certificate that does not check stake.
    pub fn new_unsigned(
        committee: &Committee,
        header: Header,
        votes: Vec<(AuthorityIdentifier, AuthoritySignature)>,
    ) -> DagResult<Certificate> {
        Self::new_unsafe(committee, header, votes, false)
    }

    /// Create a new certificate without verifying authority signatures.
    fn new_unsafe(
        committee: &Committee,
        header: Header,
        votes: Vec<(AuthorityIdentifier, AuthoritySignature)>,
        check_stake: bool,
    ) -> DagResult<Certificate> {
        let mut votes = votes;
        votes.sort_by_key(|(pk, _)| *pk);
        let mut votes: VecDeque<_> = votes.into_iter().collect();

        let mut weight = 0;
        let mut sigs = Vec::new();

        let filtered_votes = committee
            .authorities()
            .enumerate()
            .filter(|(_, authority)| {
                if !votes.is_empty() && authority.id() == votes.front().unwrap().0 {
                    sigs.push(votes.pop_front().unwrap());
                    weight += authority.stake();
                    // If there are repeats, also remove them
                    while !votes.is_empty() && votes.front().unwrap() == sigs.last().unwrap() {
                        votes.pop_front().unwrap();
                    }
                    return true
                }
                false
            })
            .map(|(index, _)| index as u32);

        let signed_authorities= roaring::RoaringBitmap::from_sorted_iter(filtered_votes)
            .map_err(|_| DagError::InvalidBitmap("Failed to convert votes into a bitmap of authority keys. Something is likely very wrong...".to_string()))?;

        // Ensure that all authorities in the set of votes are known
        ensure!(votes.is_empty(), DagError::UnknownAuthority(votes.front().unwrap().0.to_string()));

        // Ensure that the authorities have enough weight
        ensure!(
            !check_stake || weight >= committee.quorum_threshold(),
            DagError::CertificateRequiresQuorum
        );

        let aggregated_signature = if sigs.is_empty() {
            AggregateAuthoritySignature::default()
        } else {
            AggregateAuthoritySignature::aggregate::<AuthoritySignature, Vec<&AuthoritySignature>>(
                sigs.iter().map(|(_, sig)| sig).collect(),
            )
            .map_err(|_| DagError::InvalidSignature)?
        };

        Ok(Certificate::V1(CertificateV1 {
            header,
            aggregated_signature: AggregateAuthoritySignatureBytes::from(&aggregated_signature),
            signed_authorities,
            created_at: now(),
        }))
    }

    /// Return the group of authorities that signed this certificate.
    /// 
    /// This function requires that certificate was verified against given committee
    pub fn signed_authorities(&self, committee: &Committee) -> Vec<AuthorityPublicKey> {
        assert_eq!(committee.epoch(), self.epoch());
        let (_stake, pks) = self.signed_by(committee);
        pks
    }

    /// Return the total stake and group of authorities that formed the committee for this certificate.
    pub fn signed_by(&self, committee: &Committee) -> (Stake, Vec<AuthorityPublicKey>) {
        // Ensure the certificate has a quorum.
        let mut weight = 0;

        let auth_indexes = self.signed_authorities.iter().collect::<Vec<_>>();
        let mut auth_iter = 0;
        let pks = committee
            .authorities()
            .enumerate()
            .filter(|(i, authority)| match auth_indexes.get(auth_iter) {
                Some(index) if *index == *i as u32 => {
                    weight += authority.stake();
                    auth_iter += 1;
                    true
                }
                _ => false,
            })
            .map(|(_, authority)| authority.protocol_key().clone())
            .collect();
        (weight, pks)
    }

    /// Verifies the validity of the certificate.
    /// TODO: Output a different type, similar to Sui's VerifiedCertificate?
    pub fn verify(&self, committee: &Committee, worker_cache: &WorkerCache) -> DagResult<()> {
        // Ensure the header is from the correct epoch.
        ensure!(
            self.epoch() == committee.epoch(),
            DagError::InvalidEpoch { expected: committee.epoch(), received: self.epoch() }
        );

        // Genesis certificates are always valid.
        if self.round() == 0 && Self::genesis(committee).contains(self) {
            return Ok(())
        }

        // Save signature verifications when the header is invalid.
        self.header.validate(committee, worker_cache)?;

        let (weight, pks) = self.signed_by(committee);

        ensure!(weight >= committee.quorum_threshold(), DagError::CertificateRequiresQuorum);

        // Verify the signatures
        let certificate_digest: Digest<{ crypto::DIGEST_LENGTH }> = Digest::from(self.digest());
        AggregateAuthoritySignature::try_from(&self.aggregated_signature)
            .map_err(|_| DagError::InvalidSignature)?
            .verify_secure(&to_intent_message(certificate_digest), &pks[..])
            .map_err(|_| DagError::InvalidSignature)?;

        Ok(())
    }

    /// The certificate's round.
    pub fn round(&self) -> Round {
        self.header.round()
    }

    /// The certificate's epoch.
    pub fn epoch(&self) -> Epoch {
        self.header.epoch()
    }

    /// The author of the certificate.
    pub fn origin(&self) -> AuthorityIdentifier {
        self.header.author()
    }
}

/// Certificate digest.
#[cfg_attr(any(test, feature = "arbitrary"), derive(Arbitrary))]
#[derive(
    Clone, Copy, Serialize, Deserialize, Default, MallocSizeOf, PartialEq, Eq, Hash, PartialOrd, Ord,
)]

pub struct CertificateDigest([u8; crypto::DIGEST_LENGTH]);

impl CertificateDigest {
    /// Create a new instance of CertificateDigest.
    pub fn new(digest: [u8; crypto::DIGEST_LENGTH]) -> Self {
        CertificateDigest(digest)
    }
}

impl AsRef<[u8]> for CertificateDigest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<CertificateDigest> for Digest<{ crypto::DIGEST_LENGTH }> {
    fn from(hd: CertificateDigest) -> Self {
        Digest::new(hd.0)
    }
}

impl From<CertificateDigest> for CertificateDigestProto {
    fn from(hd: CertificateDigest) -> Self {
        CertificateDigestProto { digest: Bytes::from(hd.0.to_vec()) }
    }
}

impl fmt::Debug for CertificateDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", base64::encode(self.0))
    }
}

impl fmt::Display for CertificateDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", base64::encode(self.0).get(0..16).ok_or(fmt::Error)?)
    }
}

impl Hash<{ crypto::DIGEST_LENGTH }> for CertificateV1 {
    type TypedDigest = CertificateDigest;

    fn digest(&self) -> CertificateDigest {
        self.header.clone().into()
    }
}

impl fmt::Debug for Certificate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Certificate::V1(data) => write!(
                f,
                "{}: C{}({}, {}, E{})",
                data.digest(),
                data.round(),
                data.origin(),
                data.header.digest(),
                data.epoch()
            ),
        }
    }
}

impl PartialEq for Certificate {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Certificate::V1(data), Certificate::V1(other_data)) => data.eq(other_data),
        }
    }
}

impl PartialEq for CertificateV1 {
    fn eq(&self, other: &Self) -> bool {
        let mut ret = self.header().digest() == other.header().digest();
        ret &= self.round() == other.round();
        ret &= self.epoch() == other.epoch();
        ret &= self.origin() == other.origin();
        ret
    }
}

impl Affiliated for Certificate {
    fn parents(&self) -> Vec<<Self as Hash<{ crypto::DIGEST_LENGTH }>>::TypedDigest> {
        match self {
            Certificate::V1(data) => data.header().parents().iter().cloned().collect(),
        }
    }

    // This makes the genesis certificate and empty blocks compressible,
    // so that they will never be reported by a DAG walk
    // (`read_causal`, `node_read_causal`).
    fn compressible(&self) -> bool {
        match self {
            Certificate::V1(data) => data.header().payload().is_empty(),
        }
    }
}
