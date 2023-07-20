use crate::consensus::{
    error::{DagError, DagResult},
    crypto, Round, BatchDigest, TimestampMs, CertificateDigest, now, Batch, VoteDigest,
};
use crate::consensus::config::{AuthorityIdentifier, Committee, Epoch, WorkerCache, WorkerId};
use derive_builder::Builder;
use enum_dispatch::enum_dispatch;
use fastcrypto::hash::{Digest, Hash, HashFunction};
use indexmap::IndexMap;
use consensus_util_mem::MallocSizeOf;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fmt,
};
#[cfg(any(test, feature = "arbitrary"))]
use proptest_derive::Arbitrary;

#[derive(Clone, Deserialize, MallocSizeOf, Serialize)]
#[enum_dispatch(HeaderAPI)]
pub enum Header {
    V1(HeaderV1),
}

// TODO: Revisit if we should not impl Default for Header and just use
// versioned header in Certificate
impl Default for Header {
    fn default() -> Self {
        Self::V1(HeaderV1::default())
    }
}

impl Header {
    // TODO: Add version number and match on that.
    pub fn new(
        author: AuthorityIdentifier,
        round: Round,
        epoch: Epoch,
        payload: IndexMap<BatchDigest, (WorkerId, TimestampMs)>,
        parents: BTreeSet<CertificateDigest>,
    ) -> Self {
        Header::V1(HeaderV1::new(author, round, epoch, payload, parents))
    }

    pub fn digest(&self) -> HeaderDigest {
        match self {
            Header::V1(data) => data.digest(),
        }
    }

    pub fn validate(&self, committee: &Committee, worker_cache: &WorkerCache) -> DagResult<()> {
        match self {
            Header::V1(data) => data.validate(committee, worker_cache),
        }
    }
}

impl Hash<{ crypto::DIGEST_LENGTH }> for Header {
    type TypedDigest = HeaderDigest;

    fn digest(&self) -> HeaderDigest {
        match self {
            Header::V1(data) => data.digest(),
        }
    }
}

impl From<Header> for CertificateDigest {
    fn from(value: Header) -> Self {
        Self::new(value.digest().0)
    }
}

#[enum_dispatch]
pub trait HeaderAPI {
    fn author(&self) -> AuthorityIdentifier;
    fn round(&self) -> Round;
    fn epoch(&self) -> Epoch;
    fn created_at(&self) -> &TimestampMs;
    fn payload(&self) -> &IndexMap<BatchDigest, (WorkerId, TimestampMs)>;
    fn parents(&self) -> &BTreeSet<CertificateDigest>;

    // Used for testing.
    fn update_payload(&mut self, new_payload: IndexMap<BatchDigest, (WorkerId, TimestampMs)>);
    fn update_round(&mut self, new_round: Round);
    fn clear_parents(&mut self);
}

#[derive(Builder, Clone, Default, Deserialize, MallocSizeOf, Serialize)]
#[builder(pattern = "owned", build_fn(skip))]
pub struct HeaderV1 {
    // Primary that created the header. Must be the same primary that broadcasted the header.
    // Validation is at: https://github.com/MystenLabs/sui/blob/f0b80d9eeef44edd9fbe606cee16717622b68651/narwhal/primary/src/primary.rs#L713-L719
    pub author: AuthorityIdentifier,
    pub round: Round,
    pub epoch: Epoch,
    pub created_at: TimestampMs,
    #[serde(with = "indexmap::serde_seq")]
    pub payload: IndexMap<BatchDigest, (WorkerId, TimestampMs)>,
    pub parents: BTreeSet<CertificateDigest>,
    #[serde(skip)]
    pub digest: OnceCell<HeaderDigest>,
}

impl HeaderAPI for HeaderV1 {
    fn author(&self) -> AuthorityIdentifier {
        self.author
    }
    fn round(&self) -> Round {
        self.round
    }
    fn epoch(&self) -> Epoch {
        self.epoch
    }
    fn created_at(&self) -> &TimestampMs {
        &self.created_at
    }
    fn payload(&self) -> &IndexMap<BatchDigest, (WorkerId, TimestampMs)> {
        &self.payload
    }
    fn parents(&self) -> &BTreeSet<CertificateDigest> {
        &self.parents
    }

    // Used for testing.
    fn update_payload(&mut self, new_payload: IndexMap<BatchDigest, (WorkerId, TimestampMs)>) {
        self.payload = new_payload;
    }
    fn update_round(&mut self, new_round: Round) {
        self.round = new_round;
    }
    fn clear_parents(&mut self) {
        self.parents.clear();
    }
}

impl HeaderV1Builder {
    pub fn build(self) -> Result<HeaderV1, fastcrypto::error::FastCryptoError> {
        let h = HeaderV1 {
            author: self.author.unwrap(),
            round: self.round.unwrap(),
            epoch: self.epoch.unwrap(),
            created_at: self.created_at.unwrap_or(0),
            payload: self.payload.unwrap(),
            parents: self.parents.unwrap(),
            digest: OnceCell::default(),
        };
        h.digest.set(Hash::digest(&h)).unwrap();

        Ok(h)
    }

    // helper method to set directly values to the payload
    pub fn with_payload_batch(
        mut self,
        batch: Batch,
        worker_id: WorkerId,
        created_at: TimestampMs,
    ) -> Self {
        if self.payload.is_none() {
            self.payload = Some(Default::default());
        }
        let payload = self.payload.as_mut().unwrap();

        payload.insert(batch.digest(), (worker_id, created_at));

        self
    }
}

impl HeaderV1 {
    pub fn new(
        author: AuthorityIdentifier,
        round: Round,
        epoch: Epoch,
        payload: IndexMap<BatchDigest, (WorkerId, TimestampMs)>,
        parents: BTreeSet<CertificateDigest>,
    ) -> Self {
        let header = Self {
            author,
            round,
            epoch,
            created_at: now(),
            payload,
            parents,
            digest: OnceCell::default(),
        };
        let digest = Hash::digest(&header);
        header.digest.set(digest).unwrap();
        header
    }

    pub fn digest(&self) -> HeaderDigest {
        *self.digest.get_or_init(|| Hash::digest(self))
    }

    pub fn validate(&self, committee: &Committee, worker_cache: &WorkerCache) -> DagResult<()> {
        // Ensure the header is from the correct epoch.
        ensure!(
            self.epoch == committee.epoch(),
            DagError::InvalidEpoch {
                expected: committee.epoch(),
                received: self.epoch
            }
        );

        // Ensure the header digest is well formed.
        ensure!(
            Hash::digest(self) == self.digest(),
            DagError::InvalidHeaderDigest
        );

        // Ensure the authority has voting rights.
        let voting_rights = committee.stake_by_id(self.author);
        ensure!(
            voting_rights > 0,
            DagError::UnknownAuthority(self.author.to_string())
        );

        // Ensure all worker ids are correct.
        for (worker_id, _) in self.payload.values() {
            worker_cache
                .worker(
                    committee.authority(&self.author).unwrap().protocol_key(),
                    worker_id,
                )
                .map_err(|_| DagError::HeaderHasBadWorkerIds(self.digest()))?;
        }

        Ok(())
    }
}

#[cfg_attr(any(test, feature = "arbitrary"), derive(Arbitrary))]
#[derive(
    Clone,
    Copy,
    Serialize,
    Deserialize,
    Default,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    MallocSizeOf,
)]
pub struct HeaderDigest([u8; crypto::DIGEST_LENGTH]);

impl HeaderDigest {
    pub fn new(digest: [u8; crypto::DIGEST_LENGTH]) -> Self {
        HeaderDigest(digest)
    }
}

impl From<HeaderDigest> for Digest<{ crypto::DIGEST_LENGTH }> {
    fn from(hd: HeaderDigest) -> Self {
        Digest::new(hd.0)
    }
}

impl From<HeaderDigest> for VoteDigest {
    fn from(value: HeaderDigest) -> Self {
        Self::new(value.0)
    }
}

impl fmt::Debug for HeaderDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", base64::encode(self.0))
    }
}

impl fmt::Display for HeaderDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}",
            base64::encode(self.0).get(0..16).ok_or(fmt::Error)?
        )
    }
}

impl Hash<{ crypto::DIGEST_LENGTH }> for HeaderV1 {
    type TypedDigest = HeaderDigest;

    fn digest(&self) -> HeaderDigest {
        let mut hasher = crypto::DefaultHashFunction::new();
        hasher.update(bcs::to_bytes(&self).expect("Serialization should not fail"));
        HeaderDigest(hasher.finalize().into())
    }
}

impl fmt::Debug for Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::V1(data) => {
                write!(
                    f,
                    "{}: B{}({}, E{}, {}B)",
                    data.digest.get().cloned().unwrap_or_default(),
                    data.round,
                    data.author,
                    data.epoch,
                    data.payload
                        .keys()
                        .map(|x| Digest::from(*x).size())
                        .sum::<usize>(),
                )
            }
        }
    }
}

impl fmt::Display for Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::V1(data) => {
                write!(f, "B{}({})", data.round, data.author)
            }
        }
    }
}

impl PartialEq for Header {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Self::V1(data) => data.digest() == other.digest(),
        }
    }
}

