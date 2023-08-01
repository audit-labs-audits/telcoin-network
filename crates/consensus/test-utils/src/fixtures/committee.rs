// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Committee fixture and builder.
use crate::{AuthorityFixture, fixture_batch_with_transactions};
use fastcrypto::traits::KeyPair as _;
use once_cell::sync::OnceCell;
use rand::{
    rngs::{OsRng, StdRng}, SeedableRng,
};
use std::{
    collections::{BTreeSet, VecDeque},
    num::NonZeroUsize,
};
use tn_types::consensus::{
    utils::get_available_port, Committee, CommitteeBuilder,
    Epoch, Stake, WorkerCache,
    Certificate, CertificateDigest,
    Header, HeaderAPI,
    Round,
    Vote, VoteAPI,
};

/// Builder for [CommitteeFixture].
/// 
/// The builder generates all the necessary keys for a specified 
/// number of authorities to participate as a committee in consensus.
pub struct Builder<R = OsRng> {
    rng: R,
    committee_size: NonZeroUsize,
    number_of_workers: NonZeroUsize,
    randomize_ports: bool,
    epoch: Epoch,
    stake: VecDeque<Stake>,
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

impl Builder {
    /// New instance of builder with default values.
    /// - epoch: 0
    /// - rng: `rngs::OsRng`
    /// - committee_size: 4
    /// - number_of_workers: 4
    /// - randomize_ports: false
    /// - stake: empty VecDeque (results in `1` during Self::build())
    pub fn new() -> Self {
        Self {
            epoch: Epoch::default(),
            rng: OsRng,
            committee_size: NonZeroUsize::new(4).unwrap(),
            number_of_workers: NonZeroUsize::new(4).unwrap(),
            randomize_ports: false,
            stake: VecDeque::new(),
        }
    }
}

impl<R> Builder<R> {
    /// Specify the number of authorities in the committee.
    pub fn committee_size(mut self, committee_size: NonZeroUsize) -> Self {
        self.committee_size = committee_size;
        self
    }

    /// Specify the number of workers per authority in the committee.
    pub fn number_of_workers(mut self, number_of_workers: NonZeroUsize) -> Self {
        self.number_of_workers = number_of_workers;
        self
    }

    /// Randomize the network ports. If set to `true`, the system will
    /// find available ports for local network connections.
    pub fn randomize_ports(mut self, randomize_ports: bool) -> Self {
        self.randomize_ports = randomize_ports;
        self
    }

    /// Specify an epoch for the committee.
    pub fn epoch(mut self, epoch: Epoch) -> Self {
        self.epoch = epoch;
        self
    }

    /// Stake for each authority.
    /// `pop_front()` is called in a loop when creating authoritites for the
    /// committee.
    pub fn stake_distribution(mut self, stake: VecDeque<Stake>) -> Self {
        self.stake = stake;
        self
    }

    /// Assign a new rng for randomness.
    /// 
    /// `Builder::new()` just assigns `OsRng` to this value.
    ///
    /// Some tests use `let rng = StdRng::from_seed([0; 32]);`
    pub fn rng<N: rand::RngCore + rand::CryptoRng>(self, rng: N) -> Builder<N> {
        Builder {
            rng,
            epoch: self.epoch,
            committee_size: self.committee_size,
            number_of_workers: self.number_of_workers,
            randomize_ports: self.randomize_ports,
            stake: self.stake,
        }
    }
}

impl<R: rand::RngCore + rand::CryptoRng> Builder<R> {
    /// Build the committee by generating all the necessary keypairs and workers for
    /// each authority in the committee.
    /// 
    /// Cluster uses this output to create [AuthorityDetails], which is then used to
    /// start instances of `Primary`.
    pub fn build(mut self) -> CommitteeFixture {
        if !self.stake.is_empty() {
            assert_eq!(self.stake.len(), self.committee_size.get(), "Stake vector has been provided but is different length the committe - it should be the same");
        }

        let mut authorities: Vec<AuthorityFixture> = (0..self.committee_size.get())
            .map(|_| {
                AuthorityFixture::generate(
                    StdRng::from_rng(&mut self.rng).unwrap(),
                    self.number_of_workers,
                    |host| {
                        if self.randomize_ports {
                            get_available_port(host)
                        } else {
                            0
                        }
                    },
                )
            })
            .collect();

        // now order the AuthorityFixtures by the authority PublicKey so when we iterate either via
        // the committee.authorities() or via the fixture.authorities() we'll get the same
        // order.
        authorities.sort_by_key(|a1| a1.public_key());

        // create the committee in order to assign the ids to the authorities
        let mut committee_builder = CommitteeBuilder::new(self.epoch);
        for a in authorities.iter() {
            committee_builder = committee_builder.add_authority(
                a.public_key().clone(),
                self.stake.pop_front().unwrap_or(1),
                a.address.clone(),
                a.network_public_key(),
            );
        }
        let committee = committee_builder.build();

        // Update the Fixtures with the id assigned from the committee
        for authority in authorities.iter_mut() {
            let a = committee.authority_by_key(authority.keypair.public()).unwrap();
            authority.authority = OnceCell::with_value(a.clone());
            authority.stake = a.stake();
        }

        // Now update the stake to follow the order of the authorities so we produce expected
        // results
        let authorities: Vec<AuthorityFixture> = authorities
            .into_iter()
            .map(|mut authority| {
                authority.stake = self.stake.pop_front().unwrap_or(1);
                authority
            })
            .collect();

        CommitteeFixture { authorities, committee, epoch: self.epoch }
    }
}

/// Fixture for creating a Committee.
/// 
/// Used for all testing that requires a committee.
/// 
/// Called internally by `Cluster::new()` to launch instances of 
/// primary and workers.
pub struct CommitteeFixture {
    /// Information for each authority in the committee.
    /// 
    /// Authorities do not spawn workers or primary instances. They
    /// just have information required to do so. The vector is ordered
    /// for reliably testing during the build process.
    authorities: Vec<AuthorityFixture>,
    /// The committee type needed for consensus.
    committee: Committee,
    /// The epoch of this committee.
    epoch: Epoch,
}

impl CommitteeFixture {
    /// Iterator of authorities in the [CommitteeFixture]
    pub fn authorities(&self) -> impl Iterator<Item = &AuthorityFixture> {
        self.authorities.iter()
    }

    /// Return a new [Builder].
    pub fn builder() -> Builder {
        Builder::new()
    }

    /// Return the fixture's [Committee]
    pub fn committee(&self) -> Committee {
        self.committee.clone()
    }

    /// Return the fixture's [WorkerCache]
    pub fn worker_cache(&self) -> WorkerCache {
        WorkerCache {
            epoch: self.epoch,
            workers: self.authorities.iter().map(|a| (a.public_key(), a.worker_index())).collect(),
        }
    }

    /// Return a Header that is signed by last authority in the committee.
    pub fn header(&self) -> Header {
        self.authorities.last().unwrap().header(&self.committee())
    }

    pub fn headers(&self) -> Vec<Header> {
        let committee = self.committee();

        self.authorities.iter().map(|a| a.header_with_round(&committee, 1)).collect()
    }

    pub fn headers_next_round(&self) -> Vec<Header> {
        let committee = self.committee();
        self.authorities.iter().map(|a| a.header_with_round(&committee, 2)).collect()
    }

    pub fn headers_round(
        &self,
        prior_round: Round,
        parents: &BTreeSet<CertificateDigest>,
    ) -> (Round, Vec<Header>) {
        let round = prior_round + 1;
        let next_headers = self
            .authorities
            .iter()
            .map(|a| {
                let builder = tn_types::consensus::HeaderV1Builder::default();
                let header = builder
                    .author(a.id())
                    .round(round)
                    .epoch(0)
                    .parents(parents.clone())
                    .with_payload_batch(fixture_batch_with_transactions(10), 0, 0)
                    .build()
                    .unwrap();
                Header::V1(header)
            })
            .collect();

        (round, next_headers)
    }

    /// All authorities in the committee vote for the header
    /// except the authrotity that produced the header.
    pub fn votes(&self, header: &Header) -> Vec<Vote> {
        self.authorities()
            .flat_map(|a| {
                // we should not re-sign using the key of the authority
                // that produced the header
                if a.id() == header.author() {
                    None
                } else {
                    Some(a.vote(header))
                }
            })
            .collect()
    }

    /// Return a [Certificate]
    pub fn certificate(&self, header: &Header) -> Certificate {
        let committee = self.committee();
        let votes: Vec<_> =
            self.votes(header).into_iter().map(|x| (x.author(), x.signature().clone())).collect();
        Certificate::new_unverified(&committee, header.clone(), votes).unwrap()
    }
}
