// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

//! The builder responsible for creating all aspects of the committee fixture.
use super::{AuthorityFixture, CommitteeFixture};
use narwhal_typed_store::traits::Database;
use rand::{
    rngs::{OsRng, StdRng},
    SeedableRng,
};
use reth::primitives::Address;
use std::{
    collections::{BTreeMap, VecDeque},
    marker::PhantomData,
    num::NonZeroUsize,
};
use tn_config::KeyConfig;
use tn_types::{
    traits::KeyPair, utils::get_available_tcp_port, Authority, AuthorityIdentifier, BlsPublicKey,
    Committee, Epoch, Multiaddr, Stake, WorkerCache, WorkerIndex,
};

pub struct Builder<DB, F, R = OsRng> {
    rng: R,
    committee_size: NonZeroUsize,
    number_of_workers: NonZeroUsize,
    randomize_ports: bool,
    epoch: Epoch,
    stake: VecDeque<Stake>,
    new_db: F,
    _phantom_data: PhantomData<DB>,
}

impl<DB, F> Builder<DB, F>
where
    DB: Database,
    F: Fn() -> DB,
{
    pub fn new(new_db: F) -> Self {
        Self {
            epoch: Epoch::default(),
            rng: OsRng,
            committee_size: NonZeroUsize::new(4).unwrap(),
            number_of_workers: NonZeroUsize::new(1).unwrap(),
            randomize_ports: false,
            stake: VecDeque::new(),
            new_db,
            _phantom_data: PhantomData::<DB>,
        }
    }
}

impl<R, DB, F> Builder<DB, F, R>
where
    DB: Database,
    F: Fn() -> DB,
{
    pub fn committee_size(mut self, committee_size: NonZeroUsize) -> Self {
        self.committee_size = committee_size;
        self
    }

    pub fn randomize_ports(mut self, randomize_ports: bool) -> Self {
        self.randomize_ports = randomize_ports;
        self
    }

    pub fn epoch(mut self, epoch: Epoch) -> Self {
        self.epoch = epoch;
        self
    }

    pub fn stake_distribution(mut self, stake: VecDeque<Stake>) -> Self {
        self.stake = stake;
        self
    }

    pub fn rng<N: rand::RngCore + rand::CryptoRng>(self, rng: N) -> Builder<DB, F, N> {
        Builder {
            rng,
            epoch: self.epoch,
            committee_size: self.committee_size,
            number_of_workers: self.number_of_workers,
            randomize_ports: self.randomize_ports,
            stake: self.stake,
            new_db: self.new_db,
            _phantom_data: PhantomData::<DB>,
        }
    }
}

impl<R, DB, F> Builder<DB, F, R>
where
    R: rand::RngCore + rand::CryptoRng,
    DB: Database,
    F: Fn() -> DB,
{
    pub fn build(mut self) -> CommitteeFixture<DB> {
        if !self.stake.is_empty() {
            assert_eq!(self.stake.len(), self.committee_size.get(), "Stake vector has been provided but is different length the committee - it should be the same");
        }
        let committee_size = self.committee_size.get();

        let mut rng = StdRng::from_rng(&mut self.rng).unwrap();
        let mut committee_info = Vec::with_capacity(committee_size);
        let mut authorities = BTreeMap::new();
        // Pass 1 to make the committee struct we need later.
        for i in 0..committee_size {
            let key_config = KeyConfig::with_random(&mut rng);
            let host = "127.0.0.1";
            let port = if self.randomize_ports {
                get_available_tcp_port(host).unwrap_or_default()
            } else {
                0
            };
            let primary_network_address: Multiaddr =
                format!("/ip4/{host}/udp/{port}").parse().unwrap();
            let authority = Authority::new_for_test(
                (i as u16).into(),
                key_config.bls_keypair().public().clone(),
                self.stake.get(i).unwrap_or(&1).clone(),
                primary_network_address,
                Address::random_with(&mut rng),
                key_config.network_keypair().public().clone(),
                format!("authority{i}"),
            );
            authorities.insert(authority.protocol_key().clone(), authority.clone());
            committee_info.push((key_config, authority));
        }
        let committee = Committee::new_for_test(authorities, 0);
        let mut authorities: Vec<AuthorityFixture<DB>> = committee_info
            .into_iter()
            .map(|(key_config, authority)| {
                AuthorityFixture::generate(
                    self.number_of_workers,
                    |host| {
                        if self.randomize_ports {
                            get_available_tcp_port(host).unwrap_or_default()
                        } else {
                            0
                        }
                    },
                    authority,
                    key_config,
                    committee.clone(),
                    (self.new_db)(),
                )
            })
            .collect();

        // now order the AuthorityFixtures by the authority BlsPublicKey so when we iterate either
        // via the committee.authorities() or via the fixture.authorities() we'll get the
        // same order.
        authorities.sort_by_key(|a1| a1.public_key());

        let worker_cache = WorkerCache {
            epoch: self.epoch,
            workers: authorities
                .iter()
                .map(|a| {
                    let mut worker_index = BTreeMap::new();
                    worker_index.insert(a.worker().id, a.worker().info().clone());
                    (a.public_key(), WorkerIndex(worker_index.clone()))
                })
                .collect(),
        };
        for a in authorities.iter_mut() {
            a.set_worker_cache(worker_cache.clone());
        }

        /*
        // create the committee in order to assign the ids to the authorities
        let mut committee_builder = CommitteeBuilder::new(self.epoch);
        for a in authorities.iter() {
            committee_builder.add_authority(
                a.public_key().clone(),
                self.stake.pop_front().unwrap_or(1),
                a.network_address.clone(),
                a.execution_address,
                a.network_public_key(),
                a.network_address.to_string(),
            );
        }
        let committee = committee_builder.build();
        */

        // Update the Fixtures with the id assigned from the committee
        /*for authority in authorities.iter_mut() {
            let a = committee.authority_by_key(&authority.public_key()).unwrap();
            authority.authority = OnceCell::with_value(a.clone());
            authority.stake = a.stake();
        }*/

        // Now update the stake to follow the order of the authorities so we produce expected
        // results
        /*let authorities: Vec<AuthorityFixture<DB>> = authorities
        .into_iter()
        .map(|mut authority| {
            authority.stake = self.stake.pop_front().unwrap_or(1);
            authority
        })
        .collect();*/

        CommitteeFixture { authorities, committee }
    }
}
