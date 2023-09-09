// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Authority fixture for representing validators.
//! 
//! The fixutre only contains information needed to quickly 
//! simulate output from other nodes without actually using the primary's methods.
//! For instance, quickly generating enough signatures for quorum to advance the round.
//! see crates/consensus/consensus/src/tests/bullshark_tests.rs
use crate::WorkerFixture;
use tn_types::consensus::Multiaddr;
use fastcrypto::{
    hash::Hash as _,
    traits::{AllowedRng, KeyPair as _},
};
use once_cell::sync::OnceCell;
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
};
use tn_types::consensus::{
    crypto::{
        AuthorityKeyPair, NetworkKeyPair, NetworkPublicKey,
        AuthorityPublicKey,
    },
    Authority, AuthorityIdentifier, Committee,
    Stake, WorkerId, WorkerIndex, Vote, Round,
    Header, Certificate,
};

/// Authority fixture for running tests.
/// 
/// Contains [Authority] and all associated 
/// keypairs, workers, stake, and network address.
/// There is no instance of primary running.
pub struct AuthorityFixture {
    /// The [Authority] struct.
    pub(super) authority: OnceCell<Authority>,
    /// This authority's [AuthorityKeyPair]
    pub(super) keypair: AuthorityKeyPair,
    /// This authority's [NetworkKeyPair]
    pub(super) network_keypair: NetworkKeyPair,
    /// This authority's engine's public key 
    pub(super) engine_network_keypair: NetworkKeyPair,
    /// This authority's [Stake]
    pub(super) stake: Stake,
    /// This authority's [Multiaddr] for the consensus network.
    pub(super) address: Multiaddr,
    /// This authority's worker info sorted by each worker's id.
    pub(super) workers: BTreeMap<WorkerId, WorkerFixture>,
}

impl AuthorityFixture {
    /// Get the id for this authority.
    pub fn id(&self) -> AuthorityIdentifier {
        self.authority.get().unwrap().id()
    }

    /// Get a reference to the [Authority] struct for this fixture.
    pub fn authority(&self) -> &Authority {
        self.authority.get().unwrap()
    }

    /// Get a reference to this authority's [AuthorityKeyPair].
    pub fn keypair(&self) -> &AuthorityKeyPair {
        &self.keypair
    }

    /// Get an owned copy of this authority's [NetworkKeyPair].
    pub fn network_keypair(&self) -> NetworkKeyPair {
        self.network_keypair.copy()
    }

    /// Get an owned copy of this authority's engine's [NetworkKeyPair].
    pub fn engine_network_keypair(&self) -> NetworkKeyPair {
        self.engine_network_keypair.copy()
    }


    /// Create a new [anemo::Network] for this authority using it's network keypair.
    pub fn new_network(&self, router: anemo::Router) -> anemo::Network {
        anemo::Network::bind(self.address.to_anemo_address().unwrap())
            .server_name("lattice")
            .private_key(self.network_keypair().private().0.to_bytes())
            .start(router)
            .unwrap()
    }

    /// Get a reference to this authority's [Multiaddr]
    pub fn address(&self) -> &Multiaddr {
        &self.address
    }

    /// Get the [WorkerFixture] for worker with the passed `WorkerId`.
    pub fn worker(&self, id: WorkerId) -> &WorkerFixture {
        self.workers.get(&id).unwrap()
    }

    /// Get all worker's [NetworkKeyPair].
    pub fn worker_keypairs(&self) -> Vec<NetworkKeyPair> {
        self.workers.values().map(|worker| worker.keypair.copy()).collect()
    }

    /// Get an owned copy of this authority's [AuthorityPublicKey]
    pub fn public_key(&self) -> AuthorityPublicKey {
        self.keypair.public().clone()
    }

    /// Get an owned copy of this authority's [NetworkPublicKey]
    pub fn network_public_key(&self) -> NetworkPublicKey {
        self.network_keypair.public().clone()
    }

    /// Get [WorkerInfo] for all workers of this authority.
    pub fn worker_index(&self) -> WorkerIndex {
        WorkerIndex(self.workers.iter().map(|(id, w)| (*id, w.info.clone())).collect())
    }

    /// TODO: update docs
    ///
    /// Build a header for this authority based on the committee.
    pub fn header(&self, committee: &Committee) -> Header {
        let header = self.header_builder(committee).payload(Default::default()).build().unwrap();
        Header::V1(header)
    }

    /// TODO: update docs
    /// 
    /// Build a header based on committee and round.
    pub fn header_with_round(&self, committee: &Committee, round: Round) -> Header {
        let header = self
            .header_builder(committee)
            .payload(Default::default())
            .round(round)
            .build()
            .unwrap();
        Header::V1(header)
    }

    /// Build a header authored by this authority.
    pub fn header_builder(&self, committee: &Committee) -> tn_types::consensus::HeaderV1Builder {
        tn_types::consensus::HeaderV1Builder::default()
            .author(self.id())
            .round(1)
            .epoch(committee.epoch())
            .parents(Certificate::genesis(committee).iter().map(|x| x.digest()).collect())
    }

    /// Vote from this authority on a header.
    pub fn vote(&self, header: &Header) -> Vote {
        Vote::new_with_signer(header, &self.id(), &self.keypair)
    }

    /// Generate all the keys, specified number of workers, and randomly assigned (available) port needed for a new instance of [Self].
    /// 
    /// Stake for this authority is automatically assigned a value of `1`.
    pub(super) fn generate<R, P>(mut rng: R, number_of_workers: NonZeroUsize, mut get_port: P) -> Self
    where
        R: AllowedRng,
        P: FnMut(&str) -> u16,
    {
        let keypair = AuthorityKeyPair::generate(&mut rng);
        let network_keypair = NetworkKeyPair::generate(&mut rng);
        let engine_network_keypair = NetworkKeyPair::generate(&mut rng);
        let host = "127.0.0.1";
        let address: Multiaddr = format!("/ip4/{}/udp/{}", host, get_port(host)).parse().unwrap();

        let workers = (0..number_of_workers.get())
            .map(|idx| {
                let worker = WorkerFixture::generate(&mut rng, idx as u32, &mut get_port);

                (idx as u32, worker)
            })
            .collect();

        Self { authority: OnceCell::new(), keypair, network_keypair, engine_network_keypair, stake: 1, address, workers }
    }
}
