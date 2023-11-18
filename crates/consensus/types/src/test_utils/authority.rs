// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    Authority, AuthorityIdentifier, Certificate, Committee, Header, HeaderV1Builder, KeyPair,
    Multiaddr, NetworkKeyPair, NetworkPublicKey, PublicKey, Round, Stake, Vote, WorkerId,
    WorkerIndex,
};
use fastcrypto::{
    hash::Hash,
    traits::{AllowedRng, KeyPair as _},
};
use once_cell::sync::OnceCell;

use std::{collections::BTreeMap, num::NonZeroUsize};

use super::WorkerFixture;

pub struct AuthorityFixture {
    pub(crate) authority: OnceCell<Authority>,
    pub(crate) keypair: KeyPair,
    pub(crate) network_keypair: NetworkKeyPair,
    pub(crate) stake: Stake,
    pub(crate) address: Multiaddr,
    pub(crate) workers: BTreeMap<WorkerId, WorkerFixture>,
}

impl AuthorityFixture {
    pub fn id(&self) -> AuthorityIdentifier {
        self.authority.get().unwrap().id()
    }

    pub fn authority(&self) -> &Authority {
        self.authority.get().unwrap()
    }

    pub fn keypair(&self) -> &KeyPair {
        &self.keypair
    }

    pub fn network_keypair(&self) -> NetworkKeyPair {
        self.network_keypair.copy()
    }

    pub fn new_network(&self, router: anemo::Router) -> anemo::Network {
        anemo::Network::bind(self.address.to_anemo_address().unwrap())
            .server_name("narwhal")
            .private_key(self.network_keypair().private().0.to_bytes())
            .start(router)
            .unwrap()
    }

    pub fn address(&self) -> &Multiaddr {
        &self.address
    }

    pub fn worker(&self, id: WorkerId) -> &WorkerFixture {
        self.workers.get(&id).unwrap()
    }

    pub fn worker_keypairs(&self) -> Vec<NetworkKeyPair> {
        self.workers.values().map(|worker| worker.keypair.copy()).collect()
    }

    pub fn public_key(&self) -> PublicKey {
        self.keypair.public().clone()
    }

    pub fn network_public_key(&self) -> NetworkPublicKey {
        self.network_keypair.public().clone()
    }

    pub fn worker_index(&self) -> WorkerIndex {
        WorkerIndex(self.workers.iter().map(|(id, w)| (*id, w.info.clone())).collect())
    }

    pub fn header(&self, committee: &Committee) -> Header {
        let header = self.header_builder(committee).payload(Default::default()).build().unwrap();
        Header::V1(header)
    }

    pub fn header_with_round(&self, committee: &Committee, round: Round) -> Header {
        let header = self
            .header_builder(committee)
            .payload(Default::default())
            .round(round)
            .build()
            .unwrap();
        Header::V1(header)
    }

    pub fn header_builder(&self, committee: &Committee) -> HeaderV1Builder {
        HeaderV1Builder::default()
            .author(self.id())
            .round(1)
            .epoch(committee.epoch())
            .parents(Certificate::genesis(committee).iter().map(|x| x.digest()).collect())
    }

    pub fn vote(&self, header: &Header) -> Vote {
        Vote::new_with_signer(header, &self.id(), &self.keypair)
    }

    pub(crate) fn generate<R, P>(
        mut rng: R,
        number_of_workers: NonZeroUsize,
        mut get_port: P,
    ) -> Self
    where
        R: AllowedRng,
        P: FnMut(&str) -> u16,
    {
        let keypair = KeyPair::generate(&mut rng);
        let network_keypair = NetworkKeyPair::generate(&mut rng);
        let host = "127.0.0.1";
        let address: Multiaddr = format!("/ip4/{}/udp/{}", host, get_port(host)).parse().unwrap();

        let workers = (0..number_of_workers.get())
            .map(|idx| {
                let worker = WorkerFixture::generate(&mut rng, idx as u32, &mut get_port);

                (idx as u32, worker)
            })
            .collect();

        Self { authority: OnceCell::new(), keypair, network_keypair, stake: 1, address, workers }
    }
}
