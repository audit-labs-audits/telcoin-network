// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Worker fixture.
use fastcrypto::traits::KeyPair as _;
use rand::{rngs::StdRng, SeedableRng};
use tn_types::consensus::{crypto::NetworkKeyPair, WorkerId, WorkerInfo};

pub struct WorkerFixture {
    pub(super) keypair: NetworkKeyPair,
    #[allow(dead_code)]
    id: WorkerId,
    pub(super) info: WorkerInfo,
}

impl WorkerFixture {
    pub fn keypair(&self) -> NetworkKeyPair {
        self.keypair.copy()
    }

    pub fn info(&self) -> &WorkerInfo {
        &self.info
    }

    pub fn new_network(&self, router: anemo::Router) -> anemo::Network {
        anemo::Network::bind(self.info().worker_address.to_anemo_address().unwrap())
            .server_name("lattice")
            .private_key(self.keypair().private().0.to_bytes())
            .start(router)
            .unwrap()
    }

    pub(super) fn generate<R, P>(rng: R, id: WorkerId, mut get_port: P) -> Self
    where
        R: rand::RngCore + rand::CryptoRng,
        P: FnMut(&str) -> u16,
    {
        let keypair = NetworkKeyPair::generate(&mut StdRng::from_rng(rng).unwrap());
        let worker_name = keypair.public().clone();
        let host = "127.0.0.1";
        let worker_address = format!("/ip4/{}/udp/{}", host, get_port(host)).parse().unwrap();
        let transactions = format!("/ip4/{}/tcp/{}/http", host, get_port(host)).parse().unwrap();

        Self { keypair, id, info: WorkerInfo { name: worker_name, worker_address, transactions } }
    }
}
