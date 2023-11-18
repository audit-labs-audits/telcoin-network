// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    Certificate, CertificateDigest, Committee, Epoch, Header, HeaderAPI, HeaderV1Builder, Round,
    Vote, VoteAPI, WorkerCache,
};

use std::collections::BTreeSet;

use super::{fixture_batch_with_transactions, AuthorityFixture, Builder};

pub struct CommitteeFixture {
    pub(crate) authorities: Vec<AuthorityFixture>,
    pub(crate) committee: Committee,
    pub(crate) epoch: Epoch,
}

impl CommitteeFixture {
    pub fn authorities(&self) -> impl Iterator<Item = &AuthorityFixture> {
        self.authorities.iter()
    }

    pub fn builder() -> Builder {
        Builder::new()
    }

    pub fn committee(&self) -> Committee {
        self.committee.clone()
    }

    pub fn worker_cache(&self) -> WorkerCache {
        WorkerCache {
            epoch: self.epoch,
            workers: self.authorities.iter().map(|a| (a.public_key(), a.worker_index())).collect(),
        }
    }

    // pub fn header(&self, author: PublicKey) -> Header {
    // Currently sign with the last authority
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
                let builder = HeaderV1Builder::default();
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

    pub fn certificate(&self, header: &Header) -> Certificate {
        let committee = self.committee();
        let votes: Vec<_> =
            self.votes(header).into_iter().map(|x| (x.author(), x.signature().clone())).collect();
        Certificate::new_unverified(&committee, header.clone(), votes).unwrap()
    }
}
