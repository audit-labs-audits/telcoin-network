// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::metrics::PrimaryMetrics;
use fastcrypto::hash::{Digest, Hash};
use std::{collections::HashSet, sync::Arc};
use tn_types::{
    consensus::{
        AuthorityIdentifier, Committee, Stake,
        crypto::{
            self, to_intent_message, AggregateAuthoritySignature, NarwhalAuthorityAggregateSignature,
            NarwhalAuthoritySignature, AuthoritySignature,
        },
        error::{DagError, DagResult},
        Certificate, CertificateAPI, Header, Vote, VoteAPI,
    },
    ensure,
};
use tracing::warn;

/// Aggregates votes for a particular header into a certificate.
pub struct VotesAggregator {
    weight: Stake,
    votes: Vec<(AuthorityIdentifier, AuthoritySignature)>,
    used: HashSet<AuthorityIdentifier>,
    metrics: Arc<PrimaryMetrics>,
}

impl VotesAggregator {
    pub fn new(metrics: Arc<PrimaryMetrics>) -> Self {
        metrics.votes_received_last_round.set(0);

        Self { weight: 0, votes: Vec::new(), used: HashSet::new(), metrics }
    }

    pub fn append(
        &mut self,
        vote: Vote,
        committee: &Committee,
        header: &Header,
    ) -> DagResult<Option<Certificate>> {
        let author = vote.author();

        // Ensure it is the first time this authority votes.
        ensure!(self.used.insert(author), DagError::AuthorityReuse(author.to_string()));

        self.votes.push((author, vote.signature().clone()));
        self.weight += committee.stake_by_id(author);

        self.metrics.votes_received_last_round.set(self.votes.len() as i64);
        if self.weight >= committee.quorum_threshold() {
            let cert = Certificate::new_unverified(committee, header.clone(), self.votes.clone())?;
            let (_, pks) = cert.signed_by(committee);

            let certificate_digest: Digest<{ crypto::DIGEST_LENGTH }> = Digest::from(cert.digest());
            match AggregateAuthoritySignature::try_from(cert.aggregated_signature())
                .map_err(|_| DagError::InvalidSignature)?
                .verify_secure(&to_intent_message(certificate_digest), &pks[..])
            {
                Err(err) => {
                    warn!(
                        "Failed to verify aggregated sig on certificate: {} error: {}",
                        certificate_digest, err
                    );
                    let mut i = 0;
                    while i < self.votes.len() {
                        let (id, sig) = &self.votes[i];
                        let pk = committee.authority_safe(id).protocol_key();
                        if sig.verify_secure(&to_intent_message(certificate_digest), pk).is_err() {
                            warn!("Invalid signature on header from authority: {}", id);
                            self.weight -= committee.stake(pk);
                            self.votes.remove(i);
                        } else {
                            i += 1;
                        }
                    }
                    return Ok(None)
                }
                Ok(_) => return Ok(Some(cert)),
            }
        }
        Ok(None)
    }
}

/// Aggregate certificates and check if we reach a quorum.
pub struct CertificatesAggregator {
    weight: Stake,
    certificates: Vec<Certificate>,
    used: HashSet<AuthorityIdentifier>,
}

impl CertificatesAggregator {
    pub fn new() -> Self {
        Self { weight: 0, certificates: Vec::new(), used: HashSet::new() }
    }

    pub fn append(
        &mut self,
        certificate: Certificate,
        committee: &Committee,
    ) -> Option<Vec<Certificate>> {
        let origin = certificate.origin();

        // Ensure it is the first time this authority votes.
        if !self.used.insert(origin) {
            return None
        }

        self.certificates.push(certificate);
        self.weight += committee.stake_by_id(origin);
        if self.weight >= committee.quorum_threshold() {
            // Note that we do not reset the weight here. If this function is called again and
            // the proposer didn't yet advance round, we can add extra certificates as parents.
            // This is required when running Bullshark as consensus.
            return Some(self.certificates.drain(..).collect())
        }
        None
    }
}
