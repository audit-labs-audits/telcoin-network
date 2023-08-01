// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Authority information for validators to participate in a committee to reach consensus.
use super::{Epoch, Stake, Authority, Committee};
use crate::consensus::crypto::{NetworkPublicKey, AuthorityPublicKey};
use consensus_network::Multiaddr;
use std::collections::BTreeMap;

/// Public utility to create a [Committee].
#[derive(Debug)]
pub struct CommitteeBuilder {
    /// Epoch for this committee
    epoch: Epoch,
    /// The authorities in this Committee
    authorities: BTreeMap<AuthorityPublicKey, Authority>,
}

impl CommitteeBuilder {
    /// Create an instance of [CommitteeBuilder]
    pub fn new(epoch: Epoch) -> Self {
        Self { epoch, authorities: BTreeMap::new() }
    }

    /// Create a CommitteeBuilder from an existing Committee.
    /// Testing purposes only.
    pub fn from_committee(committee: Committee) -> Self {
        Self {
            epoch: committee.epoch(),
            authorities: committee.authorities,
        }
    }

    /// Add an authority to the [CommitteeBuilder].
    /// 
    /// Authorities should be added before calling `.build()`.
    pub fn add_authority(
        mut self,
        protocol_key: AuthorityPublicKey,
        stake: Stake,
        primary_address: Multiaddr,
        network_key: NetworkPublicKey,
    ) -> Self {
        let authority = Authority::new(protocol_key.clone(), stake, primary_address, network_key);
        self.authorities.insert(protocol_key, authority);
        self
    }

    /// Create a [Committee] for consensus.
    /// 
    /// This method is required for creating a new committee.
    pub fn build(self) -> Committee {
        Committee::new(self.authorities, self.epoch)
    }

}
