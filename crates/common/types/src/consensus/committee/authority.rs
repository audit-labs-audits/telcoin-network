// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Authority information for validators to participate in a committee to reach consensus.
use crate::consensus::crypto::{NetworkPublicKey, AuthorityPublicKey, AuthorityPublicKeyBytes};
use consensus_network::Multiaddr;
use consensus_util_mem::MallocSizeOf;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use crate::consensus::config::Stake;

/// Every authority gets uniquely identified by the AuthorityIdentifier.
/// 
/// The type can be easily swapped without needing to change anything else in the implementation.
#[derive(
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Clone,
    Copy,
    Debug,
    Default,
    Hash,
    Serialize,
    Deserialize,
    MallocSizeOf,
)]
pub struct AuthorityIdentifier(pub u16);

impl Display for AuthorityIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.to_string().as_str())
    }
}

/// Struct for holding all information needed to participate as a validating authority for consensus.
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct Authority {
    /// The id under which we identify this authority across Narwhal
    #[serde(skip)]
    pub(super) id: AuthorityIdentifier,
    /// The authority's main PublicKey which is used to verify the content they sign.
    protocol_key: AuthorityPublicKey,
    /// The authority's main PublicKey expressed as pure bytes
    protocol_key_bytes: AuthorityPublicKeyBytes,
    /// The voting power of this authority.
    pub(super) stake: Stake,
    /// The network address of the primary.
    pub(super) primary_address: Multiaddr,
    /// Network public key of the primary.
    pub(super) network_key: NetworkPublicKey,
    /// Indicate if this authority is ready for use.
    /// 
    /// There are secondary indexes that should be initialised before we are ready to use the
    /// authority - this bool prevents premature use.
    #[serde(skip)]
    initialised: bool,
}

impl Authority {
    /// The constructor is not public by design. Everyone who wants to create authorities should do
    /// it via Committee (more specifically can use CommitteeBuilder). As some internal properties
    /// of Authority are initialised via the Committee, to ensure that the user will not
    /// accidentally use stale Authority data, should always derive them via the Commitee.
    pub(super) fn new(
        protocol_key: AuthorityPublicKey,
        stake: Stake,
        primary_address: Multiaddr,
        network_key: NetworkPublicKey,
    ) -> Self {
        let protocol_key_bytes = AuthorityPublicKeyBytes::from(&protocol_key);

        Self {
            id: Default::default(),
            protocol_key,
            protocol_key_bytes,
            stake,
            primary_address,
            network_key,
            initialised: false,
        }
    }

    /// Mark the authority as ready for use.
    pub(super) fn initialise(&mut self, id: AuthorityIdentifier) {
        self.id = id;
        self.initialised = true;
    }

    /// The [AuthorityIdentifier] for this authority.
    pub fn id(&self) -> AuthorityIdentifier {
        assert!(self.initialised);
        self.id
    }

    /// The [AuthorityPublicKey] for this authority.
    pub fn protocol_key(&self) -> &AuthorityPublicKey {
        assert!(self.initialised);
        &self.protocol_key
    }

    /// The [AuthorityPublicKeyBytes] for this authority.
    pub fn protocol_key_bytes(&self) -> &AuthorityPublicKeyBytes {
        assert!(self.initialised);
        &self.protocol_key_bytes
    }

    /// The [Stake] for this authority.
    pub fn stake(&self) -> Stake {
        assert!(self.initialised);
        self.stake
    }

    /// The [Multiaddr] for this authority.
    pub fn primary_address(&self) -> Multiaddr {
        assert!(self.initialised);
        self.primary_address.clone()
    }

    /// The [NetworkPublicKey] for this authority.
    pub fn network_key(&self) -> NetworkPublicKey {
        assert!(self.initialised);
        self.network_key.clone()
    }
}
