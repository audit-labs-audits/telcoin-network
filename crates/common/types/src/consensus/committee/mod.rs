// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Committee type and builder.
mod builder;
pub use builder::*;
mod authority;
pub use authority::*;
use super::config::{Stake, ConfigError, CommitteeUpdateError};
use crate::consensus::crypto::{NetworkPublicKey, AuthorityPublicKey};
use consensus_network::Multiaddr;
use fastcrypto::traits::EncodeDecodeBase64;
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashSet},
    num::NonZeroU64,
};

/// The epoch number.
pub type Epoch = u64;

/// The group of authorities participating in consensus.
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct Committee {
    /// The authorities of epoch.
    authorities: BTreeMap<AuthorityPublicKey, Authority>,
    /// Keeps and index of the Authorities by their respective identifier
    #[serde(skip)]
    authorities_by_id: BTreeMap<AuthorityIdentifier, Authority>,
    /// The epoch number of this committee
    epoch: Epoch,
    /// The quorum threshold (2f+1)
    #[serde(skip)]
    quorum_threshold: Stake,
    /// The validity threshold (f+1)
    #[serde(skip)]
    validity_threshold: Stake,
}

impl Committee {
    /// Any committee should be created via the CommitteeBuilder - this is intentionally be marked
    /// as private method.
    fn new(authorities: BTreeMap<AuthorityPublicKey, Authority>, epoch: Epoch) -> Self {
        let mut committee = Self {
            authorities,
            epoch,
            authorities_by_id: Default::default(),
            validity_threshold: 0,
            quorum_threshold: 0,
        };
        committee.load();

        // Some sanity checks to ensure that we'll not end up in invalid state
        assert_eq!(committee.authorities_by_id.len(), committee.authorities.len());

        assert_eq!(committee.validity_threshold, committee.calculate_validity_threshold().get());
        assert_eq!(committee.quorum_threshold, committee.calculate_quorum_threshold().get());

        // ensure all the authorities are ordered in incremented manner with their ids - just some
        // extra confirmation here.
        for (index, (_, authority)) in committee.authorities.iter().enumerate() {
            assert_eq!(index as u16, authority.id.0);
        }

        committee
    }

    fn calculate_quorum_threshold(&self) -> NonZeroU64 {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (2 N + 3) / 3 = 2f + 1 + (2k + 2)/3 = 2f + 1 + k = N - f
        let total_votes: Stake = self.total_stake();
        NonZeroU64::new(2 * total_votes / 3 + 1).unwrap()
    }

    fn calculate_validity_threshold(&self) -> NonZeroU64 {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (N + 2) / 3 = f + 1 + k/3 = f + 1
        let total_votes: Stake = self.total_stake();
        NonZeroU64::new((total_votes + 2) / 3).unwrap()
    }

    /// Updates the committee internal secondary indexes.
    pub fn load(&mut self) {
        self.authorities_by_id = (0_u16..)
            .zip(self.authorities.iter_mut())
            .map(|(identifier, (_key, authority))| {
                let id = AuthorityIdentifier(identifier);
                authority.initialise(id);

                (id, authority.clone())
            })
            .collect();

        self.validity_threshold = self.calculate_validity_threshold().get();
        self.quorum_threshold = self.calculate_quorum_threshold().get();
    }

    /// Returns the current epoch.
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Provided an identifier it returns the corresponding authority
    pub fn authority(&self, identifier: &AuthorityIdentifier) -> Option<&Authority> {
        self.authorities_by_id.get(identifier)
    }

    /// Provided an identifier it returns the corresponding authority - if is not found then it
    /// panics
    pub fn authority_safe(&self, identifier: &AuthorityIdentifier) -> &Authority {
        self.authorities_by_id.get(identifier).unwrap_or_else(|| {
            panic!("Authority with id {:?} should have been in committee", identifier)
        })
    }

    /// Find the authority by it's public key, otherwise return `None`.
    pub fn authority_by_key(&self, key: &AuthorityPublicKey) -> Option<&Authority> {
        self.authorities.get(key)
    }

    /// Returns the keys in the committee
    pub fn keys(&self) -> Vec<AuthorityPublicKey> {
        self.authorities.keys().cloned().collect::<Vec<AuthorityPublicKey>>()
    }

    /// Returns all Authority structs in this committee.
    pub fn authorities(&self) -> impl Iterator<Item = &Authority> {
        self.authorities.values()
    }

    /// Return the [Authority] by the authority's [NetworkPublicKey]
    pub fn authority_by_network_key(&self, network_key: &NetworkPublicKey) -> Option<&Authority> {
        self.authorities
            .iter()
            .find(|(_, authority)| authority.network_key == *network_key)
            .map(|(_, authority)| authority)
    }

    /// Returns the number of authorities.
    pub fn size(&self) -> usize {
        self.authorities.len()
    }

    /// Return the stake of a specific authority.
    pub fn stake(&self, name: &AuthorityPublicKey) -> Stake {
        self.authorities.get(&name.clone()).map_or_else(|| 0, |x| x.stake)
    }

    /// Find an authority's stake by it's [AuthorityIdentifier]
    pub fn stake_by_id(&self, id: AuthorityIdentifier) -> Stake {
        self.authorities_by_id.get(&id).map_or_else(|| 0, |authority| authority.stake)
    }

    /// Returns the stake required to reach a quorum (2f+1).
    pub fn quorum_threshold(&self) -> Stake {
        self.quorum_threshold
    }

    /// Returns the stake required to reach availability (f+1).
    pub fn validity_threshold(&self) -> Stake {
        self.validity_threshold
    }

    /// Returns true if the provided stake has reached quorum (2f+1)
    pub fn reached_quorum(&self, stake: Stake) -> bool {
        stake >= self.quorum_threshold()
    }

    /// Returns true if the provided stake has reached availability (f+1)
    pub fn reached_validity(&self, stake: Stake) -> bool {
        stake >= self.validity_threshold()
    }

    /// Total stake from all authorities
    pub fn total_stake(&self) -> Stake {
        self.authorities.values().map(|x| x.stake).sum()
    }

    /// Returns a leader node as a weighted choice seeded by the provided integer
    pub fn leader(&self, seed: u64) -> Authority {
        let mut seed_bytes = [0u8; 32];
        seed_bytes[32 - 8..].copy_from_slice(&seed.to_le_bytes());
        let mut rng = StdRng::from_seed(seed_bytes);
        let choices = self
            .authorities
            .values()
            .map(|authority| (authority.clone(), authority.stake as f32))
            .collect::<Vec<_>>();
        choices
            .choose_weighted(&mut rng, |item| item.1)
            .expect("Weighted choice error: stake values incorrect!")
            .0
            .clone()
    }

    /// Returns the primary address of the target primary.
    pub fn primary(&self, to: &AuthorityPublicKey) -> Result<Multiaddr, ConfigError> {
        self.authorities
            .get(&to.clone())
            .map(|x| x.primary_address.clone())
            .ok_or_else(|| ConfigError::NotInCommittee((*to).encode_base64()))
    }

    /// Returns the primary address of the target primary.
    pub fn primary_by_id(&self, to: &AuthorityIdentifier) -> Result<Multiaddr, ConfigError> {
        self.authorities_by_id
            .get(to)
            .map(|x| x.primary_address.clone())
            .ok_or_else(|| ConfigError::NotInCommittee(to.0.to_string()))
    }

    /// Find an authority's [NetworkPublicKey] by the authority's [AuthorityPublicKey]
    pub fn network_key(&self, pk: &AuthorityPublicKey) -> Result<NetworkPublicKey, ConfigError> {
        self.authorities
            .get(&pk.clone())
            .map(|x| x.network_key.clone())
            .ok_or_else(|| ConfigError::NotInCommittee((*pk).encode_base64()))
    }

    /// Return all the network addresses in the committee.
    pub fn others_primaries(
        &self,
        myself: &AuthorityPublicKey,
    ) -> Vec<(AuthorityPublicKey, Multiaddr, NetworkPublicKey)> {
        self.authorities
            .iter()
            .filter(|(name, _)| *name != myself)
            .map(|(name, authority)| {
                (name.clone(), authority.primary_address.clone(), authority.network_key.clone())
            })
            .collect()
    }

    /// Return all the network addresses in the committee.
    pub fn others_primaries_by_id(
        &self,
        myself: AuthorityIdentifier,
    ) -> Vec<(AuthorityIdentifier, Multiaddr, NetworkPublicKey)> {
        self.authorities
            .iter()
            .filter(|(_, authority)| authority.id() != myself)
            .map(|(_, authority)| {
                (authority.id(), authority.primary_address(), authority.network_key())
            })
            .collect()
    }

    fn get_all_network_addresses(&self) -> HashSet<&Multiaddr> {
        self.authorities.values().map(|authority| &authority.primary_address).collect()
    }

    /// Return the network addresses that are present in the current committee but that are absent
    /// from the new committee (provided as argument).
    pub fn network_diff<'a>(&'a self, other: &'a Self) -> HashSet<&Multiaddr> {
        self.get_all_network_addresses()
            .difference(&other.get_all_network_addresses())
            .cloned()
            .collect()
    }

    /// Update the networking information of some of the primaries. The arguments are a full vector
    /// of authorities which Public key and Stake must match the one stored in the current
    /// Committee. Any discrepancy will generate no update and return a vector of errors.
    pub fn update_primary_network_info(
        &mut self,
        mut new_info: BTreeMap<AuthorityPublicKey, (Stake, Multiaddr)>,
    ) -> Result<(), Vec<CommitteeUpdateError>> {
        let mut errors = None;

        let table = &self.authorities;
        let push_error_and_return = |acc, error| {
            let mut error_table = if let Err(errors) = acc { errors } else { Vec::new() };
            error_table.push(error);
            Err(error_table)
        };

        let res = table.iter().fold(Ok(BTreeMap::new()), |acc, (pk, authority)| {
            if let Some((stake, address)) = new_info.remove(pk) {
                if stake == authority.stake {
                    match acc {
                        // No error met yet, update the accumulator
                        Ok(mut bmap) => {
                            let mut res = authority.clone();
                            res.primary_address = address;
                            bmap.insert(pk.clone(), res);
                            Ok(bmap)
                        }
                        // in error mode, continue
                        _ => acc,
                    }
                } else {
                    // Stake does not match: create or append error
                    push_error_and_return(acc, CommitteeUpdateError::DifferentStake(pk.to_string()))
                }
            } else {
                // This key is absent from new information
                push_error_and_return(acc, CommitteeUpdateError::MissingFromUpdate(pk.to_string()))
            }
        });

        // If there are elements left in new_info, they are not in the original table
        // If new_info is empty, this is a no-op.
        let res = new_info.iter().fold(res, |acc, (pk, _)| {
            push_error_and_return(acc, CommitteeUpdateError::NotInCommittee(pk.to_string()))
        });

        match res {
            Ok(new_table) => self.authorities = new_table,
            Err(errs) => {
                errors = Some(errs);
            }
        };

        errors.map(Err).unwrap_or(Ok(()))
    }

    /// Used for testing - not recommended to use for any other case.
    /// It creates a new instance with updated epoch
    pub fn advance_epoch(&self, new_epoch: Epoch) -> Committee {
        Committee::new(self.authorities.clone(), new_epoch)
    }
}
impl std::fmt::Display for Committee {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Committee E{}: {:?}",
            self.epoch(),
            self.authorities
                .keys()
                .map(|x| {
                    if let Some(k) = x.encode_base64().get(0..16) {
                        k.to_owned()
                    } else {
                        format!("Invalid key: {}", x)
                    }
                })
                .collect::<Vec<_>>()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{Authority, Committee};
    use crate::consensus::crypto::{AuthorityKeyPair, NetworkKeyPair, AuthorityPublicKey};
    use consensus_network::Multiaddr;
    use fastcrypto::traits::KeyPair as _;
    use rand::thread_rng;
    use std::collections::BTreeMap;

    #[test]
    fn committee_load() {
        // GIVEN
        let mut rng = thread_rng();
        let num_of_authorities = 10;

        let authorities = (0..num_of_authorities)
            .map(|_i| {
                let keypair = AuthorityKeyPair::generate(&mut rng);
                let network_keypair = NetworkKeyPair::generate(&mut rng);

                let a = Authority::new(
                    keypair.public().clone(),
                    1,
                    Multiaddr::empty(),
                    network_keypair.public().clone(),
                );

                (keypair.public().clone(), a)
            })
            .collect::<BTreeMap<AuthorityPublicKey, Authority>>();

        // WHEN
        let committee = Committee::new(authorities, 10);

        // THEN
        assert_eq!(committee.authorities_by_id.len() as u64, num_of_authorities);

        for (identifier, authority) in committee.authorities_by_id.iter() {
            assert_eq!(*identifier, authority.id());
        }

        // AND ensure thresholds are calculated correctly
        assert_eq!(committee.quorum_threshold(), 7);
        assert_eq!(committee.validity_threshold(), 4);

        // AND ensure authorities are returned in the same order
        for ((id, authority_1), (public_key, authority_2)) in
            committee.authorities_by_id.iter().zip(committee.authorities)
        {
            assert_eq!(authority_1.clone(), authority_2);
            assert_eq!(*id, authority_2.id());
            assert_eq!(&public_key, authority_1.protocol_key());
        }
    }
}