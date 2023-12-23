// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
#![allow(clippy::mutable_key_type)]

use crate::{
    crypto::{BlsPublicKey, NetworkPublicKey},
    Multiaddr,
};
use fastcrypto::traits::{EncodeDecodeBase64, InsecureDefault};
use serde::{de::DeserializeOwned, ser::SerializeMap, Deserialize, Serialize, Serializer};

use std::{
    collections::{BTreeMap, HashSet},
    fs::{self, OpenOptions},
    io::{BufWriter, Write as _},
};
use thiserror::Error;

pub mod committee;
pub use committee::*;
pub mod utils;

/// The epoch number.
pub type Epoch = u64;

/// Opaque bytes uniquely identifying the current chain. Analogue of the
/// type in `sui-types` crate.
///
/// TODO: this should be replaced with chainspec.
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct ChainIdentifier([u8; 32]);

impl ChainIdentifier {
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn unknown() -> Self {
        Self([0; 32])
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Node {0} is not in the committee")]
    NotInCommittee(String),

    #[error("Node {0} is not in the worker cache")]
    NotInWorkerCache(String),

    #[error("Unknown worker id {0}")]
    UnknownWorker(WorkerId),

    #[error("Failed to read config file '{file}': {message}")]
    ImportError { file: String, message: String },

    #[error("Failed to write config file '{file}': {message}")]
    ExportError { file: String, message: String },
}

#[derive(Error, Debug)]
pub enum CommitteeUpdateError {
    #[error("Node {0} is not in the committee")]
    NotInCommittee(String),

    #[error("Node {0} was not in the update")]
    MissingFromUpdate(String),

    #[error("Node {0} has a different stake than expected")]
    DifferentStake(String),
}

pub trait Import: DeserializeOwned {
    fn import(path: &str) -> Result<Self, ConfigError> {
        let reader = || -> Result<Self, std::io::Error> {
            let data = fs::read(path)?;
            Ok(serde_json::from_slice(data.as_slice())?)
        };
        reader().map_err(|e| ConfigError::ImportError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }
}

impl<D: DeserializeOwned> Import for D {}

pub trait Export: Serialize {
    fn export(&self, path: &str) -> Result<(), ConfigError> {
        let writer = || -> Result<(), std::io::Error> {
            let file = OpenOptions::new().create(true).write(true).open(path)?;
            let mut writer = BufWriter::new(file);
            let data = serde_json::to_string_pretty(self).unwrap();
            writer.write_all(data.as_ref())?;
            writer.write_all(b"\n")?;
            Ok(())
        };
        writer().map_err(|e| ConfigError::ExportError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }
}

impl<S: Serialize> Export for S {}

// TODO: This actually represents voting power (out of 10,000) and not amount staked.
// Consider renaming to `VotingPower`.
pub type Stake = u64;
pub type WorkerId = u16;

/// Information for the worker
/// - [NetworkPublicKey] for identifying network messages.
/// - [Multiaddr] for receiving transactions.
/// - [Multiaddr] for receiving messages from other workers and the primary.
#[derive(Clone, Serialize, Deserialize, Eq, Hash, PartialEq, Debug)]
pub struct WorkerInfo {
    /// The public key of this worker.
    pub name: NetworkPublicKey,
    /// Address to receive client transactions (WAN).
    pub transactions: Multiaddr,
    /// Address to receive messages from other workers (WAN) and our primary.
    pub worker_address: Multiaddr,
}

impl Default for WorkerInfo {
    fn default() -> Self {
        Self {
            name: NetworkPublicKey::insecure_default(),
            transactions: Multiaddr::default(),
            worker_address: Multiaddr::default(),
        }
    }
}

/// Map of all workers for the authority.
///
/// The map associates the worker's id to [WorkerInfo].
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct WorkerIndex(
    pub BTreeMap<WorkerId, WorkerInfo>,
);

// Custom serilaization impl for WorkerIndex bc int types are invalid keys for TOML files.
impl Serialize for WorkerIndex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (k, v) in self.0.iter() {
            let k = k.to_string();
            map.serialize_entry(&k, &v)?;
        }
        map.end()
    }
}

impl WorkerIndex {
    /// Insert a new worker to the index.
    ///
    /// If the worker id already existed, the worker's info
    /// is updated.
    pub fn insert(&mut self, worker_id: WorkerId, worker_info: WorkerInfo) {
        self.0.insert(worker_id, worker_info);
    }
}

/// The collection of all workers organized by authority public keys
/// that comprise the [Committee] for a specific [Epoch].
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct WorkerCache {
    /// The epoch number for workers
    pub epoch: Epoch,
    /// The authority to worker index.
    pub workers: BTreeMap<BlsPublicKey, WorkerIndex>,
}

impl std::fmt::Display for WorkerIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkerIndex {:?}",
            self.0
                .iter()
                .map(|(key, value)| { format!("{}:{:?}", key.to_string().as_str(), value) })
                .collect::<Vec<_>>()
        )
    }
}

impl std::fmt::Display for WorkerCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkerCache E{}: {:?}",
            self.epoch(),
            self.workers
                .iter()
                .map(|(k, v)| {
                    if let Some(x) = k.encode_base64().get(0..16) {
                        format!("{}: {}", x, v)
                    } else {
                        format!("Invalid key: {}", k)
                    }
                })
                .collect::<Vec<_>>()
        )
    }
}

impl WorkerCache {
    /// Returns the current epoch.
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Returns the addresses of a specific worker (`id`) of a specific authority (`to`).
    pub fn worker(&self, to: &BlsPublicKey, id: &WorkerId) -> Result<WorkerInfo, ConfigError> {
        self.workers
            .iter()
            .find_map(|v| match_opt::match_opt!(v, (name, authority) if name == to => authority))
            .ok_or_else(|| {
                ConfigError::NotInWorkerCache(ToString::to_string(&(*to).encode_base64()))
            })?
            .0
            .iter()
            .find(|(worker_id, _)| worker_id == &id)
            .map(|(_, worker)| worker.clone())
            .ok_or_else(|| ConfigError::NotInWorkerCache((*to).encode_base64()))
    }

    /// Returns the addresses of all our workers.
    pub fn our_workers(&self, myself: &BlsPublicKey) -> Result<Vec<WorkerInfo>, ConfigError> {
        let res = self
            .workers
            .iter()
            .find_map(
                |v| match_opt::match_opt!(v, (name, authority) if name == myself => authority),
            )
            .ok_or_else(|| ConfigError::NotInWorkerCache((*myself).encode_base64()))?
            .0
            .values()
            .cloned()
            .collect();
        Ok(res)
    }

    /// Returns the addresses of all known workers.
    pub fn all_workers(&self) -> Vec<(NetworkPublicKey, Multiaddr)> {
        self.workers
            .iter()
            .flat_map(|(_, w)| w.0.values().map(|w| (w.name.clone(), w.worker_address.clone())))
            .collect()
    }

    /// Returns the addresses of all workers with a specific id except the ones of the authority
    /// specified by `myself`.
    pub fn others_workers_by_id(
        &self,
        myself: &BlsPublicKey,
        id: &WorkerId,
    ) -> Vec<(BlsPublicKey, WorkerInfo)> {
        self.workers
            .iter()
            .filter(|(name, _)| *name != myself )
            .flat_map(
                |(name, authority)|  authority.0.iter().flat_map(
                    |v| match_opt::match_opt!(v,(worker_id, addresses) if worker_id == id => (name.clone(), addresses.clone()))))
            .collect()
    }

    /// Returns the addresses of all workers that are not of our node.
    pub fn others_workers(&self, myself: &BlsPublicKey) -> Vec<(BlsPublicKey, WorkerInfo)> {
        self.workers
            .iter()
            .filter(|(name, _)| *name != myself)
            .flat_map(|(name, authority)| authority.0.iter().map(|v| (name.clone(), v.1.clone())))
            .collect()
    }

    /// Return the network addresses that are present in the current worker cache
    /// that are from a primary key that are no longer in the committee. Current
    /// committee keys provided as an argument.
    pub fn network_diff(&self, keys: Vec<&BlsPublicKey>) -> HashSet<&Multiaddr> {
        self.workers
            .iter()
            .filter(|(name, _)| !keys.contains(name))
            .flat_map(|(_, authority)| {
                authority
                    .0
                    .values()
                    .map(|address| &address.transactions)
                    .chain(authority.0.values().map(|address| &address.worker_address))
            })
            .collect()
    }
}
