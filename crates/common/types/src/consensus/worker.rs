use std::collections::{BTreeMap, HashSet};

// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::consensus::{Batch, BatchDigest};
use consensus_network::Multiaddr;
use fastcrypto::traits::EncodeDecodeBase64;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{crypto::{NetworkPublicKey, AuthorityPublicKey}, config::ConfigError, Epoch};

#[cfg(test)]
#[path = "tests/batch_serde.rs"]
mod batch_serde;

/// The local id for workers.
pub type WorkerId = u32;

/// Used by workers to send a new batch.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerBatchMessage {
    /// Batch
    pub batch: Batch,
}

/// Used by primary to ask worker for the request.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestBatchRequest {
    /// Requested batch's digest
    pub batch: BatchDigest,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestBatchResponse {
    /// Batch
    pub batch: Option<Batch>,
}

/// Used by primary to bulk request batches from workers local store.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestBatchesRequest {
    /// Vec of requested batches' digests
    pub batch_digests: Vec<BatchDigest>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestBatchesResponse {
    pub batches: Vec<Batch>,
    /// If true, the primary should request the batches from the workers again.
    /// This may not be something that can be trusted from a remote worker.
    pub is_size_limit_reached: bool,
}

// TODO: support propagating errors from the worker to the primary.
/// Oneshot channel for sending the BatchDigest.
pub type TxResponse = tokio::sync::oneshot::Sender<BatchDigest>;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DigestError {
    #[error("Invalid argument: invalid byte at {0}")]
    InvalidArgumentError(usize),
    #[error("Invalid length")]
    InvalidLengthError,
}


/// Information for a single worker.
#[derive(Clone, Serialize, Deserialize, Eq, Hash, PartialEq, Debug)]
pub struct WorkerInfo {
    /// The public key of this worker.
    pub name: NetworkPublicKey,
    // TODO: this needs to be mpsc::Receiver<Vec<Batch>>
    /// Address to receive client transactions (WAN).
    pub transactions: Multiaddr,
    /// Address to receive messages from other workers (WAN) and our primary.
    pub worker_address: Multiaddr,
}

/// `BTreeMap<WorkerId, WorkerInfo>` for all workers. 
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct WorkerIndex(pub BTreeMap<WorkerId, WorkerInfo>);

/// Cached information for all workers by the Authority's Public Key.
///
/// TODO - update docs
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct WorkerCache {
    /// The authority to worker index.
    pub workers: BTreeMap<AuthorityPublicKey, WorkerIndex>,
    /// The epoch number for workers
    pub epoch: Epoch,
}

impl std::fmt::Display for WorkerIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkerIndex {:?}",
            self.0
                .iter()
                .map(|(key, value)| { format!("{}:{:?}", key, value) })
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
    pub fn worker(&self, to: &AuthorityPublicKey, id: &WorkerId) -> Result<WorkerInfo, ConfigError> {
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
    pub fn our_workers(&self, myself: &AuthorityPublicKey) -> Result<Vec<WorkerInfo>, ConfigError> {
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
        myself: &AuthorityPublicKey,
        id: &WorkerId,
    ) -> Vec<(AuthorityPublicKey, WorkerInfo)> {
        self.workers
            .iter()
            .filter(|(name, _)| *name != myself )
            .flat_map(
                |(name, authority)|  authority.0.iter().flat_map(
                    |v| match_opt::match_opt!(v,(worker_id, addresses) if worker_id == id => (name.clone(), addresses.clone()))))
            .collect()
    }

    /// Returns the addresses of all workers that are not of our node.
    pub fn others_workers(&self, myself: &AuthorityPublicKey) -> Vec<(AuthorityPublicKey, WorkerInfo)> {
        self.workers
            .iter()
            .filter(|(name, _)| *name != myself)
            .flat_map(|(name, authority)| authority.0.iter().map(|v| (name.clone(), v.1.clone())))
            .collect()
    }

    /// Return the network addresses that are present in the current worker cache
    /// that are from a primary key that are no longer in the committee. Current
    /// committee keys provided as an argument.
    pub fn network_diff(&self, keys: Vec<&AuthorityPublicKey>) -> HashSet<&Multiaddr> {
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