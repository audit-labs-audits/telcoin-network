// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Worker components for cluster.
use crate::temp_dir;
use consensus_metrics::RegistryService;
use tn_types::consensus::Multiaddr;
use lattice_network::client::NetworkClient;
use lattice_node::{
    metrics::worker_metrics_registry, worker_node::WorkerNode,
};
use lattice_storage::NodeStorage;
use lattice_worker::TrivialTransactionValidator;
use prometheus::Registry;
use std::path::PathBuf;
use tn_types::consensus::{
    AuthorityIdentifier, Committee, Parameters, WorkerCache, WorkerId,
    crypto::{NetworkKeyPair, AuthorityPublicKey},
};
use tracing::info;

#[derive(Clone)]
pub struct WorkerNodeDetails {
    pub id: WorkerId,
    pub registry: Registry,
    name: AuthorityIdentifier,
    primary_key: AuthorityPublicKey,
    node: WorkerNode,
    committee: Committee,
    worker_cache: WorkerCache,
    store_path: PathBuf,
}

impl WorkerNodeDetails {
    pub(super) fn new(
        id: WorkerId,
        name: AuthorityIdentifier,
        primary_key: AuthorityPublicKey,
        parameters: Parameters,
        committee: Committee,
        worker_cache: WorkerCache,
    ) -> Self {
        let registry_service = RegistryService::new(Registry::new());
        let node = WorkerNode::new(id, parameters, registry_service);

        Self {
            id,
            name,
            primary_key,
            registry: Registry::new(),
            store_path: temp_dir(),
            committee,
            worker_cache,
            node,
        }
    }

    /// Starts the node. When preserve_store is true then the last used
    pub(super) async fn start(
        &mut self,
        keypair: NetworkKeyPair,
        client: NetworkClient,
        preserve_store: bool,
    ) {
        if self.is_running().await {
            panic!("Worker with id {} is already running, can't start again", self.id);
        }

        let registry = worker_metrics_registry(self.id, self.name);

        // Make the data store.
        let store_path = if preserve_store { self.store_path.clone() } else { temp_dir() };

        let worker_store = NodeStorage::reopen(store_path.clone(), None);

        self.node
            .start(
                self.primary_key.clone(),
                keypair,
                self.committee.clone(),
                self.worker_cache.clone(),
                client,
                &worker_store,
                TrivialTransactionValidator::default(),
                None,
            )
            .await
            .unwrap();

        self.store_path = store_path;
        self.registry = registry;
    }

    pub(super) async fn stop(&self) {
        self.node.shutdown().await;
        info!("Aborted worker node for id {}", self.id);
    }

    /// This method returns whether the node is still running or not. We
    /// iterate over all the handlers and check whether there is still any
    /// that is not finished. If we find at least one, then we report the
    /// node as still running.
    pub async fn is_running(&self) -> bool {
        self.node.is_running().await
    }
}
