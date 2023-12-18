// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

//! Worker fixture for the cluster

use consensus_metrics::RegistryService;
use narwhal_network::client::NetworkClient;
use narwhal_storage::NodeStorage;
use narwhal_types::{
    test_utils::temp_dir, AuthorityIdentifier, Committee, Multiaddr, NetworkKeyPair, Parameters,
    PublicKey, WorkerCache, WorkerId,
};
use prometheus::Registry;
use std::path::PathBuf;
use tn_node::{engine::ExecutionNode, metrics::worker_metrics_registry, worker::WorkerNode};
use tracing::info;

#[derive(Clone)]
pub struct WorkerNodeDetails {
    pub id: WorkerId,
    pub transactions_address: Multiaddr,
    pub registry: Registry,
    name: AuthorityIdentifier,
    primary_key: PublicKey,
    node: WorkerNode,
    committee: Committee,
    worker_cache: WorkerCache,
    store_path: PathBuf,
}

impl WorkerNodeDetails {
    pub(crate) fn new(
        id: WorkerId,
        name: AuthorityIdentifier,
        primary_key: PublicKey,
        parameters: Parameters,
        transactions_address: Multiaddr,
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
            transactions_address,
            committee,
            worker_cache,
            node,
        }
    }

    /// Starts the node. When preserve_store is true then the last used
    pub(crate) async fn start(
        &mut self,
        keypair: NetworkKeyPair,
        client: NetworkClient,
        preserve_store: bool,
        execution_node: &ExecutionNode,
    ) -> eyre::Result<()> {
        if self.is_running().await {
            panic!("Worker with id {} is already running, can't start again", self.id);
        }

        let registry = worker_metrics_registry(self.id, self.name)?;

        // Make the data store.
        let store_path = if preserve_store { self.store_path.clone() } else { temp_dir() };

        let worker_store = NodeStorage::reopen(store_path.clone(), None);

        info!(target: "cluster::worker", "starting worker-{} for authority {}", self.id, self.name);

        self.node
            .start(
                self.primary_key.clone(),
                keypair,
                self.committee.clone(),
                self.worker_cache.clone(),
                client,
                &worker_store,
                None,
                execution_node,
            )
            .await?;

        self.store_path = store_path;
        self.registry = registry;

        Ok(())
    }

    pub(crate) async fn stop(&self) {
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
