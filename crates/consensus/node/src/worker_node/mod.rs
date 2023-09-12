// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Module containing container structs for all workers
//! and a threadsafe wrapper for each inner worker instance.
mod inner;
use inner::WorkerNodeInner;
use lattice_payload_builder::LatticePayloadBuilderHandle;
use crate::{metrics::new_registry, NodeError};
use tokio::sync::RwLock;
use arc_swap::{ArcSwap, ArcSwapOption};
use consensus_metrics::{RegistryID, RegistryService};
use lattice_network::client::NetworkClient;
use lattice_storage::NodeStorage;
use lattice_worker::{
    metrics::{initialise_metrics, Metrics}, TransactionValidator,
};
use std::{collections::HashMap, sync::Arc};
use tn_types::consensus::{
    Committee, Parameters, WorkerCache, WorkerId,
    crypto::{NetworkKeyPair, AuthorityPublicKey},
};
use tracing::{info, instrument};

/// Container struct for all workers in this node.
pub struct WorkerNodes {
    /// Map of worker ids to `WorkerNode` instances.
    workers: ArcSwap<HashMap<WorkerId, WorkerNode>>,
    /// The registry service for worker nodes.
    registry_service: RegistryService,
    /// Registry ID for worker nodes.
    registry_id: ArcSwapOption<RegistryID>,
    /// Parameters for all workers.
    parameters: Parameters,
    /// NetworkClient for workers.
    client: ArcSwapOption<NetworkClient>,
}

impl WorkerNodes {
    /// Create a new instance of WorkerNodes.
    /// 
    /// The information contained here is used to start workers.
    pub fn new(registry_service: RegistryService, parameters: Parameters) -> Self {
        Self {
            workers: ArcSwap::from(Arc::new(HashMap::default())),
            registry_service,
            registry_id: ArcSwapOption::empty(),
            parameters,
            client: ArcSwapOption::empty(),
        }
    }

    /// Start the workers specified in `ids_and_keypairs` arg.
    #[instrument(level = "info", skip_all)]
    pub async fn start(
        &self,
        // The primary's public key of this authority.
        primary_key: AuthorityPublicKey,
        // The ids & keypairs of the workers to spawn.
        ids_and_keypairs: Vec<(WorkerId, NetworkKeyPair)>,
        // The committee information.
        committee: Committee,
        // The worker information cache.
        worker_cache: WorkerCache,
        // Client for communications.
        client: NetworkClient,
        // The node's store
        // TODO: replace this by a path so the method can open an independent storage
        store: &NodeStorage,
        // The transaction validator defining Tx acceptance,
        tx_validator: impl TransactionValidator,
    ) -> Result<(), NodeError> {
        let worker_ids_running = self.workers_running().await;
        if !worker_ids_running.is_empty() {
            return Err(NodeError::WorkerNodesAlreadyRunning(worker_ids_running))
        }

        // create the registry first
        let registry = new_registry();

        let metrics = initialise_metrics(&registry);

        self.client.store(Some(Arc::new(client.clone())));

        // now clear the previous handles - we want to do that proactively
        // as it's not guaranteed that shutdown has been called
        self.workers.store(Arc::new(HashMap::default()));

        let mut workers = HashMap::<WorkerId, WorkerNode>::new();
        // start all the workers one by one
        for (worker_id, key_pair) in ids_and_keypairs {
            let worker =
                WorkerNode::new(worker_id, self.parameters.clone(), self.registry_service.clone());

            worker
                .start(
                    primary_key.clone(),
                    key_pair,
                    committee.clone(),
                    worker_cache.clone(),
                    client.clone(),
                    store,
                    tx_validator.clone(),
                    Some(metrics.clone()),
                )
                .await?;

            workers.insert(worker_id, worker);
        }

        // update the worker handles.
        self.workers.store(Arc::new(workers));

        // now add the registry
        let registry_id = self.registry_service.add(registry);

        if let Some(old_registry_id) = self.registry_id.swap(Some(Arc::new(registry_id))) {
            // a little of defensive programming - ensure that we always clean up the previous
            // registry
            self.registry_service.remove(*old_registry_id.as_ref());
        }

        Ok(())
    }

    /// Shuts down all the workers
    #[instrument(level = "info", skip_all)]
    pub async fn shutdown(&self) {
        tracing::debug!("\n\n\n!!!!~~~~~~~~calling shutdown on client..\n\n\n");
        if let Some(client) = self.client.load_full() {
            client.shutdown();
            tracing::debug!("worker's client shutdown");
        }

        tracing::debug!("loading workers now as_ref()..");

        for (key, worker) in self.workers.load_full().as_ref() {
            info!("Shutting down worker {}", key);
            worker.shutdown().await;
        }

        // now remove the registry id
        if let Some(old_registry_id) = self.registry_id.swap(None) {
            // a little of defensive programming - ensure that we always clean up the previous
            // registry
            self.registry_service.remove(*old_registry_id.as_ref());
        }

        // now clean up the worker handles
        self.workers.store(Arc::new(HashMap::default()));
    }

    /// returns the worker ids that are currently running
    pub async fn workers_running(&self) -> Vec<WorkerId> {
        let mut worker_ids = Vec::new();

        for (id, worker) in self.workers.load_full().as_ref() {
            if worker.is_running().await {
                worker_ids.push(*id);
            }
        }

        worker_ids
    }
}


/// The main component for managing an instance of [`Worker`].
/// 
/// WorkerNode contains a RwLock on an inner type so it is threadsafe.
#[derive(Clone)]
pub struct WorkerNode {
    internal: Arc<RwLock<WorkerNodeInner>>,
}

impl WorkerNode {
    /// Create a new instance of Self
    pub fn new(
        id: WorkerId,
        parameters: Parameters,
        registry_service: RegistryService,
    ) -> WorkerNode {
        let inner = WorkerNodeInner::new(
            id,
            parameters,
            registry_service,
        );

        Self { internal: Arc::new(RwLock::new(inner)) }
    }

    /// Lock inner and call the guard's `.start()` method.
    /// 
    /// Starts the worker node with the provided info. If the worker
    /// is already running, the method will return an error instead.
    pub async fn start(
        &self,
        // The primary's public key of this authority.
        primary_key: AuthorityPublicKey,
        // The private-public network key pair of this authority.
        network_keypair: NetworkKeyPair,
        // The committee information.
        committee: Committee,
        // The worker information cache.
        worker_cache: WorkerCache,
        // Client for communications.
        client: NetworkClient,
        // The node's store
        // TODO: replace this by a path so the method can open and independent storage
        store: &NodeStorage,
        // The transaction validator defining Tx acceptance,
        tx_validator: impl TransactionValidator,
        // An optional metrics struct
        metrics: Option<Metrics>,
    ) -> Result<(), NodeError> {
        let mut guard = self.internal.write().await;
        guard
            .start(
                primary_key,
                network_keypair,
                committee,
                worker_cache,
                client,
                store,
                tx_validator,
                metrics,
            )
            .await
    }

    /// Lock inner and call the guard's `.shutdown()` method.
    /// 
    /// Will shutdown the worker node and wait until the node has shutdown by waiting on the
    /// underlying components handles. If the node was not already running then the
    /// method will return immediately.
    pub async fn shutdown(&self) {
        let mut guard = self.internal.write().await;
        guard.shutdown().await
    }

    /// Lock inner and call the guard's `.is_running()` method.
    /// 
    /// If any of the underlying handles haven't still finished, then this method will return
    /// true, otherwise false will returned instead.
    pub async fn is_running(&self) -> bool {
        let guard = self.internal.read().await;
        guard.is_running().await
    }

    /// Lock inner and call the guard's `.wait()` method.
    /// 
    /// Helper method useful to wait on the execution of the worker node
    pub async fn wait(&self) {
        let mut guard = self.internal.write().await;
        guard.wait().await
    }
}
