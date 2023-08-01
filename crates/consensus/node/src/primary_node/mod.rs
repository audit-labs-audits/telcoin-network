// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Primary Node is a threadsafe container for inner-primary.

mod inner;
use inner::PrimaryNodeInner;
use crate::NodeError;
use consensus_metrics::{RegistryID, RegistryService};
use lattice_executor::ExecutionState;
use lattice_network::client::NetworkClient;
use lattice_storage::NodeStorage;
use prometheus::Registry;
use std::sync::Arc;
use tn_types::consensus::{
    Committee, Parameters, WorkerCache,
    crypto::{AuthorityKeyPair, NetworkKeyPair},
};
use tokio::sync::RwLock;

/// The main component for managing an instance of [`Primary`].
/// 
/// PrimaryNode contains a RwLock on an inner type so it is threadsafe.
#[derive(Clone)]
pub struct PrimaryNode {
    internal: Arc<RwLock<PrimaryNodeInner>>,
}

impl PrimaryNode {
    /// Create a new instance of Self
    pub fn new(
        parameters: Parameters,
        internal_consensus: bool,
        registry_service: RegistryService,
    ) -> PrimaryNode {
        let inner = PrimaryNodeInner::new(
            parameters,
            internal_consensus,
            registry_service,
        );

        Self { internal: Arc::new(RwLock::new(inner)) }
    }

    /// Lock inner and call the guard's `.start()` method.
    /// 
    /// Starts the primary node with the provided info. If the node is already running then this
    /// method will return an error instead.
    pub async fn start<State>(
        &self, // The private-public key pair of this authority.
        keypair: AuthorityKeyPair,
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
        // The state used by the client to execute transactions.
        execution_state: Arc<State>,
    ) -> Result<(), NodeError>
    where
        State: ExecutionState + Send + Sync + 'static,
    {
        let mut guard = self.internal.write().await;
        guard.client = Some(client.clone());
        guard
            .start(
                keypair,
                network_keypair,
                committee,
                worker_cache,
                client,
                store,
                execution_state,
            )
            .await
    }

    /// Lock inner and call the guard's `.shutdown()` method.
    /// 
    /// Will shutdown the primary node and wait until the node has shutdown by waiting on the
    /// underlying components handles. If the node was not already running then the
    /// method will return immediately.
    pub async fn shutdown(&self) {
        let mut guard = self.internal.write().await;
        guard.shutdown().await
    }

    /// Lock inner and call the guard's `.is_running()` method.
    /// 
    /// If any of the underlying handles haven't finished, then this method will return
    /// true, otherwise false will return instead.
    pub async fn is_running(&self) -> bool {
        let guard = self.internal.read().await;
        guard.is_running().await
    }

    /// Lock inner and call the guard's `.wait()` method.
    /// 
    /// Helper method useful to wait on the execution of the primary node
    pub async fn wait(&self) {
        let mut guard = self.internal.write().await;
        guard.wait().await
    }

    /// Lock inner and clone the primary's latest registry_id and registry.
    pub async fn registry(&self) -> Option<(RegistryID, Registry)> {
        let guard = self.internal.read().await;
        guard.registry.clone()
    }
}
