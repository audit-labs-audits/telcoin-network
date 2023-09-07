// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Inner worker components
//! 
use crate::{metrics::new_registry, try_join_all, FuturesUnordered, NodeError};
use anemo::PeerId;
use consensus_metrics::{RegistryID, RegistryService};
use fastcrypto::traits::KeyPair;
use lattice_network::client::NetworkClient;
use lattice_storage::NodeStorage;
use lattice_worker::{
    metrics::{initialise_metrics, Metrics},
    TransactionValidator, Worker, NUM_SHUTDOWN_RECEIVERS,
};
use prometheus::Registry;
use std::time::Instant;
use tn_types::consensus::{
    Committee, Parameters, WorkerCache, WorkerId,
    crypto::{NetworkKeyPair, AuthorityPublicKey},
    PreSubscribedBroadcastSender,
};
use tokio::task::JoinHandle;
use tracing::{info, instrument};
use lattice_payload_builder::LatticePayloadBuilderHandle;

pub struct WorkerNodeInner {
    /// The worker's id
    id: WorkerId,
    /// The configuration parameters.
    parameters: Parameters,
    /// A prometheus RegistryService to use for the metrics
    registry_service: RegistryService,
    /// The latest registry id & registry used for the node
    registry: Option<(RegistryID, Registry)>,
    /// The task handles created from primary
    handles: FuturesUnordered<JoinHandle<()>>,
    /// The shutdown signal channel
    tx_shutdown: Option<PreSubscribedBroadcastSender>,
    /// Peer ID used for local connections.
    own_peer_id: Option<PeerId>,
}

impl WorkerNodeInner {
    /// Create a new instance of Self
    pub(super) fn new(
        id: WorkerId,
        parameters: Parameters,
        registry_service: RegistryService,
    ) -> Self {
        Self {
            id,
            parameters,
            registry_service,
            registry: None,
            handles: FuturesUnordered::new(),
            tx_shutdown: None,
            own_peer_id: None,
        }
    }

    /// Starts the worker node with the provided info. If the node is already running then this
    /// method will return an error instead.
    #[instrument(level = "info", skip_all)]
    pub(super) async fn start(
        &mut self,
        // The primary's id
        primary_name: AuthorityPublicKey,
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
        // The transaction validator that should be used
        tx_validator: impl TransactionValidator,
        // Optionally, if passed, then this metrics struct should be used instead of creating our
        // own one.
        metrics: Option<Metrics>,
        // Handle to the EL batch builder.
        batch_builder: Option<LatticePayloadBuilderHandle>,
    ) -> Result<(), NodeError> {
        if self.is_running().await {
            return Err(NodeError::NodeAlreadyRunning)
        }

        self.own_peer_id = Some(PeerId(network_keypair.public().0.to_bytes()));

        let (metrics, registry) = if let Some(metrics) = metrics {
            (metrics, None)
        } else {
            // create a new registry
            let registry = new_registry();

            (initialise_metrics(&registry), Some(registry))
        };

        let mut tx_shutdown = PreSubscribedBroadcastSender::new(NUM_SHUTDOWN_RECEIVERS);

        let authority = committee.authority_by_key(&primary_name).unwrap_or_else(|| {
            panic!("Our node with key {:?} should be in committee", primary_name)
        });

        let handles = Worker::spawn(
            authority.clone(),
            network_keypair,
            self.id,
            committee.clone(),
            worker_cache.clone(),
            self.parameters.clone(),
            tx_validator.clone(),
            client.clone(),
            store.batch_store.clone(),
            metrics,
            &mut tx_shutdown,
            batch_builder,
        );

        // store the registry
        if let Some(registry) = registry {
            self.swap_registry(Some(registry));
        }

        // now keep the handlers
        self.handles.clear();
        self.handles.extend(handles);
        self.tx_shutdown = Some(tx_shutdown);

        Ok(())
    }

    /// Will shutdown the worker node and wait until the node has shutdown by waiting on the
    /// underlying components handles. If the node was not already running then the
    /// method will return immediately.
    #[instrument(level = "info", skip_all)]
    pub(super) async fn shutdown(&mut self) {
        if !self.is_running().await {
            return
        }

        let now = Instant::now();
        if let Some(tx_shutdown) = self.tx_shutdown.as_ref() {
            tx_shutdown.send().expect("Couldn't send the shutdown signal to downstream components");
            self.tx_shutdown = None;
        }

        // Now wait until handles have been completed
        try_join_all(&mut self.handles).await.unwrap();

        self.swap_registry(None);

        info!(
            "Narwhal worker {} shutdown is complete - took {} seconds",
            self.id,
            now.elapsed().as_secs_f64()
        );
    }

    /// If any of the underlying handles haven't still finished, then this method will return
    /// true, otherwise false will returned instead.
    pub(super) async fn is_running(&self) -> bool {
        self.handles.iter().any(|h| !h.is_finished())
    }

    /// Helper method useful to wait on the execution of the worker node
    pub(super) async fn wait(&mut self) {
        try_join_all(&mut self.handles).await.unwrap();
    }

    /// Accepts an Option registry. If it's Some, then the new registry will be added in the
    /// registry service and the registry_id will be updated. Also, any previous registry will
    /// be removed. If None is passed, then the registry_id is updated to None and any old
    /// registry is removed from the RegistryService.
    fn swap_registry(&mut self, registry: Option<Registry>) {
        if let Some((registry_id, _registry)) = self.registry.as_ref() {
            self.registry_service.remove(*registry_id);
        }

        if let Some(registry) = registry {
            self.registry = Some((self.registry_service.add(registry.clone()), registry));
        } else {
            self.registry = None
        }
    }
}