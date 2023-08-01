// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Authority details 
use super::{PrimaryNodeDetails, WorkerNodeDetails};
use consensus_network::multiaddr::Multiaddr;
use fastcrypto::traits::KeyPair as _;
use lattice_network::client::NetworkClient;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tn_types::consensus::{
    AuthorityIdentifier, Committee, Parameters, WorkerCache, WorkerId,
    crypto::{AuthorityKeyPair, NetworkKeyPair, AuthorityPublicKey},
    ConfigurationClient, ProposerClient, TransactionsClient,
};
use tokio::sync::RwLock;
use tonic::transport::Channel;

/// The authority details hold all the necessary structs and details
/// to identify and manage a specific authority.
/// 
/// An authority is composed of its primary node and the worker nodes.
/// Via this struct we can manage the nodes one by one or in batch
/// fashion (ex stop_all).
/// 
/// The Authority can be cloned and reused across the instances as its
/// internals are thread safe. So changes made from one instance will be
/// reflected to another.
#[allow(dead_code)]
#[derive(Clone)]
pub struct AuthorityDetails {
    pub id: usize,
    pub name: AuthorityIdentifier,
    pub public_key: AuthorityPublicKey,
    client: NetworkClient,
    internal: Arc<RwLock<AuthorityDetailsInternal>>,
}

struct AuthorityDetailsInternal {
    primary: PrimaryNodeDetails,
    worker_keypairs: Vec<NetworkKeyPair>,
    workers: HashMap<WorkerId, WorkerNodeDetails>,
}

impl AuthorityDetails {
    pub fn new(
        id: usize,
        name: AuthorityIdentifier,
        key_pair: AuthorityKeyPair,
        network_key_pair: NetworkKeyPair,
        worker_keypairs: Vec<NetworkKeyPair>,
        parameters: Parameters,
        committee: Committee,
        worker_cache: WorkerCache,
        internal_consensus_enabled: bool,
    ) -> Self {
        // Create network client.
        let client = NetworkClient::new_from_keypair(&network_key_pair);

        // Create all the nodes we have in the committee
        let public_key = key_pair.public().clone();
        let primary = PrimaryNodeDetails::new(
            id,
            name,
            key_pair,
            network_key_pair,
            parameters.clone(),
            committee.clone(),
            worker_cache.clone(),
            internal_consensus_enabled,
        );

        // Create all the workers - even if we don't intend to start them all. Those
        // act as place holder setups. That gives us the power in a clear way manage
        // the nodes independently.
        let mut workers = HashMap::new();
        for (worker_id, addresses) in worker_cache.workers.get(&public_key).unwrap().0.clone() {
            let worker = WorkerNodeDetails::new(
                worker_id,
                name,
                public_key.clone(),
                parameters.clone(),
                addresses.transactions.clone(),
                committee.clone(),
                worker_cache.clone(),
            );
            workers.insert(worker_id, worker);
        }

        let internal = AuthorityDetailsInternal { primary, worker_keypairs, workers };

        Self { id, public_key, name, client, internal: Arc::new(RwLock::new(internal)) }
    }

    /// Starts the node's primary and workers. If the num_of_workers is provided
    /// then only those ones will be started. Otherwise all the available workers
    /// will be started instead.
    /// If the preserve_store value is true then the previous node's storage
    /// will be preserved. If false then the node will  start with a fresh
    /// (empty) storage.
    pub async fn start(&self, preserve_store: bool, num_of_workers: Option<usize>) {
        self.start_primary(preserve_store).await;

        let workers_to_start;
        {
            let internal = self.internal.read().await;
            workers_to_start = num_of_workers.unwrap_or(internal.workers.len());
        }

        for id in 0..workers_to_start {
            self.start_worker(id as WorkerId, preserve_store).await;
        }
    }

    /// Starts the primary node. If the preserve_store value is true then the
    /// previous node's storage will be preserved. If false then the node will
    /// start with a fresh (empty) storage.
    pub async fn start_primary(&self, preserve_store: bool) {
        let mut internal = self.internal.write().await;

        internal.primary.start(self.client.clone(), preserve_store).await;
    }

    /// Stop this primary.
    pub async fn stop_primary(&self) {
        let internal = self.internal.read().await;

        internal.primary.stop().await;
    }

    /// Start all workers for all nodes.
    pub async fn start_all_workers(&self, preserve_store: bool) {
        let mut internal = self.internal.write().await;
        let worker_keypairs =
            internal.worker_keypairs.iter().map(|kp| kp.copy()).collect::<Vec<NetworkKeyPair>>();

        for (id, worker) in internal.workers.iter_mut() {
            let keypair = worker_keypairs.get(*id as usize).unwrap().copy();
            worker.start(keypair, self.client.clone(), preserve_store).await;
        }
    }

    /// Starts the worker node by the provided id. If worker is not found then
    /// a panic is raised. If the preserve_store value is true then the
    /// previous node's storage will be preserved. If false then the node will
    /// start with a fresh (empty) storage.
    pub async fn start_worker(&self, id: WorkerId, preserve_store: bool) {
        let mut internal = self.internal.write().await;
        let keypair = internal.worker_keypairs.get(id as usize).unwrap().copy();
        let worker = internal
            .workers
            .get_mut(&id)
            .unwrap_or_else(|| panic!("Worker with id {} not found ", id));

        worker.start(keypair, self.client.clone(), preserve_store).await;
    }

    /// Stop a single worker.
    pub async fn stop_worker(&self, id: WorkerId) {
        let internal = self.internal.read().await;

        internal
            .workers
            .get(&id)
            .unwrap_or_else(|| panic!("Worker with id {} not found ", id))
            .stop()
            .await;
    }

    /// Stops all the nodes (primary & workers).
    pub async fn stop_all(&self) {
        self.client.shutdown();

        let internal = self.internal.read().await;
        internal.primary.stop().await;
        for (_, worker) in internal.workers.iter() {
            worker.stop().await;
        }
    }

    /// Will restart the node with the current setup that has been chosen
    /// (ex same number of nodes).
    /// `preserve_store`: if true then the same storage will be used for the
    /// node
    /// `delay`: before starting again we'll wait for that long. If zero provided
    /// then won't wait at all
    pub async fn restart(&self, preserve_store: bool, delay: Duration) {
        let num_of_workers = self.workers().await.len();

        self.stop_all().await;

        tokio::time::sleep(delay).await;

        // now start again the node with the same workers
        self.start(preserve_store, Some(num_of_workers)).await;
    }

    /// Returns the current primary node running as a clone. If the primary
    ///node stops and starts again and it's needed by the user then this
    /// method should be called again to get the latest one.
    pub async fn primary(&self) -> PrimaryNodeDetails {
        let internal = self.internal.read().await;

        internal.primary.clone()
    }

    /// Returns the worker with the provided id. If not found then a panic
    /// is raised instead. If the worker is stopped and started again then
    /// the worker will need to be fetched again via this method.
    pub async fn worker(&self, id: WorkerId) -> WorkerNodeDetails {
        let internal = self.internal.read().await;

        internal
            .workers
            .get(&id)
            .unwrap_or_else(|| panic!("Worker with id {} not found ", id))
            .clone()
    }

    /// Helper method to return transaction addresses of
    /// all the worker nodes.
    /// Important: only the addresses of the running workers will
    /// be returned.
    pub async fn worker_transaction_addresses(&self) -> Vec<Multiaddr> {
        self.workers().await.iter().map(|w| w.transactions_address.clone()).collect()
    }

    /// Returns all the running workers
    async fn workers(&self) -> Vec<WorkerNodeDetails> {
        let internal = self.internal.read().await;
        let mut workers = Vec::new();

        for worker in internal.workers.values() {
            if worker.is_running().await {
                workers.push(worker.clone());
            }
        }

        workers
    }

    /// Creates a new proposer client that connects to the corresponding client.
    /// This should be available only if the internal consensus is disabled. If
    /// the internal consensus is enabled then a panic will be thrown instead.
    pub async fn new_proposer_client(&self) -> ProposerClient<Channel> {
        let internal = self.internal.read().await;

        if internal.primary.internal_consensus_enabled {
            panic!("External consensus is disabled, won't create a proposer client");
        }

        let config = consensus_network::config::Config {
            connect_timeout: Some(Duration::from_secs(10)),
            request_timeout: Some(Duration::from_secs(10)),
            ..Default::default()
        };
        let channel = config
            .connect_lazy(&internal.primary.parameters.consensus_api_grpc.socket_addr)
            .unwrap();

        ProposerClient::new(channel)
    }

    /// This method returns a new client to send transactions to the dictated
    /// worker identified by the `worker_id`. If the worker_id is not found then
    /// a panic is raised.
    pub async fn new_transactions_client(
        &self,
        worker_id: &WorkerId,
    ) -> TransactionsClient<Channel> {
        let internal = self.internal.read().await;

        let config = consensus_network::config::Config::new();
        let channel = config
            .connect_lazy(&internal.workers.get(worker_id).unwrap().transactions_address)
            .unwrap();

        TransactionsClient::new(channel)
    }

    /// Creates a new configuration client that connects to the corresponding client.
    /// This should be available only if the internal consensus is disabled. If
    /// the internal consensus is enabled then a panic will be thrown instead.
    pub async fn new_configuration_client(&self) -> ConfigurationClient<Channel> {
        let internal = self.internal.read().await;

        if internal.primary.internal_consensus_enabled {
            panic!("External consensus is disabled, won't create a configuration client");
        }

        let config = consensus_network::config::Config::new();
        let channel = config
            .connect_lazy(&internal.primary.parameters.consensus_api_grpc.socket_addr)
            .unwrap();

        ConfigurationClient::new(channel)
    }

    /// This method will return true either when the primary or any of
    /// the workers is running. In order to make sure that we don't end up
    /// in intermediate states we want to make sure that everything has
    /// stopped before we report something as not running (in case we want
    /// to start them again).
    pub(super) async fn is_running(&self) -> bool {
        let internal = self.internal.read().await;

        if internal.primary.is_running().await {
            return true
        }

        for (_, worker) in internal.workers.iter() {
            if worker.is_running().await {
                return true
            }
        }
        false
    }
}
