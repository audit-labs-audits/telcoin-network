// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// #[cfg(test)]
// #[path = "../unit_tests/narwhal_manager_tests.rs"]
// pub mod narwhal_manager_tests;

use fastcrypto::traits::KeyPair;
use consensus_metrics::RegistryService;
use tn_types::consensus::{Committee, Epoch, Parameters, WorkerCache, WorkerId, Header};
use lattice_executor::ExecutionState;
use lattice_network::client::NetworkClient;
use tn_types::consensus::crypto::{AuthorityKeyPair, NetworkKeyPair, NetworkPublicKey};
use crate::primary_node::PrimaryNode;
use crate::worker_node::WorkerNodes;
use crate::{CertificateStoreCacheMetrics, NodeStorage};
use lattice_worker::TransactionValidator;
use prometheus::{register_int_gauge_with_registry, IntGauge, Registry};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, oneshot, mpsc};
use tn_adapters::NetworkAdapter;

/// Status of the node.
#[derive(PartialEq)]
enum Running {
    True(Epoch),
    False,
}

/// Configuration for NarwhalManager.
pub struct ManagerConfiguration {
    /// The primary's keypair.
    pub primary_keypair: AuthorityKeyPair,
    /// The network keypair.
    pub network_keypair: NetworkKeyPair,
    /// The public network key for the execution layer.
    pub engine_public_key: NetworkPublicKey,
    /// Each worker's id and the worker's network keypair
    pub worker_ids_and_keypairs: Vec<(WorkerId, NetworkKeyPair)>,
    /// The storage base path.
    pub storage_base_path: PathBuf,
    /// Parameters for running consensus.
    pub parameters: Parameters,
    /// Registry for metrics.
    pub registry_service: RegistryService,
}

/// Metrics for running starting Consensus.
pub struct NarwhalManagerMetrics {
    /// The amount of time the manager took to start.
    start_latency: IntGauge,
    /// The amount of time the manager took to shutdown.
    shutdown_latency: IntGauge,
    /// The total number of retries for starting the Primary.
    start_primary_retries: IntGauge,
    /// The total number of retries for starting all workers.
    start_worker_retries: IntGauge,
}

impl NarwhalManagerMetrics {
    /// Create a new instance of metrics for the manager.
    pub fn new(registry: &Registry) -> Self {
        Self {
            start_latency: register_int_gauge_with_registry!(
                "narwhal_manager_start_latency",
                "The latency of starting up narwhal nodes",
                registry,
            )
            .unwrap(),
            shutdown_latency: register_int_gauge_with_registry!(
                "narwhal_manager_shutdown_latency",
                "The latency of shutting down narwhal nodes",
                registry,
            )
            .unwrap(),
            start_primary_retries: register_int_gauge_with_registry!(
                "narwhal_manager_start_primary_retries",
                "The number of retries took to start narwhal primary node",
                registry
            )
            .unwrap(),
            start_worker_retries: register_int_gauge_with_registry!(
                "narwhal_manager_start_worker_retries",
                "The number of retries took to start narwhal worker node",
                registry
            )
            .unwrap(),
        }
    }
}

/// The struct containing all information for managing a node at the consensus layer.
pub struct NarwhalManager {
    /// The primary's keypair.
    primary_keypair: AuthorityKeyPair,
    /// The node's network keypair.
    network_keypair: NetworkKeyPair,
    /// The node's execution layer public key.
    engine_public_key: NetworkPublicKey,
    /// Worker ids and their network keypairs.
    worker_ids_and_keypairs: Vec<(WorkerId, NetworkKeyPair)>,
    /// The instance of this node's Primary.
    primary_node: PrimaryNode,
    /// The workers for this node's Primary.
    worker_nodes: WorkerNodes,
    /// The base path for storing consensus layer data.
    storage_base_path: PathBuf,
    /// The status of the consensus layer.
    running: Mutex<Running>,
    /// The metrics for this manager.
    metrics: NarwhalManagerMetrics,
    /// Metrics for the certificate store cache.
    store_cache_metrics: CertificateStoreCacheMetrics,
}

impl NarwhalManager {
    /// Create a new instance of [Self]
    pub fn new(config: ManagerConfiguration, metrics: NarwhalManagerMetrics) -> Self {
        // Create the Narwhal Primary with configuration
        let primary_node = PrimaryNode::new(
            config.parameters.clone(),
            config.registry_service.clone(),
        );

        // Create Narwhal Workers with configuration
        let worker_nodes =
            WorkerNodes::new(config.registry_service.clone(), config.parameters.clone());

        let store_cache_metrics =
            CertificateStoreCacheMetrics::new(&config.registry_service.default_registry());

        Self {
            primary_node,
            worker_nodes,
            primary_keypair: config.primary_keypair,
            network_keypair: config.network_keypair,
            engine_public_key: config.engine_public_key,
            worker_ids_and_keypairs: config.worker_ids_and_keypairs,
            storage_base_path: config.storage_base_path,
            running: Mutex::new(Running::False),
            metrics,
            store_cache_metrics,
        }
    }

    /// Starts the Narwhal (primary & worker(s)) - if not already running.
    /// Note: After a binary is updated with the new protocol version and the node
    /// is restarted, the protocol config does not take effect until we have a quorum
    /// of validators have updated the binary. Because of this the protocol upgrade
    /// will happen in the following epoch after quorum is reached. In this case NarwhalManager
    /// is not recreated which is why we pass protocol config in at start and not at creation.
    /// To ensure correct behavior an updated protocol config must be passed in at the
    /// start of EACH epoch.
    /// 
    /// TODO: update with epochs
    pub async fn start<State, TxValidator: TransactionValidator>(
        &self,
        committee: Committee,
        worker_cache: WorkerCache,
        execution_state: Arc<State>,
        tx_validator: TxValidator,
        engine_handle: Arc<NetworkAdapter>,
    ) where
        State: ExecutionState + Send + Sync + 'static,
    {
        let mut running = self.running.lock().await;

        if let Running::True(epoch) = *running {
            tracing::warn!(
                "Narwhal node is already Running for epoch {epoch:?} - shutdown first before starting",
            );
            return;
        }

        let now = Instant::now();

        // Create a new store
        let store_path = self.get_store_path(committee.epoch());
        let store = NodeStorage::reopen(store_path, Some(self.store_cache_metrics.clone()));

        // TODO: pass this in from method so engine, primary, and workers can have a copy
        //
        // Create a new client.
        let network_client = NetworkClient::new_from_keypair(&self.network_keypair, &self.engine_public_key);
        network_client.set_primary_to_engine_local_handler(engine_handle);

        let name = self.primary_keypair.public().clone();

        tracing::info!(
            "Starting up Narwhal for epoch {}",
            committee.epoch(),
        );

        // start primary
        const MAX_PRIMARY_RETRIES: u32 = 2;
        let mut primary_retries = 0;
        loop {
            match self
                .primary_node
                .start(
                    self.primary_keypair.copy(),
                    self.network_keypair.copy(),
                    committee.clone(),
                    worker_cache.clone(),
                    network_client.clone(),
                    &store,
                    execution_state.clone(),
                )
                .await
            {
                Ok(_) => {
                    break;
                }
                Err(e) => {
                    primary_retries += 1;
                    if primary_retries >= MAX_PRIMARY_RETRIES {
                        panic!("Unable to start Narwhal Primary: {:?}", e);
                    }
                    tracing::error!("Unable to start Narwhal Primary: {:?}, retrying", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        }

        // Start Narwhal Workers with configuration
        const MAX_WORKER_RETRIES: u32 = 2;
        let mut worker_retries = 0;
        loop {
            // Copy the config for this iteration of the loop
            let id_keypair_copy = self
                .worker_ids_and_keypairs
                .iter()
                .map(|(id, keypair)| (*id, keypair.copy()))
                .collect();

            match self
                .worker_nodes
                .start(
                    name.clone(),
                    id_keypair_copy,
                    committee.clone(),
                    worker_cache.clone(),
                    network_client.clone(),
                    &store,
                    tx_validator.clone(),
                )
                .await
            {
                Ok(_) => {
                    break;
                }
                Err(e) => {
                    worker_retries += 1;
                    if worker_retries >= MAX_WORKER_RETRIES {
                        panic!("Unable to start Narwhal Worker: {:?}", e);
                    }
                    tracing::error!("Unable to start Narwhal Worker: {:?}, retrying", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        }

        tracing::info!(
            "Starting up Narwhal for epoch {} is complete - took {} seconds",
            committee.epoch(),
            now.elapsed().as_secs_f64()
        );

        self.metrics
            .start_latency
            .set(now.elapsed().as_secs_f64() as i64);

        self.metrics
            .start_primary_retries
            .set(primary_retries as i64);
        self.metrics.start_worker_retries.set(worker_retries as i64);

        *running = Running::True(committee.epoch());
    }

    /// Shuts down whole Narwhal (primary & worker(s)) and waits until nodes
    /// have shutdown.
    pub async fn shutdown(&self) {
        let mut running = self.running.lock().await;

        match *running {
            Running::True(epoch) => {
                let now = Instant::now();
                tracing::info!(
                    "Shutting down Narwhal for epoch {epoch:?}"
                );

                self.primary_node.shutdown().await;
                self.worker_nodes.shutdown().await;

                tracing::info!(
                    "Narwhal shutdown for epoch {epoch:?} is complete - took {} seconds",
                    now.elapsed().as_secs_f64()
                );

                self.metrics
                    .shutdown_latency
                    .set(now.elapsed().as_secs_f64() as i64);
            }
            Running::False => {
                tracing::info!(
                    "Narwhal Manager shutdown was called but Narwhal node is not running"
                );
            }
        }

        *running = Running::False;
    }

    /// Get the store path for the requested epoch.
    fn get_store_path(&self, epoch: Epoch) -> PathBuf {
        let mut store_path = self.storage_base_path.clone();
        store_path.push(format!("{}", epoch));
        store_path
    }

    /// Get the root base path for storage.
    pub fn get_storage_base_path(&self) -> PathBuf {
        self.storage_base_path.clone()
    }
}


#[cfg(test)]
mod tests {
    use std::time::Duration;
    use bytes::Bytes;
    use consensus_metrics::metered_channel::channel_with_total;
    use fastcrypto::{bls12381, traits::KeyPair};
    use lattice_test_utils::{CommitteeFixture, temp_dir};
    use lattice_worker::TrivialTransactionValidator;
    use tn_types::consensus::{ConsensusOutput, BatchAPI, TransactionsClient, TransactionProto};
    use tokio::{sync::broadcast, time::{interval, sleep}};

    use super::*;

    #[derive(Clone)]
    struct NoOpExecutionState {
        epoch: Epoch,
    }

    #[async_trait::async_trait]
    impl ExecutionState for NoOpExecutionState {
        async fn handle_consensus_output(&self, consensus_output: ConsensusOutput) {
            for (_, batches) in consensus_output.batches {
                for batch in batches {
                    for transaction in batch.transactions().iter() {
                        assert_eq!(
                            transaction.clone(),
                            Bytes::from(self.epoch.to_be_bytes().to_vec())
                        );
                    }
                }
            }
        }

        async fn last_executed_sub_dag_index(&self) -> u64 {
            0
        }
    }

    async fn send_transactions(
        name: bls12381::min_sig::BLS12381PublicKey,
        worker_cache: WorkerCache,
        epoch: Epoch,
        mut rx_shutdown: broadcast::Receiver<()>,
    ) {
        let target = worker_cache
            .worker(&name, /* id */ &0)
            .expect("Our key or worker id is not in the worker cache")
            .transactions;
        let config = consensus_network::config::Config::new();

        println!("sending transactions to target: {:?}", target);
        let channel = config.connect_lazy(&target).unwrap();
        let mut client = TransactionsClient::new(channel);
        // Make a transaction to submit forever.
        let tx = TransactionProto {
            transaction: Bytes::from(epoch.to_be_bytes().to_vec()),
        };
        // Repeatedly send transactions.
        let interval = interval(Duration::from_millis(1));

        tokio::pin!(interval);
        let mut succeeded_once = false;
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Send a transactions.
                    let result = client.submit_transaction(tx.clone()).await;
                    if result.is_ok() {
                        succeeded_once = true;
                    }

                },
                _ = rx_shutdown.recv() => {
                    break
                }
            }
        }
        assert!(succeeded_once);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_narwhal_manager() {
        let fixture = CommitteeFixture::builder().build();
        let committee = fixture.committee();
        let mut managers = Vec::new();
        let mut shutdown_senders = Vec::new();
        let execution_state = Arc::new(NoOpExecutionState { epoch: Epoch::default() });

        for authority in fixture.authorities() {
            // config requirements
            let registry_service = RegistryService::new(Registry::new());
            let storage_base_path = temp_dir();
            let metrics = NarwhalManagerMetrics::new(&Registry::new());

            // TODO: what is the best way to pass this?
            let engine_public_key = authority.engine_network_keypair().public().to_owned();

            // create manager config
            let config = ManagerConfiguration {
                primary_keypair: authority.keypair().copy(),
                network_keypair: authority.network_keypair(),
                engine_public_key,
                worker_ids_and_keypairs: vec![(0, authority.network_keypair())],
                storage_base_path,
                parameters: Parameters {
                    header_num_of_batches_threshold: 1,
                    max_header_delay: Duration::from_millis(200),
                    min_header_delay: Duration::from_millis(200),
                    batch_size: 1,
                    max_batch_delay: Duration::from_millis(200),
                    ..Default::default()
                },
                registry_service,
            };

            // start the manager
            let manager = NarwhalManager::new(config, metrics);
            let (sender, _receiver) = tokio::sync::mpsc::channel(1);
            manager.start(
                committee.clone(),
                fixture.worker_cache().clone(),
                execution_state.clone(),
                TrivialTransactionValidator::default(),
                sender,
            ).await;
            
            let name = authority.keypair().public().clone();
            managers.push((name.clone(), manager));

            // Send some transactions
            let (tx_shutdown, rx_shutdown) = broadcast::channel(1);
            let worker_cache = fixture.worker_cache();
            let epoch = committee.epoch();
            tokio::spawn(async move {
                send_transactions(
                    name,
                    worker_cache,
                    epoch,
                    rx_shutdown,
                )
                .await
            });
            shutdown_senders.push(tx_shutdown);
        }

        sleep(Duration::from_secs(1)).await;
        for tr_shutdown in shutdown_senders {
            _ = tr_shutdown.send(());
        }
        let mut shutdown_senders = Vec::new();

        for (name, manager) in managers {
            // stop narwhal instance
            manager.shutdown().await;

            // ensure that no primary or worker node is running
            assert!(!manager.primary_node.is_running().await);
            assert!(manager
                .worker_nodes
                .workers_running()
                .await
                .is_empty());

            let (sender, _receiver) = tokio::sync::mpsc::channel(1);
            // start the manager
            manager.start(
                committee.clone(),
                fixture.worker_cache().clone(),
                execution_state.clone(),
                TrivialTransactionValidator::default(),
                sender,
            ).await;

            // Send some transactions
            let (tx_shutdown, rx_shutdown) = broadcast::channel(1);
            let worker_cache = fixture.worker_cache();
            // TODO: advance epoch
            let epoch = committee.epoch();
            tokio::spawn(async move {
                send_transactions(
                    name,
                    worker_cache,
                    epoch,
                    rx_shutdown,
                )
                .await
            });
            shutdown_senders.push(tx_shutdown);
        }

        sleep(Duration::from_secs(5)).await;
        for tr_shutdown in shutdown_senders {
            _ = tr_shutdown.send(());
        } 
    }

}
