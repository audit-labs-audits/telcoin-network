// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Primary components for running a [`Cluster`].
use crate::temp_dir;
use consensus_metrics::{RegistryService, metered_channel::channel_with_total};
use fastcrypto::traits::KeyPair as _;
use lattice_executor::SerializedTransaction;
use lattice_network::client::NetworkClient;
use lattice_node::{
    execution_state::SimpleExecutionState, primary_node::PrimaryNode,
};
use lattice_storage::NodeStorage;
use prometheus::{proto::Metric, Registry};
use std::{cell::RefCell,  path::PathBuf, rc::Rc, sync::Arc};
use tn_types::consensus::{
    AuthorityIdentifier, Committee, Parameters, WorkerCache,
    crypto::{AuthorityKeyPair, NetworkKeyPair},
};
use tokio::{
    sync::{broadcast::Sender, mpsc::channel},
    task::JoinHandle,
};
use tracing::info;

#[derive(Clone)]
pub struct PrimaryNodeDetails {
    pub id: usize,
    pub name: AuthorityIdentifier,
    pub key_pair: Arc<AuthorityKeyPair>,
    pub network_key_pair: Arc<NetworkKeyPair>,
    pub tx_transaction_confirmation: Sender<SerializedTransaction>,
    node: PrimaryNode,
    store_path: PathBuf,
    pub(super) parameters: Parameters,
    committee: Committee,
    worker_cache: WorkerCache,
    handlers: Rc<RefCell<Vec<JoinHandle<()>>>>,
    pub(super) internal_consensus_enabled: bool,
}

impl PrimaryNodeDetails {
    pub(super) fn new(
        id: usize,
        name: AuthorityIdentifier,
        key_pair: AuthorityKeyPair,
        network_key_pair: NetworkKeyPair,
        parameters: Parameters,
        committee: Committee,
        worker_cache: WorkerCache,
        internal_consensus_enabled: bool,
    ) -> Self {
        // used just to initialise the struct value
        let (tx, _) = tokio::sync::broadcast::channel(1);

        let registry_service = RegistryService::new(Registry::new());

        let node =
            PrimaryNode::new(parameters.clone(), internal_consensus_enabled, registry_service);

        Self {
            id,
            name,
            key_pair: Arc::new(key_pair),
            network_key_pair: Arc::new(network_key_pair),
            store_path: temp_dir(),
            tx_transaction_confirmation: tx,
            committee,
            worker_cache,
            handlers: Rc::new(RefCell::new(Vec::new())),
            internal_consensus_enabled,
            node,
            parameters,
        }
    }

    /// Returns the metric - if exists - identified by the provided name.
    /// If metric has not been found then None is returned instead.
    pub async fn metric(&self, name: &str) -> Option<Metric> {
        let (_registry_id, registry) = self.node.registry().await.unwrap();
        let metrics = registry.gather();

        let metric = metrics.into_iter().find(|m| m.get_name() == name);
        metric.map(|m| m.get_metric().first().unwrap().clone())
    }

    pub(super) async fn start(&mut self, client: NetworkClient, preserve_store: bool) {
        if self.is_running().await {
            panic!("Tried to start a node that is already running");
        }

        // Make the data store.
        let store_path = if preserve_store { self.store_path.clone() } else { temp_dir() };

        info!("Primary Node {} will use path {:?}", self.id, store_path.clone());

        // The channel returning the result for each transaction's execution.
        let (tx_transaction_confirmation, mut rx_transaction_confirmation) = channel(100);

        // Primary node
        let primary_store: NodeStorage = NodeStorage::reopen(store_path.clone(), None);

        // TODO: use this
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        self.node
            .start(
                self.key_pair.copy(),
                self.network_key_pair.copy(),
                self.committee.clone(),
                self.worker_cache.clone(),
                client,
                &primary_store,
                Arc::new(SimpleExecutionState::new(tx_transaction_confirmation)),
                sender,
            )
            .await
            .unwrap();

        let (tx, _) = tokio::sync::broadcast::channel(lattice_primary::CHANNEL_CAPACITY);
        let transactions_sender = tx.clone();
        // spawn a task to listen on the committed transactions
        // and translate to a mpmc channel
        let h = tokio::spawn(async move {
            while let Some(t) = rx_transaction_confirmation.recv().await {
                // send the transaction to the mpmc channel
                let _ = transactions_sender.send(t);
            }
        });

        // add the tasks's handle to the primary's handle so can be shutdown
        // with the others.
        self.handlers.replace(vec![h]);
        self.store_path = store_path;
        self.tx_transaction_confirmation = tx;
    }

    pub(super) async fn stop(&self) {
        self.node.shutdown().await;
        self.handlers.borrow().iter().for_each(|h| h.abort());
        info!("Aborted primary node for id {}", self.id);
    }

    /// This method returns whether the node is still running or not. We
    /// iterate over all the handlers and check whether there is still any
    /// that is not finished. If we find at least one, then we report the
    /// node as still running.
    pub async fn is_running(&self) -> bool {
        self.node.is_running().await
    }
}
