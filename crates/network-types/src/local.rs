//! Client implementation for local network messages between primary and worker.
use crate::{
    network_public_key_to_libp2p, FetchBatchResponse, FetchBatchesRequest, PrimaryToWorkerClient,
    WorkerOthersBatchMessage, WorkerOwnBatchMessage, WorkerSynchronizeMessage,
    WorkerToPrimaryClient,
};
use libp2p::PeerId;
use parking_lot::RwLock;
use std::{collections::BTreeMap, sync::Arc};
use tn_types::{
    traits::{InsecureDefault, KeyPair},
    NetworkKeypair, NetworkPublicKey,
};

// // //
//
// TODO: replace the `LocalNetwork` with worker/primary implementations and add engine.
// and get rid of this stupid loop crap with retry attempts. the only reason this is here
// is because LocalNetwork does too much. code doesn't know if/when primary/worker start
// so there's options and confusing logic. Just create the clients with the config on node startup.
//
// This is not so simple.
//
// // //

/// LocalNetwork provides the interface to send requests to other nodes, and call other components
/// directly if they live in the same process. It is used by both primary and worker(s).
///
/// Currently this only supports local direct calls, and it will be extended to support remote
/// network calls.
///
/// TODO: investigate splitting this into Primary and Worker specific clients.
#[derive(Debug, Clone)]
pub struct LocalNetwork {
    inner: Arc<RwLock<Inner>>,
}

struct Inner {
    /// The primary's peer id.
    primary_peer_id: PeerId,
    /// The type that holds logic for worker to primary requests.
    worker_to_primary_handler: Option<Arc<dyn WorkerToPrimaryClient>>,
    /// The type that holds logic for primary to worker requests.
    primary_to_worker_handler: BTreeMap<PeerId, Arc<dyn PrimaryToWorkerClient>>,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LocalNetwork::Inner for {}", self.primary_peer_id)
    }
}

impl LocalNetwork {
    pub fn new(primary_peer_id: PeerId) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                primary_peer_id,
                worker_to_primary_handler: None,
                primary_to_worker_handler: BTreeMap::new(),
            })),
        }
    }

    pub fn new_from_keypair(primary_network_keypair: &NetworkKeypair) -> Self {
        Self::new(network_public_key_to_libp2p(primary_network_keypair.public()))
    }

    /// Create a new [LocalNetwork] from the primary's network key.
    pub fn new_from_public_key(primary_network_public_key: &NetworkPublicKey) -> Self {
        Self::new(network_public_key_to_libp2p(primary_network_public_key))
    }

    pub fn new_with_empty_id() -> Self {
        Self::new(network_public_key_to_libp2p(&NetworkPublicKey::insecure_default()))
    }

    pub fn set_worker_to_primary_local_handler(&self, handler: Arc<dyn WorkerToPrimaryClient>) {
        let mut inner = self.inner.write();
        inner.worker_to_primary_handler = Some(handler);
    }

    pub fn set_primary_to_worker_local_handler(
        &self,
        worker_id: PeerId,
        handler: Arc<dyn PrimaryToWorkerClient>,
    ) {
        let mut inner = self.inner.write();
        inner.primary_to_worker_handler.insert(worker_id, handler);
    }

    async fn get_primary_to_worker_handler(
        &self,
        peer_id: PeerId,
    ) -> Option<Arc<dyn PrimaryToWorkerClient>> {
        let inner = self.inner.read();
        inner.primary_to_worker_handler.get(&peer_id).cloned()
    }

    async fn get_worker_to_primary_handler(&self) -> Option<Arc<dyn WorkerToPrimaryClient>> {
        let inner = self.inner.read();
        if let Some(handler) = &inner.worker_to_primary_handler {
            Some(handler.clone())
        } else {
            None
        }
    }
}

#[async_trait::async_trait]
impl PrimaryToWorkerClient for LocalNetwork {
    async fn synchronize(
        &self,
        worker_name: NetworkPublicKey,
        request: WorkerSynchronizeMessage,
    ) -> eyre::Result<()> {
        let peer_id = network_public_key_to_libp2p(&worker_name);
        if let Some(c) = self.get_primary_to_worker_handler(peer_id).await {
            c.synchronize(worker_name, request).await
        } else {
            tracing::warn!(target = "local_network", "primary to worker handler not set yet!");
            Err(eyre::eyre!("primary to worker not set yet"))
        }
    }

    async fn fetch_batches(
        &self,
        worker_name: NetworkPublicKey,
        request: FetchBatchesRequest,
    ) -> eyre::Result<FetchBatchResponse> {
        let peer_id = network_public_key_to_libp2p(&worker_name);
        if let Some(c) = self.get_primary_to_worker_handler(peer_id).await {
            c.fetch_batches(worker_name, request).await
        } else {
            tracing::warn!(target = "local_network", "primary to worker handler not set yet!");
            Err(eyre::eyre!("primary to worker not set yet"))
        }
    }
}

#[async_trait::async_trait]
impl WorkerToPrimaryClient for LocalNetwork {
    async fn report_own_batch(&self, request: WorkerOwnBatchMessage) -> eyre::Result<()> {
        if let Some(c) = self.get_worker_to_primary_handler().await {
            c.report_own_batch(request).await?;
        } else {
            tracing::warn!(target = "local_network", "working to primary handler not set yet!");
        }
        Ok(())
    }

    async fn report_others_batch(&self, request: WorkerOthersBatchMessage) -> eyre::Result<()> {
        if let Some(c) = self.get_worker_to_primary_handler().await {
            c.report_others_batch(request).await?;
        } else {
            tracing::warn!(target = "local_network", "working to primary handler not set yet!");
        }
        Ok(())
    }
}
