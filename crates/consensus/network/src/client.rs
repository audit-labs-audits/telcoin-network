// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Abstraction for convenience.
//! 
//! All `Client` implementations here use tokio channels.
use crate::{traits::{PrimaryToWorkerClient, WorkerToPrimaryClient, PrimaryToEngineClient, EngineToWorkerClient, WorkerToEngineClient}, LocalClientError};
use anemo::{PeerId, Request};
use async_trait::async_trait;
use futures::channel::oneshot;
use lattice_common::sync::notify_once::NotifyOnce;
use parking_lot::RwLock;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tn_types::consensus::{crypto::{traits::KeyPair, NetworkKeyPair, NetworkPublicKey}, WorkerId, BatchAPI, Batch};
use tn_network_types::{
    FetchBatchesRequest, FetchBatchesResponse, PrimaryToWorker, WorkerOthersBatchMessage,
    WorkerOwnBatchMessage, WorkerSynchronizeMessage, WorkerToPrimary, BuildHeaderRequest, HeaderPayloadResponse,
    PrimaryToEngine, EngineToWorker, MissingBatchesRequest, WorkerToEngine, SealBatchRequest, SealedBatchResponse, ValidateBatchRequest,
};
use tokio::{select, time::sleep};

/// NetworkClient provides the interface to send requests to other nodes, and call other components
/// directly if they live in the same process. It is used by both primary and worker(s).
///
/// Currently this only supports local direct calls, and it will be extended to support remote
/// network calls.
///
/// TODO: investigate splitting this into Primary and Worker specific clients.
#[derive(Clone)]
pub struct NetworkClient {
    inner: Arc<RwLock<Inner>>,
    shutdown_notify: Arc<NotifyOnce>,
}

struct Inner {
    // The private-public network key pair of this authority.
    primary_peer_id: PeerId,
    engine_peer_id: PeerId,
    worker_to_primary_handler: Option<Arc<dyn WorkerToPrimary>>,
    primary_to_worker_handler: BTreeMap<PeerId, Arc<dyn PrimaryToWorker>>,
    primary_to_engine_handler: Option<Arc<dyn PrimaryToEngine>>,
    // TODO: use PeerId here for WAN
    // for now, communication is local and only one worker
    // id is hardcoded to 0
    engine_to_worker_handler: BTreeMap<WorkerId, Arc<dyn EngineToWorker>>,
    // TODO: this should be the same handle as primary to engine,
    // need to use a generic, but the refactor will be painful
    // for now, leave them as separate
    worker_to_engine_handler: Option<Arc<dyn WorkerToEngine>>,
    shutdown: bool,
}

impl NetworkClient {
    const GET_CLIENT_RETRIES: usize = 50;
    const GET_CLIENT_INTERVAL: Duration = Duration::from_millis(100);

    pub fn new(primary_peer_id: PeerId, engine_peer_id: PeerId) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                primary_peer_id,
                engine_peer_id,
                worker_to_primary_handler: None,
                primary_to_worker_handler: BTreeMap::new(),
                primary_to_engine_handler: None,
                engine_to_worker_handler: BTreeMap::new(),
                worker_to_engine_handler: None,
                shutdown: false,
            })),
            shutdown_notify: Arc::new(NotifyOnce::new()),
        }
    }

    pub fn new_from_keypair(
        primary_network_keypair: &NetworkKeyPair,
        engine_public_network_key: &NetworkPublicKey,
    ) -> Self {
        Self::new(
            PeerId(primary_network_keypair.public().0.into()),
            PeerId(engine_public_network_key.0.into()),
        )
    }

    pub fn new_with_empty_id() -> Self {
        // ED25519_PUBLIC_KEY_LENGTH is 32 bytes.
        Self::new(
            empty_peer_id(),
            empty_peer_id(),
        )
    }

    pub fn set_worker_to_primary_local_handler(&self, handler: Arc<dyn WorkerToPrimary>) {
        let mut inner = self.inner.write();
        inner.worker_to_primary_handler = Some(handler);
    }

    pub fn set_primary_to_worker_local_handler(
        &self,
        worker_id: PeerId,
        handler: Arc<dyn PrimaryToWorker>,
    ) {
        let mut inner = self.inner.write();
        inner.primary_to_worker_handler.insert(worker_id, handler);
    }

    pub fn shutdown(&self) {
        let mut inner = self.inner.write();
        if inner.shutdown {
            return
        }
        inner.worker_to_primary_handler = None;
        inner.primary_to_worker_handler = BTreeMap::new();
        inner.shutdown = true;
        let _ = self.shutdown_notify.notify();
    }

    async fn get_primary_to_worker_handler(
        &self,
        peer_id: PeerId,
    ) -> Result<Arc<dyn PrimaryToWorker>, LocalClientError> {
        for _ in 0..Self::GET_CLIENT_RETRIES {
            {
                let inner = self.inner.read();
                if inner.shutdown {
                    return Err(LocalClientError::ShuttingDown)
                }
                if let Some(handler) = inner.primary_to_worker_handler.get(&peer_id) {
                    return Ok(handler.clone())
                }
            }
            sleep(Self::GET_CLIENT_INTERVAL).await;
        }
        Err(LocalClientError::WorkerNotStarted(peer_id))
    }

    async fn get_worker_to_primary_handler(
        &self,
    ) -> Result<Arc<dyn WorkerToPrimary>, LocalClientError> {
        for _ in 0..Self::GET_CLIENT_RETRIES {
            {
                let inner = self.inner.read();
                if inner.shutdown {
                    return Err(LocalClientError::ShuttingDown)
                }
                if let Some(handler) = &inner.worker_to_primary_handler {
                    return Ok(handler.clone())
                }
            }
            sleep(Self::GET_CLIENT_INTERVAL).await;
        }
        Err(LocalClientError::PrimaryNotStarted(self.inner.read().primary_peer_id))
    }

    pub fn set_primary_to_engine_local_handler(&self, handler: Arc<dyn PrimaryToEngine>) {
        let mut inner = self.inner.write();
        inner.primary_to_engine_handler = Some(handler);
    }

    async fn get_primary_to_engine_handler(
        &self,
    ) -> Result<Arc<dyn PrimaryToEngine>, LocalClientError> {
        for _ in 0..Self::GET_CLIENT_RETRIES {
            {
                let inner = self.inner.read();
                if inner.shutdown {
                    return Err(LocalClientError::ShuttingDown)
                }
                if let Some(handler) = &inner.primary_to_engine_handler {
                    return Ok(handler.clone())
                }
            }
            sleep(Self::GET_CLIENT_INTERVAL).await;
        }
        Err(LocalClientError::EngineNotStarted(self.inner.read().engine_peer_id))
    }

    pub fn set_engine_to_worker_local_handler(
        &self,
        // worker_id: PeerId,
        worker_id: WorkerId,
        handler: Arc<dyn EngineToWorker>,
    ) {
        let mut inner = self.inner.write();
        inner.engine_to_worker_handler.insert(worker_id, handler);
    }

    async fn get_engine_to_worker_handler(
        &self,
        // peer_id: PeerId,
        worker_id: WorkerId,
    ) -> Result<Arc<dyn EngineToWorker>, LocalClientError> {
        for _ in 0..Self::GET_CLIENT_RETRIES {
            {
                let inner = self.inner.read();
                if inner.shutdown {
                    return Err(LocalClientError::ShuttingDown)
                }
                if let Some(handler) = inner.engine_to_worker_handler.get(&worker_id) {
                    return Ok(handler.clone())
                }
            }
            sleep(Self::GET_CLIENT_INTERVAL).await;
        }
        Err(LocalClientError::WorkerNotStartedById(worker_id))
    }

    pub fn set_worker_to_engine_local_handler(
        &self,
        handler: Arc<dyn WorkerToEngine>,
    ) {
        let mut inner = self.inner.write();
        inner.worker_to_engine_handler = Some(handler);
    }

    async fn get_worker_to_engine_handler(
        &self,
    ) -> Result<Arc<dyn WorkerToEngine>, LocalClientError> {
        for _ in 0..Self::GET_CLIENT_RETRIES {
            {
                let inner = self.inner.read();
                if inner.shutdown {
                    return Err(LocalClientError::ShuttingDown)
                }
                if let Some(handler) = &inner.worker_to_engine_handler {
                    return Ok(handler.clone())
                }
            }
            sleep(Self::GET_CLIENT_INTERVAL).await;
        }
        Err(LocalClientError::EngineNotStarted(self.inner.read().engine_peer_id))
    }
}

// TODO: extract common logic for cancelling on shutdown.

#[async_trait]
impl PrimaryToWorkerClient for NetworkClient {
    async fn synchronize(
        &self,
        worker_name: NetworkPublicKey,
        request: WorkerSynchronizeMessage,
    ) -> Result<(), LocalClientError> {
        let c = self.get_primary_to_worker_handler(PeerId(worker_name.0.into())).await?;
        select! {
            resp = c.synchronize(Request::new(request)) => {
                resp.map_err(|e| LocalClientError::Internal(format!("{e:?}")))?;
                Ok(())
            },
            () = self.shutdown_notify.wait() => {
                Err(LocalClientError::ShuttingDown)
            },
        }
    }

    async fn fetch_batches(
        &self,
        worker_name: NetworkPublicKey,
        request: FetchBatchesRequest,
    ) -> Result<FetchBatchesResponse, LocalClientError> {
        let c = self.get_primary_to_worker_handler(PeerId(worker_name.0.into())).await?;
        select! {
            resp = c.fetch_batches(Request::new(request)) => {
                Ok(resp.map_err(|e| LocalClientError::Internal(format!("{e:?}")))?.into_inner())
            },
            () = self.shutdown_notify.wait() => {
                Err(LocalClientError::ShuttingDown)
            },
        }
    }
}

#[async_trait]
impl WorkerToPrimaryClient for NetworkClient {
    async fn report_own_batch(
        &self,
        request: WorkerOwnBatchMessage,
    ) -> Result<(), LocalClientError> {
        let c = self.get_worker_to_primary_handler().await?;
        select! {
            // this tells the primary to include this batch in it's proposed block
            resp = c.report_own_batch(Request::new(request)) => {
                resp.map_err(|e| LocalClientError::Internal(format!("{e:?}")))?;
                Ok(())
            },
            () = self.shutdown_notify.wait() => {
                Err(LocalClientError::ShuttingDown)
            },
        }
    }

    async fn report_others_batch(
        &self,
        request: WorkerOthersBatchMessage,
    ) -> Result<(), LocalClientError> {
        let c = self.get_worker_to_primary_handler().await?;
        select! {
            // this tells primary to write the batch to it's payload store
            resp = c.report_others_batch(Request::new(request)) => {
                resp.map_err(|e| LocalClientError::Internal(format!("{e:?}")))?;
                Ok(())
            },
            () = self.shutdown_notify.wait() => {
                Err(LocalClientError::ShuttingDown)
            },
        }
    }
}

#[async_trait]
impl PrimaryToEngineClient for NetworkClient {
    async fn build_header(
        &self,
        request: BuildHeaderRequest,
    ) -> Result<HeaderPayloadResponse, LocalClientError> {
        // note: this is set by the node manager on .start()
        let c = self.get_primary_to_engine_handler().await?;
        select! {
            // this tells EL to build a block using the txs in each batch digest
            resp = c.build_header(Request::new(request)) => {
                Ok(resp.map_err(|e| LocalClientError::Internal(format!("{e:?}")))?.into_inner())
            },
            () = self.shutdown_notify.wait() => {
                Err(LocalClientError::ShuttingDown)
            },
        }
    }
}

#[async_trait]
impl EngineToWorkerClient for NetworkClient {
    async fn seal_batch(
        &self,
        worker_id: WorkerId,
        request: SealBatchRequest,
    ) -> Result<SealedBatchResponse, LocalClientError> {
        let c = self.get_engine_to_worker_handler(worker_id).await?;
        select! {
            resp = c.seal_batch(Request::new(request)) => {
                Ok(resp.map_err(|e| LocalClientError::Internal(format!("{e:?}")))?.into_inner())
            },
            () = self.shutdown_notify.wait() => {
                Err(LocalClientError::ShuttingDown)
            },
        }
    }
    async fn missing_batches(
        &self,
        worker_id: WorkerId,
        request: MissingBatchesRequest,
    ) -> Result<FetchBatchesResponse, LocalClientError> {
        let c = self.get_engine_to_worker_handler(worker_id).await?;
        select! {
            resp = c.missing_batches(Request::new(request)) => {
                Ok(resp.map_err(|e| LocalClientError::Internal(format!("{e:?}")))?.into_inner())
            },
            () = self.shutdown_notify.wait() => {
                Err(LocalClientError::ShuttingDown)
            },
        }
    }
}

#[async_trait]
impl WorkerToEngineClient for NetworkClient {
    async fn build_batch(
        &self,
        worker_id: WorkerId,
    ) -> Result<(), LocalClientError> {
        let c = self.get_worker_to_engine_handler().await?;
        select! {
            resp = c.build_batch(Request::new(worker_id.into())) => {
                Ok(resp.map_err(|e| LocalClientError::Internal(format!("{e:?}")))?.into_inner())
            },
            () = self.shutdown_notify.wait() => {
                Err(LocalClientError::ShuttingDown)
            },
        }
    }

    async fn validate_batch(
        &self,
        worker_id: WorkerId,
        batch: Batch,
    ) -> Result<(), LocalClientError> {
        let c = self.get_worker_to_engine_handler().await?;
        select! {
            resp = c.validate_batch(Request::new(ValidateBatchRequest{ batch, worker_id })) => {
                Ok(resp.map_err(|e| LocalClientError::Internal(format!("{e:?}")))?.into_inner())
            },
            () = self.shutdown_notify.wait() => {
                Err(LocalClientError::ShuttingDown)
            },
        }
    }
}

fn empty_peer_id() -> PeerId {
    PeerId([0u8; 32])
}
