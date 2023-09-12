// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Abstraction of traits for network RPC methods.
//! 
//! xxRpc is implemented for WAN calls.
//! xxClient is implemented for local channels currently.
//! 
//! I think the goal of this abstraction was to reduce boilerplate
//! for creating peer ids + clients for WAN calls and allowed
//! impl for tokio channels between workers and primary.
use crate::{CancelOnDropHandler, LocalClientError};
use anyhow::Result;
use async_trait::async_trait;
use tn_types::consensus::{
    crypto::NetworkPublicKey, Batch, BatchDigest, WorkerId,
};
use tn_network_types::{
    FetchBatchesRequest,
    FetchBatchesResponse, FetchCertificatesRequest, FetchCertificatesResponse,
    GetCertificatesRequest, GetCertificatesResponse, RequestBatchesRequest, RequestBatchesResponse,
    WorkerOthersBatchMessage, WorkerOwnBatchMessage, WorkerSynchronizeMessage, BuildHeaderRequest,
    HeaderPayloadResponse, MissingBatchesRequest, SealBatchRequest, SealedBatchResponse,
};
use tokio::task::JoinHandle;

/// TODO: this is a legacy approach, although still used by the workers
/// to broadcast batch.
/// 
/// Refactor this to use WorkerToWorker trait for client
/// like primary certifier's `request_vote()`
pub trait ReliableNetwork<Request: Clone + Send + Sync> {
    type Response: Clone + Send + Sync;

    fn send(
        &self,
        peer: NetworkPublicKey,
        message: &Request,
    ) -> CancelOnDropHandler<Result<anemo::Response<Self::Response>>>;

    fn broadcast(
        &self,
        peers: Vec<NetworkPublicKey>,
        message: &Request,
    ) -> Vec<CancelOnDropHandler<Result<anemo::Response<Self::Response>>>> {
        let mut handlers = Vec::new();
        for peer in peers {
            let handle = self.send(peer, message);
            handlers.push(handle);
        }
        handlers
    }
}

/// P2P request
#[async_trait]
pub trait PrimaryToPrimaryRpc {
    async fn get_certificates(
        &self,
        peer: &NetworkPublicKey,
        request: impl anemo::types::request::IntoRequest<GetCertificatesRequest> + Send,
    ) -> Result<GetCertificatesResponse>;
    async fn fetch_certificates(
        &self,
        peer: &NetworkPublicKey,
        request: impl anemo::types::request::IntoRequest<FetchCertificatesRequest> + Send,
    ) -> Result<FetchCertificatesResponse>;
}

/// Only implemented for tokio channels right now
/// 
/// See [PrimaryReceiver] in worker/handlers.rs
#[async_trait]
pub trait PrimaryToWorkerClient {
    async fn synchronize(
        &self,
        worker_name: NetworkPublicKey,
        request: WorkerSynchronizeMessage,
    ) -> Result<(), LocalClientError>;

    async fn fetch_batches(
        &self,
        worker_name: NetworkPublicKey,
        request: FetchBatchesRequest,
    ) -> Result<FetchBatchesResponse, LocalClientError>;
}

/// Only implemented for tokio channels right now.
/// 
/// See [WorkerReceiverHandle] in primary/primary.rs
#[async_trait]
pub trait WorkerToPrimaryClient {
    /// Reports a batch that was created by this worker.
    async fn report_own_batch(
        &self,
        request: WorkerOwnBatchMessage,
    ) -> Result<(), LocalClientError>;

    /// Reports a batch that was created by a peer worker.
    async fn report_others_batch(
        &self,
        request: WorkerOthersBatchMessage,
    ) -> Result<(), LocalClientError>;
}

/// See [WorkerReceiverHandler] in worker/handlers.rs
#[async_trait]
pub trait WorkerRpc {
    async fn request_batch(
        &self,
        peer: NetworkPublicKey,
        batch: BatchDigest,
    ) -> Result<Option<Batch>>;

    async fn request_batches(
        &self,
        peer: NetworkPublicKey,
        request: impl anemo::types::request::IntoRequest<RequestBatchesRequest> + Send,
    ) -> Result<RequestBatchesResponse>;
}

/// Rpc trait for Primary to request additional information
/// after executing a block.
/// 
/// TODO: cleanup and clarify the multiple/confusing implementations
/// of methods for anemo::Network, proto, and client.
/// - NetworkClient is for local communication
/// - anemo::Network is p2p
/// - proto: mocks and p2p traits
/// 
/// This approach seems consistent with WorkerToPrimaryClient and PrimaryToWorkerClient.
/// I'm not sure this is the best way, but it will work for now.
#[async_trait]
pub trait PrimaryToEngineClient {
    /// Reports a new header for the EL engine to build.
    async fn build_header(
        &self,
        request: BuildHeaderRequest,
    ) -> Result<HeaderPayloadResponse, LocalClientError>;

    // TODO: validate peer
}

/// Engine to Worker
#[async_trait]
pub trait EngineToWorkerClient {
    /// Seal the built batch and broadcast it.
    async fn seal_batch(
        &self,
        worker_id: WorkerId,
        request: SealBatchRequest,
    ) -> Result<SealedBatchResponse, LocalClientError>;

    /// Request missing batches from worker
    async fn missing_batches(
        &self,
        // worker_name: NetworkPublicKey,
        worker_id: WorkerId,
        request: MissingBatchesRequest,
    ) -> Result<FetchBatchesResponse, LocalClientError>;
}

/// Worker to Engine
#[async_trait]
pub trait WorkerToEngineClient {
    /// Request next batch
    async fn build_batch(
        &self,
        worker_id: WorkerId
    ) -> Result<(), LocalClientError>;
}
