// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{error::LocalClientError, CancelOnDropHandler};
use async_trait::async_trait;
use eyre::Result;
use narwhal_network_types::{
    FetchBlocksRequest, FetchBlocksResponse, FetchCertificatesRequest, FetchCertificatesResponse,
    RequestBlocksRequest, RequestBlocksResponse, WorkerOthersBlockMessage, WorkerOwnBlockMessage,
    WorkerSynchronizeMessage,
};
use tn_types::NetworkPublicKey;

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

#[async_trait]
pub trait PrimaryToPrimaryRpc {
    async fn fetch_certificates(
        &self,
        peer: &NetworkPublicKey,
        request: impl anemo::types::request::IntoRequest<FetchCertificatesRequest> + Send,
    ) -> Result<FetchCertificatesResponse>;
}

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
        request: FetchBlocksRequest,
    ) -> Result<FetchBlocksResponse, LocalClientError>;
}

#[async_trait]
pub trait WorkerToPrimaryClient {
    async fn report_own_block(
        &self,
        request: WorkerOwnBlockMessage,
    ) -> Result<(), LocalClientError>;

    async fn report_others_block(
        &self,
        request: WorkerOthersBlockMessage,
    ) -> Result<(), LocalClientError>;
}

#[async_trait]
pub trait WorkerRpc {
    async fn request_blocks(
        &self,
        peer: &NetworkPublicKey,
        request: impl anemo::types::request::IntoRequest<RequestBlocksRequest> + Send,
    ) -> Result<RequestBlocksResponse>;
}
