// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Implementations for peer-to-peer WAN connections.
//! 
//! Workers, Primary, and Engine are currently communicate
//! through channel implementations of network traits.
use crate::{
    traits::{
        PrimaryToPrimaryRpc, ReliableNetwork, WorkerRpc,
    },
    CancelOnDropHandler, RetryConfig,
};
use anemo::PeerId;
use anyhow::{format_err, Result};
use async_trait::async_trait;
use std::time::Duration;
use tn_types::consensus::{
    crypto::NetworkPublicKey, Batch, BatchDigest,
};
use tn_network_types::{
    FetchCertificatesRequest,
    FetchCertificatesResponse, GetCertificatesRequest, GetCertificatesResponse,
    PrimaryToPrimaryClient, PrimaryToWorkerClient, RequestBatchRequest, RequestBatchesRequest,
    RequestBatchesResponse, WorkerBatchMessage,
    WorkerSynchronizeMessage, WorkerToWorkerClient, BuildHeaderRequest, HeaderPayloadResponse,
    PrimaryToEngineClient,
};
use tokio::task::JoinHandle;

fn send<F, R, Fut>(
    network: anemo::Network,
    peer: NetworkPublicKey,
    f: F,
) -> CancelOnDropHandler<Result<anemo::Response<R>>>
where
    F: Fn(anemo::Peer) -> Fut + Send + Sync + 'static + Clone,
    R: Send + Sync + 'static + Clone,
    Fut: std::future::Future<Output = Result<anemo::Response<R>, anemo::rpc::Status>> + Send,
{
    // Safety:
    // Since this spawns an unbounded task, this should be called in a time-restricted fashion.

    let peer_id = PeerId(peer.0.to_bytes());
    let message_send = move || {
        let network = network.clone();
        let f = f.clone();

        async move {
            if let Some(peer) = network.peer(peer_id) {
                f(peer).await.map_err(|e| {
                    // this returns a backoff::Error::Transient
                    // so that if anemo::Status is returned, we retry
                    backoff::Error::transient(anyhow::anyhow!("RPC error: {e:?}"))
                })
            } else {
                Err(backoff::Error::transient(anyhow::anyhow!("not connected to peer {peer_id}")))
            }
        }
    };

    let retry_config = RetryConfig {
        retrying_max_elapsed_time: None, // retry forever
        ..Default::default()
    };
    let task = tokio::spawn(retry_config.retry(message_send));

    CancelOnDropHandler(task)
}

//
// Primary-to-Primary
//
#[async_trait]
impl PrimaryToPrimaryRpc for anemo::Network {
    // this is the same as primary's Certifier::request_vote()
    // and Synchronizer::push_certificates()
    //
    // except, this doesn't use `waiting_peer`
    // the others do. why?
    async fn get_certificates(
        &self,
        peer: &NetworkPublicKey,
        request: impl anemo::types::request::IntoRequest<GetCertificatesRequest> + Send,
    ) -> Result<GetCertificatesResponse> {
        let peer_id = PeerId(peer.0.to_bytes());
        let peer = self
            .peer(peer_id)
            .ok_or_else(|| format_err!("Network has no connection with peer {peer_id}"))?;
        let response = PrimaryToPrimaryClient::new(peer)
            .get_certificates(request)
            .await
            .map_err(|e| format_err!("Network error {:?}", e))?;
        Ok(response.into_body())
    }

    async fn fetch_certificates(
        &self,
        peer: &NetworkPublicKey,
        request: impl anemo::types::request::IntoRequest<FetchCertificatesRequest> + Send,
    ) -> Result<FetchCertificatesResponse> {
        let peer_id = PeerId(peer.0.to_bytes());
        let peer = self
            .peer(peer_id)
            .ok_or_else(|| format_err!("Network has no connection with peer {peer_id}"))?;
        let response = PrimaryToPrimaryClient::new(peer)
            .fetch_certificates(request)
            .await
            .map_err(|e| format_err!("Network error {:?}", e))?;
        Ok(response.into_body())
    }
}

// TODO: refactor this with new approach
// - see primary's certifier `request_vote()`
//
// Worker-to-Worker
//
impl ReliableNetwork<WorkerBatchMessage> for anemo::Network {
    type Response = ();
    fn send(
        &self,
        peer: NetworkPublicKey,
        message: &WorkerBatchMessage,
    ) -> CancelOnDropHandler<Result<anemo::Response<()>>> {
        let message = message.to_owned();
        let f = move |peer| {
            let message = message.clone();
            async move { WorkerToWorkerClient::new(peer).report_batch(message).await }
        };

        send(self.clone(), peer, f)
    }
}

#[async_trait]
impl WorkerRpc for anemo::Network {
    async fn request_batch(
        &self,
        peer: NetworkPublicKey,
        batch: BatchDigest,
    ) -> Result<Option<Batch>> {
        const BATCH_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

        let peer_id = PeerId(peer.0.to_bytes());

        let peer = self
            .peer(peer_id)
            .ok_or_else(|| format_err!("Network has no connection with peer {peer_id}"))?;
        let request =
            anemo::Request::new(RequestBatchRequest { batch }).with_timeout(BATCH_REQUEST_TIMEOUT);
        let response = WorkerToWorkerClient::new(peer)
            .request_batch(request)
            .await
            .map_err(|e| format_err!("Network error {:?}", e))?;
        Ok(response.into_body().batch)
    }

    async fn request_batches(
        &self,
        peer: NetworkPublicKey,
        request: impl anemo::types::request::IntoRequest<RequestBatchesRequest> + Send,
    ) -> Result<RequestBatchesResponse> {
        let peer_id = PeerId(peer.0.to_bytes());
        let peer = self
            .peer(peer_id)
            .ok_or_else(|| format_err!("Network has no connection with peer {peer_id}"))?;
        let response = WorkerToWorkerClient::new(peer)
            .request_batches(request)
            .await
            .map_err(|e| format_err!("Network error {:?}", e))?;
        Ok(response.into_body())
    }
}

// #[async_trait]
// impl PrimaryToEngineRpc for anemo::Network {
//     async fn build_header(
//         &self,
//         peer: NetworkPublicKey,
//         request: BuildHeaderRequest,
//     ) -> Result<HeaderPayloadResponse> {
//         let peer_id = PeerId(peer.0.to_bytes());
//         let peer = self
//             .peer(peer_id)
//             .ok_or_else(|| format_err!("Network has no connection with peer {peer_id}"))?;
//         let request = anemo::Request::new(request);
//         let response = PrimaryToEngineClient::new(peer)
//             .build_header(request)
//             .await
//             .map_err(|e| format_err!("DeleteBatches error: {e:?}"))?;
//         Ok(response.into_body())
//     }
// }
