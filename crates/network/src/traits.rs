use crate::{error::LocalClientError, CancelOnDropHandler};
use eyre::Result;
use std::future::Future;
use tn_network_types::{
    FetchBatchResponse, FetchBatchesRequest, RequestBatchesRequest, RequestBatchesResponse,
    WorkerOthersBatchMessage, WorkerOwnBatchMessage, WorkerSynchronizeMessage,
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

pub trait PrimaryToWorkerClient {
    fn synchronize(
        &self,
        worker_name: NetworkPublicKey,
        request: WorkerSynchronizeMessage,
    ) -> impl Future<Output = Result<(), LocalClientError>>;

    fn fetch_batches(
        &self,
        worker_name: NetworkPublicKey,
        request: FetchBatchesRequest,
    ) -> impl Future<Output = Result<FetchBatchResponse, LocalClientError>>;
}

// async_trait for object safety, get rid of when possible.
#[async_trait::async_trait]
pub trait WorkerToPrimaryClient: Send + Sync + 'static {
    async fn report_own_batch(
        &self,
        request: WorkerOwnBatchMessage,
    ) -> Result<(), LocalClientError>;

    async fn report_others_batch(
        &self,
        request: WorkerOthersBatchMessage,
    ) -> Result<(), LocalClientError>;
}

pub trait WorkerRpc {
    fn request_batches(
        &self,
        peer: &NetworkPublicKey,
        request: impl anemo::types::request::IntoRequest<RequestBatchesRequest> + Send,
    ) -> impl Future<Output = Result<RequestBatchesResponse>>;
}
