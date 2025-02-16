// SPDX-License-Identifier: Apache-2.0
//! Network messages for anemo communication

pub mod local;
mod notify;
mod request;
mod response;
pub mod retry;
use libp2p::PeerId;
pub use notify::*;
pub use request::*;
pub use response::*;
use tn_types::NetworkPublicKey;

// async_trait for object safety, get rid of when possible.
#[async_trait::async_trait]
pub trait WorkerToPrimaryClient: Send + Sync + 'static {
    async fn report_own_batch(&self, request: WorkerOwnBatchMessage) -> eyre::Result<()>;

    async fn report_others_batch(&self, request: WorkerOthersBatchMessage) -> eyre::Result<()>;
}

/// Dumb mock to just return Ok on calls for tests.
pub struct MockWorkerToPrimary();

#[async_trait::async_trait]
impl WorkerToPrimaryClient for MockWorkerToPrimary {
    async fn report_own_batch(&self, _request: WorkerOwnBatchMessage) -> eyre::Result<()> {
        Ok(())
    }

    async fn report_others_batch(&self, _request: WorkerOthersBatchMessage) -> eyre::Result<()> {
        Ok(())
    }
}

/// Dumb mock to just pends forever on calls for tests.
pub struct MockWorkerToPrimaryHang();

#[async_trait::async_trait]
impl WorkerToPrimaryClient for MockWorkerToPrimaryHang {
    async fn report_own_batch(&self, _request: WorkerOwnBatchMessage) -> eyre::Result<()> {
        std::future::pending().await
    }

    async fn report_others_batch(&self, _request: WorkerOthersBatchMessage) -> eyre::Result<()> {
        std::future::pending().await
    }
}

// async_trait for object safety, get rid of when possible.
#[async_trait::async_trait]
pub trait PrimaryToWorkerClient: Send + Sync + 'static {
    async fn synchronize(
        &self,
        worker_name: NetworkPublicKey,
        message: WorkerSynchronizeMessage,
    ) -> eyre::Result<()>;

    async fn fetch_batches(
        &self,
        worker_name: NetworkPublicKey,
        request: FetchBatchesRequest,
    ) -> eyre::Result<FetchBatchResponse>;
}

/// Convert an existing NetworkPublicKey into a libp2p PeerId.
pub fn network_public_key_to_libp2p(fastcrypto: &NetworkPublicKey) -> PeerId {
    let bytes = fastcrypto.as_ref().to_vec();
    let ed_public_key = libp2p::identity::ed25519::PublicKey::try_from_bytes(&bytes)
        .expect("invalid public key, not able to convert to peer id!");
    libp2p::PeerId::from_public_key(&ed_public_key.into())
}
