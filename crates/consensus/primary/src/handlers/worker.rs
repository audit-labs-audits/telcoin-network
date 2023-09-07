// Copyright (c) Telcoin, LLC
//! How the primary's network should handle incoming messages from workers.
//! Primary and workers only communicate locally for now.
use async_trait::async_trait;
use consensus_metrics::metered_channel::Sender;
use lattice_storage::PayloadStore;
use tn_types::consensus::{WorkerToPrimary, WorkerOwnBatchMessage, MetadataAPI, WorkerOthersBatchMessage};
use tokio::sync::oneshot;
use crate::proposer::OurDigestMessage;
/// Defines how the network receiver handles incoming workers messages.
#[derive(Clone)]
pub(crate) struct WorkerToPrimaryHandler {
    tx_our_digests: Sender<OurDigestMessage>,
    payload_store: PayloadStore,
}

impl WorkerToPrimaryHandler {
    /// Create a new instance of self.
    pub(crate) fn new(
        tx_our_digests: Sender<OurDigestMessage>, 
        payload_store: PayloadStore,
    ) -> Self {
        Self { tx_our_digests, payload_store }
    }
}

#[async_trait]
impl WorkerToPrimary for WorkerToPrimaryHandler {
    async fn report_own_batch(
        &self,
        request: anemo::Request<WorkerOwnBatchMessage>,
    ) -> Result<anemo::Response<()>, anemo::rpc::Status> {
        let message = request.into_body();

        let (tx_ack, rx_ack) = oneshot::channel();
        let response = self
            .tx_our_digests
            .send(OurDigestMessage {
                digest: message.digest,
                worker_id: message.worker_id,
                timestamp: *message.metadata.created_at(),
                ack_channel: Some(tx_ack),
            })
            .await
            .map(|_| anemo::Response::new(()))
            .map_err(|e| anemo::rpc::Status::internal(e.to_string()))?;

        // If we are ok, then wait for the ack
        rx_ack.await.map_err(|e| anemo::rpc::Status::internal(e.to_string()))?;

        Ok(response)
    }

    async fn report_others_batch(
        &self,
        request: anemo::Request<WorkerOthersBatchMessage>,
    ) -> Result<anemo::Response<()>, anemo::rpc::Status> {
        let message = request.into_body();
        self.payload_store
            .write(&message.digest, &message.worker_id)
            .map_err(|e| anemo::rpc::Status::internal(e.to_string()))?;
        Ok(anemo::Response::new(()))
    }
}
