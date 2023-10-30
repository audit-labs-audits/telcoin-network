// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0
//! Handlers for how a worker should handle incoming messages from this node's engine.
//! 
//! The engine may be missing batches while trying to execute a block for the primary.
//! If a batch is missing, the engine requests the batch from the worker. The incoming
//! request is handled by this handler.
use anemo::PeerId;
use anyhow::Result;
use async_trait::async_trait;
use fastcrypto::hash::Hash;
use lattice_typed_store::{rocks::DBMap, Map};
use std::collections::HashMap;
use tn_types::consensus::{
    AuthorityIdentifier, WorkerId,
    Batch, BatchDigest,
};
use tn_network_types::{
    FetchBatchesResponse, EngineToWorker,
    MissingBatchesRequest,
    SealedBatchResponse, SealBatchRequest,
};
use consensus_metrics::metered_channel::Sender;

/// Defines how the worker's network receiver handles incoming primary messages.
pub struct EngineToWorkerHandler {
    /// The id of this authority.
    pub authority_id: AuthorityIdentifier,
    /// The id of this worker.
    pub id: WorkerId,
    /// The peer id of this worker.
    pub peer_id: PeerId,
    /// The batch store
    pub store: DBMap<BatchDigest, Batch>,
    /// Output channel to deliver built batches to the `QuorumWaiter`.
    pub tx_quorum_waiter: Sender<(Batch, BatchDigest, tokio::sync::oneshot::Sender<()>)>,
}

#[async_trait]
impl EngineToWorker for EngineToWorkerHandler {
    async fn seal_batch(
        &self,
        request: anemo::Request<SealBatchRequest>,
    ) -> Result<anemo::Response<SealedBatchResponse>, anemo::rpc::Status> {
        // TODO: include metadata in the request
        let batch: Batch = request.into_body().into();
        let digest = batch.digest();

        // Send the batch through the quorum waiter.
        let (notify_done, done_sending) = tokio::sync::oneshot::channel();
        if let Err(e) = self.tx_quorum_waiter.send((batch.clone(), digest.clone(), notify_done)).await {
            tracing::debug!("Failed to send to quroum waiter...System shutting down");
            return Err(anemo::rpc::Status::internal(e.to_string()))
        }

        // store in db
        if let Err(e) = self.store.insert(&digest, &batch) {
            tracing::error!("Store failed with error: {:?}", e);
            return Err(anemo::rpc::Status::internal(e.to_string()))
        }

        // TODO: this is an old approach and doesn't really add much
        // removed to prevent missing batches from tx pool
        //
        // // Also wait for sending to be done here
        // //
        // // TODO: Here if we get back Err it means that potentially this was not send
        // //       to a quorum. However, if that happens we can still proceed on the basis
        // //       that another authority will request the batch from us, and we will deliver
        // //       it since it is now stored. So ignore the error for the moment.
        // let _ = done_sending.await;
        let worker_id = self.id.clone();
        Ok(anemo::Response::new(
            SealedBatchResponse{ batch, digest, worker_id }
        ))
    }

    async fn missing_batches(
        &self,
        request: anemo::Request<MissingBatchesRequest>,
    ) -> Result<anemo::Response<FetchBatchesResponse>, anemo::rpc::Status> {
        let message = request.body();
        let mut batches = HashMap::new();
        for digest in message.digests.iter() {
            // Check if we already have the batch.
            match self.store.get(digest) {
                Ok(Some(batch)) => {
                    batches.insert(*digest, batch);
                }
                _ => {
                    return Err(anemo::rpc::Status::internal(format!(
                        "failed to read from batch store: {digest:?}"
                    )))
                }
            };
        }

        Ok(anemo::Response::new(FetchBatchesResponse { batches }))
    }
}
