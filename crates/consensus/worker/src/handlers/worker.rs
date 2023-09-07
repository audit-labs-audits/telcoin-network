// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Handlers for how a worker should handle incoming messages from other workers
//! in the consensus network.
//! 
//! Workers report batches that they propose and request batches that they're missing.
//! Workers do not communicate locally and send messages through a WAN.
use anemo::types::response::StatusCode;
use anyhow::Result;
use async_trait::async_trait;
use fastcrypto::hash::Hash;
use itertools::Itertools;
use lattice_network::{client::NetworkClient, WorkerToPrimaryClient};
use lattice_typed_store::{rocks::DBMap, Map};
use tn_types::consensus::{
    WorkerId, now, Batch, BatchAPI, BatchDigest, MetadataAPI,
    RequestBatchRequest, RequestBatchResponse, RequestBatchesRequest,
    RequestBatchesResponse, WorkerBatchMessage,
    WorkerOthersBatchMessage, WorkerToWorker,
};
use crate::TransactionValidator;

/// Defines how the worker's network receiver handles incoming messages from
/// other workers.
#[derive(Clone)]
pub struct WorkerToWorkerHandler<V> {
    pub id: WorkerId,
    pub client: NetworkClient,
    pub store: DBMap<BatchDigest, Batch>,
    pub validator: V,
}

#[async_trait]
impl<V: TransactionValidator> WorkerToWorker for WorkerToWorkerHandler<V> {
    async fn report_batch(
        &self,
        request: anemo::Request<WorkerBatchMessage>,
    ) -> Result<anemo::Response<()>, anemo::rpc::Status> {
        let message = request.into_body();
        if let Err(err) = self.validator.validate_batch(&message.batch).await {
            return Err(anemo::rpc::Status::new_with_message(
                StatusCode::BadRequest,
                format!("Invalid batch: {err}"),
            ))
        }
        let digest = message.batch.digest();

        let mut batch = message.batch.clone();

        // Set received_at timestamp for remote batch.
        batch.versioned_metadata_mut().set_received_at(now());

        self.store.insert(&digest, &batch).map_err(|e| {
            anemo::rpc::Status::internal(format!("failed to write to batch store: {e:?}"))
        })?;
        self.client
            .report_others_batch(WorkerOthersBatchMessage { digest, worker_id: self.id })
            .await
            .map_err(|e| anemo::rpc::Status::internal(e.to_string()))?;
        Ok(anemo::Response::new(()))
    }

    async fn request_batch(
        &self,
        request: anemo::Request<RequestBatchRequest>,
    ) -> Result<anemo::Response<RequestBatchResponse>, anemo::rpc::Status> {
        // TODO [issue #7]: Do some accounting to prevent bad actors from monopolizing our resources
        let batch = request.into_body().batch;
        let batch = self.store.get(&batch).map_err(|e| {
            anemo::rpc::Status::internal(format!("failed to read from batch store: {e:?}"))
        })?;

        Ok(anemo::Response::new(RequestBatchResponse { batch }))
    }

    async fn request_batches(
        &self,
        request: anemo::Request<RequestBatchesRequest>,
    ) -> Result<anemo::Response<RequestBatchesResponse>, anemo::rpc::Status> {
        const MAX_REQUEST_BATCHES_RESPONSE_SIZE: usize = 6_000_000;
        const BATCH_DIGESTS_READ_CHUNK_SIZE: usize = 200;

        let digests_to_fetch = request.into_body().batch_digests;
        let digests_chunks = digests_to_fetch
            .chunks(BATCH_DIGESTS_READ_CHUNK_SIZE)
            .map(|chunk| chunk.to_vec())
            .collect_vec();
        let mut batches = Vec::new();
        let mut total_size = 0;
        let mut is_size_limit_reached = false;

        for digests_chunks in digests_chunks {
            let stored_batches = self.store.multi_get(digests_chunks).map_err(|e| {
                anemo::rpc::Status::internal(format!("failed to read from batch store: {e:?}"))
            })?;

            for stored_batch in stored_batches.into_iter().flatten() {
                let batch_size = stored_batch.size();
                if total_size + batch_size <= MAX_REQUEST_BATCHES_RESPONSE_SIZE {
                    batches.push(stored_batch);
                    total_size += batch_size;
                } else {
                    is_size_limit_reached = true;
                    break
                }
            }
        }

        Ok(anemo::Response::new(RequestBatchesResponse { batches, is_size_limit_reached }))
    }
}
