// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Handlers for how a worker should handle incoming messages from this node's engine.
//! 
//! The engine may be missing batches while trying to execute a block for the primary.
//! If a batch is missing, the engine requests the batch from the worker. The incoming
//! request is handled by this handler.
use anemo::Network;
use anyhow::Result;
use async_trait::async_trait;
use lattice_typed_store::{rocks::DBMap, Map};
use std::collections::HashMap;
use tn_types::consensus::{
    AuthorityIdentifier, Committee, WorkerCache, WorkerId,
    Batch, BatchDigest, FetchBatchesResponse,
    EngineToWorker, MissingBatchesRequest,
};

/// Defines how the worker's network receiver handles incoming primary messages.
pub struct EngineToWorkerHandler {
    // The id of this authority.
    pub authority_id: AuthorityIdentifier,
    // The id of this worker.
    pub id: WorkerId,
    // The committee information.
    pub committee: Committee,
    // The worker information cache.
    pub worker_cache: WorkerCache,
    // The batch store
    pub store: DBMap<BatchDigest, Batch>,
    // Synchronize header payloads from other workers.
    pub network: Option<Network>,
}

#[async_trait]
impl EngineToWorker for EngineToWorkerHandler {
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
