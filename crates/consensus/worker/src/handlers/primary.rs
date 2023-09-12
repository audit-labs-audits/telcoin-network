// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Handlers for how a worker should handle incoming messages from this node's primary.
//! 
//! The primary may be missing batches while trying to validate another primary's block.
//! If a batch is missing, the primary requests the batch from the worker. The incoming
//! request is handled by this handler.
//! 
//! If the worker doesn't have the batch digest in it's local store, it makes a remote call
//! to a peer to retrieve the batch.
use anemo::{types::response::StatusCode, Network};
use anyhow::Result;
use async_trait::async_trait;
use fastcrypto::hash::Hash;
use lattice_typed_store::{rocks::DBMap, Map};
use std::{collections::HashSet, time::Duration};
use tn_types::consensus::{
    AuthorityIdentifier, Committee, WorkerCache, WorkerId,
    now, Batch, BatchAPI, BatchDigest, MetadataAPI,
};
use tn_network_types::{
    FetchBatchesRequest, FetchBatchesResponse, 
    PrimaryToWorker, RequestBatchesRequest, WorkerSynchronizeMessage, WorkerToWorkerClient,
};
use tracing::{debug, trace};
use crate::{batch_fetcher::BatchFetcher, TransactionValidator};

/// Defines how the worker's network receiver handles incoming primary messages.
pub struct PrimaryToWorkerHandler<V> {
    /// The id of this authority.
    pub authority_id: AuthorityIdentifier,
    /// The id of this worker.
    pub id: WorkerId,
    /// The committee information.
    pub committee: Committee,
    /// The worker information cache.
    pub worker_cache: WorkerCache,
    /// The batch store
    pub store: DBMap<BatchDigest, Batch>,
    /// Timeout on RequestBatch RPC.
    pub request_batch_timeout: Duration,
    /// Number of random nodes to query when retrying batch requests.
    pub request_batch_retry_nodes: usize,
    /// Synchronize header payloads from other workers.
    pub network: Option<Network>,
    /// Fetch certificate payloads from other workers.
    pub batch_fetcher: Option<BatchFetcher>,
    /// Validate incoming batches
    pub validator: V,
}

#[async_trait]
impl<V: TransactionValidator> PrimaryToWorker for PrimaryToWorkerHandler<V> {
    async fn synchronize(
        &self,
        request: anemo::Request<WorkerSynchronizeMessage>,
    ) -> Result<anemo::Response<()>, anemo::rpc::Status> {
        let Some(network) = self.network.as_ref() else {
            return Err(anemo::rpc::Status::new_with_message(
                StatusCode::BadRequest,
                "synchronize() is unsupported via RPC interface, please call via local worker handler instead",
            ));
        };
        let message = request.body();
        let mut missing = HashSet::new();
        for digest in message.digests.iter() {
            // Check if we already have the batch.
            match self.store.get(digest) {
                Ok(None) => {
                    missing.insert(*digest);
                    debug!("Requesting sync for batch {digest}");
                }
                Ok(Some(_)) => {
                    trace!("Digest {digest} already in store, nothing to sync");
                }
                Err(e) => {
                    return Err(anemo::rpc::Status::internal(format!(
                        "failed to read from batch store: {e:?}"
                    )))
                }
            };
        }
        if missing.is_empty() {
            return Ok(anemo::Response::new(()))
        }

        let worker_name = match self
            .worker_cache
            .worker(self.committee.authority(&message.target).unwrap().protocol_key(), &self.id)
        {
            Ok(worker_info) => worker_info.name,
            Err(e) => {
                return Err(anemo::rpc::Status::internal(format!(
                    "The primary asked worker to sync with an unknown node: {e}"
                )))
            }
        };
        let Some(peer) = network.peer(anemo::PeerId(worker_name.0.to_bytes())) else {
            return Err(anemo::rpc::Status::internal(format!(
                "Not connected with worker peer {worker_name}"
            )));
        };
        let mut client = WorkerToWorkerClient::new(peer.clone());

        // Attempt to retrieve missing batches.
        // Retried at a higher level in Synchronizer::sync_batches_internal().
        let request = RequestBatchesRequest { batch_digests: missing.iter().cloned().collect() };
        debug!("Sending RequestBatchesRequest to {worker_name}: {request:?}");

        let mut response = client
            .request_batches(anemo::Request::new(request).with_timeout(self.request_batch_timeout))
            .await?
            .into_inner();
        for batch in response.batches.iter_mut() {
            if !message.is_certified {
                // This batch is not part of a certificate, so we need to validate it.
                if let Err(err) = self.validator.validate_batch(batch).await {
                    return Err(anemo::rpc::Status::new_with_message(
                        StatusCode::BadRequest,
                        format!("Invalid batch: {err}"),
                    ))
                }
            }

            let digest = batch.digest();
            if missing.remove(&digest) {
                // Set received_at timestamp for remote batch.
                batch.versioned_metadata_mut().set_received_at(now());

                self.store.insert(&digest, batch).map_err(|e| {
                    anemo::rpc::Status::internal(format!("failed to write to batch store: {e:?}"))
                })?;
            }
        }

        if missing.is_empty() {
            return Ok(anemo::Response::new(()))
        }
        Err(anemo::rpc::Status::internal("failed to synchronize batches!"))
    }

    async fn fetch_batches(
        &self,
        request: anemo::Request<FetchBatchesRequest>,
    ) -> Result<anemo::Response<FetchBatchesResponse>, anemo::rpc::Status> {
        let Some(batch_fetcher) = self.batch_fetcher.as_ref() else {
            return Err(anemo::rpc::Status::new_with_message(
                StatusCode::BadRequest,
                "fetch_batches() is unsupported via RPC interface, please call via local worker handler instead",
            ));
        };
        let request = request.into_body();
        let batches = batch_fetcher.fetch(request.digests, request.known_workers).await;
        Ok(anemo::Response::new(FetchBatchesResponse { batches }))
    }

}
