use itertools::Itertools;
use std::sync::Arc;
use tn_config::ConsensusConfig;
use tn_network_libp2p::GossipMessage;
use tn_network_types::{WorkerOthersBatchMessage, WorkerToPrimaryClient};
use tn_storage::tables::Batches;
use tn_types::{now, try_decode, BatchValidation, BlockHash, Database, SealedBatch, WorkerId};

use super::{
    error::{WorkerNetworkError, WorkerNetworkResult},
    message::{RequestBatchesResponse, WorkerGossip},
};

/// The type that handles requests from peers.
#[derive(Clone)]
pub struct RequestHandler<DB> {
    /// This worker's id.
    id: WorkerId,
    /// The type that validates batches received from peers.
    validator: Arc<dyn BatchValidation>,
    /// Consensus config with access to database.
    consensus_config: ConsensusConfig<DB>,
}

impl<DB> RequestHandler<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub fn new(
        id: WorkerId,
        validator: Arc<dyn BatchValidation>,
        consensus_config: ConsensusConfig<DB>,
    ) -> Self {
        Self { id, validator, consensus_config }
    }

    /// Process gossip from the committee.
    ///
    /// Peers gossip the CertificateDigest so peers can request the Certificate. This waits until
    /// the certificate can be retrieved and timesout after some time. It's important to give up
    /// after enough time to limit the DoS attack surface. Peers who timeout must lose reputation.
    pub(super) async fn process_gossip(&self, msg: &GossipMessage) -> WorkerNetworkResult<()> {
        // deconstruct message
        let GossipMessage { data, .. } = msg;

        // gossip is uncompressed
        let gossip = try_decode(data)?;

        match gossip {
            WorkerGossip::Batch(_block_hash) => {
                // Retrieve the block...
            }
        }

        Ok(())
    }

    /// Process a new reported batch.
    pub(crate) async fn process_report_batch(
        &self,
        sealed_batch: SealedBatch,
    ) -> WorkerNetworkResult<()> {
        let client = self.consensus_config.local_network().clone();
        let store = self.consensus_config.node_storage().clone();
        // validate batch - log error if invalid
        self.validator.validate_batch(sealed_batch.clone())?;

        let (mut batch, digest) = sealed_batch.split();

        // Set received_at timestamp for remote batch.
        batch.set_received_at(now());
        store.insert::<Batches>(&digest, &batch).map_err(|e| {
            WorkerNetworkError::Internal(format!("failed to write to batch store: {e}"))
        })?;

        // notify primary for payload store
        client
            .report_others_batch(WorkerOthersBatchMessage { digest, worker_id: self.id })
            .await
            .map_err(|e| WorkerNetworkError::Internal(e.to_string()))?;

        Ok(())
    }

    /// Attempt to return requested batches.
    pub(crate) async fn process_request_batches(
        &self,
        batch_digests: Vec<BlockHash>,
    ) -> WorkerNetworkResult<RequestBatchesResponse> {
        const MAX_REQUEST_BATCHES_RESPONSE_SIZE: usize = 6_000_000;
        const BATCH_DIGESTS_READ_CHUNK_SIZE: usize = 200;
        let store = self.consensus_config.node_storage().clone();

        let digests_chunks = batch_digests
            .chunks(BATCH_DIGESTS_READ_CHUNK_SIZE)
            .map(|chunk| chunk.to_vec())
            .collect_vec();
        let mut batches = Vec::new();
        let mut total_size = 0;
        let mut is_size_limit_reached = false;

        for digests_chunks in digests_chunks {
            let stored_batches =
                store.multi_get::<Batches>(digests_chunks.iter()).map_err(|e| {
                    WorkerNetworkError::Internal(format!("failed to read from batch store: {e:?}"))
                })?;

            for stored_batch in stored_batches.into_iter().flatten() {
                let batch_size = stored_batch.size();
                if total_size + batch_size <= MAX_REQUEST_BATCHES_RESPONSE_SIZE {
                    batches.push(stored_batch);
                    total_size += batch_size;
                } else {
                    is_size_limit_reached = true;
                    break;
                }
            }
        }

        Ok(RequestBatchesResponse { batches, is_size_limit_reached })
    }
}
