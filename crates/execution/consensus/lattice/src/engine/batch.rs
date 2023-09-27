use crate::{LatticeConsensusEngine, LatticeOnNewPayloadError};
use execution_db::database::Database;
use execution_interfaces::blockchain_tree::BlockchainTreeEngine;
use execution_provider::{
    BlockReader, CanonChainTracker, StageCheckpointReader,
};
use execution_rpc_types::engine::{
    BatchPayloadStatus, BatchExecutionPayload,
};
use tn_types::execution::SealedBlock;
use tracing::*;

impl<DB, BT> LatticeConsensusEngine<DB, BT>
where
    BT: BlockchainTreeEngine + BlockReader + CanonChainTracker + StageCheckpointReader + 'static,
    DB: Database,
{
    /// When the Consensus layer receives a new batch from a worker's peer,
    /// the transactions in the batch are sent to the execution layer for verification.
    ///
    /// The Execution layer executes the transactions and validates they are valid
    /// based on the latest finalized block. If the transactions are valid, they are
    /// added to the "seen" tx pool? (is this necessary - could prevent tx in pending with nonce
    /// issue - could not)
    ///
    /// The validation status is passed back to the Consensus layer,
    /// which votes for the batch and includes it in the next block.
    ///
    /// This returns a [`PayloadStatus`] that represents the outcome of a processed new payload and
    /// returns an error if an internal error occurred.
    /// 
    /// 
    /// Akin to `on_new_payload` from beacon consensus
    #[instrument(
        level = "trace",
        skip(self, payload),
        fields(block_hash= ?payload.block_hash, block_number = %payload.block_number.as_u64()),
        target = "consensus::engine",
    )]
    pub(super) fn validate_batch(
        &mut self,
        payload: BatchExecutionPayload,
    ) -> Result<BatchPayloadStatus, LatticeOnNewPayloadError> {
        let block = match SealedBlock::try_from(payload) {
            Ok(block) => block,
            Err(error) => {
                error!(target: "consensus::engine", ?error, "Invalid batch payload");
                return Ok(BatchPayloadStatus::from(error))
            }
        };

        // TODO: spawn this as a task to free the engine up

        // TODO:
        // update sync when processing consensus output?
        //
        // syncing process is:
        // - CL downloading & verifying certs from peers
        //  - part of verification process is EL
        // - RPC should not accept txs until CL is caught up
        // let res = if self.sync.is_pipeline_idle() {
        //     // we can only insert new payloads if the pipeline is _not_ running, because it holds
        //     // exclusive access to the database
        //     self.try_insert_new_payload(block)
        // } else {
        //     self.try_buffer_payload(block)
        // };

        // assume pipeline is in sync
        let res = self.blockchain.validate_batch_without_senders(block);
        
        // TODO: return () or Err
        let status = match res {
            Ok(()) => BatchPayloadStatus::Valid,
            Err(e) => BatchPayloadStatus::Invalid { validation_error: e.to_string() },
        };

        Ok(status)
    }
}
