use std::collections::BTreeMap;

use execution_db::database::Database;
use execution_interfaces::{consensus::{Consensus, ConsensusError}, blockchain_tree::{InsertPayloadOk, error::{InsertBlockError, BlockchainTreeError}, BlockStatus}};
use execution_provider::{ExecutorFactory, PostState, HeaderProvider};
use tn_types::execution::{SealedBlockWithSenders, Block, BlockNumHash};
use tracing::{error, instrument};

use crate::{BlockchainTree, AppendableChain, PostStateDataRef};


impl<DB: Database, C: Consensus, EF: ExecutorFactory> BlockchainTree<DB, C, EF> {
    /// Execute transactions within a peer's batch and validate the outcome.
    /// 
    /// akin to `self.insert_block()`
    pub(crate) fn validate_batch(
        &self,
        block: SealedBlockWithSenders,
    ) -> Result<(), InsertBlockError> {
        if let Err(e) = self.externals.consensus.validate_batch_standalone(&block) {
            error!(?block, "Failed to validate block {}: {e:?}", block.header.hash);
            return Err(InsertBlockError::consensus_error(e, block.block))
        }

        self.try_execute_validated_batch(block)
    }

    /// Try executing a validated [Self::validate_batch] batch.
    #[instrument(skip_all, fields(block = ?block.num_hash()), target = "blockchain_tree", ret)]
    pub(crate) fn try_execute_validated_batch(
        &self,
        block: SealedBlockWithSenders,
    ) -> Result<(), InsertBlockError> {

        let parent = block.parent_num_hash();

        // TODO: this lets batches from previous round be validated on older blocks
        //
        // only check canonical chain for parent
        if self
            .is_block_hash_canonical(&parent.hash)
            .map_err(|err| InsertBlockError::new(block.block.clone(), err.into()))?
        {
            // execute the batch based on provided parent
            return self.execute_batch(block, parent)
        }

        Err(InsertBlockError::consensus_error(
            ConsensusError::ParentUnknown {
                hash: parent.hash,
            },
            block.block,
        ))
    }

    /// Execute the batch off the canonical chain.
    /// 
    /// The canonical fork is used to recreate state up to that block so the batch
    /// is executed against the actual parent, not the canonical tip. This is necessary
    /// because nodes may receive batches after a round is complete.
    pub(crate) fn execute_batch(
        &self,
        block: SealedBlockWithSenders,
        canonical_fork: BlockNumHash,
    ) -> Result<(), InsertBlockError> {
        let factory = self.externals.database();
        let provider = factory
            .provider()
            .map_err(|err| InsertBlockError::new(block.block.clone(), err.into()))?;

        let parent_header = provider
            .header(&block.parent_hash)
            .map_err(|err| InsertBlockError::new(block.block.clone(), err.into()))?
            .ok_or_else(|| {
                InsertBlockError::tree_error(
                    BlockchainTreeError::CanonicalChain { block_hash: block.parent_hash },
                    block.block.clone(),
                )
            })?
            .seal(block.parent_hash);

        let state = PostState::default();
        let empty = BTreeMap::new();
        let canonical_block_hashes = self.canonical_chain().inner();

        let state_provider = PostStateDataRef {
            state: &state,
            sidechain_block_hashes: &empty,
            canonical_block_hashes,
            canonical_fork,
        };

        // fork off canonical chain to execute and validate batch
        AppendableChain::execute_and_validate_batch(
            block.clone(),
            &parent_header,
            state_provider,
            &self.externals,
        )
        .map_err(|err| InsertBlockError::new(block.block.clone(), err.into()))
    }
}
