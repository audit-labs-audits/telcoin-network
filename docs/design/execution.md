# Execution Layer

- verifies incoming transaction requests
    - organizes them into batches for consensus
- verifies information received by the consensus layer
- updates the canonical chain based on consensus output

Different than ethereum:
- transactions are not gossiped

## Extending the Canonical Tip
- CL reaches consensus
- Send `ConsensusOutput` to the EL Executor
- Executor executes each batch in order of ConsensusOutput
    - ignores failures
        - ie) double-spend, etc.

### Block Data
Ethereum block data is not necessarily equivalent for TN. The fields must be kept in order to maintain EVM compatability and to maintain compatibility with upstream reth changes.

Because blocks contain these fields, they are accessible to be re-purposed for TN consensus. These are some ideas of what data to include in the final SealedBlock.

[EIP-3675](https://eips.ethereum.org/EIPS/eip-3675) specifies block structure after the merge. Updating this values will require custom implementations of `Consensus` trait.

##### Ommers
*Ethereum*: A collection of uncle headers. Considered a "constant value" (empty vec) in PoS.

*TN*: The ordered list of batch hashes (flattened) in the ConsensusOutput.

*Logic*: Batches are proposed in parallel. This could be used to guarantee a complete ConsensusOutput included all batches. Batch validation ensures parent hash matches. TN can include this data without any cascading consequences using the current default `EthBeaconConsensus` logic from reth because batches don't have ommers, only final execution blocks.

##### Ommers Hash
*Ethereum*: Considered a "constant value" (EMPTY_OMMER_ROOT_HASH) post-merge.

*TN*: Hash of ommers (ordered list of batch hashes in ConsensusOutput) - ensures correct ordering.

*Logic*: Unused hash value that is unlikely to be re-assigned in Ethereum.

##### Nonce
*Ethereum*: Considered a "constant value" (0x0).

*TN*: The sub dag index's `SequenceNumber` (also u64). The "nonce" for consensus.

*Logic*: Is this redundant? Could help with block explorers. Could help with ensuring all batches in ConsensusOutput are executed. What happens during recovery? Need to ensure all batches were executed correctly. Block number cannot apply to batches or rounds since they are executed in multiples.

##### Difficulty
*Ethereum*: No longer used post-merge and must be 0.

*TN*: Batch index? This could be used for documenting validator rewards? Leave it alone and consider using it for `randao` value CL needs this value from EL?

*Logic*: [EIP-4399](https://eips.ethereum.org/EIPS/eip-4399) in favor of supplanting DIFFICULTY opcode with PREVRANDAO. If so, TN could use this for randao values.

##### Mixed Hash
*Ethereum*: A 256-bit hash which, combined with the nonce, proves that a sufficient amount of computation has been carried out on this block (formally Hm). Post-merge, execution clients (reth and geth) use it to hold the prev randao value used to select the next quorum of validators by beacon chain. The block has `mixed_hash` and the beacon block has `prev_randao`. These values are converted when the execution payload is constructed from the built block and used by Beacon chain for validator shuffling.

*TN*:

*Logic*:  On-chain programs might rely on this value for randomness, and it must be consistent when the batch is made and the final block is executed. It's also important that the random value is verifiable yet unpredictable.

##### Extra Data
*Ethereum*: Anything a validator wants to use - 32 bytes.

*TN*: Batch Hash. The index of the batch? Better to use `difficulty` because U256 type? Also consider: The priority fee rewards for validator? The hash of the batch that was used to execute this block? This could also be used for the `ConsensusOutput` hash? Can't use ms timestamp because execution times could vary between validators. Is it valuable to include the leader's round?

*Logic*: It's unclear exactly what data is most useful for indexing blocks down the road. Extra data needs to be consistent amongst all validators to ensure correct hash of executed block, but using extra data now prevents TN from ever using it again. While other "unused" block fields are in place, prefer to use these instead.

##### Parent beacon block root
*Ethereum*: the hash of the parent beacon block's root to support minimal trust for accessing consensus state.

*TN*: `ConsensusOutput` hash. Or should this be used for managing rewards/validator sets on chain?

*Logic*: The parent beacon block root is mostly helpful for supporting staking pools like Lido and isn't really relevant to TN directly. However, it could be useful for calculating epoch shuffles. IMPORTANT: ensure implementation wouldn't violate EVM expectations.
