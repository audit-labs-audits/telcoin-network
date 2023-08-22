# Telcoin Network
The Telcoin Network is a distributed network of computers that participate in consensus to finalize EVM-style blocks.

## Execution Layer
The execution layer contains the RPC and block production. It is based of `reth`'s open-source project.

## Consensus Layer
The consensus layer uses Bullshark and Narwhal mempools to finalize a collection of transactions for the next EVM block.

Transaction Flow:
RPC -> submit_raw_tx -> validation -> TxPool -> get_batch() -> BuiltPayload -> SealedBlock -> ExecutionPayload -> Batch -> Quorum -> Request Cert Block from EL -> BuiltPayload -> SealedBlock -> ExecutionPayload -> Certificate -> ConsensusOutput -> OutputAttributes -> FinalizedPool -> CanonicalBlock

Problem: users submit duplicate transactions?
    - how can validators provide a better experience for users to submit txs?
        - multiple rpc endpoints that point to the same worker's tx pool?
        - multiple workers that each own their own rpc endpoint?
        - one rpc to worker pool doesn't scale well

### Logic
Problem: How does the protocol verify execution results?
    - is it sufficient to just have the next round of batches contain the parent block's hash?
        - which is the certificate block's hash
    - should certificates be produced by the EL and include hashed info (like batches)?
        - certificates could not reach quorum unless verified by the validator's EL
            - but this is expensive
    - for testnet: validators are hosted by TN, so this is not necessarily an issue

Problem: are batch digests just block digests? If so, certificates would be forks of blocks.
    - whiteboard

Problem: time slots are used in the beacon CL to verify block timestamps in order to prevent smart contract manipulation.
    - how do our timestamps affect smart contracts in regards to block production?

Problem: account based ledgers have tx nonce issues
    - UTXO model seems to solve this, which would work better for CL
    - if we are deploying our own contract for token supply, can we also incorporate a UTXO-type model?
        - is this possible and would it allow EL to just dump txs into batches?

# TODOs
- Handle `next_batch` logic in tx pool
    - pull best txs from pending pool
        - prune for next batch
        - still execute to ensure tx is "valid" on canonical tip
    - add txs to a `BatchPool` that represent pending txs that haven't reached quorum yet
        - if batch fails, move txs back to pending pool
    - move txs to `FinalizedPool` when quorum is reached
        - primary's proposer only sends batch digests to EL for block building
        - need to obtain txs by batch digest
            - or get batches with proposer's request?
                - more reliable but less efficient
                - I think for now take happy path
                    - store in memory for each round
- Handle peer batches
    - mostly in place, just need to hook it up and write a test
- Handle header proposer
    - get data from EL for proposed block
        - pull from `FinalizedPool`
        - how to ensure batches are correct?
            - associate batch digest with `BatchPool` instance
                - tx pool sends payload
                - worker replies with batch id
                - once worker responds, add to batch pool
            - only 1 worker for now
                - assume more in the future
        - does EL need to know batch digest?
            - EL always adds txs from batch pool to finalized pool when quorum is reached
            - assume batch pool is organized by batch digest
            - all txs that match the batch digest upon quorum are moved to finalized pool
- Handle peer headers
    - send to EL for validation
    - store the block in tree?
    - wait for consensus output to create one giant block?
        - EL verified when primary proposes next header because of `parent_block` hash


# TEL as Native Currency
See log usage in crates/execution/storage/provider/src/test_utils/blocks.rs
