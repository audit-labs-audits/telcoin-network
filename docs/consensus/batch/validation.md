# Validation

Workers receive batches from their peers and validate all the transactions.

The transactions in a batch are RLP encoded `TransactionSigned` structs.

## Execution Layer
The execution layer is responsible for validating all transactions within a batch.

### Worker's Own Batch
The batch a worker proposes to peers (aka "own batch") is assembled by a payload builder in the execution layer. Transaction signatures are first verified by the RPC at the time of submission. If the transaction signature is valid, the transaction is sent to the pool's transaction validator (`EthTransactionValidatorInner`) for further validation.

The pool's transaction validator asserts:
- the transaction's size is below the max size allowed
    - prevent DoS attacks
- the transaction is below the max init code size
- the transaction's gas limit is below the block's max gas limit
- the max priority fee per gas is greater than the max fee per gas (if any)
- non-local transactions meet the minimum priority fee asserted by the node
- the chain id matches
- the transactions is signed by an EOA
- the transaction nonce is not less than the account's nonce
- the account balance is sufficient to pay for the transaction

If all of these assertions complete, the transaction is considered valid and sent to the transaction pool. Only transactions that are ready to be executed in the current state of the blockchain are sorted into a "pending transaction pool". Transactions that don't have enough gas or belong to an account with nonce gaps (missing transaction ancestor) must wait for state to change in order to become eligible for the next batch.

Workers create batches from the best group of transactions in the "pending" pool. The pending pool is constantly updated to ensure maximum gas is included in the next batch. If state changes, the pool updates.

The worker then sends the batch to its peers for validation. Once enough peers validate the worker's batch (2f + 1), quorum is reached. The batch digest is shared with the primary to be included in the next proposed block.

### Worker's Peer Batch
Workers receive batches from peers and validate the transactions by sending the vector of bytes to the execution layer. The EL recovers each transaction's signer and sends the transaction to the `BatchTransactionValiatorInner`.

The batch transaction validator asserts:
- the transaction's size is below the max size allowed
    - prevent DoS attacks
- the transaction is below the max init code size
- the transaction's gas limit is below the block's max gas limit
- the max priority fee per gas is greater than the max fee per gas (if any)
- the chain id matches
- the transactions is signed by an EOA
- the transaction nonce is not less than the account's nonce
- the account balance is sufficient to pay for the transaction

difference between "own batch":
- removed: non-local transactions meet the minimum priority fee asserted by the node

All transactions in the batch must be valid for the batch to be valid. If a peer's batch is considered valid, the worker responds to the proposing peer with a vote of approval. The worker then stores the batch information.
