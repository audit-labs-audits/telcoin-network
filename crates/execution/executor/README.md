# Executor
RPC and transaction pool implementation that takes validated transactions from consensus and executes the next canonical block.

Loosely based off of auto-mine consensus in reth library.

## Goal
Create a library for receiving the CL's `ConsensusOutput` and executing a block.

All transactions in the pool are mined. Transactions are evaluated in the order reached by consensus. If duplicate transactions are found, the later is rejected. Same for other invariations of transactions per account.

The `BatchMaker` must read from the same DB and update base fees once the canonical tip is updated.

## Initial Strategy
The most straight-forward approach is to take the `ConsensusOutput` and loop through each transaction in each batch, like the `SimpleExecutionState` example. Each transactions is submitted to the txpool in that order, and the default eth pool rules dictate execution order. The pooled transactions include the current balance for the sender and the state nonce.

We'll log for now to see what types of issues can arrise.

Significant optimizations are possible.

Instead of adding to the txpool and re-validating every transaction, we can use `ConsensusOutput` instead of `best_transactions()` to receive an iterator of all transactions to loop through. We can capture the latest state through the provider and calculate the sender's balance and nonce on the fly.

## Question
Is there a consequence for re-sequencing transactions after consensus?
> The DAG orders all transactions once consensus is reached. Executing the transactions in order seems valid, but adding them to a transaction pool that re-orders them may introduce unintended consequences.
> Imagine swaps and liquidity - seems harder to predict slippage if you don't know the collection of transactions within the batches within the output.
> Ideally, with ERC-20 native token impl there is a way to create UTXO-like model (need better understanding of Polygon's approach)

Is there output between rounds that can resemble forks?
> Even if there is, blocks need to be re-executed to prevent transaction issues associated with account-style ledgers.
