// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use async_trait::async_trait;
use std::fmt::{Debug, Display};
use tn_types::consensus::Batch;

/// Defines the validation procedure for receiving either a new single transaction (from a client)
/// of a batch of transactions (from another validator). Invalid transactions will not receive
/// further processing.
#[async_trait]
pub trait TransactionValidator: Clone + Send + Sync + 'static {
    type Error: Display + Debug + Send + Sync + 'static;
    /// Determines if a transaction valid for the worker to consider putting in a batch
    fn validate(&self, t: &[u8]) -> Result<(), Self::Error>;
    /// Determines if this batch can be voted on
    async fn validate_batch(&self, b: &Batch) -> Result<(), Self::Error>;
}

/// Simple validator that accepts all transactions and batches.
#[derive(Debug, Clone, Default)]
pub struct TrivialTransactionValidator;
#[async_trait]
impl TransactionValidator for TrivialTransactionValidator {
    type Error = eyre::Report;

    fn validate(&self, _t: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn validate_batch(&self, _b: &Batch) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Transaction validator for EVM transactions
#[derive(Debug, Clone, Default)]
pub struct LatticeTransactionValidator;
#[async_trait]
impl TransactionValidator for LatticeTransactionValidator {
    type Error = eyre::Report;

    fn validate(&self, _tx: &[u8]) -> Result<(), Self::Error> {
        // tx should be verified by the rpc before reaching this point
        todo!()
    }

    async fn validate_batch(&self, batch: &Batch) -> Result<(), Self::Error> {
        // send to EL for validation
        todo!()
    }
}
