// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use async_trait::async_trait;
use std::fmt::{Debug, Display};
use tn_types::consensus::Batch;
use execution_lattice_consensus::LatticeConsensusEngineHandle;

/// Defines the validation procedure for receiving either a new single transaction (from a client)
/// of a batch of transactions (from another validator). Invalid transactions will not receive
/// further processing.
#[async_trait]
pub trait TransactionValidator: Clone + Send + Sync + 'static {
    type Error: Display + Debug + Send + Sync + 'static;
    /// Determines if this batch can be voted on
    async fn validate_batch(&self, b: &Batch) -> Result<(), Self::Error>;
}

/// Simple validator that accepts all transactions and batches.
#[derive(Debug, Clone, Default)]
pub struct TrivialTransactionValidator;
#[async_trait]
impl TransactionValidator for TrivialTransactionValidator {
    type Error = eyre::Report;

    async fn validate_batch(&self, _b: &Batch) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Transaction validator for EVM transactions
#[derive(Debug, Clone)]
pub struct LatticeTransactionValidator {
    engine_handle: LatticeConsensusEngineHandle,
}

impl LatticeTransactionValidator {
    /// Create a new instance of Self
    fn new(engine_handle: LatticeConsensusEngineHandle) -> Self {
        Self { engine_handle }
    }
}

#[async_trait]
impl TransactionValidator for LatticeTransactionValidator {
    type Error = eyre::Report;

    async fn validate_batch(&self, batch: &Batch) -> Result<(), Self::Error> {
        self.engine_handle.validate_batch(batch.clone()).await.map_err(|e| e.into())
    }
}
