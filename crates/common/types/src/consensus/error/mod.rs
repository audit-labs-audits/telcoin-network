// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Error types for Conensus Layer.
use thiserror::Error;
mod dag;
pub use dag::{AcceptNotification, DagError, DagResult};
mod crypto;
pub use crypto::CryptoError;
mod client;
pub use client::LocalClientError;

#[cfg(test)]
#[path = "../tests/error_test.rs"]
mod error_test;

/// Return the error.
#[macro_export]
macro_rules! bail {
    ($e:expr) => {
        return Err($e)
    };
}

/// Ensure condition is met, otherwise `bail!`
#[macro_export(local_inner_macros)]
macro_rules! ensure {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            bail!($e);
        }
    };
}

/// Top-level error type that encapsulates all other possible errors in the Consensus Layer.
#[derive(Clone, Debug, Error)]
pub enum ConsensusError {
    /// Client errors
    #[error("")]
    Client(#[from] LocalClientError),

    /// Dag errors
    #[error("")]
    Dag(#[from] DagError),

    /// Crypto errors
    #[error("")]
    Crypto(#[from] CryptoError),
}
