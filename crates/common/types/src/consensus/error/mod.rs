// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use thiserror::Error;
mod dag;
pub use dag::{DagError, DagResult, AcceptNotification};
mod crypto;
pub use crypto::CryptoError;
mod client;
pub use client::LocalClientError;

#[cfg(test)]
#[path = "../tests/error_test.rs"]
mod error_test;

#[macro_export]
macro_rules! bail {
    ($e:expr) => {
        return Err($e);
    };
}

#[macro_export(local_inner_macros)]
macro_rules! ensure {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            bail!($e);
        }
    };
}

#[derive(Clone, Debug, Error)]
pub enum ConsensusError {
    #[error("")]
    Client(#[from] LocalClientError),

    #[error("")]
    Dag(#[from] DagError),

    #[error("")]
    Crypto(#[from] CryptoError),
}
