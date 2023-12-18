// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0
use futures::{future::try_join_all, stream::FuturesUnordered};
pub use narwhal_storage::{CertificateStoreCacheMetrics, NodeStorage};

pub mod engine;
mod error;
pub mod metrics;
pub mod primary;
pub mod worker;

// both:
//  - db
//  - provider factory
//  - auto seal consensus

// per primary:
//  - beacon engine
//  - network pipeline

// per worker:
//  - txpool
