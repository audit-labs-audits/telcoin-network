// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use futures::{future::try_join_all, stream::FuturesUnordered};
use lattice_executor::SubscriberError;
pub use lattice_storage::{CertificateStoreCacheMetrics, NodeStorage};
use thiserror::Error;
use tn_types::consensus::config::WorkerId;

pub mod execution_state;
pub mod metrics;
pub mod primary_node;
pub mod worker_node;

#[derive(Debug, Error, Clone)]
pub enum NodeError {
    #[error("Failure while booting node: {0}")]
    NodeBootstrapError(#[from] SubscriberError),

    #[error("Node is already running")]
    NodeAlreadyRunning,

    #[error("Worker nodes with ids {0:?} already running")]
    WorkerNodesAlreadyRunning(Vec<WorkerId>),
}
