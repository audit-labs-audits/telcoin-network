//! Payload builder service metrics.

use execution_metrics::{
    metrics::{self, Counter},
    Metrics,
};
use strum::IntoStaticStr; // used for worker's batch maker metrics

/// The metric for capturing why the payload job
/// completed. The EL stops producing batches
/// if the max gas limit is reached, if the 
/// batch size (measured in bytes) was reached,
/// or if there were no more transactions left
/// for the pending pool's "best".
#[derive(Debug, IntoStaticStr)]
pub enum BatchPayloadSizeMetric {
    /// The batch reached the maximum gas limit allowed.
    GasLimit,
    /// The batch reached the maximum size (in bytes) allowed.
    MaxBatchSize,
    /// The pending transaction pool is empty. There are no
    /// more valid transactions available to include in this batch.
    TxPoolEmpty,
}

impl Default for BatchPayloadSizeMetric {
    fn default() -> Self {
        Self::TxPoolEmpty
    }
}

/// Payload builder service metrics
#[derive(Metrics)]
#[metrics(scope = "payloads")]
pub(crate) struct LatticePayloadBuilderServiceMetrics {
    /// Total number of initiated batch jobs
    pub(crate) initiated_batch_jobs: Counter,
    /// Total number of failed batch jobs
    pub(crate) failed_batch_jobs: Counter,
    /// Total number of sealed batches
    pub(crate) sealed_batches: Counter,
    /// Total number of failed sealed batches
    pub(crate) failed_sealed_batches: Counter,
    /// Total number of initiated header jobs
    pub(crate) initiated_header_jobs: Counter,
    /// Total number of failed header jobs
    pub(crate) failed_header_jobs: Counter,
}

impl LatticePayloadBuilderServiceMetrics {
    pub(crate) fn inc_initiated_batch_jobs(&self) {
        self.initiated_batch_jobs.increment(1);
    }

    pub(crate) fn inc_failed_batch_jobs(&self) {
        self.failed_batch_jobs.increment(1);
    }

    pub(crate) fn inc_sealed_batches(&self) {
        self.sealed_batches.increment(1)
    }

    pub(crate) fn inc_failed_sealed_batches(&self) {
        self.failed_sealed_batches.increment(1)
    }

    pub(crate) fn inc_initiated_header_jobs(&self) {
        self.initiated_header_jobs.increment(1);
    }

    pub(crate) fn inc_failed_header_jobs(&self) {
        self.failed_header_jobs.increment(1);
    }
}
