//! Payload builder service metrics.

use execution_metrics::{
    metrics::{self, Counter},
    Metrics,
};
use strum::IntoStaticStr; // used for worker's batch maker metrics

/// The metric for capturing why the batch job
/// completed. The EL stops producing batches
/// if the max gas limit is reached, if the 
/// batch size (measured in bytes) was reached,
/// or if there were no more transactions left
/// for the pending pool's "best".
#[derive(Debug, IntoStaticStr)]
pub enum PayloadSizeMetric {
    GasLimit,
    MaxBatchSize,
    TxPoolEmpty,
}

impl Default for PayloadSizeMetric {
    fn default() -> Self {
        Self::TxPoolEmpty
    }
}

/// Payload builder service metrics
#[derive(Metrics)]
#[metrics(scope = "payloads")]
pub(crate) struct BatchBuilderServiceMetrics {
    /// Total number of initiated jobs
    pub(crate) initiated_jobs: Counter,
    /// Total number of failed jobs
    pub(crate) failed_jobs: Counter,
    /// Total number of sealed batches
    pub(crate) sealed_batches: Counter,
    /// Total number of failed batches
    pub(crate) failed_sealed_batches: Counter,
}

impl BatchBuilderServiceMetrics {
    pub(crate) fn inc_initiated_jobs(&self) {
        self.initiated_jobs.increment(1);
    }

    pub(crate) fn inc_failed_jobs(&self) {
        self.failed_jobs.increment(1);
    }

    pub(crate) fn inc_sealed_batches(&self) {
        self.sealed_batches.increment(1)
    }

    pub(crate) fn inc_failed_sealed_batches(&self) {
        self.failed_sealed_batches.increment(1)
    }
}
