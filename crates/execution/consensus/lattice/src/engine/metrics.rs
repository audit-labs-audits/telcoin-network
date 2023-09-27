use execution_metrics::{
    metrics::{self, Counter},
    Metrics,
};

/// Lattice consensus engine metrics.
#[derive(Metrics)]
#[metrics(scope = "consensus.engine.lattice")]
pub(crate) struct EngineMetrics {
    /// The number of times a canonical update was requested.
    pub(crate) canonical_block_update_request: Counter,
    /// The total count of batches validated.
    pub(crate) validate_batch_request: Counter,
}
