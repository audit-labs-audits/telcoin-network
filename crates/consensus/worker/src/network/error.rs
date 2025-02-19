use tn_network_libp2p::error::NetworkError;
use tn_types::{BatchValidationError, BcsError};
use tokio::time::error::Elapsed;

/// Result alias for results that possibly return [`WorkerNetworkError`].
pub type WorkerNetworkResult<T> = Result<T, WorkerNetworkError>;

/// Core error variants when executing the output from consensus and extending the canonical block.
#[derive(Debug, thiserror::Error)]
pub(crate) enum WorkerNetworkError {
    /// Error decoding with bcs.
    #[error("Failed to decode gossip message: {0}")]
    Decode(#[from] BcsError),
    /// Batch validation error occured.
    #[error("Failed batch validation: {0}")]
    BatchValidation(#[from] BatchValidationError),
    /// Internal error occurred.
    #[error("Internal error: {0}")]
    Internal(String),
    /// A network request timed out.
    #[error("Network request timed out")]
    Timeout(#[from] Elapsed),
    // Network error.
    #[error("Network error occured: {0}")]
    Network(#[from] NetworkError),
}
