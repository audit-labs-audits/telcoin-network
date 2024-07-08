//! Error types for Telcoin Network Engine.

use reth_errors::RethError;

/// Result alias for [`TNEngineError`].
pub type EngineResult<T> = Result<T, TnEngineError>;

/// Core error variants when executing the output from consensus and extending the canonical block.
#[derive(Debug, thiserror::Error)]
pub enum TnEngineError {
    /// Error from Reth
    #[error(transparent)]
    Reth(#[from] RethError),
}
