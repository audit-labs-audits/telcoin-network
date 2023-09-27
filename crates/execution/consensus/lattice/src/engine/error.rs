use execution_stages::PipelineError;

/// Lattice engine result.
pub type LatticeEngineResult<Ok> = Result<Ok, LatticeConsensusEngineError>;

/// The error type for the lattice consensus engine service
/// [LatticeConsensusEngine](crate::LatticeConsensusEngine)
///
/// Represents all possible error cases for the lattice consensus engine.
#[derive(Debug, thiserror::Error)]
pub enum LatticeConsensusEngineError {
    /// Pipeline channel closed.
    #[error("Pipeline channel closed")]
    PipelineChannelClosed,
    /// Pipeline error.
    #[error(transparent)]
    Pipeline(#[from] Box<PipelineError>),
    /// Common error. Wrapper around [execution_interfaces::Error].
    #[error(transparent)]
    Common(#[from] execution_interfaces::Error),
}

// box the pipeline error as it is a large enum.
impl From<PipelineError> for LatticeConsensusEngineError {
    fn from(e: PipelineError) -> Self {
        Self::Pipeline(Box::new(e))
    }
}

// for convenience in the lattice engine
impl From<execution_interfaces::db::DatabaseError> for LatticeConsensusEngineError {
    fn from(e: execution_interfaces::db::DatabaseError) -> Self {
        Self::Common(e.into())
    }
}


/// Represents all error cases when handling a new payload.
///
/// This represents all possible error cases that must be returned as JSON RCP errors back to the
/// lattice node.
#[derive(Debug, thiserror::Error)]
pub enum LatticeOnNewPayloadError {
    /// Thrown when the engine task is unavailable/stopped.
    #[error("lattice consensus engine task stopped")]
    EngineUnavailable,
    /// An internal error occurred, not necessarily related to the payload.
    #[error(transparent)]
    Internal(Box<dyn std::error::Error + Send + Sync>),
    /// The batch is invalid within the context of this engine.
    #[error("Batch invalid: {0}")]
    InvalidBatch(String), // TODO: include reason
}
