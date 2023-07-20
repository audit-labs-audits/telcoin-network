use thiserror::Error;
use anemo::PeerId;

#[derive(Clone, Debug, Error)]
pub enum LocalClientError {
    #[error("Primary {0} has not started yet.")]
    PrimaryNotStarted(PeerId),

    #[error("Worker {0} has not started yet.")]
    WorkerNotStarted(PeerId),

    #[error("Handler encountered internal error {0}.")]
    Internal(String),

    #[error("Narwhal is shutting down.")]
    ShuttingDown,
}