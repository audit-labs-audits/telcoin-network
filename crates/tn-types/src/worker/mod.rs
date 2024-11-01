//! Worker types.

use tokio::sync::{mpsc::Sender, oneshot};

mod pending_block;
pub use pending_block::*;

use crate::{error::BlockSealError, WorkerBlock};

/// Type for the channel sender to submit worker block to the block provider.
pub type WorkerBlockSender = Sender<(WorkerBlock, oneshot::Sender<Result<(), BlockSealError>>)>;
