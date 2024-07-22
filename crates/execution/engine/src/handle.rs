//! A _shareable TN frontend type. Used to interact with the spawned TN engine task.

use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug, Clone)]
pub(crate) struct TNEngineHandle {
    pub(crate) to_engine: UnboundedSender<()>,
    // Broadcast channel for engine events
    // event_sender: todo!(),
}
