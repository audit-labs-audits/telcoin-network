use crate::tables::Payload;
use tn_types::{BlockHash, Database, WorkerId};
use tn_utils::fail_point;

/// Access the batch digests for the primary node for the own created batches.
pub trait PayloadStore {
    fn write_payload(&self, digest: &BlockHash, worker_id: &WorkerId) -> eyre::Result<()>;

    /// Queries the store whether the batch with provided `digest` and `worker_id` exists. It
    /// returns `true` if exists, `false` otherwise.
    fn contains_payload(&self, digest: BlockHash, worker_id: WorkerId) -> eyre::Result<bool>;
}

impl<DB: Database> PayloadStore for DB {
    fn write_payload(&self, digest: &BlockHash, worker_id: &WorkerId) -> eyre::Result<()> {
        fail_point!("payload-store-before-write");

        self.insert::<Payload>(&(*digest, *worker_id), &0u8)?;

        fail_point!("payload-store-after-write");
        Ok(())
    }

    fn contains_payload(&self, digest: BlockHash, worker_id: WorkerId) -> eyre::Result<bool> {
        self.contains_key::<Payload>(&(digest, worker_id))
    }
}
