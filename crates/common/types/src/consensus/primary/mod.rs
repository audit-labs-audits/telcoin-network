//! Primary types used for consensus.

use std::time::{Duration, SystemTime};
mod batch;
mod certificate;
mod communication;
mod header;
mod metadata;
mod vote;

pub use batch::*;
pub use certificate::*;
pub use communication::*;
pub use header::*;
pub use metadata::*;
pub use vote::*;

/// The round number.
pub type Round = u64;

/// The epoch UNIX timestamp in milliseconds
pub type TimestampMs = u64;

pub trait Timestamp {
    // Returns the time elapsed between the timestamp
    // and "now". The result is a Duration.
    fn elapsed(&self) -> Duration;
}

impl Timestamp for TimestampMs {
    fn elapsed(&self) -> Duration {
        let diff = now().saturating_sub(*self);
        Duration::from_millis(diff)
    }
}
// Returns the current time expressed as UNIX
// timestamp in milliseconds
pub fn now() -> TimestampMs {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_millis() as TimestampMs,
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}

#[cfg(test)]
mod tests {
    use crate::consensus::{
        Batch, BatchAPI, BatchV1, MetadataAPI, MetadataV1, Timestamp,
        VersionedMetadata,
    };
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_elapsed() {
        // BatchV1
        let batch = Batch::new(vec![]);

        assert!(*batch.versioned_metadata().created_at() > 0);

        assert!(batch.versioned_metadata().received_at().is_none());

        sleep(Duration::from_secs(2)).await;

        assert!(
            batch
                .versioned_metadata()
                .created_at()
                .elapsed()
                .as_secs_f64()
                >= 2.0
        );
    }

    #[test]
    fn test_elapsed_when_newer_than_now() {
        // BatchV1
        let batch = Batch::V1(BatchV1 {
            transactions: vec![],
            versioned_metadata: VersionedMetadata::V1(MetadataV1 {
                created_at: 2999309726980, // something in the future - Fri Jan 16 2065 05:35:26
                received_at: None,
                ..Default::default()
            }),
        });

        assert_eq!(
            batch
                .versioned_metadata()
                .created_at()
                .elapsed()
                .as_secs_f64(),
            0.0
        );
    }
}
