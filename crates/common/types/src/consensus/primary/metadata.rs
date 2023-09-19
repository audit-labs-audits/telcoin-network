use enum_dispatch::enum_dispatch;
// TODO: impl these in exect first
// use consensus_util_mem::MallocSizeOf;
// use proptest_derive::Arbitrary;
use crate::{
    consensus::{now, TimestampSec},
    execution::{
        constants::EMPTY_RECEIPTS, proofs::EMPTY_ROOT, Address, Bloom, Bytes, Withdrawal, H256,
        U256, SealedHeader,
    },
};
#[cfg(any(test, feature = "arbitrary"))]
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

/// Additional metadata information for an entity.
///
/// The structure as a whole is not signed. As a result this data
/// should not be treated as trustworthy data and should be used
/// for NON CRITICAL purposes only. For example should not be used
/// for any processes that are part of our protocol that can affect
/// safety or liveness.
///
/// This is a versioned `Metadata` type
#[cfg_attr(any(test, feature = "arbitrary"), derive(Arbitrary))]
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq /* , MallocSizeOf */)]
#[enum_dispatch(MetadataAPI)]
pub enum VersionedMetadata {
    /// version 1
    V1(MetadataV1),
}

impl VersionedMetadata {
    /// Create a new instance of [VersionedMetadata]
    pub fn new(
        parent_hash: H256,
        receipts_root: H256,
        logs_bloom: Bloom,
        block_number: u64,
        gas_limit: u64,
        gas_used: u64,
        base_fee_per_gas: U256,
        block_hash: H256,
    ) -> Self {
        Self::V1(MetadataV1 {
            created_at: now(),
            received_at: None,
            parent_hash,
            receipts_root,
            logs_bloom,
            block_number,
            gas_limit,
            gas_used,
            base_fee_per_gas,
            block_hash,
        })
    }

    /// Create a new instance of [VersionedMetadata] with a timestamp
    pub fn new_with_timestamp(
        created_at: TimestampSec,
        parent_hash: H256,
        receipts_root: H256,
        logs_bloom: Bloom,
        block_number: u64,
        gas_limit: u64,
        gas_used: u64,
        base_fee_per_gas: U256,
        block_hash: H256,
    ) -> Self {
        Self::V1(MetadataV1 {
            created_at,
            received_at: None,
            parent_hash,
            receipts_root,
            logs_bloom,
            block_number,
            gas_limit,
            gas_used,
            base_fee_per_gas,
            block_hash,
        })
    }
}

impl Default for VersionedMetadata {
    /// Default implementation for `VersionedMetadata` used for testing only.
    fn default() -> Self {
        Self::V1(MetadataV1 {
            created_at: now(),
            received_at: None,
            parent_hash: H256::zero(),
            receipts_root: EMPTY_RECEIPTS,
            logs_bloom: Default::default(),
            block_number: Default::default(),
            gas_limit: Default::default(),
            gas_used: Default::default(),
            base_fee_per_gas: Default::default(),
            block_hash: H256::zero(),
            // prev_randao: Default::default(),
            // extra_data: Default::default(),
            // fee_recipient: Address::zero(),
            // state_root: EMPTY_ROOT,
            // withdrawals: Default::default(),
        })
    }
}

impl From<SealedHeader> for VersionedMetadata {
    fn from(value: SealedHeader) -> Self {
        let SealedHeader {
            header,
            hash,
        } = value;

        let base_fee = U256::from(
            header
                .base_fee_per_gas
                .unwrap_or_default()
        );

        VersionedMetadata::new_with_timestamp(
            header.timestamp,
            header.parent_hash,
            header.receipts_root,
            header.logs_bloom,
            header.number,
            header.gas_limit,
            header.gas_used,
            base_fee,
            hash,
        )
    }
}

/// API for accessing fields for [VersionedMetadata] variants
///
/// TODO: update comments if leaving these fields in here.
#[enum_dispatch]
pub trait MetadataAPI {
    /// TODO
	fn created_at(&self) -> &TimestampSec;
    /// TODO
	fn set_created_at(&mut self, ts: TimestampSec);
    /// TODO
	fn received_at(&self) -> Option<TimestampSec>;
    /// TODO
	fn set_received_at(&mut self, ts: TimestampSec);

    /// TODO
	fn parent_hash(&self) -> H256;
    /// TODO
	fn receipts_root(&self) -> H256;
    /// TODO
	fn logs_bloom(&self) -> Bloom;
    /// TODO
	fn block_number(&self) -> u64;
    /// TODO
	fn gas_limit(&self) -> u64;
    /// TODO
	fn gas_used(&self) -> u64;
    // use created_at, but return owned value
    /// TODO
	fn timestamp(&self) -> u64;
    /// TODO
	fn base_fee_per_gas(&self) -> U256;
    /// TODO
	fn block_hash(&self) -> H256;
    // /// TODO
	// fn prev_randao(&self) -> H256;
    // /// TODO
	// fn extra_data(&self) -> Bytes;
    // /// TODO
	// fn fee_recipient(&self) -> Address;
    // /// TODO
	// fn state_root(&self) -> H256;
    // /// TODO
	// fn withdrawals(&self) -> Option<Vec<Withdrawal>>;
}

/// Metadata for batches.
#[cfg_attr(any(test, feature = "arbitrary"), derive(Arbitrary))]
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Default, Eq /* , MallocSizeOf */)]
pub struct MetadataV1 {
    /// Timestamp of when the entity created. This is generated
    /// by the node which creates the entity.
    pub created_at: TimestampSec,
    /// Timestamp of when the entity was received by another node. This will help
    /// calculate latencies that are not affected by clock drift or network
    /// delays. This field is not set for own batches.
    pub received_at: Option<TimestampSec>,
    /// Hash of the last finalized block
    pub parent_hash: H256,
    /// Value from EL
    pub receipts_root: H256,
    /// Value from EL
    pub logs_bloom: Bloom,
    /// Value from EL
    pub block_number: u64,
    /// Value from EL
    pub gas_limit: u64,
    /// Value from EL
    pub gas_used: u64,
    /// Value from EL
    pub base_fee_per_gas: U256,
    /// Value from EL
    pub block_hash: H256,
}

impl MetadataAPI for MetadataV1 {
    fn created_at(&self) -> &TimestampSec {
        &self.created_at
    }

    fn set_created_at(&mut self, ts: TimestampSec) {
        self.created_at = ts;
    }

    fn received_at(&self) -> Option<TimestampSec> {
        self.received_at
    }

    fn set_received_at(&mut self, ts: TimestampSec) {
        self.received_at = Some(ts);
    }

    // helper methods for validating batch in EL

    fn parent_hash(&self) -> H256 {
        self.parent_hash
    }

    fn receipts_root(&self) -> H256 {
        self.receipts_root
    }

    fn logs_bloom(&self) -> Bloom {
        self.logs_bloom
    }

    fn block_number(&self) -> u64 {
        self.block_number
    }

    fn gas_limit(&self) -> u64 {
        self.gas_limit
    }

    fn gas_used(&self) -> u64 {
        self.gas_used
    }

    // use created_at, but return owned value
    fn timestamp(&self) -> u64 {
        self.created_at
    }
    fn base_fee_per_gas(&self) -> U256 {
        self.base_fee_per_gas
    }

    fn block_hash(&self) -> H256 {
        self.block_hash
    }

    // TODO: assumed to be unnecessary

    // fn fee_recipient(&self) -> Address {
    //     self.fee_recipient
    // }

    // fn prev_randao(&self) -> H256 {
    //     self.prev_randao
    // }

    // fn state_root(&self) -> H256 {
    //     self.state_root
    // }

    // fn extra_data(&self) -> Bytes {
    //     self.extra_data.clone()
    // }

    // fn withdrawals(&self) -> Option<Vec<Withdrawal>> {
    //     self.withdrawals.clone()
    // }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_v1() {
        let metadata = VersionedMetadata::default();
        assert_eq!(metadata.parent_hash(), H256::zero());
        assert_eq!(metadata.receipts_root(), EMPTY_RECEIPTS);
        assert_eq!(metadata.logs_bloom(), Bloom::default());
        assert_eq!(metadata.prev_randao(), Default::default());
        assert_eq!(metadata.block_number(), u64::default());
        assert_eq!(metadata.gas_limit(), u64::default());
        assert_eq!(metadata.gas_used(), u64::default());
        assert_eq!(metadata.block_hash(), H256::zero());
        // assert_eq!(metadata.fee_recipient(), Address::zero());
        // assert_eq!(metadata.withdrawals(), None);
        // assert_eq!(metadata.state_root(), EMPTY_ROOT);
        // assert_eq!(metadata.extra_data(), Bytes::default());
        // assert_eq!(metadata.base_fee_per_gas(), Default::default());
    }
}
