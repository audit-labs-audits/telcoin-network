use enum_dispatch::enum_dispatch;
// TODO: impl these in exect first
// use consensus_util_mem::MallocSizeOf;
// use proptest_derive::Arbitrary;
use crate::{
    consensus::{now, TimestampMs},
    execution::{
        constants::EMPTY_RECEIPTS, proofs::EMPTY_ROOT, Address, Bloom, Bytes, Withdrawal, H256,
        U256,
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
        fee_recipient: Address,
        state_root: H256,
        receipts_root: H256,
        logs_bloom: Bloom,
        prev_randao: H256,
        block_number: u64,
        gas_limit: u64,
        gas_used: u64,
        extra_data: Bytes,
        base_fee_per_gas: U256,
        block_hash: H256,
        withdrawals: Option<Vec<Withdrawal>>,
    ) -> Self {
        Self::V1(MetadataV1 {
            created_at: now(),
            received_at: None,
            parent_hash,
            fee_recipient,
            state_root,
            receipts_root,
            logs_bloom,
            prev_randao,
            block_number,
            gas_limit,
            gas_used,
            extra_data,
            base_fee_per_gas,
            block_hash,
            withdrawals,
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
            fee_recipient: Address::zero(),
            state_root: EMPTY_ROOT,
            receipts_root: EMPTY_RECEIPTS,
            logs_bloom: Default::default(),
            prev_randao: Default::default(),
            block_number: Default::default(),
            gas_limit: Default::default(),
            gas_used: Default::default(),
            extra_data: Default::default(),
            base_fee_per_gas: Default::default(),
            block_hash: H256::zero(),
            withdrawals: Default::default(),
        })
    }
}

/// API for accessing fields for [VersionedMetadata] variants
///
/// TODO: update comments if leaving these fields in here.
#[enum_dispatch]
pub trait MetadataAPI {
    /// TODO
	fn created_at(&self) -> &TimestampMs;
    /// TODO
	fn set_created_at(&mut self, ts: TimestampMs);
    /// TODO
	fn received_at(&self) -> Option<TimestampMs>;
    /// TODO
	fn set_received_at(&mut self, ts: TimestampMs);

    // test these types - might be better in `BatchV1`
    /// TODO
	fn parent_hash(&self) -> H256;
    /// TODO
	fn fee_recipient(&self) -> Address;
    /// TODO
	fn state_root(&self) -> H256;
    /// TODO
	fn receipts_root(&self) -> H256;
    /// TODO
	fn logs_bloom(&self) -> Bloom;
    /// TODO
	fn prev_randao(&self) -> H256;
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
	fn extra_data(&self) -> Bytes;
    /// TODO
	fn base_fee_per_gas(&self) -> U256;
    /// TODO
	fn block_hash(&self) -> H256;
    /// TODO
	fn withdrawals(&self) -> Option<Vec<Withdrawal>>;
}

/// Metadata for batches.
#[cfg_attr(any(test, feature = "arbitrary"), derive(Arbitrary))]
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Default, Eq /* , MallocSizeOf */)]
pub struct MetadataV1 {
    /// Timestamp of when the entity created. This is generated
    /// by the node which creates the entity.
    pub created_at: TimestampMs,
    /// Timestamp of when the entity was received by another node. This will help
    /// calculate latencies that are not affected by clock drift or network
    /// delays. This field is not set for own batches.
    pub received_at: Option<TimestampMs>,

    // test these types - might be better in `BatchV1`
    //
    // used for casting into ExecutionPayload
    /// Hash of the last finalized block
    pub parent_hash: H256,

    /// This primary's address
    pub fee_recipient: Address,

    /// Value from EL
    pub state_root: H256,
    /// Value from EL
    pub receipts_root: H256,
    /// Value from EL
    pub logs_bloom: Bloom,
    /// Value from EL
    pub prev_randao: H256,
    /// Value from EL
    pub block_number: u64,
    /// Value from EL
    pub gas_limit: u64,
    /// Value from EL
    pub gas_used: u64,

    // just use created_at for now
    // pub timestamp: U64,
    /// Value from EL
    pub extra_data: Bytes,
    /// Value from EL
    pub base_fee_per_gas: U256,
    /// Value from EL
    pub block_hash: H256,

    // pub transactions: Vec<Bytes>,
    /// Block withdrawals
    pub withdrawals: Option<Vec<Withdrawal>>,
}

impl MetadataAPI for MetadataV1 {
    fn created_at(&self) -> &TimestampMs {
        &self.created_at
    }

    fn set_created_at(&mut self, ts: TimestampMs) {
        self.created_at = ts;
    }

    fn received_at(&self) -> Option<TimestampMs> {
        self.received_at
    }

    fn set_received_at(&mut self, ts: TimestampMs) {
        self.received_at = Some(ts);
    }

    // helper methods for validating batch in EL
    //
    // TODO: test these types - might be better in `BatchV1`

    fn parent_hash(&self) -> H256 {
        self.parent_hash
    }

    fn fee_recipient(&self) -> Address {
        self.fee_recipient
    }

    fn state_root(&self) -> H256 {
        self.state_root
    }

    fn receipts_root(&self) -> H256 {
        self.receipts_root
    }

    fn logs_bloom(&self) -> Bloom {
        self.logs_bloom
    }

    fn prev_randao(&self) -> H256 {
        self.prev_randao
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

    fn extra_data(&self) -> Bytes {
        self.extra_data.clone()
    }

    fn base_fee_per_gas(&self) -> U256 {
        self.base_fee_per_gas
    }

    fn block_hash(&self) -> H256 {
        self.block_hash
    }

    fn withdrawals(&self) -> Option<Vec<Withdrawal>> {
        self.withdrawals.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_v1() {
        let metadata = VersionedMetadata::default();
        assert_eq!(metadata.parent_hash(), H256::zero());
        assert_eq!(metadata.fee_recipient(), Address::zero());
        assert_eq!(metadata.withdrawals(), None);
        assert_eq!(metadata.state_root(), EMPTY_ROOT);
        assert_eq!(metadata.receipts_root(), EMPTY_RECEIPTS);
        assert_eq!(metadata.logs_bloom(), Bloom::default());
        assert_eq!(metadata.prev_randao(), Default::default());
        assert_eq!(metadata.block_number(), u64::default());
        assert_eq!(metadata.gas_limit(), u64::default());
        assert_eq!(metadata.gas_used(), u64::default());
        assert_eq!(metadata.extra_data(), Bytes::default());
        assert_eq!(metadata.base_fee_per_gas(), Default::default());
        assert_eq!(metadata.block_hash(), H256::zero());
    }
}
