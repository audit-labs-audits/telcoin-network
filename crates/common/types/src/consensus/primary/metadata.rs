use enum_dispatch::enum_dispatch;
// TODO: impl these in exect first
// use consensus_util_mem::MallocSizeOf;
// use proptest_derive::Arbitrary;
use crate::execution::{H256, Address, Bloom, U64, Bytes, U256, Withdrawal, proofs::EMPTY_ROOT, constants::EMPTY_RECEIPTS};
use serde::{Deserialize, Serialize};
use crate::consensus::{TimestampMs, now};

// Additional metadata information for an entity.
//
// The structure as a whole is not signed. As a result this data
// should not be treated as trustworthy data and should be used
// for NON CRITICAL purposes only. For example should not be used
// for any processes that are part of our protocol that can affect
// safety or liveness.
//
// This is a versioned `Metadata` type
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq/*, Arbitrary, MallocSizeOf*/)]
#[enum_dispatch(MetadataAPI)]
pub enum VersionedMetadata {
    V1(MetadataV1),
}

impl VersionedMetadata {
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
        Self::V1(
            MetadataV1 {
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
            }
        )
    }
}

// Default implementation for `VersionedMetadata` used for testing only.
impl Default for VersionedMetadata {
    fn default() -> Self {
        Self::V1(
            MetadataV1 {
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
            }
        )
    }
}

#[enum_dispatch]
pub trait MetadataAPI {
    fn created_at(&self) -> &TimestampMs;
    fn set_created_at(&mut self, ts: TimestampMs);
    fn received_at(&self) -> Option<TimestampMs>;
    fn set_received_at(&mut self, ts: TimestampMs);


    // test these types - might be better in `BatchV1`
    fn parent_hash(&self) -> H256;
    fn fee_recipient(&self) -> Address;
    fn state_root(&self) -> H256;
    fn receipts_root(&self) -> H256;
    fn logs_bloom(&self) -> Bloom;
    fn prev_randao(&self) -> H256;
    fn block_number(&self) -> u64;
    fn gas_limit(&self) -> u64;
    fn gas_used(&self) -> u64;
    // use created_at, but return owned value
    fn timestamp(&self) -> u64;
    fn extra_data(&self) -> Bytes;
    fn base_fee_per_gas(&self) -> U256;
    fn block_hash(&self) -> H256;
    fn withdrawals(&self) -> Option<Vec<Withdrawal>>;
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Default, Eq/*, Abitrary, MallocSizeOf*/)]
pub struct MetadataV1 {
    // timestamp of when the entity created. This is generated
    // by the node which creates the entity.
    pub created_at: TimestampMs,
    // timestamp of when the entity was received by another node. This will help
    // us calculate latencies that are not affected by clock drift or network
    // delays. This field is not set for own batches.
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
