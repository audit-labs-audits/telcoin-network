//! Batch payload types for the engine
use execution_rlp::Decodable;
use serde::{ser::SerializeMap, Deserialize, Serialize, Serializer};
use tn_types::{
    consensus::{Batch, BatchAPI, BatchV1, MetadataAPI, MetadataV1, VersionedMetadata},
    execution::{
        constants::{MAXIMUM_EXTRA_DATA_SIZE, MIN_PROTOCOL_BASE_FEE_U256},
        proofs::{self, EMPTY_LIST_HASH, EMPTY_ROOT},
        Address, Block, Bloom, Bytes, Header, SealedBlock, TransactionSigned, UintTryTo,
        Withdrawal, H256, H64, U256, U64, H160,
    },
};

/// This structure maps for the return value of `engine_getPayloadV2` of the beacon chain spec.
///
/// See also: <https://github.com/ethereum/execution-apis/blob/main/src/engine/shanghai.md#engine_getpayloadv2>
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchExecutionPayloadEnvelope {
    /// Batch execution payload
    #[serde(rename = "executionPayload")]
    pub payload: BatchExecutionPayload,
    /// The expected value to be received by the feeRecipient in tel wei
    #[serde(rename = "blockValue")]
    pub batch_value: U256,
}

/// This structure maps on the ExecutionPayload structure of the beacon chain spec.
///
/// See also: <https://github.com/ethereum/execution-apis/blob/6709c2a795b707202e93c4f2867fa0bf2640a84f/src/engine/paris.md#executionpayloadv1>
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
// #[serde(rename_all = "camelCase")]
pub struct BatchExecutionPayload {
    pub parent_hash: H256,
    pub receipts_root: H256,
    pub logs_bloom: Bloom,
    pub block_number: U64,
    pub gas_limit: U64,
    pub gas_used: U64,
    pub timestamp: U64,
    pub base_fee_per_gas: U256,
    pub block_hash: H256,
    pub transactions: Vec<Bytes>,
    // TODO: assumed to be unnecessary
    //
    // pub fee_recipient: Address,
    // pub state_root: H256,
    // pub prev_randao: H256,
    // pub extra_data: Bytes,
    // /// Array of [`Withdrawal`] enabled with V2
    // /// See <https://github.com/ethereum/execution-apis/blob/6709c2a795b707202e93c4f2867fa0bf2640a84f/src/engine/shanghai.md#executionpayloadv2>
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // pub withdrawals: Option<Vec<Withdrawal>>,
}

impl From<Batch> for BatchExecutionPayload {
    fn from(batch: Batch) -> Self {
        // TODO: use Bytes in CL
        // map Vec<u8> to Vec<Bytes>
        let transactions = batch.owned_transactions().into_iter().map(|tx| tx.into()).collect();

        let value = batch.versioned_metadata();

        BatchExecutionPayload {
            parent_hash: value.parent_hash(),
            receipts_root: value.receipts_root(),
            logs_bloom: value.logs_bloom(),
            block_number: value.block_number().to_owned().into(),
            gas_limit: value.gas_limit().into(),
            gas_used: value.gas_used().into(),
            timestamp: value.timestamp().into(),
            base_fee_per_gas: value.base_fee_per_gas(),
            block_hash: value.block_hash(),
            transactions,
            // extra_data: value.extra_data(),
            // fee_recipient: H256::zero(),
            // prev_randao: value.prev_randao(),
            // state_root: EMPTY_ROOT,
            // withdrawals: value.withdrawals(),
        }
    }
}

impl From<SealedBlock> for BatchExecutionPayload {
    fn from(value: SealedBlock) -> Self {
        let transactions = value
            .body
            .iter()
            .map(|tx| {
                let mut encoded = Vec::new();
                tx.encode_enveloped(&mut encoded);
                encoded.into()
            })
            .collect();
        BatchExecutionPayload {
            parent_hash: value.parent_hash,
            receipts_root: value.receipts_root,
            logs_bloom: value.logs_bloom,
            block_number: value.number.into(),
            gas_limit: value.gas_limit.into(),
            gas_used: value.gas_used.into(),
            timestamp: value.timestamp.into(),
            base_fee_per_gas: U256::from(value.base_fee_per_gas.unwrap_or_default()),
            block_hash: value.hash(),
            transactions,
            // prev_randao: value.mix_hash,
            // extra_data: value.extra_data.clone(),
            // fee_recipient: value.beneficiary,
            // state_root: value.state_root,
            // withdrawals: value.withdrawals,
        }
    }
}

/// Try to construct a block from given payload. Perform addition validation of `extra_data` and
/// `base_fee_per_gas` fields.
///
/// NOTE: The log bloom is assumed to be validated during serialization.
/// NOTE: Empty ommers, nonce and difficulty values are validated upon computing block hash and
/// comparing the value with `payload.block_hash`.
///
/// See <https://github.com/ethereum/go-ethereum/blob/79a478bb6176425c2400e949890e668a3d9a3d05/core/beacon/types.go#L145>
impl TryFrom<BatchExecutionPayload> for SealedBlock {
    type Error = BatchPayloadError;

    fn try_from(payload: BatchExecutionPayload) -> Result<Self, Self::Error> {
        if payload.base_fee_per_gas < MIN_PROTOCOL_BASE_FEE_U256 {
            return Err(BatchPayloadError::BaseFee(payload.base_fee_per_gas))
        }

        let transactions = payload
            .transactions
            .iter()
            .map(|tx| TransactionSigned::decode(&mut tx.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        let transactions_root = proofs::calculate_transaction_root(&transactions);

        let withdrawals_root = Some(EMPTY_ROOT);

        let header = Header {
            parent_hash: payload.parent_hash,
            transactions_root,
            receipts_root: payload.receipts_root,
            withdrawals_root,
            logs_bloom: payload.logs_bloom,
            number: payload.block_number.as_u64(),
            gas_limit: payload.gas_limit.as_u64(),
            gas_used: payload.gas_used.as_u64(),
            timestamp: payload.timestamp.as_u64(),
            mix_hash: H256::zero(),
            base_fee_per_gas: Some(
                payload
                    .base_fee_per_gas
                    .uint_try_to()
                    .map_err(|_| BatchPayloadError::BaseFee(payload.base_fee_per_gas))?,
            ),
            // Defaults
            beneficiary: H160::zero(),
            state_root: EMPTY_ROOT,
            extra_data: Bytes::default(),
            ommers_hash: EMPTY_LIST_HASH,
            difficulty: Default::default(),
            nonce: Default::default(),
        }
        .seal_slow();

        if payload.block_hash != header.hash() {
            return Err(BatchPayloadError::BlockHash {
                execution: header.hash(),
                consensus: payload.block_hash,
            })
        }

        Ok(SealedBlock {
            header,
            body: transactions,
            withdrawals: Some(vec![]),
            ommers: Default::default(),
        })
    }
}


/// Error that can occur when handling payloads.
#[derive(thiserror::Error, Debug)]
pub enum BatchPayloadError {
    /// Invalid payload extra data.
    #[error("Invalid payload extra data: {0}")]
    ExtraData(Bytes),
    /// Invalid payload base fee.
    #[error("Invalid payload base fee: {0}")]
    BaseFee(U256),
    /// Invalid payload block hash.
    #[error("blockhash mismatch, want {consensus}, got {execution}")]
    BlockHash {
        /// The block hash computed from the payload.
        execution: H256,
        /// The block hash provided with the payload.
        consensus: H256,
    },
    /// Encountered decoding error.
    #[error(transparent)]
    Decode(#[from] execution_rlp::DecodeError),
}

impl BatchPayloadError {
    /// Returns `true` if the error is caused by invalid extra data.
    pub fn is_block_hash_mismatch(&self) -> bool {
        matches!(self, BatchPayloadError::BlockHash { .. })
    }
}

// /// This structure contains a body of an execution payload.
// ///
// /// See also: <https://github.com/ethereum/execution-apis/blob/6452a6b194d7db269bf1dbd087a267251d3cc7f8/src/engine/shanghai.md#executionpayloadbodyv1>
// #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
// pub struct ExecutionPayloadBody {
//     pub transactions: Vec<Bytes>,
//     pub withdrawals: Vec<Withdrawal>,
// }

// impl From<Block> for ExecutionPayloadBody {
//     fn from(value: Block) -> Self {
//         let transactions = value.body.into_iter().map(|tx| {
//             let mut out = Vec::new();
//             tx.encode_enveloped(&mut out);
//             out.into()
//         });
//         ExecutionPayloadBody {
//             transactions: transactions.collect(),
//             withdrawals: value.withdrawals.unwrap_or_default(),
//         }
//     }
// }

/// This structure contains the attributes required to initiate a payload build process in the
/// context of an `engine_forkchoiceUpdated` call.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchPayloadAttributes {
    pub timestamp: U64,
    pub prev_randao: H256,
    pub suggested_fee_recipient: Address,
    /// Array of [`Withdrawal`] enabled with V2
    /// See <https://github.com/ethereum/execution-apis/blob/6452a6b194d7db269bf1dbd087a267251d3cc7f8/src/engine/shanghai.md#payloadattributesv2>
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub withdrawals: Option<Vec<Withdrawal>>,
}

/// The status of a peer's batch.
/// 
/// akin to PayloadStatusEnum
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BatchPayloadStatus {
    /// VALID is returned by the engine API in the following calls:
    ///   - newPayloadV1:       if the payload was already known or was just validated and executed
    ///   - forkchoiceUpdateV1: if the chain accepted the reorg (might ignore if it's stale)
    Valid,

    /// INVALID is returned by the engine API in the following calls:
    ///   - newPayloadV1:       if the payload failed to execute on top of the local chain
    ///   - forkchoiceUpdateV1: if the new head is unknown, pre-merge, or reorg to it fails
    Invalid {
        #[serde(rename = "validationError")]
        validation_error: String,
    },
}

impl BatchPayloadStatus {
    /// Returns the string representation of the payload status.
    pub fn as_str(&self) -> &'static str {
        match self {
            BatchPayloadStatus::Valid => "VALID",
            BatchPayloadStatus::Invalid { .. } => "INVALID",
        }
    }

    /// Returns the validation error if the payload status is invalid.
    pub fn validation_error(&self) -> Option<&str> {
        match self {
            BatchPayloadStatus::Invalid { validation_error } => Some(validation_error),
            _ => None,
        }
    }

    /// Returns true if the payload status is valid.
    pub fn is_valid(&self) -> bool {
        matches!(self, BatchPayloadStatus::Valid)
    }

    /// Returns true if the payload status is invalid.
    pub fn is_invalid(&self) -> bool {
        matches!(self, BatchPayloadStatus::Invalid { .. })
    }
}

impl From<BatchPayloadError> for BatchPayloadStatus {
    fn from(error: BatchPayloadError) -> Self {
        BatchPayloadStatus::Invalid { validation_error: error.to_string() }
    }
}

impl std::fmt::Display for BatchPayloadStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BatchPayloadStatus::Invalid { validation_error } => {
                f.write_str(self.as_str())?;
                f.write_str(": ")?;
                f.write_str(validation_error.as_str())
            }
            _ => f.write_str(self.as_str()),
        }
    }
}

/// Various errors that can occur when validating a payload or forkchoice update.
///
/// This is intended for the [PayloadStatusEnum::Invalid] variant.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BatchPayloadValidationError {
    /// Thrown when a forkchoice update's head links to a previously rejected payload.
    #[error("links to previously rejected block")]
    LinksToRejectedPayload,
    /// Thrown when a new payload contains a wrong block number.
    #[error("invalid block number")]
    InvalidBlockNumber,
    /// Thrown when a new payload contains a wrong state root
    #[error("invalid merkle root (remote: {remote:?} local: {local:?})")]
    InvalidStateRoot {
        /// The state root of the payload we received from remote (CL)
        remote: H256,
        /// The state root of the payload that we computed locally.
        local: H256,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use execution_interfaces::test_utils::generators::{
        self, random_block, random_block_range, random_header,
    };
    use execution_rlp::{Decodable, DecodeError};
    use tn_types::execution::{
        bytes::{Bytes, BytesMut},
        TransactionSigned, H256,
    };

    fn transform_block<F: FnOnce(Block) -> Block>(src: SealedBlock, f: F) -> BatchExecutionPayload {
        let unsealed = src.unseal();
        let mut transformed: Block = f(unsealed);
        // Recalculate roots
        transformed.header.transactions_root =
            proofs::calculate_transaction_root(&transformed.body);
        transformed.header.ommers_hash = proofs::calculate_ommers_root(&transformed.ommers);
        SealedBlock {
            header: transformed.header.seal_slow(),
            body: transformed.body,
            ommers: transformed.ommers,
            withdrawals: transformed.withdrawals,
        }
        .into()
    }

    // #[test]
    // fn payload_body_roundtrip() {
    //     let mut rng = generators::rng();
    //     for block in random_block_range(&mut rng, 0..=99, H256::default(), 0..2) {
    //         let unsealed = block.clone().unseal();
    //         let payload_body: BatchExecutionPayloadBody = unsealed.into();

    //         assert_eq!(
    //             Ok(block.body),
    //             payload_body
    //                 .transactions
    //                 .iter()
    //                 .map(|x| TransactionSigned::decode(&mut &x[..]))
    //                 .collect::<Result<Vec<_>, _>>(),
    //         );

    //         assert_eq!(block.withdrawals.unwrap_or_default(), payload_body.withdrawals);
    //     }
    // }

    #[test]
    fn payload_validation() {
        let mut rng = generators::rng();
        let block = random_block(&mut rng, 100, Some(H256::random()), Some(3), Some(0));

        // Valid extra data
        let block_with_valid_extra_data = transform_block(block.clone(), |mut b| {
            b.header.extra_data = BytesMut::zeroed(32).freeze().into();
            b
        });
        assert_matches!(TryInto::<SealedBlock>::try_into(block_with_valid_extra_data), Ok(_));

        // Invalid extra data
        let block_with_invalid_extra_data: Bytes = BytesMut::zeroed(33).freeze();
        let invalid_extra_data_block = transform_block(block.clone(), |mut b| {
            b.header.extra_data = block_with_invalid_extra_data.clone().into();
            b
        });
        assert_matches!(
            TryInto::<SealedBlock>::try_into(invalid_extra_data_block),
            Err(BatchPayloadError::ExtraData(data)) if data == block_with_invalid_extra_data
        );

        // Zero base fee
        let block_with_zero_base_fee = transform_block(block.clone(), |mut b| {
            b.header.base_fee_per_gas = Some(0);
            b
        });
        assert_matches!(
            TryInto::<SealedBlock>::try_into(block_with_zero_base_fee),
            Err(BatchPayloadError::BaseFee(val)) if val == U256::ZERO
        );

        // Invalid encoded transactions
        let mut payload_with_invalid_txs: BatchExecutionPayload = block.clone().into();
        payload_with_invalid_txs.transactions.iter_mut().for_each(|tx| {
            *tx = Bytes::new().into();
        });
        assert_matches!(
            TryInto::<SealedBlock>::try_into(payload_with_invalid_txs),
            Err(BatchPayloadError::Decode(DecodeError::InputTooShort))
        );

        // Non empty ommers
        let block_with_ommers = transform_block(block.clone(), |mut b| {
            b.ommers.push(random_header(&mut rng, 100, None).unseal());
            b
        });
        assert_matches!(
            TryInto::<SealedBlock>::try_into(block_with_ommers.clone()),
            Err(BatchPayloadError::BlockHash { consensus, .. })
                if consensus == block_with_ommers.block_hash
        );

        // None zero difficulty
        let block_with_difficulty = transform_block(block.clone(), |mut b| {
            b.header.difficulty = U256::from(1);
            b
        });
        assert_matches!(
            TryInto::<SealedBlock>::try_into(block_with_difficulty.clone()),
            Err(BatchPayloadError::BlockHash { consensus, .. }) if consensus == block_with_difficulty.block_hash
        );

        // None zero nonce
        let block_with_nonce = transform_block(block.clone(), |mut b| {
            b.header.nonce = 1;
            b
        });
        assert_matches!(
            TryInto::<SealedBlock>::try_into(block_with_nonce.clone()),
            Err(BatchPayloadError::BlockHash { consensus, .. }) if consensus == block_with_nonce.block_hash
        );

        // Valid block
        let valid_block = block;
        assert_matches!(TryInto::<SealedBlock>::try_into(valid_block), Ok(_));
    }
}
