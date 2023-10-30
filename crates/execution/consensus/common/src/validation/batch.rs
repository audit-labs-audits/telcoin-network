//! Collection of methods for block validation.
use execution_interfaces::{consensus::ConsensusError, Result as RethResult};
use execution_provider::{AccountReader, HeaderProvider, WithdrawalsProvider};
use std::{
    collections::{hash_map::Entry, HashMap},
    time::SystemTime,
};
use tn_types::execution::{
    constants, BlockNumber, ChainSpec, Hardfork, Header, InvalidTransactionError, SealedBlock,
    SealedHeader, Transaction, TransactionSignedEcRecovered, TxEip1559, TxEip2930, TxLegacy,
};

/// Validate batch standalone
pub fn validate_batch_standalone(
    header: &SealedHeader,
    chain_spec: &ChainSpec,
) -> Result<(), ConsensusError> {

    // akin to validate_header_standalone for beacon

    // Gas used needs to be less then gas limit. Gas used is going to be check after execution.
    if header.gas_used > header.gas_limit {
        return Err(ConsensusError::HeaderGasUsedExceedsGasLimit {
            gas_used: header.gas_used,
            gas_limit: header.gas_limit,
        })
    }

    // Check if timestamp is in future. Clock can drift but this can be consensus issue.
    let present_timestamp =
        SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
    if header.timestamp > present_timestamp {
        return Err(ConsensusError::TimestampIsInFuture {
            timestamp: header.timestamp,
            present_timestamp,
        })
    }

    // Check if base fee is set.
    if chain_spec.fork(Hardfork::London).active_at_block(header.number) &&
        header.base_fee_per_gas.is_none()
    {
        return Err(ConsensusError::BaseFeeMissing)
    }

    Ok(())
}

/// Validate batch in regards to parent
pub fn validate_batch_regarding_parent(
    parent: &SealedHeader,
    batch: &SealedHeader,
) -> Result<(), ConsensusError> {

    if parent.hash != batch.parent_hash {
        return Err(ConsensusError::ParentHashMismatch {
            expected_parent_hash: parent.hash,
            got_parent_hash: batch.parent_hash,
        })
    }

    // timestamp in past check
    if batch.timestamp <= parent.timestamp {
        return Err(ConsensusError::TimestampIsInPast {
            parent_timestamp: parent.timestamp,
            timestamp: batch.timestamp,
        })
    }

    // TODO: check gas at the batch level?

    // let mut parent_gas_limit = parent.gas_limit;
    // // Check gas limit, max diff between child/parent gas_limit should be  max_diff=parent_gas/1024
    // if child.gas_limit > parent_gas_limit {
    //     if child.gas_limit - parent_gas_limit >= parent_gas_limit / 1024 {
    //         return Err(ConsensusError::GasLimitInvalidIncrease {
    //             parent_gas_limit,
    //             child_gas_limit: child.gas_limit,
    //         })
    //     }
    // } else if parent_gas_limit - child.gas_limit >= parent_gas_limit / 1024 {
    //     return Err(ConsensusError::GasLimitInvalidDecrease {
    //         parent_gas_limit,
    //         child_gas_limit: child.gas_limit,
    //     })
    // }

    // Only genesis can have parent hash of 0x0
    if batch.parent_hash.is_zero() && batch.number != 1 {
        return Err(ConsensusError::ParentUnknown { hash: batch.parent_hash })
    }

    Ok(())
}
