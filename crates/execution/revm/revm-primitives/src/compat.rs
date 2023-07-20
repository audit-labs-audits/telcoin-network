use revm::primitives::{AccountInfo, Log};
use tn_types::execution::{Account, Log as RethLog, H160, H256, KECCAK_EMPTY};

/// Check equality between [`tn_types::execution::Log`] and [`revm::primitives::Log`]
pub fn is_log_equal(revm_log: &Log, execution_log: &tn_types::execution::Log) -> bool {
    revm_log.topics.len() == execution_log.topics.len() &&
        revm_log.address.0 == execution_log.address.0 &&
        revm_log.data == execution_log.data.0 &&
        !revm_log
            .topics
            .iter()
            .zip(execution_log.topics.iter())
            .any(|(revm_topic, execution_topic)| revm_topic.0 != execution_topic.0)
}

/// Into reth primitive [Log] from [revm::primitives::Log].
pub fn into_execution_log(log: Log) -> RethLog {
    RethLog {
        address: H160(log.address.0),
        topics: log.topics.into_iter().map(|h| H256(h.0)).collect(),
        data: log.data.into(),
    }
}

/// Create reth primitive [Account] from [revm::primitives::AccountInfo].
/// Check if revm bytecode hash is [KECCAK_EMPTY] and put None to reth [Account]
pub fn to_execution_acc(revm_acc: &AccountInfo) -> Account {
    let code_hash = revm_acc.code_hash;
    Account {
        balance: revm_acc.balance,
        nonce: revm_acc.nonce,
        bytecode_hash: (code_hash != KECCAK_EMPTY).then_some(code_hash),
    }
}
