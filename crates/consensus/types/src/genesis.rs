//! Genesis helper methods.
//!
//! The yaml, chainspec, and Genesis struct are used for all
//! testing purposes.
//!
//! Yukon is the current name for multi-node testnet.

use std::sync::Arc;

use reth_primitives::{ChainSpec, Genesis};

/// Yukon parsed Genesis.
pub fn yukon_genesis() -> Genesis {
    let yaml = yukon_genesis_string();
    serde_json::from_str(&yaml).expect("serde parse valid yukon yaml")
}

/// Yukon chain spec parsed from genesis and ready to go.
pub fn yukon_chain_spec() -> Arc<ChainSpec> {
    let genesis = yukon_genesis();
    Arc::new(genesis.into())
}

/// Yukon genesis string in yaml format.
pub fn yukon_genesis_string() -> String {
    r#"
{
    "nonce": "0x0",
    "timestamp": "0x6553A8CC",
    "extraData": "0x5343",
    "gasLimit": "0x1c9c380",
    "difficulty": "0x0",
    "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "coinbase": "0x0000000000000000000000000000000000000000",
    "alloc": {
        "0x6Be02d1d3665660d22FF9624b7BE0551ee1Ac91b": {
            "balance": "0x4a47e3c12448f4ad000000"
        }
    },
    "number": "0x0",
    "gasUsed": "0x0",
    "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "config": {
        "ethash": {},
        "chainId": 2600,
        "homesteadBlock": 0,
        "eip150Block": 0,
        "eip155Block": 0,
        "eip158Block": 0,
        "byzantiumBlock": 0,
        "constantinopleBlock": 0,
        "petersburgBlock": 0,
        "istanbulBlock": 0,
        "berlinBlock": 0,
        "londonBlock": 0,
        "terminalTotalDifficulty": 0,
        "terminalTotalDifficultyPassed": true,
        "shanghaiTime": 0
    }
}"#
    .to_string()
}
