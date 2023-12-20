//! Genesis helper methods.
//!
//! The yaml, chainspec, and Genesis struct are used for all
//! testing purposes.
//!
//! Yukon is the current name for multi-node testnet.

use clap::Parser;
use reth::node::NodeCommand;
use reth_primitives::{ChainSpec, Genesis};
use std::sync::Arc;

/// Return a [NodeCommand] with default args parsed by `clap`.
pub fn execution_args() -> NodeCommand {
    NodeCommand::<()>::try_parse_from(["reth", "--dev", "--chain", &yukon_genesis_string()])
        .expect("clap parse node command")
}

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
///
/// Seed "Bob" and [0; 32] seed addresses.
pub fn yukon_genesis_string() -> String {
    yukon_genesis_raw().to_string()
}

/// Static strig for yukon genesis.
///
/// Used by CLI and other methods above.
pub fn yukon_genesis_raw() -> &'static str {
    r#"
{
    "nonce": "0x0",
    "timestamp": "0x6553A8CC",
    "extraData": "0x21",
    "gasLimit": "0x1c9c380",
    "difficulty": "0x0",
    "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "coinbase": "0x0000000000000000000000000000000000000000",
    "alloc": {
        "0x6Be02d1d3665660d22FF9624b7BE0551ee1Ac91b": {
            "balance": "0x4a47e3c12448f4ad000000"
        },
        "0xb14d3c4f5fbfbcfb98af2d330000d49c95b93aa7": {
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
}
