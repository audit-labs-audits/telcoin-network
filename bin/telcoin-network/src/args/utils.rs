//! Utilities for parsing args

use crate::dirs::DataDirPath;
use reth::{args::utils::genesis_value_parser, dirs::MaybePlatformPath};
use reth_primitives::ChainSpec;
use std::{str::FromStr, sync::Arc};
use tn_types::yukon_chain_spec;

/// Create a default path for the node.
pub fn tn_platform_path(value: &str) -> eyre::Result<MaybePlatformPath<DataDirPath>> {
    let path = if value.is_empty() { "telcoin-network" } else { value };

    Ok(MaybePlatformPath::<DataDirPath>::from_str(path)?)
}

/// Defaults for chain spec clap parser.
///
/// Wrapper to intercept "yukon" chain spec. If not yukon, try reth's genesis_value_parser.
pub fn clap_genesis_parser(value: &str) -> eyre::Result<Arc<ChainSpec>, eyre::Error> {
    let chain = match value {
        "yukon" => yukon_chain_spec(),
        _ => genesis_value_parser(value)?,
    };

    Ok(chain)
}
