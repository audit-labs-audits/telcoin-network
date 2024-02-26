//! Test-utilities for execution/engine node.

use clap::{Args, Parser};
use reth::{
    cli::ext::DefaultRethNodeCommandConfig,
    dirs::MaybePlatformPath,
    primitives::{Address, ChainSpec},
};
use std::{str::FromStr, sync::Arc};
use telcoin_network::node::NodeCommand;
use tempfile::tempdir;
use tn_faucet::FaucetCliExt;
use tn_node::engine::ExecutionNode;
use tn_types::AuthorityIdentifier;

/// A helper type to parse Args more easily.
#[derive(Parser, Debug)]
pub struct CommandParser<T: Args> {
    #[clap(flatten)]
    pub args: T,
}

/// Convenience function for creating engine node using tempdir and optional args.
/// Defaults if params not provided:
/// - opt_authority_identifier: `AuthorityIdentifier(1)`
/// - opt_chain: `adiri`
/// - opt_address: `0x1111111111111111111111111111111111111111`
pub fn default_test_execution_node(
    opt_authority_identifier: Option<AuthorityIdentifier>,
    opt_chain: Option<Arc<ChainSpec>>,
    opt_address: Option<Address>,
) -> eyre::Result<ExecutionNode<()>> {
    let tempdir = tempdir().expect("tempdir created").into_path();

    // use same approach as telcoin-network binary
    let command = NodeCommand::<()>::try_parse_from([
        "telcoin-network",
        // "node",
        "--dev",
        "--chain",
        "adiri",
        "--datadir",
        tempdir.to_str().expect("tempdir path clean"),
    ])?;

    let NodeCommand::<()> {
        datadir,
        config,
        chain,
        metrics,
        trusted_setup_file,
        instance,
        network,
        rpc,
        txpool,
        builder,
        debug,
        db,
        dev,
        pruning,
        ..
    } = command;

    // let ext: Ext::Node = NoArgs::with(());
    // let params = reth::node::NodeCommand::<FaucetCliExt> {
    let params = reth::node::NodeCommand {
        datadir: MaybePlatformPath::from_str(&datadir.to_string())
            .expect("datadir compatible with platform path"),
        config,
        chain: chain.clone(),
        metrics,
        instance,
        trusted_setup_file,
        network,
        rpc,
        txpool,
        builder,
        debug,
        db,
        dev,
        pruning,
        ext: DefaultRethNodeCommandConfig::default(),
        // ext: faucet,
        // ext: NoArgs::with(()),
        // ext: Default::default(),
    };

    // check args then use test defaults
    let authority_identifier = opt_authority_identifier.unwrap_or(AuthorityIdentifier(1));
    let chain = opt_chain.unwrap_or(chain);
    let address = opt_address.unwrap_or_else(|| {
        Address::from_str("0x1111111111111111111111111111111111111111").expect("address from 0x1s")
    });

    // create engine node
    let engine = ExecutionNode::new(authority_identifier, chain, address, params)?;

    Ok(engine)
}

/// Create CLI command for tests calling `ExecutionNode::new`.
pub fn execution_params() -> eyre::Result<reth::node::NodeCommand<FaucetCliExt>> {
    let tempdir = tempdir().expect("tempdir created").into_path();

    // use same approach as telcoin-network binary
    let command = NodeCommand::<FaucetCliExt>::try_parse_from([
        "telcoin-network",
        "node",
        "--dev",
        "--chain",
        "adiri",
        "--datadir",
        tempdir.to_str().expect("tempdir path clean"),
    ])?;

    let NodeCommand::<FaucetCliExt> {
        datadir,
        config,
        chain,
        metrics,
        trusted_setup_file,
        instance,
        network,
        rpc,
        txpool,
        builder,
        debug,
        db,
        dev,
        pruning,
        ext,
        ..
    } = command;

    let params = reth::node::NodeCommand::<FaucetCliExt> {
        datadir: MaybePlatformPath::from_str(&datadir.to_string())
            .expect("datadir compatible with platform path"),
        config,
        chain: chain.clone(),
        metrics,
        instance,
        trusted_setup_file,
        network,
        rpc,
        txpool,
        builder,
        debug,
        db,
        dev,
        pruning,
        ext,
    };

    Ok(params)
}

/// Convenience function for creating engine node using tempdir and optional args.
/// Defaults if params not provided:
/// - opt_authority_identifier: `AuthorityIdentifier(1)`
/// - opt_chain: `adiri`
/// - opt_address: `0x1111111111111111111111111111111111111111`
// #[cfg(feature = "faucet")]
pub fn faucet_test_execution_node(
    google_kms: bool,
    opt_authority_identifier: Option<AuthorityIdentifier>,
    opt_chain: Option<Arc<ChainSpec>>,
    opt_address: Option<Address>,
) -> eyre::Result<ExecutionNode<FaucetCliExt>> {
    let tempdir = tempdir().expect("tempdir created").into_path();

    // use same approach as telcoin-network binary
    let command = NodeCommand::<FaucetCliExt>::try_parse_from([
        "telcoin-network",
        // "node",
        "--dev",
        "--chain",
        "adiri",
        "--datadir",
        tempdir.to_str().expect("tempdir path clean"),
        // pass -google-kms flag
        (if google_kms { "--google-kms" } else { "" }),
    ])?;

    let NodeCommand::<FaucetCliExt> {
        datadir,
        config,
        chain,
        metrics,
        trusted_setup_file,
        instance,
        network,
        rpc,
        txpool,
        builder,
        debug,
        db,
        dev,
        pruning,
        ext,
        ..
    } = command;

    // let ext: Ext::Node = NoArgs::with(());
    // let params = reth::node::NodeCommand::<FaucetCliExt> {
    let params = reth::node::NodeCommand::<FaucetCliExt> {
        datadir: MaybePlatformPath::from_str(&datadir.to_string())
            .expect("datadir compatible with platform path"),
        config,
        chain: chain.clone(),
        metrics,
        instance,
        trusted_setup_file,
        network,
        rpc,
        txpool,
        builder,
        debug,
        db,
        dev,
        pruning,
        ext,
    };

    // check args then use test defaults
    let authority_identifier = opt_authority_identifier.unwrap_or(AuthorityIdentifier(1));
    let chain = opt_chain.unwrap_or(chain);
    let address = opt_address.unwrap_or_else(|| {
        Address::from_str("0x1111111111111111111111111111111111111111").expect("address from 0x1s")
    });

    // create engine node
    let engine = ExecutionNode::new(authority_identifier, chain, address, params)?;

    Ok(engine)
}
