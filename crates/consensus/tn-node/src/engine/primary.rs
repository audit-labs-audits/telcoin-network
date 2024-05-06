//! Node implementation for reth compatibility
//!
//! Inspired by reth_node_ethereum crate.

use reth_db::database::Database;
use reth_node_builder::{
    components::ComponentsBuilder,
    node::{FullNodeTypes, NodeTypes},
};
use reth_node_ethereum::{
    node::{EthereumNetworkBuilder, EthereumPayloadBuilder, EthereumPoolBuilder},
    EthEngineTypes, EthEvmConfig,
};
use reth_provider::FullProvider;
use std::marker::PhantomData;

/// Type configuration for a regular Telcoin node.
#[derive(Debug, Default, Clone, Copy)]
#[non_exhaustive]
pub struct PrimaryNode<DB, Provider> {
    db: PhantomData<DB>,
    evm: PhantomData<Provider>,
}

impl<DB, Evm> PrimaryNode<DB, Evm> {
    /// Returns an execution layer's [ComponentsBuilder] configured for a Worker node.
    pub fn components<Node>(
    ) -> ComponentsBuilder<Node, EthereumPoolBuilder, EthereumPayloadBuilder, EthereumNetworkBuilder>
    where
        Node: FullNodeTypes<Engine = EthEngineTypes>,
    {
        ComponentsBuilder::default()
            .node_types::<Node>()
            .pool(EthereumPoolBuilder::default())
            .payload(EthereumPayloadBuilder::default())
            .network(EthereumNetworkBuilder::default())
    }
}

impl<DB, Provider> NodeTypes for PrimaryNode<DB, Provider>
where
    DB: Send + Sync + 'static,
    Provider: Send + Sync + 'static,
{
    type Primitives = ();
    type Engine = EthEngineTypes;
    type Evm = EthEvmConfig;

    fn evm_config(&self) -> Self::Evm {
        EthEvmConfig::default()
    }
}

impl<DB, Provider> FullNodeTypes for PrimaryNode<DB, Provider>
where
    DB: Database + Clone + 'static,
    Provider: FullProvider<DB>,
{
    type DB = DB;
    type Provider = Provider;
}
