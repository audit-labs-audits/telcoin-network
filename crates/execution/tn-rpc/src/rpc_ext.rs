//! RPC extension that supports state sync through NVV peer request.

use crate::{error::TelcoinNetworkRpcResult, EngineToPrimary};
use async_trait::async_trait;
use jsonrpsee::proc_macros::rpc;
use tn_reth::ChainSpec;
use tn_types::{ConsensusHeader, Genesis};

/// Telcoin Network RPC namespace.
///
/// TN-specific RPC endpoints.
#[rpc(server, namespace = "tn")]
pub trait TelcoinNetworkRpcExtApi {
    /// Return the latest consensus header.
    #[method(name = "latestHeader")]
    async fn latest_header(&self) -> TelcoinNetworkRpcResult<ConsensusHeader>;
    /// Return the chain genesis.
    #[method(name = "genesis")]
    async fn genesis(&self) -> TelcoinNetworkRpcResult<Genesis>;
}

/// The type that implements `tn` namespace trait.
pub struct TelcoinNetworkRpcExt<N: EngineToPrimary> {
    /// The chain id for this node.
    chain: ChainSpec,
    /// The inner-node network.
    ///
    /// The interface that handles primary <-> engine network communication.
    inner_node_network: N,
}

#[async_trait]
impl<N: EngineToPrimary> TelcoinNetworkRpcExtApiServer for TelcoinNetworkRpcExt<N>
where
    N: Send + Sync + 'static,
{
    async fn latest_header(&self) -> TelcoinNetworkRpcResult<ConsensusHeader> {
        Ok(self.inner_node_network.get_latest_consensus_block())
    }

    async fn genesis(&self) -> TelcoinNetworkRpcResult<Genesis> {
        Ok(self.chain.genesis().clone())
    }
}

impl<N: EngineToPrimary> TelcoinNetworkRpcExt<N> {
    /// Create new instance of the Telcoin Network RPC extension.
    pub fn new(chain: ChainSpec, inner_node_network: N) -> Self {
        Self { chain, inner_node_network }
    }
}
