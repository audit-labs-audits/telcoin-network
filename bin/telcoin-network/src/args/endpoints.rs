use clap::Args;

#[derive(Args, Clone, PartialEq, Default, Debug)]
#[command(next_help_heading = "Gateway")]
/// Endpoing arguments contain all ports for creating the ZMQ sockets used
/// to communicate between Gateway, Engine, EthRPC, Worker, and Primary.
///
/// The order of arguments are organized based on the flow of a transaction
/// through the system.
pub struct EndpointArgs {
    /// Port for the ZMQ socket communication from the EthRPC to the Gateway.
    #[arg(long = "ethrpc-to-gateway.endpoint", default_value_t = 18545)]
    pub eth_gateway_endpoint: u16,

    // TODO: this is only configured for one worker needed for DEMO
    /// Port for the ZMQ socket communication from the Gateway to a single Worker.
    #[arg(long = "gateway-to-worker.endpoint", default_value_t = 28545)]
    pub gateway_worker_endpoint: u16,

    /// Port for the ZMQ socket communication from a single Worker to the Primary.
    #[arg(long = "worker-to-primary.endpoint", default_value_t = 29545)]
    pub worker_primary_endpoint: u16,

    /// Port for the ZMQ socket communication from the Primary to the Gateway.
    #[arg(long = "primary-to-gateway.endpoint", default_value_t = 38545)]
    pub primary_gateway_endpoint: u16,

    /// Port for the ZMQ socket communication from the gateway to the Engine.
    #[arg(long = "gateway-to-engine.endpoint", default_value_t = 48545)]
    pub gateway_engine_endpoint: u16,
    // TODO:
    //  - worker to worker
    //  - primary to worker
    //  - primary to primary
}
