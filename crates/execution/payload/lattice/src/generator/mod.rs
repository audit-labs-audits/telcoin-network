//! Generator for Lattice Payloads.
//! 
//! Currently, this is broken up into two different traits. The reason for this is flexibility down the road.
//! It may be the case that Lattice needs separate generators, although I doubt it. This way is easier implement
//! for the time being.
//! 
//! If the same generator creates batch and header jobs, then refactor to use enum LatticePayloadJob::BatchPayload / ::HeaderPayload.
//! 
//! This module could be more DRY, but changes are anticipated in the near future.
//! So, verbose implementation for now.
use execution_rlp::Encodable;
use std::{
    time::Duration,
    sync::Arc,
};
use tn_types::execution::{
    bytes::{Bytes, BytesMut},
    constants::{
        EXECUTION_CLIENT_VERSION, 
        ETHEREUM_BLOCK_GAS_LIMIT,
    },
    ChainSpec,
};
use tokio::sync::Semaphore;
mod batch;
mod header;
mod block;

// pub enum LatticePayloadJob<Client, Pool, Tasks> {
//     Batch(BatchPayloadJob<Client, Pool, Tasks>),
//     Header(HeaderPayloadJob<Client, Pool, Tasks>),
// }

/// The [PayloadJobGenerator] that creates [BatchPayloadJob]s
/// and [HeaderPayloadJobs].
///
/// Responsible for initializing the block and environment for 
/// pending transactions to execute against.
///
/// The generator also
/// updates the transaction pool once a batch is sealed and
/// when a Header/Block is sealed (aka Certificate Issued).
pub struct LatticePayloadJobGenerator<Client, Pool, Tasks, Network> {
    /// The client that can interact with the chain.
    client: Client,
    /// txpool
    pool: Pool,
    /// How to spawn building tasks
    executor: Tasks,
    /// The configuration for the job generator.
    /// 
    /// TODO: actually use this
    config: LatticePayloadJobGeneratorConfig,
    /// Restricts how many generator tasks can be executed at once.
    payload_task_guard: PayloadTaskGuard,
    /// The chain spec.
    chain_spec: Arc<ChainSpec>,
    /// The network client to request missing batches from workers.
    network: Network,
}

// === impl LatticePayloadJobGenerator ===

impl<Client, Pool, Tasks, Network> LatticePayloadJobGenerator<Client, Pool, Tasks, Network> {
    /// Creates a new [LatticePayloadJobGenerator] with the given config.
    pub fn new(
        client: Client,
        pool: Pool,
        executor: Tasks,
        config: LatticePayloadJobGeneratorConfig,
        chain_spec: Arc<ChainSpec>,
        network: Network,
    ) -> Self {
        Self {
            client,
            pool,
            executor,
            payload_task_guard: PayloadTaskGuard::new(config.max_payload_tasks),
            config,
            chain_spec,
            network,
        }
    }
}

/// Restricts how many generator tasks can be executed at once.
#[derive(Clone)]
pub(crate) struct PayloadTaskGuard(pub(super) Arc<Semaphore>);

// === impl PayloadTaskGuard ===

impl PayloadTaskGuard {
    fn new(max_payload_tasks: usize) -> Self {
        Self(Arc::new(Semaphore::new(max_payload_tasks)))
    }
}

/// Settings for the [LatticePayloadJobGenerator].
#[derive(Debug, Clone)]
pub struct LatticePayloadJobGeneratorConfig {
    /// Data to include in the block's extra data field.
    extradata: Bytes,
    /// Target gas ceiling for built blocks, defaults to [ETHEREUM_BLOCK_GAS_LIMIT] gas.
    max_gas_limit: u64,
    /// The interval at which the job should build a new payload after the last.
    interval: Duration,
    /// Maximum number of tasks to spawn for building a payload.
    max_payload_tasks: usize,
}

// === impl LatticePayloadJobGeneratorConfig ===

impl LatticePayloadJobGeneratorConfig {
    /// Sets the interval at which the job should build a new payload after the last.
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Sets the maximum number of tasks to spawn for building a batch payload(s).
    ///
    /// # Panics
    ///
    /// If `max_payload_tasks` is 0.
    pub fn max_payload_tasks(mut self, max_payload_tasks: usize) -> Self {
        assert!(max_payload_tasks > 0, "max_payload_tasks must be greater than 0");
        self.max_payload_tasks = max_payload_tasks;
        self
    }

    /// Sets the target gas ceiling for mined blocks.
    ///
    /// Defaults to [ETHEREUM_BLOCK_GAS_LIMIT] gas.
    pub fn max_gas_limit(mut self, max_gas_limit: u64) -> Self {
        self.max_gas_limit = max_gas_limit;
        self
    }
}

impl Default for LatticePayloadJobGeneratorConfig {
    fn default() -> Self {
        let mut extradata = BytesMut::new();
        EXECUTION_CLIENT_VERSION.as_bytes().encode(&mut extradata);
        Self {
            extradata: extradata.freeze(),
            // TODO: set default size limits
            max_gas_limit: ETHEREUM_BLOCK_GAS_LIMIT,
            // TODO: remove?
            interval: Duration::from_secs(1),
            // only 1 batch can be built from the pending pool at a time
            max_payload_tasks: 1,
        }
    }
}
