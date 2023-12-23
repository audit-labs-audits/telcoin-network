//! Configurations for the Telcoin Network.

use fastcrypto::traits::{InsecureDefault, ToFromBytes};
use serde::{Deserialize, Serialize};
use std::{num::NonZeroU32, path::PathBuf, time::Duration};
use tn_types::{
    utils::get_available_port, BlsPublicKey, ExecutionPublicKey, Multiaddr, NetworkPublicKey,
    WorkerIndex,
};
use tracing::info;

/// Configuration for the Telcoin Network node.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Serialize)]
// #[serde(default)]
pub struct Config {
    /// The path where keys are stored, if successfully generated.
    ///
    /// Note: `PathBuf` default is "".
    pub keypath: PathBuf,
    /// Configuration for the authority.
    ///
    /// Validator information.
    pub authority: AuthorityConfig,
    /// Parameters for the network.
    pub parameters: Parameters,
    /// Configuration for the workers for this authority.
    ///
    /// Note: due to TOML constraints, maps must be the last
    /// serialized value.   
    pub workers: WorkerIndex,
}

impl Config {
    /// Update the authority protocol key.
    pub fn update_protocol_key(&mut self, value: BlsPublicKey) -> eyre::Result<()> {
        self.authority.protocol_key = value;
        Ok(())
    }

    /// Update the authority network key.
    pub fn update_network_key(&mut self, value: NetworkPublicKey) -> eyre::Result<()> {
        self.authority.network_key = value;
        Ok(())
    }

    /// Update the authority execution key.
    pub fn update_execution_key(&mut self, value: ExecutionPublicKey) -> eyre::Result<()> {
        self.authority.execution_key = value;
        Ok(())
    }
}

/// Configuration for this authority.
#[derive(Debug, Clone, Deserialize, PartialEq, Serialize)]
#[serde(default)]
pub struct AuthorityConfig {
    /// The authority's protocol key.
    pub protocol_key: BlsPublicKey,
    /// The authority's network key.
    pub network_key: NetworkPublicKey,
    /// The authority's network key.
    pub execution_key: ExecutionPublicKey,
    /// The primary's [Multiaddr].
    pub primary_network_address: Multiaddr,
}

impl Default for AuthorityConfig {
    fn default() -> Self {
        Self {
            protocol_key: BlsPublicKey::insecure_default(),
            network_key: NetworkPublicKey::insecure_default(),
            // seed with [2u8; 33] bc 0 is undefined in secp256k1 curve
            execution_key: ExecutionPublicKey::from_bytes(&[2u8; 33])
                .expect("default execution public key all twos"),
            primary_network_address: Multiaddr::empty(),
        }
    }
}

/// Holds all the node properties. An example is provided to
/// showcase the usage and deserialization from a json file.
/// To define a Duration on the property file can use either
/// milliseconds or seconds (e.x 5s, 10ms , 2000ms).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Parameters {
    /// When the primary has `header_num_of_batches_threshold` num of batch digests available,
    /// then it can propose a new header.
    #[serde(default = "Parameters::default_header_num_of_batches_threshold")]
    pub header_num_of_batches_threshold: usize,

    /// The maximum number of batch digests included in a header.
    #[serde(default = "Parameters::default_max_header_num_of_batches")]
    pub max_header_num_of_batches: usize,

    /// The maximum delay that the primary should wait between generating two headers, even if
    /// other conditions are not satisfied besides having enough parent stakes.
    #[serde(with = "humantime_serde", default = "Parameters::default_max_header_delay")]
    pub max_header_delay: Duration,
    /// When the delay from last header reaches `min_header_delay`, a new header can be proposed
    /// even if batches have not reached `header_num_of_batches_threshold`.
    #[serde(with = "humantime_serde", default = "Parameters::default_min_header_delay")]
    pub min_header_delay: Duration,

    /// The depth of the garbage collection (Denominated in number of rounds).
    #[serde(default = "Parameters::default_gc_depth")]
    pub gc_depth: u64,
    /// The delay after which the synchronizer retries to send sync requests. Denominated in ms.
    #[serde(with = "humantime_serde", default = "Parameters::default_sync_retry_delay")]
    pub sync_retry_delay: Duration,
    /// Determine with how many nodes to sync when re-trying to send sync-request. These nodes
    /// are picked at random from the committee.
    #[serde(default = "Parameters::default_sync_retry_nodes")]
    pub sync_retry_nodes: usize,
    /// The preferred batch size. The workers seal a batch of transactions when it reaches this
    /// size. Denominated in bytes.
    #[serde(default = "Parameters::default_batch_size")]
    pub batch_size: usize,
    /// The delay after which the workers seal a batch of transactions, even if `max_batch_size`
    /// is not reached.
    #[serde(with = "humantime_serde", default = "Parameters::default_max_batch_delay")]
    pub max_batch_delay: Duration,
    /// The maximum number of concurrent requests for messages accepted from an un-trusted entity
    #[serde(default = "Parameters::default_max_concurrent_requests")]
    pub max_concurrent_requests: usize,
    /// Properties for the prometheus metrics
    #[serde(default = "PrometheusMetricsParameters::default")]
    pub prometheus_metrics: PrometheusMetricsParameters,
    /// Network admin server ports for primary & worker.
    #[serde(default = "NetworkAdminServerParameters::default")]
    pub network_admin_server: NetworkAdminServerParameters,
    /// Anemo network settings.
    #[serde(default = "AnemoParameters::default")]
    pub anemo: AnemoParameters,
}

impl Parameters {
    fn default_header_num_of_batches_threshold() -> usize {
        32
    }

    fn default_max_header_num_of_batches() -> usize {
        1_000
    }

    fn default_max_header_delay() -> Duration {
        Duration::from_secs(1)
    }

    fn default_min_header_delay() -> Duration {
        Duration::from_secs_f64(0.5)
    }

    fn default_gc_depth() -> u64 {
        50
    }

    fn default_sync_retry_delay() -> Duration {
        Duration::from_millis(5_000)
    }

    fn default_sync_retry_nodes() -> usize {
        3
    }

    fn default_batch_size() -> usize {
        5_000_000
    }

    fn default_max_batch_delay() -> Duration {
        Duration::from_millis(100)
    }

    fn default_max_concurrent_requests() -> usize {
        500_000
    }
}

/// Admin server settings.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct NetworkAdminServerParameters {
    /// Primary network admin server port number
    pub primary_network_admin_server_port: u16,
    /// Worker network admin server base port number
    pub worker_network_admin_server_base_port: u16,
}

impl Default for NetworkAdminServerParameters {
    fn default() -> Self {
        let host = "127.0.0.1";
        Self {
            primary_network_admin_server_port: get_available_port(host),
            worker_network_admin_server_base_port: get_available_port(host),
        }
    }
}

impl NetworkAdminServerParameters {
    fn with_available_port(&self) -> Self {
        let mut params = self.clone();
        let default = Self::default();
        params.primary_network_admin_server_port = default.primary_network_admin_server_port;
        params.worker_network_admin_server_base_port =
            default.worker_network_admin_server_base_port;
        params
    }
}

/// Parameters for the anemo network settings.
///
/// These settings affect consensus network messaging between nodes.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct AnemoParameters {
    /// Per-peer rate-limits (in requests/sec) for the PrimaryToPrimary service.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub send_certificate_rate_limit: Option<NonZeroU32>,

    /// Per-peer rate-limits (in requests/sec) for the WorkerToWorker service.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub report_batch_rate_limit: Option<NonZeroU32>,

    /// The amount of requests a worker can make to peers for a missing batch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_batches_rate_limit: Option<NonZeroU32>,

    /// Size in bytes above which network messages are considered excessively large. Excessively
    /// large messages will still be handled, but logged and reported in metrics for debugging.
    ///
    /// If unspecified, this will default to 8 MiB.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub excessive_message_size: Option<usize>,
}

impl AnemoParameters {
    /// Set the limit for the amount of certificates a peer can send.
    ///
    /// By default, at most 20 certificates can be sent concurrently to a peer.
    pub fn send_certificate_rate_limit(&self) -> u32 {
        self.send_certificate_rate_limit.unwrap_or(NonZeroU32::new(20).unwrap()).get()
    }

    /// Set the limit for the amount of batches that can be broadcast concurrently.
    ///
    /// By default, at most 100 batches can be broadcasted concurrently.
    pub fn report_batch_rate_limit(&self) -> u32 {
        self.report_batch_rate_limit.unwrap_or(NonZeroU32::new(200).unwrap()).get()
    }

    /// Set the limit for amount of requests to send a peer for batches.
    ///
    /// As of 11/02/2023, when one worker is actively fetching, each peer receives
    /// 20~30 requests per second.
    pub fn request_batches_rate_limit(&self) -> u32 {
        self.request_batches_rate_limit.unwrap_or(NonZeroU32::new(100).unwrap()).get()
    }

    /// Set the limit for excessive message size.
    pub fn excessive_message_size(&self) -> usize {
        const EXCESSIVE_MESSAGE_SIZE: usize = 8 << 20;

        self.excessive_message_size.unwrap_or(EXCESSIVE_MESSAGE_SIZE)
    }
}

/// Prometheus metrics multiaddr.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct PrometheusMetricsParameters {
    /// Socket address the server should be listening to.
    pub socket_addr: Multiaddr,
}

impl Default for PrometheusMetricsParameters {
    fn default() -> Self {
        let host = "127.0.0.1";
        Self {
            socket_addr: format!("/ip4/{}/tcp/{}/http", host, get_available_port(host))
                .parse()
                .unwrap(),
        }
    }
}

impl PrometheusMetricsParameters {
    fn with_available_port(&self) -> Self {
        let mut params = self.clone();
        let default = Self::default();
        params.socket_addr = default.socket_addr;
        params
    }
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            header_num_of_batches_threshold: Parameters::default_header_num_of_batches_threshold(),
            max_header_num_of_batches: Parameters::default_max_header_num_of_batches(),
            max_header_delay: Parameters::default_max_header_delay(),
            min_header_delay: Parameters::default_min_header_delay(),
            gc_depth: Parameters::default_gc_depth(),
            sync_retry_delay: Parameters::default_sync_retry_delay(),
            sync_retry_nodes: Parameters::default_sync_retry_nodes(),
            batch_size: Parameters::default_batch_size(),
            max_batch_delay: Parameters::default_max_batch_delay(),
            max_concurrent_requests: Parameters::default_max_concurrent_requests(),
            prometheus_metrics: PrometheusMetricsParameters::default(),
            network_admin_server: NetworkAdminServerParameters::default(),
            anemo: AnemoParameters::default(),
        }
    }
}

impl Parameters {
    /// Set prometheus metrics and network admin server ports based on what's availalbe
    /// on the OS.
    pub fn with_available_ports(&self) -> Self {
        let mut params = self.clone();
        params.prometheus_metrics = params.prometheus_metrics.with_available_port();
        params.network_admin_server = params.network_admin_server.with_available_port();
        params
    }

    /// Tracing::info! for [Self].
    pub fn tracing(&self) {
        info!("Header number of batches threshold set to {}", self.header_num_of_batches_threshold);
        info!("Header max number of batches set to {}", self.max_header_num_of_batches);
        info!("Max header delay set to {} ms", self.max_header_delay.as_millis());
        info!("Min header delay set to {} ms", self.min_header_delay.as_millis());
        info!("Garbage collection depth set to {} rounds", self.gc_depth);
        info!("Sync retry delay set to {} ms", self.sync_retry_delay.as_millis());
        info!("Sync retry nodes set to {} nodes", self.sync_retry_nodes);
        info!("Batch size set to {} B", self.batch_size);
        info!("Max batch delay set to {} ms", self.max_batch_delay.as_millis());
        info!("Max concurrent requests set to {}", self.max_concurrent_requests);
        info!("Prometheus metrics server will run on {}", self.prometheus_metrics.socket_addr);
        info!(
            "Primary network admin server will run on 127.0.0.1:{}",
            self.network_admin_server.primary_network_admin_server_port
        );
        info!(
            "Worker network admin server will run starting on base port 127.0.0.1:{}",
            self.network_admin_server.worker_network_admin_server_base_port
        );
    }
}
