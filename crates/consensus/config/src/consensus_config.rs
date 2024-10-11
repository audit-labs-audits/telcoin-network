use std::sync::Arc;

use narwhal_network::client::NetworkClient;
use narwhal_storage::NodeStorage;
use narwhal_typed_store::{mem_db::MemDatabase, traits::Database};
use rand::rngs::ThreadRng;
use tn_types::{
    test_utils::TelcoinTempDirs, traits::KeyPair, Authority, Committee, Config, ConfigTrait,
    Parameters, TelcoinDirs, WorkerCache,
};

use crate::KeyConfig;

struct ConsensusConfigInner<DB> {
    config: Config,
    committee: Committee,
    tn_datadir: Arc<dyn TelcoinDirs>,
    network_client: NetworkClient,
    node_storage: NodeStorage<DB>,
    key_config: KeyConfig,
    authority: Authority,
}

#[derive(Clone)]
pub struct ConsensusConfig<DB> {
    inner: Arc<ConsensusConfigInner<DB>>,
    worker_cache: Option<Arc<WorkerCache>>,
}

impl<DB> ConsensusConfig<DB>
where
    DB: Database,
{
    pub fn new<TND: TelcoinDirs + 'static>(
        config: Config,
        tn_datadir: TND,
        node_storage: NodeStorage<DB>,
        key_config: KeyConfig,
    ) -> eyre::Result<Self> {
        // load committee from file
        let mut committee: Committee = Config::load_from_path(tn_datadir.committee_path())?;
        committee.load();
        tracing::info!(target: "telcoin::consensus_config", "committee loaded");
        // TODO: make worker cache part of committee?
        let worker_cache: WorkerCache = Config::load_from_path(tn_datadir.worker_cache_path())?;
        // TODO: this could be a separate method on `Committee` to have robust checks in place
        // - all public keys are unique
        // - thresholds / stake
        //
        // assert committee loaded correctly
        // assert!(committee.size() >= 4, "not enough validators in committee.");

        // TODO: better assertion here
        // right now, each validator should only have 1 worker
        // this assertion would incorrectly pass if 1 authority had 2 workers and another had 0
        //
        // assert worker cache loaded correctly
        assert!(
            worker_cache.all_workers().len() == committee.size(),
            "each validator within committee must have one worker"
        );

        tracing::info!(target: "telcoin::consensus_config", "worker cache loaded");
        Self::new_with_committee(
            config,
            tn_datadir,
            node_storage,
            key_config,
            committee,
            Some(worker_cache),
        )
    }

    pub fn new_with_committee<TND: TelcoinDirs + 'static>(
        config: Config,
        tn_datadir: TND,
        node_storage: NodeStorage<DB>,
        key_config: KeyConfig,
        committee: Committee,
        mut worker_cache: Option<WorkerCache>,
    ) -> eyre::Result<Self> {
        let network_client =
            NetworkClient::new_from_public_key(config.validator_info.primary_network_key());

        let authority = committee
            .authority_by_key(key_config.bls_keypair().public())
            .unwrap_or_else(|| {
                panic!(
                    "Our node with key {:?} should be in committee",
                    key_config.bls_keypair().public()
                )
            })
            .clone();

        let tn_datadir = Arc::new(tn_datadir);
        let worker_cache = match worker_cache.take() {
            Some(worker_cache) => Some(Arc::new(worker_cache)),
            None => None,
        };
        Ok(Self {
            inner: Arc::new(ConsensusConfigInner {
                config,
                committee,
                tn_datadir,
                network_client,
                node_storage,
                key_config,
                authority,
            }),
            worker_cache,
        })
    }

    /// Create a new config with temp dirs, mem db, random keys and defaults.
    /// Useful for testing.  This will panic on error.
    pub fn new_test_config() -> ConsensusConfig<MemDatabase> {
        let config = Config::default();
        let tn_datadir = TelcoinTempDirs::default();
        let node_storage = NodeStorage::reopen(MemDatabase::default());
        let key_configs = KeyConfig::with_random(&mut ThreadRng::default());
        ConsensusConfig::<MemDatabase>::new(config, tn_datadir, node_storage, key_configs)
            .expect("failed to create config!")
    }

    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    pub fn committee(&self) -> &Committee {
        &self.inner.committee
    }

    pub fn worker_cache(&self) -> &WorkerCache {
        self.worker_cache.as_ref().expect("invalid config- missing worker cache!")
    }

    pub fn set_worker_cache(&mut self, worker_cache: WorkerCache) {
        assert!(self.worker_cache.is_none(), "Can not change the working cache on a config!");
        self.worker_cache = Some(Arc::new(worker_cache));
    }

    pub fn tn_datadir(&self) -> Arc<dyn TelcoinDirs> {
        self.inner.tn_datadir.clone()
    }

    pub fn network_client(&self) -> &NetworkClient {
        &self.inner.network_client
    }

    pub fn node_storage(&self) -> &NodeStorage<DB> {
        &self.inner.node_storage
    }

    pub fn key_config(&self) -> &KeyConfig {
        &self.inner.key_config
    }

    pub fn authority(&self) -> &Authority {
        &self.inner.authority
    }

    pub fn parameters(&self) -> &Parameters {
        &self.inner.config.parameters
    }

    pub fn database(&self) -> &DB {
        &self.inner.node_storage.batch_store
    }
}
