use std::sync::Arc;

use narwhal_network::client::NetworkClient;
use narwhal_storage::NodeStorage;
use narwhal_typed_store::traits::Database;
use tn_types::{
    traits::KeyPair, Authority, Committee, Config, ConfigTrait, Parameters, TelcoinDirs,
    WorkerCache,
};

use crate::KeyConfig;

struct ConsensusConfigInner<DB> {
    config: Config,
    committee: Committee,
    worker_cache: WorkerCache,
    tn_datadir: Arc<dyn TelcoinDirs>,
    network_client: NetworkClient,
    node_storage: NodeStorage<DB>,
    key_config: KeyConfig,
    authority: Authority,
}

#[derive(Clone)]
pub struct ConsensusConfig<DB> {
    inner: Arc<ConsensusConfigInner<DB>>,
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
        //let key_config = KeyConfig::new(&tn_datadir)?;
        let network_client =
            NetworkClient::new_from_public_key(config.validator_info.primary_network_key());

        // load committee from file
        let mut committee: Committee = Config::load_from_path(tn_datadir.committee_path())?;
        committee.load();
        tracing::info!(target: "telcoin::consensus_config", "committee loaded");
        // TODO: make worker cache part of committee?
        let worker_cache: WorkerCache = Config::load_from_path(tn_datadir.worker_cache_path())?;
        tracing::info!(target: "telcoin::consensus_config", "worker cache loaded");

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
        Ok(Self {
            inner: Arc::new(ConsensusConfigInner {
                config,
                committee,
                worker_cache,
                tn_datadir,
                network_client,
                node_storage,
                key_config,
                authority,
            }),
        })
    }

    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    pub fn committee(&self) -> &Committee {
        &self.inner.committee
    }

    pub fn worker_cache(&self) -> &WorkerCache {
        &self.inner.worker_cache
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
