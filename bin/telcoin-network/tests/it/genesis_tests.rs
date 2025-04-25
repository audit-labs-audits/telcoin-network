#[cfg(test)]
mod tests {
    use crate::util::spawn_local_testnet;
    use alloy::{
        network::EthereumWallet,
        providers::ProviderBuilder,
        sol_types::{SolCall, SolConstructor},
    };
    use jsonrpsee::{core::client::ClientT, http_client::HttpClientBuilder, rpc_params};
    use rand::{rngs::StdRng, SeedableRng};
    use std::{sync::Arc, time::Duration};
    use tempfile::TempDir;
    use tn_config::fetch_file_content_relative_to_manifest;
    use tn_reth::{
        system_calls::{ConsensusRegistry, CONSENSUS_REGISTRY_ADDRESS},
        CallRequest, RethChainSpec, RethEnv,
    };
    use tn_test_utils::{get_contract_state_for_genesis, TransactionFactory};
    use tn_types::{
        adiri_genesis, hex, sol, Address, BlsKeypair, Bytes, GenesisAccount, SolValue, TaskManager,
        U256,
    };

    #[tokio::test]
    async fn test_genesis_with_consensus_registry() -> eyre::Result<()> {
        tn_test_utils::init_test_tracing();

        // fetch registry impl bytecode from compiled output in tn-contracts
        let registry_standard_json = fetch_file_content_relative_to_manifest(
            "../../tn-contracts/artifacts/ConsensusRegistry.json",
        );
        let registry_impl_bytecode =
            RethEnv::parse_deployed_bytecode_from_json_str(&registry_standard_json)?;

        // ERC1967Proxy interface
        sol!(
            // #[sol(rpc)]
            contract ERC1967Proxy {
                constructor(address implementation, bytes memory _data);
            }
        );

        // fetch and construct registry proxy deployment transaction
        let registry_proxy_address = Address::random();
        let registry_proxy_json = fetch_file_content_relative_to_manifest(
            "../../tn-contracts/artifacts/ERC1967Proxy.json",
        );
        let registry_proxy_bytecode =
            RethEnv::parse_deployed_bytecode_from_json_str(&registry_proxy_json)?;

        // construct array of 4 validators with 1-indexed `validatorIndex`
        let active_status = ConsensusRegistry::ValidatorStatus::Active;
        let initial_validators: Vec<ConsensusRegistry::ValidatorInfo> = (1..=4)
            .map(|_| {
                // generate random bls, ed25519, and ecdsa keys for each validator
                let mut rng = StdRng::from_entropy();
                let bls_keypair = BlsKeypair::generate(&mut rng);
                let bls_pubkey = bls_keypair.public().to_bytes();
                let addr = Address::random();

                ConsensusRegistry::ValidatorInfo {
                    blsPubkey: bls_pubkey.into(),
                    validatorAddress: addr,
                    activationEpoch: 0,
                    exitEpoch: 0,
                    currentStatus: active_status,
                    isRetired: false,
                    isDelegated: false,
                    stakeVersion: 0,
                }
            })
            .collect();

        // let registry_init_params = (
        //     Address::random(),
        //     U256::from(1_000_000e18),
        //     U256::from(10_000e18),
        //     initial_validators.clone(),
        //     Address::random(),
        // )
        //     .abi_encode_params();

        // let init_calldata = [&registry_init_selector, &registry_init_params[..]].concat();

        let epoch_duration = 60 * 60 * 24; // 24-hours
        let initial_stake_config = ConsensusRegistry::StakeConfig {
            stakeAmount: U256::from(1_000_000e18),
            minWithdrawAmount: U256::from(1_000e18),
            epochIssuance: U256::from(20_000_000e18)
                .checked_div(U256::from(28))
                .expect("u256 div checked"),
            epochDuration: epoch_duration,
        };

        // generate calldata to initialize proxy
        let owner = Address::random();

        // create proxy for test
        // create temporary reth env for execution
        let task_manager = TaskManager::new("Test Task Manager");
        let tmp_dir = TempDir::new().unwrap();

        tracing::debug!(target: "bundle", "proxy bytecode:\n{:?}", registry_proxy_bytecode);

        // deploy bytecode to execute constructor/init calls
        let tmp_genesis = adiri_genesis().extend_accounts([
            (
                CONSENSUS_REGISTRY_ADDRESS,
                GenesisAccount::default().with_code(Some(registry_impl_bytecode.clone().into())),
            ),
            (
                registry_proxy_address,
                GenesisAccount::default().with_code(Some(registry_proxy_bytecode.clone().into())),
            ),
        ]);

        let chain: Arc<RethChainSpec> = Arc::new(tmp_genesis.into());
        let reth_env =
            RethEnv::new_for_test_with_chain(chain.clone(), tmp_dir.path(), &task_manager)?;

        let constructor_args = ERC1967Proxy::constructorCall {
            implementation: CONSENSUS_REGISTRY_ADDRESS,
            _data: Bytes::new(),
        }
        .abi_encode()
        .into();

        // let proxy_bundle = reth_env
        //     .execute_call_tx_for_test_bypass_evm_checks(&chain.sealed_genesis_header(), txs)?;

        // tracing::debug!(target: "bundle", "proxy bundle state:\n{:?}\n{:?}", proxy_bundle.state, proxy_bundle.reverts);

        // now init the registry impl
        let init_calldata = ConsensusRegistry::initializeCall {
            rwTEL_: Address::random(),
            genesisConfig_: initial_stake_config,
            initialValidators_: initial_validators.clone(),
            owner_: Address::random(),
        }
        .abi_encode()
        .into();

        let txs = vec![
            // constructor
            CallRequest::new(registry_proxy_address, owner, constructor_args),
            // init
            CallRequest::new(registry_proxy_address, owner, init_calldata),
        ];

        let bundle = reth_env
            .execute_call_tx_for_test_bypass_evm_checks(&chain.sealed_genesis_header(), txs)?;
        tracing::debug!(target: "bundle", "impl bundle state:\n{:?}\n{:?}", bundle.state, bundle.reverts);

        let storage = bundle.state.get(&registry_proxy_address).map(|account| {
            account.storage.iter().map(|(k, v)| ((*k).into(), v.present_value.into())).collect()
        });

        tracing::debug!(target: "bundle", "bundle state:\n{:?}\n{:?}", bundle.state, bundle.reverts);

        // perform canonical adiri chain genesis with fetched storage
        let genesis_accounts = [
            (
                CONSENSUS_REGISTRY_ADDRESS,
                GenesisAccount::default().with_code(Some(registry_impl_bytecode.clone().into())),
            ),
            (
                registry_proxy_address,
                GenesisAccount::default()
                    .with_code(Some(registry_proxy_bytecode.into()))
                    .with_storage(storage),
            ),
        ];
        let real_genesis = adiri_genesis();
        let genesis = real_genesis.extend_accounts(genesis_accounts);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

        // let genesis = RethEnv::create_consensus_registry_accounts_for_genesis();

        spawn_local_testnet(chain, "0x0000000000000000000000000000000000000000")
            .expect("failed to spawn testnet");
        // allow time for nodes to start
        tokio::time::sleep(Duration::from_secs(10)).await;

        let rpc_url = "http://127.0.0.1:8545".to_string();
        let client =
            HttpClientBuilder::default().build(&rpc_url).expect("couldn't build rpc client");

        // sanity check onchain reads
        let returned_impl_code: String = client
            .request("eth_getCode", rpc_params!(CONSENSUS_REGISTRY_ADDRESS))
            .await
            .expect("Failed to fetch registry impl bytecode");
        // trim `0x` prefix
        assert_eq!(returned_impl_code[2..], hex::encode(registry_impl_bytecode));

        let mut tx_factory = TransactionFactory::default();
        let signer = tx_factory.get_default_signer().expect("failed to fetch signer");
        let wallet = EthereumWallet::from(signer);
        let provider = ProviderBuilder::new()
            .with_recommended_fillers()
            .wallet(wallet)
            .on_http(rpc_url.parse().expect("rpc url parse error"));
        let consensus_registry = ConsensusRegistry::new(registry_proxy_address, provider.clone());

        let active_validators = consensus_registry
            .getValidators(ConsensusRegistry::ValidatorStatus::Active.into())
            .call()
            .await
            .expect("failed active validators read");

        println!("\n!!!!!!!!!\nmade it here!! :D");
        assert_eq!(active_validators._0.abi_encode(), initial_validators.abi_encode());

        // assert committees for first 3 epochs comprise all genesis validators
        for i in 0..3 {
            let epoch_info = consensus_registry
                .getEpochInfo(i)
                .call()
                .await
                .expect("failed epoch read")
                .epochInfo;
            for (j, _) in initial_validators.iter().enumerate() {
                assert_eq!(epoch_info.committee[j], initial_validators[j].validatorAddress);
            }
        }

        Ok(())
    }
}
