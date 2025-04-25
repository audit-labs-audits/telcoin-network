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
    use std::{collections::BTreeMap, sync::Arc, time::Duration};
    use tempfile::TempDir;
    use tn_config::fetch_file_content_relative_to_manifest;
    use tn_reth::{
        system_calls::{ConsensusRegistry, CONSENSUS_REGISTRY_ADDRESS},
        CallRequest, CreateRequest, RethChainSpec, RethEnv,
    };
    use tn_test_utils::{get_contract_state_for_genesis, TransactionFactory};
    use tn_types::{
        adiri_genesis, hex, sol, Address, BlsKeypair, Bytes, GenesisAccount, SolValue, TaskManager,
        U256,
    };

    #[tokio::test]
    async fn test_genesis_with_consensus_registry() -> eyre::Result<()> {
        tn_test_utils::init_test_tracing();
        // ERC1967Proxy interface
        sol!(
            // #[sol(rpc)]
            contract ERC1967Proxy {
                constructor(address implementation, bytes memory _data);
            }
        );

        let TEST_CR_ADDRESS: Address = Address::random();

        // create proxy transaction
        // let proxy_address = Address::random();
        let registry_proxy_json = fetch_file_content_relative_to_manifest(
            "../../tn-contracts/artifacts/ERC1967Proxy.json",
        );
        let registry_proxy_bytecode = RethEnv::parse_bytecode_from_json_str(&registry_proxy_json)?;
        let registry_proxy_deployed_bytecode =
            RethEnv::parse_deployed_bytecode_from_json_str(&registry_proxy_json)?;

        let constructor_args =
            ERC1967Proxy::constructorCall { implementation: TEST_CR_ADDRESS, _data: Bytes::new() }
                .abi_encode();

        let mut create_proxy = registry_proxy_bytecode.clone();
        create_proxy.extend(constructor_args);

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

        let epoch_duration = 60 * 60 * 24; // 24-hours
        let stake_amount = U256::from(1_000_000e18);
        let initial_stake_config = ConsensusRegistry::StakeConfig {
            stakeAmount: stake_amount,
            minWithdrawAmount: U256::from(1_000e18),
            epochIssuance: U256::from(20_000_000e18)
                .checked_div(U256::from(28))
                .expect("u256 div checked"),
            epochDuration: epoch_duration,
        };

        // fetch registry impl bytecode from compiled output in tn-contracts
        let registry_impl_json = fetch_file_content_relative_to_manifest(
            "../../tn-contracts/artifacts/ConsensusRegistry.json",
        );
        let registry_impl_deployed_bytecode =
            RethEnv::parse_deployed_bytecode_from_json_str(&registry_impl_json)?;

        // generate calldata to initialize proxy
        let owner = Address::random();

        // create proxy for test
        // create temporary reth env for execution
        let task_manager = TaskManager::new("Test Task Manager");
        let tmp_dir = TempDir::new().unwrap();

        tracing::debug!(target: "bundle", "proxy bytecode:\n{:?}", registry_proxy_bytecode);

        // deploy bytecode to execute constructor/init calls
        let registry_proxy_address = owner.create(0);
        tracing::debug!(target: "bundle", "expected proxy address: {:?}", registry_proxy_address);

        let mut tx_factory = TransactionFactory::default();
        let factory_address = tx_factory.address();
        let tmp_genesis = adiri_genesis().extend_accounts([
            (
                TEST_CR_ADDRESS,
                GenesisAccount::default()
                    .with_code(Some(registry_impl_deployed_bytecode.clone().into())),
            ),
            // (factory_address, GenesisAccount::default().with_balance(U256::MAX)),
        ]);

        let tmp_chain: Arc<RethChainSpec> = Arc::new(tmp_genesis.into());
        let reth_env =
            RethEnv::new_for_test_with_chain(tmp_chain.clone(), tmp_dir.path(), &task_manager)?;

        // now init the registry impl
        let init_data = ConsensusRegistry::initializeCall {
            rwTEL_: Address::random(),
            genesisConfig_: initial_stake_config,
            initialValidators_: initial_validators.clone(),
            owner_: Address::random(),
        };

        tracing::debug!(target: "bundle", "init data:\n{:#?}", init_data);
        let init_calldata: Bytes = init_data.abi_encode().into();

        tracing::debug!(target: "bundle", "init calldata:\n{:#x}\n", init_calldata);

        let constructor_params = (TEST_CR_ADDRESS, Bytes::default()).abi_encode_params();
        let registry_create_data =
            [registry_proxy_bytecode.as_slice(), &constructor_params[..]].concat();
        assert_eq!(registry_create_data, create_proxy);

        // // construct proxy deployment and initialize txs
        // let gas_price = 7;
        // let gas_limit = 3_000_000;
        // // let pre_genesis_chain: Arc<RethChainSpec> = Arc::new(tmp_genesis.into());
        // let registry_tx_raw = tx_factory.create_eip1559_encoded(
        //     tmp_chain.clone(),
        //     Some(gas_limit),
        //     gas_price,
        //     None,
        //     U256::ZERO,
        //     registry_create_data.clone().into(),
        // );
        // // registry deployment will be `factory_address`'s first tx
        // let registry_proxy_address = factory_address.create(0);
        // let initialize_tx_raw = tx_factory.create_eip1559_encoded(
        //     tmp_chain.clone(),
        //     Some(gas_limit),
        //     gas_price,
        //     Some(registry_proxy_address),
        //     U256::ZERO,
        //     init_calldata.clone().into(),
        // );
        // let raw_txs = vec![registry_tx_raw.clone(), initialize_tx_raw];
        // let tmp_dir = TempDir::new().unwrap();
        // // fetch storage changes from pre-genesis for actual genesis
        // let execution_outcome =
        //     get_contract_state_for_genesis(tmp_chain.clone(), raw_txs, tmp_dir.path())
        //         .await
        //         .expect("unable to fetch contract state");
        // let bundle = execution_outcome.bundle;

        let txs = vec![
            // constructor
            CreateRequest::new(owner, create_proxy.into()).into(),
            // init
            CallRequest::new(registry_proxy_address, Address::random(), init_calldata.into())
                .into(),
        ];

        let bundle = reth_env
            .execute_call_tx_for_test_bypass_evm_checks(&tmp_chain.sealed_genesis_header(), txs)?;

        let proxy_account = bundle.account(&registry_proxy_address).map(|account| account);
        tracing::debug!(target: "bundle", "proxy_account:\n\n\n{:#?}\n\n", proxy_account);

        // tracing::debug!(target: "bundle", "all done bundle state:\n\n\n{:?}\n\n\n{:?}\n\n\n", bundle.state, bundle.reverts);

        let proxy_storage = bundle.state.get(&registry_proxy_address).map(|account| {
            account.storage.iter().map(|(k, v)| ((*k).into(), v.present_value.into())).collect()
        });

        // assert!(proxy_storage.clone().map(|tree: BTreeMap<_, _>| !tree.is_empty()).unwrap());

        // perform canonical adiri chain genesis with fetched storage
        let genesis_accounts = [
            (
                TEST_CR_ADDRESS,
                GenesisAccount::default()
                    .with_code(Some(registry_impl_deployed_bytecode.clone().into())),
            ),
            (
                registry_proxy_address,
                GenesisAccount::default()
                    .with_code(Some(registry_proxy_deployed_bytecode.clone().into()))
                    .with_balance(
                        U256::from(4)
                            .checked_mul(stake_amount)
                            .expect("U256 checked mul for total stake"),
                    )
                    .with_storage(proxy_storage),
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

        tracing::debug!(target: "bundle", "calling get code on consensus registry: {:?}", CONSENSUS_REGISTRY_ADDRESS);

        // sanity check onchain reads
        let returned_impl_code: String = client
            .request("eth_getCode", rpc_params!(TEST_CR_ADDRESS))
            .await
            .expect("Failed to fetch registry impl bytecode");
        // trim `0x` prefix
        assert_eq!(returned_impl_code[2..], hex::encode(registry_impl_deployed_bytecode.clone()));

        let returned_proxy_code: String = client
            .request("eth_getCode", rpc_params!(registry_proxy_address))
            .await
            .expect("Failed to fetch registry impl bytecode");
        assert_eq!(returned_proxy_code[2..], hex::encode(registry_proxy_deployed_bytecode));

        // sanity check onchain spawned in genesis
        let returned_impl_code: String = client
            .request("eth_getCode", rpc_params!(CONSENSUS_REGISTRY_ADDRESS))
            .await
            .expect("Failed to fetch registry impl bytecode");
        // trim `0x` prefix
        assert_eq!(returned_impl_code[2..], hex::encode(registry_impl_deployed_bytecode));

        let returned_impl_storage: String = client
            .request("eth_getStorage", rpc_params!(CONSENSUS_REGISTRY_ADDRESS))
            .await
            .expect("Failed to fetch registry impl bytecode");

        tracing::debug!(target: "bundle", "on-chain storage??\n\n{:#?}\n\n!!!!!!!!!!!!!!", returned_impl_storage);

        // trim `0x` prefix
        // assert_eq!(returned_impl_storage[2..], hex::encode(registry_impl_deployed_bytecode));

        let signer = tx_factory.get_default_signer().expect("failed to fetch signer");
        let wallet = EthereumWallet::from(signer);
        let provider = ProviderBuilder::new()
            .with_recommended_fillers()
            .wallet(wallet)
            .on_http(rpc_url.parse().expect("rpc url parse error"));
        let consensus_registry =
            ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, provider.clone());

        let rpc_call = (CONSENSUS_REGISTRY_ADDRESS, "0x374ed18c").abi_encode_params();
        // let epoch_validators: String = client //consensus_registry
        //     // .getValidators(ConsensusRegistry::ValidatorStatus::Active.into())
        //     // .getCurrentEpochInfo()
        //     .request(
        //         "eth_call",
        //         // rpc_call,
        //         rpc_params!({
        //             "to": CONSENSUS_REGISTRY_ADDRESS.to_string(),
        //             "data": "0x374ed18c"
        //         }, "latest"),
        //     )
        //     .await
        //     .expect("failed active validators read");

        // tracing::debug!(target: "bundle", "active validators??\n{:?}", active_validators);
        // let active_validators = active_validators;

        panic!("made it here");
        // assert_eq!(active_validators._0.abi_encode(), initial_validators.abi_encode());

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
