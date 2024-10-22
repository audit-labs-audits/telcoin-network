#[cfg(test)]
mod tests {
    use crate::util::get_contract_state_for_genesis;
    use rand::{rngs::StdRng, SeedableRng};
    use reth_chainspec::ChainSpec;
    use tn_types::{
        adiri_genesis, test_utils::{contract_artifacts::{CONSENSUSREGISTRY_RUNTIMECODE, ERC1967PROXY_INITCODE, ERC1967PROXY_RUNTIMECODE}, execution_outcome_for_tests, TransactionFactory}, BlsKeypair, NetworkKeypair, WorkerBlock
    };
    use alloy::{primitives::{FixedBytes, Uint}, sol, sol_types::SolValue};
    use fastcrypto::traits::{KeyPair, ToFromBytes};
    use std::{str::FromStr, sync::Arc};
    use reth::{primitives::{Address, Bytes, GenesisAccount, SealedHeader, U256}, tasks::TaskManager};

    #[tokio::test]
    async fn test_genesis_with_consensus_registry() {
        let network_genesis = adiri_genesis();
        let tmp_chain: Arc<ChainSpec> = Arc::new(network_genesis.into());
        let registry_impl_address = Address::random();
        let registry_impl_bytecode = *CONSENSUSREGISTRY_RUNTIMECODE;
        let mut tx_factory = TransactionFactory::new();
        let factory_address = tx_factory.address();

        // deploy impl and fund `factory_address`
        let tmp_genesis = tmp_chain.genesis.clone().extend_accounts(
            vec![
                (factory_address, GenesisAccount::default().with_balance(U256::MAX)),
                (registry_impl_address, GenesisAccount::default().with_code(Some(registry_impl_bytecode.into()))),
            ]
            .into_iter(),
        );

        // ERC1967Proxy interface
        sol!(
            #[allow(clippy::too_many_arguments)]
            #[sol(rpc)]
            contract ERC1967Proxy {
                constructor(address implementation, bytes memory _data);
            }
        );

        let constructor_params = (registry_impl_address, Bytes::default()).abi_encode_params();
        let registry_create_data = [ERC1967PROXY_INITCODE.as_slice(), &constructor_params[..]].concat();

        // ConsensusRegistry interface
        sol!(
            #[allow(clippy::too_many_arguments)]
            #[sol(rpc)]
            contract ConsensusRegistry {
                enum ValidatorStatus {
                    Undefined,
                    PendingActivation,
                    Active,
                    PendingExit,
                    Exited
                }
                struct ValidatorInfo {
                    bytes blsPubkey;
                    bytes32 ed25519Pubkey;
                    address ecdsaPubkey;
                    uint32 activationEpoch;
                    uint32 exitEpoch;
                    uint24 validatorIndex;
                    ValidatorStatus currentStatus;
                }
                function initialize(
                    address rwTEL_, 
                    uint256 stakeAmount_, 
                    uint256 minWithdrawAmount_,
                    ValidatorInfo[] memory initialValidators_,
                    address owner_
                );
            }
        );

        let registry_init_selector = [97, 175, 158, 105];
        let activation_epoch = u32::default();
        let exit_epoch = u32::default();
        let active_status = ConsensusRegistry::ValidatorStatus::Active;

        // construct array of 4 validators with 1-indexed `validatorIndex`
        let initial_validators: Vec<ConsensusRegistry::ValidatorInfo> = (1..=4).map(|i| {
            // generate random bls, ed25519, and ecdsa keys for each validator
            let mut rng = StdRng::from_entropy();
            let bls_keypair = BlsKeypair::generate(&mut rng);
            let bls_pubkey = bls_keypair.public().as_bytes().to_vec();
            let ed_25519_keypair = NetworkKeypair::generate(&mut rng);
            let ecdsa_pubkey = Address::random();

            ConsensusRegistry::ValidatorInfo {
                blsPubkey: bls_pubkey.clone().into(),
                ed25519Pubkey: FixedBytes::<32>::from_slice(ed_25519_keypair.public().as_bytes()),
                ecdsaPubkey: ecdsa_pubkey,
                activationEpoch: activation_epoch,
                exitEpoch: exit_epoch,
                validatorIndex: Uint::<24, 1>::from(i),
                currentStatus: active_status,
            }
        }).collect();

        let registry_init_params = (Address::random(), U256::from(1_000_000e18), U256::from(10_000e18), initial_validators.clone(), Address::random()).abi_encode_params();
        let init_call = [&registry_init_selector, &registry_init_params[..]].concat();

        // construct proxy deployment and initialize txs
        let gas_price = 100;
        let pre_genesis_chain: Arc<ChainSpec> = Arc::new(tmp_genesis.into());
        let registry_tx_raw = tx_factory.create_eip1559(
            tmp_chain.clone(),
            gas_price,
            None,
            U256::ZERO,
            registry_create_data.clone().into(),
        );
        // registry deployment will be `factory_address`'s first tx
        let registry_proxy_address = factory_address.create(0);
        let initialize_tx_raw = tx_factory.create_eip1559(
            tmp_chain.clone(),
            gas_price,
            Some(registry_proxy_address),
            U256::ZERO,
            init_call.clone().into(),
        );
        let raw_txs = vec![registry_tx_raw, initialize_tx_raw];

        let execution_outcome = get_contract_state_for_genesis(pre_genesis_chain, raw_txs).await.expect("unable to fetch contract state");
        println!("{:?}", execution_outcome);
        println!("{:?}", alloy::hex::encode(init_call));

        // fetch state to be set on registry address
        let execution_bundle = execution_outcome.bundle;
        let execution_storage_registry = &execution_bundle
            .state
            .get(&registry_proxy_address)
            .expect("registry address missing from bundle state")
            .storage;
        let registry_proxy_bytecode = *ERC1967PROXY_RUNTIMECODE;

        // perform real genesis with fetched storage
        let genesis_accounts = vec![
            (registry_impl_address, GenesisAccount::default().with_code(Some(registry_impl_bytecode.into()))),
            (   
                registry_proxy_address, 
                GenesisAccount::default()
                    .with_code(Some(registry_proxy_bytecode.into()))
                    .with_storage(Some(
                        execution_storage_registry
                            .iter()
                            .map(|(k, v)| ((*k).into(), v.present_value.into()))
                            .collect(),
                    )),
            )
        ];

        // start chain with fetched storage
        let real_genesis = adiri_genesis();
        let genesis = real_genesis.extend_accounts(genesis_accounts.into_iter());
        // add read / write
    }
}

/*
foundry:
61af9e69
00000000000000000000000000000000000000000000000000000000000007e1
00000000000000000000000000000000000000000000d3c21bcecceda1000000
00000000000000000000000000000000000000000000021e19e0c9bab2400000
00000000000000000000000000000000000000000000000000000000000000a0
0000000000000000000000000000000000000000000000000000000000c0ffee
0000000000000000000000000000000000000000000000000000000000000004
0000000000000000000000000000000000000000000000000000000000000080
00000000000000000000000000000000000000000000000000000000000001e0
0000000000000000000000000000000000000000000000000000000000000340
00000000000000000000000000000000000000000000000000000000000004a0
00000000000000000000000000000000000000000000000000000000000000e0
290decd9548b62a8d60345a988386fc84ba6bc95484008f6362f93160ef3e563
000000000000000000000000000000000000000000000000000000000000babe
0000000000000000000000000000000000000000000000000000000000000000
0000000000000000000000000000000000000000000000000000000000000000
0000000000000000000000000000000000000000000000000000000000000001
0000000000000000000000000000000000000000000000000000000000000002
0000000000000000000000000000000000000000000000000000000000000060
290decd9548b62a8d60345a988386fc84ba6bc95484008f6362f93160ef3e563
290decd9548b62a8d60345a988386fc84ba6bc95484008f6362f93160ef3e563
290decd9548b62a8d60345a988386fc84ba6bc95484008f6362f93160ef3e563
00000000000000000000000000000000000000000000000000000000000000e0
b10e2d527612073b26eecdfd717e6a320cf44b4afac2b0732d9fcbe2b7fa0cf6
000000000000000000000000000000000000000000000000000000000bababee
0000000000000000000000000000000000000000000000000000000000000000
0000000000000000000000000000000000000000000000000000000000000000
0000000000000000000000000000000000000000000000000000000000000002
0000000000000000000000000000000000000000000000000000000000000002
0000000000000000000000000000000000000000000000000000000000000060
b10e2d527612073b26eecdfd717e6a320cf44b4afac2b0732d9fcbe2b7fa0cf6
b10e2d527612073b26eecdfd717e6a320cf44b4afac2b0732d9fcbe2b7fa0cf6
b10e2d527612073b26eecdfd717e6a320cf44b4afac2b0732d9fcbe2b7fa0cf6
00000000000000000000000000000000000000000000000000000000000000e0
405787fa12a823e0f2b7631cc41b3ba8828b3321ca811111fa75cd3aa3bb5ace
000000000000000000000000000000000000000000000000000000babababeee
0000000000000000000000000000000000000000000000000000000000000000
0000000000000000000000000000000000000000000000000000000000000000
0000000000000000000000000000000000000000000000000000000000000003
0000000000000000000000000000000000000000000000000000000000000002
0000000000000000000000000000000000000000000000000000000000000060
405787fa12a823e0f2b7631cc41b3ba8828b3321ca811111fa75cd3aa3bb5ace
405787fa12a823e0f2b7631cc41b3ba8828b3321ca811111fa75cd3aa3bb5ace
405787fa12a823e0f2b7631cc41b3ba8828b3321ca811111fa75cd3aa3bb5ace
00000000000000000000000000000000000000000000000000000000000000e0
c2575a0e9e593c00f959f8c92f12db2869c3395a3b0502d05e2516446f71f85b
000000000000000000000000000000000000000000000000000bababababeeee
0000000000000000000000000000000000000000000000000000000000000000
0000000000000000000000000000000000000000000000000000000000000000
0000000000000000000000000000000000000000000000000000000000000004
0000000000000000000000000000000000000000000000000000000000000002
0000000000000000000000000000000000000000000000000000000000000060
c2575a0e9e593c00f959f8c92f12db2869c3395a3b0502d05e2516446f71f85b
c2575a0e9e593c00f959f8c92f12db2869c3395a3b0502d05e2516446f71f85b
c2575a0e9e593c00f959f8c92f12db2869c3395a3b0502d05e2516446f71f85b

abi_encode_params:
61af9e69
0000000000000000000000002b8fff9ddbebada8c44e26481ed20f0d33abddf3
00000000000000000000000000000000000000000000d3c21bcecceda0000000
00000000000000000000000000000000000000000000021e19e0c9bab2400000
00000000000000000000000000000000000000000000000000000000000000a0
000000000000000000000000d0309dc1d4d74b508453f2769bde804a7132048b
0000000000000000000000000000000000000000000000000000000000000004
0000000000000000000000000000000000000000000000000000000000000080
00000000000000000000000000000000000000000000000000000000000001e0
0000000000000000000000000000000000000000000000000000000000000340
00000000000000000000000000000000000000000000000000000000000004a0
00000000000000000000000000000000000000000000000000000000000000e0
5c2cf00a87454e353040cda6b13f89384d79a32ce28bd4ca31bcf49c507f08bb
000000000000000000000000adc6bead9c9851ace029757ebc709a6712c6a3a4
0000000000000000000000000000000000000000000000000000000000000000
0000000000000000000000000000000000000000000000000000000000000000
0000000000000000000000000000000000000000000000000000000000000001
0000000000000000000000000000000000000000000000000000000000000002
0000000000000000000000000000000000000000000000000000000000000060
97f8ce9fd7dd08e9b1bc2438edc740f1163ebac9d7e771a2e00a77b80e1858a9
b5d6f9f9255271f30ff34854bebcc72a196e110c806a5fc3ff23b574c6e9e0f8
e3e70e7e2c66e0e3cd9183ab49c148483d219197b7843c047483774e5341e7a6
00000000000000000000000000000000000000000000000000000000000000e0
30710b15d2bd43974866f4a7a008ac55c4ffcd5258d5fdca3cedc9dd74857327
00000000000000000000000027c5c95c755b0e4f7f1d1807ed22d3b8e6e522ad
0000000000000000000000000000000000000000000000000000000000000000
0000000000000000000000000000000000000000000000000000000000000000
0000000000000000000000000000000000000000000000000000000000000002
0000000000000000000000000000000000000000000000000000000000000002
0000000000000000000000000000000000000000000000000000000000000060
814b4be81553a9acc708ffe1368175ea90a5547651f13100195a97160c96d209
aaa4bc6b4754df0cb7510a74b0f9d55e0f252c9038491c7c8ed770425f76df58
0eba52ccb03da82611b617fdac5c288dcc341cec907bb5c757151c6d6878155a
00000000000000000000000000000000000000000000000000000000000000e0
bf85176cc0f677942d2ffc5c97a060dac6467ed075f2da36094e6b551d4faea7
000000000000000000000000c9b8899b3725fc07df5e4b0fc3c030136e77b854
0000000000000000000000000000000000000000000000000000000000000000
0000000000000000000000000000000000000000000000000000000000000000
0000000000000000000000000000000000000000000000000000000000000003
0000000000000000000000000000000000000000000000000000000000000002
0000000000000000000000000000000000000000000000000000000000000060
8d2822eccbfeb03de2501f6fd71b7af3e62b78a5fcf80305421c05967761398a
e88a02796361574c7dc97a04811af694178663a9be9229e2f8a6c104ecd6fb66
3c04abec8eb58c4c6d212b9eec7242fcbf17ea28f77b5e7431a5f2305cc4831e
00000000000000000000000000000000000000000000000000000000000000e0
c7d972d946c2c19e5f034b878e9bffdaaa97349d27b60f9333ac0c4ff36fe30b
000000000000000000000000b890150dca0e49cf380bafbca93c6753e0133f7a
0000000000000000000000000000000000000000000000000000000000000000
0000000000000000000000000000000000000000000000000000000000000000
0000000000000000000000000000000000000000000000000000000000000004
0000000000000000000000000000000000000000000000000000000000000002
0000000000000000000000000000000000000000000000000000000000000060
90553e55328b315077ce36d7fe5487454bf145aa7c18b709e11ad24d723e26b2
0f8d97bf3cbc78d1d3862e9db6611d850450792eb00a316ddb21a153c69b76a2
1b2924f1cc4dfe102492cd1474e288365dd7ff1bb7915481cc69deaa4b5278f6

*/