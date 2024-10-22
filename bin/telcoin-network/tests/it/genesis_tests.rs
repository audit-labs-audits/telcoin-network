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
        let registry_impl_address = Address::from_str("0x65b54A4646369D8ad83CB58A5a6b39F22fcd8cEe").expect("omg");
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
        let mut rng = StdRng::from_entropy();
        let bls_keypair = BlsKeypair::generate(&mut rng);
        let bls_pubkey = bls_keypair.public().as_bytes().to_vec();
        let ed_25519_keypair = NetworkKeypair::generate(&mut rng);
        let ecdsa_pubkey = Address::random();
        let activation_epoch = u32::default();
        let exit_epoch = u32::default();
        let active_status = ConsensusRegistry::ValidatorStatus::Active;

        // construct array of 4 validators with 1-indexed `validatorIndex`
        let initial_validators: Vec<ConsensusRegistry::ValidatorInfo> = (1..=4).map(|i| {
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

        // `initial_validators` length is not written correctly by `abi_encode_params()` - it writes length 0x20 regardless of array length (0x04)
        let registry_init_params = (Address::random(), U256::from(1_000_000e18), U256::from(10_000e18), initial_validators.clone(), Address::random()).abi_encode_params();
        
        let init_call = [&registry_init_selector, &registry_init_params[..]].concat();
        // let init_call = Bytes::from_str("61af9e6900000000000000000000000000000000000000000000000000000000000007e100000000000000000000000000000000000000000000d3c21bcecceda100000000000000000000000000000000000000000000000000021e19e0c9bab240000000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000c0ffee0000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000e0011201deed66c3b3a1b2afb246b1436fd291a5f4b65e4ff0094a013cd922f803000000000000000000000000000000000000000000000000000000000000babe00000000000000000000000000000000000000000000000000000000ffffffff00000000000000000000000000000000000000000000000000000000ffffffff00000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000006074f9856d5cce56785fd436368e27c4dbfadb0ae8a9ce40873291ae4bcebad41974f9856d5cce56785fd436368e27c4dbfadb0ae8a9ce40873291ae4bcebad41974f9856d5cce56785fd436368e27c4dbfadb0ae8a9ce40873291ae4bcebad419").expect("omg");

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
00000000000000000000000000000000000000000000000000000000000007e1 rwtel
00000000000000000000000000000000000000000000d3c21bcecceda1000000 stakeamt
00000000000000000000000000000000000000000000021e19e0c9bab2400000 minwithdraw
00000000000000000000000000000000000000000000000000000000000000a0 initialvalidators offs
0000000000000000000000000000000000000000000000000000000000c0ffee owner
0000000000000000000000000000000000000000000000000000000000000001 len
0000000000000000000000000000000000000000000000000000000000000020 struct offs
00000000000000000000000000000000000000000000000000000000000000e0 bls ofs
011201deed66c3b3a1b2afb246b1436fd291a5f4b65e4ff0094a013cd922f803 ed25519
000000000000000000000000000000000000000000000000000000000000babe ecdsa
00000000000000000000000000000000000000000000000000000000ffffffff activation
00000000000000000000000000000000000000000000000000000000ffffffff exit
0000000000000000000000000000000000000000000000000000000000000001 index
0000000000000000000000000000000000000000000000000000000000000002 status
0000000000000000000000000000000000000000000000000000000000000060 bls len (e0)
74f9856d5cce56785fd436368e27c4dbfadb0ae8a9ce40873291ae4bcebad419 bls
74f9856d5cce56785fd436368e27c4dbfadb0ae8a9ce40873291ae4bcebad419 bls 
74f9856d5cce56785fd436368e27c4dbfadb0ae8a9ce40873291ae4bcebad419 bls

abi_encode_params:

*/