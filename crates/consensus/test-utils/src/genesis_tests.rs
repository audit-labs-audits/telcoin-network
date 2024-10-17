#[cfg(test)]
mod tests {
    use crate::default_test_execution_node;
    use rand::{rngs::StdRng, SeedableRng};
    use reth_chainspec::ChainSpec;
    use tn_types::{
        adiri_genesis, test_utils::{contract_artifacts::{CONSENSUSREGISTRY_RUNTIMECODE, ERC1967PROXY_INITCODE}, execution_outcome_for_tests, TransactionFactory}, BlsKeypair, NetworkKeypair, WorkerBlock
    };
    use alloy::{primitives::FixedBytes, sol, sol_types::SolValue};
    use fastcrypto::traits::{KeyPair, ToFromBytes};
    // use reth_primitives::{ruint::aliases::U32, Address, Bytes, GenesisAccount, SealedHeader, B256, U256};
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
                    uint16 validatorIndex;
                    bytes4 unused;
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

        let registry_init_selector = [147, 34, 229, 182];
        let mut rng = StdRng::from_entropy();
        // let bls_keypair = BlsKeypair::generate(&mut rng);
        let bls_pubkey = Bytes::from_str("123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456").expect("bad bls string");// bls_keypair.public().as_bytes().to_vec();
        let ed_25519_keypair = NetworkKeypair::generate(&mut rng);
        let ecdsa_pubkey = Address::random();
        let activation_epoch = u32::default();
        let exit_epoch = u32::default();
        let active_status = ConsensusRegistry::ValidatorStatus::Active;
        // let base_validator = ConsensusRegistry::ValidatorInfo {
        //     blsPubkey: bls_pubkey,
        //     ed25519Pubkey: FixedBytes::<32>::from_slice(ed_25519_keypair.public().as_bytes()),
        //     ecdsaPubkey: ecdsa_pubkey,
        //     activationEpoch: activation_epoch,
        //     exitEpoch: exit_epoch,
        //     validatorIndex: u16::default(),
        //     unused: FixedBytes::<4>::default(),
        //     currentStatus: active_status,
        // };

        // let first_validator = ConsensusRegistry::ValidatorInfo {
        //     validatorIndex: 1,
        //     ..base_validator.clone()
        // };
        // let second_validator = ConsensusRegistry::ValidatorInfo {
        //     validatorIndex: 2,
        //     ..base_validator.clone()
        // };
        // let third_validator = ConsensusRegistry::ValidatorInfo {
        //     validatorIndex: 3,
        //     ..base_validator.clone()
        // };
        // let fourth_validator = ConsensusRegistry::ValidatorInfo {
        //     validatorIndex: 4,
        //     ..base_validator.clone()
        // };

        // let initial_validators = [first_validator /* , second_validator, third_validator, fourth_validator*/];

        // even inline struct definition doesn't include correct array length member in abi encoding 
        // let initial_validators = [(
        //     bls_pubkey, 
        //     FixedBytes::<32>::from_slice(ed_25519_keypair.public().as_bytes()),
        //     ecdsa_pubkey,
        //     activation_epoch,
        //     exit_epoch,
        //     u16::default(),
        //     FixedBytes::<4>::default(),
        //     active_status
        // )].abi_encode_params();

        // `initial_validators` length is not written correctly by `abi_encode_params()` - it writes length 0x20 regardless of array length (0x04)
        // let registry_init_params = (Address::random(), U256::from(1_000_000e18), U256::from(10_000e18), initial_validators.clone(), Address::random()).abi_encode_params();
        
        // let init_call = [&registry_init_selector, &registry_init_params[..]].concat();
        let init_call = Bytes::from_str("9322e5b600000000000000000000000000000000000000000000000000000000000007e100000000000000000000000000000000000000000000d3c21bcecceda100000000000000000000000000000000000000000000000000021e19e0c9bab240000000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000c0ffee000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000100011201deed66c3b3a1b2afb246b1436fd291a5f4b65e4ff0094a013cd922f803000000000000000000000000000000000000000000000000000000000000babe00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000003074f9856d5cce56785fd436368e27c4dbfadb0ae8a9ce40873291ae4bcebad4195252e822906cbbb969d9fa097bb45a6600000000000000000000000000000000").expect("omg");

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
            init_call.into(),
        );
        let raw_txs = vec![registry_tx_raw, initialize_tx_raw];

        // execution components
        let manager = TaskManager::current();
        let executor = manager.executor();
        let execution_node = default_test_execution_node(Some(pre_genesis_chain.clone()), None, executor).expect("test execution node");
        let provider = execution_node.get_provider().await;
        let block_executor = execution_node.get_block_executor().await;
        // execute batch
        let batch = WorkerBlock::new(raw_txs, SealedHeader::default());
        let parent = pre_genesis_chain.sealed_genesis_header();
        let execution_outcome = execution_outcome_for_tests(&batch, &parent, &provider, &block_executor);
        println!("{:?}", execution_outcome);
        // println!("{:?}", alloy::hex::encode(init_call));
        // println!("{:?}", alloy::hex::encode(initial_validators));
    }
}

/* correct calldata generated by solidity abi encoder
9322e5b6
00000000000000000000000000000000000000000000000000000000000007e1
00000000000000000000000000000000000000000000d3c21bcecceda1000000
00000000000000000000000000000000000000000000021e19e0c9bab2400000
00000000000000000000000000000000000000000000000000000000000000a0
0000000000000000000000000000000000000000000000000000000000c0ffee
0000000000000000000000000000000000000000000000000000000000000001 this word is excluded by sol abi encoder
0000000000000000000000000000000000000000000000000000000000000020
0000000000000000000000000000000000000000000000000000000000000100
011201deed66c3b3a1b2afb246b1436fd291a5f4b65e4ff0094a013cd922f803
000000000000000000000000000000000000000000000000000000000000babe
0000000000000000000000000000000000000000000000000000000000000000
0000000000000000000000000000000000000000000000000000000000000000
0000000000000000000000000000000000000000000000000000000000000001
0000000000000000000000000000000000000000000000000000000000000000
0000000000000000000000000000000000000000000000000000000000000002
0000000000000000000000000000000000000000000000000000000000000030
74f9856d5cce56785fd436368e27c4dbfadb0ae8a9ce40873291ae4bcebad419
5252e822906cbbb969d9fa097bb45a6600000000000000000000000000000000
*/