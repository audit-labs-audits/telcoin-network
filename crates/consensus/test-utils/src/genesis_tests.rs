#[cfg(test)]
mod tests {
    use crate::default_test_execution_node;
    use rand::{rngs::StdRng, SeedableRng};
    use reth_chainspec::ChainSpec;
    use tn_types::{
        adiri_genesis, test_utils::{contract_artifacts::{CONSENSUSREGISTRY_RUNTIMECODE, ERC1967PROXY_INITCODE}, execution_outcome_for_tests, TransactionFactory}, BlsKeypair, NetworkKeypair, PrimaryInfo, WorkerBlock
    };
    use alloy::{primitives::FixedBytes, sol, sol_types::SolValue};
    use fastcrypto::traits::{KeyPair, ToFromBytes};
    // use reth_primitives::{ruint::aliases::U32, Address, Bytes, GenesisAccount, SealedHeader, B256, U256};
    use std::sync::Arc;
    use reth::{primitives::{Address, Bytes, GenesisAccount, SealedHeader, U256}, tasks::TaskManager};

    #[tokio::test]
    async fn test_genesis_with_consensus_registry() {
        let network_genesis = adiri_genesis();
        let pre_genesis_chain: Arc<ChainSpec> = Arc::new(network_genesis.into());
        let registry_impl_address = Address::random();
        let registry_impl_bytecode = *CONSENSUSREGISTRY_RUNTIMECODE;
        let mut tx_factory = TransactionFactory::new();
        let factory_address = tx_factory.address();

        // deploy impl and fund `factory_address`
        let tmp_genesis = pre_genesis_chain.genesis.clone().extend_accounts(
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
        let bls_pubkey = BlsKeypair::generate(&mut rng).public().as_bytes();
        let ed_25519_pubkey = NetworkKeypair::generate(&mut rng).public().as_bytes();
        let ecdsa_pubkey = Address::random();
        let activation_epoch = u32::default();
        let exit_epoch = u32::default();
        let active_status = ConsensusRegistry::ValidatorStatus::Active;
        let base_validator = ConsensusRegistry::ValidatorInfo {
            blsPubkey: bls_pubkey.into(),
            ed25519Pubkey: FixedBytes::<32>::from(ed_25519_pubkey.into()),
            ecdsaPubkey: ecdsa_pubkey,
            activationEpoch: activation_epoch,
            exitEpoch: exit_epoch,
            validatorIndex: u16::default(),
            unused: FixedBytes::<4>::default(),
            currentStatus: active_status,
        };
        let first_validator = ConsensusRegistry::ValidatorInfo {
            validatorIndex: 1,
            ..base_validator.clone()
        };
        let second_validator = ConsensusRegistry::ValidatorInfo {
            validatorIndex: 2,
            ..base_validator.clone()
        };
        let third_validator = ConsensusRegistry::ValidatorInfo {
            validatorIndex: 3,
            ..base_validator.clone()
        };
        let fourth_validator = ConsensusRegistry::ValidatorInfo {
            validatorIndex: 4,
            ..base_validator.clone()
        };
        let initial_validators = [first_validator, second_validator, third_validator, fourth_validator].abi_encode();
        let init_call = [&registry_init_selector, Address::random().abi_encode().as_slice(), U256::from(1_000_000e18).as_le_slice(), U256::from(10_000e18).as_le_slice(), &initial_validators, Address::random().abi_encode().as_slice()].concat();

        // construct proxy deployment and initialize txs
        let gas_price = 100;
        let registry_tx_raw = tx_factory.create_eip1559(
            pre_genesis_chain.clone(),
            gas_price,
            None,
            U256::ZERO,
            registry_create_data.clone().into(),
        );
        // registry deployment will be `factory_address`'s first tx
        let registry_proxy_address = factory_address.create(0);
        let initialize_tx_raw = tx_factory.create_eip1559(
            pre_genesis_chain.clone(),
            gas_price,
            Some(registry_proxy_address),
            U256::ZERO,
            init_call.clone().into(),
        );
        let raw_txs = vec![registry_tx_raw, initialize_tx_raw];

        // execution components
        let manager = TaskManager::current();
        let executor = manager.executor();
        let execution_node = default_test_execution_node(Some(pre_genesis_chain.clone()), None, executor)?;
        let provider = execution_node.get_provider().await;
        let block_executor = execution_node.get_block_executor().await;
        // execute batch
        let batch = WorkerBlock::new(raw_txs, SealedHeader::default());
        let parent = pre_genesis_chain.sealed_genesis_header();
        let execution_outcome = execution_outcome_for_tests(&batch, &parent, &provider, &block_executor);
    }
}