//! Integration test for RPC Faucet feature.
//!
//! The faucet receives an rpc request containing an address and submits
//! a direct transfer to the address if it is not found in the LRU time-based
//! cache. The signing process is handled by an API call to Google KMS using
//! secp256k1 algorithm. However, additional information is needed for the
//! signature to be EVM compatible. The faucet service does all of this and
//! then submits the transaction to the RPC Transaction Pool for the next batch.

use crate::util::create_validator_info;
use clap::Parser;
use gcloud_sdk::{
    google::cloud::kms::v1::{
        key_management_service_client::KeyManagementServiceClient, GetPublicKeyRequest,
    },
    GoogleApi, GoogleAuthMiddleware, GoogleEnvironment,
};
use jsonrpsee::{
    core::client::ClientT,
    http_client::{HttpClient, HttpClientBuilder},
    rpc_params,
};
use k256::{elliptic_curve::sec1::ToEncodedPoint, pkcs8::DecodePublicKey, FieldBytes, PublicKey as PubKey};
use secp256k1::Secp256k1;
use narwhal_test_utils::CommandParser;
use reth::{
    tasks::{TaskExecutor, TaskManager},
    CliContext,
};
use reth_chainspec::ChainSpec;
use reth_node_ethereum::{EthEvmConfig, EthExecutorProvider};
use reth_primitives::{
    alloy_primitives::U160, public_key_to_address, Address, Bytes, GenesisAccount, U256,
};
use reth_tracing::init_test_tracing;
use secp256k1::PublicKey;
use std::{env, str::FromStr, sync::Arc, time::Duration};
use alloy::{hex, providers::{Provider, ProviderBuilder}, network::{EthereumWallet, TransactionBuilder}, sol_types::SolValue, signers::local::PrivateKeySigner};
use telcoin_network::{genesis::GenesisArgs, node::NodeCommand};
use tn_faucet::FaucetArgs;
use tn_node::launch_node;
use tn_types::{adiri_genesis, test_utils::{TransactionFactory, deploy_contract_faucet_initialize, deploy_contract_stablecoin, deploy_contract_proxy}};
use tokio::{runtime::Handle, task::JoinHandle, time::timeout};
use tracing::{error, info};
use rand::{SeedableRng, rngs::StdRng};
use dotenvy::dotenv;

#[tokio::test]
async fn test_faucet_transfers_tel_with_google_kms_e2e() -> eyre::Result<()> {
    init_test_tracing();

    // task manager
    let manager = TaskManager::new(Handle::current());
    let task_executor = manager.executor();

    // create google env and chain spec
    let chain = prepare_google_kms_env().await?;

    // setup env for `transfer_admin_role()` which is necessary to reproduce counterfactual faucet 
    dotenv().ok();
    let init_admin_key = env::var("SHARED_PRIVATE_KEY").expect("SHARED_PRIVATE_KEY not found");
    let init_admin_signer = PrivateKeySigner::from_str(&init_admin_key).expect("Unable to parse env string");
    let init_admin_wallet = EthereumWallet::from(init_admin_signer);

    // create and launch validator nodes on local network
    spawn_local_testnet(&task_executor, chain.clone()).await?;

    info!("nodes started");

    tokio::time::sleep(Duration::from_secs(10)).await;

    let address = Address::from(U160::from(8991));
    let rpc_url = "http://127.0.0.1:8545".to_string();
    let client = HttpClientBuilder::default().build(&rpc_url)?;

    // assert starting balance is 0
    let starting_balance: String = client.request("eth_getBalance", rpc_params!(address)).await?;
    println!("starting balance: {starting_balance:?}");
    assert_eq!(U256::from_str(&starting_balance)?, U256::ZERO);

    // assert deployer starting balance is properly seeded
    let default_deployer_address = TransactionFactory::default().address();
    let deployer_balance: String = client.request("eth_getBalance", rpc_params!(default_deployer_address)).await?;
    println!("Deployer starting balance: {deployer_balance:?}");
    assert_eq!(U256::from_str(&deployer_balance)?, U256::MAX);

    // get values for default (insecure) account
    let mut rng = StdRng::from_seed([0; 32]);
    let (private_key, _) = Secp256k1::new().generate_keypair(&mut rng);
    // circumvent Secp256k1 <> k256 type incompatibility via FieldBytes intermediary
    let binding = private_key.secret_bytes();
    let secret_bytes_array = FieldBytes::from_slice(&binding);
    let signer = PrivateKeySigner::from_field_bytes(secret_bytes_array).expect("Error constructing signer from private key");
    let wallet = EthereumWallet::from(signer.clone());
    let provider = ProviderBuilder::new().with_recommended_fillers().wallet(wallet.clone()).on_http(rpc_url.parse()?);

    // deploy stablecoin contracts and initialize(create2 not needed)
    let stablecoin_impl = deploy_contract_stablecoin(&rpc_url, Some(&wallet)).await?;
    // keccak256("initialize(string,string,uint8)"") = 0x1624f6c6
    let stablecoin_init_selector = [22, 36, 246, 198];
    let stablecoin_init_params = ("Telcoin NOK", "eNOK", 6).abi_encode_params();
    let stablecoin_init_data = [&stablecoin_init_selector, &stablecoin_init_params[..]].concat().into();
    let stablecoin_contract = deploy_contract_proxy(&rpc_url, stablecoin_impl, stablecoin_init_data, Some(&wallet)).await?;

    // deploy faucet contracts and initialize + upgrade to recreate create2 address with current impl version
    let faucet_contract = deploy_contract_faucet_initialize(&rpc_url, init_admin_wallet, Some(&wallet)).await?;

    // keccak256("UpdateXYZ(address,bool,uint256,uint256)") = 0xe9aea396
    let update_xyz_selector = [233, 174, 163, 150];
    let update_xyz_params = (stablecoin_contract, true, U256::MAX, U256::from(1000)).abi_encode_params();
    let update_xyz_data: Bytes = [&update_xyz_selector, &update_xyz_params[..]].concat().into();
    let update_xyz_tx = provider.transaction_request()
        .with_to(faucet_contract)
        .with_input(update_xyz_data);
    // register stablecoin in faucet
    let pending_update_xyz_tx = provider.send_transaction(update_xyz_tx).await?;
    println!("Pending registration of XYZ with StablecoinManager in tx hash: {}", pending_update_xyz_tx.tx_hash());
    let update_xyz_receipt = pending_update_xyz_tx.get_receipt().await?;
    println!("Registration transaction confirmed in block: {}", update_xyz_receipt.block_number.expect("XYZ registration failed"));

    // keccak256("grantRole(bytes32,address)")= 0x2f2ff15d
    let grant_role_selector = [47, 47, 241, 93];
    // role can be fetched with `Stablecoin::MINTER_ROLE()`
    let grant_role_params = (hex!("9f2df0fed2c77648de5860a4cc508cd0818c85b8b8a1ab4ceeef8d981c8956a6"), faucet_contract).abi_encode_params();
    let grant_role_data: Bytes = [&grant_role_selector, &grant_role_params[..]].concat().into();
    let grant_role_tx = provider.transaction_request()
        .with_to(stablecoin_contract)
        .with_input(grant_role_data);
    // grant `MINTER_ROLE` to faucet
    let pending_grant_role_tx = provider.send_transaction(grant_role_tx).await?;
    println!("Pending minter role granting to faucet on XYZ contract in tx hash: {}", pending_grant_role_tx.tx_hash());
    let grant_role_receipt = pending_grant_role_tx.get_receipt().await?;
    println!("Granted minter role to faucet in block: {}", grant_role_receipt.block_number.expect("Failed to grant minter role to faucet"));

    // note: response is different each time bc KMS
    let zero_address = Address::from(U160::ZERO);
    let tx_hash: String = client.request("faucet_transfer", rpc_params![address, zero_address]).await?;
    info!(target: "faucet-transaction", ?tx_hash);

    // more than enough time for the nodes to launch RPCs
    let duration = Duration::from_secs(30);

    // ensure account balance increased
    let balance = timeout(duration, ensure_account_balance(&client, address))
        .await?
        .expect("balance timeout");
    let expected_balance = U256::from_str("0xde0b6b3a7640000")?; // 1*10^18 (1 TEL)
    assert_eq!(balance, expected_balance);

    // duplicate request is err
    assert!(client.request::<String, _>("faucet_transfer", rpc_params![address]).await.is_err());

    Ok(())
}

/// Retrieve the public key from KMS.
///
/// This simulates what the startup script should do on deployed nodes:
/// - set an env variable to the PEM formatted key.
async fn set_google_kms_public_key_env_var() -> eyre::Result<()> {
    // Detect Google project ID using environment variables PROJECT_ID/GCP_PROJECT_ID
    // or GKE metadata server when the app runs inside GKE
    let google_project_id = GoogleEnvironment::detect_google_project_id().await
        .expect("No Google Project ID detected. Please specify it explicitly using env variable: PROJECT_ID");

    let kms_client: GoogleApi<KeyManagementServiceClient<GoogleAuthMiddleware>> =
        GoogleApi::from_function(
            KeyManagementServiceClient::new,
            "https://cloudkms.googleapis.com",
            None,
        )
        .await?;

    // retrieve api information from env
    let locations = std::env::var("KMS_KEY_LOCATIONS")
        .expect("KMS_KEY_LOCATIONS must be set in the environment");
    let key_rings =
        std::env::var("KMS_KEY_RINGS").expect("KMS_KEY_RINGS must be set in the environment");
    let crypto_keys =
        std::env::var("KMS_CRYPTO_KEYS").expect("KMS_CRYPTO_KEYS must be set in the environment");
    let crypto_key_versions = std::env::var("KMS_CRYPTO_KEY_VERSIONS")
        .expect("KMS_CRYPTO_KEY_VERSIONS must be set in the environment");

    // construct api endpoint for Google KMS requests
    let name = format!(
        "projects/{}/locations/{}/keyRings/{}/cryptoKeys/{}/cryptoKeyVersions/{}",
        google_project_id, locations, key_rings, crypto_keys, crypto_key_versions
    );

    // request KMS public key
    let kms_pubkey_response =
        kms_client.get().get_public_key(GetPublicKeyRequest { name: name.clone() }).await?;

    // convert pem pubkey format
    let kms_pem_pubkey = kms_pubkey_response.into_inner().pem;
    // store to env
    std::env::set_var("FAUCET_PUBLIC_KEY", kms_pem_pubkey);

    Ok(())
}

/// Use Google KMS credentials json to fetch public key, seed account at genesis, and set env vars
/// for faucet signature requests.
async fn prepare_google_kms_env() -> eyre::Result<Arc<ChainSpec>> {
    // set application credentials for accessing Google KMS API
    std::env::set_var(
        "GOOGLE_APPLICATION_CREDENTIALS",
        "../../crates/execution/faucet/gcloud-credentials.json",
    );
    // set Project ID for google_sdk
    std::env::set_var("PROJECT_ID", "telcoin-network");
    // set env vars for faucet cli
    std::env::set_var("KMS_KEY_LOCATIONS", "global");
    std::env::set_var("KMS_KEY_RINGS", "tests");
    std::env::set_var("KMS_CRYPTO_KEYS", "key-for-unit-tests");
    std::env::set_var("KMS_CRYPTO_KEY_VERSIONS", "1");

    // fetch kms address from google and set env
    set_google_kms_public_key_env_var().await?;
    let kms_pem_pubkey = std::env::var("FAUCET_PUBLIC_KEY")?;
    // k256 public key to convert from pem
    let pubkey_from_pem = PubKey::from_public_key_pem(&kms_pem_pubkey)?;
    // secp256k1 public key from uncompressed k256 variation
    let public_key = PublicKey::from_slice(pubkey_from_pem.to_encoded_point(false).as_bytes())?;
    // calculate address from uncompressed public key
    let wallet_address = public_key_to_address(public_key);

    // create genesis and fund relevant accounts
    let genesis = adiri_genesis();
    let faucet_account = vec![(wallet_address, GenesisAccount::default().with_balance(U256::MAX))];
    let default_deployer_address = TransactionFactory::default().address();
    let default_deployer_account = vec![(default_deployer_address, GenesisAccount::default().with_balance(U256::MAX))];

    let accounts_to_fund = faucet_account.into_iter().chain(default_deployer_account.into_iter());
    let genesis = genesis.extend_accounts(accounts_to_fund);
    
    Ok(Arc::new(genesis.into()))
}

/// Create validator info, genesis ceremony, and spawn node command with faucet active.
async fn spawn_local_testnet(
    task_executor: &TaskExecutor,
    chain: Arc<ChainSpec>,
) -> eyre::Result<Vec<JoinHandle<()>>> {
    // create temp path for test
    let temp_path = tempfile::TempDir::new().expect("tempdir is okay").into_path();

    let validators = ["validator-1", "validator-2", "validator-3", "validator-4"];

    // create shared genesis dir
    let shared_genesis_dir = temp_path.join("shared-genesis");
    let copy_path = shared_genesis_dir.join("genesis/validators");
    std::fs::create_dir_all(&copy_path)?;

    // create validator info and copy to shared genesis dir
    for v in validators.into_iter() {
        let dir = temp_path.join(v);
        let datadir = dir.to_str().expect("validator temp dir");
        // init genesis ceremony to create committee / worker_cache files
        create_validator_info(datadir, "0").await?;

        // copy to shared genesis dir
        let copy = dir.join("genesis/validators");
        for config in std::fs::read_dir(copy)? {
            let entry = config?;
            std::fs::copy(entry.path(), copy_path.join(entry.file_name()))?;
        }
    }

    // create committee from shared genesis dir
    let create_committee_command = CommandParser::<GenesisArgs>::parse_from([
        "tn",
        "create-committee",
        "--datadir",
        shared_genesis_dir.to_str().expect("shared genesis dir"),
    ]);
    create_committee_command.args.execute().await?;

    let mut node_handles = Vec::with_capacity(validators.len());
    for v in validators.into_iter() {
        let dir = temp_path.join(v);
        let datadir = dir.to_str().expect("validator temp dir");

        // copy genesis files back to validator dirs
        std::fs::copy(
            shared_genesis_dir.join("genesis/committee.yaml"),
            dir.join("genesis/committee.yaml"),
        )?;
        std::fs::copy(
            shared_genesis_dir.join("genesis/worker_cache.yaml"),
            dir.join("genesis/worker_cache.yaml"),
        )?;

        let instance = v.chars().last().expect("validator instance").to_string();

        let mut command = NodeCommand::<FaucetArgs>::parse_from([
            "tn",
            "--dev",
            "--datadir",
            datadir,
            //
            // TODO: debug max-block doesn't work
            //
            // "--debug.max-block",
            // "5",
            // "--debug.terminate",
            "--chain",
            "adiri",
            "--instance",
            &instance,
            "--google-kms",
        ]);

        let cli_ctx = CliContext { task_executor: task_executor.clone() };

        // update genesis with seeded accounts
        command.chain = chain.clone();

        // collect join handles
        node_handles.push(task_executor.spawn_critical(
            v,
            Box::pin(async move {
                let err = command
                    .execute(
                        cli_ctx,
                        false, // don't overwrite chain with the default
                        |mut builder, faucet_args, tn_datadir| async move {
                            builder.opt_faucet_args = Some(faucet_args);
                            let evm_config = EthEvmConfig::default();
                            let executor = EthExecutorProvider::new(
                                std::sync::Arc::clone(&builder.node_config.chain),
                                evm_config,
                            );
                            launch_node(builder, executor, evm_config, &tn_datadir).await
                        },
                    )
                    .await;
                error!("{:?}", err);
            }),
        ));
    }

    Ok(node_handles)
}

/// RPC request to continually check until an account balance is above 0.
///
/// Warning: this should only be called with a timeout - could result in infinite loop otherwise.
async fn ensure_account_balance(client: &HttpClient, address: Address) -> eyre::Result<U256> {
    while let Ok(bal) = client.request::<String, _>("eth_getBalance", rpc_params!(address)).await {
        println!("bal: {bal:?}");
        let balance = U256::from_str(&bal)?;
        if balance > U256::ZERO {
            return Ok(balance);
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    Ok(U256::ZERO)
}
