// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

//! Specific test utils for execution layer
use crate::{adiri_genesis, now, Batch, BatchAPI, ExecutionKeypair, MetadataAPI, TimestampSec, test_utils::artifacts};
use rand::{rngs::StdRng, Rng, SeedableRng};
use reth_chainspec::{BaseFeeParams, ChainSpec};
use reth_evm::execute::{BlockExecutionOutput, BlockExecutorProvider, Executor as _};
use reth_primitives::{
    constants::{
        EMPTY_TRANSACTIONS, EMPTY_WITHDRAWALS, ETHEREUM_BLOCK_GAS_LIMIT, MIN_PROTOCOL_BASE_FEE,
    }, proofs, public_key_to_address, sign_message, Address, Block, Bytes, FromRecoveredPooledTransaction, Genesis, GenesisAccount, Header, PooledTransactionsElement, SealedHeader, Signature, Transaction, TransactionSigned, TxEip1559, TxHash, TxKind, Withdrawals, B256, EMPTY_OMMER_ROOT_HASH, U256
};
use reth_provider::{BlockReaderIdExt, ExecutionOutcome, StateProviderFactory};
use reth_revm::database::StateProviderDatabase;
use reth_transaction_pool::{TransactionOrigin, TransactionPool};
use secp256k1::Secp256k1;
use std::{str::FromStr as _, sync::Arc};
use alloy::{hex, network::{EthereumWallet, TransactionBuilder}, providers::{Provider, ProviderBuilder}, signers::{k256::FieldBytes, local::PrivateKeySigner}, sol, sol_types::SolValue};

/// Adiri genesis with funded [TransactionFactory] default account.
pub fn test_genesis() -> Genesis {
    let genesis = adiri_genesis();
    let default_address = TransactionFactory::default().address();
    let default_factory_account =
        vec![(default_address, GenesisAccount::default().with_balance(U256::MAX))];
    genesis.extend_accounts(default_factory_account)
}

/// Helper function to seed an instance of Genesis with accounts from a random batch.
pub fn seeded_genesis_from_random_batch(
    genesis: Genesis,
    batch: &Batch,
) -> (Genesis, Vec<TransactionSigned>, Vec<Address>) {
    let mut txs = vec![];
    let mut senders = vec![];
    let mut accounts_to_seed = Vec::new();

    // loop through the transactions
    for tx in batch.transactions_owned() {
        let tx_signed =
            TransactionSigned::decode_enveloped(&mut tx.as_ref()).expect("decode tx signed");
        let address = tx_signed.recover_signer().expect("signer recoverable");
        txs.push(tx_signed);
        senders.push(address);
        // fund account with 99mil TEL
        let account = (
            address,
            GenesisAccount::default().with_balance(
                U256::from_str("0x51E410C0F93FE543000000").expect("account balance is parsed"),
            ),
        );
        accounts_to_seed.push(account);
    }
    (genesis.extend_accounts(accounts_to_seed), txs, senders)
}

/// Helper function to seed an instance of Genesis with random batches.
///
/// The transactions in the randomly generated batches are decoded and their signers are recovered.
///
/// The function returns the new Genesis, the signed transactions by batch, and the addresses for
/// further use it testing.
pub fn seeded_genesis_from_random_batches<'a>(
    mut genesis: Genesis,
    batches: impl IntoIterator<Item = &'a Batch>,
) -> (Genesis, Vec<Vec<TransactionSigned>>, Vec<Vec<Address>>) {
    let mut txs = vec![];
    let mut senders = vec![];
    for batch in batches {
        let (g, t, s) = seeded_genesis_from_random_batch(genesis, batch);
        genesis = g;
        txs.push(t);
        senders.push(s);
    }
    (genesis, txs, senders)
}

/// Optional parameters to pass to the `execute_test_batch` function.
///
/// These optional parameters are used to replace default in the batch's header if included.
pub struct OptionalTestBatchParams {
    /// Optional beneficiary address.
    ///
    /// Default is `Address::random()`.
    pub beneficiary_opt: Option<Address>,
    /// Optional withdrawals.
    ///
    /// Default is `Withdrawals<vec![]>` (empty).
    pub withdrawals_opt: Option<Withdrawals>,
    /// Optional timestamp.
    ///
    /// Default is `now()`.
    pub timestamp_opt: Option<TimestampSec>,
    /// Optional mix_hash.
    ///
    /// Default is `B256::random()`.
    pub mix_hash_opt: Option<B256>,
    /// Optional base_fee_per_gas.
    ///
    /// Default is [MIN_PROTOCOL_BASE_FEE], which is 7 wei.
    pub base_fee_per_gas_opt: Option<u64>,
}

/// Attempt to update batch with accurate header information.
///
/// NOTE: this is loosely based on reth's auto-seal consensus
pub fn execute_test_batch<P, E>(
    batch: &mut Batch,
    parent: &SealedHeader,
    optional_params: OptionalTestBatchParams,
    provider: &P,
    executor: &E,
) where
    P: StateProviderFactory + BlockReaderIdExt,
    E: BlockExecutorProvider,
{
    // deconstruct optional parameters for header
    let OptionalTestBatchParams {
        beneficiary_opt,
        withdrawals_opt,
        timestamp_opt,
        mix_hash_opt,
        base_fee_per_gas_opt,
    } = optional_params;

    // create "empty" header with default values
    let mut header = Header {
        parent_hash: parent.hash(),
        ommers_hash: EMPTY_OMMER_ROOT_HASH,
        beneficiary: beneficiary_opt.unwrap_or_else(|| Address::random()),
        state_root: Default::default(),
        transactions_root: Default::default(),
        receipts_root: Default::default(),
        withdrawals_root: Some(
            withdrawals_opt
                .clone()
                .map_or(EMPTY_WITHDRAWALS, |w| proofs::calculate_withdrawals_root(&w)),
        ),
        logs_bloom: Default::default(),
        difficulty: U256::ZERO,
        number: parent.number + 1,
        gas_limit: ETHEREUM_BLOCK_GAS_LIMIT,
        gas_used: 0,
        timestamp: timestamp_opt.unwrap_or_else(now),
        mix_hash: mix_hash_opt.unwrap_or_else(|| B256::random()),
        nonce: 0,
        base_fee_per_gas: base_fee_per_gas_opt.or(Some(MIN_PROTOCOL_BASE_FEE)),
        blob_gas_used: None,
        excess_blob_gas: None,
        extra_data: Default::default(),
        parent_beacon_block_root: None,
        requests_root: None,
    };

    // decode batch transactions
    let mut txs = vec![];
    for tx in batch.transactions_owned() {
        let tx_signed =
            TransactionSigned::decode_enveloped(&mut tx.as_ref()).expect("decode tx signed");
        txs.push(tx_signed);
    }

    // update header's transactions root
    header.transactions_root = if batch.transactions().is_empty() {
        EMPTY_TRANSACTIONS
    } else {
        proofs::calculate_transaction_root(&txs)
    };

    // recover senders from block
    let block = Block {
        header,
        body: txs,
        ommers: vec![],
        withdrawals: withdrawals_opt.clone(),
        requests: None,
    }
    .with_recovered_senders()
    .expect("unable to recover senders while executing test batch");

    // create execution db
    let mut db = StateProviderDatabase::new(
        provider.latest().expect("provider retrieves latest during test batch execution"),
    );

    // convenience
    let block_number = block.number;

    // execute the block
    let BlockExecutionOutput { state, receipts, gas_used, .. } = executor
        .executor(&mut db)
        .execute((&block, U256::ZERO).into())
        .expect("executor can execute test batch transactions");
    let bundle_state = ExecutionOutcome::new(state, receipts.into(), block_number, vec![]);

    // retrieve header to update values post-execution
    let Block { mut header, .. } = block.block;

    // update header
    header.gas_used = gas_used;
    header.state_root = db
        .state_root(bundle_state.state())
        .expect("state root calculation during test batch execution");
    header.receipts_root = bundle_state
        .receipts_root_slow(block_number)
        .expect("receipts root calculation during test batch execution");
    header.logs_bloom = bundle_state
        .block_logs_bloom(block_number)
        .expect("logs bloom calculation during test batch execution");

    // seal header and update batch's metadata
    let sealed_header = header.seal_slow();
    let md = batch.versioned_metadata_mut();
    md.update_header(sealed_header);
}

/// Transaction factory
pub struct TransactionFactory {
    /// Keypair for signing transactions
    keypair: ExecutionKeypair,
    /// The nonce for the next transaction constructed.
    nonce: u64,
}

impl Default for TransactionFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionFactory {
    /// Create a new instance of self from a [0; 32] seed.
    ///
    /// Address: 0xb14d3c4f5fbfbcfb98af2d330000d49c95b93aa7
    /// Secret: 9bf49a6a0755f953811fce125f2683d50429c3bb49e074147e0089a52eae155f
    pub fn new() -> Self {
        let mut rng = StdRng::from_seed([0; 32]);
        let secp = Secp256k1::new();
        let (secret_key, _public_key) = secp.generate_keypair(&mut rng);
        let keypair = ExecutionKeypair::from_secret_key(&secp, &secret_key);
        Self { keypair, nonce: 0 }
    }

    /// create a new instance of self from a provided seed.
    pub fn new_random_from_seed<R: Rng + ?Sized>(rand: &mut R) -> Self {
        let secp = Secp256k1::new();
        let (secret_key, _public_key) = secp.generate_keypair(rand);
        let keypair = ExecutionKeypair::from_secret_key(&secp, &secret_key);
        Self { keypair, nonce: 0 }
    }

    /// create a new instance of self from a random seed.
    pub fn new_random() -> Self {
        let secp = Secp256k1::new();
        let (secret_key, _public_key) = secp.generate_keypair(&mut rand::thread_rng());
        let keypair = ExecutionKeypair::from_secret_key(&secp, &secret_key);
        Self { keypair, nonce: 0 }
    }

    /// Return the address of the signer.
    pub fn address(&self) -> Address {
        let public_key = self.keypair.public_key();
        public_key_to_address(public_key)
    }

    /// Change the nonce for the next transaction.
    pub fn set_nonce(&mut self, nonce: u64) {
        self.nonce = nonce;
    }

    /// Increment nonce after a transaction was created and signed.
    fn inc_nonce(&mut self) {
        self.nonce += 1;
    }

    /// Create and sign an EIP1559 transaction.
    pub fn create_eip1559(
        &mut self,
        chain: Arc<ChainSpec>,
        gas_price: u128,
        to: Address,
        value: U256,
    ) -> TransactionSigned {
        // Eip1559
        let transaction = Transaction::Eip1559(TxEip1559 {
            chain_id: chain.chain.id(),
            nonce: self.nonce,
            max_priority_fee_per_gas: 0,
            max_fee_per_gas: gas_price,
            gas_limit: 1_000_000,
            to: TxKind::Call(to),
            value,
            input: Default::default(),
            access_list: Default::default(),
        });

        let tx_signature_hash = transaction.signature_hash();
        let signature = self.sign_hash(tx_signature_hash);

        // increase nonce for next tx
        self.inc_nonce();

        TransactionSigned::from_transaction_and_signature(transaction, signature)
    }

    /// Sign the transaction hash with the key in memory
    fn sign_hash(&self, hash: B256) -> Signature {
        // let env = std::env::var("WALLET_SECRET_KEY")
        //     .expect("Wallet address is set through environment variable");
        // let secret: B256 = env.parse().expect("WALLET_SECRET_KEY must start with 0x");
        // let secret = B256::from_slice(self.keypair.secret.as_ref());
        let secret = B256::from_slice(&self.keypair.secret_bytes());
        let signature = sign_message(secret, hash);
        signature.expect("failed to sign transaction")
    }

    /// Helper to instantiate an `alloy::PrivateKeySigner` wrapping the default account
    pub fn get_default_signer() -> eyre::Result<PrivateKeySigner> {
        let mut rng = StdRng::from_seed([0; 32]);
        let (private_key, _) = Secp256k1::new().generate_keypair(&mut rng);
        // circumvent Secp256k1 <> k256 type incompatibility via FieldBytes intermediary
        let binding = private_key.secret_bytes();
        let secret_bytes_array = FieldBytes::from_slice(&binding);
        let signer = PrivateKeySigner::from_field_bytes(secret_bytes_array).expect("Error constructing signer from private key");

        Ok(signer)
    }

    /// Create and submit the next transaction to the provided [TransactionPool].
    pub async fn create_and_submit_eip1559_pool_tx<Pool>(
        &mut self,
        chain: Arc<ChainSpec>,
        gas_price: u128,
        to: Address,
        value: U256,
        pool: Pool,
    ) -> TxHash
    where
        Pool: TransactionPool,
    {
        let tx = self.create_eip1559(chain, gas_price, to, value);
        let pooled_tx =
            PooledTransactionsElement::try_from_broadcast(tx).expect("tx valid for pool");
        let recovered = pooled_tx.try_into_ecrecovered().expect("tx is recovered");
        let transaction = <Pool::Transaction>::from_recovered_pooled_transaction(recovered);

        pool.add_transaction(TransactionOrigin::Local, transaction)
            .await
            .expect("recovered tx added to pool")
    }

    /// Submit a transaction to the provided pool.
    pub async fn submit_tx_to_pool<Pool>(&self, tx: TransactionSigned, pool: Pool) -> TxHash
    where
        Pool: TransactionPool,
    {
        let pooled_tx =
            PooledTransactionsElement::try_from_broadcast(tx).expect("tx valid for pool");
        let recovered = pooled_tx.try_into_ecrecovered().expect("tx is recovered");
        let transaction = <Pool::Transaction>::from_recovered_pooled_transaction(recovered);

        println!("transaction: \n{transaction:?}\n");

        pool.add_transaction(TransactionOrigin::Local, transaction)
            .await
            .expect("recovered tx added to pool")
    }
}

/// Helper to get the gas price based on the provider's latest header.
pub fn get_gas_price<Provider>(provider: &Provider) -> u128
where
    Provider: BlockReaderIdExt,
{
    let header = provider
        .latest_header()
        .expect("latest header from provider for gas price")
        .expect("latest header is some for gas price");
    header.next_block_base_fee(BaseFeeParams::ethereum()).unwrap_or_default().into()
}

/// Helper to deploy implementation contract for an eXYZ
pub async fn deploy_contract_stablecoin(rpc_url: &str, opt_wallet: Option<&EthereumWallet>) -> eyre::Result<Address> {
    // stablecoin abi
    sol!(
        #[sol(rpc)]
        Stablecoin,
        "src/test_utils/artifacts/Stablecoin.json"
    );

    let wallet = match opt_wallet {
        Some(wallet) => wallet.clone(),
        None => {
            let signer: PrivateKeySigner = TransactionFactory::get_default_signer().unwrap().into();
            EthereumWallet::from(signer)
        }   
    };
    let provider = ProviderBuilder::new().with_recommended_fillers().wallet(wallet).on_http(rpc_url.parse()?);
    let stablecoin_contract = Stablecoin::deploy(&provider).await?;
    println!("Stablecoin implementation deployed");

    Ok(*stablecoin_contract.address())
} 

/// Helper to deploy implementation contract for the canonical Telcoin faucet
/// Since Alloy doesn't yet offer utilities for the `CREATE2`, we rebuild and submit deploy and upgrade transactions to derive the expected faucet proxy address
pub async fn deploy_contract_faucet_initialize(rpc_url: &str, opt_wallet: Option<&EthereumWallet>) -> eyre::Result<Address> {
    // faucet abi
    sol!(
        #[sol(rpc)]
        StablecoinManager,
        "src/test_utils/artifacts/StablecoinManager.json"
    );

    let wallet = match opt_wallet {
        Some(wallet) => wallet.clone(),
        None => {
            let signer: PrivateKeySigner = TransactionFactory::get_default_signer().unwrap().into();
            EthereumWallet::from(signer)
        }
    };
    let provider = ProviderBuilder::new().with_recommended_fillers().wallet(wallet.clone()).on_http(rpc_url.parse()?);
    
    // deploy Arachnid Deterministic Deployment proxy
    let arachnid_presigned_raw_tx = artifacts::transaction_artifacts::ARACHNID_DETERMINISTIC_FACTORY_CREATE_TX;
    let pending_arachnid_creation = provider.send_raw_transaction(arachnid_presigned_raw_tx).await?;
    println!("Pending arachnid factory deployment... {}", pending_arachnid_creation.tx_hash());
    let arachnid_creation_receipt = pending_arachnid_creation.get_receipt().await?;
    println!("Arachnid factory deployed to: {}", arachnid_creation_receipt.contract_address.expect("Arachnid create2 factory deployment failed"));

    // submit create2 transaction to Arachnid Deterministic Deployment proxy
    let arachnid_address = Address::from(hex!("4e59b44847b379578588920ca78fbf26c0b4956c"));
    let deterministic_deploy_data = artifacts::transaction_artifacts::DETERMINISTIC_FAUCET_DEPLOY_DATA;
    let create2_faucet_impl_tx = provider.transaction_request()
        .with_to(arachnid_address)
        .with_input(deterministic_deploy_data);

    let pending_deterministic_impl_tx: alloy::providers::PendingTransactionBuilder<alloy::transports::http::Http<alloy::transports::http::Client>, alloy::network::Ethereum> = provider.send_transaction(create2_faucet_impl_tx).await?;
    println!("Pending deterministic impl deployment... {}", pending_deterministic_impl_tx.tx_hash());
    let initial_impl_tx_receipt = pending_deterministic_impl_tx.get_receipt().await?;

    let initial_faucet_implementation = initial_impl_tx_receipt.contract_address.expect("Initial faucet implementation deployment failed");
    println!("Initial deterministic faucet implementation deployed to: {}", initial_faucet_implementation);
    
    // keccak256("initialize((address,address,address[],uint256,uint256,address[],uint256,uint256))") = 0x16ada6b1
    let init_selector: [u8; 4] = [22, 173, 166, 177];
    let admin = hex!("c1612C97537c2CC62a11FC4516367AB6F62d4B23");
    let stables = [hex!("c8156af812714b8cedb540adec69fc104d99930b")];
    let max_limit = U256::MAX;
    let min_limit = U256::from(1000);
    let authorized_faucets = [
        hex!("E626Ce81714CB7777b1Bf8aD2323963fb3398ad5"),
        hex!("B3FabBd1d2EdDE4D9Ced3CE352859CE1bebf7907"),
        hex!("A3478861957661b2D8974D9309646A71271D98b9"),
        hex!("E69151677E5aeC0B4fC0a94BFcAf20F6f0f975eB")
    ];
    let drip_amount = U256::from(100_000_000); // 100 $XYZ == 100e6
    let native_drip_amount = U256::from(1_000_000_000_000_000_000u128); // 1 $TEL
    let params = (admin, admin, stables, max_limit, min_limit, authorized_faucets, drip_amount, native_drip_amount).abi_encode_params();
    let init_bytes = [&init_selector, &params[..]].concat().into();

    // deploy proxy with initcall
    let faucet_contract = deploy_contract_proxy(rpc_url, initial_faucet_implementation, init_bytes, Some(&wallet)).await?;

    // deploy new implementation and upgrade
    let new_faucet_implementation = StablecoinManager::deploy(&provider).await?;
    // keccak256("upgradeToAndCall(address,bytes)") = 0x4f1ef286
    let upgrade_selector = [79, 30, 242, 134];
    let upgrade_params = (*new_faucet_implementation.address(), Bytes::new()).abi_encode_params();
    let upgrade_data: Bytes = [&upgrade_selector, &upgrade_params[..]].concat().into();
    let tx = provider.transaction_request()
        .with_to(faucet_contract)
        .with_input(upgrade_data);

    let pending_new_impl_tx = provider.send_transaction(tx).await?;
    println!("Pending faucet upgrade transaction... {}", pending_new_impl_tx.tx_hash());
    let upgrade_tx_receipt = pending_new_impl_tx.get_receipt().await?;
    println!("Faucet contract successfully upgraded in block: {}", upgrade_tx_receipt.block_number.expect("Faucet upgrade transaction failed"));

    Ok(faucet_contract)
}

/// Helper function to deploy an ERC1967 proxy contract
pub async fn deploy_contract_proxy(rpc_url: &str, implementation: Address, init_bytes: Bytes, opt_wallet: Option<&EthereumWallet>) -> eyre::Result<Address> {
    // ERC1967Proxy abi
    sol!(
        #[sol(rpc)]
        ERC1967Proxy,
        "src/test_utils/artifacts/ERC1967Proxy.json"
    );

    let wallet = match opt_wallet {
        Some(wallet) => wallet.clone(),
        None => {
            let signer: PrivateKeySigner = TransactionFactory::get_default_signer().unwrap().into();
            EthereumWallet::from(signer)
        }
    };
    let provider = ProviderBuilder::new().with_recommended_fillers().wallet(wallet).on_http(rpc_url.parse()?);
    let proxy_contract = ERC1967Proxy::deploy(provider, implementation, init_bytes).await?; 
    
    Ok(*proxy_contract.address())
}

#[cfg(test)]
mod tests {
    use reth_primitives::hex;
    // use std::str::FromStr;

    use super::*;
    #[test]
    fn test_print_key_info() {
        // let mut rng = StdRng::from_seed([0; 32]);
        // let keypair = ExecutionKeypair::generate(&mut rng);

        let secp = Secp256k1::new();
        let (secret_key, _public_key) = secp.generate_keypair(&mut rand::thread_rng());
        let keypair = ExecutionKeypair::from_secret_key(&secp, &secret_key);

        // let private = base64::encode(keypair.secret.as_bytes());
        let secret = keypair.secret_bytes();
        println!("secret: {:?}", hex::encode(secret));
        let pubkey = keypair.public_key().serialize();
        println!("public: {:?}", hex::encode(pubkey));

        // 9bf49a6a0755f953811fce125f2683d50429c3bb49e074147e0089a52eae155f
        // println!("{:?}", hex::encode(bytes));
        // public key hex [0; 32]
        // 029bef8d556d80e43ae7e0becb3a7e6838b95defe45896ed6075bb9035d06c9964
        //
        // let pkey = secp256k1::PublicKey::from_str(
        //     "029bef8d556d80e43ae7e0becb3a7e6838b95defe45896ed6075bb9035d06c9964",
        // )
        // .unwrap();
        // println!("{:?}", public_key_to_address(pkey));
        // println!("pkey: {pkey:?}");
    }
}
