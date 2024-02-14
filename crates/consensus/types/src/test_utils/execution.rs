// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

//! Specific test utils for execution layer
use crate::ExecutionKeypair;
use fastcrypto::traits::{KeyPair as _, ToFromBytes};
use rand::{
    rngs::{OsRng, StdRng},
    SeedableRng,
};
use reth_primitives::{
    public_key_to_address, sign_message, Address, BaseFeeParams, ChainSpec,
    FromRecoveredPooledTransaction, Signature, Transaction, TransactionKind, TransactionSigned,
    TxEip1559, TxHash, TxValue, B256,
};
use reth_provider::BlockReaderIdExt;
use reth_transaction_pool::{TransactionOrigin, TransactionPool};
use std::sync::Arc;

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
    pub fn new() -> Self {
        let mut rng = StdRng::from_seed([0; 32]);
        let keypair = ExecutionKeypair::generate(&mut rng);
        Self { keypair, nonce: 0 }
    }

    /// Create a new instance of self from a random seed.
    pub fn new_random() -> Self {
        let mut rng = StdRng::from_rng(OsRng).expect("OsRng available");
        let keypair = ExecutionKeypair::generate(&mut rng);
        Self { keypair, nonce: 0 }
    }

    /// Create a new instance of [Self] from a provided keypair.
    pub fn new_from_keypair(private: &[u8]) -> Self {
        let keypair = ExecutionKeypair::from_bytes(private).unwrap();
        Self { keypair, nonce: 0 }
    }

    /// Return the address of the signer.
    pub fn address(&self) -> Address {
        let public_key = self.keypair.public.pubkey;
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
        value: TxValue,
    ) -> TransactionSigned {
        // Eip1559
        let transaction = Transaction::Eip1559(TxEip1559 {
            chain_id: chain.chain.id(),
            nonce: self.nonce,
            max_priority_fee_per_gas: 0,
            max_fee_per_gas: gas_price,
            gas_limit: 1_000_000,
            to: TransactionKind::Call(to),
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
        let secret = B256::from_slice(self.keypair.secret.as_ref());
        let signature = sign_message(secret, hash);
        signature.expect("failed to sign transaction")
    }

    /// Create and submit the next transaction to the provided [TransactionPool].
    pub async fn create_and_submit_eip1559_pool_tx<Pool>(
        &mut self,
        chain: Arc<ChainSpec>,
        gas_price: u128,
        to: Address,
        value: TxValue,
        pool: Pool,
    ) -> TxHash
    where
        Pool: TransactionPool,
    {
        let tx = self.create_eip1559(chain, gas_price, to, value);
        let recovered = tx.try_into_ecrecovered().expect("tx is recovered");
        let transaction = <Pool::Transaction>::from_recovered_pooled_transaction(recovered.into());
        
        pool
            .add_transaction(TransactionOrigin::Local, transaction)
            .await
            .expect("recovered tx added to pool")
    }

    /// Submit a transaction to the provided pool.
    pub async fn submit_tx_to_pool<Pool>(&self, tx: TransactionSigned, pool: Pool) -> TxHash
    where
        Pool: TransactionPool,
    {
        let recovered = tx.try_into_ecrecovered().expect("tx is recovered");
        let transaction = <Pool::Transaction>::from_recovered_pooled_transaction(recovered.into());
        
        pool
            .add_transaction(TransactionOrigin::Local, transaction)
            .await
            .expect("recovered tx added to pool")
    }
}

/// Helper to get the gas price based on the provider's latest header.
pub fn get_gas_price<Provider>(provider: Provider) -> u128
where
    Provider: BlockReaderIdExt,
{
    let header = provider
        .latest_header()
        .expect("latest header from provider for gas price")
        .expect("latest header is some for gas price");
    header.next_block_base_fee(BaseFeeParams::ethereum()).unwrap_or_default().into()
}
