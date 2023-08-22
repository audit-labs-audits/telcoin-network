use execution_transaction_pool::TransactionPool;
use tn_types::execution::{
    hex, Address, Bytes, Signature, Transaction as Tx, TransactionKind, TransactionSigned,
    TxEip1559, U256, FromRecoveredTransaction,
};
use execution_rlp::Decodable;
use std::str::FromStr;

#[allow(unused)]
pub fn recover_tx_signer(tx: &Bytes) -> Address {
    let mut data = tx.as_ref();
    let transaction = TransactionSigned::decode(&mut data)
        .unwrap()
        .into_ecrecovered()
        .unwrap();

    let signer = transaction.signer();
    // debug!("transaction signer's address: {:#?}", signer);
    signer
}

pub fn tx_signed_from_raw(tx: &str) -> TransactionSigned {
    let raw_tx = Bytes::from_str(tx).unwrap();
    let mut data = raw_tx.as_ref();

    let transaction = TransactionSigned::decode(&mut data).unwrap();
    // debug!("transaction: {:#?}", transaction);
    transaction
}

pub fn pool_transaction_from_raw<Pool: TransactionPool + Clone + 'static>(_pool: &Pool, tx: &str) -> Pool::Transaction {
    let recovered = tx_signed_from_raw(tx).into_ecrecovered().unwrap();
    <Pool::Transaction>::from_recovered_transaction(recovered)
}

// bob -> alice
// bob -> charlie
// bob -> darlene

// for each transaction: 
// - to
// - from
// - raw
// - signed

// alice:   0x86FC4954D645258e68E71de59A41066C55bd9966
// bob:     0x6Be02d1d3665660d22FF9624b7BE0551ee1Ac91b 
// charlie: 0x70c22051eecA7Bd6E29cd6D24DD38fB5824aEdc4
// darlene: 0x26faf4f987baf26631719a53d3a42a666a9289cb

// bob tx1 -> alice
// send 123456
// !!!
// Bytes(0x02f876820a2880847735940084773594008252089486fc4954d645258e68e71de59a41066c55bd99668a1a24902bee142100000080c001a099c0a92cc8dc90c9af87059ade1c2e5d8976ee04b6f620f0b1c8c0153fe465e2a0467f17b0589dc6021ec13a96d932ddf9cdb7396a13785df3273de4d5a00b323d)!!!
// !!!recovered
// TransactionSignedEcRecovered { signer: 0x6be02d1d3665660d22ff9624b7be0551ee1ac91b, signed_transaction: TransactionSigned { hash: 0xe30a29ec131ac12c152e198ee5ba6e25a4059f7e6fe7415f9f4915e1aa6f8c27, signature: Signature { r: 0x99c0a92cc8dc90c9af87059ade1c2e5d8976ee04b6f620f0b1c8c0153fe465e2_U256, s: 0x467f17b0589dc6021ec13a96d932ddf9cdb7396a13785df3273de4d5a00b323d_U256, odd_y_parity: true }, transaction: Eip1559(TxEip1559 { chain_id: 2600, nonce: 0, gas_limit: 21000, max_fee_per_gas: 2000000000, max_priority_fee_per_gas: 2000000000, to: Call(0x86fc4954d645258e68e71de59a41066c55bd9966), value: 123456000000000000000000, access_list: AccessList([]), input: Bytes(0x) }) } }!!!

// bob tx2 -> charlie
// send 3333
// !!!
// Bytes(0x02f875820a2801847735940084773594008252089470c22051eeca7bd6e29cd6d24dd38fb5824aedc489b4aeaab10258f4000080c001a035ef4e4e58c53dc19c73ce482b72a9c373f88ea36151b349b4098eadecd5a37ba048be1306dfbf95f2902eb3f8b6c8252be351536db6a48ca9fefba7d0d9fcc114)!!!
// !!!recovered
// TransactionSignedEcRecovered { signer: 0x6be02d1d3665660d22ff9624b7be0551ee1ac91b, signed_transaction: TransactionSigned { hash: 0x7098010f53192fc5c857294cb64cab00b3ff60bd554ecb838e20e4266086c4a7, signature: Signature { r: 0x35ef4e4e58c53dc19c73ce482b72a9c373f88ea36151b349b4098eadecd5a37b_U256, s: 0x48be1306dfbf95f2902eb3f8b6c8252be351536db6a48ca9fefba7d0d9fcc114_U256, odd_y_parity: true }, transaction: Eip1559(TxEip1559 { chain_id: 2600, nonce: 1, gas_limit: 21000, max_fee_per_gas: 2000000000, max_priority_fee_per_gas: 2000000000, to: Call(0x70c22051eeca7bd6e29cd6d24dd38fb5824aedc4), value: 3333000000000000000000, access_list: AccessList([]), input: Bytes(0x) }) } }!!!

// bob tx3 -> darlene
// send 10000
// !!!
// Bytes(0x02f876820a2802847735940084773594008252089426faf4f987baf26631719a53d3a42a666a9289cb8a021e19e0c9bab240000080c001a0c5faa9d6da4feb494ca84773394f1c982888915baa43d9ce3f5a7bd53fbeb9bda05cecc85080f09e4cd91f0f75e3c2a8460f26175403f0804c8cb1f238fb95bd11)!!!
// !!!recovered
// TransactionSignedEcRecovered { signer: 0x6be02d1d3665660d22ff9624b7be0551ee1ac91b, signed_transaction: TransactionSigned { hash: 0xc2a0145abd20fe28d95cc6a9d6a0fa3f624568004f808457d310a729d6542244, signature: Signature { r: 0xc5faa9d6da4feb494ca84773394f1c982888915baa43d9ce3f5a7bd53fbeb9bd_U256, s: 0x5cecc85080f09e4cd91f0f75e3c2a8460f26175403f0804c8cb1f238fb95bd11_U256, odd_y_parity: true }, transaction: Eip1559(TxEip1559 { chain_id: 2600, nonce: 2, gas_limit: 21000, max_fee_per_gas: 2000000000, max_priority_fee_per_gas: 2000000000, to: Call(0x26faf4f987baf26631719a53d3a42a666a9289cb), value: 10000000000000000000000, access_list: AccessList([]), input: Bytes(0x) }) } }!!!



/// Send 123456 TEL to alice
pub fn bob_raw_tx1() -> &'static str {
    "0x02f876820a2880847735940084773594008252089486fc4954d645258e68e71de59a41066c55bd99668a1a24902bee142100000080c001a099c0a92cc8dc90c9af87059ade1c2e5d8976ee04b6f620f0b1c8c0153fe465e2a0467f17b0589dc6021ec13a96d932ddf9cdb7396a13785df3273de4d5a00b323d"
}

/// Send 3333 TEL to charlie
pub fn bob_raw_tx2() -> &'static str {
    "0x02f875820a2801847735940084773594008252089470c22051eeca7bd6e29cd6d24dd38fb5824aedc489b4aeaab10258f4000080c001a035ef4e4e58c53dc19c73ce482b72a9c373f88ea36151b349b4098eadecd5a37ba048be1306dfbf95f2902eb3f8b6c8252be351536db6a48ca9fefba7d0d9fcc114"
}

/// Send 10000 TEL to darlene
pub fn bob_raw_tx3() -> &'static str {
    "0x02f876820a2802847735940084773594008252089426faf4f987baf26631719a53d3a42a666a9289cb8a021e19e0c9bab240000080c001a0c5faa9d6da4feb494ca84773394f1c982888915baa43d9ce3f5a7bd53fbeb9bda05cecc85080f09e4cd91f0f75e3c2a8460f26175403f0804c8cb1f238fb95bd11"
}

/// From bob
pub fn _broken_bob_tx1() -> TransactionSigned {
    let transaction = Tx::Eip1559(TxEip1559 {
        chain_id: 2600,
        nonce: 0,
        max_priority_fee_per_gas: 1500000000,
        max_fee_per_gas: 1500000013,
        gas_limit: 21000,
        to: TransactionKind::Call(Address::from_slice(
            &hex::decode("61815774383099e24810ab832a5b2a5425c154d5").unwrap()[..],
        )),
        value: 3000000000000000000u64.into(),
        input: Default::default(),
        access_list: Default::default(),
    });
    let signature = Signature {
        odd_y_parity: true,
        r: U256::from_str("0x59e6b67f48fb32e7e570dfb11e042b5ad2e55e3ce3ce9cd989c7e06e07feeafd")
            .unwrap(),
        s: U256::from_str("0x016b83f4f980694ed2eee4d10667242b1f40dc406901b34125b008d334d47469")
            .unwrap(),
    };
    
    TransactionSigned::from_transaction_and_signature(transaction, signature)
}
