// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// Error types
#[macro_use]
pub mod error;

mod consensus;
pub use consensus::*;

mod primary;
pub use primary::*;

mod metrics;
pub use metrics::*;

mod serde;

mod pre_subscribed_broadcast;
pub use pre_subscribed_broadcast::*;

mod crypto;
pub use crypto::*;
mod config;
pub use config::*;
mod multiaddr;
pub use multiaddr::*;
mod genesis;
pub use genesis::*;
mod worker;
use ::serde::{Deserialize, Serialize};
pub use worker::*;

pub use reth_primitives::{BlockHash, TransactionSigned};

/// Collection of database test utilities
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

/// Decode bytes to a Deserializable type.
/// This version will panic if bytes can not deserialize.
pub fn decode<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> T {
    /*bincode::DefaultOptions::new()
    .with_big_endian()
    .with_fixint_encoding()
    .deserialize(bytes)
    .expect("Invalid bytes!")*/
    bcs::from_bytes(bytes).expect("Invalid bytes!")
}

/// Decode bytes to a Deserializable type.
pub fn try_decode<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> eyre::Result<T> {
    Ok(bcs::from_bytes(bytes)?)
    /*Ok(bincode::DefaultOptions::new()
    .with_big_endian()
    .with_fixint_encoding()
    .deserialize(bytes)?)*/
}

/// Encode a Serializable object to a byte vector.
/// Will panic if the type fails to serialize (this should not happen).
pub fn encode<T: Serialize>(obj: &T) -> Vec<u8> {
    /*bincode::DefaultOptions::new()
    .with_big_endian()
    .with_fixint_encoding()
    .serialize(obj)
    .expect("Can not serialize!")*/
    bcs::to_bytes(obj).unwrap_or_else(|_| panic!("Serialization should not fail"))
}
