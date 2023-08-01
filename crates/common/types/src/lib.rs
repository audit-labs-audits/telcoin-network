#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_debug_implementations, missing_docs, unreachable_pub, future_incompatible, nonstandard_style, rust_2018_idioms, rust_2021_compatibility)]
#![deny(unused_must_use, rust_2018_idioms)]
#![doc(test(
    no_crate_inject,
    attr(deny(warnings, rust_2018_idioms), allow(dead_code, unused_variables))
))]

//! Commonly used types in telcoin-network.
//!
//! This crate contains Ethereum primitive types and helper functions.
//!
//! ## Feature Flags
//!
//! - `arbitrary`: Adds `proptest` and `arbitrary` support for primitive types.
//! - `test-utils`: Export utilities for testing
pub mod consensus;
pub mod execution;
