#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs, unreachable_pub)]
#![deny(unused_must_use, rust_2018_idioms)]
#![doc(test(
    no_crate_inject,
    attr(deny(warnings, rust_2018_idioms), allow(dead_code, unused_variables))
))]

//! lattice payload job generator
//! 
//! The module handles creating new batches for workers to propose
//! and building the next canonical block from consensus output.

mod metrics;
pub mod batch;
pub mod block;
