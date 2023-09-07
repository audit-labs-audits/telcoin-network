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

//! Payloads are the information exchanged from the EL to the CL.
//! 
//! The CL requests two types of payloads from the EL:
//! - BatchPayload
//! - HeaderPayload
//! 
//! Both payloads use the same service, handle, and generator to produce
//! separate jobs depending on the payload type.
mod metrics;
mod job;
mod error;
mod generator;
mod handle;
mod helpers;
mod service;
mod traits;
pub use metrics::*;
pub use job::*;
pub use error::*;
pub use generator::*;
pub use handle::*;
pub use service::*;
pub use traits::{BatchPayloadJobGenerator, HeaderPayloadJobGenerator};
