#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_debug_implementations, missing_docs, unreachable_pub)]
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
pub mod execution;
pub mod consensus;

// pub use consensus::{
//     CollectionErrorType,
//     RetrievalResult,
//     ConfigurationClient,
//     Configuration, ConfigurationServer,
//     PrimaryToPrimaryClient,
//     MockPrimaryToPrimary, PrimaryToPrimary, PrimaryToPrimaryServer,
//     PrimaryToWorkerClient,
//     MockPrimaryToWorker, PrimaryToWorker, PrimaryToWorkerServer,
//     ProposerClient,
//     Proposer, ProposerServer,
//     TransactionsClient,
//     Transactions, TransactionsServer,
//     ValidatorClient,
//     Validator, ValidatorServer,
//     WorkerToPrimaryClient,
//     MockWorkerToPrimary, WorkerToPrimary, WorkerToPrimaryServer,
//     WorkerToWorkerClient,
//     MockWorkerToWorker, WorkerToWorker, WorkerToWorkerServer,
//     CertificateDigestProto, Collection, CollectionError,
//     CollectionRetrievalResult, Empty, GetCollectionsRequest, GetCollectionsResponse,
//     GetPrimaryAddressResponse, MultiAddrProto, NewEpochRequest, NewNetworkInfoRequest,
//     NodeReadCausalRequest, NodeReadCausalResponse, PublicKeyProto, ReadCausalRequest,
//     ReadCausalResponse, RemoveCollectionsRequest, RoundsRequest, RoundsResponse,
//     TransactionProto, ValidatorData,
// };
