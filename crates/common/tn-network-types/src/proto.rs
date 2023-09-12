// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// The module responsible for generating anemo network utilities.
/// 
/// The `build.rs` file in this crate defines the services and their
/// methods. The output is generated here.
/// 
/// Current understanding:
/// - each service generates a server, client, and trait
///     - changes should be made to this file and build.rs
/// - traits are impl on xxHandlers and passed in to xxServer::new()
///     - so, the traits should be implemented on types that handle
///       the server logic (handle the requests)
///     - see `PrimaryToPrimary` and `PrimaryToPrimaryServer`
/// - clients are structs that actually send the request
/// - server structs are wrappers for thread safety and
///   take an inner type that implements the server's trait
///     - ie) PrimaryToPrimary
/// 
/// Unknowns:
/// - what are inbound request layers? `add_layer_for...`
///     - see crates/consensus/primary/src/primary.rs:L248
/// - is there more to Routes than just keeping wires from crossing?
/// - probably want to update codec to be consistent, but I think 
///   this can wait for post testnet
mod narwhal {
    // // TODO: I think this is only used when external consensus is enabled
    // #![allow(clippy::derive_partial_eq_without_eq)]
    // tonic::include_proto!("narwhal");

    // these are for internal consensus
    include!(concat!(env!("OUT_DIR"), "/narwhal.PrimaryToPrimary.rs"));
    include!(concat!(env!("OUT_DIR"), "/narwhal.PrimaryToWorker.rs"));
    include!(concat!(env!("OUT_DIR"), "/narwhal.PrimaryToEngine.rs"));
    include!(concat!(env!("OUT_DIR"), "/narwhal.WorkerToPrimary.rs"));
    include!(concat!(env!("OUT_DIR"), "/narwhal.WorkerToWorker.rs"));
    include!(concat!(env!("OUT_DIR"), "/narwhal.EngineToWorker.rs"));
    include!(concat!(env!("OUT_DIR"), "/narwhal.WorkerToEngine.rs"));
}

// use std::{array::TryFromSliceError, ops::Deref};

// use tn_types::consensus::{
//     crypto::AuthorityPublicKey, BlockError, BlockErrorKind, CertificateDigest, Transaction,
// };
// use bytes::Bytes;

pub use narwhal::{
    // collection_error::CollectionErrorType,
    // collection_retrieval_result::RetrievalResult,
    // configuration_client::ConfigurationClient, // TODO: remove this and Proposer - only used by external consensus
    // configuration_server::{Configuration, ConfigurationServer},
    primary_to_primary_client::PrimaryToPrimaryClient,
    primary_to_primary_server::{MockPrimaryToPrimary, PrimaryToPrimary, PrimaryToPrimaryServer},
    primary_to_worker_client::PrimaryToWorkerClient,
    primary_to_engine_client::PrimaryToEngineClient,
    engine_to_worker_client::EngineToWorkerClient,
    engine_to_worker_server::{EngineToWorker, MockEngineToWorker, EngineToWorkerServer},
    primary_to_engine_server::{PrimaryToEngine, MockPrimaryToEngine, PrimaryToEngineServer},
    primary_to_worker_server::{MockPrimaryToWorker, PrimaryToWorker, PrimaryToWorkerServer},
    // proposer_client::ProposerClient, // TODO: remove this - external consensus
    // proposer_server::{Proposer, ProposerServer},
    // transactions_client::TransactionsClient,
    // transactions_server::{Transactions, TransactionsServer},
    // validator_client::ValidatorClient,
    // validator_server::{Validator, ValidatorServer},
    worker_to_primary_client::WorkerToPrimaryClient,
    worker_to_primary_server::{MockWorkerToPrimary, WorkerToPrimary, WorkerToPrimaryServer},
    worker_to_worker_client::WorkerToWorkerClient,
    worker_to_worker_server::{MockWorkerToWorker, WorkerToWorker, WorkerToWorkerServer},
    worker_to_engine_client::WorkerToEngineClient,
    worker_to_engine_server::{MockWorkerToEngine, WorkerToEngine, WorkerToEngineServer},
    // CertificateDigest as CertificateDigestProto, Collection, CollectionError,
    // CollectionRetrievalResult, Empty, GetCollectionsRequest, GetCollectionsResponse,
    // GetPrimaryAddressResponse, MultiAddr as MultiAddrProto, NewEpochRequest, NewNetworkInfoRequest,
    // NodeReadCausalRequest, NodeReadCausalResponse, PublicKey as PublicKeyProto, ReadCausalRequest,
    // ReadCausalResponse, RemoveCollectionsRequest, RoundsRequest, RoundsResponse,
    // Transaction as TransactionProto, ValidatorData,
};

// impl From<AuthorityPublicKey> for PublicKeyProto {
//     fn from(pub_key: AuthorityPublicKey) -> Self {
//         PublicKeyProto { bytes: Bytes::from(pub_key.as_ref().to_vec()) }
//     }
// }

// impl From<Transaction> for TransactionProto {
//     fn from(transaction: Transaction) -> Self {
//         TransactionProto { transaction: Bytes::from(transaction) }
//     }
// }

// impl From<TransactionProto> for Transaction {
//     fn from(transaction: TransactionProto) -> Self {
//         transaction.transaction.to_vec()
//     }
// }

// impl From<BlockError> for CollectionError {
//     fn from(error: BlockError) -> Self {
//         CollectionError {
//             id: Some(error.digest.into()),
//             error: CollectionErrorType::from(error.error).into(),
//         }
//     }
// }

// impl From<BlockErrorKind> for CollectionErrorType {
//     fn from(error_type: BlockErrorKind) -> Self {
//         match error_type {
//             BlockErrorKind::BlockNotFound => CollectionErrorType::CollectionNotFound,
//             BlockErrorKind::BatchTimeout => CollectionErrorType::CollectionTimeout,
//             BlockErrorKind::BatchError => CollectionErrorType::CollectionError,
//         }
//     }
// }

// impl TryFrom<CertificateDigestProto> for CertificateDigest {
//     type Error = TryFromSliceError;

//     fn try_from(digest: CertificateDigestProto) -> Result<Self, Self::Error> {
//         Ok(CertificateDigest::new(digest.digest.deref().try_into()?))
//     }
// }
