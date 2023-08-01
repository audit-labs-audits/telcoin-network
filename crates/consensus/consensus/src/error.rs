// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use lattice_typed_store::StoreError;
use thiserror::Error;
use tn_types::consensus::{Certificate, AuthorityIdentifier, Round, dag::node_dag::NodeDagError};

#[derive(Clone, Debug, Error, PartialEq)]
pub enum ConsensusError {
    #[error("Storage failure: {0}")]
    StoreError(#[from] StoreError),

    #[error("Certificate {0:?} equivocates with earlier certificate {1:?}")]
    CertificateEquivocation(Certificate, Certificate),

    #[error("System shutting down")]
    ShuttingDown,
}

/// Represents the errors that can be encountered in this concrete,
/// [`fastcrypto::traits::VerifyingKey`], [`Certificate`] and [`Round`]-aware variant of the Dag.
#[derive(Debug, Error)]
pub enum ValidatorDagError {
    #[error("No remaining certificates in Dag for this authority: {0}")]
    OutOfCertificates(AuthorityIdentifier),

    #[error("No known certificates for this authority: {0} at round {1}")]
    NoCertificateForCoordinates(AuthorityIdentifier, Round),

    // an invariant violation at the level of the generic DAG (unrelated to Certificate specifics)
    #[error("Dag invariant violation {0}")]
    DagInvariantViolation(#[from] NodeDagError),
}
