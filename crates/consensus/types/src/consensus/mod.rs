// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Types for the output of consensus.
#![allow(clippy::mutable_key_type)]

mod reputation;
pub use reputation::*;
mod output;
pub use output::*;

/// A global sequence number assigned to every CommittedSubDag.
pub type SequenceNumber = u64;
