// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Test utilities for the consensus layer.
pub mod cluster;

mod helpers;
pub use helpers::*;

mod mock_servers;
pub use mock_servers::*;

mod fixtures;
pub use fixtures::*;

pub const VOTES_CF: &str = "votes";
pub const HEADERS_CF: &str = "headers";
pub const CERTIFICATES_CF: &str = "certificates";
pub const CERTIFICATE_DIGEST_BY_ROUND_CF: &str = "certificate_digest_by_round";
pub const CERTIFICATE_DIGEST_BY_ORIGIN_CF: &str = "certificate_digest_by_origin";
pub const PAYLOAD_CF: &str = "payload";
pub(crate) const BATCHES_CF: &str = "batches";
