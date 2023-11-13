// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{BatchDigest};




// TODO: support propagating errors from the worker to the primary.
pub type TxResponse = tokio::sync::oneshot::Sender<BatchDigest>;
