// Copyright (c) Telcoin, LLC
//! How the primary's network should handle incoming messages.

mod worker;
mod primary;

pub(crate) use worker::WorkerToPrimaryHandler;
pub(crate) use primary::PrimaryToPrimaryHandler;
