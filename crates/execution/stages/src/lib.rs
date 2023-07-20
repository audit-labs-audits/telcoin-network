#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_debug_implementations, missing_docs, unreachable_pub)]
#![deny(unused_must_use, rust_2018_idioms)]
#![doc(test(
    no_crate_inject,
    attr(deny(warnings, rust_2018_idioms), allow(dead_code, unused_variables))
))]
#![allow(clippy::result_large_err)]
//! Staged syncing primitives for reth.
//!
//! This crate contains the syncing primitives [`Pipeline`] and [`Stage`], as well as all stages
//! that reth uses to sync.
//!
//! A pipeline can be configured using [`Pipeline::builder()`].
//!
//! For ease of use, this crate also exposes a set of [`StageSet`]s, which are collections of stages
//! that perform specific functions during sync. Stage sets can be customized; it is possible to
//! add, disable and replace stages in the set.
//!
//! # Examples
//!
//! ```
//! # use std::sync::Arc;
//! # use execution_db::test_utils::create_test_rw_db;
//! # use execution_downloaders::bodies::bodies::BodiesDownloaderBuilder;
//! # use execution_downloaders::headers::reverse_headers::ReverseHeadersDownloaderBuilder;
//! # use execution_interfaces::consensus::Consensus;
//! # use execution_interfaces::test_utils::{TestBodiesClient, TestConsensus, TestHeadersClient};
//! # use execution_revm::Factory;
//! # use execution_primitives::{PeerId, MAINNET, H256};
//! # use execution_stages::Pipeline;
//! # use execution_stages::sets::DefaultStages;
//! # use execution_stages::stages::HeaderSyncMode;
//! # use tokio::sync::watch;
//! # let consensus: Arc<dyn Consensus> = Arc::new(TestConsensus::default());
//! # let headers_downloader = ReverseHeadersDownloaderBuilder::default().build(
//! #    Arc::new(TestHeadersClient::default()),
//! #    consensus.clone()
//! # );
//! # let db = create_test_rw_db();
//! # let bodies_downloader = BodiesDownloaderBuilder::default().build(
//! #    Arc::new(TestBodiesClient { responder: |_| Ok((PeerId::zero(), vec![]).into()) }),
//! #    consensus.clone(),
//! #    db.clone()
//! # );
//! # let (tip_tx, tip_rx) = watch::channel(H256::default());
//! # let factory = Factory::new(MAINNET.clone());
//! // Create a pipeline that can fully sync
//! # let pipeline =
//! Pipeline::builder()
//!     .with_tip_sender(tip_tx)
//!     .add_stages(
//!         DefaultStages::new(HeaderSyncMode::Tip(tip_rx), consensus, headers_downloader, bodies_downloader, factory)
//!     )
//!     .build(db, MAINNET.clone());
//! ```
//!
//! ## Feature Flags
//!
//! - `test-utils`: Export utilities for testing
mod error;
mod metrics;
mod pipeline;
mod stage;
mod util;

#[allow(missing_docs)]
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

/// A re-export of common structs and traits.
pub mod prelude;

/// Implementations of stages.
pub mod stages;

pub mod sets;

pub use error::*;
pub use metrics::*;
pub use pipeline::*;
pub use stage::*;
