//! Standalone crate for Telcoin Netwokr configuration types.

#![doc(
    html_logo_url = "https://www.telco.in/logos/TEL.svg",
    html_favicon_url = "https://www.telco.in/logos/TEL.svg",
    issue_tracker_base_url = "https://github.com/telcoin-association/telcoin-network/issues/"
)]
#![warn(missing_debug_implementations, missing_docs, unreachable_pub, rustdoc::all)]
#![deny(unused_must_use, rust_2018_idioms)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

pub use tn_types::{
    read_validator_keypair_from_file, AnemoParameters, Config, ConfigTrait,
    NetworkAdminServerParameters, Parameters, PrometheusMetricsParameters, BLS_KEYFILE,
    PRIMARY_NETWORK_KEYFILE, WORKER_NETWORK_KEYFILE,
};
