//! Fixtures for test-utils.
//! 
//! Fixtures are useful for simulating output from a committee
//! then interacting with the data as this node.
//! 
//! For network testing that simulates primary <-> primary
//! communication, use [`Cluster`].

mod committee;
pub use committee::*;

mod authority;
pub use authority::*;

mod worker;
pub use worker::*;
