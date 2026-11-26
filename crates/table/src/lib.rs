//! Memtable and SST table primitives used by the Midge engine.
//!
//! This crate owns the in-memory table, sorted-string-table format, readers,
//! bloom filters, trie index, and block cache. Shared filesystem, codec,
//! error, and value types are provided by `cntryl-midge-core`.

#![cfg_attr(not(test), deny(clippy::unwrap_used))]

pub use cntryl_midge_core::common::{MidgeError, MidgeResult};

#[doc(hidden)]
pub mod memtable;
#[doc(hidden)]
pub mod sst;

pub use memtable::SkipListMemtable;
pub use sst::{FsSstFactoryIo, SstFactory, SstStateReader};
