//! Iterator support shims
//!
//! Re-exports the lock-free memtable skiplist under a stable internal path for
//! benches. This module is not part of the canonical public API; it is only
//! reachable through `crate::__internal` under the `internal-testing` feature.

pub mod skiplist {
    pub use crate::memtable::skiplist::*;
}

pub use skiplist::SkipList;
