//! Engine operation modules.
//!
//! This module splits the MidgeEngine operations into focused submodules:
//! - `reads`: Point reads and range scans
//! - `writes`: Put, delete, and write batch operations
//! - `mutations`: Insert, CAS, and merge operations
//! - `transactions`: Transaction coordination
//! - `snapshots`: Snapshot management
//! - `maintenance`: Flush, compaction, and checkpoint operations
//! - `observability`: Metrics and cache access

pub(crate) mod reads;
pub(crate) mod writes;
pub(crate) mod maintenance;
pub(crate) mod mutations;
pub(crate) mod transactions;
pub(crate) mod snapshots;
pub(crate) mod observability;

// No re-exports needed - methods are implemented directly on MidgeEngine
