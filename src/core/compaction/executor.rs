//! Backward compatibility re-exports for compaction execution.
//!
//! This module has been refactored into `execution/` with focused submodules.
//! These re-exports maintain backward compatibility for existing code.

#[deprecated(
    since = "0.1.0",
    note = "Import from `compaction::execution` instead of `compaction::executor`"
)]
pub use super::execution::*;
