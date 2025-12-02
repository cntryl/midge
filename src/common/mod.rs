//! Common building blocks with no external dependencies.
//!
//! This module contains foundational types and utilities used throughout
//! the codebase that have no dependencies on other midge modules.

pub mod codec;
pub mod error;
pub mod internal_key;
pub mod range_tombstone;
pub mod rate_limiter;
pub mod test_hooks;
pub mod timestamp;
pub mod tlv;
pub mod worker;

#[cfg(test)]
pub mod test_cleanup;

// Re-export commonly used error types for convenience
pub use error::{MidgeError, MidgeResult};
