//! Result types for advanced operations
//!
//! Result enums for operations like Compare-and-Swap and Insert that
//! need to communicate both success and what was present before.

use bytes::Bytes;

/// Result of an Insert operation (which fails if key already exists)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertResult {
    /// Key was successfully inserted
    Ok,
    /// Key already exists with this value
    AlreadyExists(Bytes),
}

impl InsertResult {
    pub fn is_ok(&self) -> bool {
        matches!(self, InsertResult::Ok)
    }
}

/// Result of a Compare-and-Swap operation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasResult {
    /// CAS succeeded - value was swapped
    Swapped,
    /// CAS failed - expected value didn't match; returns actual value
    Mismatch(Option<Bytes>),
}

impl CasResult {
    pub fn is_ok(&self) -> bool {
        matches!(self, CasResult::Swapped)
    }

    pub fn is_mismatch(&self) -> bool {
        matches!(self, CasResult::Mismatch(_))
    }
}
