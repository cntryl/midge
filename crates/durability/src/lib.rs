//! Write-ahead logging, writer fencing, and persisted metadata for Midge.

#![cfg_attr(not(test), deny(clippy::unwrap_used))]

#[doc(hidden)]
pub mod lease;
#[doc(hidden)]
pub mod metadata;
#[doc(hidden)]
pub mod wal;

/// Recovery policy for engine open.
///
/// `Strict` fails engine open if manifest, intent-log, or WAL recovery cannot
/// establish a trustworthy state. `Salvage` preserves legacy best-effort
/// behavior and may open in a degraded state after logging warnings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RecoveryPolicy {
    #[default]
    Strict,
    Salvage,
}
