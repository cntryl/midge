//! Write-Ahead Log (WAL) subsystem
//!
//! Provides durable write-ahead logging with filesystem implementations.

pub mod encoding;
pub mod fs;
pub mod policy;
pub mod recovery;
pub mod traits;
pub mod types;

// Re-export main WAL types
pub use types::{ColumnFamilyId, WalOpKind, WalPos, WalRecord, WalRecoveryStats, WalSyncMode};

// Re-export traits
pub use traits::{WalFactory, WalReader, WalReaderDyn, WalWriter};

// Re-export encoding functions
pub use encoding::{decode, encode};

// Re-export filesystem implementations
pub use fs::{FsWalFactory, FsWalReader, FsWalWriter};

// Re-export recovery
pub use recovery::{replay_wal, RecoveryStats};

// Re-export policy types
pub use policy::{BatchConfig, DurabilityPolicy};
