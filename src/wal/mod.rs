//! Write-Ahead Log (WAL) subsystem
//!
//! Provides durable write-ahead logging with filesystem implementations.

pub mod traits;
pub mod types;
pub mod encoding;
pub mod fs;
pub mod recovery;
pub mod backends;

// Re-export main WAL types
pub use types::{WalOpKind, WalPos, WalRecord, WalRecoveryStats, WalSyncMode, ColumnFamilyId};

// Re-export traits
pub use traits::{WalFactory, WalReader, WalReaderDyn, WalWriter};

// Re-export encoding functions
pub use encoding::{encode, decode};


// Re-export filesystem implementations
pub use fs::{FsWalFactory, FsWalWriter, FsWalReader};

// Re-export recovery
pub use recovery::{replay_wal, RecoveryStats};

