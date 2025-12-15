//! Write-Ahead Log (WAL) subsystem
//!
//! Provides durable write-ahead logging with filesystem implementations.
//!
//! ## Implementation
//!
//! Uses modern io::Fs-based implementations for production code:
//! - [`fs::FsWalWriterIo`] - Append records to WAL
//! - [`fs::FsWalReaderIo`] - Read records from WAL
//! - [`fs::FsWalFactoryIo`] - Factory for creating readers/writers
//!
//! The io::Fs abstraction enables better testability with swappable implementations
//! (Real, Mock, Chaos). All production code has been migrated to this interface.

pub mod encoding;
pub mod fs;
pub mod policy;
pub mod recovery;
pub mod traits;
pub mod types;

// Re-export main WAL types
pub use types::{ColumnFamilyId, WalOpKind, WalPos, WalRecord};

// Re-export traits
pub use traits::{WalFactory, WalReader, WalReaderDyn, WalWriter};

// Re-export encoding functions
pub use encoding::{decode, encode};

// Re-export io::Fs-based implementations
pub use fs::{FsWalFactoryIo, FsWalReaderIo, FsWalWriterIo};

// Re-export recovery
pub use recovery::{replay_wal, RecoveryStats};

// Re-export policy types
pub use policy::{BatchConfig, DurabilityPolicy};
