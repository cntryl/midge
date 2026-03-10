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
pub mod frame;
pub mod fs;
pub mod policy;
pub mod recovery;
pub mod traits;
pub mod types;

// Re-export main WAL types
pub use types::{WalOpKind, WalRecord};

// Re-export traits
pub use traits::{WalReaderDyn, WalWriter};

// Re-export encoding functions

// Re-export io::Fs-based implementations
pub use fs::FsWalFactoryIo;

// Re-export recovery

// Re-export policy types
pub use policy::DurabilityPolicy;
