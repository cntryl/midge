//! Write-Ahead Log (WAL) subsystem
//!
//! Provides durable write-ahead logging with filesystem, in-memory,
//! and cloud-backed implementations.

pub mod arena;
pub mod cloud;
pub mod coordinator;
pub mod encode_pipeline;
pub mod encoding;
pub mod fs;
pub mod mem;
pub mod traits;
pub mod types;

// Re-export main WAL types from types module
pub use types::{WalOpKind, WalPos, WalRecord, WalSyncMode};

// Re-export traits
pub use traits::{WalFactory, WalReader, WalReaderDyn, WalWriter};

// Re-export concrete implementations
pub use cloud::{CloudWalReader, CloudWalWriter, WalBatchManager};
pub use coordinator::WalController;
pub use encoding::{decode, encode};
pub use fs::{FsWalFactory, Wal};
pub use mem::{MemWalFactory, WalMem, WalMemReader};

// Convenience aliases
pub use traits::WalFile;
