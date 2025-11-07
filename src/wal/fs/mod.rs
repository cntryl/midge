//! Filesystem-backed WAL implementation.
//!
//! Provides durable write-ahead logging using filesystem storage with:
//! - TLV encoding format for efficient parsing
//! - Optional compression (LZ4) for large values
//! - Group commit coordination for batching fsyncs

mod factory;
mod group_commit;
mod reader;
mod writer;

pub use factory::FsWalFactory;
pub use group_commit::{GroupCommitConfig, GroupCommitCoordinator};
pub use reader::replay_wal_file;
pub use writer::{replay_wal_file_with_mode, Wal};
