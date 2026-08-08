//! Storage-runtime actors grouped by responsibility.
//!
//! Ownership boundaries:
//!
//! - `wal`: append, sync, rotate, and replay-related durability frontiers
//! - `flush`: freeze memtables and stage SST publication
//! - `compaction`: plan and execute replacement SST sets, then hand publication back to runtime
//! - `manifest`: publish authoritative file-set changes
//! - `cloud`: cloud durability and upload coordination
//! - `gc`: delete obsolete files only after publication makes them safe to remove

#[cfg(test)]
pub mod cloud;
pub mod cloud_write_queue;
pub mod compaction;
pub mod flush;
pub mod gc;
pub mod manifest;
pub mod wal;

#[cfg(test)]
pub use cloud::CloudActor;
pub use compaction::CompactionActor;
pub use flush::FlushActor;
pub use gc::GcActor;
pub use manifest::ManifestActor;
pub use wal::WalActor;
