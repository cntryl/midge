//! Cloud-backed SST implementation.
//!
//! This module provides SST (Sorted String Table) functionality using cloud storage backends:
//! - `reader.rs` - Cloud SST reader (SstCloudReader)
//! - `writer.rs` - Cloud SST writer (SstCloudWriter)
//! - `factory.rs` - Factory implementations for creating readers/writers
//! - `lifecycle.rs` - SST lifecycle management types (archival, deletion, etc.)
//!
//! The cloud SST implementation builds SSTs in memory and then uploads them to
//! cloud storage via the `StorageBackend` trait. Reads fetch the entire SST
//! blob on first access and cache it locally (or use ranged reads for metadata).

mod factory;
mod lifecycle;
mod reader;
mod writer;

// Re-export public types
pub use factory::{CloudSstFactory, CloudSstReaderFactory};
pub use lifecycle::{
    ArchiveTier, CloudSst, CloudSstManager, CloudSstManagerConfig, SstLifecycleState, SstMetadata,
    SstUploadMeta,
};
pub use reader::SstCloudReader;
pub use writer::SstCloudWriter;
