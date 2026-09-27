//! SST (Sorted String Table) module
//!
//! Provides on-disk SST file implementations.
//!
//! ## Key Design: SST uses `std::fs` directly, NOT `storage/` layer
//!
//! SSTs intentionally use synchronous, direct filesystem I/O via `std::fs` rather than
//! the callback-driven `StorageBackend` trait. This is correct because:
//!
//! - **Immutable after `finalize()`**: SST files never change once written, only read or deleted
//! - **Blocking I/O required**: SST access patterns (seek + read at offset) need synchronous I/O
//! - **Local files first**: SSTs are written locally, then persisted to cloud via `HybridStorage`
//! - **Hot path on read side**: Reader needs fast, direct access without callback overhead
//!
//! ### Integration with Storage Layer
//!
//! - **Write path**: Compaction creates SSTs via `FsSstFactoryIo` (using `io::Fs` abstraction)
//!   → Files stored in local directory
//!   → `HybridStorage` persists to cloud (via `StorageBackend` callbacks)
//!
//! - **Read path**: Queries use `SstFileIo` to read local SSTs
//!   → Uses `io::Fs` for flexible real and mock filesystem backends
//!   → Block cache + bloom filters for optimization
//!   → No cloud access on read (reads hit local cache or cloud-synced local file)
//!
//! ## Module Overview
//!
//! - **encoding**: TLV-based entry encoding for SST files
//! - **types**: SST file format types (blocks, footers, handles)
//! - **traits**: Reader/Writer/Factory contracts for SST implementations
//! - **fs**: Filesystem-backed SST implementation (uses `io::Fs` abstraction)

pub mod bloom;
pub mod cache;
pub mod encoding;
pub mod fs;
pub(crate) mod identity;
pub mod index;
pub mod read_amp_metrics;
pub(crate) mod read_path_metrics;
pub mod traits;
pub mod trie;
pub mod types;

pub use fs::FsSstFactoryIo;

pub use read_amp_metrics::ReadAmpMetrics;

pub use traits::{SstFactory, SstStateReader};
