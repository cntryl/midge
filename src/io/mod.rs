//! Base I/O subsystem - domain-agnostic filesystem abstraction
//!
//! This is the foundation for all filesystem interactions in Midge:
//! - SST layer (block read/write)
//! - WAL layer (segment write/replay)
//! - Any other component needing filesystem access
//!
//! ## Design Principles
//!
//! - **Synchronous & fast**: Blocking I/O for direct access patterns
//! - **Vectorized first-class**: `readv_at`, `writev_at`, `appendv` for efficiency
//! - **Platform-optimizable**: Implementations can use preadv/pwritev, direct I/O, etc.
//! - **Fully domain-agnostic**: Zero knowledge of WAL, SST, actors, etc.
//! - **Swappable**: `RealFs` and `MockFs` for production and deterministic tests
//!
//! ## Module Structure
//!
//! - **`traits`**: Core `ReadFs`, `Fs`, `ReadFile` and `File` abstractions
//! - **`real`**: `RealFs` - production filesystem via `std::fs`
//! - **`mock`**: `MockFs` - in-memory deterministic backend
//! - **`staging`**: atomic temp-write + rename helpers for bootstrap files
//!
//! ## Usage Pattern
//!
//! Higher layers (SST, WAL, storage) depend on this via trait objects:

pub(crate) mod buffered_read;
#[cfg(test)]
mod conformance_tests;
pub(crate) mod durable_dir;
mod location;
pub mod mock;
pub mod real;
mod scoped_fs;
pub mod staging;
pub(crate) mod stream;
mod subdirectory;
pub mod traits;
#[cfg(test)]
pub(crate) mod transient_read;
mod whole_file;

pub(crate) use location::FsLocation;
pub use mock::MockFs;
pub use real::RealFs;
pub(crate) use scoped_fs::{scope_fs, scope_read_fs};
pub(crate) use subdirectory::subdirectory;
pub use traits::{
    Durability, File, Fs, FsError, FsPath, FsResult, HostAddressing, OpenMode, OpenOptions,
    ReadFile, ReadFs,
};
pub(crate) use whole_file::{copy_durably, read_all, truncate_durably};
