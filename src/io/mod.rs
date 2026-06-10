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
//! - **Swappable**: RealFs, MockFs, ChaosFs for different scenarios
//!
//! ## Module Structure
//!
//! - **`traits`**: Core `Fs` and `File` abstractions
//! - **`real`**: `RealFs` - production filesystem via std::fs
//! - **`mock`**: `MockFs` - in-memory deterministic backend
//! - **`chaos`**: `ChaosFs` - failure injection wrapper
//! - **`staging`**: atomic temp-write + rename helpers for bootstrap files
//!
//! ## Usage Pattern
//!
//! Higher layers (SST, WAL, storage) depend on this via trait objects:

pub mod chaos;
pub mod mock;
pub mod real;
pub mod staging;
pub mod traits;

#[allow(unused_imports)]
pub use mock::MockFs;
#[allow(unused_imports)]
pub use real::RealFs;
#[allow(unused_imports)]
pub use traits::{Durability, File, Fs, FsPath, FsResult, OpenMode, OpenOptions};
