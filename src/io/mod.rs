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
//!
//! ## Usage Pattern
//!
//! Higher layers (SST, WAL, storage) depend on this via trait objects:
//!
//! ```ignore
//! // Use base abstraction
//! let fs: Arc<dyn Fs> = Arc::new(RealFs::new(path)?);
//!
//! // Or with chaos injection for testing
//! let fs: Arc<dyn Fs> = Arc::new(ChaosFs::new(
//!     Arc::new(MockFs::new()),
//!     fail_every,
//! ));
//! ```

pub mod chaos;
pub mod mock;
pub mod real;
pub mod traits;

pub use chaos::ChaosFs;
pub use mock::MockFs;
pub use real::RealFs;
pub use traits::{
    DirEntry, Durability, File, FileCaps, Fs, FsError, FsPath, FsResult, Metadata, OpenMode,
    OpenOptions,
};
