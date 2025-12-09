//! Filesystem utilities
//!
//! Common filesystem operations shared across WAL and SST modules:
//! - File I/O operations (read_exact_at, read_range, vectorized writes)
//! - Numbered file management (find latest, generate paths)
//! - Platform-specific sync operations (fdatasync)
//! - Sequential reading with position tracking

pub mod io;
pub mod numbered_files;
pub mod sync;

// Re-export commonly used functions
pub use io::{
    current_position, file_size, read_exact, read_exact_at, read_file, read_from_end, read_range,
    seek, write_all, write_all_with_hooks, write_vectored, write_vectored_with_hooks,
    SequentialReader,
};
pub use numbered_files::{find_latest_numbered_file, list_numbered_files, numbered_file_path};
pub use sync::sync_data_only;
pub use sync::sync_parent;
