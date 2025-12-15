//! Filesystem-backed Write-Ahead Log (WAL).
//!
//! Provides FsWalReader, FsWalWriter, and FsWalFactory.
//! Higher-level behavior (rotation, sync, recovery, sequencing) is handled by runtime actors.

mod factory;
mod reader;
mod writer;

pub use factory::FsWalFactory;
pub use reader::FsWalReader;
pub use writer::FsWalWriter;

use crate::storage::abstraction::StoragePath;

pub fn join(dir: &StoragePath, leaf: &str) -> StoragePath {
    let base = dir.as_str().trim_end_matches('/');
    if base.is_empty() {
        StoragePath::new(leaf)
    } else {
        StoragePath::new(format!("{base}/{leaf}"))
    }
}
