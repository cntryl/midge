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
