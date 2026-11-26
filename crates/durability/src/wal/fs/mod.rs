//! Filesystem-backed Write-Ahead Log (WAL).
//!
//! This module provides the `io::Fs-based` WAL implementations:
//! - **`FsWalWriterIo`**: Append WAL records to local file
//! - **`FsWalFactoryIo`**: Factory for creating writers with swappable Fs implementations
//!
//! The `io::Fs` abstraction enables better testability with Mock and Chaos implementations.
//!
//! Reading a WAL back belongs to `crate::wal::recovery`, which owns frame
//! scanning, torn-tail tolerance, and writer-epoch fencing; no reader type is
//! published here.

mod factory_io;
mod writer_io;
mod writer_runner;

// Re-export the io::Fs-based implementations
pub use factory_io::FsWalFactoryIo;
pub use writer_io::FsWalWriterIo;
