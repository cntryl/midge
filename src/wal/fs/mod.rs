//! Filesystem-backed Write-Ahead Log (WAL).
//!
//! This module provides the io::Fs-based WAL implementations:
//! - **FsWalWriterIo**: Append WAL records to local file
//! - **FsWalReaderIo**: Read WAL records from local file
//! - **FsWalFactoryIo**: Factory for creating readers/writers with swappable Fs implementations
//!
//! The io::Fs abstraction enables better testability with Mock and Chaos implementations.

mod factory_io;
mod reader_io;
mod writer_io;
mod writer_runner;

// Re-export the io::Fs-based implementations
pub use factory_io::FsWalFactoryIo;
pub use reader_io::FsWalReaderIo;
pub use writer_io::FsWalWriterIo;
