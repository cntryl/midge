//! Filesystem-backed SST implementation using io::Fs abstraction
//!
//! This module provides SST file reader and factory using the base io::Fs trait,
//! allowing for swappable filesystem implementations (Real, Mock, Chaos) for testing.

pub mod factory_io;
pub mod reader_io;

pub use factory_io::FsSstFactoryIo;
pub use reader_io::{SstFileIo, SstFileSummary};
