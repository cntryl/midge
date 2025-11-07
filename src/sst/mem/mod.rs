//! In-memory SST implementation.
//!
//! This module provides SST (Sorted String Table) functionality using in-memory buffers:
//! - `reader.rs` - In-memory SST reader (SstMemReader)
//! - `writer.rs` - In-memory SST writer (SstMemWriter)
//! - `factory.rs` - Factory implementations for creating readers/writers

mod factory;
mod reader;
mod writer;

// Re-export public types
pub use factory::{MemSstFactory, MemSstReaderFactory};
pub use reader::SstMemReader;
pub use writer::SstMemWriter;

// Internal data structure shared between reader and writer
#[derive(Debug, Default, Clone)]
pub(super) struct MemSstData {
    pub(super) raw: Vec<u8>,
}
