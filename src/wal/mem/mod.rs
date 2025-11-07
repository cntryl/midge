//! In-memory WAL implementation.
//!
//! Provides a non-durable WAL for testing and scenarios where
//! persistence is not required. Uses bincode for serialization.

mod factory;
mod reader;
mod shared;
mod writer;

pub use factory::MemWalFactory;
pub use reader::WalMemReader;
pub use writer::{WalMem, WalMemReaderHandle, WalMemWriter};
