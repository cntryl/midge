//! Write-ahead logging abstraction
//!
//! Traits for different WAL implementations

pub mod traits;
pub mod segment;
pub mod writer;
pub mod reader;
pub mod index;
pub mod backends;

pub use traits::{WalReader, WalWriter, WalRecord, WalOpKind, WalPos};
pub use segment::Segment;
pub use index::Index;
pub use backends::{LocalWal, HybridWal, BatchedSyncWal, CloudWal};

/// WAL entry - legacy type for compatibility
#[derive(Clone, Debug)]
pub struct WalEntry {
    pub sequence: u64,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}
