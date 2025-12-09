//! WAL trait definitions for different implementations
use crate::common::MidgeResult;
use bytes::Bytes;

/// Operation kind for WAL entries
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalOpKind {
    Put,
    Delete,
    Merge,
    DeleteRange,
}

/// Position of a record in the WAL
#[derive(Debug, Clone, Copy)]
pub struct WalPos {
    pub segment: u64,
    pub offset: u64,
}

/// A complete WAL record
#[derive(Debug, Clone)]
pub struct WalRecord {
    pub cf_id: u32,
    pub op: WalOpKind,
    pub key: Bytes,
    pub value: Option<Bytes>,
    pub seq: u64,
    pub expiration: Option<u64>,
    pub range_end: Option<Bytes>,
}

impl WalRecord {
    pub fn new(op: WalOpKind, key: Bytes, value: Option<Bytes>, seq: u64) -> Self {
        Self {
            cf_id: 0,
            op,
            key,
            value,
            seq,
            expiration: None,
            range_end: None,
        }
    }
}

/// WAL reader trait
pub trait WalReader: Send + Sync {
    /// Read the next record from the WAL
    fn next_record(&mut self) -> MidgeResult<Option<WalRecord>>;
    /// Seek to a specific position
    fn seek(&mut self, pos: WalPos) -> MidgeResult<()>;
}

/// WAL writer trait
pub trait WalWriter: Send + Sync {
    /// Append a record to the WAL
    fn append_record(&self, record: &WalRecord) -> MidgeResult<WalPos>;
    /// Append an operation (convenience method)
    fn append_op(
        &self,
        op: WalOpKind,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> MidgeResult<WalPos> {
        let record = WalRecord {
            cf_id: 0,
            op,
            key: Bytes::copy_from_slice(key),
            value: value.map(|v| Bytes::copy_from_slice(v)),
            seq: 0,
            expiration: None,
            range_end: None,
        };
        self.append_record(&record)
    }
    /// Sync all pending writes to durable storage
    fn sync(&mut self) -> MidgeResult<()>;
    /// Close the WAL
    fn close(&mut self) -> MidgeResult<()> {
        self.sync()
    }
}
