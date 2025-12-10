//! SST traits for readers and writers

use bytes::Bytes;
use std::path::Path;

use crate::common::MidgeResult;

/// Reader contract for SST implementations
pub trait SstReader: Send + Sync {
    /// Get the value for a specific key, if present
    fn get(&self, key: &[u8]) -> MidgeResult<Option<Bytes>>;

    /// Scan a key range [start, end) where either bound may be None
    /// Returns list of (key, value) pairs
    fn scan_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<Vec<(Bytes, Bytes)>>;
}

/// Stateful reader contract exposing tombstones and metadata
pub trait SstStateReader {
    /// Get presence state (value/tombstone/absent) for a specific key
    fn get_state(&self, key: &[u8]) -> MidgeResult<super::types::KeyState>;

    /// Scan a key range returning presence state for each key
    fn scan_range_state(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<Vec<(Bytes, super::types::KeyState)>>;

    /// Snapshot-aware point lookup (entries with seq > snapshot_seq are ignored)
    fn get_state_at(&self, key: &[u8], snapshot_seq: u64) -> MidgeResult<super::types::KeyState> {
        let state = self.get_state(key)?;
        match state {
            super::types::KeyState::Value(_val, seq, _exp, _op) if seq > snapshot_seq => {
                Ok(super::types::KeyState::Absent)
            }
            super::types::KeyState::Tombstone(seq) if seq > snapshot_seq => {
                Ok(super::types::KeyState::Absent)
            }
            _ => Ok(state),
        }
    }

    /// Return all range tombstones stored in this SST
    fn range_tombstones(&self) -> Vec<super::types::RangeTombstone> {
        Vec::new()
    }
}

/// Writer contract for SST implementations
pub trait SstWriter: Send {
    type Reader: SstReader;

    /// Add a key-value entry to the SST
    fn add(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()>;

    /// Finalize and produce a reader instance
    fn finish(self) -> MidgeResult<Self::Reader>;
}

/// Object-safe SST writer for polymorphic use
pub trait DynSstWriter: Send {
    /// Add a simple key-value entry
    fn add(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()>;

    /// Add an entry with metadata
    /// op_type: 0=Put, 1=Insert, 2=Delete, 3=Merge
    fn add_with_meta(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        _seq: u64,
        _op_type: u8,
        _expiration: Option<u64>,
    ) -> MidgeResult<()> {
        match value {
            Some(v) => self.add(key, v),
            None => Ok(()),
        }
    }

    /// Add a range tombstone
    fn add_range_tombstone(&mut self, start: &[u8], end: &[u8], seq: u64) -> MidgeResult<()> {
        let _ = (start, end, seq);
        Ok(())
    }

    /// Finalize and get SST bytes
    fn finish_bytes(self: Box<Self>) -> MidgeResult<Vec<u8>>;

    /// Finalize and write SST directly to path
    fn finish_to_path(self: Box<Self>, path: &Path) -> MidgeResult<()> {
        let bytes = self.finish_bytes()?;
        std::fs::write(path, &bytes)?;
        Ok(())
    }
}

/// Factory trait for creating SST writers and readers
pub trait SstFactory: Send + Sync {
    /// Create a new dynamic SST writer
    fn create(&self) -> MidgeResult<Box<dyn DynSstWriter>>;

    /// Open an existing SST file for reading
    fn open(&self, path: &Path) -> MidgeResult<Box<dyn SstReader>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test that traits are object-safe
    fn _assert_object_safe() {
        let _: &dyn DynSstWriter;
        let _: &dyn SstReader;
    }
}
