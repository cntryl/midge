use parking_lot::Mutex;
use std::sync::Arc;

use super::reader::{WalMemReader, WalMemReaderDynAdapter};
use super::shared::{MemInner, MEM_REGISTRY};
use super::writer::WalMem;

/// In-memory WAL factory
pub struct MemWalFactory;

impl crate::wal::WalFactory for MemWalFactory {
    fn create_writer(
        &self,
        _dir: &std::path::Path,
    ) -> crate::error::MidgeResult<Box<dyn crate::wal::WalWriter>> {
        // Use a registry keyed by the directory path so reader/writer for the
        // same dir share the same in-memory buffer.
        let key = _dir.to_string_lossy().to_string();
        let arc = {
            let mut reg = MEM_REGISTRY.lock();
            reg.entry(key)
                .or_insert_with(|| Arc::new(Mutex::new(MemInner::default())))
                .clone()
        };
        Ok(Box::new(WalMem { inner: arc }))
    }

    fn create_reader(
        &self,
        _dir: &std::path::Path,
    ) -> crate::error::MidgeResult<Box<dyn crate::wal::WalReaderDyn>> {
        let key = _dir.to_string_lossy().to_string();
        let arc = {
            let mut reg = MEM_REGISTRY.lock();
            reg.entry(key)
                .or_insert_with(|| Arc::new(Mutex::new(MemInner::default())))
                .clone()
        };
        Ok(Box::new(WalMemReaderDynAdapter(WalMemReader {
            inner: arc,
        })))
    }

    fn rotate_writer(
        &self,
        _dir: &std::path::Path,
        _seq: u64,
    ) -> crate::error::MidgeResult<Box<dyn crate::wal::WalWriter>> {
        // For in-memory WAL, rotation is a no-op: just create a new buffer-backed writer
        Ok(Box::new(WalMem::new()))
    }
}
