use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

// TODO: Refactor to NoOpWal - an in-memory WAL defeats the purpose of durability.
// This implementation exists to support StorageMode::Memory, but should be replaced
// with a simpler no-op implementation that explicitly discards writes rather than
// maintaining an in-memory buffer that will be lost on restart anyway.

// No-op WAL implementation for in-memory mode - explicitly discards all writes
// since durability is impossible in memory-only storage.
pub struct NoOpWal;

impl NoOpWal {
    pub fn new() -> Self {
        Self
    }
}

impl Default for NoOpWal {
    fn default() -> Self {
        Self::new()
    }
}

impl crate::wal::traits::WalWriter for NoOpWal {
    fn append_record(
        &self,
        _record: &crate::wal::types::WalRecord,
    ) -> crate::error::MidgeResult<crate::wal::types::WalPos> {
        // Explicitly discard - no durability in memory mode
        Ok(0)
    }

    fn append_op(
        &self,
        _kind: crate::wal::types::WalOpKind,
        _key: &[u8],
        _value: Option<&[u8]>,
    ) -> crate::error::MidgeResult<crate::wal::types::WalPos> {
        Ok(0)
    }

    fn flush(&self) -> crate::error::MidgeResult<()> {
        Ok(())
    }

    fn sync(&self) -> crate::error::MidgeResult<()> {
        Ok(())
    }

    fn current_pos(&self) -> crate::wal::types::WalPos {
        0
    }

    fn close(&self) -> crate::error::MidgeResult<()> {
        Ok(())
    }
}

impl crate::wal::traits::WalReaderDyn for NoOpWal {
    fn read_at(
        &mut self,
        _pos: crate::wal::types::WalPos,
    ) -> crate::error::MidgeResult<Option<crate::wal::types::WalRecord>> {
        // No records to read - all writes discarded
        Ok(None)
    }

    fn replay_boxed(
        &mut self,
        _start: crate::wal::types::WalPos,
        _cb: &mut dyn FnMut(&crate::wal::types::WalRecord) -> crate::error::MidgeResult<()>,
    ) -> crate::error::MidgeResult<()> {
        // Nothing to replay - all writes discarded
        Ok(())
    }

    fn close(&mut self) -> crate::error::MidgeResult<()> {
        Ok(())
    }
}

#[derive(Default)]
pub(super) struct MemInner {
    pub(super) buf: Vec<u8>,
}

// Global registry mapping directory paths to shared in-memory buffers so
// MemWalFactory can return writer/reader pairs that operate on the same buffer
#[allow(dead_code)]
pub(super) static MEM_REGISTRY: LazyLock<Mutex<HashMap<String, Arc<Mutex<MemInner>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
