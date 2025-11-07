use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

// TODO: Refactor to NoOpWal - an in-memory WAL defeats the purpose of durability.
// This implementation exists to support StorageMode::Memory, but should be replaced
// with a simpler no-op implementation that explicitly discards writes rather than
// maintaining an in-memory buffer that will be lost on restart anyway.

#[derive(Default)]
pub(super) struct MemInner {
    pub(super) buf: Vec<u8>,
}

// Global registry mapping directory paths to shared in-memory buffers so
// MemWalFactory can return writer/reader pairs that operate on the same buffer
pub(super) static MEM_REGISTRY: LazyLock<Mutex<HashMap<String, Arc<Mutex<MemInner>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
