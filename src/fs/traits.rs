use bytes::Bytes;
use thiserror::Error;

/// Durability semantics exposed to engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Data is visible but may be lost on a crash or power loss.
    Unsafe,
    /// Data is durable according to backend contract (fsync/commit acked).
    Durable,
}

/// Errors raised by EngineFs implementations.
#[derive(Error, Debug)]
pub enum FsError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("already exists: {0}")]
    AlreadyExists(String),
    #[error("corruption: {0}")]
    Corruption(String),
    #[error("io: {0}")]
    Io(String),
    #[error("backend unavailable: {0}")]
    Unavailable(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
}

/// Convenience alias for results produced by FS operations.
pub type FsResult<T> = Result<T, FsError>;

/// Typed identifier for a column family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CfId(pub u32);

/// Typed WAL identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WalId(pub u64);

/// Typed SST identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SstId(pub u64);

/// High-level Engine-oriented filesystem trait.
pub trait EngineFs: Send + Sync + 'static {
    // ---------- WAL ----------
    fn wal_open(&self, cf: CfId, wal: WalId) -> FsResult<Box<dyn WalWriter>>;
    fn wal_read(&self, cf: CfId, wal: WalId) -> FsResult<Box<dyn WalReader>>;
    fn wal_list(&self, cf: CfId) -> FsResult<Vec<WalId>>;
    fn wal_delete(&self, cf: CfId, wal: WalId) -> FsResult<()>;

    // ---------- SST ----------
    fn sst_create(&self, cf: CfId, sst: SstId) -> FsResult<Box<dyn SstWriter>>;
    fn sst_open(&self, cf: CfId, sst: SstId) -> FsResult<Box<dyn SstReader>>;
    fn sst_list(&self, cf: CfId) -> FsResult<Vec<SstId>>;
    fn sst_delete(&self, cf: CfId, sst: SstId) -> FsResult<()>;

    // ---------- MANIFEST ----------
    fn manifest_read(&self, cf: CfId) -> FsResult<Bytes>;
    fn manifest_replace_atomic(
        &self,
        cf: CfId,
        new_contents: Bytes,
        dur: Durability,
    ) -> FsResult<()>;

    // ---------- MAINTENANCE ----------
    /// Best-effort directory sync where supported. No-op on backends that
    /// do not require it.
    fn sync_dir_if_supported(&self, cf: CfId) -> FsResult<()>;
}

/// WAL writer semantics. `append()` may buffer. `commit()` is the
/// durability boundary for the WAL; the engine must wait for a successful
/// `commit(Durability::Durable)` before making writes visible when
/// operating in durable modes.
pub trait WalWriter: Send {
    /// Append a WAL record (opaque blob). May buffer.
    fn append(&mut self, record: Bytes) -> FsResult<()>;

    /// Ensure prior appends are durable according to `dur`.
    fn commit(&mut self, dur: Durability) -> FsResult<()>;

    /// Best-effort close (explicit commit semantics handled separately).
    fn close(self: Box<Self>) -> FsResult<()>;
}

/// WAL reader used for recovery.
pub trait WalReader: Send {
    fn read_all(&mut self) -> FsResult<Vec<Bytes>>;
}

/// SST writer; `finish()` makes the SST atomically visible when it
/// returns successfully under `dur=Durability::Durable`.
pub trait SstWriter: Send {
    fn write_block(&mut self, block: Bytes) -> FsResult<()>;
    fn finish(self: Box<Self>, dur: Durability) -> FsResult<()>;
}

/// SST reader.
pub trait SstReader: Send {
    fn read_block(&mut self, offset: u64, len: u64) -> FsResult<Bytes>;
    fn len(&self) -> FsResult<u64>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn traits_are_send_sync() {
        // Ensure the trait objects are Send + Sync where expected.
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<&dyn EngineFs>();
    }
}
