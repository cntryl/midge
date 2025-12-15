Yep — what you have is *EngineStorage*, not really “fs”. It’s clean, but the `wal_* / sst_* / manifest_*` surface bakes in LSM nouns, which makes it harder to reuse for chaos/fault injection, io-uring, Windows/Linux differences, etc.

A good way to “move closer to fs” is:

* make the trait about **paths + file handles + ops** (open/create/read/write/sync/rename/list)
* move “WAL/SST/manifest naming” into a **layout module** (pure functions that map `(cf, id)` → `Path`)
* keep your `Durability` concept, but express it as **sync/commit semantics on files and directories**, not “WAL commit”

Here’s a concrete shape that keeps what you like (typed errors, durability) but strips LSM concepts out of the FS boundary.

## 1) Make FS generic: paths, handles, atomic ops

```rust
use bytes::Bytes;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    Unsafe,
    Durable,
}

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

pub type FsResult<T> = Result<T, FsError>;

/// Your own tiny path type helps keep this portable (vs std::path leaking everywhere).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FsPath(pub String);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    ReadOnly,
    ReadWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenOptions {
    pub mode: OpenMode,
    pub create: bool,
    pub create_new: bool,
    pub truncate: bool,
}

pub trait Fs: Send + Sync + 'static {
    // --- Files ---
    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File>>;
    fn remove_file(&self, path: &FsPath) -> FsResult<()>;
    fn exists(&self, path: &FsPath) -> FsResult<bool>;
    fn metadata(&self, path: &FsPath) -> FsResult<Metadata>;

    // --- Directories ---
    fn create_dir_all(&self, path: &FsPath) -> FsResult<()>;
    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>>;
    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()>;
    fn sync_dir(&self, path: &FsPath, dur: Durability) -> FsResult<()>;

    // --- Atomicity ---
    /// Must be atomic within the same filesystem/volume when supported.
    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()>;
}

#[derive(Debug, Clone)]
pub struct Metadata {
    pub len: u64,
}

#[derive(Debug, Clone)]
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
}

pub trait File: Send {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes>;
    fn write_at(&mut self, offset: u64, data: Bytes) -> FsResult<()>;

    /// Common fast-path write (append-only logs).
    fn append(&mut self, data: Bytes) -> FsResult<u64>; // returns starting offset

    fn len(&self) -> FsResult<u64>;

    /// The durability boundary (fsync / flush / multipart commit / etc.)
    fn sync(&mut self, dur: Durability) -> FsResult<()>;

    fn close(self: Box<Self>) -> FsResult<()>;
}
```

**What this buys you**

* ChaosFs becomes trivial (wrap `Fs` + `File`, inject faults everywhere).
* WindowsFs/LinuxFs are just implementations of generic operations.
* io-uring fits naturally: your `File` impl can be backed by an io-uring reactor, but the engine doesn’t care.

## 2) Put LSM naming in a layout module (pure mapping)

Your current `CfId/WalId/SstId` types are great — just move them out of the FS trait and into a “layout” helper that produces `FsPath`.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CfId(pub u32);
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WalId(pub u64);
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SstId(pub u64);

pub struct EngineLayout {
    pub root: FsPath,
}

impl EngineLayout {
    pub fn cf_dir(&self, cf: CfId) -> FsPath {
        FsPath(format!("{}/cf_{:08}", self.root.0, cf.0))
    }

    pub fn wal_path(&self, cf: CfId, wal: WalId) -> FsPath {
        FsPath(format!("{}/wal/{:020}.wal", self.cf_dir(cf).0, wal.0))
    }

    pub fn sst_path(&self, cf: CfId, sst: SstId) -> FsPath {
        FsPath(format!("{}/sst/{:020}.sst", self.cf_dir(cf).0, sst.0))
    }

    pub fn manifest_path(&self, cf: CfId) -> FsPath {
        FsPath(format!("{}/manifest.json", self.cf_dir(cf).0))
    }

    pub fn manifest_tmp_path(&self, cf: CfId) -> FsPath {
        FsPath(format!("{}/manifest.json.tmp", self.cf_dir(cf).0))
    }
}
```

