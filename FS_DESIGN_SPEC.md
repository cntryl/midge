Yep. Here’s a **filesystem / persistence abstraction** that’s *intent-based*, portable, chaos-injectable, and keeps your **WAL-before-visible** invariant enforceable.

## Design goals

* Engine code never touches `std::fs` (or S3/Azure SDK) directly.
* Engine asks for **intent** (“append WAL + commit”, “publish SST atomically”, “replace manifest atomically”).
* One seam for:

  * `RealFs` (std::fs + OS quirks)
  * `FastFs` (bench, in-memory / no sync)
  * `ChaosFs` (faults + latency)
  * `CloudFs` (object store / append service)
* Keep the hard invariant: **memtable visibility waits for `wal.commit()` ack**.

---

## Core types

### Errors and durability

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Data is visible but may be lost on crash/power loss (bench / debug only).
    Unsafe,
    /// Data is durable according to backend contract (fsync/commit acked).
    Durable,
}

#[derive(thiserror::Error, Debug)]
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
```

### Engine paths are typed (avoid stringly-typed footguns)

```rust
#[derive(Debug, Clone)]
pub struct CfId(pub u32);

#[derive(Debug, Clone)]
pub struct WalId(pub u64);

#[derive(Debug, Clone)]
pub struct SstId(pub u64);
```

---

## The abstraction: `EngineFs`

This is what your runtime/actors use. It is **not** a general filesystem.

```rust
use bytes::Bytes;

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
    fn manifest_replace_atomic(&self, cf: CfId, new_contents: Bytes, dur: Durability) -> FsResult<()>;

    // ---------- MAINTENANCE ----------
    fn sync_dir_if_supported(&self, cf: CfId) -> FsResult<()>; // no-op on backends that don't need it
}
```

### WAL writer/reader (commit boundary lives here)

```rust
pub trait WalWriter: Send {
    /// Append a record. May buffer.
    fn append(&mut self, record: Bytes) -> FsResult<()>;

    /// Ensures prior appends are durable per backend contract.
    /// Engine’s visibility boundary depends on this.
    fn commit(&mut self, dur: Durability) -> FsResult<()>;

    /// Best-effort close (commit semantics still explicit).
    fn close(self: Box<Self>) -> FsResult<()>;
}

pub trait WalReader: Send {
    fn read_all(&mut self) -> FsResult<Vec<Bytes>>;
}
```

### SST writer/reader (publish is atomic)

```rust
pub trait SstWriter: Send {
    fn write_block(&mut self, block: Bytes) -> FsResult<()>;
    fn finish(self: Box<Self>, dur: Durability) -> FsResult<()>;
}

pub trait SstReader: Send {
    fn read_block(&mut self, offset: u64, len: u64) -> FsResult<Bytes>;
    fn len(&self) -> u64;
}
```

**Key:** `finish()` is where the backend guarantees “either the SST exists fully, or not at all”.

---

## Required semantics (the contract)

These are the *only* semantics your engine assumes:

### WAL

* `append()` may buffer.
* `commit(Durable)` is the durability ack:

  * Local: `write + fsync(fd)` (and optionally directory sync when needed)
  * Cloud: “server committed and acknowledged” (upload/append service ack)
* Engine must **not** apply to memtable until `commit(Durable)` succeeds (in durable modes).

### SST

* `finish(Durable)` makes SST visible atomically.

  * Local: write temp → fsync temp → rename → fsync dir (where required)
  * Cloud: upload temp key → server-side finalize / atomic pointer swap (or content-addressed + manifest reference)

### Manifest

* `manifest_replace_atomic(Durable)` is **atomic replace**:

  * Local: write temp → fsync → rename → fsync dir
  * Cloud: write new manifest blob → update “current” pointer atomically (or versioned manifest + compare-and-swap)

---

## Implementation sketch

### RealFs (portable; hides OS quirks)

* Uses `std::fs` and internal `cfg(windows)` strategies:

  * Atomic replace via `write temp + rename` (Windows may require `replace_file` semantics).
  * Optional `sync_dir_if_supported()` does the right thing on platforms that need it.

### FastFs (bench)

* In-memory map keyed by `(cf, wal/sst/manifest ids)`.
* `commit(Unsafe)` is no-op; `commit(Durable)` can still be no-op but should simulate a boundary (or optionally block for deterministic “latency”).

### ChaosFs (wrapper)

Wrap any `EngineFs` and inject:

* delay on specific operations (`wal.commit`, `sst.finish`, `manifest_replace_atomic`)
* fail rates (e.g. 1% commit failures)
* corruption injection for targeted reads (for recovery tests)

```rust
pub struct ChaosFs<F: EngineFs> {
    inner: F,
    // rules: latency, failure injection, deterministic seed, etc.
}
```

### CloudFs

* Still implements the same *intent* API.
* If you later add `io-uring`, it’s **inside RealFs**, not visible in the trait.

---

## How this plugs into your actors

* WalActor owns a `WalWriter`.
* Every “put” does:

  1. `wal.append(record)`
  2. `wal.commit(Durable)`  ✅ durability boundary
  3. apply to memtable
* FlushActor uses `sst_create → write_block… → finish(Durable)`
* ManifestActor uses `manifest_replace_atomic(Durable)`

No one else touches persistence.
