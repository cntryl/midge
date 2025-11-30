# Midge Architecture Deep Dive (Sections A-G)

Detailed code review for evaluating configurability, write path integration, compaction scheduling, SST correctness, manifest safety, durability profile, and mutation visibility.

---

## A. MidgeOptions — Configuration

**File:** `src/config/options.rs`

### Key Configuration Fields

```rust
pub struct MidgeOptions {
    pub storage_mode: StorageMode,          // LocalDisk | Memory | CloudBacked | Hybrid
    pub memtable_size: usize,               // 64MB default
    pub max_levels: usize,                  // 7 (leveled LSM)
    pub block_size: usize,                  // 4KB
    pub compression: CompressionType,       // LZ4 default (also Zstd 1/3/5/9)
    pub wal_sync: WalSyncMode,              // EveryWrite | BatchedSync | None
    pub cache_size_mb: usize,               // 128MB block cache
    pub wal_recovery_mode: WalRecoveryMode, // AbsoluteConsistency | TolerateCorruptedTail
    // ... additional fields
}
```

### Validation

- `validate()` method with bounds checking for all parameters
- Storage mode validation
- Sensible defaults for typical workloads

### Assessment

| Aspect | Status |
|--------|--------|
| Defaults | ✅ Good for typical workloads |
| Validation | ✅ Bounds checking present |
| Missing | ⚠️ bloom_bits_per_key, write_buffer_count, level0_file_num_compaction_trigger |

---

## B. FlushCoordinator + CompactionController

### FlushCoordinator

**File:** `src/core/persistence/flush_coordinator.rs`

```rust
pub struct FlushCoordinator {
    tx: Sender<FlushMsg>,
    handle: Option<JoinHandle<()>>,
}

pub enum FlushMsg {
    Entries { ... },
    Barrier(Sender<()>),
    Shutdown,
}
```

**Key Features:**
- Background worker thread with crossbeam channels
- `spawn()` returns (Self, WorkerHandle)
- `request_flush()` queues memtable entries
- `wait_until_idle()` uses barrier pattern for deterministic testing

### CompactionController

**File:** `src/core/compaction/controller.rs`

```rust
pub struct CompactionController {
    tx: Mutex<Option<Sender<CompactionMsg>>>,
    handle: Mutex<Option<JoinHandle<()>>>,
    version_manager: Arc<VersionManager>,
    background_error: Arc<Mutex<Option<MidgeError>>>,
}

pub enum CompactionMsg {
    CompactLevel { level: usize },
    CompactRange { start: Vec<u8>, end: Vec<u8> },
    Barrier(Sender<()>),
    Shutdown,
}
```

**Key Features:**
- Leveled compaction strategy with automatic + manual triggering
- Version manager integration for atomic manifest updates
- Background error propagation to engine
- `run_plan_sync()` for deterministic testing

### Assessment

| Aspect | Status |
|--------|--------|
| Architecture | ✅ Clean actor pattern with message passing |
| Error handling | ✅ Background error propagation |
| Testing | ✅ Barrier support for determinism |
| Missing | ⚠️ Rate limiter integration, compaction score logging |

---

## C. SST Writer/Iterator

### FsDynWriter

**File:** `src/sst/fs/writer.rs`

```rust
pub struct FsDynWriter {
    file: std::fs::File,
    temp_path: PathBuf,
    block_size: usize,
    compression: CompressionType,
    use_internal_keys: bool,
    cur_block: DataBlockBuilder,
    offsets: Vec<(Vec<u8>, BlockHandle)>,
    index: IndexBlockBuilder,
    bloom_builder: BloomFilterBuilder,
    range_tombstones: Vec<RangeTombstone>,
    offset: u64,
    test_hooks: Option<TestHooks>,
}
```

**Key Features:**
- Streaming writer to temp file (avoids full SST in memory)
- Block-at-a-time encoding with configurable compression
- Sparse index, bloom filter, range tombstones built during write
- `finish_to_path()` does atomic rename + parent dir fsync (best-effort)

**Write Flow:**
1. Add entries → accumulate in DataBlockBuilder
2. When block full → encode, compress, write to temp file
3. On finish → write index block, bloom filter, meta-index, footer
4. Atomic rename temp → final path
5. fsync parent directory

### SstRangeIter

**File:** `src/sst/fs/iterator.rs`

```rust
pub struct SstRangeIter {
    path: PathBuf,
    blocks: Vec<BlockHandle>,
    blk_idx: usize,
    data: Option<Vec<u8>>,
    cursor: usize,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    use_internal_keys: bool,
}
```

**Key Features:**
- Block-by-block lazy loading from disk
- TLV parsing with prefix-compressed keys
- Range filtering with start/end bounds
- Tombstone skipping built-in

### Assessment

| Aspect | Status |
|--------|--------|
| Memory efficiency | ✅ Streaming design for large SSTs |
| Crash safety | ✅ Atomic rename + parent fsync |
| I/O pattern | ⚠️ Each block read opens new file handle |
| Optimization | ⚠️ No bloom filter check in iterator |

---

## D. Manifest — VersionSet + VersionManager

### VersionSet

**File:** `src/core/manifest/version_set.rs`

```rust
#[derive(Clone)]
pub struct VersionSet {
    pub manifest: Manifest,
}

pub enum VersionEdit {
    AddFile { file: Box<FileMeta> },
    RemoveFiles { names: Vec<String> },
    UpdateSequence { sequence: u64 },
    CombinedAddRemove { add: Box<FileMeta>, remove: Vec<String> },
}

pub struct AtomicVersionSet {
    inner: Arc<ArcSwap<VersionSet>>,
}
```

**Key Features:**
- Immutable snapshot via `Arc<ArcSwap<VersionSet>>`
- `apply_edit()` clones manifest, applies edit, returns new VersionSet
- `apply_edits()` batch multiple edits in single clone (O(n) vs O(n²))
- Lock-free reads via ArcSwap

### VersionManager

**File:** `src/core/manifest/version_manager.rs`

```rust
pub struct VersionManager {
    tx: Mutex<Option<Sender<VersionEditRequest>>>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

// Actor loop: process_edit()
fn process_edit(version_set, db_path, edit, test_hooks, mem_mode) {
    let current = version_set.load();
    let new_version = current.apply_edit(edit)?;
    if !mem_mode {
        new_version.manifest.save_atomic_with_hooks(db_path, test_hooks)?;
    }
    version_set.store(Arc::new(new_version));
}
```

**Key Features:**
- Actor pattern with crossbeam channel (100-depth backpressure)
- `apply_edit_sync()` blocks until manifest persisted
- `apply_edit_async()` fire-and-forget for background operations
- Atomic visibility: manifest write + version publish in single operation

### Assessment

| Aspect | Status |
|--------|--------|
| Read path | ✅ Lock-free via ArcSwap |
| Write path | ✅ Serial writes via actor pattern |
| Persistence | ✅ Atomic manifest save |
| Batching | ⚠️ Each edit = one disk write (no batching) |
| Growth | ⚠️ No manifest compaction/snapshotting |

---

## E. WAL — Encode Pipeline + Group Commit + Fsync

### WalEncoder

**File:** `src/wal/encode_pipeline.rs`

```rust
pub struct WalEncoder<E: BodyEncoder> {
    cfg: EncodeConfig,
    enc: E,
    pool: Option<&'static ThreadPool>,
}

pub struct EncodeConfig {
    pub parallelism: usize,
    pub max_body_len: usize,
    pub parallel_threshold_bytes: usize,  // 128KB default
}

pub struct EncodedBatch {
    pub headers: Vec<[u8; 8]>,  // [CRC32C (4) | LEN (4)]
    pub bodies: Vec<u8>,        // Contiguous arena
    pub offsets: Vec<(usize, usize)>,
}
```

**Key Features:**
- Parallel encoding via rayon ThreadPool (global, reused across bench runs)
- `StreamingBodyEncoder`: CRC32C computed incrementally during encode (no second pass)
- `EncodedBatch`: contiguous body arena + header array for vectored I/O
- Automatic fallback to sequential for small batches

### BatchedSyncCoordinator

**File:** `src/wal/fs/batched_sync.rs`

```rust
pub struct BatchedSyncCoordinator {
    in_progress: AtomicBool,     // Leadership flag
    epoch: AtomicU64,            // Batch epoch (monotonic)
    result: AtomicU8,            // 0=pending, 1=ok, 2=err
    pending: AtomicU64,          // Waiters count
    park_lock: Mutex<()>,
    park_cv: Condvar,
    config: BatchedSyncConfig,
}

pub struct BatchedSyncConfig {
    pub wait_micros: u64,        // 100µs default
    pub spin_loops: u32,         // 100 default
}
```

**Algorithm:**
1. All callers increment `pending` and attempt CAS on `in_progress`
2. Winner becomes leader, sleeps `wait_micros` to accumulate batch
3. Leader performs ONE fsync for all concurrent callers
4. Leader publishes result + increments epoch
5. Followers observe epoch change and read shared result

### WAL Writer

**File:** `src/wal/fs/writer.rs`

```rust
pub struct Wal {
    path: PathBuf,
    inner: Mutex<WalInner>,
    sync_mode: WalSyncMode,
    group_commit: Option<Arc<BatchedSyncCoordinator>>,
    encoder: WalEncoder<DefaultBodyEncoder>,
    test_hooks: Option<TestHooks>,
}

struct WalInner {
    file: FsBufWriter,  // 128KB buffer
    pos: u64,
    scratch: Arena,     // 256KB reusable buffer
}
```

**Write Paths:**

| Path | Condition | Strategy |
|------|-----------|----------|
| Single record | `append_record()` | Encode → flush → vectored write |
| Small batch | `< 256KB` | Encode → scratch buffer → write_all |
| Large batch | `>= 256KB` | Encode → chunked vectored I/O |

**Sync Path:**
```rust
fn sync(&self) {
    let file_clone = { inner.file.flush()?; inner.file.try_clone()? };
    if let Some(coord) = &self.group_commit {
        coord.wait_for_sync(|| fs::sync_data_only(&file_clone, hooks))?;
    } else {
        fs::sync_data_only(&file_clone, hooks)?;
    }
}
```

### Assessment

| Aspect | Status |
|--------|--------|
| Encode throughput | ✅ Parallel encode scales well |
| Sync throughput | ✅ Group commit batches 100+ fsyncs into 1 |
| I/O efficiency | ✅ Zero-copy vectored I/O path |
| Allocations | ⚠️ No write-ahead buffer pooling |
| Async I/O | ⚠️ No io_uring integration |

---

## F. Memtable + SkipList

### MemTable

**File:** `src/core/memtable/core.rs`

```rust
#[derive(Clone)]
pub struct MemTable {
    inner: Arc<SkipList>,
    bytes: Arc<AtomicUsize>,
    range_tombstones: RangeTombstones,
}
```

**Key Operations:**

| Method | Description |
|--------|-------------|
| `get(key)` | Latest value with TTL check |
| `get_at(key, seq)` | Snapshot read (MVCC) |
| `put_owned_with_seq()` | Zero-copy hot path |
| `drain_with_meta_internal()` | Internal-key encoding for SST |
| `delete_range_with_seq()` | Range tombstone + immediate visibility |

**TTL Enforcement:**
```rust
fn get(&self, key: &[u8]) -> Option<Bytes> {
    match self.inner.get_visible_with_exp(key, u64::MAX) {
        Some(Some((v, exp))) => {
            if is_expired(exp) { None } else { Some(v) }
        }
        _ => None,
    }
}
```

### SkipList

**File:** `src/core/data_structures/skiplist.rs`

```rust
pub struct SkipList {
    head: Arc<Node>,
    top_level: AtomicUsize,
}

struct Node {
    key: Bytes,
    versions_head: Atomic<VersionNode>,  // Newest-first chain
    forward: [Atomic<Node>; MAX_LEVEL],  // 20 levels
    level: usize,
}

struct VersionNode {
    seq: u64,
    val: Option<Bytes>,
    exp: Option<u64>,
    op: OpType,
    next: Atomic<VersionNode>,
}

pub enum OpType { Put, Merge, Delete }
```

**Concurrency Model:**
- Lock-free via crossbeam-epoch for safe memory reclamation
- Readers are wait-free (epoch guard only)
- Writers use CAS on version chain head or tower pointers

**Snapshot Semantics:**
```rust
// LSM-style: visible if vn.seq < snapshot_seq (strictly less-than)
fn visible_version(versions_head, snapshot_seq, guard) -> Option<&VersionNode> {
    let mut v = versions_head.load(Acquire, guard);
    while let Some(vn) = v.as_ref() {
        if vn.seq < snapshot_seq { return Some(vn); }
        v = vn.next.load(Relaxed, guard);
    }
    None
}
```

**Insert Algorithm:**
1. Find predecessors/successors at all levels
2. If key exists: CAS new version onto version chain head
3. If key absent: allocate node with random level, CAS at level 0 (linearization point), then link higher levels

### Assessment

| Aspect | Status |
|--------|--------|
| Concurrency | ✅ Fully lock-free (no reader contention) |
| MVCC | ✅ Snapshot isolation via version chains |
| Memory safety | ✅ Epoch-based safe reclamation |
| Memory growth | ⚠️ No physical deletion until flush |
| Version chains | ⚠️ Can grow long under heavy updates |

---

## G. Known Issues / Performance Anomalies

### Documented Bugs

#### 1. Merge Operator Persistence Bug

**Location:** `tests/engine_merge_operators.rs:383,469`

**Symptom:**
- CloudBacked mode: merge operands return wrong value after restart
- Example: `merge("10") + merge("20")` → expected `"30"`, got `"20"`

**Root Cause:**
```
EntryMeta has op_type field but add_with_meta() only accepts tombstone boolean.
Merge operands are written as Put entries during flush, losing merge semantics.
```

**Fix Required:**
- Update SST writer API to accept OpType
- Write `entry_type=3` for merge operands
- Preserve merge chain across restarts

#### 2. Memory WAL Refactoring

**Location:** `src/wal/mem/shared.rs:5`

```rust
// TODO: Refactor to NoOpWal - an in-memory WAL defeats the purpose of durability.
```

#### 3. Stress Workload Flush Bug

**Location:** `tests/stress_workloads.rs:21`

```rust
// triggers a flush bug "Key ordering violation" - see engine_merge_operators bug
```

Related to merge operator persistence issue.

### Potential Issues from Code Audit

| Issue | Location | Severity |
|-------|----------|----------|
| No manifest compaction | `version_manager.rs` | Medium |
| Per-block file open | `sst/fs/iterator.rs` | Low |
| No io_uring | `wal/fs/writer.rs` | Low |
| Version chain growth | `skiplist.rs` | Low |

---

## Summary Assessment

| Component | Strength | Gap | Priority |
|-----------|----------|-----|----------|
| **MidgeOptions** | Good defaults, validation | Missing tuning knobs | Low |
| **Flush/Compact** | Clean actor pattern, error propagation | No rate limiting | Medium |
| **SST Writer** | Streaming, atomic write, crash-safe | Per-block file open | Low |
| **Manifest** | Lock-free reads, serial writes | No batching, no compaction | Medium |
| **WAL** | Parallel encode, group commit, vectored I/O | No io_uring | Low |
| **Memtable** | Lock-free skiplist, MVCC, epoch-based | Version chain growth | Low |
| **Known bugs** | Documented in tests | Merge operator persistence | **High** |

### Priority Fixes for World-Class Status

1. **Fix merge operator persistence** — SST entry_type must preserve OpType
2. **Add manifest edit batching** — Reduce disk writes during compaction
3. **Add io_uring write path** — Linux async I/O for WAL
4. **Implement compaction rate limiter** — Prevent I/O starvation
