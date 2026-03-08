# Architecture

Technical architecture guide for developers working on Midge.

## Table of Contents

- [Overview](#overview)
- [Module Structure](#module-structure)
- [Layer Dependencies](#layer-dependencies)
- [Core Components](#core-components)
- [Data Flow](#data-flow)
- [Threading Model](#threading-model)
- [Actor System](#actor-system)
- [Storage Subsystems](#storage-subsystems)
- [Key Abstractions](#key-abstractions)
- [Finding Things](#finding-things)

## Overview

Midge is structured as a layered embedded LSM engine with explicit dependencies between layers.

**Design principles:**
- **Actor-based concurrency**: Single EventLoop serializes state mutations
- **Explicit dependencies**: Lower layers never depend on higher layers
- **Deterministic execution**: Same inputs produce same state transitions
- **Testability**: Components are independently testable
- **Clear boundaries**: Each module has a single, well-defined responsibility

**Key architectural decisions:**
- Synchronous API (no async/await)
- Transaction-scoped operations (no auto-commit)
- Explicit durability choices (WriteOptions)
- Cloud-native storage (first-class, not bolted on)

See [the-big-idea.md](the-big-idea.md) for philosophy and rationale.

## Module Structure

```
src/
├── lib.rs                    # Public API surface
├── common/                   # Foundation types (zero dependencies)
│   ├── errors.rs            # Error types
│   ├── types.rs             # Sequence numbers, keys, values
│   └── result.rs            # Result type alias
│
├── io/                       # I/O abstractions (depends on common)
│   ├── file.rs              # File I/O traits
│   └── buffer.rs            # Buffered I/O
│
├── storage/                  # Storage backends (depends on io, common)
│   ├── local/               # Local filesystem storage
│   ├── cloud/               # Cloud object storage (S3, Azure, GCS)
│   └── hybrid/              # Hybrid storage (local cache + cloud)
│
├── wal/                      # Write-Ahead Log (depends on storage, io, common)
│   ├── writer.rs            # WAL writer
│   ├── reader.rs            # WAL reader
│   ├── policy.rs            # Durability policies
│   └── record.rs            # WAL record format
│
├── sst/                      # SSTable format (depends on storage, io, common)
│   ├── writer.rs            # SST builder
│   ├── reader.rs            # SST reader
│   ├── block.rs             # Block format (TLV encoding)
│   ├── index.rs             # Sparse index
│   ├── bloom.rs             # Bloom filters
│   ├── trie.rs              # Prefix trie
│   └── memtable.rs          # In-memory sorted table
│
├── metadata/                 # Manifest and version management
│   ├── manifest.rs          # Manifest writer/reader
│   ├── version.rs           # Version set (active SSTs)
│   └── compaction_log.rs    # Compaction intent log
│
├── iterators/                # Iterator abstractions
│   ├── merge.rs             # Merge iterator (multi-SST)
│   ├── memtable.rs          # Memtable iterator
│   └── filter.rs            # Filtering iterator
│
├── compaction/               # Compaction logic
│   ├── planner.rs           # Compaction planning
│   ├── executor.rs          # Compaction execution
│   └── policy.rs            # Leveled/tiered strategies
│
├── runtime/                  # Actor-based runtime
│   ├── event_loop.rs        # Central message dispatcher
│   ├── state.rs             # Runtime state (owned by EventLoop)
│   ├── actors/              # Actor implementations
│   │   ├── wal.rs           # WAL actor
│   │   ├── flush.rs         # Flush actor
│   │   ├── compaction.rs    # Compaction actor
│   │   ├── cloud.rs         # Cloud upload actor
│   │   ├── gc.rs            # Garbage collection actor
│   │   └── manifest.rs      # Manifest actor
│   ├── scheduler.rs         # Task scheduling
│   └── durability.rs        # Durability coordinator
│
├── engine/                   # Public API layer (depends on all below)
│   ├── mod.rs               # Engine implementation
│   ├── api/                 # Public API types
│   │   ├── transaction.rs   # Transaction API
│   │   ├── options.rs       # OpenOptions
│   │   ├── write_options.rs # WriteOptions
│   │   ├── query.rs         # Query builder
│   │   └── iterator.rs      # Scan iterator
│   ├── ingest.rs            # Write ingest batching
│   └── context.rs           # Transaction context
│
├── lease/                    # Exclusive access control
│   └── lease.rs             # File-based lease
│
├── metrics/                  # Observability
│   └── metrics.rs           # Runtime metrics
│
├── telemetry/                # Tracing and monitoring
│   ├── spans.rs             # Operation spans
│   └── metrics.rs           # Telemetry metrics
│
└── testkit/                  # Test utilities
    └── helpers.rs           # Test helpers
```

## Layer Dependencies

**Critical rule:** Lower layers MUST NOT depend on higher layers.

```
Layer 0: common/              (no dependencies)
         ↑
Layer 1: io/                  (depends on common)
         ↑
Layer 2: storage/             (depends on io, common)
         ↑
Layer 3: wal/, sst/           (depends on storage, io, common)
         ↑
Layer 4: metadata/, iterators/ (depends on sst, wal, storage, io, common)
         ↑
Layer 5: compaction/          (depends on metadata, sst, storage, io, common)
         ↑
Layer 6: runtime/             (depends on all below)
         ↑
Layer 7: engine/              (depends on all below, public API)
```

**Validation:**

- CI runs `cargo clippy --all-targets -- -D warnings`, `cargo build`, and `cargo test`.
- The optional validator script checks test naming and AAA markers:

    ```bash
    python ./scripts/validate_tests.py --summary
    ```

**Why this matters:**
- Changes to `common/` affect everything (be careful)
- Changes to `engine/` affect nothing below (safe to iterate)
- Each layer can be tested in isolation

## Core Components

### Engine (Public API)

**Location:** `src/engine/mod.rs`

**Responsibilities:**
- Public API surface (open, begin_tx, commit, flush_cf)
- Transaction lifecycle management
- RuntimeHandle ownership (message passing to EventLoop)
- Column family registry

**Key methods:**
- `Engine::open(opts)` → Opens database, starts runtime, acquires lease
- `engine.begin_tx(cf_id, mode)` → Creates transaction with snapshot
- `engine.commit(tx, opts)` → Commits transaction with durability choice
- `engine.flush_cf(cf)` → Forces memtable flush to SST

**Thread safety:**
- Engine is `Send + Sync`
- Can be shared across threads (via Arc)
- All mutations go through runtime (serialized)

### Transaction

**Location:** `src/engine/api/transaction.rs`

**Responsibilities:**
- Scoped read/write API
- Snapshot isolation (reads at specific sequence number)
- Write buffering (accumulates writes, commits atomically)

**Modes:**
- `ReadOnly`: Snapshot reads, no writes
- `ReadWrite`: Snapshot reads + buffered writes

**Key methods:**
- `tx.put(key, value, ttl)` → Buffers write
- `tx.get(key)` → Reads from snapshot
- `tx.delete(key)` → Buffers delete
- `tx.scan(query)` → Returns iterator

**Lifecycle:**
```
begin_tx → put/get/delete → commit (or drop to rollback)
```

### EventLoop (Runtime Core)

**Location:** `src/runtime/event_loop.rs`

**Responsibilities:**
- Central message dispatcher
- Owns `RuntimeState` (all mutable state)
- Routes messages to actors
- Enforces single-threaded state mutation

**Message flow:**
```
Engine → RuntimeHandle.send(msg) → EventLoop → Actor → State mutation → Response
```

**Thread model:**
- Runs on dedicated thread
- Synchronous execution (no async)
- Messages processed serially (no parallelism)

**Key messages:**
- `RuntimeMsg::ApplyTransaction` → Commit transaction
- `RuntimeMsg::WalSync` → Fsync WAL
- `RuntimeMsg::FlushMemtable` → Trigger flush
- `RuntimeMsg::CompactLevel` → Trigger compaction

### RuntimeState

**Location:** `src/runtime/state.rs`

**Responsibilities:**
- Owns all mutable engine state
- Sequence number allocation
- Memtable management (active + immutable queue)
- Version set (active SSTs per level)
- In-flight compaction tracking

**Key state:**
```rust
pub struct RuntimeState {
    pub sequence: u64,                  // Current sequence number
    pub memtables: MemtableState,       // Active + immutable memtables
    pub version: VersionSet,            // Active SSTs per level
    pub wal: WalState,                  // WAL writer + sequence tracking
    pub compaction: CompactionState,    // In-flight compactions
    pub manifest: ManifestState,        // Manifest writer
}
```

**State transitions:**
- All mutations happen via actor messages
- Deterministic (same messages → same state)
- Loggable and replayable

### Actors

**Location:** `src/runtime/actors/`

Actors are **stateless handlers** that mutate `RuntimeState` and return updates.

**Key actors:**

| Actor | Responsibility | Key Operations |
|-------|---------------|----------------|
| **WalActor** | WAL writes, fsync | `append()`, `sync()`, `rotate()` |
| **FlushActor** | Memtable → SST | `flush()`, `build_sst()` |
| **CompactionActor** | SST merging | `plan()`, `execute()`, `commit()` |
| **CloudActor** | Cloud uploads | `upload_wal()`, `upload_sst()` |
| **GcActor** | Old SST cleanup | `collect()`, `delete_obsolete()` |
| **ManifestActor** | Manifest updates | `record_flush()`, `record_compaction()` |

**Actor pattern:**
```rust
pub trait Actor {
    fn handle(&self, state: &mut RuntimeState, msg: Message) -> Result<Response>;
}
```

**Why actors:**
- No shared mutable state (state is passed in)
- Testable (actors are pure functions of state + message)
- Deterministic (same state + message → same result)

### IngestCoordinator

**Location:** `src/engine/ingest.rs`

**Responsibilities:**
- Batches writes for throughput
- Accumulates up to 1024 ops, 4MB, or 500μs
- Sends batched `ApplyTransaction` to runtime
- Returns results to individual callers

**Why batching:**
- Amortizes message-passing overhead
- Better cache locality
- Allows group commit (multiple writes, single fsync)

**Bypassed for:**
- CloudAsync mode (cloud has own batching)
- DeleteRange operations (rare, go direct)

## Data Flow

### Write Path

```
┌─────────────┐
│ Application │
└──────┬──────┘
       │ tx.put(key, value)
       ▼
┌──────────────┐
│ Transaction  │ (buffer writes)
└──────┬───────┘
       │ engine.commit(tx, WriteOptions::buffered())
       ▼
┌───────────────┐
│ IngestCoord   │ (batch up to 1024 ops / 4MB / 500μs)
└──────┬────────┘
       │ RuntimeMsg::ApplyTransaction
       ▼
┌──────────────┐
│  EventLoop   │ (dispatch to actors)
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   WalActor   │ (append to WAL)
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   Memtable   │ (in-memory sorted)
└──────────────┘
```

**Async background work:**
- Group commit fsync (batched)
- Memtable flush (when full)
- Compaction (when level thresholds met)
- Cloud upload (if Cloud mode)

### Read Path

```
┌─────────────┐
│ Application │
└──────┬──────┘
       │ tx.get(key)
       ▼
┌──────────────┐
│ Transaction  │ (snapshot isolation)
└──────┬───────┘
       │ sequence number
       ▼
┌──────────────────┐
│ 1. Active        │ (check current memtable)
│    Memtable      │
└──────┬───────────┘
       │ miss
       ▼
┌──────────────────┐
│ 2. Immutable     │ (check flushing memtables)
│    Memtables     │
└──────┬───────────┘
       │ miss
       ▼
┌──────────────────┐
│ 3. Block Cache   │ (check cached SST blocks)
└──────┬───────────┘
       │ miss
       ▼
┌──────────────────┐
│ 4. SST Files     │ (binary search, bloom filter)
│    (L0 → LN)     │
└──────────────────┘
```

**Read optimization:**
- Bloom filters (skip SSTs without key)
- Block cache (hot blocks in memory)
- Sparse index (skip blocks)
- Prefix trie (accelerate prefix scans)

### Flush Path

```
Memtable full → RuntimeMsg::FlushMemtable
                       ↓
                 FlushActor::flush()
                       ↓
              Sort entries by key
                       ↓
              Build SST blocks
                       ↓
              Write to storage
                       ↓
              Update manifest
                       ↓
              Add to version set
                       ↓
              Delete memtable
```

**SST output:**
- TLV-encoded blocks (4KB-64KB)
- Sparse index (every Nth key)
- Bloom filter (per SST)
- Optional trie (prefix acceleration)

### Compaction Path

```
Level threshold met → RuntimeMsg::CompactLevel
                            ↓
                   CompactionActor::plan()
                            ↓
              Select overlapping SSTs
                            ↓
              Merge iterators (N-way)
                            ↓
              Write output SSTs
                            ↓
              Update manifest (atomic)
                            ↓
              Delete input SSTs
```

**Compaction strategies:**
- Leveled (default): Minimize read amplification
- Tiered: Minimize write amplification
- Hybrid: Balance both

## Threading Model

Midge uses a **hybrid threading model**:

### Main Threads

1. **EventLoop thread** (runtime)
   - Owns all mutable state
   - Processes messages serially
   - Dispatches to actors
   - No parallelism (single-threaded)

2. **IngestCoordinator thread** (optional)
   - Batches writes
   - Runs async if enabled
   - Sends to EventLoop

3. **Application threads** (caller threads)
   - Call Engine API (put, get, commit)
   - Block on RuntimeHandle::send_and_wait()
   - Wake up when EventLoop responds

### Background Work (Thread Pool)

- **Flush tasks**: Build SSTs in background
- **Compaction tasks**: Merge SSTs in background
- **Cloud upload tasks**: Upload to S3/Azure/GCS
- **GC tasks**: Delete obsolete SSTs

**Key insight:**
- State mutation is single-threaded (EventLoop)
- I/O work is parallel (thread pool)
- No locks on critical path (actor serialization)

### Synchronization

**RuntimeHandle** (message passing):
```rust
pub struct RuntimeHandle {
    tx: Sender<RuntimeMsg>,
    rx: Receiver<RuntimeResponse>,
}

impl RuntimeHandle {
    pub fn send_and_wait(&self, msg: RuntimeMsg) -> RuntimeResponse {
        self.tx.send(msg)?;
        self.rx.recv()  // Blocks until EventLoop responds
    }
}
```

**No locks on state:**
- RuntimeState owned by EventLoop
- No Arc<Mutex<State>>
- No data races (single thread mutates)

## Actor System

### Message Flow

```
Caller thread:
    handle.send_and_wait(msg) → blocks
                 ↓
EventLoop thread:
    recv(msg) → dispatch_to_actor(msg)
                     ↓
    actor.handle(&mut state, msg) → state mutation
                     ↓
    respond(result) → wakes caller
                 ↓
Caller thread:
    returns with result
```

### Actor Lifecycle

Actors are **stateless** and **reentrant**:

```rust
impl WalActor {
    pub fn append(
        &self,
        state: &mut RuntimeState,  // State passed in
        ops: Vec<Operation>
    ) -> MidgeResult<u64> {
        // 1. Mutate state
        let seq = state.allocate_sequences(ops.len());
        
        // 2. Write to WAL
        state.wal.writer.append(&ops)?;
        
        // 3. Apply to memtable
        for op in ops {
            state.memtables.active.insert(seq, op);
        }
        
        // 4. Return result
        Ok(seq)
    }
}
```

**Key properties:**
- No actor-local state
- Pure function: `(State, Message) → (State', Result)`
- Testable: Mock state, call handler, assert state changes

### Task Scheduling

**Scheduler** decides what to run next:

```rust
pub enum SchedulerDecision {
    RunTask(Task),
    Idle,
    Shutdown,
}

pub trait Scheduler {
    fn next(&self, state: &RuntimeState) -> SchedulerDecision;
}
```

**Priority:**
1. Flush (if memtable full)
2. Compaction (if level threshold met)
3. GC (if obsolete SSTs accumulate)
4. Cloud upload (background)

**Determinism:**
- Same state → same decision
- Reproducible execution
- Testable (inject scheduler)

## Storage Subsystems

### WAL (Write-Ahead Log)

**Purpose:** Durability before memtable flush

**Format:**
```
[Header: magic, version]
[Record: checksum, length, type, seqno, key, value]
[Record: ...]
[Footer: final checksum]
```

**Operations:**
- `append(record)` → Write record to log
- `sync()` → Fsync to disk
- `rotate()` → Start new segment
- `replay()` → Read records on recovery

**Policies** (when to fsync / what frontier they use):
- `Strict`: After every write
- `Batched`: Periodic local fsync / group commit
- `CloudAsync`: Visible after the local append barrier, cloud durability later
- `BestEffort`: Never (no WAL)

### SST (Sorted String Table)

**Purpose:** Immutable on-disk sorted data

**Structure:**
```
┌──────────────────┐
│ Data Block 1     │ ← 4KB-64KB of sorted key-value pairs
├──────────────────┤
│ Data Block 2     │
├──────────────────┤
│ ...              │
├──────────────────┤
│ Sparse Index     │ ← Every Nth key → block offset
├──────────────────┤
│ Bloom Filter     │ ← Probabilistic membership test
├──────────────────┤
│ Trie (optional)  │ ← Prefix acceleration
├──────────────────┤
│ Footer           │ ← Metadata offsets, checksum
└──────────────────┘
```

**TLV Block Encoding:**
```
[Tag: type, flags]
[Length: varint]
[Value: data]
[Checksum: CRC32]
```

**Benefits:**
- Extensible (new block types)
- Compressed (per-block compression)
- Verified (checksums)

### Memtable

**Purpose:** In-memory write buffer

**Implementation:** Skip list or B-tree (sorted by key)

**States:**
- `Active`: Receives new writes
- `Immutable`: Full, awaiting flush
- `Flushing`: Being written to SST

**Rotation:**
```
Active full (64MB) → becomes Immutable → schedule flush
                     New Active created
```

### Block Cache

**Purpose:** Hot SST blocks in memory

**Implementation:** LRU cache

**Key:** `(SST file ID, block offset)`  
**Value:** Decompressed block bytes

**Size:** Configurable via `MemoryBudget` (Auto uses a fraction of effective memory limits)

### HybridStorage (Cloud Mode)

**Purpose:** Local cache + cloud persistence

**Layers:**
```
┌─────────────────────┐
│   Application       │
└──────────┬──────────┘
           ▼
    ┌──────────────┐
    │ Block Cache  │ (in-memory)
    └──────┬───────┘
           ▼
    ┌──────────────┐
    │ Local Cache  │ (NVMe/SSD)
    └──────┬───────┘
           ▼
    ┌──────────────┐
    │ Cloud Object │ (S3/Azure/GCS)
    │   Storage    │ ◄── Source of truth
    └──────────────┘
```

**Operations:**
- `read_block()`: Check cache → download if miss → cache locally
- `write_sst()`: Write local → upload async → update manifest
- `evict()`: Delete local cached blocks (space pressure)

## Key Abstractions

### Sequence Numbers

**Type:** `u64`

**Purpose:** Total order of all operations

**Properties:**
- Monotonically increasing
- Never reused
- Allocated by EventLoop (single source)
- Used for snapshot isolation

**Visibility:**
```rust
pub fn is_visible(op_seq: u64, snapshot_seq: u64) -> bool {
    op_seq <= snapshot_seq
}
```

### ColumnFamilyId

**Type:** `u32`

**Purpose:** Logical keyspace partitioning

**Registry:**
```rust
pub struct Engine {
    cf_registry: DashMap<u32, ColumnFamilyHandle>,
}
```

**Why separate:**
- Independent compaction schedules
- Different retention policies (TTLs)
- Access pattern isolation

### Query Builder

**Purpose:** Fluent API for range scans

**Example:**
```rust
let query = Query::new()
    .prefix(b"user:".into())
    .limit(100)
    .direction(Direction::Forward);

let iter = tx.scan(&query)?;
```

**Optimizations:**
- Prefix scans use trie
- Range scans use sparse index
- Bloom filters skip SSTs

### WriteOptions vs WAL Policy

**Confusion alert:** These are separate concepts.

**WriteOptions** (API level):
- Controls when `commit()` returns
- `sync()`: Block until durable
- `buffered()`: Return immediately
- `best_effort()`: Skip WAL

**WAL DurabilityPolicy** (runtime level):
- Controls the local/cloud durability frontier
- `Strict`: Every write
- `Batched`: Group commit
- `CloudAsync`: Local append visibility now, cloud durability later

**Relationship:**
```rust
// API choice (caller waits?)
engine.commit(tx, WriteOptions::sync())?;

// Runtime policy (when fsync?)
OpenOptions::new()
    .wal_durability_policy(DurabilityPolicy::Batched)  // Internal
```

Most users only see WriteOptions.

## Finding Things

### "Where do I find...?"

**Opening a database:**
- `src/engine/mod.rs`: `Engine::open()`
- `src/engine/api/options.rs`: `OpenOptions`

**Transaction API:**
- `src/engine/api/transaction.rs`: `Transaction`, `put()`, `get()`, `commit()`

**Write durability:**
- `src/engine/api/write_options.rs`: `WriteOptions`
- `src/wal/policy.rs`: `DurabilityPolicy`
- `src/runtime/durability.rs`: Group commit coordinator

**WAL implementation:**
- `src/wal/writer.rs`: WAL writing
- `src/wal/reader.rs`: WAL replay
- `src/runtime/actors/wal.rs`: WAL actor

**SST implementation:**
- `src/sst/writer.rs`: SST building
- `src/sst/reader.rs`: SST reading
- `src/sst/block.rs`: TLV block format
- `src/sst/bloom.rs`: Bloom filter
- `src/sst/trie.rs`: Prefix trie

**Flush logic:**
- `src/runtime/actors/flush.rs`: Flush actor
- `src/sst/memtable.rs`: Memtable

**Compaction logic:**
- `src/runtime/actors/compaction.rs`: Compaction actor
- `src/compaction/planner.rs`: Level selection
- `src/compaction/executor.rs`: SST merging

**Cloud storage:**
- `src/storage/cloud/`: S3/Azure/GCS backends
- `src/storage/hybrid/`: Hybrid storage (cache + cloud)
- `src/runtime/actors/cloud.rs`: Cloud upload actor

**Recovery:**
- `src/runtime/event_loop.rs`: Recovery on startup
- `src/metadata/manifest.rs`: Manifest replay
- `src/wal/reader.rs`: WAL replay

**Metrics:**
- `src/metrics/`: Runtime metrics
- `src/telemetry/`: Tracing and spans

**Tests:**
- `tests/`: Integration tests
- `src/*/tests.rs`: Unit tests (inline)
- `benches/`: Criterion benchmarks

### "How do I modify...?"

**Add new WriteOptions variant:**
1. `src/engine/api/write_options.rs`: Add variant
2. `src/runtime/actors/wal.rs`: Handle in `append()`
3. `src/runtime/durability.rs`: Update coordinator
4. `docs/user-guides/api-guide.md`: Document behavior
5. `docs/user-guides/durability.md`: Document guarantees

**Add new storage backend:**
1. `src/storage/`: Create new module
2. Implement `Storage` trait
3. Add to `OpenOptions::storage`
4. Update `docs/operations/cloud-setup.md`

**Add new SST metadata:**
1. `src/sst/block.rs`: Define TLV tag
2. `src/sst/writer.rs`: Write block
3. `src/sst/reader.rs`: Read block
4. Update tests

**Add new actor:**
1. `src/runtime/actors/`: Create actor module
2. Implement message handler
3. Add to `RuntimeMsg` enum
4. Dispatch in `event_loop.rs`
5. Add tests

## Development Workflow

### Running Tests

```bash
# All tests
cargo test

# Specific test
cargo test should_recover_after_crash

# Integration tests only
cargo test --test engine_basic

# Benchmarks
cargo bench --bench tier1_hotpath_api
```

### Test Validation

```bash
# Enforce test naming and structure
python ./scripts/validate_tests.py --summary

# Generate test inventory
python ./scripts/generate_inventory.py
```

### Linting

```bash
# Fix all clippy warnings before committing
cargo clippy --all-targets --fix

# Check without fixing
cargo clippy --all-targets
```

### Documentation

```bash
# Generate API docs
cargo doc --no-deps --open

# Check doc comments
cargo doc --document-private-items
```

## Common Patterns

### Adding a New Operation

1. **Add to Transaction API:**
   ```rust
   // src/engine/api/transaction.rs
   pub fn my_operation(&mut self, arg: Foo) -> MidgeResult<Bar> {
       self.check_mode()?;
       // Add to write buffer
       self.writes.push(WriteIntent::MyOp { arg });
       Ok(bar)
   }
   ```

2. **Add to RuntimeMsg:**
   ```rust
   // src/runtime/mod.rs
   pub enum RuntimeMsg {
       MyOperation { arg: Foo },
   }
   ```

3. **Dispatch in EventLoop:**
   ```rust
   // src/runtime/event_loop.rs
   RuntimeMsg::MyOperation { arg } => {
       let result = self.my_actor.handle(&mut self.state, arg)?;
       self.respond(request_id, RuntimeResponse::MyResult(result));
   }
   ```

4. **Implement Actor Logic:**
   ```rust
   // src/runtime/actors/my_actor.rs
   pub fn handle(&self, state: &mut RuntimeState, arg: Foo) -> MidgeResult<Bar> {
       // Mutate state
       state.my_field = arg.process();
       Ok(bar)
   }
   ```

5. **Add Tests:**
   ```rust
   #[test]
   fn should_handle_my_operation_when_called() {
       // Arrange
       let engine = Engine::open(test_opts())?;
       
       // Act
       let result = engine.my_operation(arg)?;
       
       // Assert
       assert_eq!(result, expected);
   }
   ```

### Performance Investigation

1. **Profile with Criterion:**
   ```bash
   cargo bench --bench tier1_hotpath_api -- --profile-time 10
   ```

2. **Check metrics:**
   ```rust
   let metrics = engine.get_read_amp_metrics()?;
   println!("Avg SSTs per read: {}", metrics.avg_ssts_per_read);
   ```

3. **Trace with spans:**
   ```rust
   // src/telemetry/spans.rs
   let span = Span::new(OperationType::Get);
   // ... operation ...
   span.end();
   ```

4. **Analyze flamegraph:**
   ```bash
   cargo flamegraph --bench tier1_hotpath_api
   ```

## Next Steps

- **Philosophy**: [the-big-idea.md](the-big-idea.md)
- **API usage**: [../user-guides/api-guide.md](../user-guides/api-guide.md)
- **Testing guide**: [testing.md](testing.md)
- **Benchmark guide**: [benchmarks.md](benchmarks.md)


