# API Guide

Comprehensive guide to using Midge in your application.

## Table of Contents

- [Opening a Database](#opening-a-database)
- [Storage Modes](#storage-modes)
- [Transactions](#transactions)
- [Write Operations](#write-operations)
- [Read Operations](#read-operations)
- [Durability Options](#durability-options)
- [Column Families](#column-families)
- [Lifecycle Management](#lifecycle-management)
- [Error Handling](#error-handling)

## Opening a Database

All database operations start with `Engine::open()`:

```rust
use cntryl_midge::{Engine, OpenOptions, Storage};

let opts = OpenOptions::new()
    .storage(Storage::Local { path: "./mydb".into() })
    .build();
    
let engine = Engine::open(opts)?;
```

### OpenOptions Builder

The builder provides explicit configuration without magic defaults:

```rust
let opts = OpenOptions::new()
    .storage(Storage::Local { path: "./mydb".into() })
    .goal(Goal::Throughput)              // Latency | Throughput | Economy
    .memory_budget(MemoryBudget::Auto)    // Or MemoryBudget::Bytes(512 * MB)
    .workload_profile(WorkloadProfile::Mixed)  // ReadHeavy | WriteHeavy | Mixed
    .build();
```

**Key parameters:**
- `goal`: Primary optimization target (affects block sizes, buffer sizes, cache allocation)
- `memory_budget`: Total memory available (Auto uses system memory with safety margins)
- `workload_profile`: Access pattern hint (affects prefetching, bloom filters, compaction)

All other parameters (block sizes, compaction triggers, cache ratios) are **derived automatically**.

## Storage Modes

Midge supports three explicit storage modes. Choose one based on your deployment requirements.

### InMemory

No persistence. Data lost when engine drops or process exits.

```rust
let opts = OpenOptions::new()
    .storage(Storage::InMemory)
    .build();
```

**Use for:**
- Testing
- Benchmarks
- Ephemeral caches
- Temporary workloads

### Local

Data persists to local filesystem. Classic embedded database model.

```rust
let opts = OpenOptions::new()
    .storage(Storage::Local { 
        path: "/var/lib/myapp/db".into() 
    })
    .build();
```

**Use for:**
- Traditional deployments
- Single-node applications
- When local disk is durable and reliable

### Cloud

Data persists to cloud object storage. Local disk is ephemeral cache.

```rust
let opts = OpenOptions::new()
    .storage(Storage::Cloud {
        local_cache_path: "/tmp/cache".into(),
        bucket: "my-app-database".to_string(),
        prefix: "prod/instance-1/".to_string(),
        endpoint: None,  // Or Some("https://s3.us-west-2.amazonaws.com")
        region: None,    // Or Some("us-west-2")
    })
    .build();
```

**Use for:**
- Cloud-native deployments
- Serverless applications
- Distributed systems
- When local disk may disappear without warning

**Cloud credentials:**
- Uses standard environment variables (AWS_ACCESS_KEY_ID, etc.)
- Supports IAM roles, instance profiles
- Provider-agnostic (S3, Azure, GCS, R2, MinIO)

See [cloud-setup.md](cloud-setup.md) for detailed cloud configuration.

## Transactions

All data operations happen inside explicit transactions. No auto-commit.

### Transaction Modes

**ReadOnly**: Snapshot-isolated reads, no writes allowed.

```rust
let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
let value = tx.get(b"key")?;
// tx.put(...) would return error
```

**ReadWrite**: Atomic writes with snapshot-based reads.

```rust
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.put(b"key1".to_vec(), b"value1".to_vec(), None)?;
tx.put(b"key2".to_vec(), b"value2".to_vec(), None)?;
tx.delete(b"key3".to_vec())?;
engine.commit(tx, WriteOptions::sync())?;
```

### Transaction Lifecycle

```
begin_tx → put/get/delete → commit (or drop to rollback)
```

**Snapshots:**
- Captured at `begin_tx()` time
- All reads see consistent view at that sequence number
- Writes are isolated until commit
- No long-lived locks

**Atomic commits:**
- All writes in a transaction succeed or fail together
- Single sequence number assigned to entire batch
- Visibility is atomic (other readers see all or none)

## Write Operations

All writes happen on `Transaction` objects in ReadWrite mode.

### Put

Write a key-value pair with optional TTL:

```rust
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;

// No expiration
tx.put(b"user:42".to_vec(), b"alice".to_vec(), None)?;

// Expires after 1 hour
let expiration = Some(std::time::Duration::from_secs(3600));
tx.put(b"session:xyz".to_vec(), b"data".to_vec(), expiration)?;

engine.commit(tx, WriteOptions::buffered())?;
```

### Delete

Remove a single key:

```rust
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.delete(b"user:42".to_vec())?;
engine.commit(tx, WriteOptions::sync())?;
```

**Note:** Delete is a tombstone. Key is marked deleted but not removed until compaction.

### Delete Range

Remove all keys in a range (start inclusive, end exclusive):

```rust
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.delete_range(
    b"user:100".to_vec(),  // start (inclusive)
    b"user:200".to_vec()   // end (exclusive)
)?;
engine.commit(tx, WriteOptions::sync())?;
```

**Use cases:**
- Bulk deletions (e.g., delete all sessions)
- Time-series data cleanup
- Partition drops

**Performance:** O(1) to write tombstone, O(N) at read time to check range.

## Read Operations

### Get (Point Read)

Retrieve a single key:

```rust
let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;

match tx.get(b"user:42")? {
    Some(value) => println!("Found: {:?}", value),
    None => println!("Not found"),
}
```

**Returns:** `Option<Bytes>`
- `Some(value)` if key exists and not expired
- `None` if key doesn't exist, was deleted, or is expired

### Scan (Range Query)

Iterate over keys using `Query` builder:

```rust
use cntryl_midge::{Query, Direction};

let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;

// All keys (forward)
let mut iter = tx.scan(&Query::new())?;
while let Some((key, value)) = iter.next() {
    println!("k={:?} v={:?}", key, value);
}

// Prefix scan
let query = Query::new().prefix(b"user:".to_vec().into());
let mut iter = tx.scan(&query)?;

// Range scan (start inclusive, end exclusive)
let query = Query::new()
    .start_key(b"user:100".to_vec().into())
    .end_key(b"user:200".to_vec().into());
let mut iter = tx.scan(&query)?;

// Limit results
let query = Query::new()
    .prefix(b"user:".to_vec().into())
    .limit(100);
let mut iter = tx.scan(&query)?;

// Reverse scan
let query = Query::new()
    .direction(Direction::Reverse)
    .start_key(b"user:999".to_vec().into())
    .end_key(b"user:000".to_vec().into());
let mut iter = tx.scan(&query)?;
```

**Query builder methods:**
- `.prefix(bytes)`: Match keys starting with prefix
- `.start_key(bytes)`: Range lower bound (inclusive)
- `.end_key(bytes)`: Range upper bound (exclusive)
- `.limit(n)`: Max results to return
- `.direction(Direction::Forward | Direction::Reverse)`: Iteration order

**Performance tips:**
- Use prefix scans when possible (better than full range)
- Set `.limit()` to avoid scanning entire keyspace
- Bloom filters accelerate negative lookups

## Durability Options

Every commit requires explicit `WriteOptions`. No defaults.

### sync()

**Full durability:** Blocks until fsync completes.

```rust
engine.commit(tx, WriteOptions::sync())?;
```

**Guarantees:**
- Write is durable when call returns
- Survives process crash, power loss
- Maximum latency (fsync blocks)

**Use for:**
- Financial transactions
- Critical metadata
- Anything that cannot be lost

### buffered()

**Deferred durability:** Write accepted, fsync happens asynchronously.

```rust
engine.commit(tx, WriteOptions::buffered())?;
```

**Guarantees:**
- Write is visible immediately
- Durability achieved via background group commit (batched fsync)
- If crash before batch fsync: data in memtable may be lost

**Use for:**
- General workloads
- High throughput requirements
- Acceptable to lose <1 second of writes on crash

**Performance:** ~100x faster than sync() due to batching.

### best_effort()

**No durability:** Fastest, no WAL writes.

```rust
engine.commit(tx, WriteOptions::best_effort())?;
```

**Guarantees:**
- Write is visible immediately
- NO durability: data lost on crash before flush
- Data becomes durable only after explicit `engine.flush_cf()`

**Use ONLY for:**
- Bulk data loads (setup phase)
- Benchmark initialization
- Test data
- Data that can be reloaded from source

**Safe pattern:**
```rust
// Phase 1: Fast load (no durability)
for i in 0..100_000 {
    let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    tx.put(format!("key:{}", i).into_bytes(), b"value".to_vec(), None)?;
    engine.commit(tx, WriteOptions::best_effort())?;
}

// Phase 2: Persist to SST
engine.flush_cf(&cf)?;

// Phase 3: Measured workload (with durability)
// ... use WriteOptions::buffered() or sync()
```

### cloud_strict()

**Immediate cloud durability:** Forces WAL seal + upload, blocks until complete.

```rust
engine.commit(tx, WriteOptions::cloud_strict())?;
```

**Guarantees:**
- Write is durable in cloud storage when call returns
- Local disk can be lost without data loss
- Highest latency (network round-trip)

**Use ONLY for:**
- Critical cloud-first deployments
- When local disk is known to be ephemeral
- Explicit cloud durability requirements

**Note:** Regular Cloud mode uses background uploads. Use `cloud_strict()` only when you need guaranteed cloud persistence before proceeding.

## Column Families

Column families provide logical partitioning within a single database.

### Creating Column Families

```rust
let cf1 = engine.create_column_family("users")?;
let cf2 = engine.create_column_family("sessions")?;
```

**Use cases:**
- Separate keyspaces with different access patterns
- Independent compaction schedules
- Different retention policies (TTLs)

### Using Column Families

Specify column family in every transaction:

```rust
// Write to users CF
let mut tx = engine.begin_tx(cf1.id(), TransactionMode::ReadWrite)?;
tx.put(b"user:42".to_vec(), b"alice".to_vec(), None)?;
engine.commit(tx, WriteOptions::sync())?;

// Read from sessions CF
let tx = engine.begin_tx(cf2.id(), TransactionMode::ReadOnly)?;
let value = tx.get(b"session:xyz")?;
```

### Default Column Family

Always available, no need to create:

```rust
let default_cf = engine.default_column_family();
let tx = engine.begin_tx(default_cf.id(), TransactionMode::ReadWrite)?;
```

### Column Family Operations

```rust
// Get by name
let cf = engine.get_column_family("users");

// Flush specific CF
engine.flush_cf(&cf)?;

// List all CFs
let cf_names = engine.list_column_families();
```

## Lifecycle Management

### Flushing

Force memtable flush to SST files:

```rust
engine.flush_cf(&cf)?;
```

**When to flush:**
- Before shutdown (ensure data persists)
- After bulk load with `best_effort()`
- Before taking backups
- To free memory

**Note:** Flushes happen automatically when memtable is full. Manual flush is optional but recommended for graceful shutdown.

### Compaction

Trigger compaction manually:

```rust
engine.compact_all()?;
```

**When to compact:**
- After bulk deletes (reclaim space)
- During maintenance windows
- To improve read performance

**Note:** Compaction happens automatically in background. Manual compaction is optional.

### Shutdown

Clean shutdown sequence:

```rust
// 1. Flush all data
for cf_name in engine.list_column_families() {
    if let Some(cf) = engine.get_column_family(&cf_name) {
        engine.flush_cf(&cf)?;
    }
}

// 2. Optional: compact to optimize on-disk state
engine.compact_all()?;

// 3. Drop engine (automatically closes resources)
drop(engine);
```

**Note:** Engine implements `Drop` and cleans up automatically. Explicit flush is recommended but not required.

### Reopening

Close and reopen same database:

```rust
drop(engine);  // Close

let engine = Engine::open(opts)?;  // Reopen - recovers from WAL and manifest
```

**Recovery:**
- WAL is replayed automatically
- Manifest tracks SST files
- Sequence numbers resume from last checkpoint

See [recovery.md](recovery.md) for recovery guarantees.

## Error Handling

All operations return `MidgeResult<T>` (alias for `Result<T, MidgeError>`).

### Common Errors

**WriteStall**: Backpressure signal, memtable queue full.

```rust
match engine.commit(tx, WriteOptions::sync()) {
    Ok(_) => println!("Committed"),
    Err(MidgeError::WriteStall) => {
        // Retry with exponential backoff
        std::thread::sleep(Duration::from_millis(100));
    }
    Err(e) => return Err(e),
}
```

**ColumnFamilyNotFound**: Invalid CF handle.

```rust
let cf = engine.get_column_family("nonexistent")
    .ok_or(MidgeError::ColumnFamilyNotFound("nonexistent".into()))?;
```

**ReadOnly**: Attempted write in ReadOnly transaction.

```rust
let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
match tx.put(b"key".to_vec(), b"value".to_vec(), None) {
    Err(MidgeError::ReadOnlyTransaction) => {
        // Transaction mode doesn't allow writes
    }
    _ => {}
}
```

### Error Recovery

**Transient errors** (WriteStall):
- Retry with backoff
- Reduce write rate
- Wait for compaction to catch up

**Permanent errors** (IO, Corruption):
- Log and propagate
- Consider database recovery
- May require restore from backup

### Best Practices

1. **Always specify WriteOptions explicitly**
   ```rust
   engine.commit(tx, WriteOptions::buffered())?;  // ✅ Good
   // No default - forces conscious choice
   ```

2. **Handle WriteStall gracefully**
   ```rust
   loop {
       match engine.commit(tx, opts) {
           Ok(_) => break,
           Err(MidgeError::WriteStall) => {
               std::thread::sleep(Duration::from_millis(50));
               // Consider exponential backoff
           }
           Err(e) => return Err(e),
       }
   }
   ```

3. **Flush before shutdown**
   ```rust
   engine.flush_cf(&cf)?;
   drop(engine);
   ```

4. **Use transactions for atomicity**
   ```rust
   // Atomic: both succeed or both fail
   let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
   tx.put(b"account:1".to_vec(), b"-100".to_vec(), None)?;
   tx.put(b"account:2".to_vec(), b"+100".to_vec(), None)?;
   engine.commit(tx, WriteOptions::sync())?;
   ```

5. **Choose appropriate storage mode**
   - Development: `InMemory`
   - Production (single node): `Local`
   - Production (cloud): `Cloud`

## Advanced Topics

### Memory Management

Memory usage is controlled by `MemoryBudget`:

```rust
let opts = OpenOptions::new()
    .memory_budget(MemoryBudget::Bytes(512 * 1024 * 1024))  // 512MB
    .build();
```

**Budget distribution:**
- ~40%: Block cache (hot SST blocks)
- ~30%: Write buffers (memtables)
- ~20%: Bloom filters
- ~10%: Metadata overhead

### Observability

Query runtime metrics:

```rust
let metrics = engine.get_read_amp_metrics()?;
println!("Average SSTs per read: {}", metrics.avg_ssts_per_read);
println!("L0 overlap rate: {}", metrics.l0_overlap_rate);
```

**Key metrics:**
- `avg_ssts_per_read`: Read amplification (lower is better)
- `l0_overlap_rate`: L0 compaction pressure (higher = more compaction needed)
- `sst_budget_violation_rate`: Fraction of reads exceeding SST budget

### Performance Tuning

See [performance-tuning.md](performance-tuning.md) for detailed tuning guide.

**Quick wins:**
- Use `Goal::Throughput` for write-heavy workloads
- Use `Goal::Latency` for read-latency-sensitive apps
- Set appropriate `memory_budget` (more cache = better reads)
- Use `buffered()` instead of `sync()` when acceptable
- Batch writes in transactions (100-1000 ops per commit)

## Next Steps

- **Cloud deployments**: [cloud-setup.md](cloud-setup.md)
- **Recovery guarantees**: [recovery.md](recovery.md)
- **Architecture details**: [big-idea.md](big-idea.md)
- **Benchmarks**: [benchmarks.md](benchmarks.md)
