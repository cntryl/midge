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
use cntryl_midge::{Engine, OpenOptions};

let engine = Engine::open(OpenOptions::local("./mydb").build())?;
```

### Configuration

`OpenOptions` uses named constructors for storage mode, then builder methods for tuning:

```rust
let opts = OpenOptions::local("./mydb")
    .goal(Goal::Throughput)           // Latency | Throughput | Economy
    .memory_budget(MemoryBudget::Auto) // Or MemoryBudget::Bytes(512_000_000)
    .workload(WorkloadProfile::Mixed)  // ReadMostly | WriteHeavy | Mixed | RangeScan
    .build();

let engine = Engine::open(opts)?;
```

**Configuration knobs:**

- `goal`: Optimization target (affects block sizes, compaction triggers)
- `memory_budget`: Total memory (Auto = ~512MB; explicit via Bytes(n))
- `workload`: Access pattern hint (affects cache allocation, bloom filters)

All low-level parameters are derived automatically from these high-level knobs.

## Storage Modes

Midge has three storage modes. **Choose via named constructor**—there are no defaults.

### InMemory

No persistence. Data lost on engine drop.

```rust
let engine = Engine::open(OpenOptions::in_memory().build())?;
```

**Use for:** Testing, benchmarks, ephemeral caches.

### Local

Persists to local filesystem.

```rust
let engine = Engine::open(OpenOptions::local("/var/lib/myapp/db").build())?;
```

**Use for:** Traditional deployments, single-node apps, durable local disk.

### Cloud

Persists to cloud object storage (S3, Azure, GCS, R2). Local disk is ephemeral cache only.

```rust
let opts = OpenOptions::cloud(
    "/tmp/cache",           // local cache path
    "my-bucket",            // bucket/container name
    "prod/instance-1/"      // object key prefix
).build();

let engine = Engine::open(opts)?;
```

**Use for:** Cloud-native deployments, serverless, when local disk can disappear.

**Authentication:** Uses standard environment variables (`AWS_ACCESS_KEY_ID`, etc.) and IAM roles. See [cloud-setup.md](cloud-setup.md).

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

**Snapshot isolation:** Captured at `begin_tx()`. All reads see consistent view at that seqno.

**Atomic commits:** All writes succeed or fail together. Single seqno for entire batch. Visibility is atomic.

## Write Operations

All writes happen on `Transaction` objects in ReadWrite mode.

### Put

Write a key-value pair:

```rust
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.put(b"user:42".to_vec(), b"alice".to_vec(), None)?;
engine.commit(tx, WriteOptions::buffered())?;
```

Optional TTL (third parameter):

```rust
let ttl_secs = Some(3600u64);  // expires in 1 hour
tx.put(b"session:xyz".to_vec(), b"data".to_vec(), ttl_secs)?;
```

### Delete

Remove a single key:

```rust
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.delete(b"user:42".to_vec())?;
engine.commit(tx, WriteOptions::sync())?;
```

Deletes are tombstones—removed during compaction.

### Delete Range

Remove all keys in range `[start, end)`:

```rust
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.delete_range(b"user:100".to_vec(), b"user:200".to_vec())?;
engine.commit(tx, WriteOptions::sync())?;
```

**Use for:** Bulk deletions, time-series cleanup, partition drops.
**Cost:** O(1) to write, O(N) at read time.

## Read Operations

### Get (Point Read)

Retrieve a single key:

```rust
let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
if let Some(value) = tx.get(b"user:42")? {
    println!("Found: {:?}", value);
}
```

Returns `Option<Bytes>`: `Some(value)` if exists and not expired, `None` otherwise.

### Scan (Range Query)

Iterate over keys using `Query` builder:

```rust
use cntryl_midge::Query;

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
    .reverse()
    .start_key(b"user:999".to_vec().into())
    .end_key(b"user:000".to_vec().into());
let mut iter = tx.scan(&query)?;
```

**Query methods:**
- `.prefix(bytes)`: Keys starting with prefix
- `.start_key(bytes)` / `.end_key(bytes)`: Range bounds (start inclusive, end exclusive)
- `.limit(n)`: Max results
- `.reverse()`: Reverse iteration (default is forward)

**Tip:** Use prefix scans and limits. Bloom filters help negative lookups.

## Durability Options

Every commit requires explicit `WriteOptions`. No defaults.

### sync()

Blocks until fsync completes. Write is durable when call returns.

```rust
engine.commit(tx, WriteOptions::sync())?;
```

**Use for:** Financial transactions, critical metadata, anything that cannot be lost.

### buffered()

Write accepted immediately, fsync batched in background.

```rust
engine.commit(tx, WriteOptions::buffered())?;
```

**Guarantees:** Visible immediately. Durable after background group commit. May lose <1s of writes on crash.

**Use for:** General workloads, high throughput. ~100x faster than `sync()`.

### best_effort()

**No durability.** Fastest. No WAL writes. Data lost on crash before flush.

```rust
engine.commit(tx, WriteOptions::best_effort())?;
```

**Use ONLY for:** Bulk loads, benchmark setup, test data.

**Safe pattern:**

```rust
// Load data fast (no durability)
for i in 0..100_000 {
    let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    tx.put(format!("key:{}", i).into_bytes(), b"value".to_vec(), None)?;
    engine.commit(tx, WriteOptions::best_effort())?;
}

// Make durable
engine.flush_cf(&cf)?;

// Now switch to buffered() or sync() for real workload
```

### cloud_strict()

Forces WAL upload, blocks until complete. Write is durable in cloud when call returns.

```rust
engine.commit(tx, WriteOptions::cloud_strict())?;
```

**Use ONLY when:** You need guaranteed cloud persistence before proceeding. Regular cloud mode uses background uploads.

## Column Families

Logical partitioning within a database. Separate keyspaces, independent compaction.

```rust
let cf1 = engine.create_column_family("users")?;
let cf2 = engine.create_column_family("sessions")?;

// Each transaction specifies its CF
let mut tx = engine.begin_tx(cf1.id(), TransactionMode::ReadWrite)?;
tx.put(b"user:42".to_vec(), b"alice".to_vec(), None)?;
engine.commit(tx, WriteOptions::sync())?;
```

**Default CF:** Always available:

```rust
let default_cf = engine.default_column_family();
```

**Operations:**

```rust
let cf = engine.get_column_family("users");  // Get by name
engine.flush_cf(&cf)?;                        // Flush to SST
let names = engine.list_column_families();    // List all
```

## Lifecycle Management

### Flushing

Force memtable to SST:

```rust
engine.flush_cf(&cf)?;
```

**When:** Before shutdown, after `best_effort()` loads, before backups. (Automatic flushes happen when memtable is full.)

### Compaction

Trigger manual compaction:

```rust
engine.compact_all()?;
```

**When:** After bulk deletes, maintenance windows. (Automatic compaction runs in background.)

### Shutdown

Recommended pattern:

```rust
// Flush all column families
for cf_name in engine.list_column_families() {
    if let Some(cf) = engine.get_column_family(&cf_name) {
        engine.flush_cf(&cf)?;
    }
}

drop(engine);  // Engine::drop() cleans up automatically
```

### Recovery

Reopeninig recovers automatically:

```rust
let engine = Engine::open(opts)?;  // Replays WAL, resumes from last checkpoint
```

See [recovery.md](recovery.md) for details.

## Error Handling

All operations return `MidgeResult<T>` (alias for `Result<T, MidgeError>`).

### Common Errors

**WriteStall:** Backpressure—memtable queue full.

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

**ReadOnlyTransaction:** Attempted write in readonly mode.

**ColumnFamilyNotFound:** Invalid CF name.

### Error Recovery

**Transient** (WriteStall): Retry with backoff, reduce write rate.

**Permanent** (IO, corruption): Log and propagate. May require restore from backup.

### Best Practices

**1. Always specify WriteOptions:**

```rust
engine.commit(tx, WriteOptions::buffered())?;  // No defaults - explicit choice
```

**2. Handle WriteStall with backoff:**

```rust
loop {
    match engine.commit(tx, opts) {
        Ok(_) => break,
        Err(MidgeError::WriteStall) => std::thread::sleep(Duration::from_millis(50)),
        Err(e) => return Err(e),
    }
}
```

**3. Flush before shutdown:**

```rust
engine.flush_cf(&cf)?;
```

**4. Use transactions for atomicity:**

```rust
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.put(b"account:1".to_vec(), b"-100".to_vec(), None)?;
tx.put(b"account:2".to_vec(), b"+100".to_vec(), None)?;
engine.commit(tx, WriteOptions::sync())?;  // Both succeed or both fail
```

## Advanced Topics

### Memory Management

```rust
let opts = OpenOptions::local("./db")
    .memory_budget(MemoryBudget::Bytes(512_000_000))  // 512MB
    .build();
```

**Budget distribution:** ~40% block cache, ~30% memtables, ~20% bloom filters, ~10% metadata.

### Observability

```rust
let metrics = engine.get_read_amp_metrics()?;
println!("Avg SSTs per read: {}", metrics.avg_ssts_per_read);  // lower is better
println!("L0 overlap rate: {}", metrics.l0_overlap_rate);      // higher = more compaction
```

### Performance Tuning

**Quick wins:**

- `Goal::Throughput` for write-heavy; `Goal::Latency` for read-latency-sensitive
- Larger `memory_budget` = better read performance
- `buffered()` instead of `sync()` when acceptable
- Batch 100-1000 ops per transaction

See [performance-tuning.md](performance-tuning.md) for details.

## Next Steps

- **Cloud deployments**: [cloud-setup.md](cloud-setup.md)
- **Recovery guarantees**: [recovery.md](recovery.md)
- **Architecture details**: [big-idea.md](big-idea.md)
- **Benchmarks**: [benchmarks.md](benchmarks.md)
