# Frequently Asked Questions

## General

### What is Midge?

Midge is an embedded LSM-tree key-value storage engine designed for predictable behavior and cloud-native deployments. It runs in-process as a library with explicit control over durability, memory, and lifecycle.

### When should I use Midge vs RocksDB?

**Use Midge when you need:**
- Predictable, deterministic behavior for testing/debugging
- Cloud object storage as primary durability target
- Synchronous APIs without async/await complexity
- Explicit control over state transitions

**Use RocksDB when you need:**
- Maximum raw throughput (500k-2M+ ops/sec)
- Battle-tested production stability
- Multi-threaded concurrent compaction
- Drop-in compatibility with existing RocksDB users

### Is Midge production-ready?

Yes. Midge core engine and all storage modes (InMemory, Local, and Cloud) are production-ready. See [../operations/cloud-setup.md](../operations/cloud-setup.md) for cloud provider setup details.

### What languages does Midge support?

Rust only. Native library written in Rust with Rust APIs. C bindings or FFI wrappers could be added in the future.

## Storage Modes

### Which storage mode should I use?

**InMemory**
- Use for: Testing, ephemeral caches, throwaway workloads
- Data lost when engine drops
- No filesystem access required

**Local**
- Use for: Traditional single-node deployments, on-premise servers
- Data persists to local disk
- Standard embedded database semantics

**Cloud**
- Use for: Cloud-native apps, serverless, distributed systems
- Cloud is source of truth (S3, GCS, Azure Blob)
- Local disk is ephemeral cache
- **Status**: Production-ready

### Can I migrate between storage modes?

Not directly. Storage modes have different durability semantics and file formats. To migrate:

1. Export data from old mode (iterate and write to file)
2. Create new engine with target mode
3. Import data into new mode

### Does Cloud mode work offline?

No. Cloud mode requires network connectivity to cloud storage. For offline operation, use Local mode.

### What cloud providers are supported?

Production-supported providers:
- AWS S3
- Azure Blob Storage
- Google Cloud Storage
- Cloudflare R2
- MinIO (S3-compatible)
- Any S3-compatible object storage

See [../operations/cloud-setup.md](../operations/cloud-setup.md) for provider configuration.

## Durability

### Which WriteOptions should I use?

**Quick reference:**

| Use Case | Recommended | Rationale |
|----------|-------------|-----------|
| Financial data | `sync()` | Zero tolerance for loss |
| User content | `buffered()` | Balance performance/durability |
| Logs/metrics | `buffered()` or `best_effort()` | Approximate data acceptable |
| Bulk import | `best_effort()` + `flush()` | Reloadable from source |
| Cache | `InMemory` | No persistence needed |

See [durability.md](durability.md) for detailed guarantees.

### What happens if I crash before flush with best_effort?

All `best_effort()` writes are lost. This mode skips WAL entirely. Only use when data can be reloaded from source.

### Can I mix WriteOptions in the same database?

Yes! Each transaction commit specifies its own WriteOptions. You can use `sync()` for critical writes and `buffered()` for normal writes in the same database.

## Performance

### What throughput can I expect?

Midge is designed for **predictability** not raw speed:

- **Write latency**: 1-10ms (depends on WriteOptions)
- **Read latency**: <1ms cached, 10-100ms cloud
- **Throughput**: ~50-75k ops/sec (typical workload: 1KB values, buffered mode; limited by WAL I/O and per-operation work)
- **Event loop**: 67M messages/sec (not the bottleneck)

For hundreds of thousands or millions of ops/sec, use RocksDB or a sharded design.

### How do I tune performance?

Midge uses automatic parameter derivation. Set high-level goals:

```rust
let opts = OpenOptions::local("./db")
    .goal(Goal::Latency)              // Low latency vs Throughput vs Economy
    .memory_budget(MemoryBudget::Auto) // Memory allocation
    .workload(WorkloadProfile::ReadMostly)  // Workload hint
    .build();
```

All low-level parameters (block sizes, memtable sizes, compaction triggers) are derived automatically.

See [../operations/performance-tuning.md](../operations/performance-tuning.md) for tuning guidance.

### Why is my write being stalled?

Write stalls occur when memtable queue is full (backpressure). This prevents memory exhaustion.

```rust
match engine.commit(tx, opts) {
    Err(MidgeError::WriteStall) => {
        // Wait for flush to complete
        std::thread::sleep(Duration::from_millis(100));
        // Retry...
    }
    Ok(_) => { /* success */ }
    Err(e) => { /* other error */ }
}
```

**Prevention:**
- Reduce write rate
- Increase memory budget
- Call `flush_cf()` manually for bulk loads

### How much memory does Midge use?

Memory usage is controlled by `MemoryBudget`:

```rust
// Auto: ~50% of available system memory
.memory_budget(MemoryBudget::Auto)

// Explicit: 1 GiB total allocation
.memory_budget(MemoryBudget::Bytes(1 << 30))
```

Memory is divided between:
- Block cache (~60%)
- Memtables (~30%)
- Metadata / overhead (~10%)

## Configuration

### What is Goal and how does it affect performance?

`Goal` determines optimization target for derived parameters:

**Goal::Latency** (default)
- Smaller block sizes (16 KiB)
- More aggressive bloom filters
- Lower compaction triggers
- Optimizes for p99 < 10ms

**Goal::Throughput**
- Larger block sizes (64 KiB)
- Larger memtables (256 MiB)
- Higher compaction concurrency
- Optimizes for MB/s bulk throughput

**Goal::Economy**
- Minimal cache allocation
- Higher compression
- Lower resource usage
- Optimizes for cost/memory usage

### What is WorkloadProfile?

WorkloadProfile provides hints for parameter derivation:

- `Mixed` (default): Balanced read/write
- `WriteHeavy`: >70% writes - larger memtables, more aggressive compaction
- `ReadMostly`: >70% reads - higher cache allocation, aggressive bloom filters
- `RangeScan`: Frequent range queries - larger blocks, less bloom filtering
- `TtlHeavy`: Frequent expirations - aggressive tombstone cleanup

### Can I override individual parameters?

Not currently. Midge uses smart defaults with automatic derivation. Future versions may expose advanced tuning knobs for expert users.

## Transactions

### Are transactions ACID?

Yes, within a single process:

- **Atomicity**: All writes in a transaction commit together or not at all
- **Consistency**: Transactions see consistent snapshots
- **Isolation**: Snapshot isolation - reads see stable view from transaction start
- **Durability**: Per WriteOptions (sync/buffered/best_effort/cloud_strict)

**Note:** Midge is single-process embedded storage. No distributed ACID across processes.

### Can I have long-running transactions?

Read-only transactions can be long-running. They hold a snapshot at a specific sequence number.

Read-write transactions should be short-lived. Commit before starting long-running work.

### What isolation level do transactions provide?

**Snapshot isolation** - reads within a transaction see a consistent view from transaction start. Written values become visible only after commit.

### Can multiple transactions run concurrently?

Yes. Multiple transactions can be in flight simultaneously. Commits are serialized through the actor event loop.

## Operations

### How do I backup a Midge database?

**Local mode:**
```bash
# Stop writes, flush, then backup files
tar -czf backup-$(date +%Y%m%d).tar.gz ./db/
```

**Cloud mode:**
```bash
# Backup cloud storage
aws s3 sync s3://my-bucket/db1/ ./backups/$(date +%Y%m%d)/
```

See [../operations/migration-guide.md](../operations/migration-guide.md) for backup strategies.

### How do I monitor Midge?

Expose metrics via engine APIs:

```rust
let metrics = engine.read_amplification_metrics(&cf)?;
println!("Avg SSTs per read: {}", metrics.avg_ssts_per_read);
println!("L0 overlap rate: {}", metrics.l0_overlap_rate);
```

**Key metrics:**
- Read amplification (SSTs touched per read)
- Memtable count and size
- SST count per level
- Flush/compaction frequency
- Cache hit rate

### How do I handle engine shutdown gracefully?

```rust
// 1. Stop accepting new writes
// 2. Flush all column families
for cf_name in engine.list_column_families() {
    if let Some(cf) = engine.get_column_family(&cf_name) {
        engine.flush_cf(&cf)?;
    }
}

// 3. Drop engine (releases locks, closes files)
drop(engine);
```

## Troubleshooting

### Why is recovery slow?

Recovery time depends on WAL size and manifest complexity:

- **Clean shutdown**: <100ms (no WAL to replay)
- **Dirty shutdown**: 1-10s (WAL replay required)
- **Cloud recovery**: 10-30s (download from cloud)

**Optimization:**
- Flush before shutdown (reduces WAL size)
- Use smaller memtables (more frequent flushes)

See [troubleshooting.md](troubleshooting.md) for debugging slow recovery.

### Why am I getting KeyNotFound errors?

Possible causes:

1. Key was never written
2. Key was deleted
3. Reading from wrong column family
4. TTL expired (key was evicted)
5. Transaction sees snapshot before write

### Why are my writes slow?

Check WriteOptions:

- `sync()`: Slow (~10ms) - every write fsynced
- `buffered()`: Fast (~1-5ms) - group commit batching
- `best_effort()`: Fastest (~0.1ms) - no WAL

Also check for write stalls (memtable queue full).

### Where can I get help?

- **Documentation**: Start with [overview.md](overview.md)
- **API reference**: See [api-guide.md](api-guide.md)
- **Troubleshooting**: See [troubleshooting.md](troubleshooting.md)
- **Issues**: File bug reports on GitHub

## Advanced Topics

### Can I run Midge in serverless environments?

Yes, with Cloud mode. Local disk is ephemeral cache only. Cloud storage is source of truth.

**Requirements:**
- Cloud storage credentials in environment
- Network access to cloud provider
- Ephemeral local storage for cache

**Current status**: Development/testing only.

### What compression algorithms are supported?

Midge supports pluggable compression:

- LZ4 (fast, moderate compression)
- Snappy (balanced)
- Zstd (high compression, slower)

Compression is automatically selected based on `Goal` (Economy uses higher compression).

### How does bloom filter tuning work?

Bloom filters are automatically configured based on:

- `Goal::Latency`: Aggressive bloom (10 bits/key, low false positive rate)
- `Goal::Economy`: Minimal bloom (5 bits/key, higher false positive rate)
- `WorkloadProfile::RangeScan`: Disabled (not useful for range queries)

### Can I disable compaction?

No. Compaction is essential for LSM-tree health (prevents unbounded L0 growth). Midge automatically manages compaction based on level triggers.

## Next Steps

- **Get started**: [quick-start.md](quick-start.md)
- **API reference**: [api-guide.md](api-guide.md)
- **Durability guide**: [durability.md](durability.md)
- **Troubleshooting**: [troubleshooting.md](troubleshooting.md)
- **Cloud setup**: [../operations/cloud-setup.md](../operations/cloud-setup.md)
- **Performance tuning**: [../operations/performance-tuning.md](../operations/performance-tuning.md)
