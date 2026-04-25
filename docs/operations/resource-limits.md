# Resource Limits and Graceful Degradation

**Managing Midge in constrained environments**

## Overview

Midge implements comprehensive resource management to ensure stable operation in both resource-rich and resource-constrained environments. The engine follows a **"never fall down"** design principle: when resources are plentiful, performance is maximized; when resources are constrained, performance degrades gracefully rather than failing catastrophically.

## Thread Management

### Thread Budget

Midge spawns background threads for various subsystems. Understanding the thread budget helps prevent exhaustion on platforms with thread limits.

**Platform Limits:**
- Windows: ~2,000 threads per process (practical limit)
- Linux: ~32,000 threads per process (ulimit dependent)

**Per-Engine Thread Usage:**

| Component | Threads | Purpose | Spawn Condition |
|-----------|---------|---------|-----------------|
| BlockCache (per instance) | 16 | Cache admission workers (1 per shard) | Always |
| WAL Writer | 1 | Background write coalescing | Always |
| HybridStorage Upload Worker | 1 | CloudAsync WAL upload pipeline | Cloud storage mode |
| CloudExecutor Runtime | ~4-8 | Embedded tokio runtime for cloud I/O | Cloud storage mode |
| Compaction Runtime | 4 | Parallel compaction jobs | Configurable |
| Metadata Writer | 1 | Manifest persistence | Always |

**Example: Multiple Engine Instances:**
```rust
// Each engine spawns ~25-35 threads (local mode) or ~30-45 threads (cloud mode)
let engine1 = Engine::open(opts1)?;  // +30 threads
let engine2 = Engine::open(opts2)?;  // +30 threads
let engine3 = Engine::open(opts3)?;  // +30 threads
// Total: ~90-135 threads for 3 engines
```

**Recommendation:** On Windows, limit to ~50-60 engine instances per process to stay under the 2,000 thread limit.

### Thread Spawn Failure Handling

When thread spawn fails (typically from OS resource exhaustion), Midge falls back to inline processing:

**BlockCache Admission:**
```rust
// Normal: Background worker processes admission queue
cache.put(key, value);  // Enqueues to admission worker

// Fallback: Inline admission on spawn failure
cache.put(key, value);  // Processes admission immediately (adds ~1-5µs latency)
```

**HybridStorage Upload:**
```rust
// Normal: Background worker uploads WAL to cloud
storage.write(data);  // Enqueues to upload worker

// Fallback: Inline upload on spawn failure
storage.write(data);  // Uploads synchronously (adds ~10-50ms latency)
```

**Monitoring:**
Use `engine.get_runtime_metrics()?`, `engine.get_read_amp_metrics()?`, and `engine.get_storage_layout()?` for stable operator-facing snapshots. Internal fallback counters for thread-spawn failures are not currently exposed as public API.

### Thread Cleanup

All background threads are explicitly joined on `Drop` with generous timeouts to prevent resource leaks:

**Cleanup Timeouts:**
- BlockCache workers: 30 seconds
- HybridStorage upload worker: 30 seconds
- CloudExecutor runtime: 10 seconds
- WAL writer: 30 seconds

If a thread doesn't join within the timeout, a warning is logged but the Drop continues to prevent hangs.

## Memory Management

### WAL Buffer Pool

The WAL writer maintains a bounded buffer pool to prevent unbounded memory growth during backpressure:

**Configuration:**
```rust
// Internal constant in src/wal/fs/writer_runner.rs
const MAX_BUFFER_POOL_SIZE: usize = 64;  // 64 buffers × ~4KB = ~256KB max
const MAX_QUEUE_DEPTH: usize = 5000;     // Max pending writes
```

**Behavior:**
- Pool starts empty and grows up to 64 buffers as writes arrive
- When pool is full, oldest buffers are dropped instead of pooled
- The overflow counter is currently an internal telemetry metric, not part of the stable public API
- Backpressure triggers when queue depth exceeds 5,000 entries

**Monitoring:**
Use `engine.get_runtime_metrics()?` to watch `wal_pending_writes`, `write_stalled`, and compaction backlog. Those public counters tell you when the write path is under sustained pressure even though the buffer-pool overflow counter itself is internal.

**Tuning:** If overflow is frequent, consider:
1. Increasing flush rate (reduce memtable size)
2. Using larger value sizes to reduce write count
3. Batching writes to reduce queue pressure

### Cache Memory

BlockCache uses a strict capacity limit configured via `OpenOptions`:

```rust
let opts = OpenOptions::local("./db")
    .memory_budget(MemoryBudget::Bytes(512 << 20))  // 512 MiB total
    .build();
// BlockCache gets ~60% = ~307 MiB (Goal::Latency)
// BlockCache gets ~30% = ~154 MiB (Goal::Economy)
```

**Enforcement:**
- Cache evicts entries when capacity is exceeded (LRU policy)
- Admission control prevents low-value entries from entering
- No unbounded growth - strict memory limit

### Memtable Memory

Memtables are bounded by configuration:

```rust
// Memtable size derived from MemoryBudget
// Example: 512 MiB budget with Goal::Latency
// → Memtable size: ~51 MiB (10% of budget)
// → Max memtables: 2 active + 2 pending flush = 4 × 51 MiB = 204 MiB max
```

**Backpressure:** When memtables are full and compaction can't keep up:
1. Commits can return `MidgeError::WriteStall`
2. `wait_for_write_stall_clear()` can be used to wait for recovery
3. No unbounded memory growth - the engine applies backpressure instead

## Performance Characteristics

### Hot Path Overhead

Resource management introduces minimal hot path overhead:

**Cache Admission Check:**
```rust
// Single atomic load with Relaxed ordering
if self.admission_inline.load(Ordering::Relaxed) {
    self.put_inline(key, value);  // Inline fallback
} else {
    self.admission_tx.send((key, value));  // Enqueue to worker
}
```
**Overhead:** <10 nanoseconds (single atomic load, no contention)

**Upload Worker Check:**
```rust
// Single atomic load with Relaxed ordering
if self.upload_worker_failed.load(Ordering::Relaxed) {
    self.process_upload_inline(batch);  // Inline fallback
} else {
    self.upload_tx.send(batch);  // Enqueue to worker
}
```
**Overhead:** <10 nanoseconds (single atomic load, no contention)

### Graceful Degradation

When resources are constrained, Midge degrades gracefully:

**Scenario: Thread Spawn Failure**

| Component | Normal Performance | Degraded Performance | Correctness |
|-----------|-------------------|---------------------|-------------|
| BlockCache | Async admission, <1µs put() | Inline admission, ~1-5µs put() | ✅ Preserved |
| HybridStorage | Async upload, ~100µs write() | Inline upload, ~10-50ms write() | ✅ Preserved |
| CloudExecutor | Full async I/O | Degraded throughput | ✅ Preserved |

**Scenario: Memory Pressure**

| Component | Normal Behavior | Under Pressure | Correctness |
|-----------|----------------|----------------|-------------|
| WAL Buffer Pool | 64 buffers pooled | Buffers dropped, tracked | ✅ Preserved |
| BlockCache | High hit rate | More evictions, lower hit rate | ✅ Preserved |
| Memtable | Background flush | Blocking writes until flush | ✅ Preserved |

**Key Guarantee:** Correctness is never compromised. Performance degrades, but operations complete successfully.

## Monitoring and Observability

### Key Metrics

Monitor these public snapshots to detect resource constraints:

```rust
let runtime = engine.get_runtime_metrics()?;
let read_amp = engine.get_read_amp_metrics()?;
let layout = engine.get_storage_layout()?;

println!("health={:?}", runtime.health);
println!("write_stalled={}", runtime.write_stalled);
println!("wal_pending_writes={}", runtime.wal_pending_writes);
println!("pending_compactions={}", runtime.pending_compactions);
println!("avg_ssts_per_read={}", read_amp.avg_ssts_per_read);
println!("sst_count={}", runtime.sst_count);
println!("levels={}", layout.levels.len());
```

### Warning Signs

**Degraded runtime health:**
```
health: Degraded
write_stalled: true
pending_compactions: 27
```
**Diagnosis:** The engine is under sustained write or compaction pressure  
**Action:** Reduce concurrent writers, increase memory budget, or compact and flush more aggressively

**High read amplification:**
```
avg_ssts_per_read: 13.4
l0_overlap_rate: 0.62
```
**Diagnosis:** Too many SSTs are being touched per read  
**Action:** Increase memory budget, use a read-heavy profile, or trigger compaction

**Large layout backlog:**
```
sst_count: 412
pending_compactions: 19
obsolete_files: 87
```
**Diagnosis:** File cleanup and compaction are lagging  
**Action:** Run `compact_all()`, review workload profile, and inspect storage layout for hot levels

## Production Recommendations

### Deployment Guidelines

**1. Size memory budget appropriately:**
```rust
// Allocate 50-70% of available RAM to Midge
let total_ram_mb = 8192;  // 8 GiB host
let opts = OpenOptions::local("./db")
    .memory_budget(MemoryBudget::Bytes((total_ram_mb as usize * 60 / 100) << 20))
    .build();
```

**2. Limit engine instances on Windows:**
```rust
// Windows: max 50-60 engines per process
// Linux: max 500-1000 engines per process (ulimit dependent)
const MAX_ENGINES: usize = if cfg!(windows) { 50 } else { 500 };
```

**3. Monitor public engine metrics:**
```rust
loop {
    std::thread::sleep(std::time::Duration::from_secs(60));

    let runtime = engine.get_runtime_metrics()?;
    if runtime.write_stalled || runtime.pending_compactions > 0 {
        println!(
            "health={:?} write_stalled={} pending_compactions={}",
            runtime.health,
            runtime.write_stalled,
            runtime.pending_compactions
        );
    }
}
```

**4. Keep cloud configuration to the supported builder surface:**
```rust
let opts = OpenOptions::cloud("./cache", "my-bucket", "prod/")
    .goal(Goal::Throughput)
    .build();
```

Provider credentials, endpoints, and regions are configured through the selected cloud provider environment rather than additional public timeout builders.

### Troubleshooting

**Problem: Benchmark thread pool panic**
```
thread 'name' panicked at 'failed to spawn thread: ...'
```
**Solution:** Multiple BlockCache instances exhausted thread pool. Solution implemented:
- Added `Drop` implementations to join worker threads
- Added inline fallback for cache admission and WAL upload
- Increased buffer pool size from 16 to 64

**Problem: CloudAsync writes hanging**
```
write() call never returns, no error
```
**Solution:** HybridStorage upload worker failed to spawn, no fallback. Solution implemented:
- Added `process_upload_inline()` fallback method
- Detects worker spawn failure and processes uploads synchronously
- Prevents CloudAck deadlock

**Problem: Memory grows unbounded**
```
RSS grows continuously under sustained load
```
**Solution:** WAL buffer pool had no upper bound. Solution implemented:
- Added `MAX_BUFFER_POOL_SIZE = 64` limit
- Drops buffers instead of pooling when limit reached
- Tracks drops via an internal telemetry counter

## Related Documentation

- [Performance Tuning Guide](./performance-tuning.md) - High-level configuration and optimization
- [API Guide](../user-guides/api-guide.md) - API reference and examples
- [Cloud Setup Guide](./cloud-setup.md) - Cloud storage configuration


