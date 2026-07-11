# Performance Tuning

**Optimizing Midge for your workload**

## Overview

Midge uses **smart parameter derivation** - configure high-level goals, and all low-level tuning parameters are derived automatically. This guide explains how to optimize performance through configuration and monitoring.

**Core tuning philosophy:**
- Start with high-level goals (`Goal`, `MemoryBudget`, `WorkloadProfile`)
- Monitor actual behavior (read amplification, cache hit rates, flush frequency)
- Adjust based on observed metrics

For API details, see [../user-guides/api-guide.md](../user-guides/api-guide.md).

## High-Level Configuration

### Goal - Optimization Target

`Goal` determines what Midge optimizes for during parameter derivation:

#### Goal::Latency (Default)

```rust
let opts = OpenOptions::local("./db")
    .goal(Goal::Latency)
    .build();
```

**Optimizes for:** p99 latency < 10ms for point queries

**Parameter derivation:**
- Smaller block sizes (16 KiB) - less data read per block
- Aggressive bloom filters (10 bits/key, 1% false positive rate)
- Lower L0 compaction trigger (4 files) - reduces read amplification
- Higher cache allocation (~60% of memory budget)

**Use when:**
- Serving user-facing requests with SLAs
- Interactive applications requiring fast response
- Tail latency matters more than throughput

**Performance characteristics:**
- Read latency: usually best for point-lookups and hot-cache workloads
- Write latency: lowest among the durable modes for latency-sensitive configurations
- Throughput: lower than throughput-optimized settings, but more predictable

---

#### Goal::Throughput

```rust
let opts = OpenOptions::local("./db")
    .goal(Goal::Throughput)
    .build();
```

**Optimizes for:** Maximum MB/s and ops/sec for bulk operations

**Parameter derivation:**
- Larger block sizes (64 KiB) - better compression, fewer I/O ops
- Larger memtables (256 MiB) - batch more writes before flush
- Higher L0 trigger (8 files) - less frequent compaction
- Larger SST files (512 MiB) - fewer files overall

**Use when:**
- Batch processing pipelines
- Bulk data loads
- Analytics workloads
- Sustained high write rate

**Performance characteristics:**
- Read latency: often higher than `Goal::Latency` because blocks and files are larger
- Write latency: optimized for sustained ingest rather than lowest p99
- Throughput: best suited to bulk operations and steady write-heavy workloads

---

#### Goal::Economy

```rust
let opts = OpenOptions::local("./db")
    .goal(Goal::Economy)
    .build();
```

**Optimizes for:** Minimal memory and CPU usage

**Parameter derivation:**
- Minimal cache allocation (~30% of memory budget)
- Smaller bloom filters (5 bits/key, higher false positive rate)
- Higher compression levels (Zstd instead of LZ4)
- Lower compaction concurrency

**Use when:**
- Resource-constrained environments
- Cost optimization priority
- Embedded/IoT devices
- Many database instances per host

**Performance characteristics:**
- Read latency: 10-100ms (smaller cache, more disk I/O)
- Write latency: 5-20ms (higher compression overhead)
- Memory usage: 30-50% lower than Latency mode

---

### Memory Budget - Resource Allocation

`MemoryBudget` controls total memory allocation for cache, memtables, and overhead:

#### MemoryBudget::Auto (Default)

```rust
let opts = OpenOptions::local("./db")
    .memory_budget(MemoryBudget::Auto)
    .build();
```

**Behavior:**
- Detects available system memory (cgroup-aware)
- Allocates ~50% of effective memory limit
- Safe default for most deployments

**Use when:**
- Midge is primary application on host
- Container with memory limit (Kubernetes, Docker)
- Don't want to manually tune

---

#### MemoryBudget::Bytes(n)

```rust
let opts = OpenOptions::local("./db")
    .memory_budget(MemoryBudget::Bytes(2 << 30))  // 2 GiB
    .build();
```

**Behavior:**
- Explicit memory budget in bytes
- All allocations must fit within budget
- Predictable resource usage

**Memory distribution:**
- Block cache: ~60% (Goal::Latency) to ~30% (Goal::Economy)
- Memtables: ~30%
- Metadata/overhead: ~10%

**Use when:**
- Multiple database instances per host
- Precise resource control required
- Known workload characteristics

**Sizing guidance:**

| Workload | Recommended Budget | Rationale |
|----------|-------------------|-----------|
| Small dataset (<1 GiB) | 256 MiB | Entire dataset fits in cache |
| Medium dataset (1-10 GiB) | 1-2 GiB | Hot data fits in cache |
| Large dataset (>10 GiB) | 4-8 GiB | Working set coverage |
| Write-heavy | 2-4 GiB | Large memtables reduce flush frequency |
| Read-heavy | 4-8 GiB | Maximize cache hit rate |

---

### Workload Profile - Access Pattern Hints

`WorkloadProfile` provides hints about expected access patterns:

#### Mixed (Default)

```rust
let opts = OpenOptions::local("./db")
    .workload(WorkloadProfile::Mixed)
    .build();
```

Balanced read/write configuration. Use when workload is unknown or balanced.

---

#### WriteHeavy

```rust
let opts = OpenOptions::local("./db")
    .workload(WorkloadProfile::WriteHeavy)
    .build();
```

**Optimizations:**
- Larger memtables (reduce flush frequency)
- More aggressive compaction (prevent L0 buildup)
- Lower bloom filter priority (reads are rare)

**Use when:** >70% of operations are writes

---

#### ReadMostly

```rust
let opts = OpenOptions::local("./db")
    .workload(WorkloadProfile::ReadMostly)
    .build();
```

**Optimizations:**
- Aggressive bloom filters (reduce false lookups)
- Higher cache allocation
- Lower compaction priority (less write amplification)

**Use when:** >70% of operations are reads

---

#### RangeScan

```rust
let opts = OpenOptions::local("./db")
    .workload(WorkloadProfile::RangeScan)
    .build();
```

**Optimizations:**
- Larger block sizes (better sequential read)
- Bloom filters disabled (not useful for ranges)
- Sequential access optimization

**Use when:** Most operations are range scans, not point queries

---

#### TtlHeavy

```rust
let opts = OpenOptions::local("./db")
    .workload(WorkloadProfile::TtlHeavy)
    .build();
```

**Optimizations:**
- More aggressive compaction (clean up tombstones)
- Higher tombstone cleanup priority

**Use when:** Many keys have short TTL with frequent expirations

---

## Monitoring Performance

### Read Amplification Metrics

Monitor how many SSTs are accessed per read:

```rust
let metrics = engine.get_read_amp_metrics()?;

println!("Total reads: {}", metrics.reads_total);
println!("Avg SSTs per read: {}", metrics.avg_ssts_per_read);
println!("Avg L0 SSTs per read: {}", metrics.avg_l0_ssts_per_read);
println!("L0 overlap rate: {:.1}%", metrics.l0_overlap_rate * 100.0);
println!("SST budget violations: {:.2}%", 
    metrics.sst_budget_violation_rate * 100.0);
```

**Interpreting metrics:**

| Metric | Good | Warning | Action |
|--------|------|---------|--------|
| avg_ssts_per_read | <5 | >10 | Trigger compaction, increase memory |
| avg_l0_ssts_per_read | <2 | >4 | L0 buildup, trigger compaction |
| l0_overlap_rate | <20% | >50% | L0 files overlapping, need compaction |
| sst_budget_violation_rate | <1% | >5% | Read amplification too high |

---

### Cache Performance

Monitor cache hit rates (if exposed in future versions):

```rust
// Future API
let cache_stats = engine.cache_stats(&cf)?;
println!("Hit rate: {:.1}%", cache_stats.hit_rate * 100.0);
```

**Target hit rates:**
- Read-heavy workload: >90%
- Mixed workload: >70%
- Write-heavy workload: >50%

**If hit rate is low:**
- Increase memory budget
- Use Goal::Latency (higher cache allocation)
- Verify working set fits in cache

---

### Flush and Compaction Frequency

Monitor background activity:

```rust
// Check flush frequency via logs or metrics
// High flush frequency indicates:
// - Small memtables
// - High write rate
// - May need larger memory budget
```

**Healthy patterns:**
- Flushes: Every 30-60 seconds for steady write load
- L0→L1 compaction: Every 2-5 minutes
- Major compaction: Every 10-30 minutes

**Problem patterns:**
- Flush every <10 seconds: Memtables too small, increase memory budget
- No flushes for hours: No writes (expected) or stalled (investigate)
- Continuous compaction: L0 buildup, increase compaction triggers

---

## Tuning Scenarios

### Scenario 1: High Read Latency

**Symptoms:**
- get() calls take >50ms
- p99 latency spikes

**Diagnosis:**

```rust
let metrics = engine.get_read_amp_metrics()?;
if metrics.avg_ssts_per_read > 10.0 {
    println!("High read amplification detected");
}
```

**Solutions:**

1. **Increase memory budget:**
   ```rust
   let opts = OpenOptions::local("./db")
       .memory_budget(MemoryBudget::Bytes(4 << 30))  // 4 GiB
       .build();
   ```

2. **Optimize for reads:**
   ```rust
   let opts = OpenOptions::local("./db")
       .goal(Goal::Latency)
       .workload(WorkloadProfile::ReadMostly)
       .build();
   ```

3. **Trigger compaction:**
   ```rust
   engine.flush_cf(&cf)?;
   // Wait for background compaction
   ```

---

### Scenario 2: High Write Latency / Write Stalls

**Symptoms:**
- commit() calls slow or fail with WriteStall
- Frequent backpressure

**Diagnosis:**

```rust
match engine.commit(tx, WriteOptions::buffered()) {
    Err(MidgeError::WriteStall) => {
        println!("Memtable queue full");
    }
    _ => {}
}
```

**Solutions:**

1. **Increase memory budget:**
   ```rust
   let opts = OpenOptions::local("./db")
       .memory_budget(MemoryBudget::Bytes(4 << 30))
       .goal(Goal::Throughput)  // Larger memtables
       .build();
   ```

2. **Optimize for writes:**
   ```rust
   let opts = OpenOptions::local("./db")
       .workload(WorkloadProfile::WriteHeavy)
       .build();
   ```

3. **Manual flush during bulk load:**
   ```rust
   // Periodically flush to prevent stalls
   if write_count % 100_000 == 0 {
       engine.flush_cf(&cf)?;
   }
   ```

---

### Scenario 3: Range Scan Performance

**Symptoms:**
- scan() operations slow
- Reading same data multiple times

**Solutions:**

1. **Optimize for scans:**
   ```rust
   let opts = OpenOptions::local("./db")
       .workload(WorkloadProfile::RangeScan)
       .goal(Goal::Throughput)  // Larger blocks
       .build();
   ```

2. **Use limits appropriately:**
   ```rust
   use cntryl_midge::{Bytes, Query};

   let query = Query::new()
       .prefix(Bytes::from_static(b"user:"))
       .limit(100);  // Don't scan entire dataset
   let mut iter = tx.scan(&query)?;
   ```

3. **Consider data layout:**
   - Keys with common prefixes benefit from prefix scanning
   - Lexicographically ordered keys scan efficiently

---

### Scenario 4: Memory Constraints

**Symptoms:**
- OOM errors
- Memory usage exceeds expected

**Solutions:**

1. **Use Economy mode:**
   ```rust
   let opts = OpenOptions::local("./db")
       .goal(Goal::Economy)
       .memory_budget(MemoryBudget::Bytes(512 << 20))  // 512 MiB
       .build();
   ```

2. **Explicit budget:**
   ```rust
   let opts = OpenOptions::local("./db")
       .memory_budget(MemoryBudget::Bytes(256 << 20))  // 256 MiB
       .build();
   ```

3. **Flush frequently:**
   ```rust
   // Explicit flush to free memtables
   engine.flush_cf(&cf)?;
   ```

---

## Measuring Performance

### Benchmarking

Use the registered `cntryl-stress` suites to validate changes:

```bash
# Run a fast semantic pass
cargo bench --bench tier1_hotpath_api -- --profile smoke

# Emit an artifact suitable for before/after comparison
cargo bench --bench tier1_hotpath_api -- --profile default --json
```

Retain the timestamped artifacts under `target/stress/tier1_hotpath_api/`, make
the change, and run the same profile again on the same host. Compare matching
rows from the two JSON artifacts; the harness does not use Criterion baseline
flags.

See [../development/benchmarks.md](../development/benchmarks.md) for benchmarking best practices.

### Application-Level Metrics

Instrument your application:

```rust
use std::time::Instant;

let start = Instant::now();
let result = tx.get(key)?;
let latency = start.elapsed();

if latency.as_millis() > 10 {
    log::warn!("Slow read: {:?} for key {:?}", latency, key);
}
```

**Key metrics to track:**
- p50, p95, p99 read latency
- p50, p95, p99 write latency
- Operations per second
- Cache hit rate (if exposed)
- Flush/compaction frequency

---

## Configuration Examples

### Low-Latency Web Service

```rust
let opts = OpenOptions::local("./db")
    .goal(Goal::Latency)
    .memory_budget(MemoryBudget::Bytes(4 << 30))  // 4 GiB
    .workload(WorkloadProfile::ReadMostly)
    .build();

// Use buffered() for fast commits
engine.commit(tx, WriteOptions::buffered())?;
```

---

### High-Throughput Batch Processing

```rust
let opts = OpenOptions::local("./db")
    .goal(Goal::Throughput)
    .memory_budget(MemoryBudget::Bytes(8 << 30))  // 8 GiB
    .workload(WorkloadProfile::WriteHeavy)
    .build();

// Use best_effort() for bulk loads
engine.commit(tx, WriteOptions::best_effort())?;
// Flush when done
engine.flush_cf(&cf)?;
```

---

### Resource-Constrained Environment

```rust
let opts = OpenOptions::local("./db")
    .goal(Goal::Economy)
    .memory_budget(MemoryBudget::Bytes(256 << 20))  // 256 MiB
    .workload(WorkloadProfile::Mixed)
    .build();
```

---

## Next Steps

- **Benchmarking guide**: [../development/benchmarks.md](../development/benchmarks.md)
- **API reference**: [../user-guides/api-guide.md](../user-guides/api-guide.md)
- **Troubleshooting**: [../user-guides/troubleshooting.md](../user-guides/troubleshooting.md)
