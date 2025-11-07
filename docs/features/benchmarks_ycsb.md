# YCSB Benchmarks for Midge

**Status:** ✅ Implemented (Workloads A, B, C)  
**Last Updated:** October 27, 2025

## Overview

This document describes Midge's implementation of the **Yahoo! Cloud Serving Benchmark (YCSB)**, an industry-standard benchmark suite for evaluating database performance.

**Goal:** Prove that Midge achieves **80-90% of RocksDB/Pebble performance** on mixed workloads while providing superior cloud-native capabilities.

## Implemented Workloads

### Workload A: Update-Heavy (50% Read / 50% Write)

**Use Case:** Session store where reads and writes are balanced  
**Target Performance:** 150-250K ops/sec  
**Status:** ✅ Implemented

**Operation Mix:**
- 50% Read (point lookups)
- 50% Update (overwrites of existing keys)

**Access Pattern:** Zipfian distribution (theta=0.99) - highly skewed, 20% of keys get 80% of traffic

**Running:**
```bash
# Smoke test (quick validation)
cargo test --test ycsb_smoke should_run_ycsb_workload_a_smoke_test

# Full benchmark
cargo bench --bench ycsb_workload_a
```

---

### Workload B: Read-Mostly (95% Read / 5% Write)

**Use Case:** Photo tagging application with read-heavy access  
**Target Performance:** 250-400K ops/sec  
**Status:** ✅ Implemented

**Operation Mix:**
- 95% Read (point lookups)
- 5% Update (overwrites)

**Access Pattern:** Zipfian distribution (theta=0.99)

**Running:**
```bash
# Smoke test
cargo test --test ycsb_smoke should_run_ycsb_workload_b_smoke_test

# Full benchmark
cargo bench --bench ycsb_workload_b
```

---

### Workload C: Read-Only (100% Read)

**Use Case:** User profile cache with read-only access  
**Target Performance:** 400-500K ops/sec  
**Status:** ✅ Implemented

**Operation Mix:**
- 100% Read (point lookups)

**Access Pattern:** Zipfian distribution (theta=0.99)

**Running:**
```bash
# Smoke test
cargo test --test ycsb_smoke should_run_ycsb_workload_c_smoke_test

# Full benchmark
cargo bench --bench ycsb_workload_c
```

---

### Workload D: Read-Latest (Planned)

**Use Case:** User status updates  
**Target Performance:** 200-300K ops/sec  
**Status:** 🚧 Not yet implemented

**Operation Mix:**
- 90% Read (biased towards recently inserted keys)
- 10% Insert

---

### Workload F: Read-Modify-Write (Planned)

**Use Case:** User database with record updates  
**Target Performance:** 100-150K ops/sec  
**Status:** 🚧 Not yet implemented

**Operation Mix:**
- Read-modify-write transactions
- Validates atomic operations

---

### Workload E-Cloud: Range Scans (Midge-Specific, Planned)

**Use Case:** Range queries over cloud-backed SSTs  
**Target Performance:** TBD  
**Status:** 🚧 Not yet implemented

**Operation Mix:**
- Range scans with varying sizes
- Tests cloud SST download performance

---

## Benchmark Configuration

### Dataset Parameters

| Parameter | Value | Description |
|--|--|--|
| **Record Sizes** | 10K, 50K, 100K | Number of records in dataset |
| **Key Size** | 16 bytes | Format: `user{:012}` |
| **Value Size** | 1000 bytes | Random data with deterministic seed |
| **Total Dataset** | ~10MB to ~100MB | Depends on record count |

### Access Pattern: Zipfian Distribution

All workloads use **Zipfian distribution (theta=0.99)** to model realistic access patterns:

- **Highly skewed:** Top 20% of keys receive ~80% of requests
- **Long tail:** Remaining 80% of keys get infrequent access
- **Realistic:** Matches production database behavior

This is significantly more realistic than uniform random access.

### Benchmark Settings

| Setting | Value | Rationale |
|--|--|--|
| **Sample Size** | 20 iterations | Statistical significance |
| **Measurement Time** | 30 seconds | Stable throughput measurement |
| **Operations per Iteration** | 1,000 | Balance speed and accuracy |
| **MemTable Size** | 64 MB | Standard configuration |
| **WAL Sync** | Disabled | Pure engine performance |
| **Compaction** | Enabled | Realistic conditions |

## Running Benchmarks

### Quick Validation (Smoke Tests)

Run all smoke tests in **2.5 seconds**:

```bash
cargo test --test ycsb_smoke -- --nocapture
```

Output:
```
✅ Workload A smoke test passed (50% R / 50% W)
✅ Workload B smoke test passed (95% R / 5% W)
✅ Workload C smoke test passed (100% R)

test result: ok. 3 passed; 0 failed; 0 ignored
```

### Full Benchmarks

Run individual workload (~3-4 minutes each):

```bash
cargo bench --bench ycsb_workload_a
cargo bench --bench ycsb_workload_b
cargo bench --bench ycsb_workload_c
```

Run all YCSB benchmarks (~10-12 minutes):

```bash
cargo bench --bench ycsb_workload_a --bench ycsb_workload_b --bench ycsb_workload_c
```

### Saving Baselines

```bash
# Save baseline for comparison
cargo bench --bench ycsb_workload_a -- --save-baseline ycsb_a_v1

# Compare against baseline
cargo bench --bench ycsb_workload_a -- --baseline ycsb_a_v1
```

## Interpreting Results

### Example Output

```
ycsb_workload_a/update_heavy/100000
                        time:   [45.2 ms 46.1 ms 47.0 ms]
                        thrpt:  [21.3K elem/s 21.7K elem/s 22.1K elem/s]
```

**Interpretation:**
- **Time per iteration:** 46.1 ms for 1,000 operations
- **Throughput:** 21.7K operations per second
- **Per-operation latency:** ~46 µs average

### Performance Metrics

Criterion provides:
- **Mean:** Average performance
- **Median:** Middle value (less affected by outliers)
- **Std Dev:** Variability in measurements
- **Throughput:** Operations per second

## Performance Targets

### Comparison to RocksDB

| Workload | RocksDB Baseline | Midge Target (80-90%) | Current Status |
|--|--|--|--|
| A (50/50 R/W) | 200-300K ops/sec | 150-250K ops/sec | 🎯 TBD |
| B (95/5 R/W) | 300-500K ops/sec | 250-400K ops/sec | 🎯 TBD |
| C (100% R) | 500-600K ops/sec | 400-500K ops/sec | 🎯 TBD |
| D (Read-Latest) | 250-350K ops/sec | 200-300K ops/sec | 🚧 Not implemented |
| F (RMW) | 120-180K ops/sec | 100-150K ops/sec | 🚧 Not implemented |

**Note:** RocksDB baselines are approximations from published benchmarks. Actual comparison requires running RocksDB with equivalent configuration.

## Implementation Details

### Files

- **Benchmarks:**
  - `benches/ycsb/workload_a.rs` - Update-heavy (50/50 R/W)
  - `benches/ycsb/workload_b.rs` - Read-mostly (95/5 R/W)
  - `benches/ycsb/workload_c.rs` - Read-only (100% R)
  - `benches/ycsb/README.md` - Benchmark documentation

- **Tests:**
  - `tests/ycsb_smoke.rs` - Quick validation tests

### Zipfian Generator

Custom implementation using inverse transform method:

```rust
struct ZipfianGenerator {
    items: usize,      // Total number of keys
    theta: f64,        // Skew parameter (0.99 = highly skewed)
    zeta_n: f64,       // Normalization constant
    alpha: f64,        // Power law exponent
    eta: f64,          // Tail correction
}
```

Formula: `P(k) ∝ 1/k^theta` where k is the rank

### Data Generation

**Deterministic and reproducible:**

```rust
fn generate_key(id: usize) -> Bytes {
    Bytes::from(format!("user{:012}", id))
}

fn generate_value(id: usize, seed: u64) -> Bytes {
    let mut rng = StdRng::seed_from_u64(seed.wrapping_add(id as u64));
    let data: Vec<u8> = (0..1000).map(|_| rng.random()).collect();
    Bytes::from(data)
}
```

## Optimization Opportunities

Based on benchmark results, potential optimizations:

1. **Read-Heavy Workloads (B, C):**
   - Increase block cache size
   - Tune bloom filter bits per key
   - Enable SST index caching
   - Prefetching for sequential access

2. **Write-Heavy Workloads (A):**
   - Increase memtable size
   - Add more compaction threads
   - Tune flush thresholds
   - Optimize WAL sync strategy

3. **General:**
   - Lock-free data structures (already implemented for skiplist)
   - SIMD optimizations for bloom filters (already implemented)
   - Async I/O for SST reads
   - Compression tuning

## Roadmap

- [x] Workload A: Update-heavy (50/50 R/W)
- [x] Workload B: Read-mostly (95/5 R/W)
- [x] Workload C: Read-only (100% R)
- [x] Smoke tests for validation
- [ ] Workload D: Read-latest
- [ ] Workload F: Read-modify-write
- [ ] Workload E-Cloud: Range scans with cloud SSTs
- [ ] Latency histograms (p50/p90/p99/p999)
- [ ] RocksDB comparison benchmarks
- [ ] Results export to `infra/proofs/benchmarks/ycsb-midge-v1.json`
- [ ] Automated CI benchmark runs
- [ ] Performance regression detection

## References

- **YCSB Paper:** [Benchmarking Cloud Serving Systems with YCSB](https://research.yahoo.com/files/ycsb.pdf)
- **YCSB Workloads:** [Core Workloads Documentation](https://github.com/brianfrankcooper/YCSB/wiki/Core-Workloads)
- **Midge Performance Targets:** [PERFORMANCE_TARGETS.md](../wip/PERFORMANCE_TARGETS.md)
- **Criterion User Guide:** [Criterion.rs Book](https://bheisler.github.io/criterion.rs/book/index.html)

## Next Steps

1. **Run Full Benchmarks:** Execute `cargo bench` on representative hardware
2. **Collect Baselines:** Save results for future comparison
3. **RocksDB Comparison:** Run equivalent YCSB workloads on RocksDB
4. **Identify Bottlenecks:** Profile with flamegraph to find optimization opportunities
5. **Iterate:** Implement optimizations and re-benchmark

---

**Last Run:** October 27, 2025  
**Smoke Tests:** ✅ All passing (3/3)  
**Full Benchmarks:** 🎯 Ready to run
