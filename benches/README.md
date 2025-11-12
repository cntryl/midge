# Midge Benchmarks

This directory contains performance benchmarks for all major components of the Midge storage engine.

## Organization

Benchmarks are organized by functional area:

```
benches/
├── api/              # High-level Engine API benchmarks
│   ├── batch.rs      # Batch write operations
│   ├── multi_get.rs  # Batched point lookups
│   ├── point_lookup.rs # Single key get/put operations
│   ├── query.rs      # Range scan and query operations
│   ├── snapshot.rs   # Snapshot creation and reads
│   └── transaction.rs # Transaction commit performance
│
├── storage/          # Core storage component benchmarks
│   ├── flush_sst.rs  # Memtable-to-SST flush operations
│   ├── memtable.rs   # MemTable put/get/scan operations
│   └── skiplist.rs   # Skip-list insert/get/scan operations
│
├── wal/              # Write-Ahead Log benchmarks
│   ├── append.rs     # WAL record append operations
│   ├── wal_fs.rs     # File-based WAL performance
│   └── wal_mem.rs    # In-memory WAL performance
│
├── compaction/       # Compaction and merge benchmarks
│   └── compactor.rs  # Compaction filter and merge operations
│
├── index/            # Indexing structure benchmarks
│   └── bloom.rs      # Bloom filter build and query
│
├── utils/            # Utility component benchmarks
│   └── cache.rs      # LRU block cache insert/get/evict
│
├── backup/           # Backup and restore benchmarks
│   └── backup.rs     # Backup creation and restore
│
├── cloud/            # Cloud storage backend benchmarks
│   └── upload_mock.rs # Mock cloud upload/download
│
├── engine/           # Engine-level integration benchmarks
│   ├── basic.rs      # Basic engine operations
│   └── insert.rs     # Bulk insert performance
│
├── hotpath/          # Tier 1 — Hot Path Micro-benchmarks
│   ├── api.rs        # API entry point overhead
│   ├── cache.rs      # Block cache hit/miss patterns
│   ├── index.rs      # Index lookup micro-benchmarks
│   ├── overhead_analysis.rs # Subsystem overhead breakdown
│   ├── sst.rs        # SSTable format operations
│   ├── storage.rs    # Low-level storage ops
│   ├── tlv.rs        # TLV encoding/decoding
│   └── wal.rs        # WAL write overhead
│
├── subsystem/        # Tier 2-3 — Subsystem & Integration Benchmarks
│   ├── engine_basic.rs            # Tier 1-2: CRUD (put/get/delete, write modes)
│   ├── engine_advanced.rs         # Tier 2: TTL, CF scaling, large values, delete-heavy
│   ├── concurrency_stress.rs      # Tier 3: Concurrent writers, read/write contention, multi-CF
│   └── isolation_mvcc.rs          # Tier 3: Snapshots, MVCC, transactions, compaction interaction
│
├── system/           # Tier 3 — System-Level Benchmarks
│   ├── compaction.rs       # Full compaction cycles
│   ├── recovery.rs         # Crash recovery & WAL replay
│   ├── durability_modes.rs # WAL sync modes comparison
│   ├── ycsb_workload_a.rs  # Read/write mix (50/50)
│   ├── ycsb_workload_b.rs  # Read-heavy (95/5)
│   ├── ycsb_workload_c.rs  # Read-only
│   ├── ycsb_workload_d.rs  # Latest data (recency bias)
│   ├── ycsb_workload_e.rs  # Range scans (95% scans, 5% inserts)
│   └── ycsb_common.rs      # YCSB utilities (Zipfian, keygen, data loading)
│
└── (standalone benchmarks)
    ├── codec.rs          # Compression codec performance
    ├── merge_iterator.rs # Merging iterator performance
    └── sst.rs            # SST format encoding/decoding

```

## Benchmark Tiers

### Tier 1 — Hot Path Micro-benchmarks (hotpath/)

**Target Runtime:** < 5 seconds  
**Run Frequency:** CI / Pre-commit

Micro-benchmarks focused on individual components with minimal setup overhead:
- Component isolation (API, cache, index, SST, storage, WAL, TLV)
- Subsystem overhead analysis
- Optimal case performance baseline

### Tier 2-3 — Subsystem Benchmarks (subsystem/)

**Target Runtime:** < 30 seconds total (4 separate benchmarks)  
**Run Frequency:** Daily CI / Nightly

Integrated benchmarks that test multiple components together:

| File | Tier | Runtime | Focus |
|------|------|---------|-------|
| **engine_basic.rs** | 1-2 | ~2s | CRUD ops (put/get/delete), write modes (sync/async/batch), memory mode |
| **engine_advanced.rs** | 2 | ~3s | TTL operations, CF scaling (1-16 CFs), large values (64KB-1MB), delete-heavy workloads |
| **concurrency_stress.rs** | 3 | ~10s | Concurrent puts (1-16 threads), read/write contention, compaction pressure, concurrent deletes, multi-CF scaling |
| **isolation_mvcc.rs** | 3 | ~15s | Snapshot stress, transaction isolation, MVCC overhead, baseline latency (p50/p99), compaction amplification, read interference during compaction |

### Tier 3 — System Benchmarks (system/)

**Target Runtime:** Varies (5-60s per workload)  
**Run Frequency:** Nightly / Release validation

Full-system workload benchmarks:

| File | Runtime | Purpose | Key Metrics |
|------|---------|---------|------------|
| **ycsb_workload_a.rs** | ~15s | Read/write mix (50/50) | Throughput ops/sec (1-16 CFs, 1-8 threads) |
| **ycsb_workload_b.rs** | ~15s | Read-heavy (95/5) | Read throughput with minimal write load |
| **ycsb_workload_c.rs** | ~10s | Read-only | Cache efficiency, read-path latency |
| **ycsb_workload_d.rs** | ~12s | Recency bias (latest 95%) | Zipfian distribution realism |
| **ycsb_workload_e.rs** | ~12s | Range scans (95% scans, 5% inserts) | Scan throughput, iterator efficiency |
| **recovery.rs** | ~20s | Crash recovery & WAL replay | Recovery throughput (10K-500K records) |
| **durability_modes.rs** | ~30s | WAL sync modes (async, sync-every) | Write throughput vs durability trade-off |
| **compaction.rs** | ~5s | Full compaction cycles | Flush + compact throughput |

### Running Benchmarks

### Run All Benchmarks

```bash
cargo bench
```

### Run Subsystem Benchmarks (Recommended for development)

```bash
# Run all 4 subsystem benchmark files sequentially
cargo bench --bench subsystem_engine_basic
cargo bench --bench subsystem_engine_advanced
cargo bench --bench subsystem_concurrency_stress
cargo bench --bench subsystem_isolation_mvcc

# Or run a specific subsystem benchmark
cargo bench subsystem_engine_basic -- --list  # List all benchmarks in this tier
```

### Run System Benchmarks (Nightly)

```bash
# Full system workloads (YCSB A-E)
cargo bench --bench ycsb_workload_a
cargo bench --bench ycsb_workload_b
cargo bench --bench ycsb_workload_c
cargo bench --bench ycsb_workload_d
cargo bench --bench ycsb_workload_e

# Durability & Recovery
cargo bench --bench system_recovery
cargo bench --bench system_durability_modes
cargo bench --bench system_compaction
```

### Run Specific Benchmark Category

```bash
# API benchmarks
cargo bench --bench point_lookup
cargo bench --bench multi_get
cargo bench --bench batch
cargo bench --bench transaction

# Storage benchmarks
cargo bench --bench memtable
cargo bench --bench skiplist
cargo bench --bench flush_sst

# WAL benchmarks

cargo bench --bench append
cargo bench --bench wal_fs
cargo bench --bench wal_mem

# Index benchmarks
cargo bench --bench bloom

# Utility benchmarks
cargo bench --bench cache
cargo bench --bench codec

# Other benchmarks
cargo bench --bench sst
cargo bench --bench merge_iterator
```

### Run Specific Benchmark Function

```bash
# Run only a specific benchmark function
cargo bench -- memtable_put

# Filter benchmarks by name pattern
cargo bench -- "bloom.*insert"
```

# Full comparison

```bash
cargo bench --benches -- --save-baseline full
# later
cargo bench --benches -- --baseline full

```

## Benchmark Guidelines

When adding new benchmarks, follow these conventions:

### 1. Naming Convention

- **File names**: Use snake_case, descriptive names (e.g., `memtable.rs`, `point_lookup.rs`)
- **Function names**: Prefix with `bench_` and use descriptive names (e.g., `bench_memtable_put`)
- **Benchmark IDs**: Use consistent parameter naming (e.g., `"size_1k"`, `"seq_10k"`)

### 2. Organization

- Place benchmarks in the appropriate subdirectory based on the component being tested
- Group related benchmarks in the same file
- Use criterion groups for parameterized benchmarks

### 3. Code Structure

```rust
use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};

// Helper functions for test data generation
fn make_test_data(size: usize) -> Vec<u8> {
    // ...
}

// Individual benchmark functions
fn bench_operation_name(c: &mut Criterion) {
    // Setup
    let data = make_test_data(1000);

    // Benchmark
    c.bench_function("operation_name", |b| {
        b.iter(|| {
            // Measured code
        });
    });
}

// Parameterized benchmarks using groups
fn bench_operation_parameterized(c: &mut Criterion) {
    let mut group = c.benchmark_group("operation_group");

    for &size in &[100, 1_000, 10_000] {
        group.bench_with_input(
            BenchmarkId::new("param", size),
            &size,
            |b, &n| {
                b.iter(|| {
                    // Measured code with parameter
                });
            }
        );
    }

    group.finish();
}

criterion_group!(benches, bench_operation_name, bench_operation_parameterized);
criterion_main!(benches);
```

### 4. Best Practices

- **Use `black_box()`**: Prevent optimizer from removing measured code
- **Pre-generate data**: Don't include data generation in measured code
- **Use appropriate sizes**: Test with realistic data sizes (1K, 10K, 100K entries)
- **Measure what matters**: Focus on hot paths and user-facing operations
- **Document expectations**: Add comments explaining expected performance characteristics
- **Use `BatchSize`**: For setup-heavy benchmarks, use `iter_batched` with appropriate `BatchSize`

### 5. Performance Targets

General performance expectations for common operations:

| Operation          | Target Throughput | Notes                  |
| ------------------ | ----------------- | ---------------------- |
| MemTable Put (seq) | 1M+ ops/sec       | In-memory, sequential  |
| MemTable Get       | 500K+ ops/sec     | Skip-list lookup       |
| WAL Append (sync)  | 10K+ ops/sec      | Limited by fsync       |
| WAL Append (async) | 100K+ ops/sec     | Buffered writes        |
| Bloom Add          | 1M+ ops/sec       | Hash + bit set         |
| Bloom Query        | 5M+ ops/sec       | Hash + bit check       |
| SST Block Encode   | 100+ MB/sec       | Depends on compression |
| Cache Insert       | 1M+ ops/sec       | LRU update             |
| Cache Get (hit)    | 5M+ ops/sec       | HashMap lookup         |

## Benchmark Results

Results are saved to `target/criterion/` and include:

- HTML reports with charts
- Statistical analysis (mean, median, std dev)
- Comparison with previous runs
- Regression detection

View results:

```bash
# Open HTML report
open target/criterion/report/index.html  # macOS
xdg-open target/criterion/report/index.html  # Linux
start target/criterion/report/index.html  # Windows
```

## Continuous Benchmarking

Benchmarks should be run:

- Before and after performance-critical changes
- As part of release validation
- Periodically to detect performance regressions

## See Also

- [Test Guidelines](../docs/dev/test_guidelines.md) - Testing best practices
- [Bench Guidelines](../docs/dev/bench_guidelines.md) - Detailed benchmarking guide
- [Performance](../docs/wip/OPTIMIZATIONS_TODO.md) - Performance optimization roadmap
