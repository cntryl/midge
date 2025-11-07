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
└── (standalone benchmarks)
    ├── codec.rs          # Compression codec performance
    ├── merge_iterator.rs # Merging iterator performance
    └── sst.rs            # SST format encoding/decoding

```

## Running Benchmarks

### Run All Benchmarks

```bash
cargo bench
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
