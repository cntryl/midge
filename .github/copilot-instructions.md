# GitHub Copilot Instructions for Midge Project

Midge is a **high-performance embedded LSM-tree storage engine** in Rust, offering high-level configuration, MVCC transactions, column families, compaction filters, and cloud storage backends.

## Architecture Overview

**Module Dependency Layers** (see docs/DEPENDENCY_ANALYSIS.md):

`
Layer 0 (Foundation):
api/ - Public traits (KvStore, KvTransaction, WriteBatch)
common/ - Error types, test hooks
fs/ - Filesystem abstractions
metrics/ - Cross-cutting performance metrics

Layer 1 (Configuration & Cloud):
config/ - High-level ConfigBuilder + MidgeOptions
cloud/ - S3/Azure/GCS backends (mock for testing)

Layer 2 (Storage Components):
wal/ - Write-ahead log (persistence layer)
sst/ - SSTable format, bloom filters, sparse index
health/ - Database health checks

Layer 3 (Core Engine):
core/ - LSM engine, compaction, transactions, manifest
engine/ - MidgeEngine and operations
compaction/ - Background compaction coordinator
transaction/ - MVCC TransactionController
memtable/ - In-memory skiplist
persistence/ - Flush coordinator, WAL replay
manifest/ - Metadata tracking
locking/ - Distributed locks (can use cloud)
`

**Key Constraint:** Lower layers must NOT depend on higher layers. Core may depend on cloud for distributed locking (user-approved exception).

## Configuration Philosophy

Midge offers **two initialization paths**:

1. **High-Level Config API** (Recommended):

   - Answer 3 questions: Goal (Latency/Throughput/Cost), Durability (Strict/Steady/CloudReplicated), Memory Budget
   - All parameters auto-derived (block size, cache, compaction threads, etc.)
   - Use ConfigBuilder::new(path).goal(...).durability(...).build()

2. **Low-Level MidgeOptions**:
   - Manual control over every parameter
   - For advanced tuning only

**Examples:** See README.md Quick Start section and examples/config_complete.rs

## Code Guidelines

Code must be **idiomatic Rust** and **always clean of Clippy errors and warnings**. Follow these principles:

### Idiomatic Rust Practices

- Use ownership, borrowing, and lifetimes effectively.
- Prefer &str over String for read-only strings.
- Use Result and Option for error handling instead of panics.
- Implement common traits (Debug, Clone, PartialEq, etc.) where appropriate.
- Use iterators and functional programming idioms.
- Avoid unnecessary allocations; prefer stack allocation.
- Use unsafe only when absolutely necessary and well-justified.

### Clippy Compliance

- Run cargo clippy --all-targets regularly.
- Fix all warnings and errors before committing.
- Common issues to avoid: needless ranges, unnecessary clones, unused variables, style violations.

### Development Workflows

#### Running Tests

`ash
cargo test                              # All tests
cargo test test_guidelines_compliance   # Meta-test (validates naming/AAA)
cargo test --test engine_basic_ops      # Specific integration test
`

#### Test Validation

`ash

# Check test compliance (naming, AAA structure)

cargo run --bin validate_tests -- --summary
cargo run --bin validate_tests -- --file src/wal/wal_helpers.rs
`

#### Automation Scripts

**Prefer Python over PowerShell** for all project automation:

- **Python**: Cross-platform, better library support, VS Code integration
- **PowerShell**: Only for quick Windows-specific admin tasks
- Store automation in scripts/ directory (e.g., enchmark_summary.py)

## Test Guidelines

When generating or suggesting tests, **ALWAYS** follow these rules.

### 1. Naming Convention (MANDATORY)

- Use should\_\* naming pattern
- NEVER use est\_\* naming
- Format: should*{action}*{condition}_given_{context}

`
ust
// CORRECT #[test]
fn should_return_value_when_key_exists() { }

// WRONG - Will fail meta-test! #[test]
fn test_get_value() { }
`

### 2. AAA Structure (MANDATORY for tests >5 lines)

Every test **must** have exactly these three comments:

`
ust #[test]
fn should_do_something() {
// Arrange
let setup = create_test_data();

    // Act
    let result = perform_operation(setup);

    // Assert
    assert_eq!(result, expected);

}
`

**NEVER use:**

- // Arrange & Act (combined)
- // Act & Assert (combined)
- // Setup (use // Arrange)
- Descriptive comments like // Arrange - create database

**ALWAYS use:**

- Exactly // Arrange
- Exactly // Act
- Exactly // Assert

### 3. Single Behavior Principle (PEDANTIC RULE)

Each test must verify **one behavior only**.
If each ssert_eq! tests a different input/output pair, **split into multiple tests**.

`
ust
// WRONG - Testing multiple inputs #[test]
fn should_return_files_at_level() {
let l0 = manifest.files_at_level(0);
let l1 = manifest.files_at_level(1);
let l2 = manifest.files_at_level(2);
assert_eq!(l0.len(), 1);
assert_eq!(l1.len(), 2);
assert_eq!(l2.len(), 0);
}

// CORRECT - Focused, one behavior per test #[test]
fn should_return_files_at_level_zero() {
// Arrange
let manifest = setup_with_level_0_files();

    // Act
    let result = manifest.files_at_level(0);

    // Assert
    assert_eq!(result.len(), 1);

}

#[test]
fn should_return_files_at_level_one() {
// Arrange
let manifest = setup_with_level_1_files();

    // Act
    let result = manifest.files_at_level(1);

    // Assert
    assert_eq!(result.len(), 2);

}
`

**Exception:**
Multiple assertions verifying **facets of the same operation** are acceptable:

`
ust
// CORRECT - All assertions validate one property #[test]
fn should_preserve_data_across_save_load() {
// Arrange
let original = create_manifest();

    // Act
    let loaded = save_and_load(original);

    // Assert
    assert_eq!(loaded.id, original.id);
    assert_eq!(loaded.name, original.name);
    assert_eq!(loaded.size, original.size);

}
`

### 4. No Multiple Act Sections

**Never** have more than one // Act section per test.

`
ust
// WRONG #[test]
fn should_upload_and_download() {
// Arrange
let backend = Backend::new();

    // Act
    backend.upload("data");

    // Assert
    assert_eq!(backend.count(), 1);

    // Act  //  SECOND ACT - WRONG
    let downloaded = backend.download();

    // Assert
    assert_eq!(downloaded, "data");

}

// CORRECT #[test]
fn should_upload_data_successfully() {
// Arrange
let backend = Backend::new();

    // Act
    backend.upload("data");

    // Assert
    assert_eq!(backend.count(), 1);

}

#[test]
fn should_download_uploaded_data() {
// Arrange
let backend = Backend::new();
backend.upload("data");

    // Act
    let downloaded = backend.download();

    // Assert
    assert_eq!(downloaded, "data");

}
`

### 5. Small Tests Can Omit AAA

Tests with 5 lines may omit AAA comments, but still require correct naming.

`ust
//  CORRECT - Short test, AAA optional
#[test]
fn should_create_default_config() {
    let config = Config::default();
    assert_eq!(config.timeout, 30);
}`

## Common Patterns

### Testing Serialization/Deserialization

Always **separate** serialization and deserialization tests.

`
ust #[test]
fn should_serialize_manifest() {
// Arrange
let manifest = create_manifest();

    // Act
    let result = serde_json::to_string(&manifest);

    // Assert
    assert!(result.is_ok());

}

#[test]
fn should_deserialize_manifest() {
// Arrange
let original = create_manifest();
let json = serde_json::to_string(&original).unwrap();

    // Act
    let deserialized: Manifest = serde_json::from_str(&json).unwrap();

    // Assert
    assert_eq!(deserialized.id, original.id);

}
`

### Testing Multiple Scenarios

Each scenario must have its own test.

`
ust #[test]
fn should_return_value_when_key_exists() {
// Arrange
let db = Database::new();
db.insert("key", "value");

    // Act
    let result = db.get("key");

    // Assert
    assert_eq!(result, Some("value"));

}

#[test]
fn should_return_none_when_key_does_not_exist() {
// Arrange
let db = Database::new();

    // Act
    let result = db.get("nonexistent");

    // Assert
    assert_eq!(result, None);

}
`

### Table-Driven Tests (When Appropriate)

Use only when verifying the same logic with multiple inputs.

`
ust #[test]
fn should_validate_range_bounds_correctly() {
// Arrange
let test_cases = vec![
        (0, 10, true),
        (10, 0, false),
        (5, 5, false),
    ];

    // Act & Assert
    for (start, end, expected) in test_cases {
        let result = is_valid_range(start, end);
        assert_eq!(result, expected, "Failed for ({}, {})", start, end);
    }

}
`

## Meta-Test Enforcement

Compliance is validated by ests/test_guidelines_compliance.rs.
The meta-test will **fail** if:

- Any test uses est\_\* naming
- Tests >5 lines are missing AAA comments
- Combined AAA comments (// Arrange & Act, etc.) exist

Run manually with:

`ash
cargo test test_guidelines_compliance
`

## Quick Checklist for Copilot

Before suggesting a test, verify:

- [ ] Name starts with should\_
- [ ] If >4 lines, includes // Arrange, // Act, // Assert
- [ ] Only **one** // Act section
- [ ] Verifies **one** behavior per test
- [ ] Multiple assertions only if validating **facets of one operation**

## Example References

Excellent examples can be found in:

- src/manifest.rs Clean AAA structure
- src/index/range_tombstone.rs Single-behavior tests
- src/cloud/mock.rs Upload/download split properly

## Why These Rules Exist

1. **Consistency** Tests all follow the same readable pattern.
2. **Debuggability** A failing test pinpoints the exact behavior.
3. **Maintainability** Code changes require minimal test churn.
4. **Documentation** Tests serve as living usage examples.
5. **CI Enforceability** The meta-test automatically guards rules.

**REMEMBER:** When in doubt, create **more smaller tests**, not fewer large ones.
Clarity beats cleverness.

## Benchmark Guidelines

**Always generate benchmarks that follow these rules unless a file explicitly asks for something different.**

### Benchmark Philosophy (Critical)

Benchmarks MUST:

- Measure **only the hot path** (no setup, allocations, or I/O unless specified).
- Avoid **all allocations inside the measured loop**.
- Avoid spawning threads inside the loop.
- Use **precomputed key/value buffers** outside the loop.
- Use **deterministic input** (fixed seeds, no randomness in hot path).
- Use **flat sampling mode** for stable microbench numbers.
- Run **fast** (Tier-1 < 1 second total, Tier-2 < 3 seconds).
- Reflect real performance of Midge components: MemTable, SkipList, WAL write path, SST block builder, SST decode + search, WriteBatch application, MergingIterator over multiple SSTs.

### General Structure to Follow

Every benchmark file MUST:

1. Import: use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, SamplingMode, Throughput};
2. Use criterion_config() for configuration.
3. Group related benchmarks into enchmark_group!.
4. Precompute ALL data outside .iter(|| ...).
5. Use lack_box() to prevent compiler optimizations.
6. End with criterion_group! { name, config, group }; criterion_main!(name);

Template:

`
ust
use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, SamplingMode, Throughput};
use midge::...; // Import Midge types

fn criterion_config() -> Criterion {
Criterion::default()
.sampling_mode(SamplingMode::Flat)
.measurement_time(std::time::Duration::from_secs(1))
}

fn benchmark_group(c: &mut Criterion) {
// Precompute data here
let (keys, vals) = make_fixed_kv(1000);

    c.bench_function("example_bench", |b| {
        b.iter(|| {
            // Hot path only
            black_box(do_operation(&keys, &vals));
        });
    });

}

criterion_group! {
name = benches;
config = criterion_config();
targets = benchmark_group
}
criterion_main!(benches);
`

### Precomputation Rules (Mandatory Patterns)

ALWAYS use these exact patterns:

#### Fixed-Size K/V Pairs:

`ust
fn make_fixed_kv(n: usize) -> (Vec<Bytes>, Vec<Bytes>) {
    let mut keys = Vec::with_capacity(n);
    let mut vals = Vec::with_capacity(n);
    for i in 0..n {
        let mut k = [0u8; 16];
        k[8..16].copy_from_slice(&(i as u64).to_be_bytes());
        keys.push(Bytes::copy_from_slice(&k));
        vals.push(Bytes::copy_from_slice(&[0xAB; 32]));
    }
    (keys, vals)
}`

#### Deterministic Random Order:

`ust
fn shuffle_indices(len: usize) -> Vec<usize> {
    let mut idx: Vec<usize> = (0..len).collect();
    let mut seed = 0xDEADBEEFCAFEBABE;
    for i in (1..len).rev() {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let j = (seed as usize) % (i + 1);
        idx.swap(i, j);
    }
    idx
}`

#### Concurrency (Thread-Reuse Pattern):

`
ust
use std::sync::{Arc, Barrier, AtomicBool};
use std::thread;

fn concurrent_bench(c: &mut Criterion, num_threads: usize) {
let barrier = Arc::new(Barrier::new(num_threads + 1)); // +1 for main
let stop = Arc::new(AtomicBool::new(false));
let mut handles = vec![];

    // Spawn threads once
    for _ in 0..num_threads {
        let barrier = barrier.clone();
        let stop = stop.clone();
        handles.push(thread::spawn(move || {
            // Precompute per-thread data
            let data = prepare_data();
            barrier.wait(); // Sync start
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                // Hot path work
                do_work(&data);
            }
        }));
    }

    c.bench_function("concurrent_operation", |b| {
        b.iter(|| {
            barrier.wait(); // Start threads
            black_box(()); // Measure coordination if needed
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
            // Reset for next iteration
            stop.store(false, std::sync::atomic::Ordering::Relaxed);
        });
    });

    // Cleanup
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for h in handles { h.join().unwrap(); }

}
`

### Benchmark Requirements (Enforced)

ALL benchmarks MUST use:

`ust
group.sampling_mode(SamplingMode::Flat);
group.throughput(Throughput::Elements(N as u64)); // For N operations`

For I/O benchmarks (e.g., WAL fsync), use Throughput::Bytes if measuring data transfer.

### Hot-Path Benchmarks to Generate

For each component (SkipList, MemTable, WriteBatch, WAL, SST, MergingIterator), generate:

- **Sequential insert**: Precompute ordered keys, insert in order.
- **Random insert**: Use shuffled indices, insert randomly.
- **Concurrent insert**: Thread-reuse pattern with barriers.
- **Read path**: Precompute data, measure gets/lookups.
- **Encode/compress**: Measure serialization/compression.
- **Decode/decompress**: Measure deserialization/decompression.
- **File builder (SST)**: Measure building SST files.
- **MergingIterator scan**: Measure merging multiple iterators.

Follow patterns in hotpath_storage.rs and subsystem_storage.rs.

### Benchmark Quality Standards

#### YES (Required)

- Allocation-free inside loops.
- Deterministic (fixed seeds, no RNG in hot path).
- CI-friendly (fast, stable).
- Flat sampling mode.
- Clear grouping and naming.
- Minimal noise (precompute everything).
- Rust-idiomatic.
- Correct lack_box() usage.
- Real Midge types (MemTable, SkipList, WalWriter, SstFileBuilder, etc.).

#### NO (Forbidden)

- String formatting inside loops.
- Vec::push inside measured loop.
- Cloning large buffers inside loops.
- Thread spawn per iteration.
- Random RNG calls inside hot path.
- I/O unless explicitly for WAL/fsync.

#### Common Mistakes (Avoid These)

**Bad: Allocations in loop**
`ust
b.iter(|| {
    let key = format!("key_{}", i); // Allocation!
    memtable.get(&key);
});`

**Good: Precompute**
`ust
let keys = make_fixed_kv(1000).0;
b.iter(|| {
    for key in &keys {
        black_box(memtable.get(key));
    }
});`

**Bad: Randomness in hot path**
`ust
b.iter(|| {
    let idx = rand::random::<usize>() % keys.len(); // RNG!
    memtable.get(&keys[idx]);
});`

**Good: Deterministic shuffle**
`ust
let order = shuffle_indices(1000);
b.iter(|| {
    for &idx in &order {
        black_box(memtable.get(&keys[idx]));
    }
});`

### File Naming (Strict)

- enches/hotpath_storage.rs Tier 1 (pure in-memory hot path).
- enches/subsystem_storage.rs Tier 2 (WAL, SST, WriteBatch).
- enches/system_storage.rs Tier 3 (full-engine + compaction).

### CI Integration

Integrate benchmarks into CI with cargo bench --bench <name>. Use --save-baseline and --baseline for regression tracking.

### Checklist for Copilot

Before generating a benchmark, verify:

- [ ] Measures only hot path (no setup in .iter).
- [ ] All data precomputed outside loop.
- [ ] Uses SamplingMode::Flat and appropriate Throughput.
- [ ] Deterministic input (no randomness in hot path).
- [ ] Correct lack_box() usage.
- [ ] Follows file naming and structure.
- [ ] Uses real Midge types and patterns.
- [ ] Fast execution (< 3 seconds total).
- [ ] No forbidden operations (allocations, threads, etc.).

### Example Instruction

If asked: "Generate benchmarks for the SST read path"

Produce:

- Block decode benchmark (precompute blocks, measure decode).
- Block binary search benchmark (precompute sorted keys, measure search).
- SSTFile decode benchmark (precompute file data, measure full decode).
- MergingIterator benchmark (3-way merge with VecSource, measure scan).
- All with zero allocations/randomness in hot loop, using above patterns.
