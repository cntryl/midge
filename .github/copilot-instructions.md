Here is your document **properly formatted**, cleaned up, consistent, and Markdown-valid.
I did **not** alter your rules — only fixed formatting, code fences, indentation, escaping, and structure.

---

# **GitHub Copilot Instructions for Midge Project**

Midge is a **high-performance embedded LSM-tree storage engine** in Rust, with:

- MVCC transactions
- Column families
- Cloud storage backends
- Compaction filters
- High-level and low-level configuration
- WAL + SST persistence layers
- Fast skiplist-based memtable

---

# **Architecture Overview**

Module dependency layers (see `docs/DEPENDENCY_ANALYSIS.md`):

```
Layer 0 (Foundation):
  api/     - Public traits (KvStore, KvTransaction, WriteBatch)
  common/  - Error types, test hooks
  fs/      - Filesystem abstractions
  metrics/ - Cross-cutting performance metrics

Layer 1 (Configuration & Cloud):
  config/  - ConfigBuilder, MidgeOptions
  cloud/   - S3/Azure/GCS backends, mocks for testing

Layer 2 (Storage Components):
  wal/     - Write-ahead log (persistence)
  sst/     - SSTable format, bloom filters, sparse index
  health/  - Database health checks

Layer 3 (Core Engine):
  core/        - LSM engine, compaction, transactions, manifest
  engine/      - MidgeEngine and operations
  compaction/  - Background compaction coordinator
  transaction/ - MVCC TransactionController
  memtable/    - In-memory skiplist
  persistence/ - Flush coordinator, WAL replay
  manifest/    - Metadata tracking
  locking/     - Distributed locks
```

**Key Constraint:**
Lower layers must **never depend** on higher layers.
_Exception:_ Core may depend on cloud backends for distributed locks.

---

# **Configuration Philosophy**

Midge exposes **two setup paths**:

### **1. High-Level Config API (recommended)**

Answer 3 questions:

- Latency or throughput–optimized?
- Durability level (strict, steady, cloud replicated)
- Memory budget

Midge derives all optimal settings (block size, cache, compaction threads, etc.).

Example:

```rust
let engine = ConfigBuilder::new(path)
    .goal(Goal::Throughput)
    .durability(Durability::Strict)
    .build()?;
```

### **2. Low-Level MidgeOptions**

Full manual control — for experts only.

---

# **Code Guidelines**

## Idiomatic Rust Practices

- Prefer `&str` over `String` for read-only text
- Avoid unnecessary clones
- Prefer stack allocation
- Avoid panics in library code
- Use iterators over index loops where appropriate
- Implement standard traits (`Debug`, `Clone`, `Send`, etc.)
- Use `unsafe` only when absolutely necessary, and justify it

## Clippy Compliance

Always run:

```
cargo clippy --all-targets
```

Fix _everything_ before committing.

---

# **Development Workflow**

## Running Tests

```bash
cargo test                               # Run all tests
cargo test test_guidelines_compliance    # Test naming/AAA meta-test
cargo test --test engine_basic_ops       # Specific integration test
```

## Test Validation Tool

```bash
cargo run --bin validate_tests -- --summary
cargo run --bin validate_tests -- --file src/wal/wal_helpers.rs
```

## Automation Scripts

Prefer **Python** for all automation.
Store all scripts under `/scripts`.

---

# **Test Guidelines (Mandatory)**

Copilot MUST follow these rules.

## **1. Naming Convention (Required)**

Tests must use:

```
should_{action}_when_{context}
```

Correct:

```rust
#[test]
fn should_return_value_when_key_exists() {}
```

Incorrect:

```rust
#[test]
fn test_get_value() {}   // ❌ will fail meta-test
```

---

## **2. AAA Structure (Required for tests > 5 lines)**

Every non-trivial test must have:

- `// Arrange`
- `// Act`
- `// Assert`

Example:

```rust
#[test]
fn should_do_something() {
    // Arrange
    let setup = create_test_data();

    // Act
    let result = perform_operation(setup);

    // Assert
    assert_eq!(result, expected);
}
```

Forbidden:

- `// Setup`
- `// Arrange & Act`
- `// Act & Assert`

---

## **3. One Behavior Per Test**

Bad (3 behaviors):

```rust
#[test]
fn should_return_files_at_level() {
    assert_eq!(manifest.files_at_level(0).len(), 1);
    assert_eq!(manifest.files_at_level(1).len(), 2);
    assert_eq!(manifest.files_at_level(2).len(), 0);
}
```

Good (split):

```rust
#[test]
fn should_return_files_at_level_zero() {
    // Arrange
    let manifest = setup_with_level_0_files();
    // Act
    let result = manifest.files_at_level(0);
    // Assert
    assert_eq!(result.len(), 1);
}
```

---

## **4. Single Act Rule**

Never have more than one `// Act` per test.

---

## **5. Small Tests (< 5 lines)**

AAA optional:

```rust
#[test]
fn should_create_default_config() {
    let config = Config::default();
    assert_eq!(config.timeout, 30);
}
```

---

# **Benchmark Guidelines (Critical)**

Copilot **must** generate Pebble-quality microbenches using these rules.

## **Benchmark Philosophy**

All benchmarks:

- Measure **only the hot path**
- Avoid **all allocations** in measured loop
- Avoid RNG inside hot path
- Avoid thread creation inside hot path
- Precompute keys/values outside loops
- Use deterministic seeds
- Use `SamplingMode::Flat`
- Run fast (Tier 1 <1s, Tier 2 <3s)

Components to benchmark:

- SkipList
- MemTable
- WriteBatch
- WAL
- SST block builder
- SST decode/search
- MergeIterator
- SST file builder

---

# **Benchmark Structure (Mandatory)**

Every bench file must:

1. `use criterion::{...}`
2. Use `criterion_config()`
3. Put all precomputation **outside** the `b.iter(|| ...)` call
4. Terminate with:

```rust
criterion_group! { name, config, targets }
criterion_main!(name);
```

---

# **Precomputation Patterns**

### **Fixed K/V Buffers**

```rust
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
}
```

### **Deterministic Shuffle**

```rust
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
}
```

### **Concurrency (Thread-Reuse Pattern)**

```rust
let barrier = Arc::new(Barrier::new(num_threads + 1));
let stop = Arc::new(AtomicBool::new(false));
let handles = ...; // Threads spawned once

c.bench_function("concurrent", |b| {
    b.iter(|| {
        barrier.wait();
        barrier.wait();
        black_box(());
    });
});

stop.store(true, Ordering::SeqCst);
```

---

# **Required Benchmark API Usage**

Copilot must always generate:

```rust
group.sampling_mode(SamplingMode::Flat);
group.throughput(Throughput::Elements(N as u64));
```

---

# **Hot-Path Benchmarks Copilot Should Always Produce**

For each component:

- Sequential insert
- Random insert
- Concurrent insert
- Read path
- Encode/compress
- Decode/decompress
- SST build
- MergeIterator scan

---

# **Benchmark Quality Standards**

### ✔ Allowed / Required

- Allocation-free
- Deterministic
- CI-friendly
- Fast
- Clear grouping
- Uses `black_box`
- Uses real Midge types

### ❌ Forbidden

- String formatting inside loop
- Randomness inside loop
- Vec::push inside loop
- Thread spawn inside loop
- Unbounded allocations
- Disk I/O unless testing WAL

---

# **Copilot Benchmark Checklist**

Before generating a bench, check:

- [ ] All data precomputed
- [ ] Hot path only in loop
- [ ] Flat sampling mode
- [ ] Deterministic seeds
- [ ] Correct throughput
- [ ] No thread spawns
- [ ] No allocations
- [ ] Uses Midge types
- [ ] Fast (<3s)
