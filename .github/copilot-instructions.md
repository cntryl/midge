# GitHub Copilot Instructions for Midge Project

Midge is a **high-performance embedded LSM-tree storage engine** in Rust, offering high-level configuration, MVCC transactions, column families, compaction filters, and cloud storage backends.

## Architecture Overview

**Module Dependency Layers** (see `docs/DEPENDENCY_ANALYSIS.md`):

```
Layer 0 (Foundation):
  api/        - Public traits (KvStore, KvTransaction, WriteBatch)
  common/     - Error types, test hooks
  fs/         - Filesystem abstractions
  metrics/    - Cross-cutting performance metrics

Layer 1 (Configuration & Cloud):
  config/     - High-level ConfigBuilder + MidgeOptions
  cloud/      - S3/Azure/GCS backends (mock for testing)

Layer 2 (Storage Components):
  wal/        - Write-ahead log (persistence layer)
  sst/        - SSTable format, bloom filters, sparse index
  health/     - Database health checks

Layer 3 (Core Engine):
  core/       - LSM engine, compaction, transactions, manifest
    ├── engine/       - MidgeEngine and operations
    ├── compaction/   - Background compaction coordinator
    ├── transaction/  - MVCC TransactionController
    ├── memtable/     - In-memory skiplist
    ├── persistence/  - Flush coordinator, WAL replay
    ├── manifest/     - Metadata tracking
    └── locking/      - Distributed locks (can use cloud)
```

**Key Constraint:** Lower layers must NOT depend on higher layers. Core may depend on cloud for distributed locking (user-approved exception).

## Configuration Philosophy

Midge offers **two initialization paths**:

1. **High-Level Config API** (Recommended):

   - Answer 3 questions: Goal (Latency/Throughput/Cost), Durability (Strict/Steady/CloudReplicated), Memory Budget
   - All parameters auto-derived (block size, cache, compaction threads, etc.)
   - Use `ConfigBuilder::new(path).goal(...).durability(...).build()`

2. **Low-Level MidgeOptions**:
   - Manual control over every parameter
   - For advanced tuning only

**Examples:** See `README.md` Quick Start section and `examples/config_complete.rs`

## Development Workflows

### Running Tests

```bash
cargo test                              # All tests
cargo test test_guidelines_compliance   # Meta-test (validates naming/AAA)
cargo test --test engine_basic_ops      # Specific integration test
```

### Test Validation

```bash
# Check test compliance (naming, AAA structure)
cargo run --bin validate_tests -- --summary
cargo run --bin validate_tests -- --file src/wal/wal_helpers.rs
```

## Automation Scripts

**Prefer Python over PowerShell** for all project automation:

- **Python**: Cross-platform, better library support, VS Code integration
- **PowerShell**: Only for quick Windows-specific admin tasks
- Store automation in `scripts/` directory (e.g., `benchmark_summary.py`)

## Test Writing Guidelines — STRICTLY ENFORCE

When generating or suggesting tests, **ALWAYS** follow these rules.

### 1. Naming Convention (MANDATORY)

- ✅ Use `should_*` naming pattern
- ❌ NEVER use `test_*` naming
- Format: `should_{action}_{condition}_given_{context}`

```rust
// ✅ CORRECT
#[test]
fn should_return_value_when_key_exists() { }

// ❌ WRONG - Will fail meta-test!
#[test]
fn test_get_value() { }
```

### 2. AAA Structure (MANDATORY for tests >5 lines)

Every test **must** have exactly these three comments:

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

**NEVER use:**

- ❌ `// Arrange & Act` (combined)
- ❌ `// Act & Assert` (combined)
- ❌ `// Setup` (use `// Arrange`)
- ❌ Descriptive comments like `// Arrange - create database`

**ALWAYS use:**

- ✅ Exactly `// Arrange`
- ✅ Exactly `// Act`
- ✅ Exactly `// Assert`

### 3. Single Behavior Principle (PEDANTIC RULE)

Each test must verify **one behavior only**.
If each `assert_eq!` tests a different input/output pair, **split into multiple tests**.

```rust
// ❌ WRONG - Testing multiple inputs
#[test]
fn should_return_files_at_level() {
    let l0 = manifest.files_at_level(0);
    let l1 = manifest.files_at_level(1);
    let l2 = manifest.files_at_level(2);
    assert_eq!(l0.len(), 1);
    assert_eq!(l1.len(), 2);
    assert_eq!(l2.len(), 0);
}

// ✅ CORRECT - Focused, one behavior per test
#[test]
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
```

**Exception:**
Multiple assertions verifying **facets of the same operation** are acceptable:

```rust
// ✅ CORRECT - All assertions validate one property
#[test]
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
```

### 4. No Multiple Act Sections

**Never** have more than one `// Act` section per test.

```rust
// ❌ WRONG
#[test]
fn should_upload_and_download() {
    // Arrange
    let backend = Backend::new();

    // Act
    backend.upload("data");

    // Assert
    assert_eq!(backend.count(), 1);

    // Act  // ❌ SECOND ACT - WRONG
    let downloaded = backend.download();

    // Assert
    assert_eq!(downloaded, "data");
}

// ✅ CORRECT
#[test]
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
```

### 5. Small Tests Can Omit AAA

Tests with ≤5 lines may omit AAA comments, but still require correct naming.

```rust
// ✅ CORRECT - Short test, AAA optional
#[test]
fn should_create_default_config() {
    let config = Config::default();
    assert_eq!(config.timeout, 30);
}
```

## Common Patterns

### Testing Serialization/Deserialization

Always **separate** serialization and deserialization tests.

```rust
#[test]
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
```

### Testing Multiple Scenarios

Each scenario must have its own test.

```rust
#[test]
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
```

### Table-Driven Tests (When Appropriate)

Use only when verifying the same logic with multiple inputs.

```rust
#[test]
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
```

## Meta-Test Enforcement

Compliance is validated by `tests/test_guidelines_compliance.rs`.
The meta-test will **fail** if:

- Any test uses `test_*` naming
- Tests >5 lines are missing AAA comments
- Combined AAA comments (`// Arrange & Act`, etc.) exist

Run manually with:

```bash
cargo test test_guidelines_compliance
```

## Quick Checklist for Copilot

Before suggesting a test, verify:

- [ ] Name starts with `should_`
- [ ] If >4 lines, includes `// Arrange`, `// Act`, `// Assert`
- [ ] Only **one** `// Act` section
- [ ] Verifies **one** behavior per test
- [ ] Multiple assertions only if validating **facets of one operation**

## Example References

Excellent examples can be found in:

- `src/manifest.rs` — Clean AAA structure
- `src/index/range_tombstone.rs` — Single-behavior tests
- `src/cloud/mock.rs` — Upload/download split properly

## Why These Rules Exist

1. **Consistency** — Tests all follow the same readable pattern.
2. **Debuggability** — A failing test pinpoints the exact behavior.
3. **Maintainability** — Code changes require minimal test churn.
4. **Documentation** — Tests serve as living usage examples.
5. **CI Enforceability** — The meta-test automatically guards rules.

**REMEMBER:** When in doubt, create **more smaller tests**, not fewer large ones.
Clarity beats cleverness.

## Benchmarks

**Always generate benchmarks that follow these rules unless a file explicitly asks for something different.**

## **🎯 Benchmark Philosophy (Very Important)**

Benchmarks MUST:

- Measure **only the hot path**, not allocations or setup work.
- Avoid **all allocations inside the measured loop**.
- Avoid spawning threads inside the loop.
- Use **precomputed key/value buffers** outside the loop.
- Use **deterministic input** (fixed seeds).
- Use **flat sampling mode** for stable microbench numbers.
- Run **fast** (Tier-1 < 1 second total, Tier-2 < 3 seconds).

These benchmarks must reflect the real performance of:

- MemTable
- SkipList
- WAL write path
- SST block builder
- SST decode + search
- WriteBatch application
- MergeIterator over multiple SSTs

## **🧱 General Structure to Follow**

Every benchmark file should:

1. Import `criterion::{Criterion, BenchmarkId, SamplingMode, Throughput}`
2. Use `criterion_config()` for configuration
3. Group related benchmarks into `benchmark_group`s
4. Precompute all data _outside_ `b.iter(|| ...)`
5. Use `black_box()` correctly
6. End with:

```rust
criterion_group! { ... }
criterion_main!(...);
```

## **🔧 Precomputation Rules**

Copilot must ALWAYS follow these patterns:

### **For fixed-size K/V pairs:**

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

### **For deterministic random order:**

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

### **For concurrency:**

- Use a `Barrier`
- Use an `AtomicBool` stop flag
- Spawn worker threads **once**
- Synchronize work inside the benchmark

Copilot must not spawn threads in the hot loop.

## **🔥 Benchmark Requirements**

Copilot MUST generate benchmarks that use:

```rust
group.sampling_mode(SamplingMode::Flat);
```

and:

```rust
group.throughput(Throughput::Elements(N as u64));
```

for any “N operations” microbench.

## **📌 Hot-Path Benchmarks Copilot Should Produce**

Each time I ask Copilot for benches involving:

- SkipList
- MemTable
- WriteBatch
- WAL
- SST
- MergeIterator

Copilot must generate:

- **Sequential insert**
- **Random insert**
- **Concurrent insert (thread-reuse pattern)**
- **Read path**
- **Encode/compress**
- **Decode/decompress**
- **File builder (SST)**
- **MergeIterator scan**

Following the patterns in `hotpath_storage.rs` and `subsystem_storage.rs`.

## **🚀 Benchmark Quality Standards**

Copilot must ensure its generated benchmarks:

### **YES**

- Allocation-free inside loops
- Deterministic
- CI-friendly
- Use flat sampling
- Clear grouping and naming
- Minimal noise
- Rust-idiomatic
- Use `black_box()` correctly
- Use real Midge types (MemTable, SkipList, WalWriter, SstFileBuilder, etc.)

### **NO**

- No string formatting inside loops
- No `Vec::push` inside measured loop
- No cloning large buffers inside loops
- No thread spawn per iteration
- No random RNG calls inside measured loop
- No I/O unless the benchmark is explicitly for WAL/fsync

## **📁 File Naming**

Copilot must use the following benchmark file structure:

- `benches/hotpath_storage.rs` → Tier 1 (pure in-memory hot path)
- `benches/subsystem_storage.rs` → Tier 2 (WAL, SST, writebatch)
- `benches/system_storage.rs` → Tier 3 (full-engine + compaction)

## **📄 Example Instruction to Copilot**

If I ask:

> “Generate benchmarks for the SST read path”

Copilot must produce:

- A block decode benchmark
- A block binary search benchmark
- An SSTFile decode benchmark
- A MergeIterator benchmark
- All using the construction patterns above
- Zero allocations or randomness inside the hot loop
