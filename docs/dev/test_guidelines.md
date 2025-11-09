# Test Guidelines

These guidelines define how we write tests across the Midge repository to ensure consistency, readability, and reliability. **All 239+ tests follow these patterns** — this document reflects our actual validated practices.

## Table of Contents

- [Test Organization Philosophy](#test-organization-philosophy)
- [Naming Convention](#naming-convention)
- [File Organization](#file-organization)
- [Single Behavior Principle](#single-behavior-principle)
- [Test Structure (Arrange/Act/Assert)](#test-structure-arrangea ctassert)
- [Async & Timeout Patterns](#async--timeout-patterns)
- [Trait Behavior Tests](#trait-behavior-tests)
- [Table-Driven Tests](#table-driven-tests)
- [Determinism & Isolation](#determinism--isolation)
- [Assertions](#assertions)
- [Negative Testing](#negative-testing)
- [CI Commands](#ci-commands)
- [Quick Reference Checklist](#quick-reference-checklist)

---

## Test Organization Philosophy

**Principle:** Following Rust conventions, we distinguish between **unit tests** and **integration tests**.

### Unit Tests vs Integration Tests

**Unit Tests:**

- Located in `src/` files within `#[cfg(test)] mod tests { }` blocks
- Test **single modules** in isolation
- Have access to private functions and internal implementation
- Test individual components, functions, structs, and traits
- Run with `cargo test --lib`

**Integration Tests:**

- Located in the `tests/` directory
- Test **multiple modules working together** or the public API
- Only have access to public interfaces
- Test cross-cutting concerns, end-to-end scenarios
- Run with `cargo test --test <name>`
- **Use `tests/common/` for shared test utilities** to reduce duplication

### Common Test Helpers

The `tests/common/` module provides reusable test utilities to eliminate code duplication and ensure consistent test patterns:

```rust
mod common;
use common::*;

#[test]
fn should_persist_data_across_restart() {
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    with_engine_restart(
        opts,
        |eng| {
            eng.put(Bytes::from("key"), Bytes::from("value")).unwrap();
        },
        |eng| {
            assert_get_equals(eng, b"key", b"value");
        },
    );
}
```

**Available helpers:**

- `test_temp_dir()` - Isolated temp directory
- `durability_opts(path)` - Options for durability tests
- `flush_test_opts(path, size)` - Options with custom memtable size
- `with_engine(opts, f)` - Execute closure with engine
- `with_engine_restart(opts, before, after)` - Test restart behavior
- `assert_get_equals(eng, key, expected)` - Assert key value
- `assert_key_absent(eng, key)` - Assert key doesn't exist
- `assert_manifest_exists(path)` - Verify manifest file
- Cloud helpers in `tests/common/cloud.rs`

**Benefits:**

- Eliminates 100+ instances of duplicate setup code
- Single source of truth for test infrastructure
- Tests focus on behavior, not boilerplate

### Organization Rules

```
src/
├── storage/
│   ├── bloom.rs          → Contains #[cfg(test)] mod tests { ... }
│   ├── skiplist.rs       → Contains #[cfg(test)] mod tests { ... }
│   ├── memtable.rs       → Contains #[cfg(test)] mod tests { ... }
│   └── sst.rs            → Contains #[cfg(test)] mod tests { ... }
├── wal/
│   ├── wal.rs            → Contains #[cfg(test)] mod tests { ... }
│   ├── wal_fs.rs         → Contains #[cfg(test)] mod tests { ... }
│   └── wal_mem.rs        → Contains #[cfg(test)] mod tests { ... }

tests/                     (Integration tests only)
├── engine.rs             → Tests MidgeEngine (multiple modules)
├── backup.rs             → Tests backup system (multiple modules)
├── compaction/
│   └── compactor.rs      → Tests compaction (multiple modules)
└── api/
    ├── query.rs          → Tests query API (public interface)
    └── transaction.rs    → Tests transaction API (public interface)
```

### Decision Tree: Where Should This Test Go?

```
Does the test use MidgeEngine or test multiple modules?
├─ YES → tests/ directory (integration test)
│  Example: Testing backup, compaction, full CRUD flows
│
└─ NO → Is it testing a single module's internal behavior?
   ├─ YES → src/<module>.rs in #[cfg(test)] mod tests { }
   │  Example: Testing skiplist insert, bloom filter contains, codec roundtrip
   │
   └─ UNSURE → Does it need access to private functions/fields?
      ├─ YES → Unit test (src/ file)
      └─ NO → Could be either; prefer unit test for speed

---

## Naming Convention

Use **behavior-first** test names that clearly describe the expected outcome:

```

should*<behavior>\_given*<context>_when_<condition>

````

### Real Examples from Midge

✅ **Good:**
```rust
should_resolve_put_on_get_local()
should_enforce_quota_given_file_registration_when_limit_exceeded()
should_track_operation_counts_given_basic_operations()
should_roundtrip_given_small_input()
should_return_none_given_deleted_key_when_delete()
should_defer_deletion_given_grace_period_when_marked()
````

❌ **Bad:**

```rust
test_get_local()           // What behavior? What's expected?
test_quota()               // Too vague
basic_operations()         // Not behavior-first
roundtrip_small()          // Missing should/given/when
test_1()                   // Meaningless
```

### Naming Guidelines

- **Be descriptive:** Clarity beats brevity. Long, clear names > short, vague names
- **Include context:** State preconditions (`given`) when they affect the outcome
- **Omit redundancies:** If action is obvious from outcome, `when_<action>` can be omitted
- **Use consistent verbs:**
  - `should_return` — for getters/queries
  - `should_enforce` — for validation/limits
  - `should_track` — for metrics/counters
  - `should_apply` — for state changes
  - `should_defer` — for delayed operations
  - `should_coordinate` — for multi-component behavior

### Naming Decision Tree

```
What does the test verify?

Getter/Query → should_return_<value>_given_<state>
  Example: should_return_none_given_missing_key

Validation → should_enforce_<rule>_given_<condition>
  Example: should_enforce_quota_given_limit_exceeded

State Change → should_apply_<change>_when_<trigger>
  Example: should_apply_new_rate_given_dynamic_adjustment

Metric/Counter → should_track_<metric>_given_<operation>
  Example: should_track_flush_operations_given_memtable_flush

Complex/Other → should_<behavior>_given_<context>_when_<action>
  Example: should_defer_deletion_given_grace_period_when_marked
```

---

## File Organization

**Principle:** Unit tests live in `src/` files, integration tests in `tests/` directory.

### Unit Test Pattern

```rust
// src/storage/skiplist.rs

pub struct SkipList {
    // ... implementation ...
}

impl SkipList {
    pub fn new() -> Self {
        // ... implementation ...
    }

    pub fn upsert(&mut self, key: Vec<u8>, value: Option<Vec<u8>>, seq: u64) {
        // ... implementation ...
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_insert_and_get_value() {
        // Arrange
        let mut sl = SkipList::new();

        // Act
        sl.upsert(b"key".to_vec(), Some(b"val".to_vec()), 1);

        // Assert
        assert_eq!(sl.get(b"key"), Some(&Some(b"val".to_vec())));
    }

    #[test]
    fn should_return_none_given_missing_key() {
        // Arrange
        let sl = SkipList::new();

        // Act
        let result = sl.get(b"nonexistent");

        // Assert
        assert_eq!(result, None);
    }
}
```

### Integration Test Pattern

```rust
// tests/engine.rs

use cntryl_midge::{MidgeEngine, MidgeOptions};
use bytes::Bytes;
use tempfile::TempDir;

#[test]
fn should_persist_data_across_restart() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let opts = MidgeOptions {
        db_path: dir.path().to_path_buf(),
        ..Default::default()
    };

    // Act: Write and close
    {
        let mut engine = MidgeEngine::open(opts.clone()).unwrap();
        engine.put(Bytes::from("key"), Bytes::from("value")).unwrap();
    }

    // Act: Reopen and read
    let engine = MidgeEngine::open(opts).unwrap();
    let result = engine.get(&Bytes::from("key")).unwrap();

    // Assert
    assert_eq!(result, Some(Bytes::from("value")));
}
```

### Current Midge Structure

**Unit Tests (in `src/`):**

```
src/
├── index/
│   ├── bloom.rs              → 14 tests
│   ├── merge_iterator.rs     → 10 tests
│   └── range_tombstone.rs    → 21 tests
├── storage/
│   ├── skiplist.rs           → 4 tests
│   ├── memtable.rs           → 13 tests
│   ├── sparse_index.rs       → 5 tests
│   ├── sst.rs                → 7 tests
│   ├── mem.rs            → 11 tests
│   ├── fs.rs             → 6 tests
│   └── file_manager.rs       → 11 tests
├── wal/
│   ├── wal.rs                → 4 tests
│   ├── wal_fs.rs             → 12 tests
│   └── wal_mem.rs            → 16 tests
└── utils/
    ├── tlv.rs                → 15 tests
    └── codec.rs              → 20+ tests
```

**Integration Tests (in `tests/`):**

```
tests/
├── engine.rs                 → Full engine integration
├── backup.rs                 → Backup system
├── manifest.rs               → Manifest persistence
├── api/
│   ├── query.rs              → Query API
│   ├── transaction.rs        → Transaction API
│   └── snapshot.rs           → Snapshot isolation
├── compaction/
│   ├── compactor.rs          → Compaction logic
│   └── compaction_filter.rs  → Custom filters
└── utils/
    ├── cache.rs              → Cache integration with engine
    ├── metrics.rs            → Metrics collection
    └── rate_limiter.rs       → Rate limiting integration
```

### Import Patterns

**Unit Tests (same file):**

```rust
#[cfg(test)]
mod tests {
    use super::*;  // Import from parent module
    use bytes::Bytes;
    use crate::error::MidgeResult;  // Absolute crate path

    #[test]
    fn should_behave_correctly() {
        let obj = MyStruct::new();  // Can access private items
        // ...
    }
}
```

**Integration Tests (separate file):**

```rust
// tests/engine.rs
use cntryl_midge::{MidgeEngine, MidgeOptions};  // Public API only
use cntryl_midge::error::MidgeError;
use bytes::Bytes;

#[test]
fn should_work_end_to_end() {
    let engine = MidgeEngine::open(opts).unwrap();
    // Can only access public API
}
```

### Migration Summary

We recently migrated 70+ tests from `tests/` to `src/` following Rust conventions:

**Migrated to `src/` (Unit Tests):**

- ✅ bloom.rs (14 tests)
- ✅ merge_iterator.rs (10 tests)
- ✅ range_tombstone.rs (21 tests)
- ✅ tlv.rs (15 tests)
- ✅ skiplist.rs (4 tests)
- ✅ memtable.rs (13 tests)
- ✅ codec.rs (20+ tests)
- ✅ sparse_index.rs (5 tests)
- ✅ sst.rs (7 tests)
- ✅ mem.rs (11 tests)
- ✅ fs.rs (6 tests)
- ✅ wal.rs (4 tests)
- ✅ wal_fs.rs (12 tests)
- ✅ wal_mem.rs (16 tests)
- ✅ file_manager.rs (11 tests)

**Kept in `tests/` (Integration Tests):**

- engine.rs (uses MidgeEngine)
- backup.rs (multi-module)
- manifest.rs (persistence integration)
- compaction/\* (multi-module compaction)
- api/\* (public API tests)
- utils/cache.rs (uses MidgeEngine)
- utils/metrics.rs (uses MidgeEngine)
- utils/rate_limiter.rs (hybrid integration)

---

## Single Behavior Principle

**Core Rule:** Each test verifies **exactly one behavior** with **exactly one Act**.

### What is the "Act"?

The **Act** is the single operation/method call you're testing — the subject of verification.

```rust
#[test]
fn should_resolve_put_on_get_local() {
    // Arrange
    let mut tx = Transaction::new(1, 0);
    tx.put(Bytes::from_static(b"a"), Bytes::from_static(b"1"), None);

    // Act ← This ONE operation is what we're testing
    let got = tx.get_local(b"a");

    // Assert
    assert_eq!(got, Some(Some(Bytes::from_static(b"1"))));
}
```

**Setup operations** in Arrange (like `tx.put()` above) are **not** Acts — they're preconditions.

### Multiple Assertions: When They're OK

Multiple assertions are fine **if they all verify the same behavior**:

✅ **Good:** All assertions verify "quota enforcement"

```rust
#[test]
fn should_enforce_quota_given_file_registration_when_limit_exceeded() {
    // Arrange
    let fm = FileManager::with_limits(1_000_000, 0);
    fm.register_file(Path::new("f1"), 400_000).unwrap();
    fm.register_file(Path::new("f2"), 400_000).unwrap();

    // Act: Try to exceed quota
    let result = fm.register_file(Path::new("f3"), 300_000);

    // Assert: All verify the same "quota enforcement" behavior
    assert!(result.is_err());                    // ✓ Error occurred
    if let Err(FileManagerError::QuotaExceeded { requested, available }) = result {
        assert_eq!(requested, 300_000);          // ✓ Correct request size
        assert_eq!(available, 200_000);          // ✓ Correct remaining space
    }
}
```

✅ **Good:** Related side-effects of one behavior

```rust
#[test]
fn should_track_operation_counts_given_basic_operations() {
    // ... arrange ...

    // Act
    engine.put(k1, v1).unwrap();
    engine.put(k2, v2).unwrap();

    // Assert: All verify "operation tracking" behavior
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.put_count, 2);           // ✓ Direct counter
    assert_eq!(snapshot.memtable_writes, 2);     // ✓ Related metric
    assert_eq!(snapshot.total_operations(), 2);  // ✓ Derived value
}
```

### When to Split Tests

Split into separate tests when you encounter:

| Symptom                  | Action                                      |
| ------------------------ | ------------------------------------------- | -------- |
| **Multiple Acts**        | Different operations being tested           | ✂️ Split |
| **Unrelated Assertions** | Independent behaviors even with same Act    | ✂️ Split |
| **Vague Name**           | Can't describe as "should_X"                | ✂️ Split |
| **Failure Ambiguity**    | When it fails, unclear which behavior broke | ✂️ Split |
| **Complex Setup**        | Need different Arrange for different checks | ✂️ Split |

### Anti-Pattern Example

❌ **Bad:** Multiple unrelated Acts

```rust
#[test]
fn should_exercise_many_things() { // ← Red flag: vague name
    // Arrange
    let mut tx = Transaction::new(1, 0);
    tx.put(b"a", b"1", None);

    // Act 1: Query
    let got = tx.get_local(b"a");
    assert_eq!(got, Some(Some(b"1")));  // ← First behavior

    // Act 2: Commit
    let muts = tx.commit().unwrap();
    assert_eq!(muts.len(), 1);  // ← Completely different behavior!
}
```

✅ **Good:** Split into focused tests

```rust
#[test]
fn should_resolve_put_on_get_local() {
    // Arrange
    let mut tx = Transaction::new(1, 0);
    tx.put(b"a", b"1", None);

    // Act
    let got = tx.get_local(b"a");

    // Assert
    assert_eq!(got, Some(Some(b"1")));
}

#[test]
fn should_return_staged_mutations_in_order_when_commit() {
    // Arrange
    let mut tx = Transaction::new(1, 0);
    tx.put(b"a", b"1", None);

    // Act
    let muts = tx.commit().unwrap();

    // Assert
    assert_eq!(muts.len(), 1);
}
```

### Reviewer Checklist

When reviewing tests, verify:

- [ ] Test name clearly describes **one** expected behavior
- [ ] Exactly **one** Act operation
- [ ] All assertions verify the **same** behavior or its direct side-effects
- [ ] If test fails, the **behavior** that broke is immediately clear
- [ ] No "and" in the test name (usually indicates multiple behaviors)

---

## Test Structure (Arrange/Act/Assert)

**Mandatory Pattern:** Every test must have clearly marked sections.

### Standard Pattern

```rust
#[test]
fn should_<behavior>() {
    // Arrange
    // ... setup code ...

    // Act
    // ... single operation under test ...

    // Assert
    // ... verify expected outcome ...
}
```

### Real Example

```rust
#[test]
fn should_exclude_deleted_keys_given_tombstones_when_scanning() {
    // Arrange
    let mt = Memtable::new();
    mt.put(b"a", b"1");
    mt.put(b"b", b"2");
    mt.delete(b"a");

    // Act
    let rows = mt.scan_range(Some(b"a"), Some(b"z"));

    // Assert: tombstone 'a' excluded
    assert_eq!(
        rows,
        vec![(Bytes::from_static(b"b"), Bytes::from_static(b"2"))]
    );
}
```

### Automated Enforcement

Test guidelines are automatically enforced via meta-tests in `tests/test_guidelines_compliance.rs`. These tests:

1. **Fail the build** if any test uses `test_*` naming instead of `should_*`
2. **Warn about missing AAA structure** in integration tests (>10 lines)
3. **Run with every `cargo test`** - no separate linter needed

To check compliance:

```bash
cargo test --test test_guidelines_compliance
```

The meta-test scans all test files (except itself) and reports violations with file paths and line numbers, making it easy to fix non-compliant tests.

## Guidelines

**Arrange:**

- Create test data, objects, setup state
- Use helpers like `tempfile::tempdir()` for isolation
- Keep setup **minimal** — only what's needed for this test
- Comment complex setup: `// Arrange: force WAL rotation by...`

**Act:**

- **Exactly one** operation under test
- Can span multiple lines if it's one logical operation
- Should be obvious what's being tested

**Assert:**

- Verify the expected behavior
- Add comments for non-obvious expectations
- Use exact matches when deterministic
- Check side-effects that prove correctness

### Combined Arrange+Act

For very simple tests, Arrange and Act can be combined if it aids clarity:

```rust
#[test]
fn should_return_none_given_missing_key() {
    // Arrange + Act
    let mt = Memtable::new();
    let result = mt.get(b"nonexistent");

    // Assert
    assert_eq!(result, None);
}
```

But prefer separate sections when **any** complexity is involved.

---

## Async & Timeout Patterns

### Use Timeouts, Not Sleep

❌ **Bad:** Arbitrary sleep

```rust
std::thread::sleep(Duration::from_secs(1));
// Hope something happened...
assert!(condition);
```

✅ **Good:** Timeout with fast failure

```rust
use tokio::time::{timeout, Duration};

let result = timeout(Duration::from_millis(200), operation).await;
assert!(result.is_ok(), "operation timed out");
```

### Async Test Pattern

```rust
#[tokio::test]
async fn should_complete_within_timeout() {
    // Arrange
    let service = Service::new();

    // Act
    let result = timeout(
        Duration::from_millis(100),
        service.async_operation()
    ).await;

    // Assert
    assert!(result.is_ok(), "operation should complete quickly");
    assert_eq!(result.unwrap(), expected_value);
}
```

### Timeout Guidelines

- **Fast tests:** Use 50-200ms timeouts for fast operations
- **Slow operations:** Use minimal timeout needed + margin (e.g., 1s operation → 1.5s timeout)
- **Comment delays:** If a `sleep()` is truly needed, explain why
- **Prefer polling:** For eventual consistency, use retry loops with timeout

### Example: Testing "No Message"

```rust
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn should_receive_no_messages_given_no_publisher() {
    // Arrange
    let mut subscriber = create_subscriber();

    // Act: Try to receive with timeout
    let result = timeout(Duration::from_millis(100), subscriber.next()).await;

    // Assert: Timeout means no message (expected)
    assert!(result.is_err(), "unexpected message received");
}
```

---

## Trait Behavior Tests

When a trait has multiple implementations, write tests that enforce the **trait's contract**, not implementation details.

### Principle

All implementations must pass the same behavioral tests.

### Pattern: Factory Function

```rust
// Shared behavior tests
fn test_compressor_behavior<F, C>(factory: F)
where
    F: Fn() -> C,
    C: Compressor,
{
    // should_roundtrip_given_small_input
    {
        // Arrange
        let codec = factory();  // Fresh instance
        let input = b"hello";

        // Act
        let compressed = codec.compress(input).unwrap();
        let output = codec.decompress(&compressed).unwrap();

        // Assert
        assert_eq!(output.as_slice(), input);
    }

    // should_roundtrip_given_empty_input
    {
        // Arrange
        let codec = factory();  // Fresh instance
        let input: &[u8] = &[];

        // Act
        let compressed = codec.compress(input).unwrap();
        let output = codec.decompress(&compressed).unwrap();

        // Assert
        assert_eq!(output.as_slice(), input);
    }
}

// Apply to each implementation
#[test]
fn noop_codec_behavior() {
    test_compressor_behavior(|| NoopCodec::new());
}

#[test]
fn snappy_codec_behavior() {
    test_compressor_behavior(|| SnappyCodec::new());
}

#[test]
fn lz4_codec_behavior() {
    test_compressor_behavior(|| Lz4Codec::new());
}
```

### Pattern: Macro (for many implementations)

```rust
macro_rules! behavior_tests_for {
    ($modname:ident, $codec_ty:ty, $ctor:expr) => {
        mod $modname {
            use super::*;

            #[test]
            fn should_roundtrip_given_small_input() -> Result<()> {
                let codec = $ctor();
                let input = b"hello world";
                roundtrip(codec, input)
            }

            #[test]
            fn should_roundtrip_given_empty_input() -> Result<()> {
                let codec = $ctor();
                let input: &[u8] = &[];
                roundtrip(codec, input)
            }

            #[test]
            fn should_roundtrip_given_binary_input() -> Result<()> {
                let codec = $ctor();
                let input = &[0u8, 1, 2, 3, 4, 5, 0xff, 0x00, 0x7f];
                roundtrip(codec, input)
            }
        }
    };
}

// Helper function
fn roundtrip<C: Compressor>(codec: C, input: &[u8]) -> Result<()> {
    let compressed = codec.compress(input)?;
    let decompressed = codec.decompress(&compressed)?;
    assert_eq!(decompressed.as_slice(), input);
    Ok(())
}

// Generate tests for each implementation
behavior_tests_for!(noop_codec, NoopCodec, NoopCodec::new);
behavior_tests_for!(snappy_codec, SnappyCodec, SnappyCodec::new);
behavior_tests_for!(lz4_codec, Lz4Codec, Lz4Codec::new);
```

### Benefits

✅ Contract enforcement: All implementations must satisfy the same behaviors  
✅ Zero duplication: Write tests once, run for all implementations  
✅ Easy to extend: New implementation? Add one line  
✅ Isolation: Fresh instance per behavior via factory pattern

### Anti-Pattern

❌ **Don't** copy-paste tests for each implementation — use the patterns above

---

## Table-Driven Tests

For testing the same behavior with multiple inputs, use table-driven tests.

### Sync Pattern

```rust
#[test]
fn should_parse_valid_inputs() {
    // Arrange: table of (name, input, expected)
    let cases = vec![
        ("empty", "", 0),
        ("single", "a", 1),
        ("many", "abc", 3),
        ("unicode", "日本語", 3),
    ];

    for (name, input, expected) in cases {
        // Act
        let result = parse(input);

        // Assert
        assert_eq!(result, expected, "case '{}' failed", name);
    }
}
```

### Async Pattern

```rust
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn should_handle_various_inputs() {
    // Arrange
    let cases = vec![
        ("small", vec![1, 2, 3], 6),
        ("large", vec![10, 20, 30], 60),
        ("empty", vec![], 0),
    ];

    for (name, input, expected) in cases {
        let name = name.to_string();  // Move into async block

        // Act
        let fut = async move {
            let result = async_sum(&input).await;
            assert_eq!(result, expected, "case: {}", name);
        };

        // Assert: Complete within timeout
        let res = timeout(Duration::from_millis(100), fut).await;
        assert!(res.is_ok(), "case '{}' timed out", name);
    }
}
```

### When to Use

✅ **Use table-driven** when:

- Testing the same behavior with different inputs
- Verifying boundary conditions (empty, min, max, etc.)
- Checking multiple valid/invalid input variations

❌ **Don't use** when:

- Testing different behaviors (use separate tests instead)
- Each case needs different setup or assertions
- Failure diagnosis would be unclear

---

## Determinism & Isolation

Tests must be **reliable** and **independent**.

### Rules

1. **No Shared State**  
   Each test creates its own resources

2. **No External Dependencies**  
   Tests should not depend on:

   - Network services (except localhost)
   - File system state outside temp dirs
   - Environment variables
   - Current time (unless explicitly testing time)

3. **Order Independence**  
   Tests can run in any order, including parallel

4. **Cleanup**  
   Use RAII (`tempfile::TempDir`) or explicit cleanup

### Isolation Patterns

**Temp Directories:**

```rust
#[test]
fn should_persist_data() {
    // Arrange: Isolated temp dir, auto-cleaned
    let dir = tempfile::tempdir().unwrap();
    let engine = MidgeEngine::open(MidgeOptions {
        db_path: dir.path().to_path_buf(),
        ..Default::default()
    }).unwrap();

    // ... test ...

    // Cleanup: automatic when `dir` drops
}
```

**In-Memory State:**

```rust
#[test]
fn should_track_metrics() {
    // Arrange: Fresh engine instance (not shared)
    let engine = create_test_engine();

    // ... test ...
}
```

### Avoid

❌ Global mutable state
❌ Singleton patterns without isolation
❌ Tests that depend on execution order
❌ Tests that modify shared file system paths

---

## Assertions

### Prefer Exact Matches

When behavior is deterministic, assert exact values:

✅ **Good:**

```rust
assert_eq!(result, Some(Bytes::from_static(b"value")));
assert_eq!(count, 42);
assert_eq!(rows.len(), 3);
```

❌ **Bad:**

```rust
assert!(count > 0);  // How many? Why not assert the exact count?
```

### Assert What Matters

Don't over-assert internal details:

✅ **Good:** Verify observable behavior

```rust
let stats = file_manager.stats();
assert_eq!(stats.current_total_bytes, 500_000);
assert_eq!(stats.space_utilization(), 0.5);
```

❌ **Bad:** Assert internal implementation details

```rust
// Don't assert HashMap ordering, private fields, etc.
```

### Assertion Messages

Add messages for non-obvious failures:

```rust
assert_eq!(
    actual, expected,
    "Bloom filter false positive rate too high"
);

assert!(
    elapsed < Duration::from_millis(100),
    "Operation took too long: {:?}", elapsed
);
```

### Pattern Matching

For complex types, use pattern matching:

```rust
match result {
    Err(FileManagerError::QuotaExceeded { requested, available }) => {
        assert_eq!(requested, 300_000);
        assert_eq!(available, 200_000);
    }
    _ => panic!("Expected QuotaExceeded error"),
}
```

---

## Negative Testing

Tests must verify error cases, not just happy paths.

### Categories

1. **Validation Errors:** Invalid input
2. **State Errors:** Operation not allowed in current state
3. **Resource Errors:** Quota exceeded, file not found, etc.
4. **Corruption:** Malformed data, checksums, etc.

### Pattern: Validation Errors

```rust
#[test]
fn should_reject_empty_key() {
    // Arrange
    let engine = create_engine();

    // Act
    let result = engine.put(Bytes::new(), Bytes::from("value"));

    // Assert
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::InvalidKey));
}
```

### Pattern: Corruption Testing

```rust
#[test]
fn should_detect_corrupted_wal() {
    // Arrange: Create valid file
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("wal.log");
    std::fs::write(&path, valid_wal_bytes()).unwrap();

    // Act: Corrupt the file
    let mut data = std::fs::read(&path).unwrap();
    data[10] ^= 0xFF;  // Flip bits
    std::fs::write(&path, &data).unwrap();

    // Assert: Replay detects corruption
    let result = replay_wal_file(&path);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::Corruption));
}
```

### Pattern: Resource Limits

```rust
#[test]
fn should_error_when_quota_exceeded() {
    // Arrange
    let fm = FileManager::with_limits(1_000_000, 0);
    fm.register_file(Path::new("f1"), 900_000).unwrap();

    // Act: Exceed quota
    let result = fm.register_file(Path::new("f2"), 200_000);

    // Assert: Error with details
    assert!(result.is_err());
    if let Err(FileManagerError::QuotaExceeded { requested, available }) = result {
        assert_eq!(requested, 200_000);
        assert_eq!(available, 100_000);
    } else {
        panic!("Expected QuotaExceeded error");
    }
}
```

### Edge Cases

Always test:

- Empty collections
- Boundary values (0, MAX, MIN)
- Missing keys
- Duplicate keys
- Concurrent operations (where applicable)

---

## CI Commands

These are our canonical test commands for local development and CI:

### Run All Tests

```powershell
# Full test suite (unit + integration)
cargo test --all

# Unit tests only (tests in src/)
cargo test --lib

# Integration tests only (tests in tests/)
cargo test --tests

# Quiet mode (less output)
cargo test --all --quiet

# Show test output even on success
cargo test --all -- --nocapture
```

### Run Specific Tests

```powershell
# Single integration test file
cargo test --test engine

# Single unit test module
cargo test --lib storage::skiplist

# Single test by name
cargo test should_resolve_put_on_get_local

# All tests in a module (unit tests)
cargo test --lib bloom::tests::
```

### Test Counts

```powershell
# Count unit tests
cargo test --lib --quiet 2>&1 | Select-String "test result"

# Count integration tests
cargo test --tests --quiet 2>&1 | Select-String "test result"

# Count all tests
cargo test --all --quiet 2>&1 | Select-String "test result"
```

### Coverage

```powershell
# Generate coverage report (requires cargo-llvm-cov)
cargo llvm-cov --tests

# HTML coverage report
cargo llvm-cov --tests --html

# Unit tests coverage only
cargo llvm-cov --lib
```

### Performance

```powershell
# Release mode (for benchmarks)
cargo test --release

# Single-threaded (for debugging)
cargo test -- --test-threads=1

# Specific test with timing
cargo test should_insert_and_get_value -- --nocapture --test-threads=1
```

### Watch Mode (with cargo-watch)

```powershell
# Auto-run tests on file changes
cargo watch -x test

# Auto-run unit tests only
cargo watch -x "test --lib"

# Auto-run specific module
cargo watch -x "test --lib skiplist"
```

---

## Quick Reference Checklist

Use this checklist when writing or reviewing tests:

### Test Location

- [ ] **Unit test?** → Place in `src/<module>.rs` within `#[cfg(test)] mod tests { }`
- [ ] **Integration test?** → Place in `tests/<name>.rs`
- [ ] Uses `use super::*;` for unit tests
- [ ] Uses `use cntryl_midge::<module>::<Type>` for integration tests

### Test Structure

- [ ] Name: `should_<outcome>_given_<context>_when_<action>`
- [ ] Clear `// Arrange`, `// Act`, `// Assert` sections
- [ ] Exactly **one** Act operation
- [ ] All assertions verify the **same** behavior

### Organization

- [ ] Unit tests can access private functions/fields
- [ ] Integration tests only use public API
- [ ] Helper functions are clearly separated
- [ ] No duplication between unit and integration tests

### Behavior

- [ ] Test name clearly describes expected behavior
- [ ] Single, focused behavior (not testing multiple things)
- [ ] When test fails, immediately clear what broke
- [ ] No "and" in test name (indicates multiple behaviors)

### Isolation

- [ ] Uses `tempfile::tempdir()` for file system tests
- [ ] No shared global state
- [ ] No external dependencies (except localhost)
- [ ] Can run in any order, including parallel

### Async (if applicable)

- [ ] Uses `#[tokio::test]` for async
- [ ] Uses `timeout()` instead of `sleep()`
- [ ] Timeout duration is justified (50-200ms for fast ops)

### Trait Tests (if applicable)

- [ ] Factory pattern for fresh instances
- [ ] Same tests for all implementations
- [ ] Tests trait contract, not implementation details

### Assertions

- [ ] Exact matches where deterministic
- [ ] Error messages for non-obvious failures
- [ ] Asserts observable behavior, not internals

### Edge Cases

- [ ] Tests error paths (not just happy path)
- [ ] Tests boundary conditions (empty, max, min)
- [ ] Tests invalid input
- [ ] Tests resource limits (where applicable)

---

## Examples Repository

See our actual test files for reference implementations:

**Best Unit Test Examples:**

- `src/storage/skiplist.rs` — Clean unit tests in #[cfg(test)]
- `src/storage/memtable.rs` — Comprehensive coverage
- `src/utils/codec.rs` — Trait behavior tests with macro
- `src/index/bloom.rs` — Well-structured, clear naming
- `src/wal/wal_mem.rs` — Excellent test organization

**Best Integration Test Examples:**

- `tests/engine.rs` — End-to-end engine tests
- `tests/api/transaction.rs` — Perfect Arrange/Act/Assert structure
- `tests/backup.rs` — Multi-module integration
- `tests/compaction/compactor.rs` — Complex integration scenarios
- `tests/api/query.rs` — Public API testing

**For Specific Patterns:**

- Async + Timeout: `tests/utils/rate_limiter.rs`
- Trait Testing: `src/utils/codec.rs`
- Negative Testing: `src/storage/fs.rs`
- Table-Driven: `src/index/bloom.rs` (via macro)
- Integration: `tests/compaction/compactor.rs`
- Unit Tests: `src/storage/skiplist.rs`

---

## Document History

- **2025-10-20:** Major update: Migrated to Rust unit test conventions
  - Moved 70+ tests from `tests/` to `src/` in `#[cfg(test)]` modules
  - Updated file organization to match Rust standards
  - Added decision tree for unit vs integration tests
  - Updated all examples to reflect new structure
  - Test count: 239 tests (182 unit + 57 integration)
- **2025-10-14:** Updated to reflect 214-test suite alignment
- **2025-10-14:** Added real examples from Midge codebase
- **2025-10-14:** Enhanced naming guidelines with decision tree
- **2025-10-14:** Added comprehensive trait testing patterns
- **2025-10-14:** Improved async/timeout guidance

These guidelines evolve with the codebase. When patterns change, update this document to reflect our actual practices.
