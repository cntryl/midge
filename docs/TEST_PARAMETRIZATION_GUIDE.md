# Test Parametrization Guide

This guide explains how to write parametrized integration tests across all storage modes (Memory, FS, Cloud) using the testkit helpers.

## Quick Start

Every test should follow this pattern:

```rust
#[test]
fn should_<behavior>_given_<context>_when_<condition>() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open(opts, mode);
        let cf = engine.default_column_family();
        
        // Act
        // ... test logic here ...
        
        // Assert
        assert_eq!(result, expected, "description in mode: {}", mode);
    });
}
```

## Available Helper Functions

### Storage Mode Lists

Choose the appropriate set of modes for your test:

#### `all_storage_modes_new() -> Vec<&'static str>`
- **Includes:** Memory, Local (FS), Cloud
- **Use for:** General logic tests (put, get, delete, iterators, snapshots, etc.)
- **Example:** `engine_basic.rs`, `engine_iterators.rs`

#### `durable_storage_modes() -> Vec<&'static str>`
- **Includes:** Local (FS), Cloud only
- **Use for:** Tests requiring persistence (WAL, recovery, durability, compaction)
- **Example:** `durability_wal.rs`, `durability_recovery.rs`, `sst_writer.rs`

#### `memory_storage_modes() -> Vec<&'static str>`
- **Includes:** Memory only
- **Use for:** Tests explicitly validating non-persistent behavior
- **Example:** One test in `transaction_spill.rs`

#### `filesystem_storage_modes() -> Vec<&'static str>`
- **Includes:** Local (FS) only
- **Use for:** Tests requiring filesystem-specific behavior
- **Example:** (rare) filesystem integrity tests

### Engine Opening

#### `opts_for_mode(mode: &str) -> MidgeOptions`
Generates appropriate `MidgeOptions` for a storage mode.

**Modes recognized:**
- `"memory"` → `StorageMode::Memory` (no WAL sync)
- `"local"` → `StorageMode::LocalDisk` (WAL sync enabled)
- `"cloud"` → `StorageMode::CloudBacked` (WAL sync enabled)

```rust
let opts = opts_for_mode("memory");
let engine = open_engine(opts)?;
```

#### `for_each_storage_mode<F>(modes: &[&str], test_fn: F)`
Parametrization helper that loops over modes and invokes test closure.

**Signature:**
```rust
pub fn for_each_storage_mode<F>(modes: &[&str], test_fn: F)
where
    F: Fn(&str, MidgeOptions),
```

**Usage:**
```rust
for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
    // Your test runs 3 times: once for each mode
    let engine = open_engine(opts).expect("open");
    // ... test logic ...
});
```

#### `open_engine(opts: MidgeOptions) -> MidgeResult<MidgeEngine>`
Opens an engine with the given options. Use with explicit error handling per mode:

```rust
fn open(opts: MidgeOptions, mode: &str) -> MidgeEngine {
    open_engine(opts).unwrap_or_else(|e| {
        panic!("open_engine failed in mode {}: {}", mode, e)
    })
}
```

## Naming Convention

Follow this convention for all tests:

```
should_<behavior>_given_<context>_when_<condition>
```

**Examples:**
- `should_get_value_given_existing_key_when_put`
- `should_return_none_given_deleted_key_when_get`
- `should_overwrite_value_given_existing_key_when_put`
- `should_persist_write_given_fsync_enabled_when_crash_occurs`

## Test Structure: Arrange / Act / Assert

Every test must clearly separate these three phases:

```rust
#[test]
fn should_write_and_read_when_sequential() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange: set up engine, data, state
        let engine = open(opts, mode);
        let cf = engine.default_column_family();

        // Act: perform the operation(s) being tested
        engine.put(cf, b"key", b"value").expect("put");

        // Assert: verify the outcome
        let got = engine.get(cf, b"key").expect("get");
        assert_eq!(got, Some(Bytes::from_static(b"value")), "...in mode: {}", mode);
    });
}
```

**Key points:**
- Comments must be exactly `// Arrange`, `// Act`, `// Assert`
- Small tests (<5 lines) may omit comments; larger tests must include them
- Assertions should include mode context: `"message in mode: {}", mode`

## Error Messages

Always include storage mode in assertion messages:

```rust
// ❌ Bad
assert_eq!(got, expected);

// ✅ Good
assert_eq!(got, expected, "unexpected value in mode: {}", mode);
```

This makes failures much clearer when tests run across 3 modes.

## Example: Full Test File Structure

```rust
//! Clear description of what this file tests.
//!
//! These tests run across all storage modes unless noted otherwise.

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, testkit::*};

/// Helper: unwrap engine open with consistent error context.
fn open(opts: MidgeOptions, mode: &str) -> MidgeEngine {
    open_engine(opts).unwrap_or_else(|e| {
        panic!("open_engine failed in mode {}: {}", mode, e)
    })
}

// ============================================================================
// TEST SUITE NAME
// ============================================================================

#[test]
fn should_get_value_given_existing_key_when_put() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open(opts, mode);
        let cf = engine.default_column_family();

        // Act
        engine.put(cf, b"key", b"value").expect("put");

        // Assert
        let got = engine.get(cf, b"key").expect("get");
        assert_eq!(got, Some(Bytes::from_static(b"value")), "in mode: {}", mode);
    });
}

#[test]
fn should_handle_many_operations_when_sequential() {
    // Use durable_storage_modes() if this test requires persistence
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open(opts, mode);
        let cf = engine.default_column_family();
        const COUNT: usize = 100;

        // Act
        for i in 0..COUNT {
            let key = format!("key_{i}");
            let val = format!("value_{i}");
            engine.put(cf, key.as_bytes(), val.as_bytes()).expect("put");
        }

        // Assert
        for i in 0..COUNT {
            let key = format!("key_{i}");
            let expected = format!("value_{i}");
            let got = engine.get(cf, key.as_bytes()).expect("get");
            assert_eq!(got, Some(Bytes::from(expected)), "key {i} in mode: {}", mode);
        }
    });
}
```

## Storage Mode Decision Matrix

| Test Type | Memory | FS | Cloud | Helper |
|-----------|--------|----|----- |--------|
| Logic (put, get, delete, iterators) | ✅ | ✅ | ✅ | `all_storage_modes_new()` |
| Durability (WAL, recovery, crash) | ❌ | ✅ | ✅ | `durable_storage_modes()` |
| Persistence (SST, compaction, flush) | ❌ | ✅ | ✅ | `durable_storage_modes()` |
| Non-persistent behavior | ✅ | ❌ | ❌ | `memory_storage_modes()` |
| Filesystem-specific | ❌ | ✅ | ❌ | `filesystem_storage_modes()` |

## Common Patterns

### Testing with Binary Data
```rust
let data = vec![0, 1, 2, 255, 254];
engine.put(cf, b"key", &data).expect("put");
let got = engine.get(cf, b"key").expect("get");
assert_eq!(got, Some(Bytes::from(data)), "in mode: {}", mode);
```

### Testing Deletes
```rust
engine.put(cf, b"key", b"value").expect("put");
engine.delete(cf, b"key").expect("delete");
let got = engine.get(cf, b"key").expect("get");
assert_eq!(got, None, "expected None after delete in mode: {}", mode);
```

### Testing Multiple Keys
```rust
for i in 0..100 {
    let key = format!("key_{i}");
    engine.put(cf, key.as_bytes(), b"val").expect("put");
}
```

## Reference Implementation

See `tests/engine_basic.rs` for the canonical polished example of this pattern.

## FAQ

**Q: Why do we parametrize inside the test instead of using `#[test]` macros?**  
A: This approach is simpler, provides better error context (mode in failure messages), and avoids macro complexity while remaining highly readable.

**Q: What if my test only needs to run on certain modes?**  
A: Use the appropriate helper:
```rust
for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })
for_each_storage_mode(&memory_storage_modes(), |mode, opts| { ... })
```

**Q: How do I debug a test failure in a specific mode?**  
A: The error message will say which mode failed. Run tests with `--nocapture`:
```bash
cargo test --test engine_basic -- --nocapture --test-threads=1
```

**Q: Can I use traditional test assertions?**  
A: Yes, but always include the mode in the message:
```rust
assert!(condition, "assertion failed in mode: {}", mode);
```

**Q: What goes in the `open()` helper?**  
A: This is a convenience function to convert panic to a more informative message. Every test file should define it.
