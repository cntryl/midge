# Test Timeout Utility

## Overview

Midge provides utilities to detect hanging tests by wrapping test code in timeouts. If a test doesn't complete within the specified duration, it's marked as failed with a clear timeout message.

## Why Use Timeouts?

Tests can hang due to:
- **Deadlocks** in concurrent code
- **Infinite retry loops** in cloud/network operations
- **Blocking operations** that never complete
- **Engine shutdown issues** when background threads don't exit

Without timeouts, hanging tests block the test suite indefinitely. With timeouts, you get:
- **Fast feedback** - tests fail quickly instead of hanging forever
- **Clear diagnostics** - timeout errors indicate which test is problematic
- **Better CI/CD** - test suites complete even if individual tests hang

## Two Approaches

### Approach 1: Use Timeout-Protected Engine Helpers (Recommended)

For tests using `with_engine` or `with_engine_restart`, use the timeout-protected versions:

```rust
use common::{with_engine_restart_timeout, with_engine_timeout};

#[test]
fn test_engine_restart() {
    let dir = test_temp_dir();
    let opts = cloud_backed_opts(dir.path().to_path_buf());
    
    // Automatic 60-second timeout
    with_engine_restart_timeout(
        opts,
        |eng| {
            eng.put(&eng.default_column_family(), b"key", b"value").unwrap();
        },
        |eng| {
            assert_get_equals(eng, b"key", b"value");
        }
    );
}
```

**Benefits:**
- ✅ No need to capture variables with `move`
- ✅ Clean, readable test code  
- ✅ Protects the most common hang points (engine lifecycle)
- ✅ Consistent timeout durations across tests

### Approach 2: Manual Timeout Wrapping

For custom test scenarios or non-engine tests:

## Usage

### Basic Example

```rust
use std::time::Duration;

#[test]
fn my_test() {
    run_with_timeout(
        || {
            // Your test code here
            let engine = MidgeEngine::open(opts).unwrap();
            engine.put(b"key", b"value").unwrap();
        },
        Duration::from_secs(30),  // 30-second timeout
    ).expect("Test should not hang");
}
```

### With Engine Restart

```rust
#[test]
fn test_persistence() {
    run_with_timeout(
        || {
            let dir = test_temp_dir();
            let opts = durability_opts(dir.path().to_path_buf());
            
            with_engine_restart(
                opts,
                |eng| {
                    // Write data
                    eng.put(&eng.default_column_family(), b"key", b"value").unwrap();
                },
                |eng| {
                    // Verify after restart
                    assert_get_equals(eng, b"key", b"value");
                },
            );
        },
        Duration::from_secs(30),
    ).expect("Engine restart should complete without hanging");
}
```

### CloudBacked Tests

Cloud operations are particularly prone to hanging due to retry loops:

```rust
#[test]
fn test_cloud_upload() {
    use std::time::Duration;

    run_with_timeout(
        || {
            let backend = Arc::new(MockCloudBackend::new());
            backend.set_fail_upload_after(1);  // Simulate failures
            
            let opts = MidgeOptions {
                storage_mode: StorageMode::CloudBacked {
                    // ... config ...
                },
                // ...
            };
            
            let eng = MidgeEngine::open(opts).unwrap();
            // Test operations...
        },
        Duration::from_secs(30),
    ).expect("Cloud test should complete within 30 seconds");
}
```

## Choosing Timeout Duration

**Short operations** (in-memory, no I/O):
- Use 5-10 seconds
- Examples: memtable operations, parsing, validation

**Medium operations** (disk I/O, flushes):
- Use 15-30 seconds
- Examples: SST writes, WAL flushes, basic engine operations

**Long operations** (compaction, bulk data):
- Use 60+ seconds
- Examples: multi-level compaction, large dataset tests

**Stress tests**:
- Use 120+ seconds or disable timeout
- Mark with `#[ignore]` if they're intentionally long-running

## Error Messages

When a timeout occurs, you'll see:
```
Test timed out after 30s - likely hanging
```

When a test panics (not a hang):
```
Test panicked (but did not hang)
```

## When NOT to Use Timeouts

- **Micro-benchmarks** - timeouts add overhead
- **Integration tests with external services** - real network latency varies
- **Load/soak tests** - intentionally long-running
- **Simple unit tests** - <100ms execution time, timeout overhead isn't worth it

## Implementation Details

- Runs test code in a separate thread
- Polls `thread::is_finished()` every 100ms
- Cannot forcibly kill hung threads (Rust limitation)
- Hung threads persist until process exit, but test runner continues

## Examples

See `tests/test_timeout_demo.rs` for:
- ✅ Fast operations completing within timeout
- ✅ Panic detection (not treated as hang)
- ❌ Infinite loop detection (ignored by default)
- ✅ Engine restart with timeout
- ❌ Hanging engine operations (ignored by default)

## Tips

1. **Start with longer timeouts** - Better to have false negatives (hung tests that pass) than false positives (slow tests that fail)

2. **Use descriptive expect messages**:
   ```rust
   .expect("CloudBacked engine should shutdown cleanly within 30 seconds")
   ```

3. **Group tests by timeout needs** - Put slow tests in separate files with appropriate timeouts

4. **CI considerations** - CI machines may be slower; use 2x local timeout for CI

5. **Debugging hung tests** - If a test times out, remove the timeout wrapper temporarily and attach a debugger to see where it's stuck
