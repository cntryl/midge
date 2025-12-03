# Test Utilities

Development utilities for maintaining test quality and consistency.

## validate_tests.rs

A Rust-based tool to validate test compliance with project guidelines.

**Usage:**

```bash
# Check all tests in the project
cargo run --bin validate_tests -- --summary

# Check a specific file
cargo run --bin validate_tests -- --file src/wal/wal_helpers.rs
cargo run --bin validate_tests -- --file tests/engine.rs
```

**What it checks:**

1. **Naming Convention**: All tests must start with `should_`
2. **AAA Structure**: Tests >5 lines must have clear `// Arrange`, `// Act`, `// Assert` comments
3. **Single Behavior**: Tests should not have multiple `// Act` sections
4. **Focused Tests**: Test names containing `_and_` may indicate testing multiple behaviors

**Example Output:**

```
Checking tests in: src/wal/wal_helpers.rs

Results: 4/4 compliant (100.0%)

[OK] should_calculate_varint_len_for_small_values (line 196)
[OK] should_calculate_varint_len_for_medium_values (line 208)
[OK] should_calculate_varint_len_for_large_values (line 220)
[OK] should_calculate_wal_record_len_for_various_operations (line 232)
```

## detect_deadlocks.rs

A static analysis tool to detect potential deadlock patterns in source code.

**Usage:**

```bash
# Scan entire codebase
cargo run --bin detect_deadlocks -- --summary

# Check a specific file
cargo run --bin detect_deadlocks -- --file src/wal/fs/batched_sync.rs
```

**What it detects:**

1. **condvar.wait() without loop** (HIGH): Missing proper condition check after wake-up
2. **Double lock attempts** (HIGH): Same lock acquired twice in quick succession  
3. **.lock().clone() anti-pattern** (MEDIUM): Lock guard dropped before use
4. **Spin loops without parking** (MEDIUM): Atomic loops without condvar fallback
5. **Notify while holding lock** (LOW): Performance issue with condvar notifications

**Example Output:**

```
━━━ HIGH SEVERITY ━━━
  📍 src/example.rs:42
     Pattern: condvar.wait() without loop
     Issue: Condvar.wait() outside of loop - may miss notifications
     Fix: Use .wait_while() or wrap in loop with condition check
```

For detailed debugging patterns, see `docs/DEADLOCK_DETECTION.md`.

## deadlock_detector.rs

Runtime deadlock detection utilities for integration tests (in `tests/common/`).

**Usage in tests:**

```rust
mod common;
use common::deadlock_detector::DeadlockDetector;
use std::time::Duration;

#[test]
fn my_concurrent_test() {
    let _detector = DeadlockDetector::new("my_test", Duration::from_secs(10));
    // Test automatically warns if it takes >10 seconds
}
```

See `tests/deadlock_detector_demo.rs` for more examples.

## add_aaa.rs

Automatically adds AAA (Arrange-Act-Assert) comments to test files for better readability.

**Usage:**

```bash
cargo run --bin add_aaa --manifest-path testutils/Cargo.toml -- tests/some_test.rs
```

Or create a standalone script to run it.
