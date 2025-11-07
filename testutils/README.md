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

## add_aaa.rs

Automatically adds AAA (Arrange-Act-Assert) comments to test files for better readability.

**Usage:**

```bash
cargo run --bin add_aaa --manifest-path testutils/Cargo.toml -- tests/some_test.rs
```

Or create a standalone script to run it.
