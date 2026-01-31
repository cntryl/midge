# Testing

Midge has a mix of unit tests (in `src/`) and integration tests (in `tests/`).

## Running tests

- Run everything:

  ```bash
  cargo test
  ```

- Run a subset by name (Rust test filter):

  ```bash
  cargo test should_open
  ```

- Run a specific integration test file:

  ```bash
  cargo test --test engine_basic
  ```

## Test conventions

### Naming

Use the convention:

- `should_{action}_when_{context}`

This makes it easy to scan failures and supports automated validation.

### Arrange / Act / Assert (AAA)

For non-trivial tests, use a clear AAA structure:

```rust
#[test]
fn should_do_the_thing_when_condition() {
    // Arrange

    // Act

    // Assert
}
```

Keep only one `// Act` section per test (multi-behavior tests are harder to debug).

## Test validation script

There is a lightweight validator that checks naming and AAA structure:

```bash
python ./scripts/validate_tests.py --summary
```

Notes:

- The current codebase contains legacy tests that don’t fully comply yet.
- When adding or modifying tests, prefer making them compliant and avoid increasing the total violation count.

## Where to add tests

- **Unit tests** (`src/**`): good for small components and invariants.
- **Integration tests** (`tests/**`): preferred for end-to-end behaviors (engine API, recovery, durability, compaction interactions).

## Debugging failures

- Use `RUST_BACKTRACE=1` to get stack traces:

  ```bash
  RUST_BACKTRACE=1 cargo test
  ```

- For flaky tests, try running the same test repeatedly:

  ```bash
  cargo test should_x -- --nocapture
  ```
