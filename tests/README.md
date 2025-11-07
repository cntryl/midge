# Integration Tests

This directory contains **integration tests** for Midge. These tests verify that multiple modules work together correctly and test the public API of the database engine.

## Test Organization Philosophy

Midge follows a **strict separation** between unit tests and integration tests:

###Unit Tests (in `src/`)

- Located in `src/` files within `#[cfg(test)] mod tests { }` blocks
- Test **single modules** in isolation with access to private internals
- Test individual components, functions, structs, and traits
- Run with: `cargo test --lib`

**Examples:**

- `src/storage/skiplist.rs` - Tests skiplist operations
- `src/utils/cache.rs` - Tests cache internals
- `src/wal/wal_fs.rs` - Tests WAL file operations
- `src/utils/internal_key.rs` - Tests key encoding/decoding

**Total Unit Tests:** 688 tests

### Integration Tests (in `tests/`)

- Located in this `tests/` directory as **flat, root-level files**
- Test **multiple modules working together** or the public API only
- Only have access to public interfaces (`use midge::*`)
- Test cross-cutting concerns and end-to-end scenarios
- Run with: `cargo test --tests`

**Examples:**

- `engine.rs` - Tests MidgeEngine public API (55 tests)
- `durability_wal.rs` - Tests WAL persistence end-to-end (6 tests)
- `crash_recovery_matrix.rs` - Crash recovery verification (3 tests)

**Total Integration Tests:** 139 tests (14 test files)

## Directory Structure

**IMPORTANT RULES:**

1. **Flat structure only** - All integration test files MUST be at `tests/` root level
2. **No tests in subdirectories** - Subdirectories are for helper modules only (no `#[test]` attributes)
3. **Each file = separate test binary** - Each `.rs` file in root becomes its own test executable
4. **Integration tests only** - Use `src/` for unit tests, `tests/` for integration tests

### Current Structure

```
tests/
├── README.md                          (this file)
│
├── ─── Integration Test Files (14 files, 139 tests) ───
├── crash_recovery_matrix.rs           Crash recovery proof (3 tests: 1 smoke + 2 ignored)
├── debug_wal_replay.rs                WAL replay debugging (1 test)
├── durability_cloud.rs                Cloud upload durability (12 tests)
├── durability_compaction.rs           Compaction data preservation (9 tests)
├── durability_manifest.rs             Manifest atomicity & flush (10 tests)
├── durability_recovery.rs             Crash recovery semantics (10 tests)
├── durability_wal.rs                  WAL persistence & replay (6 tests)
├── engine.rs                          Core engine API tests (55 tests)
├── error.rs                           Error handling tests (12 tests)
├── health.rs                          Health monitoring tests (9 tests)
├── minimal_repro.rs                   Minimal reproduction cases (2 tests)
├── read_latency_smoke.rs              Read latency smoke tests (3 tests)
├── test_guidelines_compliance.rs      Test quality meta-tests (4 tests)
├── ycsb_smoke.rs                      YCSB workload smoke tests (3 tests)
│
├── ─── Helper Modules (Subdirectories - NO #[test] attributes) ───
├── cloud/                             Cloud test helpers
│   └── cloud.rs                       Shared cloud test utilities
├── common/                            Common test utilities
│   └── mod.rs                         Test helper functions
└── verification/                      Verification helpers
    └── (verification utilities)
```

### Why Flat Structure?

1. **Cargo auto-discovery** - Only root-level `.rs` files are discovered as test binaries
2. **Clear separation** - Easy to see all integration tests at a glance
3. **No hidden tests** - Prevents accidental dead code in subdirectories (we deleted 306!)
4. **Standard Rust convention** - Follows official Rust testing guidelines

### Adding New Integration Tests

**DO:**

- ✅ Create new test files at `tests/` root: `tests/new_feature.rs`
- ✅ Use descriptive names: `durability_*.rs`, `engine.rs`, etc.
- ✅ Test public API only (no access to private internals)
- ✅ Test cross-module interactions

**DON'T:**

- ❌ Create test files in subdirectories (`tests/api/something.rs`)
- ❌ Add `#[test]` attributes to files in `common/`, `cloud/`, `verification/`
- ❌ Test single-module logic (use unit tests in `src/` instead)
- ❌ Import private module internals

## Running Tests

### Run All Tests (Unit + Integration)

```bash
cargo test
```

### Run Only Integration Tests

```bash
cargo test --tests
```

### Run Only Unit Tests

```bash
cargo test --lib
```

### Run Specific Integration Test File

```bash
cargo test --test engine
cargo test --test durability_wal
```

### Run Tests With Output

```bash
cargo test -- --nocapture
```

### Run Tests in Release Mode (Faster)

```bash
cargo test --release
```

### Run Ignored/Long-Running Tests

Some tests are marked `#[ignore]` because they take significant time:

```bash
# Run all ignored tests
cargo test -- --ignored --nocapture

# Run 1K crash scenarios (~1.4 hours)
cargo test --test crash_recovery_matrix should_survive_crash_recovery_matrix_1000_scenarios -- --ignored --nocapture --test-threads=1

# Run 10K crash scenarios proof (~14 hours)
cargo test --test crash_recovery_matrix should_survive_10k_crash_scenarios -- --ignored --nocapture --test-threads=1
```

## Test Categories

### Core Engine Tests (67 tests)

- `engine.rs` - CRUD operations, persistence, recovery (55 tests)
- `error.rs` - Error handling and propagation (12 tests)

### Durability Test Suite (47 tests)

Comprehensive tests verifying data persistence guarantees:

- `durability_wal.rs` - WAL persistence (6 tests)
- `durability_manifest.rs` - Manifest atomicity & flush (10 tests)
- `durability_compaction.rs` - Compaction data preservation (9 tests)
- `durability_recovery.rs` - Crash recovery semantics (10 tests)
- `durability_cloud.rs` - Cloud upload durability (12 tests)

### Verification & Proof Tests (3 tests)

- `crash_recovery_matrix.rs` - Crash recovery proof (1 smoke + 2 ignored long-running)
  - Smoke: 5 scenarios (~25s, runs with normal suite)
  - 1K: 200 iterations × 5 crash points (~1.4 hours, manual)
  - 10K: 2,000 iterations × 5 crash points (~14 hours, manual)

### Workload Tests (3 tests)

- `ycsb_smoke.rs` - YCSB workload smoke tests (Workload A, B, C)

### Debug & Development (10 tests)

- `minimal_repro.rs` - Minimal reproduction cases (2 tests)
- `debug_wal_replay.rs` - WAL replay debugging (1 test)
- `read_latency_smoke.rs` - Read latency smoke tests (3 tests)
- `test_guidelines_compliance.rs` - Test quality meta-tests (4 tests)

### Health & Monitoring (9 tests)

- `health.rs` - Health monitoring and rehydration

## Writing New Tests

Before writing a test, decide where it belongs:

**Integration Test (goes in `tests/<name>.rs` at root)?**

- ✅ Tests multiple modules working together
- ✅ Tests public API of MidgeEngine
- ✅ Tests end-to-end workflows (backup, recovery, compaction)
- ✅ Tests cross-cutting concerns (TTL, transactions, cloud sync)
- ✅ Only needs access to public interfaces

**Unit Test (goes in `src/<module>.rs` within `#[cfg(test)] mod tests`)?**

- ✅ Tests a single module in isolation
- ✅ Needs access to private functions or fields
- ✅ Tests internal implementation details
- ✅ Tests individual utility functions or data structures

See the project's GitHub Copilot instructions (`.github/copilot-instructions.md`) for comprehensive testing guidelines including:

- Naming conventions (`should_*` pattern)
- Test structure (Arrange/Act/Assert)
- Single behavior principle
- Best practices

## Current Test Stats

**Total Tests:** 827 tests (688 unit + 139 integration)

- **Unit Tests (688)** - In `src/` files
- **Integration Tests (139)** - In `tests/` root files
  - Core Engine: 67 tests
  - Durability Suite: 47 tests
  - Verification: 3 tests (1 runs normally, 2 ignored)
  - Workloads: 3 tests
  - Debug/Development: 10 tests
  - Health: 9 tests

**All tests passing** (688/688 unit, 139/139 integration when non-ignored)

**Dead Code Cleanup (Oct 29, 2025):**

- Deleted 42 test files with 306 unreachable tests in subdirectories
- Removed 9 empty subdirectory structures
- Cleaned up validator confusion (was finding 1125 tests, now finds 821)

## CI/CD

Tests run automatically on:

- Every pull request
- Merges to main branch
- Nightly builds

Use `cargo test` locally before pushing to ensure all tests pass.
