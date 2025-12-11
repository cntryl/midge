# Parametrization Helpers Summary

This is a quick reference for the testkit parametrization infrastructure.

## What Changed

### Before
Each test looped over modes manually, risking duplication and inconsistency:
```rust
#[test]
fn some_test() {
    for mode in vec!["Memory", "LocalDisk"] {
        let opts = match mode { /* ... */ };
        let engine = /* ... */;
        // test code
    }
}
```

### After
Tests use standardized helpers for clean, consistent parametrization:
```rust
#[test]
fn some_test() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        let engine = open(opts, mode);
        // test code
    });
}
```

## New Functions in `src/testkit/mod.rs`

| Function | Returns | Purpose |
|----------|---------|---------|
| `all_storage_modes_new()` | `Vec<&'static str>` | All modes: memory, local, cloud |
| `durable_storage_modes()` | `Vec<&'static str>` | Durable only: local, cloud |
| `memory_storage_modes()` | `Vec<&'static str>` | Memory only |
| `filesystem_storage_modes()` | `Vec<&'static str>` | FS only: local |
| `opts_for_mode(mode: &str)` | `MidgeOptions` | Generate options for a mode |
| `for_each_storage_mode(modes, fn)` | `()` | Loop closure over modes |
| `open_engine(opts)` | `MidgeResult<MidgeEngine>` | Open with options |

## Naming Conventions

### Function Naming
- **Mode lists** are lowercase: `memory`, `local`, `cloud`
- **Old API** uses uppercase (backward compat): `Memory`, `LocalDisk`
- **New API** uses lowercase everywhere for consistency

### Test Naming
```
should_<behavior>_given_<context>_when_<condition>
```

Examples:
- `should_get_value_given_existing_key_when_put`
- `should_return_none_given_nonexistent_key_when_get`
- `should_overwrite_value_given_existing_key_when_put`

## Storage Mode Coverage

### Memory Mode
- ✅ Logical correctness tests (put, get, delete, iterators, snapshots, transactions)
- ❌ Durability tests (WAL, recovery, persistence)
- ❌ Crash/restart tests

### Local (FS) Mode
- ✅ All Memory tests
- ✅ All durability tests
- ✅ Crash/restart tests
- ✅ Filesystem-specific tests

### Cloud Mode
- ✅ All Memory tests
- ✅ All durability tests (via local cache)
- ✅ Crash/restart tests
- ✅ Cloud-specific behavior

## Test Structure Pattern

Every test file should follow this skeleton:

```rust
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, testkit::*};

/// Helper: unwrap engine open with consistent error context.
fn open(opts: MidgeOptions, mode: &str) -> MidgeEngine {
    open_engine(opts).unwrap_or_else(|e| {
        panic!("open_engine failed in mode {}: {}", mode, e)
    })
}

#[test]
fn should_<behavior>_given_<context>_when_<condition>() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange: setup
        let engine = open(opts, mode);
        
        // Act: perform operation
        engine.put(...).expect("put");
        
        // Assert: verify result
        assert_eq!(result, expected, "description in mode: {}", mode);
    });
}
```

## Implementation Notes

- **Modes are lowercase strings:** `"memory"`, `"local"`, `"cloud"`
- **Options are generated per-mode:** memory has `wal_sync=false`, others have `wal_sync=true`
- **Temp directories:** Each mode gets a fresh `TempDir` via `test_temp_dir()`
- **Cleanup is automatic:** `TempDir` is dropped when scope exits

## Migration Path

For old tests using uppercase modes:
1. Keep using `all_storage_modes()` (returns `["Memory", "LocalDisk"]`) if migrating incrementally
2. Or update to `all_storage_modes_new()` (returns `["memory", "local", "cloud"]`) for consistency
3. Use `create_storage_mode(mode)` for old-style triple return if needed

## Example: Single Test

```rust
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
        assert_eq!(
            got,
            Some(Bytes::from_static(b"value")),
            "unexpected value in mode: {}",
            mode
        );
    });
}
```

This single test runs **3 times** automatically:
1. With Memory mode options
2. With Local (FS) mode options
3. With Cloud mode options

All assertions automatically include which mode failed.

## Files Modified

- `src/testkit/mod.rs` - Added parametrization helpers
- `tests/engine_basic.rs` - Updated to use new pattern (canonical reference)
- `docs/TEST_PARAMETRIZATION_GUIDE.md` - Detailed guide
- `docs/PARAMETRIZATION_SUMMARY.md` - This file

## Next Steps

1. Use this pattern in all new test files
2. Reference `tests/engine_basic.rs` as the canonical example
3. Follow test naming conventions consistently
4. Include mode context in all assertion messages
5. Choose appropriate mode list (`all_`, `durable_`, `memory_`, `filesystem_`)
