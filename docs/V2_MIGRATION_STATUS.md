# V2 API Migration - Broken Call Sites

**Status:** API replaced, now fixing call sites one by one  
**Date:** January 8, 2026

---

## Summary of Changes

### What Changed
1. ✅ Removed `KvTransaction` trait
2. ✅ Removed `TransactionImpl` wrapper
3. ✅ Made `WriteOptions` require explicit policy (no Default)
4. ✅ Updated `WriteOptions` to use `DurabilityPolicy` enum
5. ✅ Made internal modules `pub(crate)` (hidden from public API)
6. ✅ Removed convenience methods from `MidgeEngine` (will be removed next)

### What Needs Fixing

#### In `src/engine/mod.rs`:
- Lines 1305, 1343: `begin_transaction` methods return `Box<dyn KvTransaction>` → should return `Transaction`
- Lines 1330, 1367: `TransactionImpl::new()` calls → should create `Transaction` directly
- Line 1432: `commit_transaction_boxed` takes `Box<dyn KvTransaction>` → should take `Transaction`

#### Expected Additional Breakages (not yet compiled):
- All tests using `put()`, `get()`, `delete()` directly on engine
- All benchmarks using convenience methods
- All examples using old API
- Integration tests using `WriteBatch` without transactions

---

## Compilation Errors

```
error[E0433]: failed to resolve: could not find `TransactionImpl` in `api`
    --> src\engine\mod.rs:1330:24
error[E0433]: failed to resolve: could not find `TransactionImpl` in `api`
    --> src\engine\mod.rs:1367:24
error[E0405]: cannot find trait `KvTransaction` in module `api`
    --> src\engine\mod.rs:1305:35
error[E0405]: cannot find trait `KvTransaction` in module `api`
    --> src\engine\mod.rs:1343:35
error[E0405]: cannot find trait `KvTransaction` in module `api`
    --> src\engine\mod.rs:1432:31
```

---

## Next Steps

1. Fix `begin_transaction` methods in engine/mod.rs
2. Fix `commit_transaction_boxed` method
3. Remove old convenience methods (`put`, `get`, `delete`, etc.)
4. Compile again to find next batch of errors
5. Fix tests one by one
6. Fix benchmarks one by one
7. Fix examples one by one
8. Update integration tests

---

## Call Sites to Fix Per Module

### Priority 1: Core Engine (blocking everything)
- `src/engine/mod.rs` - transaction creation methods

### Priority 2: Tests (most numerous)
- `tests/*.rs` - all integration tests
- `src/engine/*.rs` - unit tests
- `src/runtime/*.rs` - unit tests

### Priority 3: Benchmarks
- `benches/tier1_*.rs` - hotpath benches
- `benches/tier2_*.rs` - subsystem benches  
- `benches/tier3_*.rs` - system benches
- `benches/tier4_*.rs` - workload benches

### Priority 4: Examples
- `examples/basic_usage.rs`
- `examples/metrics_usage.rs`
- `examples/smart_config.rs`

---

## Strategy

Fix in layers:
1. Fix core engine methods first (enables compilation)
2. Let all tests/benches/examples break
3. Fix them systematically one file at a time
4. Document migration patterns as we go

This is intentional breakage - we're forcing the entire codebase to adopt the new explicit transaction model.
