# Implementation Checklist & Quick Reference

## Critical Path Checklist

### Phase 1: Signatures & Infrastructure (Est. 2-3 hours)

- [ ] **Item 1.1**: Fix engine method signatures
  - [ ] Review test calls in `tests/engine_basic.rs`
  - [ ] Verify `engine.put()` uses 2 args (key, value)
  - [ ] Verify `engine.get()` uses 1 arg (key)
  - [ ] Run: `cargo build --tests` → should pass for engine_basic
  - Test file: `tests/engine_basic.rs`

- [ ] **Item 1.2**: Expose open_with_options()
  - [ ] Check implementation in `src/engine/mod.rs` line ~110
  - [ ] Ensure public visibility (not just internal)
  - [ ] Verify signature: `pub fn open_with_options(MidgeOptions) -> MidgeResult<Self>`
  - [ ] Run: `cargo build --lib` → should compile
  - Code file: `src/engine/mod.rs`

- [ ] **Item 1.3**: Add column family creation message
  - [ ] Add `ManifestCreateColumnFamily` handler to event_loop.rs
  - [ ] Update RuntimeResponse enum with `ColumnFamilyCreated { cf_id: u32 }`
  - [ ] Add manifest actor method `handle_create_column_family()`
  - [ ] Add engine method `create_column_family(name) -> MidgeResult<ColumnFamilyId>`
  - Code files: `src/runtime/event_loop.rs`, `src/runtime/actors/manifest.rs`, `src/engine/mod.rs`

### Phase 2: Read Path (Est. 2-3 hours)

- [ ] **Item 2.1**: Implement RuntimeMsg::Read handler
  - [ ] Add match arm in event_loop.rs for `RuntimeMsg::Read`
  - [ ] Query active memtable first (state.get_active_memtable)
  - [ ] Query immutable memtables next (in FIFO order)
  - [ ] Query manifest for SST files
  - [ ] Open SST readers and search for key
  - [ ] Return ReadValue response
  - Code file: `src/runtime/event_loop.rs` (~100 lines)
  
  **Validation**:
  ```bash
  cargo test engine_basic::should_get_value_given_existing_key_when_put -- --nocapture
  cargo test engine_basic::should_return_none_given_nonexistent_key_when_get -- --nocapture
  ```

---

## Supporting Items Checklist

### Phase 3: Batch & Snapshot (Est. 2-3 hours)

- [ ] **Item 3.1**: WriteBatch struct
  - [ ] Define `WriteBatch` struct with ops vector
  - [ ] Implement `.put()`, `.delete()` methods
  - [ ] Define `WriteOp` enum
  - Code file: `src/engine/api/write_batch.rs` (~40 lines)

- [ ] **Item 3.2**: Engine write_batch() method
  - [ ] Add `engine.write_batch(&batch) -> MidgeResult<()>`
  - [ ] Increment sequence once, use seq+offset for each op
  - [ ] Write to local memtable
  - [ ] Send WalAppend messages to runtime
  - Code file: `src/engine/mod.rs` (~40 lines)
  
  **Validation**:
  ```bash
  cargo test engine_basic::should_write_100_key_values_in_batch_when_batch_write -- --nocapture
  ```

- [ ] **Item 3.3**: Snapshot support
  - [ ] Define `Snapshot { sequence: u64 }`
  - [ ] Implement `engine.get_snapshot() -> Snapshot`
  - [ ] Implement `engine.get_at_snapshot(snap, key) -> MidgeResult<Option<Bytes>>`
  - Code file: `src/engine/api/snapshot.rs` (~30 lines) + `src/engine/mod.rs` (~30 lines)
  
  **Validation**:
  ```bash
  cargo test engine_snapshots -- --nocapture
  ```

### Phase 4: Range Operations (Est. 3-4 hours)

- [ ] **Item 4.1**: Iterator trait & builder
  - [ ] Define `pub struct Iterator { /* buffered results */ }`
  - [ ] Implement `.next() -> Option<(Bytes, Bytes)>`
  - [ ] Define `IteratorBuilder` with start_key, end_key, snapshot
  - Code file: `src/engine/api/iterator.rs` (~60 lines)

- [ ] **Item 4.2**: Engine range_cf() method
  - [ ] Implement `engine.range_cf(&cf, start, end) -> Vec<(K,V)>`
  - [ ] Create IteratorBuilder, set start/end, iterate and collect
  - Code file: `src/engine/mod.rs` (~20 lines)

- [ ] **Item 4.3**: MergeIterator implementation
  - [ ] Create iterator over active memtable
  - [ ] Create iterators over all immutable memtables
  - [ ] Create iterators over all SST files in manifest
  - [ ] Merge in sorted order, dedup by key (keep latest)
  - [ ] Filter by sequence if snapshot provided
  - Code file: `src/iterators/merge.rs` (likely exists, may need updates) (~100 lines)
  
  **Validation**:
  ```bash
  cargo test engine_iterators -- --nocapture
  ```

- [ ] **Item 4.4**: Delete range optimization
  - [ ] Add `Memtable::delete_range()` method
  - [ ] Add `RuntimeMsg::WalAppendRange` variant
  - [ ] Update engine.delete_range() to send WalAppendRange
  - [ ] Update WAL actor to handle range deletes
  - Code files: `src/engine/mod.rs`, `src/runtime/mod.rs`, `src/runtime/actors/wal.rs` (~50 lines total)

### Phase 5: Metadata (Est. 2-3 hours)

- [ ] **Item 5.1**: Manifest actor methods
  - [ ] Add `handle_add_sst(file_meta) -> MidgeResult<()>`
  - [ ] Add `handle_remove_sst(sst_name) -> MidgeResult<()>`
  - [ ] Update manifest.sst_metadata map
  - Code file: `src/runtime/actors/manifest.rs` (~40 lines)

- [ ] **Item 5.2**: Event loop handlers for manifest
  - [ ] Add handler for `RuntimeMsg::ManifestAddSst`
  - [ ] Add handler for `RuntimeMsg::ManifestRemoveSst`
  - Code file: `src/runtime/event_loop.rs` (~30 lines)

- [ ] **Item 5.3**: Read path SST discovery
  - [ ] In RuntimeMsg::Read handler, query manifest for SSTs
  - [ ] Add `state.manifest.get_ssts_for_cf(cf_id) -> Vec<SstMetadata>`
  - Code files: `src/runtime/state.rs`, `src/runtime/event_loop.rs` (~20 lines)

---

## Testing Checkpoints

After each phase, run:

```bash
# Build check
cargo build --tests 2>&1 | Select-Object -Last 30

# Quick smoke test
cargo test engine_basic::should_get_value_given_existing_key_when_put -- --nocapture --test-threads=1

# Full basic test suite
cargo test engine_basic -- --nocapture

# Check compilation of other tests (may still have errors)
cargo build --tests 2>&1 | grep "error\[" | wc -l
```

---

## File Modification Summary

| File | Phase | Changes | Lines |
|------|-------|---------|-------|
| `src/engine/mod.rs` | 1 | Signatures, open_with_options, put_cf, get_cf, delete_cf | 20-40 |
| `src/engine/mod.rs` | 2 | RuntimeMsg::Read handler call | 5-10 |
| `src/engine/mod.rs` | 3 | write_batch(), get_snapshot(), get_at_snapshot() | 50-70 |
| `src/engine/mod.rs` | 4 | range_cf(), delete_range() fixes | 30-50 |
| `src/engine/mod.rs` | 5 | Integration with manifest queries | 10-20 |
| `src/engine/api/write_batch.rs` | 3 | WriteBatch struct, WriteOp enum | 40-50 |
| `src/engine/api/snapshot.rs` | 3 | Snapshot struct | 10-20 |
| `src/engine/api/iterator.rs` | 4 | Iterator struct, IteratorBuilder | 60-80 |
| `src/runtime/event_loop.rs` | 1 | Column family creation handler | 15-25 |
| `src/runtime/event_loop.rs` | 2 | RuntimeMsg::Read handler | 100-150 |
| `src/runtime/event_loop.rs` | 4 | WalAppendRange handler | 10-15 |
| `src/runtime/event_loop.rs` | 5 | ManifestAddSst, ManifestRemoveSst handlers | 30-40 |
| `src/runtime/mod.rs` | 1 | ColumnFamilyCreated response variant | 2 |
| `src/runtime/mod.rs` | 4 | WalAppendRange message variant | 3 |
| `src/runtime/actors/manifest.rs` | 1,5 | handle_create_cf, handle_add_sst, handle_remove_sst | 60-80 |
| `src/runtime/actors/wal.rs` | 4 | handle WalAppendRange | 20-30 |
| `src/runtime/state.rs` | 5 | Add manifest query helpers | 20-30 |
| `src/sst/mod.rs` or memtable impl | 4 | delete_range() method | 10-20 |
| `src/iterators/` | 4 | MergeIterator (likely exists, may need fixes) | 0-100 |

**Total LOC to write**: ~600-800 lines across 15-20 files

**Time estimate**: 14-18 hours for a developer familiar with the codebase

---

## Command Reference

```powershell
# Build just the library
cargo build --lib

# Build with tests enabled
cargo build --tests

# Run specific test
cargo test engine_basic::should_get_value_given_existing_key_when_put -- --nocapture

# Run all tests in a file
cargo test --test engine_basic -- --nocapture

# Run tests with output
cargo test -- --nocapture --test-threads=1

# Check for warnings/errors
cargo clippy --all-targets

# Build and run all tests
cargo test --all

# Build benchmarks
cargo build --benches
```

---

## Common Errors & Fixes

### Error: "no field `memtable` on type `RuntimeState`"
- **Fix**: RuntimeState has `column_families: HashMap<u32, ColumnFamilyState>`
- Use: `state.column_families.get(&cf_id).memtable`

### Error: "RuntimeMsg::Read not found in enum"
- **Fix**: You need to add it to the RuntimeMsg enum definition
- File: `src/runtime/mod.rs` around line 100

### Error: "response_tx doesn't match response type"
- **Fix**: RuntimeResponse enum might be missing variants
- Add: `ColumnFamilyCreated`, `ReadValue`, etc. to enum

### Error: "MidgeEngine::open takes 1 argument, got struct"
- **Fix**: Need `open_with_options()` method, not just `open()`
- Add public method to MidgeEngine

### Error: "memtable.get() returns wrapped value"
- **Fix**: Memtable returns `Option<Vec<u8>>`, need to unwrap to `Option<Bytes>`
- Use: `.map(|v| Bytes::from(v))`

---

## Debug Tips

### Print what runtime receives:
In event_loop.rs, add tracing:
```rust
if self.trace_enabled {
    tracing::trace!(?msg, "Processing message");
}
```

### Check memtable state:
```rust
// In engine.rs
println!("Memtable size: {}", self.memtable.size_bytes());
```

### Verify runtime state:
```rust
// In event_loop.rs
tracing::debug!(
    cf_count = state.column_families.len(),
    sequence = state.sequence,
    "Current state"
);
```

### Run with full output:
```powershell
RUST_LOG=debug cargo test engine_basic::should_get_value_given_existing_key_when_put -- --nocapture
```

---

## Key Architecture Concepts

**Message Passing**: All work goes through messages, no direct function calls
```
Engine → RuntimeMsg → Receiver → Actor → Update State → Response
```

**Single-threaded Runtime**: All actors run in one event loop thread, serialized
```
Event Loop processes one message at a time
This avoids race conditions, makes debugging easier
```

**Memtable Duality**: Engine has local memtable (write cache), Runtime has authoritative copy
```
Read order: Local → Immutable → SST Files
Local is fast (in-process), Runtime is authoritative (for durability)
```

**Sequence Numbers**: Global counter for causality
```
sequence = 1, 2, 3, ... (monotonic)
Every write gets a sequence
Snapshots capture a sequence for MVCC
```

---

## Next Steps After Critical Path

1. **Verify tests compile**: `cargo build --tests`
2. **Run engine_basic tests**: `cargo test engine_basic`
3. **Count passing tests**: `cargo test -- --test-threads=1 | grep "test result"`
4. **Note failures**: Group by error type
5. **Prioritize next items** based on what unblocks most tests

---

## Documentation Artifacts

All analysis documents in same directory:
- `ANALYSIS_SUMMARY.md` - High-level overview
- `PORTING_PLAN.md` - Detailed breakdown by module
- `IMPLEMENTATION_DETAILS.md` - Code snippets & implementation guides
- `IMPLEMENTATION_CHECKLIST.md` - This file (task tracking)

