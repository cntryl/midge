# Background Compaction Investigation

## Date: 2025-01-13

## Status: **RESOLVED** ✅

## Issue
Test `should_background_compact_when_threshold_exceeded` was failing with:
1. Windows PermissionDenied error during engine open
2. Background compaction not reducing SST count

## Root Causes

### 1. Windows Permission Issue
**Problem**: `manifest.save_atomic` called `sync_all()` on a read-only file handle.  
**Windows Behavior**: `sync_all()` on read-only handles returns `PermissionDenied (OS error 5)`

**Fix**: Changed `src/core/manifest/io.rs` to open manifest file with `.read(true).write(true)` before calling `sync_data_only()`.

```rust
// Before
let file = File::open(&final_path)?;

// After
let file = OpenOptions::new()
    .read(true)
    .write(true)  // Required for sync_all() on Windows
    .open(&final_path)?;
```

### 2. Test Design Issue
**Problem**: Test created SSTs with **non-overlapping keys** (`key0`, `key1`, `key2`), so compaction couldn't merge them.

**LSM Compaction Behavior**:
- Compaction **doesn't reduce file count** when moving non-overlapping files between levels
- Only **merges and deduplicates** when keys overlap across multiple SSTs

**Test Sublevel Strategy**:
- With `l0_file_count < 4`: compacts **one sublevel at a time** (incremental)
- With `l0_file_count >= 4`: compacts **all sublevels together** (aggressive cleanup)

**Fix**: 
1. Changed test to write **overlapping keys** (`key_a`, `key_b`, `key_c`) in each SST
2. Increased from 3 to 4 SSTs to trigger "compact all sublevels" path
3. Compaction now merges 4 SSTs → 1 SST with latest versions

## Changes

### Code Changes
- `src/core/manifest/io.rs`: Windows-compatible manifest sync
- `src/core/compaction/coordinator.rs`: Added tracing logs (manifest state, plan selection, execution results)
- `src/core/compaction/strategy.rs`: Added tracing logs (L0 trigger checks, sublevel selection, level size checks)
- `tests/engine_compaction.rs`: Fixed test to create overlapping keys

### Added Dependencies
- `Cargo.toml`: Added `tracing-subscriber` as dev-dependency for test logging

## Tracing Instrumentation

Added comprehensive tracing logs to debug future compaction issues:

**Coordinator** (`src/core/compaction/coordinator.rs`):
- `loaded manifest for automatic compaction check` (sst_count, file_count)
- `automatic compaction plan selected` (cf_id, source_level, target_level, input_count)
- `executing compaction plan` (input_files list)
- `compaction executed successfully`

**Strategy** (`src/core/compaction/strategy.rs`):
- `checking L0 compaction trigger` (l0_file_count, l0_size, l0_threshold)
- `selecting L0 compaction strategy` (compact_all_sublevels decision)
- `picking oldest sublevel for incremental L0 compaction` (sublevel_count)
- `checking level for compaction trigger` (level, level_size, target_size, file_count)

## Test Results

**Before**:
- Test ignored with TODO comment
- Failed with PermissionDenied on Windows
- SST count remained at 3 even after 10 seconds

**After**:
- Test passes consistently
- Compaction reduces 4 SSTs → 1 SST within ~8 seconds
- All 3 `engine_compaction` tests pass

```
test should_compact_all_merge_newest_and_drop_tombstones ... ok
test should_preserve_snapshot_visibility_across_compaction ... ok
test should_background_compact_when_threshold_exceeded ... ok
```

## Lessons Learned

1. **Windows filesystem semantics**: Always open files with write permission before calling `sync_all()`
2. **LSM compaction design**: File count reduction requires **overlapping keys** to merge, not just level movement
3. **Sublevel strategy**: L0 compaction behavior changes at `l0_file_count >= 4` threshold
4. **Tracing is essential**: Proper logging revealed the exact compaction behavior (1 file at a time vs. all files)

## Next Steps

- ✅ Test un-ignored and passing
- ✅ Tracing instrumentation in place for future debugging
- Clean up temporary diagnostic println statements (optional)
- Consider documenting L0 sublevel compaction strategy in `docs/features/`
