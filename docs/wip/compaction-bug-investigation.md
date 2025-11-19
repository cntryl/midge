# Compaction Bug Investigation

## Problem
Test `should_background_compact_when_threshold_exceeded` consistently returns `value_v2` instead of `value_v3` after compaction.

## Observed Behavior
- Test writes 4 iterations (i=0,1,2,3) creating values: `value_v0`, `value_v1`, `value_v2`, `value_v3`
- Each iteration flushes to SST before closing engine
- Background compaction merges all 4 SST files
- Final read returns `value_v2` instead of expected `value_v3`
- Failure is **deterministic** (10/10 runs)

## Root Cause Hypotheses

### 1. **File Ordering in collect_compaction_versions** (LIKELY)
In `src/core/compaction/executor.rs:60`:
```rust
for name in sst_names.iter().rev() {
```

The code iterates SST files in **reverse** order. If `sst_names` is ordered newest-first, then `.rev()` processes oldest-first, potentially causing newer versions to be overwritten by older ones.

**Evidence needed:**
- What is the actual order of `sst_names` in the CompactionPlan?
- Does file naming include sequence numbers that would affect sort order?

### 2. **Deduplication Logic**
In `deduplicate_versions()`, the code keeps:
- First version of each key (newest if sorted correctly)
- Versions visible to snapshots (seq < min_snapshot_seq)

If versions aren't sorted correctly by sequence (descending), the "first" version might not be the newest.

### 3. **SST File Deletion Race**
The 4th iteration's SST might be deleted before compaction reads it. However:
- Test calls `flush_cf()` and `wait_for_flush()` explicitly
- Files should be persisted before compaction starts

### 4. **Sequence Number Not Properly Tracked**
The manifest's `last_persisted_sequence` might not advance properly for iteration i=3.

## Proposed Unit Tests

### Test 1: Version Collection Order
```rust
#[test]
fn should_collect_versions_preserving_newest_first() {
    // Create 3 SST files with overlapping key "foo"
    // File 1 (oldest): foo @ seq=1 = "v1"
    // File 2 (middle): foo @ seq=5 = "v2"  
    // File 3 (newest): foo @ seq=10 = "v3"
    
    let versions = collect_compaction_versions(
        &reader_factory,
        &sst_dir,
        &["file1.sst", "file2.sst", "file3.sst"]
    );
    
    // After sorting
    sort_versions_for_output(&mut versions);
    
    // Verify newest version is first
    assert_eq!(versions[0].seq, 10);
    assert_eq!(versions[0].value, Some("v3"));
}
```

### Test 2: Deduplication Preserves Newest
```rust
#[test]
fn should_deduplicate_keeping_newest_version() {
    let versions = vec![
        CompactionVersion { user_key: b"key".to_vec(), seq: 10, value: Some("v3"), .. },
        CompactionVersion { user_key: b"key".to_vec(), seq: 5, value: Some("v2"), .. },
        CompactionVersion { user_key: b"key".to_vec(), seq: 1, value: Some("v1"), .. },
    ];
    
    let result = deduplicate_versions(&versions, None);
    
    // Should keep only newest (seq=10)
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].seq, 10);
}
```

### Test 3: Full Compaction Pipeline
```rust
#[test]
fn should_preserve_newest_value_through_full_compaction() {
    // Create 4 SST files with increasing sequence numbers
    // Each overwrites key "test_key" with newer value
    
    // Execute compaction
    let result = execute_full_compaction(sst_files);
    
    // Verify output contains newest value
    assert_eq!(get_value_from_compacted_sst("test_key"), "value_v3");
}
```

### Test 4: Sublevel Ordering
```rust
#[test]
fn should_order_l0_sublevels_correctly() {
    // Create manifest with 4 L0 files at different sublevels
    // Verify compaction plan orders them correctly (newest last?)
    
    let plan = compactor.pick_leveled_compaction(&files, 0, 10, 64*1024*1024);
    
    // Check input_files order matches expected sequence
}
```

## Next Steps

1. **Add instrumentation** to log:
   - Order of `sst_names` in CompactionPlan
   - Order of files after `.rev()`
   - Sequence numbers found during collection
   - Order after `sort_versions_for_output()`

2. **Run test with RUST_LOG=debug** to capture full execution trace

3. **Implement unit tests above** to isolate the bug

4. **Fix the bug** based on findings

5. **Add regression test** to prevent recurrence
