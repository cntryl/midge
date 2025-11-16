# Flush Manifest Cache Synchronization Issue

## Problem

When the background flush coordinator completes a flush, it saves the updated manifest to disk but does not update the `MidgeEngine`'s cached manifest (`engine.manifest_cache`). This causes a race condition where:

1. Test calls `eng.flush()` - marks memtable immutable and queues flush job
2. Background flush worker processes the job:
   - Creates SST file
   - Loads manifest from disk
   - Adds new SST to manifest
   - **Saves manifest to disk** (line 212 in `flush.rs`)
3. Test calls `eng.wait_for_flush(5s)` - waits for Barrier message
4. Barrier is processed and acknowledged (line 103 in `spawn_flush_worker`)
5. Test calls `eng.get(&cf, b"key")` - reads from cached manifest
6. **Cached manifest doesn't have the new SST yet!**
7. Read returns None even though data is flushed to disk

## Root Cause

The `ManifestCache` in `MidgeEngine` is never updated by the flush worker. The cache only gets refreshed on:
- Engine initialization
- Explicit call to `eng.update_manifest_cache(manifest)`
- Manual call to manifest_cache.reload() (not exposed publicly)

## Current Workaround

Tests that need immediate read visibility after flush must retry with a delay:

```rust
eng.flush();
eng.wait_for_flush(Duration::from_secs(5))?;

// Retry reading until manifest is visible
let mut retries = 0;
loop {
    if eng.get(&cf, key)?.is_some() {
        break;
    }
    retries += 1;
    if retries >= 50 {
        panic!("data not visible after flush");
    }
    std::thread::sleep(Duration::from_millis(10));
}
```

See `tests/admin_concurrency.rs` for implementation.

## Proposed Solutions

### Option 1: Callback to Engine (Recommended)

Modify `FlushWorkerConfig` to include a callback that updates the engine's manifest cache:

```rust
pub struct FlushWorkerConfig {
    // ... existing fields ...
    pub manifest_update_callback: Option<Arc<dyn Fn(Manifest) + Send + Sync>>,
}
```

In `process_flush_job` after saving manifest:

```rust
m.save_atomic(&config.db_path)?;

// Update engine's cached manifest
if let Some(ref callback) = config.manifest_update_callback {
    callback(m.clone());
}
```

Engine initialization passes closure:

```rust
let manifest_cache_clone = engine.manifest_cache.clone();
config.manifest_update_callback = Some(Arc::new(move |m| {
    manifest_cache_clone.update(m);
}));
```

**Pros:**
- Clean separation of concerns
- No coupling between flush worker and engine internals
- Works for all flush scenarios

**Cons:**
- Adds complexity to config structure
- Callback overhead (though minimal - just Arc clone)

### Option 2: Return Manifest from Flush

Make `wait_for_flush()` return the updated manifest:

```rust
pub fn wait_for_flush(&self, timeout: Duration) -> MidgeResult<Manifest> {
    // ... wait logic ...
    // Load latest manifest from disk
    Manifest::load(&self.db_path)
}
```

Caller updates cache manually:

```rust
let updated_manifest = eng.wait_for_flush(timeout)?;
eng.update_manifest_cache(updated_manifest);
```

**Pros:**
- Simple implementation
- Explicit in test code

**Cons:**
- Requires exposing `update_manifest_cache()` publicly
- Every caller must remember to update cache
- Inefficient - loads manifest from disk on every wait

### Option 3: Periodic Cache Refresh

Add background task to periodically reload manifest:

```rust
// In engine initialization
let manifest_cache_clone = engine.manifest_cache.clone();
std::thread::spawn(move || {
    loop {
        std::thread::sleep(Duration::from_millis(100));
        let _ = manifest_cache_clone.reload();
    }
});
```

**Pros:**
- No API changes
- Works automatically

**Cons:**
- Wastes resources
- Adds latency (up to 100ms)
- Doesn't solve core architectural issue
- Background thread management complexity

## Recommendation

Implement **Option 1** - it's the cleanest architectural solution that maintains separation of concerns while ensuring cache consistency.

## Related Files

- `src/core/persistence/flush.rs` - `process_flush_job()` saves manifest
- `src/core/engine/operations/maintenance.rs` - `wait_for_flush()` waits for barrier
- `src/core/engine/core.rs` - `manifest_cache` field and `update_manifest_cache()`
- `src/sst/manifest_cache.rs` - `ManifestCache` implementation
- `tests/admin_concurrency.rs` - Test demonstrating the issue

## Impact

**Current Tests Affected:**
- `tests/admin_concurrency.rs` - Both tests require retry loop workaround

**Production Impact:**
- Reads immediately after flush may not see flushed data
- Could cause apparent data loss in edge cases
- Not critical for normal operation (cache will eventually update on next flush/compaction)
- Critical for admin operations like backup that assume flush is complete

## Priority

**Medium-High** - This is an architectural flaw that affects correctness, but has workarounds and limited production impact in typical workloads.
