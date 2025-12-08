# Phase 7.2 Integration Plan: Cloud SST Upload Coordination

## Overview

Phase 7.2 replaces the background thread-based cloud upload (`spawn_cloud_upload`) with a `RuntimeTask`-based upload coordinated through `CloudCoordinator`. This ensures cloud operations follow the same deterministic, single-executor model as all other background work.

## Current State (Pre-Phase 7.2)

### Cloud Upload Flow

```
flush_manager::request_flush()
    ↓
process_flush_job()
    ↓
write_sst_to_storage()  // Writes SST to local disk
    ↓
spawn_cloud_upload()  // Spawns background thread (NOT coordinated with runtime)
    ↓
cloud_manager.upload_sst_async()
    ↓
Upload runs independently of EngineRuntime
    ↓
Manifest updated with cloud status (eventual consistency)
```

**Problem**: Cloud upload is not coordinated with EngineRuntime, leading to:
- Non-deterministic upload sequencing
- Potential race conditions with manifest updates
- Cloud operations not ordered with other background work
- Difficult to reason about upload completion guarantees

## Phase 7.2 Target State

### New Cloud Upload Flow

```
flush_manager::request_flush()
    ↓
process_flush_job()
    ↓
write_sst_to_storage()  // Writes SST to local disk
    ↓
cloud_coordinator.submit_sst_upload_task()  // Submit to runtime
    ↓
RuntimeTask(Maintenance, "cloud_upload_sst(sst_id)")
    ↓
EngineRuntime executor (single-threaded)
    ↓
Execute task: cloud_manager.upload_sst_async()
    ↓
Upload runs as sequential runtime task
    ↓
Manifest updated with cloud status (deterministic)
```

**Benefits**:
- Cloud uploads sequenced deterministically
- Upload order matches with other background operations
- Easier to reason about upload completion guarantees
- Aligns with runtime-coordinated background work model

## Integration Points

### 1. File: `src/core/persistence/flush/process.rs`

**Current Code** (lines 180-195):

```rust
// Manifest updated
current_manifest.add_sst(
    cf_id,
    sst_id.clone(),
    level,
    // ...metadata...
);

// Then spawn background upload
spawn_cloud_upload(
    cloud_manager.clone(),
    sst_id.clone(),
    sst_path.clone(),
    (Some(min_seq), Some(max_seq)),
    (Some(key_start.clone()), Some(key_end.clone())),
    test_hooks,
);
```

**Changes Required**:

1. Replace `spawn_cloud_upload()` call with `CloudCoordinator::submit_sst_upload_task()`
2. Create callback closure that captures cloud_manager, sst_id, sst_path, seq_range, key_range
3. Callback executes the same `cloud_manager.upload_sst_async()` logic
4. Pass `&engine.cloud_coordinator` and `&engine.runtime`

**New Code Pattern**:

```rust
// Manifest updated
current_manifest.add_sst(
    cf_id,
    sst_id.clone(),
    level,
    // ...metadata...
);

// Then submit upload as runtime task
let sst_id_copy = sst_id.clone();
let sst_path_copy = sst_path.clone();
let cloud_mgr = cloud_manager.clone();
let test_hooks_copy = test_hooks.clone();

engine.cloud_coordinator.submit_sst_upload_task(
    &engine.runtime,
    sst_id_copy.clone(),
    move || {
        if let Err(e) = cloud_mgr.upload_sst_async(
            sst_id_copy,
            sst_path_copy,
            (Some(min_seq), Some(max_seq)),
            (Some(key_start.clone()), Some(key_end.clone())),
            test_hooks_copy,
        ) {
            tracing::error!("Failed to upload SST to cloud: {}", e);
        }
    },
)?;
```

**Why This Works**:

- Closure captures all necessary data (sst_id, path, ranges, cloud_manager)
- CloudCoordinator wraps it as RuntimeTask(Maintenance)
- Runtime executor calls closure sequentially
- Cloud upload now sequenced with flushes, compactions, etc.

### 2. File: `src/core/cloud_coordinator.rs`

**Current Implementation**:

```rust
pub fn submit_sst_upload_task<F>(
    &self,
    runtime: &Arc<EngineRuntime>,
    sst_id: String,
    upload_fn: F,
) -> MidgeResult<()>
where
    F: Fn() + Send + 'static,
{
    let task = RuntimeTask::new(
        RuntimeTaskKind::Maintenance,
        format!("cloud_upload_sst({})", sst_id),
        Box::new(upload_fn),
    );
    runtime.submit(task)
}
```

**Status**: ✅ Already implemented and tested

**What It Does**:
- Accepts callback for actual upload logic
- Wraps it as RuntimeTask(Maintenance)
- Submits to runtime for deterministic execution

## Compaction Cloud Upload Path (Phase 7.2 Secondary)

Similar integration needed for compaction uploads in `src/core/compaction_controller.rs`:

**Current**:
```rust
// After compaction writes new SST
spawn_cloud_upload(cloud_manager, sst_id, sst_path, ...);
```

**New**:
```rust
// After compaction writes new SST
cloud_coordinator.submit_sst_upload_task(
    &engine.runtime,
    sst_id,
    move || { cloud_manager.upload_sst_async(...) }
)?;
```

## Testing Strategy for Phase 7.2

### Unit Tests

1. **Test: Cloud upload submission during flush**
   - Arrange: Create engine, trigger memtable flush
   - Act: Execute flush, capture submitted tasks
   - Assert: RuntimeTask(Maintenance) with "cloud_upload_sst" was submitted

2. **Test: Cloud upload sequencing with other flushes**
   - Arrange: Create engine with compaction triggered
   - Act: Trigger multiple flushes rapid-fire
   - Assert: Cloud uploads submitted in same order as flushes

3. **Test: Cloud upload submission during compaction**
   - Arrange: Create engine, trigger compaction
   - Act: Execute compaction, capture submitted tasks
   - Assert: RuntimeTask(Maintenance) with "cloud_upload_sst" was submitted

### Integration Tests

1. **Test: Hybrid storage eviction (Phase 7.2 bonus)**
   - Arrange: Create engine, flush SSTs to cloud
   - Act: Reach cache eviction threshold
   - Assert: Oldest cached SST evicted, cloud copy still available

2. **Test: Read after cloud-only SST**
   - Arrange: Create engine, flush SST, evict from local cache
   - Act: Read key from SST
   - Assert: Key found via cloud fallback

## Success Criteria

- [ ] `spawn_cloud_upload()` call removed from `flush/process.rs`
- [ ] `spawn_cloud_upload()` call removed from `compaction_controller.rs`
- [ ] Calls replaced with `cloud_coordinator.submit_sst_upload_task()`
- [ ] All Phase 5/6 tests still pass
- [ ] New cloud upload submission tests pass (4-5 tests)
- [ ] 2329+ total tests at 100% compliance
- [ ] Zero clippy warnings

## Implementation Checklist

Phase 7.2 implementation order:

1. **Flush integration** (primary, ~30 mins)
   - [ ] Modify `flush/process.rs` to submit cloud upload task
   - [ ] Build and test flush path
   - [ ] Add 2 unit tests for flush → cloud upload sequencing

2. **Compaction integration** (secondary, ~20 mins)
   - [ ] Modify `compaction_controller.rs` to submit cloud upload task
   - [ ] Build and test compaction path
   - [ ] Add 2 unit tests for compaction → cloud upload sequencing

3. **Validation** (continuous, ~15 mins)
   - [ ] Run all tests: `cargo test --lib`
   - [ ] Check compliance: `cargo run --bin validate_tests -- --summary`
   - [ ] Run clippy: `cargo clippy --all-targets`
   - [ ] Check formatting: `cargo fmt --check`

4. **Commit** (~5 mins)
   - [ ] Stage changes: `git add -A`
   - [ ] Commit: `git commit -m "phase-7.2: wire cloud upload into runtime coordination"`
   - [ ] Update ROADMAP.md with completion status

## Expected Outcome

After Phase 7.2:
- All cloud SST uploads coordinated through EngineRuntime
- Upload sequencing deterministic (same manifest → same upload sequence)
- Cloud operations follow same actor model as flush/compaction
- Ready for Phase 7.3 (cache eviction coordination)
- 2329+ tests at 100% compliance

## Notes

- `spawn_cloud_upload()` function can be deleted after both integrations complete
- Test hooks support already in CloudCoordinator.submit_sst_upload_task()
- No API changes to CloudSstManager needed
- Error handling delegated to closure (same as current spawn_cloud_upload)
