# Phase 2A: Cloud-First Recovery Implementation Plan

## Objective
Refactor the recovery path to be manifest-first (cloud-sourced) instead of filesystem-dependent. This aligns with THE_BIG_IDEA principle: "recovery driven by manifest + WAL + compaction log, not whatever's on the local FS."

## Current Architecture (Pre-Refactoring)

### Recovery Flow (Simplified)
```
1. Load manifest from local filesystem
   - Try CURRENT file → manifest.json
   - Fall back to default if either missing
   - (No cloud consultation)

2. Replay WAL from local filesystem
   - Iterate local WAL directory
   - Apply records to memtables
   - (Cloud WAL assumed optional upload)

3. Result: Hybrid recovery, not cloud-first
```

### Code Locations
- **Manifest load**: `src/core/manifest/io.rs` - `Manifest::load()`
- **Recovery init**: `src/core/engine/factory.rs` - `init_manifest()`
- **WAL replay**: `src/core/persistence/wal_replay.rs`
- **Recovery orchestration**: `src/core/engine/state.rs`

### Problem
- Recovery always assumes local filesystem is source of truth
- Cloud checkpoint information ignored during recovery initialization
- WAL recovery doesn't prioritize cloud WAL over local segments
- No mechanism to recover from cloud-only scenarios

## Target Architecture (Post-Refactoring)

### Recovery Flow (Cloud-First)
```
1. Try cloud manifest first
   - If cloud_backend exists, load manifest from cloud checkpoint
   - Verify manifest integrity (hash/signature)
   - Use this as source of truth

2. Fall back to local manifest only if
   - No cloud backend configured, OR
   - Cloud load fails with connection error

3. Replay WAL
   - Get safe prune sequence from cloud checkpoint
   - Load local + cloud WAL segments
   - Apply records > checkpoint_sequence
   - Result: Complete recovery without local FS assumptions

4. Result: Cloud-first with graceful local fallback
```

## Implementation Strategy

### Phase 2A-1: Add Cloud Manifest Loading

**New module**: `src/core/manifest/cloud_recovery.rs`

```rust
/// Cloud-first manifest recovery
pub trait ManifestRecovery {
    /// Try to load manifest from cloud, falling back to local
    /// 
    /// Priority order:
    /// 1. Cloud checkpoint + manifest (if cloud_backend available)
    /// 2. Local manifest (via Manifest::load)
    /// 3. Default manifest (brand new DB)
    fn load_with_cloud_fallback(
        db_path: &Path,
        cloud_backend: Option<&dyn StorageBackend>,
        cloud_prefix: Option<&str>,
    ) -> MidgeResult<Self>;
}

impl ManifestRecovery for Manifest {
    fn load_with_cloud_fallback(...) -> MidgeResult<Self> {
        // Try cloud first
        if let Some(backend) = cloud_backend {
            if let Ok(manifest) = Self::load_from_cloud(backend, cloud_prefix) {
                tracing::info!("recovered manifest from cloud");
                return Ok(manifest);
            }
            // Cloud failed; warn but continue
            tracing::warn!("cloud manifest unavailable, trying local");
        }
        
        // Fall back to local
        match Self::load(db_path) {
            Ok(m) => Ok(m),
            Err(_) => {
                tracing::warn!("local manifest unavailable, using default");
                Ok(Manifest::default())
            }
        }
    }
}
```

### Phase 2A-2: Implement Cloud Manifest Loading

**Methods to add to `src/core/manifest/io.rs`**:

```rust
/// Load manifest from cloud storage via checkpoint
pub fn load_from_cloud(
    backend: &dyn StorageBackend,
    cloud_prefix: Option<&str>,
) -> MidgeResult<Self> {
    let prefix = cloud_prefix.unwrap_or("midge");
    
    // Read cloud checkpoint to find manifest location
    let checkpoint_key = format!("{}/manifest/CLOUD_CHECKPOINT", prefix);
    let checkpoint_data = backend.get_blob(&checkpoint_key)?;
    let checkpoint: CloudCheckpoint = serde_json::from_slice(&checkpoint_data)?;
    
    // Load manifest from cloud
    let manifest_key = format!("{}/manifest/{}", prefix, checkpoint.manifest_name);
    let manifest_data = backend.get_blob(&manifest_key)?;
    let manifest: Manifest = serde_json::from_slice(&manifest_data)?;
    
    // Verify manifest integrity
    manifest.verify_cloud_integrity()?;
    
    Ok(manifest)
}

/// Verify manifest was correctly uploaded to cloud
fn verify_cloud_integrity(&self) -> MidgeResult<()> {
    // Check cloud_checkpoint exists and is consistent
    // This ensures we loaded from a valid checkpoint
    self.cloud_checkpoint
        .as_ref()
        .ok_or_else(|| MidgeError::internal(
            "cloud manifest missing checkpoint information"
        ))?;
    Ok(())
}
```

### Phase 2A-3: Cloud-First WAL Recovery

**New function in `src/core/engine/factory.rs`**:

```rust
/// Recovery that respects cloud checkpoint and loads WAL from cloud if needed
pub(crate) fn replay_wal_with_cloud_fallback(
    wal_dir: &Path,
    cloud_backend: Option<&dyn StorageBackend>,
    cloud_prefix: Option<&str>,
    manifest: &Manifest,
    cf_set: &ColumnFamilySet,
    recovery_mode: WalRecoveryMode,
    mem_mode: bool,
) -> MidgeResult<u64> {
    // Get safe prune sequence from cloud checkpoint
    let safe_prune_seq = manifest
        .get_cloud_checkpoint()
        .map(|cp| cp.checkpoint_sequence)
        .unwrap_or(0);
    
    tracing::info!(
        "recovering WAL with cloud checkpoint seq={}",
        safe_prune_seq
    );
    
    // Load local WAL segments that are > safe_prune_seq
    let local_max_seq = if !mem_mode {
        replay_local_wal_segments(
            wal_dir,
            cf_set,
            safe_prune_seq,  // Skip segments already in cloud
            recovery_mode,
            mem_mode,
        )?
    } else {
        safe_prune_seq
    };
    
    // TODO: Load cloud WAL segments if needed
    // (Requires cloud WAL reader - separate task)
    
    Ok(local_max_seq)
}
```

### Phase 2A-4: Update Recovery Initialization

**Changes to `src/core/engine/factory.rs` - `init_manifest()`**:

```rust
pub(crate) fn init_manifest_cloud_first(
    db_path: &Path,
    cloud_backend: Option<&dyn StorageBackend>,
    cloud_prefix: Option<&str>,
    read_only: bool,
    memtable_size: usize,
    mem_mode: bool,
) -> MidgeResult<(Manifest, u32)> {
    // Cloud-first loading with local fallback
    let mut manifest = Manifest::load_with_cloud_fallback(
        db_path,
        cloud_backend,
        cloud_prefix,
    )?;
    
    // Ensure default CF exists
    if !manifest.has_cf(DEFAULT_CF_ID) {
        let default_cf_config = ColumnFamilyConfig {
            memtable_max_bytes: memtable_size,
            ..ColumnFamilyConfig::default()
        };
        manifest.add_cf(
            DEFAULT_CF_ID,
            DEFAULT_CF_NAME.to_string(),
            Some(default_cf_config),
        );
        
        // Save to both local and cloud (if available)
        if !read_only && !mem_mode {
            manifest.save_atomic(db_path)?;
            if let Some(backend) = cloud_backend {
                manifest.save_to_cloud(backend, cloud_prefix)?;
            }
        }
    }
    
    let max_cf_id = manifest
        .column_families
        .iter()
        .map(|cf| cf.id)
        .max()
        .unwrap_or(0);
    
    Ok((manifest, max_cf_id))
}
```

### Phase 2A-5: Update Engine Initialization Chain

**Changes to `src/core/engine/state.rs` - `open_with_factories()`**:

```rust
// Before (filesystem-first):
let (manifest, max_cf_id) = crate::core::engine::factory::init_manifest(
    &db_path,
    opts.read_only,
    opts.memtable_size,
    mem_mode,
)?;

// After (cloud-first):
let cloud_backend = opts.storage_mode.cloud_backend();
let cloud_prefix = opts.storage_mode.cloud_prefix();
let (manifest, max_cf_id) = crate::core::engine::factory::init_manifest_cloud_first(
    &db_path,
    cloud_backend.as_deref(),
    cloud_prefix.as_deref(),
    opts.read_only,
    opts.memtable_size,
    mem_mode,
)?;

// And for WAL recovery:
let max_replay_seq = crate::core::engine::factory::replay_wal_with_cloud_fallback(
    &wal_dir,
    cloud_backend.as_deref(),
    cloud_prefix.as_deref(),
    &manifest,
    &cf_set_arc,
    opts.wal_recovery_mode,
    mem_mode,
)?;
```

## Implementation Checklist

- [ ] **Phase 2A-1**: Create `cloud_recovery.rs` module with trait
- [ ] **Phase 2A-2**: Implement `load_from_cloud()` + `verify_cloud_integrity()`
- [ ] **Phase 2A-3**: Implement `replay_wal_with_cloud_fallback()`
- [ ] **Phase 2A-4**: Create `init_manifest_cloud_first()` wrapper
- [ ] **Phase 2A-5**: Update state.rs to use cloud-first path
- [ ] Add tests for cloud-first recovery
- [ ] Add tests for cloud fallback scenarios
- [ ] Update documentation
- [ ] Verify all existing tests still pass

## Testing Strategy

### Unit Tests
```rust
#[test]
fn should_load_manifest_from_cloud_when_available() {
    // Arrange: Mock cloud backend with manifest
    let backend = MockCloudBackend::new();
    backend.put_blob("midge/manifest/CLOUD_CHECKPOINT", checkpoint_data);
    backend.put_blob("midge/manifest/manifest.json", manifest_data);
    
    // Act
    let loaded = Manifest::load_from_cloud(&backend, Some("midge"))?;
    
    // Assert
    assert_eq!(loaded.version, expected_version);
}

#[test]
fn should_fallback_to_local_when_cloud_unavailable() {
    // Arrange: Backend fails, but local manifest exists
    let backend = FailingBackend::new();
    
    // Act
    let loaded = Manifest::load_with_cloud_fallback(
        db_path,
        Some(&backend as &dyn StorageBackend),
        None,
    )?;
    
    // Assert
    assert_eq!(loaded, local_manifest);
}

#[test]
fn should_skip_wal_before_cloud_checkpoint() {
    // Arrange
    let manifest = load_manifest_with_checkpoint(seq=100);
    
    // Act
    let max_seq = replay_wal_with_cloud_fallback(...)?;
    
    // Assert
    // Only WAL records > 100 should be replayed
}
```

### Integration Tests
- Recovery with cloud-backed storage mode
- Recovery with cloud unavailable
- Recovery with corrupted cloud checkpoint
- Recovery with missing local WAL segments

## Backward Compatibility

**This refactoring is backward compatible because:**
1. Cloud-first path is additive (local fallback always available)
2. Existing single-storage-mode engines unchanged
3. Cloud checkpoint is optional (defaults are safe)
4. All existing tests continue to work

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|-----------|
| Cloud checkpoint corrupted | Low | High | Verify integrity before use, fall back to local |
| Network timeout on load | Medium | Medium | Implement fallback with timeout + tracing |
| Manifest mismatch (cloud vs local) | Very Low | High | Always prefer cloud if available and valid |
| Infinite loop on fallback failure | Very Low | High | Clear error path, no fallback chains |

## Success Criteria

- ✅ Recovery loads manifest from cloud when available
- ✅ Recovery falls back to local when cloud unavailable
- ✅ Recovery respects cloud checkpoint for WAL pruning
- ✅ All existing tests pass
- ✅ New tests cover cloud recovery scenarios
- ✅ No performance regression in local-only scenarios
- ✅ Documentation updated to explain cloud-first recovery

## Effort Estimate

- Implementation: 3-4 hours
- Testing: 2-3 hours
- Documentation: 1 hour
- **Total: 6-8 hours**

## Dependencies

- Manifest types with cloud_checkpoint field (already exists)
- StorageBackend trait for cloud access (already exists)
- CloudCheckpoint type (already exists)

All infrastructure is in place; this is purely a refactoring of orchestration logic.
