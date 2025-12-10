# Implementation Details: Per-Module Guide

This document provides implementation specifics for each porting item, with code locations, dependencies, and exact changes needed.

---

## CRITICAL PATH ITEMS

### 1. RuntimeMsg::Read Handler

**Location**: `src/runtime/event_loop.rs`, main loop match statement (after line 250)

**Current State**: Not handled at all

**Implementation**:

```rust
RuntimeMsg::Read { cf_id, key, sequence } => {
    // 1. Get column family state
    let cf = match state.column_families.get(&cf_id) {
        Some(cf) => cf,
        None => {
            let _ = response_tx.send(RuntimeResponse::Error(
                format!("Column family {} not found", cf_id)
            ));
            continue;
        }
    };

    // 2. Try active memtable first (most likely to have it)
    if let Ok(Some(value)) = cf.memtable.get(key) {
        let _ = response_tx.send(RuntimeResponse::ReadValue(
            Some(value.to_vec())
        ));
        continue;
    }

    // 3. Try immutable memtables (in FIFO order)
    for immt in &cf.immutable_memtables {
        if let Ok(Some(value)) = immt.get(key) {
            let _ = response_tx.send(RuntimeResponse::ReadValue(
                Some(value.to_vec())
            ));
            continue;
        }
    }

    // 4. Try SST files from manifest
    let ssts_to_check = state.manifest.get_ssts_for_cf(cf_id);
    
    for sst_meta in ssts_to_check {
        let sst_path = state.sst_dir.join(&sst_meta.name);
        match crate::sst::FsSstFactory::open_reader(&sst_path) {
            Ok(reader) => {
                match reader.get(key) {
                    Ok(Some(value)) => {
                        let _ = response_tx.send(RuntimeResponse::ReadValue(
                            Some(value.to_vec())
                        ));
                        continue;
                    }
                    _ => {} // Not found in this SST, try next
                }
            }
            Err(e) => {
                tracing::warn!("Failed to open SST {}: {}", sst_meta.name, e);
            }
        }
    }

    // 5. Key not found anywhere
    let _ = response_tx.send(RuntimeResponse::ReadValue(None));
}
```

**Test Validation**: 
- After implementing, `tests/engine_basic.rs::should_get_value_given_existing_key_when_put` should pass
- Run: `cargo test --test engine_basic should_get_value_given_existing_key_when_put`

**Complexity Estimate**: ~100 lines including comments and error handling

---

### 2. Fix Engine Method Signatures

**Files Involved**:
- `src/engine/mod.rs` (engine methods)
- `tests/engine_basic.rs` (and other test files)

**Current Problem**: Tests call `engine.put(&cf, key, value)` but implementation is `engine.put(key, value)` (default CF only)

**Solution**: 
- The implementation is actually CORRECT - default CF methods take 2 args (key, value)
- Test files need updating to NOT pass CF for default column family

**Files to Fix**: 
```
tests/engine_basic.rs (multiple places)
tests/engine_iterators.rs (multiple places)
tests/engine_snapshots.rs (multiple places)
```

**Pattern**: Change `engine.put_cf(&cf, key, value)` to `engine.put(key, value)` for default CF

**Complexity**: Easy, just search-and-replace in test files

---

### 3. Expose open_with_options()

**Location**: `src/engine/mod.rs` around line 100

**Current State**: Method exists (line 110-120) but needs testing

**Change**: Ensure the signature matches what tests expect:

```rust
pub fn open_with_options(opts: crate::testkit::MidgeOptions) -> MidgeResult<Self> {
    let db_path = match &opts.storage_mode {
        crate::testkit::StorageMode::Memory => {
            PathBuf::from(":memory:")
        }
        crate::testkit::StorageMode::LocalDisk { db_path } => {
            db_path.clone()
        }
        crate::testkit::StorageMode::CloudBacked { local_cache_path } => {
            local_cache_path.clone()
        }
    };
    
    Self::open(db_path)
}
```

This already exists - just verify it compiles and works.

**Test Validation**:
- `cargo build --tests` should compile all test files

---

### 4. Column Family Creation Handler

**Files**:
- `src/runtime/event_loop.rs` (add message handler)
- `src/runtime/actors/manifest.rs` (update manifest)

**Current State**: Message defined but not handled

**Event Loop Handler** (add after line 250 in event_loop.rs):

```rust
RuntimeMsg::ManifestCreateColumnFamily { name } => {
    // Get next CF ID
    let cf_id = state.manifest.next_cf_id;
    state.manifest.next_cf_id += 1;

    // Create CF in runtime state
    state.column_families.insert(
        cf_id,
        super::state::ColumnFamilyState::new(cf_id, name.clone()),
    );

    tracing::info!(cf_id, name = %name, "Created column family");
    
    let _ = response_tx.send(RuntimeResponse::ColumnFamilyCreated { cf_id });
}
```

**Manifest Actor** (add method to manifest.rs):

```rust
pub fn handle_create_column_family(
    &mut self,
    state: &mut RuntimeState,
    name: String,
) -> MidgeResult<u32> {
    let cf_id = state.manifest.next_cf_id;
    state.manifest.next_cf_id += 1;
    state.manifest.column_families.insert(cf_id, name.clone());
    Ok(cf_id)
}
```

**Engine API** (add to src/engine/mod.rs):

```rust
pub fn create_column_family(&self, name: &str) -> MidgeResult<ColumnFamilyId> {
    let response = self.runtime_handle.send_and_wait_filtered(
        RuntimeMsg::ManifestCreateColumnFamily {
            name: name.to_string(),
        },
        |resp| matches!(resp, RuntimeResponse::ColumnFamilyCreated { .. } | RuntimeResponse::Error(_)),
    )?;

    match response {
        RuntimeResponse::ColumnFamilyCreated { cf_id } => {
            Ok(ColumnFamilyId(cf_id))
        }
        RuntimeResponse::Error(e) => Err(MidgeError::Internal(e)),
        _ => Err(MidgeError::Internal("Unexpected response".to_string())),
    }
}
```

**Complexity**: ~60 lines total

---

## SUPPORTING ITEMS

### 5. WriteBatch Implementation

**Location**: `src/engine/api/write_batch.rs` and `src/engine/mod.rs`

**Current State**: Empty skeleton

**WriteBatch struct**:

```rust
pub struct WriteBatch {
    ops: Vec<(ColumnFamilyId, WriteOp)>,
}

pub enum WriteOp {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

impl WriteBatch {
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    pub fn put(&mut self, cf: ColumnFamilyId, key: Vec<u8>, value: Vec<u8>) {
        self.ops.push((cf, WriteOp::Put(key, value)));
    }

    pub fn delete(&mut self, cf: ColumnFamilyId, key: Vec<u8>) {
        self.ops.push((cf, WriteOp::Delete(key)));
    }

    pub fn clear(&mut self) {
        self.ops.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }
}
```

**Engine method** (add to src/engine/mod.rs):

```rust
pub fn write_batch(&self, batch: WriteBatch) -> MidgeResult<()> {
    let seq_start = self.next_sequence();
    
    for (idx, (cf_id, op)) in batch.ops.iter().enumerate() {
        let seq = seq_start + idx as u64;
        
        match op {
            WriteOp::Put(key, value) => {
                self.memtable.put(key.clone(), value.clone())?;
                self.runtime_handle.send(RuntimeMsg::WalAppend {
                    cf_id: cf_id.0,
                    key: key.clone(),
                    value: Some(value.clone()),
                    sequence: seq,
                })?;
            }
            WriteOp::Delete(key) => {
                self.memtable.delete(key.clone())?;
                self.runtime_handle.send(RuntimeMsg::WalAppend {
                    cf_id: cf_id.0,
                    key: key.clone(),
                    value: None,
                    sequence: seq,
                })?;
            }
        }
    }
    
    Ok(())
}
```

**Test Validation**: Tests that create 100s of operations should be much faster

**Complexity**: ~80 lines

---

### 6. Snapshot Support

**Location**: `src/engine/api/snapshot.rs` and `src/engine/mod.rs`

**Snapshot struct**:

```rust
#[derive(Debug, Clone, Copy)]
pub struct Snapshot {
    pub sequence: u64,
}

impl Snapshot {
    pub fn new(sequence: u64) -> Self {
        Self { sequence }
    }
}
```

**Engine methods** (add to src/engine/mod.rs):

```rust
pub fn get_snapshot(&self) -> Snapshot {
    let seq = self.sequence.load(std::sync::atomic::Ordering::SeqCst);
    Snapshot::new(seq)
}

pub fn get_at_snapshot(&self, snap: &Snapshot, key: &[u8]) -> MidgeResult<Option<bytes::Bytes>> {
    // Query with snapshot sequence
    let response = self.runtime_handle.send_and_wait_filtered(
        RuntimeMsg::Read {
            cf_id: 0,
            key: key.to_vec(),
            sequence: snap.sequence,  // Read at this sequence
        },
        |resp| matches!(resp, RuntimeResponse::ReadValue(_) | RuntimeResponse::Error(_)),
    )?;

    match response {
        RuntimeResponse::ReadValue(value) => Ok(value.map(bytes::Bytes::from)),
        RuntimeResponse::Error(e) => Err(MidgeError::Internal(e)),
        _ => Err(MidgeError::Internal("Unexpected response".to_string())),
    }
}
```

**Read handler change**: Modify the RuntimeMsg::Read handler to respect `sequence` parameter:

```rust
// When checking memtables/SSTs, skip entries with seq > read_sequence
// This requires memtable iterator to support filtering by sequence
```

**Complexity**: ~50 lines (structure) + 30 lines (filtering logic in readers)

---

### 7. Iterator / Range Scan

**Location**: `src/engine/api/iterator.rs` and `src/engine/mod.rs`

This is the most complex supporting item. Requires:

1. **IteratorBuilder pattern**:
```rust
pub struct IteratorBuilder {
    start_key: Option<Vec<u8>>,
    end_key: Option<Vec<u8>>,
    reverse: bool,
    snapshot: Option<Snapshot>,
}

impl IteratorBuilder {
    pub fn new() -> Self { /* ... */ }
    pub fn start_key(mut self, key: Vec<u8>) -> Self { /* ... */ }
    pub fn end_key(mut self, key: Vec<u8>) -> Self { /* ... */ }
    pub fn build(self) -> MidgeResult<Iterator> { /* ... */ }
}
```

2. **MergeIterator over sources**:
   - Active memtable iterator
   - Each immutable memtable iterator
   - Each SST iterator
   - Merge all in sorted order, dedup by key (keep latest)

3. **Sequence filtering**: If snapshot provided, only return entries with seq <= snapshot.sequence

**Complexity**: ~200 lines (hard)

---

### 8. Delete Range Optimization

**Location**: `src/engine/mod.rs` line 236

Current (inefficient):
```rust
pub fn delete_range(&self, cf: &ColumnFamilyHandle, start: &[u8], end: &[u8]) -> MidgeResult<()> {
    let keys = self.range_cf(cf, start, end)?;
    for (key, _) in keys {
        self.delete_cf(cf, &key)?;
    }
    Ok(())
}
```

Better approach using range tombstones:

```rust
pub fn delete_range(&self, cf: &ColumnFamilyHandle, start: &[u8], end: &[u8]) -> MidgeResult<()> {
    let seq = self.next_sequence();

    // Record tombstone in local memtable
    self.memtable.delete_range(start.to_vec(), end.to_vec())?;

    // Send to WAL as DeleteRange operation
    self.runtime_handle.send(RuntimeMsg::WalAppendRange {
        cf_id: cf.id.0,
        start: start.to_vec(),
        end: end.to_vec(),
        sequence: seq,
    })?;

    Ok(())
}
```

Requires:
- `Memtable::delete_range()` method
- `RuntimeMsg::WalAppendRange` variant
- WAL encoder for range deletes
- Compaction executor respects range tombstones (likely already done)

**Complexity**: ~80 lines

---

### 9. Manifest Integration

**Location**: `src/metadata/manifest.rs` and `src/runtime/actors/manifest.rs`

**Current State**: Manifest exists but is not persisted and not populated

**Manifest struct additions**:

```rust
pub struct Manifest {
    // ... existing fields ...
    
    // New fields for runtime integration
    pub column_families: HashMap<u32, String>,
    pub next_cf_id: u32,
    pub next_sst_seqs: HashMap<u32, u64>,
    pub sst_metadata: HashMap<String, SstMetadata>,
}

pub struct SstMetadata {
    pub name: String,
    pub cf_id: u32,
    pub level: u32,
    pub size_bytes: u64,
    pub smallest_key: Option<Vec<u8>>,
    pub largest_key: Option<Vec<u8>>,
}
```

**Manifest actor methods**:

```rust
pub fn handle_add_sst(
    &mut self,
    state: &mut RuntimeState,
    file_meta: crate::runtime::FileMeta,
) -> MidgeResult<()> {
    state.manifest.sst_metadata.insert(
        file_meta.name.clone(),
        SstMetadata {
            name: file_meta.name,
            cf_id: file_meta.cf_id,
            level: file_meta.level,
            size_bytes: file_meta.size_bytes,
            smallest_key: file_meta.smallest_key,
            largest_key: file_meta.largest_key,
        },
    );
    Ok(())
}

pub fn handle_remove_sst(
    &mut self,
    state: &mut RuntimeState,
    sst_name: String,
) -> MidgeResult<()> {
    state.manifest.sst_metadata.remove(&sst_name);
    Ok(())
}
```

**Event loop handlers** (add to event_loop.rs):

```rust
RuntimeMsg::ManifestAddSst { file_meta } => {
    let result = self.manifest_actor.handle_add_sst(&mut self.state, file_meta);
    let _ = response_tx.send(match result {
        Ok(()) => RuntimeResponse::Ok,
        Err(e) => RuntimeResponse::Error(e.to_string()),
    });
}

RuntimeMsg::ManifestRemoveSst { sst_name } => {
    let result = self.manifest_actor.handle_remove_sst(&mut self.state, sst_name);
    let _ = response_tx.send(match result {
        Ok(()) => RuntimeResponse::Ok,
        Err(e) => RuntimeResponse::Error(e.to_string()),
    });
}
```

**Complexity**: ~100 lines

---

## Testing Checkpoints

After each critical path item, run:
```bash
cargo build --tests          # Check compilation
cargo test engine_basic      # Check basic CRUD
```

After supporting items:
```bash
cargo test engine_iterators  # Check ranges
cargo test engine_snapshots  # Check MVCC
cargo test --lib            # Check library code
```

---

## Common Pitfalls

1. **Forgetting to lock state**: If using Arc<Mutex<RuntimeState>>, ensure state is locked during reads
2. **Memtable sync issues**: Engine memtable and runtime memtable can get out of sync if WAL append fails
3. **Sequence overflows**: Use u64 everywhere for sequence numbers, never u32
4. **SST reader caching**: Opening SST files repeatedly is slow; consider caching readers
5. **Memory leaks in iterators**: Ensure iterators are dropped to release SST reader handles

