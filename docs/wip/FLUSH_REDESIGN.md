# Flush Operation Redesign

## Problem Statement

Current flush has split responsibilities that create race conditions:
1. `flush_memtable_to_sst()` writes SST but doesn't update manifest
2. Caller must manually update manifest, cache, and coordinate visibility
3. Critical gaps between file creation, manifest update, and cache update
4. Bugs like sparse index lookup failures are symptoms of this fundamental issue

## Root Cause

**Flush is not atomic.** Data written to SST isn't immediately visible because:
- SST file exists but manifest doesn't know about it
- Manifest knows but cache isn't updated  
- Cache updated but readers using old cache still

## Proposed Architecture: Lock-Free Atomic Swaps

### Design Goals
1. **No mutexes on read/write hot path**
2. **Atomic visibility transitions via ArcSwap**
3. **Serialized version management**
4. **Simple reconciliation on startup**

### Core Components

```rust
use arc_swap::ArcSwap;

// Per-CF: Atomic memtable swap
struct ColumnFamily {
    memtable: ArcSwap<MemTable>,  // ← Lock-free reads/writes
    // ...
}

// Global: Atomic version/manifest visibility
struct MidgeEngine {
    version_set: ArcSwap<VersionSet>,  // ← Lock-free reads
    version_manager: VersionManager,    // ← Single actor for edits
    // ...
}

// Immutable snapshot of SST files + manifest state
struct VersionSet {
    manifest: Manifest,
    sst_readers: HashMap<String, Arc<dyn SstReader>>,
    // Immutable - readers can hold this across operations
}
```

### Flush Protocol: Freeze → Build → Publish

```rust
pub fn flush_cf(&self, cf: &ColumnFamilyHandle) -> MidgeResult<()> {
    // STEP 1: Atomic memtable freeze (no locks!)
    let frozen = {
        let new_memtable = Arc::new(MemTable::new());
        let old_memtable = cf.memtable.swap(new_memtable);
        old_memtable  // Frozen memtable
    };
    
    // STEP 2: Build SST file (no locks, take your time)
    let sst_file = self.build_sst_from_frozen(cf, frozen)?;
    
    // STEP 3: Publish via version manager (serialized actor)
    self.version_manager.apply_edit(VersionEdit::AddFile {
        cf_id: cf.id(),
        file: sst_file.metadata,
    })?;
    
    Ok(())
}
```

#### Step 1: Freeze Memtable (Lock-Free)
```rust
// ArcSwap makes this atomic and wait-free for readers
let frozen = cf.memtable.swap(Arc::new(MemTable::new()));

// Readers doing:
//   let mt = cf.memtable.load();
//   mt.get(key)
// Never block, never see torn state
```

**Properties:**
- ✅ No locks held
- ✅ Wait-free for readers
- ✅ Atomic swap (readers see old or new, never partial)
- ✅ Frozen memtable kept alive by Arc until flush completes

#### Step 2: Build SST (Pure Computation)
```rust
fn build_sst_from_frozen(&self, cf: &CF, frozen: Arc<MemTable>) 
    -> MidgeResult<SstFile> 
{
    let entries = frozen.drain_with_meta_internal();
    let range_tombstones = frozen.drain_range_tombstones();
    
    // Pure I/O - no coordination needed
    SstBuilder::new(self.sst_dir.clone())
        .cf_id(cf.id())
        .entries(entries)
        .range_tombstones(range_tombstones)
        .build()
}
```

**Properties:**
- ✅ No locks
- ✅ Can be slow (I/O bound)
- ✅ Returns complete metadata (no fixups)

#### Step 3: Publish via Version Manager (Serialized)
```rust
struct VersionManager {
    tx: Sender<VersionEdit>,  // Single actor inbox
}

impl VersionManager {
    fn apply_edit(&self, edit: VersionEdit) -> MidgeResult<()> {
        self.tx.send(edit)?;
        // Actor processes edits serially:
        // 1. Load current version
        // 2. Apply edit (add file, remove file, etc.)
        // 3. Write manifest atomically
        // 4. Publish new version via ArcSwap
        Ok(())
    }
}

// In actor loop:
loop {
    let edit = rx.recv()?;
    
    // Load current version
    let current = engine.version_set.load();
    
    // Create new version with edit applied
    let mut new_version = current.as_ref().clone();
    new_version.apply(edit);
    
    // Write manifest atomically
    new_version.manifest.save_atomic(&db_path)?;
    
    // Publish new version atomically
    engine.version_set.store(Arc::new(new_version));
    
    // Readers now see new SST immediately
}
```

**Properties:**
- ✅ Serialized visibility (no races)
- ✅ Atomic manifest write + version publish
- ✅ Readers use ArcSwap::load() - lock-free
- ✅ Simple: one actor, linear processing

## Key Improvements

### 1. Lock-Free Hot Path
**Before:**
```rust
let guard = memtable.read();  // ← Lock on every read!
guard.get(key)
```

**After:**
```rust
let mt = memtable.load();     // ← Wait-free atomic load
mt.get(key)                   // ← No locks
```

### 2. Atomic Visibility via ArcSwap
**Before:**
```
Write SST | Update manifest | Gap | Update cache | Gap | Readers see inconsistency
```

**After:**
```
Write SST | Actor: manifest + version_set.store() | Readers see atomically
```

### 3. Serialized Version Management
- Single actor processes all visibility changes
- No coordination complexity
- Simple linear reasoning
- Natural backpressure (actor queue fills)

### 4. Startup Reconciliation
```rust
fn reconcile_on_startup() {
    let manifest = Manifest::load(&db_path)?;
    let disk_ssts = list_sst_files(&sst_dir)?;
    
    // Manifest is source of truth
    let referenced_ssts: HashSet<_> = 
        manifest.files.iter().map(|f| &f.name).collect();
    
    // Clean up orphaned SSTs (not in manifest)
    for sst_file in disk_ssts {
        if !referenced_ssts.contains(&sst_file) {
            fs::remove_file(sst_file)?;  // Cleanup
        }
    }
}
```

### 5. Testability
```rust
#[test]
fn flush_is_atomic() {
    let engine = create_test_engine();
    engine.put(b"key", b"value");
    
    // BEFORE flush: data in memtable
    assert!(read_from_memtable(b"key").is_some());
    
    engine.flush();
    
    // AFTER flush: data in SST (no intermediate state)
    assert!(read_from_sst(b"key").is_some());
}
```

## Implementation Plan

### Phase 1: Add Dependencies
```toml
[dependencies]
arc-swap = "1.7"
crossbeam-channel = "0.5"  # For version manager actor
```

### Phase 2: Introduce VersionSet
```rust
#[derive(Clone)]
pub struct VersionSet {
    pub manifest: Manifest,
    pub sst_readers: Arc<HashMap<String, Arc<dyn SstReader>>>,
}

impl VersionSet {
    fn apply_edit(&mut self, edit: VersionEdit) {
        match edit {
            VersionEdit::AddFile { cf_id, file } => {
                self.manifest.files.push(file.clone());
                self.manifest.ssts.push(file.name.clone());
                // Open SST reader for new file
                if let Ok(reader) = self.open_sst(&file.name) {
                    Arc::make_mut(&mut self.sst_readers)
                        .insert(file.name.clone(), reader);
                }
            }
            VersionEdit::RemoveFile { name } => {
                self.manifest.files.retain(|f| f.name != name);
                self.manifest.ssts.retain(|s| s != &name);
                Arc::make_mut(&mut self.sst_readers).remove(&name);
            }
        }
    }
}
```

### Phase 3: Create Version Manager Actor
```rust
pub struct VersionManager {
    tx: Sender<VersionEdit>,
    handle: JoinHandle<()>,
}

impl VersionManager {
    pub fn new(engine: Arc<MidgeEngine>) -> Self {
        let (tx, rx) = crossbeam_channel::unbounded();
        let handle = thread::spawn(move || {
            Self::run_actor(engine, rx);
        });
        Self { tx, handle }
    }
    
    fn run_actor(engine: Arc<MidgeEngine>, rx: Receiver<VersionEdit>) {
        while let Ok(edit) = rx.recv() {
            // Load current version
            let current = engine.version_set.load();
            
            // Create new version
            let mut new_version = VersionSet::clone(&current);
            new_version.apply_edit(edit);
            
            // Write manifest atomically
            if let Err(e) = new_version.manifest.save_atomic(&engine.db_path) {
                eprintln!("Failed to save manifest: {}", e);
                continue;
            }
            
            // Publish new version atomically
            engine.version_set.store(Arc::new(new_version));
        }
    }
    
    pub fn apply_edit(&self, edit: VersionEdit) -> MidgeResult<()> {
        self.tx.send(edit)
            .map_err(|_| MidgeError::internal("version manager stopped"))
    }
}
```

### Phase 4: Replace Memtable RwLock with ArcSwap
```rust
pub struct ColumnFamily {
    pub memtable: ArcSwap<MemTable>,  // Was: RwLock<MemTable>
    // ...
}

// Reads become lock-free:
let mt = cf.memtable.load();
let value = mt.get(key);

// Writes still need sequence number coordination:
let seq = self.seq.fetch_add(1, Ordering::SeqCst);
let mt = cf.memtable.load();
mt.put(key, value, seq);

// Freeze becomes atomic:
let frozen = cf.memtable.swap(Arc::new(MemTable::new()));
```

### Phase 5: Update Flush to Use New Protocol
```rust
pub fn flush_cf(&self, cf: &ColumnFamilyHandle) -> MidgeResult<()> {
    // 1. Freeze (atomic, lock-free)
    let frozen = cf.memtable.swap(Arc::new(MemTable::new()));
    
    if frozen.is_empty() {
        return Ok(());
    }
    
    // 2. Build SST (pure I/O)
    let sst = self.build_sst_from_frozen(cf, frozen)?;
    
    // 3. Publish (via actor)
    self.version_manager.apply_edit(VersionEdit::AddFile {
        cf_id: cf.id(),
        file: sst.metadata,
    })
}
```

### Phase 6: Update Reads to Use VersionSet
```rust
pub fn get(&self, cf: &CF, key: &[u8]) -> MidgeResult<Option<Bytes>> {
    // Check memtable (lock-free)
    let mt = cf.memtable.load();
    if let Some(value) = mt.get(key) {
        return Ok(Some(value));
    }
    
    // Check SSTs (lock-free version load)
    let version = self.version_set.load();
    for file in &version.manifest.files {
        if let Some(reader) = version.sst_readers.get(&file.name) {
            if let Some(value) = reader.get(key)? {
                return Ok(Some(value));
            }
        }
    }
    
    Ok(None)
}
```

### Phase 7: Add Startup Reconciliation
```rust
fn reconcile_on_startup(&self) -> MidgeResult<()> {
    // Manifest is source of truth
    let manifest = Manifest::load(&self.db_path)?;
    let referenced: HashSet<_> = manifest.ssts.iter().collect();
    
    // Find orphaned SST files
    for entry in fs::read_dir(&self.sst_dir)? {
        let path = entry?.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.ends_with(".sst") && !referenced.contains(name) {
                warn!("Removing orphaned SST: {}", name);
                fs::remove_file(&path)?;
            }
        }
    }
    
    Ok(())
}

## Testing Strategy

1. **Unit Tests**: SstBuilder creates complete files
2. **Integration Tests**: Flush makes data immediately visible
3. **Concurrency Tests**: No race windows
4. **Crash Tests**: Manifest atomicity

## Migration Path

1. Introduce SstBuilder alongside existing code
2. Refactor flush_frozen_memtable to use new protocol
3. Update all callers (background flush, explicit flush)
4. Remove old flush_memtable_to_sst
5. Verify all tests pass

## Benefits

✅ **Lock-Free Reads**: No contention on hot path (reads never block)
✅ **Lock-Free Writes**: Memtable writes don't need locks (just atomic seq)
✅ **Atomic Visibility**: Version changes are instant and consistent
✅ **Simple Coordination**: Single actor serializes all version changes
✅ **Natural Backpressure**: Actor queue provides flow control
✅ **Crash Safe**: Manifest is always source of truth
✅ **Easy Testing**: Immutable versions make testing deterministic
✅ **Better Performance**: No lock contention, no cache line bouncing

## Design Decisions

1. **Memtable Per-CF or Global?**
   - ✅ Per-CF ArcSwap (allows independent CF freezing)

2. **VersionSet Global or Per-CF?**
   - ✅ Global ArcSwap (single atomic view of all SSTs)

3. **Version Manager: Actor or CAS Loop?**
   - ✅ Actor (simpler reasoning, natural backpressure)
   - Alternative: CAS loop with `version_set.compare_and_swap()`
   - Actor is easier to reason about and test

4. **Manifest Write Location?**
   - ✅ In version manager actor (serialized with version publish)

5. **SST Reader Caching?**
   - ✅ In VersionSet (immutable, cloned on edits)
   - Readers get consistent view of open SSTs

6. **Orphan Cleanup?**
   - ✅ On startup only (manifest is truth)
   - Could add periodic background cleanup

7. **Compaction Integration?**
   - ✅ Same VersionEdit protocol (add new files, remove old files)
   - Compaction sends batch of edits to actor

## Performance Characteristics

**Read Path:**
- 1x atomic load (memtable) - ~10ns
- 1x atomic load (version_set) - ~10ns  
- No mutex contention
- No cache line bouncing
- **~20ns overhead vs raw pointer**

**Write Path:**
- 1x atomic fetch_add (sequence) - ~10ns
- 1x atomic load (memtable) - ~10ns
- Memtable insert - ~100ns (skiplist)
- **~120ns total**

**Flush Path:**
- 1x atomic swap (freeze) - ~10ns
- SST build - ~10ms (I/O bound)
- 1x channel send (actor) - ~100ns
- **Dominated by I/O, coordination is negligible**

**Visibility Path (in actor):**
- Load current version - ~10ns
- Clone VersionSet - ~1μs (Arc clones)
- Apply edit - ~100ns
- Save manifest - ~100μs (fsync)
- Atomic store - ~10ns
- **~101μs, dominated by fsync**
