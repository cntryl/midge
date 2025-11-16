# Immediate Fix: Atomic Manifest + Cache Update

## The Bug

Current code in `flush_frozen_memtable()`:

```rust
// 1. Write SST file (no lock needed - file I/O)
let (file_path, file_meta) = flush_memtable_to_sst(...)?;

// 2. Update manifest
let mut m = Manifest::load(...)?;
m.files.push(file_meta);
m.save_atomic(&self.db_path)?;

// 3. Update cache
self.update_manifest_cache(m);
```

**Race window between steps 2 and 3:**
- Manifest on disk has new SST
- Cache in memory doesn't
- Reads use cache → can't find data!

## The Fix

Hold `flush_mutex` during the entire visibility transition:

```rust
// 1. Write SST file (no lock)
let (file_path, file_meta) = write_sst(...)?;

// 2. Atomic visibility update (under lock)
{
    let _visibility_guard = self.flush_mutex.lock();
    
    // Load manifest
    let mut m = Manifest::load(...)?;
    
    // Update manifest
    m.files.push(file_meta);
    m.save_atomic(&self.db_path)?;
    
    // Update cache immediately (before releasing lock)
    self.update_manifest_cache(m);
    
    // Lock released - file is now visible atomically
}
```

## Why This Works

1. **Atomic Transition**: Manifest write + cache update happen atomically under lock
2. **No Reader Race**: Readers either see old state (both places) or new state (both places)
3. **Minimal Lock Time**: Lock only held during manifest update (~microseconds)
4. **No Deadlock**: Lock ordering is consistent (always flush_mutex for visibility)

## Implementation

See changes to `src/core/engine/operations/maintenance.rs`
