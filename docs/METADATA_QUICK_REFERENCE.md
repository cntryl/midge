# Metadata Module - Developer Quick Reference

## Module Purpose

The metadata module is the source of truth for all SST files, column families, and database state. It enables:
- Tracking LSM tree structure (levels, files, column families)
- Persisting manifest state to disk for recovery
- Managing manifest versions for concurrent reads
- Coordinating with cloud providers via checkpoints

## Quick API Reference

### Manifest (Primary Operations)

```rust
// Create/initialize
let manifest = Manifest::default();
let manifest = Manifest::new();

// Column family management
let cf_id = manifest.create_column_family("my_cf".to_string());
let cf = manifest.get_column_family_by_name("my_cf"); // Option<&ColumnFamilyMeta>
let cf = manifest.get_column_family_by_id(cf_id);     // Option<&ColumnFamilyMeta>
manifest.delete_column_family(cf_id);                 // bool
let active_cfs = manifest.active_column_families();   // Vec<&ColumnFamilyMeta>

// File management
let file = FileMeta { /* ... */ };
manifest.add_file(file);
manifest.remove_file("sst_001.sst");
let files = manifest.files_at_level(0); // Vec<&FileMeta>

// WAL sequence tracking
let next_seq = manifest.next_wal_seq();
manifest.increment_wal_seq();
```

### Persistence (I/O Operations)

```rust
// Load from disk (graceful: returns default if missing)
let manifest = ManifestPersistence::load(&db_path)?;

// Save to disk (atomic: uses temp file + rename)
ManifestPersistence::save(&db_path, &manifest)?;

// Delete manifest file
ManifestPersistence::delete(&db_path)?;
```

### VersionManager (Edit Management)

```rust
let mut manager = VersionManager::new(&mut manifest, &mut version_set);

// Queue edits
manager.add_edit(ManifestEdit::AddFile(file));
manager.add_edit(ManifestEdit::DeleteFile("sst_001.sst".to_string()));

// Apply edits atomically (all succeed or none)
manager.apply_edits()?; // Creates new version

// Clear edits without applying
manager.clear_edits();
```

### VersionSet (Version History)

```rust
let mut version_set = VersionSet::new(manifest);

// Install new version
version_set.install_version(new_manifest);

// Query versions
let current = version_set.current_version();       // &Manifest
let version = version_set.get_version(version_id); // Option<&Manifest>
let has_version = version_set.has_version(id);    // bool

// Query files
let files = version_set.files_for_cf(cf_id); // Vec<&FileMeta>
```

## Common Patterns

### Creating and Using a Column Family

```rust
let mut manifest = Manifest::default();

// Create column family
let cf_id = manifest.create_column_family("my_cf".to_string());

// Add files to this CF
let file = FileMeta {
    cf_id,
    name: "sst_001.sst".to_string(),
    level: 0,
    size_bytes: 4096,
    ..Default::default()
};
manifest.add_file(file);

// Persist to disk
ManifestPersistence::save(&db_path, &manifest)?;
```

### Batch Apply Edits

```rust
let mut manager = VersionManager::new(&mut manifest, &mut version_set);

// Queue multiple edits
for file in files_to_compact {
    manager.add_edit(ManifestEdit::DeleteFile(file.name.clone()));
}
for new_file in compacted_files {
    manager.add_edit(ManifestEdit::AddFile(new_file));
}

// Apply all atomically
manager.apply_edits()?; // Either all succeed or manifest unchanged
```

### Delete Column Family

```rust
// Delete marks CF with deleted_at timestamp
manifest.delete_column_family(cf_id); // bool - returns false if not found

// get_column_family_by_id returns None for deleted CFs
let cf = manifest.get_column_family_by_id(cf_id); // None

// But deleted CF still in manifest.column_families with deleted_at set
for cf in &manifest.column_families {
    if cf.id == cf_id && cf.deleted_at.is_some() {
        println!("CF was deleted");
    }
}

// active_column_families() only returns non-deleted CFs
let active = manifest.active_column_families();
```

## Error Handling

### Missing Manifest File
```rust
// Graceful: returns default manifest if file missing
let manifest = ManifestPersistence::load(&db_path)?; // Never fails for missing files
```

### Invalid Operations
```rust
// Returns false if CF not found
let deleted = manifest.delete_column_family(999); // false

// Returns Option<...> for queries
let cf = manifest.get_column_family_by_id(999); // None
```

### Empty Edit List
```rust
let mut manager = VersionManager::new(&mut manifest, &mut version_set);
// Must add at least one edit before applying
manager.apply_edits()?; // Err: "No edits to apply"
```

## Testing Examples

### Testing CF Creation

```rust
#[test]
fn should_create_column_family() {
    // Arrange
    let mut manifest = Manifest::default();

    // Act
    let cf_id = manifest.create_column_family("my_cf".to_string());

    // Assert
    assert_eq!(cf_id, 1);
    assert_eq!(manifest.column_families.len(), 1);
    assert_eq!(manifest.column_families[0].name, "my_cf");
}
```

### Testing File Management

```rust
#[test]
fn should_get_files_at_level() {
    // Arrange
    let mut manifest = Manifest::default();
    manifest.add_file(FileMeta {
        level: 0,
        name: "l0_file.sst".to_string(),
        ..Default::default()
    });

    // Act
    let files = manifest.files_at_level(0);

    // Assert
    assert_eq!(files.len(), 1);
}
```

### Testing Persistence

```rust
#[test]
fn should_roundtrip_manifest() {
    // Arrange
    let mut manifest = Manifest::default();
    manifest.create_column_family("cf".to_string());
    let db_path = tempdir().path();

    // Act
    ManifestPersistence::save(&db_path, &manifest)?;
    let loaded = ManifestPersistence::load(&db_path)?;

    // Assert
    assert_eq!(loaded.column_families.len(), 1);
}
```

## Key Invariants to Remember

1. **CF IDs are unique and auto-incrementing** - Don't manually create IDs
2. **Deleted CFs return None from get_column_family_by_id()** - Use active_column_families() to skip deleted
3. **Edits must be applied atomically** - Either all succeed or manifest unchanged
4. **Manifest saves are atomic** - Uses temp file + rename, never corrupts
5. **Versions are immutable snapshots** - Safe for concurrent reads

## Integration Points

- **Engine**: Queries manifest during reads/writes
- **Compaction**: Uses file levels and CF info for decisions
- **WAL**: Coordinates sequence numbers
- **SST**: Tracks file metadata
- **Cloud**: Stores checkpoint state

## Documentation Links

- [Full Review](METADATA_MODULE_REVIEW.md)
- [Test Coverage](METADATA_TEST_COVERAGE.md)
- [Audit Report](../METADATA_AUDIT_COMPLETE.md)

---

**Quick Tip**: Always use `active_column_families()` when you only want non-deleted CFs, and remember that deleted CFs are marked with `deleted_at` timestamp for historical tracking.
