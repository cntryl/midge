# Midge Operational Modes: LOCAL vs CLOUD-NATIVE

## Quick Reference

| Aspect | Local-Only Mode | Cloud-Native Mode |
|--------|-----------------|-------------------|
| **Configuration** | `StorageMode::LocalDisk` | `StorageMode::CloudBacked` |
| **Source of Truth** | Local WAL + SST files | Cloud WAL + SST files |
| **Manifest Role** | Metadata/optimization only | Metadata/optimization only |
| **Local Disk Role** | Primary storage | Ephemeral cache (data reconstructible from WAL/SSTs) |
| **Can Delete Local Data?** | ❌ No (would lose data) | ✅ Yes (recoverable from cloud) |
| **Recovery After Crash** | Read from local disk | Read from cloud, use local cache as fallback |
| **Cloud Backend** | Not used | Required for operation |
| **Use Case** | Embedded, single-machine | Distributed, cloud-native, high-availability |
| **Code Path** | Simple, no cloud operations | Cloud-first with fallback |

---

## Detailed Mode Description

### LOCAL-ONLY MODE

**When to Use:**
- Database embedded in a single application
- Testing and development
- Air-gapped deployments (no network)
- Non-critical data (data loss acceptable)
- Single machine, single process

**Characteristics:**
- Zero cloud interaction
- WAL and SST files are the source of truth
- Manifest is metadata/optimization (can be rebuilt from WAL/SSTs)
- All storage operations are filesystem calls
- Faster (no network latency)
- Less resilient (one local failure = data loss)

**Configuration Example:**
```rust
let opts = MidgeOptions {
    storage_mode: StorageMode::LocalDisk {
        db_path: PathBuf::from("/var/data/mydb"),
    },
    ..Default::default()
};

let engine = MidgeEngine::open(opts)?;
```

**Recovery Example:**
```
Crash occurs → Restart → open() called
  ↓
init_manifest_cloud_first(db_path, cloud_backend=None, ...)
  ↓
Manifest::load_with_cloud_fallback(..., cloud_backend=None, ...)
  ↓
Cloud code skipped (backend is None)
  ↓
Read local CURRENT → {db_path}/manifest.json
  ↓
Load memtables and apply WAL
  ↓
Database recovered
```

**Data Layout:**
```
{db_path}/
├── CURRENT              # Points to manifest file
├── manifest.json        # Database metadata
├── manifest.json.tmp    # During atomic write
├── wal/
│   ├── 000001.log       # WAL segments
│   ├── 000002.log
│   └── ...
└── sst/
    ├── sst_0_0001       # SST files
    ├── sst_0_0002
    └── ...
```

---

### CLOUD-NATIVE MODE

**When to Use:**
- Distributed systems
- Cloud-deployed applications
- Multi-zone/region deployments
- Critical data (high availability required)
- Needs disaster recovery capability
- Multiple processes/replicas accessing same data

**Characteristics:**
- Cloud backend is configured and required
- WAL and SST files in cloud are the source of truth
- Manifest stored in cloud as metadata/optimization (can be rebuilt from WAL/SSTs)
- Local disk used for caching only (can be deleted)
- Network latency for remote operations
- Highly resilient (cloud survives local failures)
- Recoverable from zone/node failures

**Configuration Example:**
```rust
let cloud_backend = Arc::new(MockCloudBackend::new());  // Or S3Backend, AzureBackend, etc.

let opts = MidgeOptions {
    storage_mode: StorageMode::CloudBacked {
        local_cache_path: PathBuf::from("/tmp/midge_cache"),
        cloud_backend,
        storage_context: StorageContext::default(),
        local_wal_sync: true,
        wal_batch_size: 4 * 1024 * 1024,
        sst_cache_capacity: 16,
    },
    ..Default::default()
};

let engine = MidgeEngine::open(opts)?;
```

**Recovery Example - Cloud Available:**
```
Crash occurs → Node dies → New node starts recovery
  ↓
init_manifest_cloud_first(db_path, cloud_backend=Some(backend), ...)
  ↓
Manifest::load_with_cloud_fallback(..., cloud_backend=Some(backend), ...)
  ↓
Try Cloud: Load manifest from {prefix}/manifest/CLOUD_CHECKPOINT ✓
  ↓
Verify cloud checkpoint integrity ✓
  ↓
Use cloud manifest metadata to locate WAL + SSTs (the actual source of truth)
  ↓
Load memtables from WAL + SST files (the real data)
  ↓
Database recovered, fully consistent
```

**Recovery Example - Cloud Temporarily Unavailable:**
```
Crash occurs → Cloud connection temporarily down
  ↓
Try Cloud: Load manifest from cloud → FAILS (network error)
  ↓
Fall back to Local: Load manifest from {local_cache}/manifest.json ✓
  ↓
Warn user: "Cloud unavailable, using local cache (may be stale)"
  ↓
Load memtables and apply local WAL
  ↓
Database recovered (may lose recent writes not synced to cloud)
  ↓
Once cloud available again, can verify and reconcile
```

**Recovery Example - Disaster (Both Cloud and Local Lost):**
```
Disaster: Zone failure → Node lost → Local cache deleted → Cloud in different zone
  ↓
New node starts with empty local cache
  ↓
Try Cloud: Load manifest from cloud → SUCCESS
  ↓
Cloud was available in different zone, has all data
  ↓
Load manifest from cloud
  ↓
Fetch WAL and SST from cloud
  ↓
Rebuild local cache from cloud state
  ↓
Database fully recovered in new zone
```

**Data Layout - Cloud:**
```
Cloud Storage (S3/Azure/GCP):
midge/
├── manifest/
│   ├── CLOUD_CHECKPOINT    # {"checkpoint_sequence": 1000, "covering_ssts": [...]}
│   └── manifest.json       # Full manifest with all metadata
├── wal/
│   ├── 000001.wal          # WAL segment seq 0-100
│   ├── 000002.wal          # WAL segment seq 101-200
│   └── ...
└── sst/
    ├── sst_0_0001.blob     # SST file, level 0, id 1
    ├── sst_0_0002.blob     # SST file, level 0, id 2
    ├── sst_1_0001.blob     # SST file, level 1, id 1
    └── ...
```

**Data Layout - Local Cache:**
```
{local_cache_path}/
├── CURRENT              # Cached copy (may be stale)
├── manifest.json        # Cached copy (may be stale)
├── wal/
│   ├── 000001.log       # Cached WAL segments (may be pruned in cloud)
│   ├── 000002.log
│   └── ...
└── sst/
    ├── sst_0_0001       # Cached SST blocks (can be deleted/refetched)
    ├── sst_0_0002
    └── ...
```

**Important**: Local cache can be deleted safely (recoverable from cloud WAL + SSTs). Cloud is where the actual data lives; manifest is just metadata/optimization to avoid scanning all WAL/SSTs on every startup.

---

## Implementation: How Mode is Selected

**Decision Point**: During engine initialization

```rust
// In src/core/engine/state.rs::open_with_factories()

let cloud_backend = opts.storage_mode.cloud_backend();  // None or Some
let cloud_prefix = opts.storage_mode.cloud_prefix();

// Cloud_backend decision:
// - If None → LOCAL-ONLY MODE (cloud code skipped)
// - If Some → CLOUD-NATIVE MODE (cloud-first recovery)

let (manifest, _) = init_manifest_cloud_first(
    &db_path,
    cloud_backend.as_deref(),  // ← This decides the mode
    cloud_prefix.as_deref(),
    ...
)?;
```

**In manifest recovery** (`src/core/manifest/cloud_recovery.rs`):

```rust
pub fn load_with_cloud_fallback(
    db_path: &Path,
    cloud_backend: Option<&dyn StorageBackend>,  // ← Key decision point
    cloud_prefix: Option<&str>,
) -> MidgeResult<Self> {
    // If cloud_backend is None:
    if cloud_backend.is_none() {
        // LOCAL-ONLY: Skip all cloud code
        return Manifest::load(db_path)
            .or_else(|_| Ok(Manifest::default()));
    }
    
    // If cloud_backend is Some:
    // CLOUD-NATIVE: Try cloud first
    if let Some(backend) = cloud_backend {
        match Manifest::load_from_cloud(backend, cloud_prefix) {
            Ok(manifest) => return Ok(manifest),
            Err(_) => {
                // Fall back to local
            }
        }
    }
    
    // Fall back to local
    Manifest::load(db_path)
        .or_else(|_| Ok(Manifest::default()))
}
```

---

## Key Invariants

### LOCAL-ONLY Mode
1. Cloud backend is never accessed
2. Local WAL + SST files are the source of truth
3. Local manifest is metadata/optimization (can be rebuilt from WAL/SSTs)
4. Recovery reads from local disk
5. Losing local disk = losing data
6. No network calls

### CLOUD-NATIVE Mode
1. Cloud WAL + SST files are the source of truth
2. Cloud manifest is metadata/optimization (helps avoid re-scanning WAL/SSTs)
3. Cloud checkpoint MUST be kept reasonably current (enables fast recovery)
4. Local cache is ephemeral (all data reconstructible from cloud WAL/SSTs)
5. Recovery tries to use cloud manifest metadata, falls back to scanning if needed
6. Network calls expected (and acceptable)

---

## Testing Approach

### For LOCAL-ONLY Mode Tests
```rust
#[test]
fn should_recover_local_only_database() {
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path },
        ..Default::default()
    };
    
    // Cloud backend is not provided
    // Tests verify local-only paths work
}
```

### For CLOUD-NATIVE Mode Tests
```rust
#[test]
fn should_recover_from_cloud_checkpoint() {
    let backend = MockCloudBackend::new();
    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            cloud_backend: Arc::new(backend),
            ..Default::default()
        },
        ..Default::default()
    };
    
    // Cloud backend is provided
    // Tests verify cloud-first paths work
}

#[test]
fn should_fallback_to_local_when_cloud_unavailable() {
    let backend = FailingCloudBackend::new();
    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            cloud_backend: Arc::new(backend),
            ..Default::default()
        },
        ..Default::default()
    };
    
    // Cloud fails, but local cache available
    // Tests verify fallback path works
}
```

---

## Summary

**Two Clear Operational Modes:**

1. **LOCAL-ONLY**: Embedded database, no cloud
   - Simple, fast, single source (local disk)
   - Code path: Skip cloud, use local
   - Risk: Local failure = data loss

2. **CLOUD-NATIVE**: Distributed database, cloud storage
   - Complex, remote durable, cloud is source of truth
   - Code path: Try cloud first, fall back to local, use default
   - Resilience: Can recover from any single point of failure

**The key**: The `cloud_backend` parameter is `Option`, where:
- `None` = LOCAL mode (cloud code never runs)
- `Some` = CLOUD-NATIVE mode (WAL/SSTs in cloud, manifest is just optimization)

This design ensures clear semantics:
- **WAL + SSTs** = Real data (source of truth)
- **Manifest** = Metadata/optimization (can be rebuilt, not critical)
- **Cloud**: Enables resilience by storing WAL + SSTs remotely
- **Cloud code never runs unless explicitly configured**
