# Midge Dependency Graph - Quick Reference

## Layered Architecture (Bottom-Up)

```
╔════════════════════════════════════════════════════════════════════╗
║                        Layer 3: CORE                              ║
║   Engine • Compaction • Transactions • Manifest • Backup          ║
║   ──────────────────────────────────────────────────────────────  ║
║   Dependencies: All modules below (api, wal, sst, health)        ║
║   + cloud (approved for locking only)                            ║
╚════════════════════════════════════════════════════════════════════╝
                              ▲
                              │ depends on
                              │
╔════════════════════════════════════════════════════════════════════╗
║                    Layer 2: STORAGE                               ║
║   WAL • SST • Health                                              ║
║   ──────────────────────────────────────────────────────────────  ║
║   WAL:     Uses api::ColumnFamilyId for records                  ║
║   SST:     Uses cloud::StorageBackend for multi-backend support  ║
║   Health:  Independent health checks                             ║
╚════════════════════════════════════════════════════════════════════╝
                              ▲
                              │ depends on
                              │
╔════════════════════════════════════════════════════════════════════╗
║                   Layer 1: CONFIG & CLOUD                         ║
║   Config • Cloud • FS                                             ║
║   ──────────────────────────────────────────────────────────────  ║
║   Config:  Uses wal::cloud for cloud WAL mode selection          ║
║   Cloud:   S3 • Azure • GCS • OCI implementations                ║
║   FS:      Filesystem abstraction                                ║
╚════════════════════════════════════════════════════════════════════╝
                              ▲
                              │ depends on
                              │
╔════════════════════════════════════════════════════════════════════╗
║                    Layer 0: FOUNDATION                            ║
║   API • Common • Metrics                                          ║
║   ──────────────────────────────────────────────────────────────  ║
║   API:      Public traits: KvStore, KvTransaction, WriteBatch   ║
║   Common:   Error types, codecs, utilities, timestamps           ║
║   Metrics:  Performance tracking (cross-cutting concern)         ║
╚════════════════════════════════════════════════════════════════════╝
```

## Dependency Matrix

```
         │  api  common metrics  config  cloud   fs   wal   sst  health  core
    ─────┼──────────────────────────────────────────────────────────────────────
    api  │   -     ✓     -       -       -      -    -     -     -       -
    common│  -     -     -       -       -      -    -     -     -       -
    metrics│ -     -     -       -       -      -    -     -     -       -
    config│ -     ✓     ✓       -       ✓      -    ✓     -     -       -
    cloud │ -     ✓     ✓       -       -      -    -     -     -       -
    fs    │ -     ✓     ✓       -       -      -    -     -     -       -
    wal   │ ✓     ✓     ✓       -       -      -    -     -     -       -
    sst   │ -     ✓     ✓       -       ✓      -    -     -     -       -
    health│ -     ✓     ✓       -       -      -    -     -     -       -
    core  │ ✓     ✓     ✓       ✓       ✓      -    ✓     ✓     ✓       -
    ─────┴──────────────────────────────────────────────────────────────────────
    ✓ = module X depends on module Y
    - = no dependency
```

## Key Dependency Paths

### API Module (Foundation)
```
api/
  ├─ error.rs              → common::error
  ├─ column_family.rs      → (no internal deps)
  ├─ kv_store.rs           → common::error
  ├─ merge_operator.rs     → common::error
  ├─ mutation.rs           → api::column_family
  ├─ query.rs              → (no internal deps)
  ├─ snapshot.rs           → (no internal deps)
  ├─ write_batch.rs        → api::column_family
  └─ write_options.rs      → (no internal deps)
```

### Config Module (Layer 1)
```
config/
  ├─ builder.rs            → wal::cloud::CloudStorageBackend
  ├─ cloud_builder.rs      → cloud::HybridStorage, cloud::StorageBackend
  ├─ storage_mode.rs       → wal::cloud::CloudStorageBackend
  ├─ autotune.rs           → (foundation only)
  ├─ column_family.rs       → (foundation + api re-export)
  ├─ derivation.rs         → (foundation only)
  ├─ validation.rs         → (foundation only)
  └─ profile.rs            → (foundation only)
```

### WAL Module (Layer 2)
```
wal/
  ├─ types.rs              → api::column_family::ColumnFamilyId
  ├─ traits.rs             → (foundation only)
  ├─ encoding.rs           → common::codec, common::tlv
  ├─ fs/
  │  ├─ writer.rs          → wal::* (internal)
  │  └─ reader.rs          → wal::* (internal)
  ├─ mem/
  │  ├─ writer.rs          → wal::* (internal)
  │  └─ reader.rs          → wal::* (internal)
  ├─ cloud/
  │  ├─ reader.rs          → cloud::StorageBackend
  │  └─ writer.rs          → cloud::StorageBackend
  └─ coordinator.rs        → wal::* (internal)
```

### SST Module (Layer 2)
```
sst/
  ├─ format.rs             → (foundation only)
  ├─ bloom.rs              → (foundation only)
  ├─ fs/
  │  ├─ reader.rs          → sst::* (internal)
  │  ├─ writer.rs          → sst::* (internal)
  │  └─ factory.rs         → sst::* (internal)
  ├─ cloud/
  │  ├─ reader.rs          → cloud::StorageBackend
  │  ├─ writer.rs          → cloud::StorageBackend
  │  └─ factory.rs         → cloud::StorageBackend
  └─ cache.rs              → sst::* (internal)
```

### Core Module (Layer 3)
```
core/
  ├─ engine/
  │  ├─ operations/writes.rs      → api::*, wal::*, sst::*
  │  └─ state/initialization.rs   → core::*, metrics::*
  ├─ transaction/
  │  ├─ core.rs                    → api::mutation::*
  │  ├─ spill.rs                   → api::*
  │  └─ engine_transaction.rs      → api::KvTransaction
  ├─ persistence/
  │  ├─ wal_replay.rs              → wal::WalRecord
  │  ├─ flush.rs                   → metrics::*
  │  └─ flush_coordinator.rs       → (internal)
  ├─ manifest/
  │  ├─ types.rs                   → api::column_family::*
  │  └─ column_families.rs         → api::column_family::*
  ├─ memtable/
  │  ├─ core.rs                    → wal::WalRecord
  │  └─ wal_loading.rs             → wal::WalRecord
  ├─ locking/
  │  └─ cloud.rs                   → cloud::StorageBackend (approved)
  └─ backup/                       → (internal + api)
```

## Circular Dependency Check: ✅ PASSED

```
Core → WAL ✓ (one-way)
WAL → Core ✗ (FIXED: metrics moved to foundation)

Core → Cloud ✓ (one-way, approved for locking)
Cloud → Core ✗ (ENFORCED)

API → Config ✗ (FIXED: types moved to API)
Config → API ✓ (one-way)

Result: DAG ✓ (Directed Acyclic Graph)
```

## Module Statistics

| Module   | Layer | Files | Primary Responsibility |
|----------|-------|-------|------------------------|
| api      | 0     | 8     | Public traits & types |
| common   | 0     | 7     | Error, codec, utilities |
| metrics  | 0     | 2     | Performance tracking |
| config   | 1     | 7     | High-level configuration |
| cloud    | 1     | 6     | Cloud storage backends |
| fs       | 1     | 1     | Filesystem abstraction |
| wal      | 2     | 9     | Write-ahead logging |
| sst      | 2     | 15    | SSTable format & filters |
| health   | 2     | 1     | Database health checks |
| core     | 3     | 25+   | LSM engine & transactions |
| **TOTAL**| -     | **81** | - |

## Last Updated

Generated: 2025-11-12
Analysis Type: Comprehensive dependency graph review
Status: ✅ CLEAN - No violations detected
