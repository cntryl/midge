# Midge Architecture Inventory

## 1. Storage Engine Surface

- **MidgeEngine** is the core struct with WAL, memtables, SST factory, compaction coordinator
- **Operations**: `put`, `get`, `delete`, `delete_range`, `scan`, `write_batch`
- **Transaction surface**: Full OCC (optimistic concurrency control) with `begin_transaction`, `commit_transaction`, `rollback`
- **MVCC**: Yes — sequence numbers per key, version chains in skiplist, snapshot isolation
- **Iterators**: Yes — forward/reverse scans with snapshot consistency
- **Column families**: Yes — full CF lifecycle (create/drop/list)
- **Merge operators**: Yes — user-defined merge with `MergeOperator` trait

---

## 2. Memtable Layer

- **Data structure**: Lock-free skiplist (`src/core/data_structures/skiplist.rs`) with MVCC version chains
- **Write order**: WAL-first (append to WAL, then memtable)
- **Immutable memtable queue**: Yes — multiple immutables allowed
- **Concurrent flushes**: Via `FlushCoordinator` with configurable parallelism
- **Write-stall mechanism**: Yes — tracked via `write_stalls`, `background_write_stalls`, `capacity_write_stalls` metrics; stalls on too many immutables or L0 files

---

## 3. WAL Layer

- **Segmentation**: Yes — multiple WAL segments with rotation
- **Checksums**: CRC32C per frame (paranoid mode optional)
- **Frame format**: TLV (Tag-Length-Value) with compression support (LZ4 for values ≥256 bytes)
- **Recovery**: Replay WAL to memtables after last manifest sequence
- **Group commit**: No explicit batch writer (per-write append + optional sync)
- **Fsync**: Configurable via `wal_sync` option; `TestHooks` can inject `FsyncBehavior::Skip`

---

## 4. SST Layer

| Feature | Status |
|---------|--------|
| Block-based | ✅ Yes |
| Index block | ✅ Yes |
| Filter block | ✅ Yes (Bloom) |
| Partitioned index | ❌ No |
| Restart points | ✅ Yes (in data blocks) |
| Prefix compression | ❌ No (full keys stored) |
| Checksums per block | ✅ Yes (CRC32C in 5-byte trailer) |
| Footer with offsets | ✅ Yes (48 bytes, RocksDB-compatible magic) |
| Bloom per file or block | ✅ Per-file (single filter block) |
| Meta-index block | ✅ Yes |

---

## 5. Compaction

- **Strategy**: Leveled compaction (configurable L0 → L1 → ... → Ln)
- **Picker**: `LeveledCompactionConfig` with `pick_leveled_compaction()` — picks by level size ratio
- **Compaction filters**: Yes — `CompactionFilter` trait with `FilterDecision`
- **Back-pressure**: Write stalls on L0 file count exceeded
- **Parallel compactions**: Via `CompactionController` with configurable threads
- **Boundary logic**: Target level sizes multiply by 10× per level; L0 file count trigger

---

## 6. Manifest

- **Structure**: `Manifest` with SST list, sequence number, CF metadata
- **Edit log**: Yes — `VersionEdit` enum (`AddFile`, `RemoveFiles`, `CombinedAddRemove`, `UpdateSequence`)
- **Atomic swap**: Via `VersionManager` actor with serialized updates → `AtomicVersionSet` for lock-free reads
- **Checksums**: Yes (on SST files; manifest itself serialized via TLV)
- **Reopen performance**: Cached via `ManifestCache` — eliminates disk load on get()
- **Version ancestry**: Linear (single parent, not DAG)

---

## 7. Block Cache

- **Exists**: ✅ Yes (`src/sst/block_cache.rs`)
- **Sharded**: ✅ Yes — `create_sharded_cache(size, num_shards)`
- **Admission policy**: LRU + adaptive cache that auto-switches on contention
- **Hot tier**: Lock-free `CompactBlockKey` fast-path lookups
- **Cached types**: Data blocks, index blocks, filter blocks
- **Charge**: By bytes

---

## 8. Bloom Filters

- **Level**: Per-file (single filter block per SST)
- **Format**: Blocked layout (256-byte blocks), Kirsch-Mitzenmacher double hashing, xxh3
- **Trigger rules**: Always built during SST creation
- **Caching**: Separate `BloomCache` for fast SST pre-checks
- **Sparse index**: Separate `SparseIndexCache` for block lookups

---

## 9. Testing Infrastructure

| Feature | Status |
|---------|--------|
| Tiered benches (1–6) | ✅ Yes (`benches/tier1_hotpath/` through `tier6_capacity/`) |
| Stress tests | ✅ Yes (`stress_workloads.rs`, `stress_large_values.rs`) |
| Fuzz tests | ✅ Yes (`fuzz/fuzz_targets/` — WAL, bloom, TLV, SST, internal key, block) |
| Crash injection | ✅ Yes via `TestHooks` (`FsyncBehavior::Skip`, `IoBehavior`, etc.) |
| Memtable gating | ✅ Yes (flush coordinator integration) |
| Compaction gating | ✅ Yes (`CompactionGatePoint::AfterManifestUpdate`) |
| WAL gating | ✅ Yes (`WalBehavior` hook) |
| Deterministic harness | ✅ Yes (`TestHooks` with clock fast-forward, fsync skip, IO injection) |
| Proptest | ✅ Yes (`proptest_parsers.rs`) |

---

## 10. Storage Modes

| Mode | Status |
|------|--------|
| LocalDisk | ✅ Full support |
| Memory | ✅ In-memory mode (no disk) |
| CloudBacked | ✅ Full (S3/Azure/GCS/OCI via `src/cloud/`) |
| Hybrid | ✅ Yes (`src/cloud/hybrid.rs`) — local cache + cloud tiering |
| Compression | ✅ LZ4, Zstd (levels 1/3/5/9) |
| Encryption | ❌ Not implemented |

---

## Directory Tree

```
src/
├── api/          # Public API (KvStore, WriteBatch, Transaction, Snapshot, Query)
├── cloud/        # Cloud backends (aws, azure, gcp, oci, mock, hybrid)
├── common/       # Codec, TLV, test_hooks, timestamps
├── config/       # MidgeOptions, Autotuner, profiles
├── core/
│   ├── backup/         # Backup/restore
│   ├── compaction/     # Controller, executor, filter, strategy
│   ├── data_structures/# SkipList
│   ├── engine/         # MidgeEngine core + operations
│   ├── locking/        # DB lock (local + cloud lease)
│   ├── manifest/       # VersionSet, VersionEdit, VersionManager
│   ├── memtable/       # MemTable, range tombstones
│   ├── persistence/    # Flush coordinator, WAL replay
│   └── transaction/    # TransactionController, OCC
├── fs/           # Filesystem abstractions
├── health/       # Health checks
├── metrics/      # Engine metrics, performance metrics
├── sst/          # SST format, bloom, block_cache, readers/writers, cloud SST
└── wal/          # WAL controller, encoding, fs/cloud/mem implementations

benches/
├── tier1_hotpath/    # Bloom, memtable, skiplist, WAL, cache
├── tier2_subsystem/  # Block cache eviction, bloom build
├── tier3_system/     # Durability modes, compaction
├── tier4_integration/
├── tier5_soak/
└── tier6_capacity/

tests/            # 56 integration test files
fuzz/fuzz_targets/# 6 fuzz targets
```

---

## Summary

Midge is a feature-complete LSM engine with WAL, leveled compaction, MVCC, transactions, column families, cloud tiering, and comprehensive testing.

**Main gaps**:
- Partitioned index
- Prefix compression
- Encryption
