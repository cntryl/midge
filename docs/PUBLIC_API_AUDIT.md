# Midge Public API Audit

**Generated:** January 8, 2026  
**Purpose:** Document the complete public API surface to identify simplification opportunities

---

## Overview

The Midge crate exposes a comprehensive LSM-tree engine API. This document catalogs all public types, functions, and modules currently exported in `src/lib.rs`.

---

## Top-Level Exports (from `src/lib.rs`)

### Public Modules
```rust
pub mod common;
pub mod io;
pub mod telemetry;
pub mod storage;
pub mod iterators;
pub mod wal;
pub mod sst;
pub mod metadata;
pub mod engine;
pub mod runtime;
pub mod compaction;
pub mod metrics;
pub mod testkit;
```

**Analysis:** 13 top-level public modules. Many of these expose internal implementation details.

---

## Re-Exported Types (Stable API)

### Core Error Types
```rust
pub use common::{AckPolicy, MidgeError, MidgeResult};
```

### Main Engine API
```rust
pub use engine::{
    open_engine,
    ColumnFamilyHandle,
    ColumnFamilyId,
    MidgeEngine,
};
```

### High-Level API Types (from `engine::api`)
```rust
pub use engine::api::{
    // Errors
    ApiError,
    ApiResult,
    
    // Results
    CasResult,
    InsertResult,
    
    // Column families
    ColumnFamily,
    
    // Core types
    Direction,
    Durability,
    Goal,
    IsolationLevel,
    Iterator,
    Key,
    KvPair,
    KvTransaction,
    MemoryBudget,
    MergeOperator,
    OpenOptions,
    Query,
    Snapshot,
    Transaction,
    Value,
    WorkloadProfile,
    WriteBatch,
    WriteOptions,
};
```

### Observability
```rust
pub use metrics::{EngineMetrics, PerformanceMetrics};
```

### Testing Utilities
```rust
pub use testkit::{MidgeOptions, MockStorage, StorageMode};
```

---

## Main Engine Operations (MidgeEngine Methods)

### Initialization & Lifecycle (5 methods)
- `open<P: OpenParam>(param: P) -> MidgeResult<Self>`
- `open_with_options(opts: MidgeOptions) -> MidgeResult<Self>`
- `shutdown(self) -> MidgeResult<()>`
- `default_column_family(&self) -> &ColumnFamilyHandle`
- `memtable_size(&self) -> usize`

### Basic KV Operations (8 methods)
- `put(&self, cf, key, value) -> MidgeResult<()>`
- `put_with_ttl(&self, cf, key, value, ttl) -> MidgeResult<()>`
- `put_cf(&self, cf, key, value) -> MidgeResult<()>` *(duplicate)*
- `get(&self, cf, key) -> MidgeResult<Option<Bytes>>`
- `get_cf(&self, cf, key) -> MidgeResult<Option<Bytes>>` *(duplicate)*
- `get_transactional(&self, cf, key, snapshot_seq) -> MidgeResult<Option<Bytes>>`
- `get_at(&self, cf, key, sequence) -> MidgeResult<Option<Bytes>>`
- `delete(&self, cf, key) -> MidgeResult<()>`
- `delete_cf(&self, cf, key) -> MidgeResult<()>` *(duplicate)*

### Advanced KV Operations (8 methods)
- `insert(&self, cf, key, value) -> MidgeResult<bool>` *(put-if-absent)*
- `insert_with_ttl(&self, cf, key, value, ttl) -> MidgeResult<bool>`
- `insert_with_value(&self, cf, key, value) -> MidgeResult<InsertResult>`
- `insert_with_value_and_ttl(&self, cf, key, value, ttl) -> MidgeResult<InsertResult>`
- `compare_and_swap(&self, cf, key, old, new) -> MidgeResult<CasResult>`
- `register_merge_operator(&self, cf, op) -> MidgeResult<()>`
- `merge(&self, key, operand) -> MidgeResult<()>`
- `merge_cf(&self, cf, key, operand) -> MidgeResult<()>`

### Range Operations (4 methods)
- `range(&self, cf, start, end) -> MidgeResult<Vec<KvPair>>`
- `range_cf(&self, cf, start, end) -> MidgeResult<Vec<KvPair>>`
- `scan(&self, cf, query: &Query) -> MidgeResult<Vec<KvPair>>`
- `delete_range(&self, cf, start, end) -> MidgeResult<()>`

### Batch Operations (1 method)
- `write_batch(&self, batch: &WriteBatch) -> MidgeResult<()>`

### Transactions (7 methods)
- `begin_transaction(&self, isolation) -> MidgeResult<Box<dyn KvTransaction>>`
- `begin_transaction_with_isolation(&self, isolation) -> MidgeResult<Box<dyn KvTransaction>>`
- `transaction(&self) -> Transaction`
- `transaction_with_isolation(&self, isolation) -> Transaction`
- `commit_transaction_boxed(&self, txn: Box<dyn KvTransaction>) -> MidgeResult<()>`
- `commit_transaction(&self, txn: Transaction) -> MidgeResult<()>`
- `rollback_transaction(&self, txn: Transaction) -> MidgeResult<()>`

### Snapshots (3 methods)
- `snapshot(&self) -> Snapshot`
- `snapshot_cf(&self, cf) -> Snapshot`
- *(snapshots used implicitly in `get_transactional` and `get_at`)*

### Durability & Maintenance (3 methods)
- `sync(&self) -> MidgeResult<()>` *(WAL fsync)*
- `flush(&self) -> MidgeResult<()>`
- `flush_cf(&self, cf) -> MidgeResult<()>`
- `compact_all(&self) -> MidgeResult<()>`

### Column Families (4 methods)
- `create_column_family(&self, name) -> MidgeResult<ColumnFamilyHandle>`
- `drop_column_family(&self, cf_id) -> MidgeResult<()>`
- `list_column_families(&self) -> MidgeResult<Vec<ColumnFamilyHandle>>`
- `default_column_family(&self) -> &ColumnFamilyHandle`

### Bulk Ingest Mode (4 methods)
- `get_runtime_config(&self) -> MidgeResult<IngestModeSnapshot>`
- `is_ingesting(&self) -> MidgeResult<bool>`
- `enter_ingest_mode(&self) -> MidgeResult<IngestModeSnapshot>`
- `exit_ingest_mode(&self, prev) -> MidgeResult<()>`

### Observability (1 method)
- `get_read_amp_metrics(&self) -> MidgeResult<ReadAmpMetricsSnapshot>`

### Total: ~54 public methods on MidgeEngine

---

## Prelude Module

The `prelude` module re-exports a subset of commonly used types:

```rust
pub mod prelude {
    pub use crate::{
        AckPolicy,
        ApiError,
        ApiResult,
        ColumnFamily,
        ColumnFamilyHandle,
        Direction,
        Iterator,
        Key,
        KvPair,
        MergeOperator,
        MidgeEngine,
        MidgeError,
        MidgeResult,
        OpenOptions,
        Query,
        Snapshot,
        Transaction,
        Value,
        WriteBatch,
        WriteOptions,
    };
}
```

---

## API Surface Issues & Opportunities

### 1. **Duplicate Methods (High Priority)**
These pairs do the same thing:
- `put()` vs `put_cf()` 
- `get()` vs `get_cf()`
- `delete()` vs `delete_cf()`
- `merge()` vs `merge_cf()`
- `range()` vs `range_cf()`

**Recommendation:** Remove `_cf` suffixes since all methods already take a CF handle.

### 2. **Transaction API Duplication**
Four ways to create a transaction:
- `begin_transaction(isolation)` → `Box<dyn KvTransaction>`
- `begin_transaction_with_isolation(isolation)` → `Box<dyn KvTransaction>`
- `transaction()` → `Transaction`
- `transaction_with_isolation(isolation)` → `Transaction`

Two ways to commit:
- `commit_transaction_boxed(Box<dyn KvTransaction>)`
- `commit_transaction(Transaction)`

**Recommendation:** Unify around single concrete `Transaction` type.

### 3. **Insert Variants Proliferation**
Four insert methods with different return types:
- `insert()` → `bool` 
- `insert_with_ttl()` → `bool`
- `insert_with_value()` → `InsertResult`
- `insert_with_value_and_ttl()` → `InsertResult`

**Recommendation:** Use builder pattern or options struct to reduce combinatorial explosion.

### 4. **Exposed Internal Modules**
These are currently public but expose implementation details:
- `pub mod io` - filesystem abstraction
- `pub mod telemetry` - internal tracing
- `pub mod storage` - storage layer internals
- `pub mod iterators` - iterator implementations
- `pub mod wal` - WAL format details
- `pub mod sst` - SST format details
- `pub mod metadata` - manifest internals
- `pub mod runtime` - background actor internals
- `pub mod compaction` - compaction algorithms

**Recommendation:** Make these `pub(crate)` or feature-gate for advanced users.

### 5. **Snapshot Methods**
- `snapshot()` - creates snapshot
- `snapshot_cf()` - creates snapshot for CF
- `get_transactional()` - takes snapshot sequence
- `get_at()` - takes sequence number

**Recommendation:** Clarify snapshot vs sequence-based reads.

### 6. **Ingest Mode API**
Four specialized methods for bulk loading:
- `get_runtime_config()`
- `is_ingesting()`
- `enter_ingest_mode()`
- `exit_ingest_mode()`

**Recommendation:** Consider builder or RAII guard pattern.

### 7. **OpenOptions vs MidgeOptions**
Two ways to configure engine:
- `OpenOptions` (smart config with `Goal`, `WorkloadProfile`)
- `MidgeOptions` (testkit explicit config)

**Recommendation:** Unify or clarify which is production vs testing.

---

## What End Users Actually Use (from examples)

### Basic Usage Example Uses:
```rust
// Creation
MidgeEngine::open(PathBuf)
default_column_family()

// Core ops
put(cf, key, val)
get(cf, key)
delete(cf, key)

// Batch
WriteBatch::new()
batch.put()
batch.delete()
write_batch()

// Range
Query::new().start_key().end_key().limit()
scan(cf, query)

// Transactions
transaction()
txn.put()
commit_transaction()

// Snapshots
snapshot()
snapshot.sequence()

// CAS
compare_and_swap()

// Maintenance
sync()
flush()
shutdown()
```

### Smart Config Example Uses:
```rust
OpenOptions::new()
    .path()
    .goal(Goal::Latency)
    .workload(WorkloadProfile::WriteHeavy)
    .durability(Durability::Strict)
    .memory_budget(MemoryBudget::Bytes())
    .build()
```

### Metrics Example Uses:
```rust
EngineMetrics::new()
metrics.record_read()
metrics.record_write()
metrics.total_ops()
metrics.read_latency_ns.avg_nanos()
```

---

## Recommended Simplifications

### Phase 1: Remove Duplication (Breaking)
1. Remove `_cf` suffix methods (already take CF handle)
2. Unify transaction API to single concrete type
3. Consolidate insert methods to builder pattern

### Phase 2: Hide Internals (Non-Breaking with Feature Flags)
1. Make internal modules `pub(crate)` by default
2. Add `unstable-internals` feature for advanced users
3. Keep only `engine::api` types in stable public API

### Phase 3: Simplify Configuration
1. Make `OpenOptions` primary, relegate `MidgeOptions` to testkit
2. Consider RAII guards for ingest mode
3. Clarify snapshot vs sequence-based reads

### Phase 4: Optimize Common Cases
1. Add convenience methods for default CF operations
2. Reduce builder boilerplate for common configs
3. Better defaults in `OpenOptions`

---

## Stable API Target (Minimal Surface)

### Core Types
- `MidgeEngine` - main engine
- `MidgeError`, `MidgeResult` - errors
- `OpenOptions` - configuration

### Data Types
- `Key`, `Value`, `KvPair` - data
- `WriteBatch` - batched writes
- `Query` - range queries
- `Transaction` - ACID transactions
- `Snapshot` - point-in-time reads

### Operations (15 core methods)
```rust
impl MidgeEngine {
    // Lifecycle
    fn open(OpenOptions) -> Result<Self>
    fn shutdown(self) -> Result<()>
    
    // Core KV
    fn put(&self, cf, key, value) -> Result<()>
    fn get(&self, cf, key) -> Result<Option<Value>>
    fn delete(&self, cf, key) -> Result<()>
    
    // Batches
    fn write_batch(&self, WriteBatch) -> Result<()>
    
    // Scans
    fn scan(&self, cf, Query) -> Result<Vec<KvPair>>
    
    // Transactions
    fn transaction(&self) -> Transaction
    fn commit(&self, Transaction) -> Result<()>
    
    // Snapshots
    fn snapshot(&self) -> Snapshot
    
    // Maintenance
    fn sync(&self) -> Result<()>
    fn flush(&self) -> Result<()>
    
    // Column Families
    fn create_cf(&self, name) -> Result<ColumnFamilyHandle>
    fn default_cf(&self) -> &ColumnFamilyHandle
}
```

**Total:** ~15 essential methods (down from 54)

---

## Next Steps

1. **Audit Usage:** Grep benchmarks and tests to confirm which APIs are actually used
2. **Deprecation Plan:** Mark duplicates as deprecated in next release
3. **Migration Guide:** Document how to move from old API to simplified API
4. **Feature Gating:** Add `unstable` feature for power users who need internals
5. **Documentation:** Add examples showing migration path

---

## Questions for Discussion

1. Should `_cf` methods be removed if all methods already take CF handle?
2. Is the transaction trait (`KvTransaction`) needed or can we use concrete type?
3. Should internal modules like `wal`, `sst`, `compaction` be public?
4. Do we need both `OpenOptions` (smart) and `MidgeOptions` (explicit)?
5. Is the ingest mode API too specialized for core surface?
6. Should we have a separate `MidgeEngineBuilder` instead of `open_with_options`?
