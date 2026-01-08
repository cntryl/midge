# Midge Public API V2 - Explicit Transaction-Based Design

**Status:** Design Document  
**Date:** January 8, 2026  
**Goal:** Define minimal, explicit, AI-proof public API

---

## Design Principles (Non-Negotiable)

1. **One operation, one meaning** - No overloads, aliases, or helper variants
2. **No implicit behavior** - All correctness choices explicit at call site
3. **Transactions mandatory** - All reads and writes through explicit transactions
4. **Column-family isolation** - Transactions bound to exactly one CF
5. **Single Transaction type** - No trait objects or type splits

---

## Core Public API

### Types

```rust
// Main engine
pub struct MidgeEngine { /* ... */ }

// Transaction (single concrete type)
pub struct Transaction { /* ... */ }

// Transaction mode
pub enum TransactionMode {
    ReadOnly,
    ReadWrite,
}

// Write options (NO Default impl)
pub struct WriteOptions { /* ... */ }

pub enum DurabilityPolicy {
    Sync,      // fsync immediately
    Buffered,  // OS buffer only
    NoWAL,     // skip WAL (dangerous)
}

// Column family
pub struct ColumnFamilyId(pub u32);
pub struct ColumnFamilyHandle {
    id: ColumnFamilyId,
    name: String,
}

// Data types
pub type Key = Vec<u8>;
pub type Value = Vec<u8>;
pub type KvPair = (Bytes, Bytes);

// Batch operations
pub enum BatchOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Insert { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    DeleteRange { start: Vec<u8>, end: Vec<u8> },
}

// Configuration
pub struct OpenOptions { /* ... */ }

// Errors
pub enum MidgeError { /* ... */ }
pub type MidgeResult<T> = Result<T, MidgeError>;
```

### Operations

```rust
impl MidgeEngine {
    // ========================================
    // Lifecycle
    // ========================================
    
    /// Open database with explicit configuration
    pub fn open(opts: OpenOptions) -> MidgeResult<Self>;
    
    /// Shutdown database gracefully
    pub fn shutdown(self) -> MidgeResult<()>;
    
    // ========================================
    // Column Families
    // ========================================
    
    /// Get default column family handle
    pub fn default_cf(&self) -> &ColumnFamilyHandle;
    
    /// Create new column family
    pub fn create_cf(&self, name: &str) -> MidgeResult<ColumnFamilyHandle>;
    
    /// List all column families
    pub fn list_cf(&self) -> MidgeResult<Vec<ColumnFamilyHandle>>;
    
    // ========================================
    // Transactions (REQUIRED for all data ops)
    // ========================================
    
    /// Begin transaction - REQUIRED for all reads and writes
    ///
    /// # Arguments
    /// * `cf_id` - Column family to bind transaction to
    /// * `mode` - Transaction mode (ReadOnly or ReadWrite)
    ///
    /// # Example
    /// ```ignore
    /// let tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    /// tx.put(b"key".to_vec(), b"value".to_vec())?;
    /// engine.commit(tx, WriteOptions::sync())?;
    /// ```
    pub fn begin_tx(
        &self,
        cf_id: ColumnFamilyId,
        mode: TransactionMode
    ) -> MidgeResult<Transaction>;
    
    /// Get value within transaction
    ///
    /// Provides repeatable reads within transaction's snapshot.
    pub fn get(&self, tx: &mut Transaction, key: &[u8]) -> MidgeResult<Option<Bytes>>;
    
    /// Scan range within transaction
    ///
    /// Returns pairs in range [start, end) at transaction's snapshot.
    pub fn scan(
        &self,
        tx: &Transaction,
        start: &[u8],
        end: &[u8]
    ) -> MidgeResult<Vec<KvPair>>;
    
    /// Commit transaction - WriteOptions REQUIRED
    ///
    /// For ReadOnly transactions: validates and marks complete.
    /// For ReadWrite transactions: atomically applies all writes.
    ///
    /// # Arguments
    /// * `tx` - Transaction to commit
    /// * `opts` - Write options (MUST be explicit)
    ///
    /// # Example
    /// ```ignore
    /// engine.commit(tx, WriteOptions::sync())?;
    /// ```
    pub fn commit(&self, tx: Transaction, opts: WriteOptions) -> MidgeResult<()>;
    
    /// Rollback transaction explicitly
    ///
    /// Dropping uncommitted transaction also rolls back.
    pub fn rollback(&self, tx: Transaction) -> MidgeResult<()>;
    
    // ========================================
    // Maintenance
    // ========================================
    
    /// Force WAL fsync
    pub fn sync(&self) -> MidgeResult<()>;
    
    /// Force memtable flush
    pub fn flush(&self) -> MidgeResult<()>;
}

impl Transaction {
    /// Put key-value (upsert)
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> MidgeResult<()>;
    
    /// Insert key-value (error if exists)
    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> MidgeResult<()>;
    
    /// Delete key (idempotent)
    pub fn delete(&mut self, key: Vec<u8>) -> MidgeResult<()>;
    
    /// Delete range [start, end) (idempotent)
    pub fn delete_range(&mut self, start: Vec<u8>, end: Vec<u8>) -> MidgeResult<()>;
    
    /// Execute batch of operations atomically
    ///
    /// Order is preserved. All succeed or all fail.
    pub fn batch(&mut self, ops: Vec<BatchOp>) -> MidgeResult<()>;
    
    /// Get transaction ID
    pub fn id(&self) -> u64;
    
    /// Get bound column family
    pub fn cf_id(&self) -> ColumnFamilyId;
    
    /// Get transaction mode
    pub fn mode(&self) -> TransactionMode;
    
    /// Check if active
    pub fn is_active(&self) -> bool;
}

impl WriteOptions {
    /// Create WriteOptions with Sync policy (full durability)
    pub fn sync() -> Self;
    
    /// Create WriteOptions with Buffered policy (fast, not durable)
    pub fn buffered() -> Self;
    
    /// Create WriteOptions with NoWAL policy (fastest, completely non-durable)
    pub fn no_wal() -> Self;
    
    /// Get durability policy
    pub fn policy(&self) -> DurabilityPolicy;
}

impl OpenOptions {
    /// Create new builder
    pub fn new() -> Self;
    
    /// Set database path (REQUIRED)
    pub fn path<P: Into<PathBuf>>(self, path: P) -> Self;
    
    /// Build with defaults
    pub fn build(self) -> Self;
    
    // ... other configuration methods
}
```

---

## Usage Examples

### Basic Operations

```rust
use cntryl_midge::{MidgeEngine, OpenOptions, TransactionMode, WriteOptions};

// Open database
let engine = MidgeEngine::open(
    OpenOptions::new()
        .path("./my_db")
        .build()
)?;

// Get default column family
let cf = engine.default_cf();

// ========================================
// Write Transaction
// ========================================

// Begin write transaction
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;

// Perform writes
tx.put(b"key1".to_vec(), b"value1".to_vec())?;
tx.put(b"key2".to_vec(), b"value2".to_vec())?;
tx.delete(b"old_key".to_vec())?;

// Commit with explicit durability
engine.commit(tx, WriteOptions::sync())?;

// ========================================
// Read Transaction
// ========================================

// Begin read-only transaction
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;

// Reads see consistent snapshot
let val1 = engine.get(&mut tx, b"key1")?;
let val2 = engine.get(&mut tx, b"key2")?;

// ReadOnly transactions don't need WriteOptions
engine.commit(tx, WriteOptions::sync())?; // Or just drop tx

// ========================================
// Range Scan
// ========================================

let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
let pairs = engine.scan(&tx, b"a", b"z")?;
engine.commit(tx, WriteOptions::sync())?;

// ========================================
// Batch Operations
// ========================================

use cntryl_midge::BatchOp;

let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;

tx.batch(vec![
    BatchOp::Put {
        key: b"batch1".to_vec(),
        value: b"value1".to_vec(),
    },
    BatchOp::Put {
        key: b"batch2".to_vec(),
        value: b"value2".to_vec(),
    },
    BatchOp::Delete {
        key: b"old".to_vec(),
    },
])?;

engine.commit(tx, WriteOptions::buffered())?;

// ========================================
// Rollback
// ========================================

let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.put(b"temp".to_vec(), b"data".to_vec())?;

// Explicit rollback
engine.rollback(tx)?;

// Or implicit via drop:
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.put(b"temp2".to_vec(), b"data2".to_vec())?;
drop(tx); // Implicitly rolls back

// ========================================
// Error: Write in ReadOnly Mode
// ========================================

let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
// This WILL ERROR:
tx.put(b"key".to_vec(), b"value".to_vec())?; // Error!

// ========================================
// Shutdown
// ========================================

engine.shutdown()?;
```

### Column Families

```rust
// Create column family
let users_cf = engine.create_cf("users")?;
let posts_cf = engine.create_cf("posts")?;

// Transactions are CF-bound
let tx1 = engine.begin_tx(users_cf.id(), TransactionMode::ReadWrite)?;
let tx2 = engine.begin_tx(posts_cf.id(), TransactionMode::ReadWrite)?;

// Each transaction operates on its own CF
tx1.put(b"user:1".to_vec(), b"alice".to_vec())?;
tx2.put(b"post:1".to_vec(), b"hello".to_vec())?;

// Commit separately
engine.commit(tx1, WriteOptions::sync())?;
engine.commit(tx2, WriteOptions::sync())?;
```

### Durability Choices

```rust
// Full durability - fsync on every commit
engine.commit(tx, WriteOptions::sync())?;

// Fast mode - buffer in OS, no fsync
engine.commit(tx, WriteOptions::buffered())?;

// Bulk load mode - skip WAL entirely (dangerous!)
engine.commit(tx, WriteOptions::no_wal())?;
engine.flush()?; // Must flush to persist
```

---

## Forbidden Operations (Do NOT Exist)

The following operations are explicitly FORBIDDEN and do not exist in the API:

```rust
// ❌ NO implicit writes
engine.put(cf, key, value)  // DOES NOT EXIST

// ❌ NO implicit reads
engine.get(cf, key)  // DOES NOT EXIST

// ❌ NO autocommit
engine.put_autocommit(cf, key, value)  // DOES NOT EXIST

// ❌ NO convenience methods
engine.put_cf(cf, key, value)  // DOES NOT EXIST
engine.get_cf(cf, key)  // DOES NOT EXIST

// ❌ NO merge operators
engine.merge(cf, key, operand)  // DOES NOT EXIST
engine.register_merge_operator(cf, op)  // DOES NOT EXIST

// ❌ NO CAS operations
engine.compare_and_swap(cf, key, old, new)  // DOES NOT EXIST

// ❌ NO TTL variants
engine.put_with_ttl(cf, key, value, ttl)  // DOES NOT EXIST

// ❌ NO implicit snapshots
engine.snapshot()  // DOES NOT EXIST
engine.get_at(cf, key, sequence)  // DOES NOT EXIST

// ❌ NO transaction trait objects
let tx: Box<dyn KvTransaction> = ...  // DOES NOT EXIST

// ❌ NO default WriteOptions
engine.commit(tx)  // DOES NOT EXIST - must supply WriteOptions

// ❌ NO write batch without transaction
let batch = WriteBatch::new();
batch.put(...);
engine.write_batch(batch)  // DOES NOT EXIST
```

---

## Module Visibility

### Public API (Stable)

```rust
// lib.rs
pub use engine::MidgeEngine;
pub use engine::api::{
    Transaction,
    TransactionMode,
    WriteOptions,
    DurabilityPolicy,
    BatchOp,
    ColumnFamilyId,
    ColumnFamilyHandle,
    OpenOptions,
};
pub use common::{MidgeError, MidgeResult};

// Minimal prelude
pub mod prelude {
    pub use crate::{
        MidgeEngine,
        Transaction,
        TransactionMode,
        WriteOptions,
        DurabilityPolicy,
        BatchOp,
        ColumnFamilyId,
        ColumnFamilyHandle,
        OpenOptions,
        MidgeError,
        MidgeResult,
    };
}
```

### Internal Modules (Hidden)

```rust
// All internal implementation modules are pub(crate)
pub(crate) mod wal;
pub(crate) mod sst;
pub(crate) mod compaction;
pub(crate) mod runtime;
pub(crate) mod storage;
pub(crate) mod io;
pub(crate) mod iterators;
pub(crate) mod metadata;
pub(crate) mod telemetry;
```

### Optional Advanced API (Feature-Gated)

```rust
// Cargo.toml
[features]
unstable-internals = []

// lib.rs
#[cfg(feature = "unstable-internals")]
pub mod unstable {
    pub use crate::wal;
    pub use crate::sst;
    pub use crate::compaction;
    // ... other internals
}
```

---

## API Invariants (Must Hold)

1. ✅ Every read executes inside explicit Transaction
2. ✅ Every write executes inside explicit Transaction  
3. ✅ Every Transaction is bound to exactly one ColumnFamilyId
4. ✅ Every commit supplies explicit WriteOptions (no defaults)
5. ✅ TransactionMode determines capabilities, not type
6. ✅ ReadOnly transactions CANNOT write (enforced at runtime)
7. ✅ Transactions provide repeatable reads via snapshot isolation
8. ✅ Transactions cannot be reused after commit or rollback
9. ✅ Internal modules are pub(crate) by default
10. ✅ No implicit behavior anywhere in the API

---

## AI-Proof Characteristics

This design minimizes AI ambiguity by:

1. **Single path to success** - Only one way to do each operation
2. **Explicit everything** - No guessing about defaults or implicit behavior
3. **Type-driven correctness** - Compiler enforces transaction discipline
4. **Clear naming** - `begin_tx`, not `transaction` or `begin` or `start_tx`
5. **Mandatory parameters** - Cannot forget WriteOptions, mode, or CF
6. **No overloading** - Each method name has exactly one meaning
7. **Fail fast** - Runtime errors for misuse (write in ReadOnly mode)
8. **Documentation by design** - API shape teaches correct usage

---

## Migration from V1 API

### Old API (V1) → New API (V2)

```rust
// V1: Implicit writes
engine.put(cf, b"key", b"value")?;

// V2: Explicit transaction
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.put(b"key".to_vec(), b"value".to_vec())?;
engine.commit(tx, WriteOptions::sync())?;

// ────────────────────────────────────────

// V1: Implicit reads
let val = engine.get(cf, b"key")?;

// V2: Explicit transaction
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
let val = engine.get(&mut tx, b"key")?;
drop(tx); // Or commit

// ────────────────────────────────────────

// V1: WriteBatch without transaction
let mut batch = WriteBatch::new();
batch.put(key1, val1);
batch.put(key2, val2);
engine.write_batch(&batch)?;

// V2: Batch within transaction
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.batch(vec![
    BatchOp::Put { key: key1, value: val1 },
    BatchOp::Put { key: key2, value: val2 },
])?;
engine.commit(tx, WriteOptions::sync())?;

// ────────────────────────────────────────

// V1: Implicit durability
engine.put(cf, key, value)?;
engine.flush()?;

// V2: Explicit durability
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.put(key, value)?;
engine.commit(tx, WriteOptions::sync())?; // Explicit!
```

---

## Implementation Status

**Current Status:** Design Phase

**Files Created:**
- `src/engine/api/transaction_v2.rs` - New transaction implementation
- `src/engine/api/write_options_v2.rs` - Explicit write options
- `src/engine/api/engine_api_v2.rs` - New engine API facade
- `docs/API_DESIGN_V2.md` - This document

**Next Steps:**
1. Integrate V2 types into runtime message passing
2. Update engine facade to use V2 API
3. Make all V1 methods `#[deprecated]` with migration notes
4. Update all examples to V2 API
5. Update all tests to V2 API
6. Hide internal modules with feature gates
7. Release as breaking 2.0 version

---

## Summary

This design achieves:

✅ **Minimal surface** - ~10 core methods on MidgeEngine  
✅ **Explicit everything** - No defaults, no implicit behavior  
✅ **Transaction-mandatory** - All data ops through transactions  
✅ **CF-isolated** - Transactions bound to one CF  
✅ **Type-safe** - Compiler-enforced correctness  
✅ **AI-proof** - Unambiguous, single-path design  
✅ **Hidden internals** - Implementation details encapsulated  
✅ **Feature-gated power** - Advanced users can opt-in to internals  

This is the production-ready, long-term stable API for Midge.
