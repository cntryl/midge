# Transaction API Migration Status

## ✅ Completed Files

1. **tests/engine_basic.rs** - Already fixed by user
2. **tests/durability_atomicity.rs** - ✅ Fixed and compiling
3. **tests/durability_recovery.rs** - ⚠️ Partially fixed (imports added, first test fixed)

## 🔄 In Progress / Remaining Files

### High Priority (Heavy API Usage)

1. **tests/durability_wal.rs** - 25 occurrences of put/get/delete
   - Needs: All put/get calls converted to transactions
   
2. **tests/engine_write_batch.rs** - 50+ occurrences
   - **Complex**: Uses WriteBatch which needs special handling
   - Pattern: Replace `engine.write_batch(&batch)` with multi-operation transactions
   
3. **tests/engine_snapshots.rs** - 50+ occurrences  
   - Needs: All put/get/delete calls + snapshot() API changes
   - Pattern: Replace `engine.snapshot()` with `begin_tx(cf_id, ReadOnly)` for consistent reads
   
4. **tests/engine_merge.rs** - Multiple merge operations
   - Needs: merge_cf() calls + put/get conversions
   
5. **tests/edge_cases.rs** - Various edge case tests
   - Needs: Standard put/get/delete conversions
   
6. **tests/engine_init.rs** - ✅ Simple initialization tests (no API calls to fix)

### Additional Files with Compilation Errors

Based on `cargo test --no-run` output:
- tests/transaction_isolation_audit.rs
- tests/transaction_isolation_lww.rs  
- tests/engine_cloud.rs
- tests/memory_spill_audit.rs
- tests/ingest_invariants.rs
- tests/transaction_basic.rs
- tests/transaction_advanced.rs
- tests/transaction_spill.rs

## Transformation Patterns

### 1. Simple Put
```rust
// OLD:
engine.put(cf, b"key", b"value").expect("put");

// NEW:
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite).expect("begin_tx");
tx.put(cf.id(), b"key".to_vec(), b"value".to_vec(), None).expect("put");
engine.commit(tx, WriteOptions::buffered()).expect("commit");
```

### 2. Simple Get
```rust
// OLD:
let value = engine.get(cf, b"key").expect("get");

// NEW:
let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).expect("begin_tx");
let value = engine.tx_get(&tx, b"key").expect("get");
```

### 3. Simple Delete
```rust
// OLD:
engine.delete(cf, b"key").expect("delete");

// NEW:
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite).expect("begin_tx");
tx.delete(cf.id(), b"key".to_vec()).expect("delete");
engine.commit(tx, WriteOptions::buffered()).expect("commit");
```

### 4. WriteBatch (Multi-Operation)
```rust
// OLD:
let mut batch = WriteBatch::new();
batch.put(Bytes::copy_from_slice(b"key1"), Bytes::copy_from_slice(b"val1"));
batch.put(Bytes::copy_from_slice(b"key2"), Bytes::copy_from_slice(b"val2"));
batch.delete(Bytes::copy_from_slice(b"key3"));
engine.write_batch(&batch).expect("write_batch");

// NEW:
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite).expect("begin_tx");
tx.put(cf.id(), b"key1".to_vec(), b"val1".to_vec(), None).expect("put");
tx.put(cf.id(), b"key2".to_vec(), b"val2".to_vec(), None).expect("put");
tx.delete(cf.id(), b"key3".to_vec()).expect("delete");
engine.commit(tx, WriteOptions::buffered()).expect("commit");
```

### 5. Snapshot (for Consistent Reads)
```rust
// OLD:
let snapshot = engine.snapshot();
let value = snapshot.get(cf, b"key").unwrap();

// NEW (if snapshot isolation needed):
let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).expect("begin_tx");
let value = engine.tx_get(&tx, b"key").expect("get");
```

## Required Import
All test files need:
```rust
use cntryl_midge::{TransactionMode, WriteOptions};
```

## Next Steps

1. Complete durability_recovery.rs (50+ more replacements needed)
2. Fix durability_wal.rs (25 replacements)
3. Fix engine_write_batch.rs (complex - requires WriteBatch → Transaction conversion)
4. Fix engine_snapshots.rs (50+ replacements + snapshot API changes)
5. Fix engine_merge.rs
6. Fix edge_cases.rs
7. Fix remaining files with compilation errors

## Automation Recommendation

Given the scale (~200+ individual replacements across 15+ files), consider:
1. Using the Python script in `scripts/fix_transaction_api.py` (partially complete)
2. Manual verification of each converted test
3. Running `cargo test` after each file to catch errors early

## Compilation Status

Current: 2/15 priority files fully fixed and compiling
- ✅ durability_atomicity.rs
- ⚠️ durability_recovery.rs (partially)
- ❌ 13+ files remain
