# Porting Strategy: src_old/ → src/

**Status**: We've implemented core API surface (Query, CasResult, InsertResult) and basic CRUD works. However, transaction API has a **critical mismatch** between high-level test expectations and current implementation.

**Token Budget Situation**: ~190K tokens used (40% of budget). Work is getting complex. Need focused priority.

---

## 1. CRITICAL FINDING: Transaction API Mismatch

### Current State (Low-Level)
```rust
// What we have now:
let txn = engine.transaction();  // Returns api::Transaction
txn.put(cf_id, key, value);
engine.commit_transaction(txn)
```

### Test Expectations (High-Level)
```rust
// What tests expect:
let mut txn = engine.begin_transaction(&cf)?;  // Takes column family
txn.put(b"key", b"value")?;
txn.get(b"key")?;
engine.commit_transaction(txn, WriteOptions)?;  // Takes WriteOptions
```

**Missing Types:**
- `KvTransaction` trait (user-facing, column-family scoped)
- `WriteOptions` struct
- `engine.begin_transaction(&cf)` method
- Transaction needs to track which CF it operates on

**Affected Test Files:** transaction_conflicts.rs, transaction_isolation.rs, transaction_deadlock.rs, transaction_advanced.rs, transaction_spill.rs (5 files, ~1500+ lines)

---

## 2. API Gap Summary

### ✅ Implemented (Working)
- **Query Builder**: Full fluent API for scans (start_key, end_key, prefix, limit, reverse)
- **Advanced Ops**: insert(), insert_with_value(), compare_and_swap() signatures
- **Basic CRUD**: put, get, delete work (14/25 tests pass in engine_basic.rs)
- **Snapshots**: Basic snapshot API exists
- **Column Families**: create/drop/list work
- **WriteBatch**: Exists and compiles
- **Low-Level Transaction**: api::Transaction struct with read/write sets

### ⚠️ Partially Implemented
- **Scan**: Returns empty (stub)
- **Delete Range**: Scan+delete (inefficient stub)
- **Transactions**: Low-level API exists, but high-level API missing
- **Runtime Sync**: Just fixed (WalActor now updates memtable), needs testing

### ❌ Missing (Tests Will Fail)
- **KvTransaction trait**: User-facing transaction interface
- **WriteOptions**: Commit options struct
- **begin_transaction(&cf)**: Column-family scoped transaction creation
- **TTL Support**: 30+ tests skip this
- **Conflict Detection**: INSERT/CAS should detect conflicts at commit time
- **Per-block Bloom Filters**: Tests exist but feature not in src/
- **Merge Operators**: Tests exist but not fully implemented
- **Iterator API**: Tests expect advanced iteration
- **Fault Injection**: Tests skip this
- **Paranoid Mode**: Tests skip this
- **Rate Limiting**: Tests skip this

---

## 3. Port Priority (Recommended Order)

### **PHASE 1: Core Foundation (Next 2 Steps)**

#### 1a. Verify WalActor Fix ⚠️ BLOCKING
- **Time**: 5 minutes
- **Action**: Re-run `cargo test engine_basic --lib` 
- **Goal**: Should see 10 previously-failing tests now pass (14→24 passing)
- **Why**: If this doesn't work, we have a major architecture issue
- **Impact**: Unblocks all read/write operations

#### 1b. Implement KvTransaction Trait + begin_transaction()
- **Time**: 30 minutes
- **Files**: 
  - Create `src/engine/api/transaction_api.rs` 
  - Implement `KvTransaction` trait
  - Implement `engine.begin_transaction(&cf)` → `Box<dyn KvTransaction>`
  - Create `WriteOptions` struct
  - Update `engine.commit_transaction()` signature
- **Tests Fixed**: All 5 transaction_*.rs files (estimated 40-50 tests)
- **Why**: Required for ANY transaction test to compile
- **Complexity**: Medium (trait design, but logic mostly exists)

---

### **PHASE 2: Test Compatibility (Steps 3-5)**

#### 2a. Port Remaining engine_basic.rs Tests
- **Time**: 20 minutes
- **Goal**: Get all 25 tests passing
- **Action**: 
  - Implement scan() properly (not just stub)
  - Fix delete_range() 
  - Add missing test helpers
- **Impact**: Validates CRUD API is solid

#### 2b. Fix Compilation on transaction_*.rs Files
- **Time**: 40 minutes
- **Goal**: Get all 5 transaction files compiling
- **Action**:
  - Implement conflict detection for INSERT/CAS
  - Implement proper commit sequence tracking
  - Handle concurrency in transaction execution
- **Tests**: 50+ transaction tests
- **Why**: High-level transaction tests are critical

#### 2c. Fix Other High-Confidence Test Files
- **Time**: 1 hour
- **Files to Target**:
  - engine_iterators.rs
  - engine_snapshots.rs
  - engine_write_batch.rs
  - concurrency_writes.rs
- **Goal**: Get 10-15 more test files compiling
- **Why**: These are closer to compiling than advanced features

---

### **PHASE 3: Extended API (Steps 6-7)**

#### 3a. TTL Support (30+ test files)
- **Time**: 1.5 hours
- **Files Affected**: All TTL-related tests
- **Implementation**: Add TTL to memtable, scan filters
- **Status**: Currently tests are skipped as .rs.skip files

#### 3b. Advanced Features (Conditional)
- Merge operators, iterators, fault injection
- Only if time allows and needed for test pass rate

---

## 4. Current Test Status (By Likelihood of Success)

| Category | Files | Status | Priority | Est. Time |
|----------|-------|--------|----------|-----------|
| **CRUD** | 1 | 14/25 passing | 🔴 BLOCKING | 30min |
| **Transactions** | 5 | 0 compiling | 🔴 BLOCKING | 1 hour |
| **Snapshots** | 1 | Unknown | 🟡 MEDIUM | 30min |
| **Iterators** | 1 | Unknown | 🟡 MEDIUM | 30min |
| **WriteBatch** | 1 | Unknown | 🟡 MEDIUM | 20min |
| **Concurrency** | 10 | Unknown | 🟡 MEDIUM | 2 hours |
| **TTL** | 30+ | Skipped | 🟢 LOW | 1.5 hours |
| **Advanced** | 10+ | Skipped | 🟢 LOW | N/A |

---

## 5. Architectural Issues to Watch

### Issue 1: Engine vs Runtime Memtable Split 
- **Status**: Partially mitigated by WalActor.append() fix
- **Remaining**: Need to verify sync is complete
- **Action**: Re-run tests after WalActor fix

### Issue 2: SST File I/O Disabled
- **Status**: Windows file locking issue
- **Impact**: No persistence past memtable
- **Mitigation**: Use LocalMemory for now, LocalDisk for tests
- **Action**: Re-enable if critical, otherwise skip

### Issue 3: Scan Implementation is Stub
- **Status**: Returns empty vec
- **Impact**: Range queries fail
- **Action**: Implement proper memtable scan

---

## 6. Recommended Next Command

```bash
cargo test engine_basic --lib -- --nocapture
```

**Expected Result**: Should see significant improvement from 14 passing (before WalActor fix).

**If still ~14 passing**: WalActor fix didn't work, needs investigation
**If 24+ passing**: Great! Proceed to Phase 1b (KvTransaction trait)

---

## 7. Game Plan Summary

1. **Verify**: Re-run tests to see if WalActor fix worked
2. **Build Transaction API**: Implement KvTransaction trait + begin_transaction()
3. **Fix Transactions**: Get transaction_*.rs files compiling
4. **Port CRUD**: Finish engine_basic.rs
5. **Target Others**: Snapshots, iterators, write_batch
6. **TTL**: Add if time allows
7. **Advanced**: Merge ops, fault injection (low priority)

**Estimated Total Time**: 4-5 hours for phases 1-2, 2+ hours for phase 3

**Success Metric**: 50+ tests passing (currently 14)

