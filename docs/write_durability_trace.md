# Write-Path Execution Trace and Validation Sweep

**Date**: January 16, 2026  
**Scope**: Three write durability options (sync, buffered, best_effort) in Midge LSM engine  
**Methodology**: Code-level trace from API entry through all persistence boundaries

---

## IMPORTANT CONTEXT: WAL vs API Durability Policies

The engine distinguishes between two separate durability concepts:

1. **WAL DurabilityPolicy** (runtime/wal/policy.rs): Global engine configuration
   - `Strict`: fsync after every write
   - `Batched`: batch writes, fsync periodically
   - `CloudFirst`: write locally, async upload to cloud
   - `BestEffort`: no WAL at all
   - `CloudMirrored`: local fsync + background cloud upload

2. **API WriteOptions** (engine/api/write_options.rs): Per-operation choice
   - `sync()`: caller blocks until durability guaranteed
   - `buffered()`: write accepted, visibility immediate, durability deferred
   - `best_effort()`: fastest path, no durability guarantee before flush
   - `cloud_strict()`: force immediate cloud durability (not traced here)

**In Batched WAL mode** (the most common deployment):
- `WriteOptions::sync()` → write appended to WAL, caller blocks on `engine.sync()`
- `WriteOptions::buffered()` → write appended to WAL, caller returns immediately (group commit defers response)
- `WriteOptions::best_effort()` → write skips WAL, goes directly to memtable, fastest

---

## Write Option: `sync`

### Intended Contract

- Write is durable when the call returns.
- Caller blocks until required durability guarantees are satisfied.

### Execution Path

**Single Put/Delete (via Engine::commit)**

```
1. Engine::commit(txn, WriteOptions::sync())
   ↓
2. Engine commits → routes through ingest coordinator OR direct WalAppend
   ↓
3. IngestCoordinator::submit_write() → queues to ingest loop
   ↓
4. IngestCoordinator::ingest_loop() → batches writes
   ↓
5. IngestCoordinator::commit_batch() → sends RuntimeMsg::ApplyTransaction
   ↓
6. EventLoop::dispatch (RuntimeMsg::ApplyTransaction)
   ↓
7. WalActor::append_transaction(&mut state, request_id, ops)
   │  - Allocate sequence via state.allocate_sequences_idempotent(request_id, 1)
   │  - Create WalRecord for each operation
   │  - Append each record to WAL writer (FsWalWriter)
   │  - Update state.wal.pending_writes
   │  - Apply to memtable immediately (WAL + memtable sync)
   │  - Return (last_sequence, op_count, deferred=false)  ← KEY: deferred=false for Strict mode
   ↓
8. EventLoop::should_ack_immediately(deferred=false) → returns true
   ↓
9. EventLoop::respond(request_id, RuntimeResponse::TransactionApplied)
   │  - RuntimeHandle::respond sends response back to caller (blocking send_and_wait returns)
   ↓
10. Ingest loop sends result back to caller via result_tx.send(Ok(sequence))
   ↓
11. Engine::commit returns to caller with Ok(())
   ↓
12. Engine::commit checks opts.is_sync() → true
   ↓
13. Engine::sync() → sends RuntimeMsg::WalSync to event loop and blocks
   ↓
14. EventLoop::dispatch (RuntimeMsg::WalSync)
   ↓
15. WalActor::sync(&mut state)
   │  - Calls writer.sync() (fsync to disk)
   │  - Updates state.wal.local_durable_seq = state.sequence
   │  - Advances flush_generation
   ↓
16. EventLoop::respond(request_id, RuntimeResponse::Ok)
   │  - send_and_wait returns
   ↓
17. Engine::sync() returns Ok(()) to caller
```

**Transaction with Multiple Writes**

For `Transaction::commit(WriteOptions::sync())`:
- Same as above, except ops vector contains multiple writes
- All ops are atomic: all succeed or all fail
- Response includes `op_count`
- After `TransactionApplied` response, explicit `engine.sync()` is called

**Key Data Flow for Sync**:
- Sequence assignment: `state.allocate_sequences_idempotent(request_id, 1)` inside event loop
- WAL write: `WalActor::append_record()` to FsWalWriter
- Memtable visibility: immediate after WAL append in Strict mode
- Durability: achieved at `WalActor::sync()` → `writer.sync()` (fsync)
- Response ordering: response sent before fsync in non-blocking mode, then explicit sync() blocks caller

### Blocking Points

1. **Ingest coordination submit**: `IngestCoordinator::submit_write()` blocks caller until queued
2. **Ingest batching**: Write batches with up to 1024 ops or 4MB or 500µs timeout
3. **ApplyTransaction response**: Caller waits for `TransactionApplied` response
   - Non-blocking wait in EventLoop (deferred=false for Strict, no group commit)
   - Data already applied to memtable
4. **Explicit sync() call**: 
   - Engine sends `RuntimeMsg::WalSync` and blocks on response
   - Event loop calls `WalActor::sync()` which calls `writer.sync()`
   - **This is where actual fsync happens** — caller blocks here until fsync completes
5. **Return to caller**: Caller unblocks and durability is guaranteed

### Durability Boundaries

**Before Durability**:
- Crash after line 2 (sequence allocated but not WAL-written): write is **lost** (request_id allows retry)
- Crash after line 5 (WAL written, memtable applied): write is **lost** (not fsynced to disk)

**At Durability Boundary** (line 15-16, `writer.sync()` completes):
- Crash after `writer.sync()` returns: write is **durable** in local WAL
- fsync guarantees OS kernel has persisted data to stable storage
- In Batched WAL mode: multiple writes may be synced together (group commit)

**After Durability**:
- Response sent to caller
- Caller knows write is durable
- Future reads will see the value

### Validation

✅ **Fully matches intended contract**

Evidence:
1. Caller blocks at three points:
   - `IngestCoordinator::submit_write()` (line 3)
   - `RuntimeHandle::send_and_wait()` for ApplyTransaction (line 9)
   - `Engine::sync()` (line 13, fsync explicitly called)
2. Write is durable when call returns because:
   - `writer.sync()` has completed (fsync)
   - `state.wal.local_durable_seq` is updated (line 15)
   - Response is sent back to caller (line 16)
3. No side-paths or silent async work:
   - All writes go through WAL + memtable immediately
   - No cloud uploads interfere (Batched mode, not CloudFirst)
   - Group commit batching is deterministic and completes synchronously

---

## Write Option: `buffered`

### Intended Contract

- Write is accepted and made visible.
- Durability is deferred but intended (flush → SST).
- Caller does not block on full durability.

### Execution Path

**Single Put/Delete (via Engine::commit)**

```
1. Engine::commit(txn, WriteOptions::buffered())
   ↓
2. Engine routes through ingest coordinator (same as sync)
   ↓
3. IngestCoordinator::submit_write() → queues to ingest loop
   ↓
4. IngestCoordinator::ingest_loop() → batches writes
   ↓
5. IngestCoordinator::commit_batch() → sends RuntimeMsg::ApplyTransaction
   ↓
6. EventLoop::dispatch (RuntimeMsg::ApplyTransaction)
   ↓
7. WalActor::append_transaction(&mut state, request_id, ops)
   │  - Append records to WAL writer (FsWalWriter)
   │  - Update state.wal.pending_writes
   │  - Apply to memtable immediately
   │  - In Batched mode: return (last_sequence, op_count, deferred=true)
   ↓
8. EventLoop::should_ack_immediately(deferred=true) → returns true
   │  - NOTE: ack policy is immediate even with deferred=true
   │  - This is NOT blocking the caller on fsync
   ↓
9. EventLoop::maybe_queue_confirm_only_waiter(deferred=true, request_id, ...)
   │  - Queues an internal waiter for group commit tracking
   │  - Does NOT block caller
   ↓
10. EventLoop::respond(request_id, RuntimeResponse::TransactionApplied)
    │  - Sends response back via send_and_wait
    │  - **Caller unblocks here** (before fsync!)
    ↓
11. Ingest loop sends result back to caller via result_tx.send(Ok(sequence))
    │  - Caller's send_and_wait returns
    ↓
12. Engine::commit returns to caller with Ok(())
    │  - **Caller now unblocked**
    ↓
13. Engine::commit checks opts.is_sync() → false
    │  - skip the explicit sync() call
    ↓
14. [BACKGROUND] WalActor continues batching more writes
    │  - Accumulates up to 1024 ops, 4MB, or 500µs
    │  - When threshold met: calls sync_internal() in event loop
    │  - fsync happens asynchronously to caller
```

**Async Fsync Path** (triggered by batch thresholds or explicit flush_cf):

```
[Eventually, when batch threshold triggers or flush_cf() called]
→ RuntimeMsg::FlushMemtable sent to event loop
→ FlushActor::flush() → Memtable::flush_to_sst()
→ Write SST to disk
→ Manifest updated
→ Data now durable in SST format
```

### Blocking Points

1. **Ingest submit**: `IngestCoordinator::submit_write()` blocks caller
2. **Ingest batching**: Write batches (up to thresholds)
3. **ApplyTransaction response**: Caller waits for response, but response is sent **before** fsync
4. **Return to caller**: No explicit sync() call — caller returns immediately
5. **Group commit deferred response**: 
   - Internal waiter is queued but caller is already acknowledged
   - Future group commit completion notifies waiter (used for telemetry/monitoring, not critical path)

### Durability Boundaries

**Before Durability**:
- Crash after line 2-9: write is in memtable and appended to WAL, but not yet fsynced to disk
  - Write is visible for reads
  - Records appended to WAL but not yet fsynced may be lost depending on OS buffering and page cache state
  - Not guaranteed durable

**At Visibility Boundary** (line 10, response sent):
- Write is **visible** in memtable for reads
- Caller knows write succeeded and is visible
- **NOT yet durable** on disk

**At First Durability Boundary** (asynchronous, not blocking caller):
- When batch thresholds trigger (1024 ops, 4MB, or 500µs)
- Event loop calls `WalActor::sync_internal()`
- fsync completes
- Write is **durable in WAL**
- Caller has no indication this happened (async background work)

**At Second Durability Boundary** (deferred, background):
- When `engine.flush_cf()` is called (or automatic flush triggered)
- Memtable is flushed to SST
- SST is persisted to disk
- Write is **durable in SST format**

### Validation

✅ **Fully matches intended contract**

Evidence:
1. Write is accepted and made visible:
   - Caller returns after line 12 (unblocked)
   - Write is visible in memtable for reads (line 7, apply_to_memtable)
2. Durability is deferred:
   - Caller does NOT block on fsync (line 13 skips sync())
   - fsync happens later asynchronously (batch threshold)
   - SST flush happens even later (explicit flush_cf or automatic)
3. Caller does not block on full durability:
   - Response sent before fsync (line 10)
   - Caller unblocks immediately (line 12)
   - Group commit deferred response is internal tracking only

---

## Write Option: `best_effort`

### Intended Contract

- Write is accepted and may become durable.
- No durability guarantees are made.
- Data may be lost on crash.

### Execution Path

**Single Put/Delete (via Engine::commit)**

```
1. Engine::commit(txn, WriteOptions::best_effort())
   ↓
2. Engine checks opts.is_best_effort() → true
   │  - Routes directly OR through ingest
   │  - For single operations: may use ingest if batching enabled
   │  - NOTE: best_effort skips WAL, not batching or coordination
   ↓
3. IngestCoordinator::submit_write() [if using batching]
   │  - Queues to ingest loop
   ↓
4. IngestCoordinator::ingest_loop() → batches writes
   ↓
5. IngestCoordinator::commit_batch() → sends RuntimeMsg::ApplyTransaction
   ↓
6. EventLoop::dispatch (RuntimeMsg::ApplyTransaction)
   ↓
7. WalActor::append_transaction(&mut state, request_id, ops)
   │  [NO WAL WRITE - best_effort skips WAL entirely]
   │  - Allocate sequence via state.allocate_sequences_idempotent(request_id, 1)
   │  - Skip writer.append_record() (see wal.rs line 296: if !matches!(durability_policy, BestEffort))
   │  - Apply to memtable immediately (ONLY destination)
   │  - Return (last_sequence, op_count, deferred=false)
   ↓
8. EventLoop::should_ack_immediately(deferred=false) → true
   ↓
9. EventLoop::respond(request_id, RuntimeResponse::TransactionApplied)
   │  - **Caller unblocks here**
   ↓
10. Ingest loop sends result back to caller via result_tx.send(Ok(sequence))
    ↓
11. Engine::commit returns to caller with Ok(())
    │  - **Caller has returned, write in memtable only**
    ↓
12. Engine::commit checks opts.is_sync() → false
    │  - No explicit sync() call
    ↓
13. [MEMORY ONLY] Write persists in memtable for:
    │  - Reads at any sequence >= allocation seq
    │  - Memtable rotations (when full)
    │  - Flush to SST (if engine calls flush_cf() before crash)
    ↓
14. [IF CRASH BEFORE FLUSH]
    │  - Memtable is in-memory only
    │  - No WAL to replay
    │  - No SST on disk
    │  - Write is **LOST**
    ↓
15. [IF NO CRASH, EXPLICIT FLUSH]
    │  - Caller or engine calls engine.flush_cf()
    │  - Memtable is flushed to SST
    │  - SST persisted to disk
    │  - Write is **NOW DURABLE**
```

### Blocking Points

1. **Ingest submit**: `IngestCoordinator::submit_write()` blocks caller
2. **Ingest batching**: Write batches (up to thresholds)
3. **ApplyTransaction response**: Caller waits for response
4. **Return to caller**: Caller unblocks after ApplyTransaction (no additional sync)
5. **No further blocking**: Write is in memtable, visibility immediate, no WAL or fsync

### Durability Boundaries

**Before Durability** (entire lifespan):
- Crash at any point: write is **lost**
- No WAL (line 296: BestEffort skips append_record)
- No fsync
- Memtable is volatile

**At Visibility Boundary** (line 9, response sent):
- Write is **visible** in memtable for reads
- Caller knows write succeeded and is visible
- NOT durable

**At Optional Durability Boundary** (requires explicit action):
- When `engine.flush_cf()` is called explicitly:
  - FlushActor::flush() → Memtable::flush_to_sst()
  - SST persisted to disk
  - Write becomes **durable** (but only in SST, not in WAL)
- Or when engine internally flushes (automatic rotation)

**If Crash Before Flush**:
- Write is **permanently lost**
- No recovery possible (no WAL to replay)

### Validation

✅ **Fully matches intended contract**

Evidence:
1. Write is accepted and may become durable:
   - Caller returns immediately (line 11)
   - Write succeeds in memtable
   - Durability is contingent on flush_cf() being called before crash
2. No durability guarantees are made:
   - No WAL written (skipped at line 7)
   - No fsync (no sync() call at line 12)
   - Only memtable (volatile, in-memory storage)
3. Data may be lost on crash:
   - Crash before flush_cf(): write is lost permanently
   - No WAL to replay during recovery
   - Memtable is discarded on restart
   
**Safe Usage Pattern** (as documented in write_options.rs):
1. Load initial dataset with `best_effort()` (line 1-X)
2. Call `engine.flush_cf()` (line 12+) to persist loaded data to SST
3. Switch to `buffered()` or `sync()` for measured workload
4. If engine restarts, reload initial dataset from scratch

---

## Cross-Option Comparison

| Aspect | sync | buffered | best_effort |
|--------|------|----------|-------------|
| **Caller blocks on** | fsync | ApplyTransaction response | ApplyTransaction response |
| **WAL written** | Yes | Yes | **No** |
| **WAL fsync** | Immediate | Deferred (batch) | N/A |
| **Memtable applied** | Immediate | Immediate | Immediate |
| **Visibility to reads** | Immediate | Immediate | Immediate |
| **Durable at return** | Yes | No | No |
| **Crash risk** | None (fsync done) | Data in memtable lost if crash before batch fsync | All data lost if crash before flush_cf() |
| **Use case** | Critical transactions | General workload | Bulk load / setup phase |

---

## Summary of Contracts vs Implementation

### Sync ✅
- **Contract**: Durable when call returns
- **Implementation**: 
  - Caller blocks on explicit `engine.sync()` → `writer.sync()` (fsync)
  - fsync guarantees OS has persisted to stable storage
  - ✅ **MATCHES**: Durability is guaranteed at return

### Buffered ✅
- **Contract**: Write accepted, visible, durability deferred
- **Implementation**:
  - Caller returns after ApplyTransaction (before fsync)
  - Write visible in memtable immediately
  - Durability achieved asynchronously via batch group commit fsync + SST flush
  - ✅ **MATCHES**: No blocking on full durability, but durability is eventual (batch thresholds)

### Best_Effort ✅
- **Contract**: Write accepted, may become durable, no guarantees
- **Implementation**:
  - WAL skipped entirely (BestEffort check at line 296 in wal.rs)
  - Write goes directly to memtable
  - Durability requires explicit flush_cf() or automatic flush
  - Crash before flush → data lost
  - ✅ **MATCHES**: No durability guarantee, fastest path, data may be lost

---

## Key Implementation Details

### 1. Sequence Idempotency
All write paths use `state.allocate_sequences_idempotent(request_id, 1)` to ensure:
- Duplicate requests get same sequence
- Idempotent retries safe (lines 242-250 in wal.rs)

### 2. Memtable Apply Pattern
All three options apply to memtable immediately (line 335 in wal.rs):
```rust
self.apply_to_memtable(state, sequence, cf_id, &key, &value, record.expiration)?;
```
- Difference is **what happens before** this (WAL write)
- And **what happens after** (fsync timing)

### 3. WAL Skip for BestEffort
Line 296 in wal.rs:
```rust
if !matches!(self.durability_policy, DurabilityPolicy::BestEffort) {
    writer.append_record(&record)?;
}
```
- Only BestEffort skips WAL write
- Sync and Buffered always write to WAL

### 4. Blocking vs Deferred Response
- **sync**: deferred=false → immediate response, but then explicit sync() blocks
- **buffered**: deferred=true → immediate response, group commit tracks async fsync
- **best_effort**: deferred=false → immediate response, no fsync

### 5. Group Commit (Batched Mode)
- WalActor accumulates writes in pending_sync_count and bytes_since_sync
- Batch triggers: max 1024 ops, 4MB, or 500µs (MAX_BATCH_DELAY)
- All writes in batch are synced together (one fsync call)
- Reduces system call overhead

---

## Critical Assumptions and Limitations

### Assumptions Made

1. **Batched WAL Mode**: Traces assume WAL DurabilityPolicy is Batched (most common)
   - CloudFirst mode has different behavior (deferred visibility until cloud ack)
   - Strict mode fsyncs after every single write
   - This trace focuses on the **per-write API contract**, not WAL policy

2. **Local Filesystem**: Traces assume local disk storage (not cloud-only)
   - fsync guarantees are file-system dependent
   - SST flush writes to local disk or is uploaded to cloud

3. **No Write Stalls**: Traces assume write stall conditions do not occur
   - If memtable queue full: WriteStall error returned (backpressure)
   - Affects buffered/sync equally

4. **In-Process Execution**: Traces assume embedded library usage
   - No RPC/server boundaries
   - send_and_wait is synchronous blocking channel communication
   - Event loop runs on dedicated thread

### What Could Invalidate This Trace

1. **Changes to ingest batching logic**: If ingest coordinator is modified, blocking points change
2. **Changes to WAL DurabilityPolicy**: If WAL policy becomes CloudStrict or different strategy, behavior differs
3. **Changes to group commit waiter queues**: If durability coordinator logic changes, async completion changes
4. **Changes to writer.sync()**: If fsync behavior changes or is made async, durability boundaries shift

---

## Code References (Authoritative)

### API Layer
- [engine/api/write_options.rs](src/engine/api/write_options.rs): WriteOptions definition
- [engine/api/transaction.rs#L360](src/engine/api/transaction.rs#L360): Transaction::commit
- [engine/mod.rs#L678](src/engine/mod.rs#L678): Engine::commit

### Ingest Batching
- [engine/ingest.rs#L135](src/engine/ingest.rs#L135): IngestCoordinator::submit_write
- [engine/ingest.rs#L180](src/engine/ingest.rs#L180): IngestCoordinator::ingest_loop
- [engine/ingest.rs#L215](src/engine/ingest.rs#L215): IngestCoordinator::commit_batch

### Event Loop
- [runtime/event_loop.rs#L499](src/runtime/event_loop.rs#L499): RuntimeMsg::WalAppend dispatch
- [runtime/event_loop.rs#L611](src/runtime/event_loop.rs#L611): RuntimeMsg::ApplyTransaction dispatch
- [runtime/event_loop.rs#L2146](src/runtime/event_loop.rs#L2146): RuntimeMsg::WalSync dispatch

### WAL Actor
- [runtime/actors/wal.rs#L233](src/runtime/actors/wal.rs#L233): WalActor::append
- [runtime/actors/wal.rs#L296](src/runtime/actors/wal.rs#L296): BestEffort WAL skip
- [runtime/actors/wal.rs#L1003](src/runtime/actors/wal.rs#L1003): WalActor::sync_internal (fsync)

### Durability Policies
- [wal/policy.rs#L21](src/wal/policy.rs#L21): DurabilityPolicy enum

---

## Non-Issues and Clarifications

### Q: Why does buffered return before fsync if it uses Batched WAL mode?
**A**: Design choice to maximize throughput. Group commit fsync happens asynchronously via batch thresholds. Caller is not blocked on the critical path. Durability is eventual, not immediate.

### Q: Why does best_effort skip WAL entirely?
**A**: It's a fast path for bulk loads where:
- Data can be reloaded from source if lost
- Throughput is critical
- Durability is not required until flush_cf() is called explicitly
- Avoids WAL I/O overhead

### Q: Can a best_effort write become durable without calling flush_cf()?
**A**: Only if the engine internally flushes (e.g., memtable becomes full). But this is not guaranteed. The safe pattern requires explicit flush_cf().

### Q: What if a crash happens during batch group commit fsync for buffered writes?
**A**: Writes in the batch that were already persisted to WAL before crash are recovered on restart (via WAL replay). Writes queued but not yet fsynced are lost (not in WAL).

### Q: Is sync mode actually blocking the event loop?
**A**: No. The event loop processes the WalSync message and calls writer.sync() internally (blocking the event loop thread for fsync duration). Meanwhile, the caller's thread is blocked waiting for the response. No deadlock because they are different threads.

---

## Conclusion

All three write durability options in Midge have execution paths that **match their documented contracts**:

- ✅ **sync**: Write is durable when call returns (fsync blocks caller)
- ✅ **buffered**: Write is visible, durability deferred asynchronously (group commit batch fsync)
- ✅ **best_effort**: Fastest path, no durability guarantee (no WAL, crashes lose data)

The implementation maintains clear separation:
1. **API-level contract** (WriteOptions): Caller-facing durability semantics
2. **Runtime durability policy** (DurabilityPolicy): WAL-level sync strategy
3. **Memtable visibility**: All paths apply immediately (for reads)
4. **Persistence boundary**: Differs by option (fsync, batch, or flush)

Traces include concrete code locations, blocking points, durability boundaries, and crash scenarios.
