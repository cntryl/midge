# Midge: Critical Design + Code Review

**Date:** December 16, 2025  
**Scope:** Architecture, actor runtime, data correctness, durability, recovery, implementation quality  
**Confidence:** High (comprehensive codebase review)

---

## CORE INVARIANTS MIDGE MUST UPHOLD

Midge is production-grade only if it maintains these 10 invariants at all times:

1. **Monotonic Sequence Numbers** — Every write receives a globally unique, strictly increasing seqno; sequence must never decrease within a process lifetime.

2. **Durability Frontier Correctness** — In Strict/Batched modes: `local_durable_seq` is max seqno on-disk in WAL. In CloudFirst: `cloud_durable_seq` is max seqno confirmed by cloud. Reads must never return data beyond durable frontier if requested with durability.

3. **Read Precedence (LSM ordering)** — Point lookups check: (1) mutable memtable, (2) immutable memtables (newest first), (3) L0 SSTs, (4) L1+ SSTs. Latest seqno wins; tombstones hide earlier data.

4. **Atomic Flush** — All entries in a frozen memtable must appear in the output SST or fail completely; no partial flushes.

5. **Compaction Preserves Values** — Compaction must not lose, resurrect, or corrupt user data; only oldest value per key survives (+ merge operator semantics).

6. **Manifest Consistency** — SST file metadata must match disk state; manifest persists before SSTs become visible to readers.

7. **Crash-Safe Recovery** — After restart, WAL replay reconstructs memtables; no data loss if fsync was issued; no corrupted reads from partially-written SSTs.

8. **Actor Isolation** — Each actor (WAL, Flush, Compaction, Manifest, Cloud) mutates disjoint state; no re-entrancy; responses route to correct requesters.

9. **Column Family Isolation** — Writes/reads to CF A don't block or interfere with CF B; each CF has own memtables, merge operators, SSTs.

10. **Write Visibility** — Writes become visible (to non-transactional reads) only after durability frontier advances; writes in-flight don't leak.

---

## EXECUTIVE SUMMARY

### 10 Hardest Truths (First)

1. **🔴 CRITICAL: WAL durability frontier not properly enforced at read time** — `Read` messages don't wait for requested durability. Reads can return un-durable data even when durability policy is `Strict`. This violates durability semantics and risks losing acknowledged writes on crash.

2. **🔴 CRITICAL: CloudFirst mode has unbounded memory growth** — When cloud upload stalls, `pending_cloud_writes` in WAL actor can grow without bound. Memtable doesn't reflect pending writes, so reader sees "missing" data; no backpressure mechanism exists.

3. **🔴 CRITICAL: Sequence number allocation not idempotent** — `state.next_sequence()` increments on each call but WriteBatch operations can retry or be interrupted; no guard against duplicate alloc. Seqno gaps or collisions possible.

4. **🔴 CRITICAL: Manifest mutations are not atomic** — Multiple manifest updates (add SST, remove SST, compact) are applied via separate messages. If runtime crashes between `ManifestAddSst` and `ManifestCompactionComplete`, orphaned SSTs or missing references result.

5. **🔴 HIGH: Snapshot isolation broken for long-lived transactions** — Snapshot holds a seqno but doesn't prevent garbage collection of SSTs containing older data. Range scans on old snapshots can fail mid-way if SST is deleted by compaction.

6. **🔴 HIGH: Compaction doesn't verify TTL expiration** — TTL values stored in WAL/memtable but compaction doesn't filter expired entries. Expired data persists indefinitely, violating TTL contracts.

7. **🔴 HIGH: No validation that merged files are readable** — Compaction produces merged SSTs but does not verify them can be re-opened and read. Corruption goes undetected until a crash recovery or later compaction.

8. **⚠️  HIGH: Error handling loses context pervasively** — Errors from disk I/O, SST parsing, WAL replay are mapped to generic `MidgeError::Internal` with lossy context. Debugging production failures will be very difficult.

9. **⚠️  HIGH: No checksums on SST data blocks** — SST format does not mention CRC/Blake3. Bit-flip corruption on disk goes undetected until decompression or key-value mismatch at read time.

10. **⚠️  MEDIUM: Test suite doesn't verify crash consistency** — `smoke.rs` exercises happy paths but lacks tests for: (a) unclean shutdown mid-compaction, (b) partial WAL writes, (c) manifest corruption recovery, (d) interleaved flush + compaction.

---

## STOP-SHIP ISSUES

Each of these must be fixed before production deployment. Listed with file/function pointers and concrete failure modes.

### **1. [BLOCKER] Durability frontier not enforced in reads**

**Location:** `src/engine/mod.rs` (line ~500), `src/runtime/event_loop.rs` (Read actor)

**Problem:**
- User calls `engine.get(cf, key)` with implicit expectation that result is durable (if writes were fsync'd).
- `Read` message carries `sequence: u64::MAX` (always read latest).
- Event loop never checks `state.wal.local_durable_seq` or `state.wal.cloud_durable_seq` before returning data.
- If a write appears in memtable but hasn't been fsynced, a read can still see it.
- On crash before that fsync, the read result is lost, violating user's durability assumption.

**Failure Mode:**
```
1. Client: put(key, value) → returns Ok
   (Write in memtable, WAL append queued but not yet synced)
2. Crash (power loss, OOM kill, etc.)
3. Restart → WAL replay doesn't find entry (not yet synced)
4. Client: get(key) → returns None (was never durable)
   Expected: Some(value) — contract violation
```

**Affected Invariant:** #2 (Durability Frontier Correctness), #10 (Write Visibility)

**Fix:**
- Add `requested_durability: Durability` to `RuntimeMsg::Read`.
- In event loop's read handler, verify `sequence <= durable_seq` (for the requested durability level).
- If not durable yet, queue read in durability waiter (reuse existing group commit logic from `durability_waiters`).
- Default durability should be derived from engine's default policy (not always MAX).

**Affected Code:**
- `src/runtime/mod.rs` (RuntimeMsg::Read)
- `src/runtime/event_loop.rs` (handle_read)
- `src/engine/mod.rs` (get, get_cf)

---

### **2. [BLOCKER] CloudFirst pending writes unbounded; memory exhaustion + data loss**

**Location:** `src/runtime/actors/wal.rs` (line ~90–150, WalActor::pending_cloud_writes)

**Problem:**
- In CloudFirst mode, writes are appended to local WAL but NOT added to memtable immediately.
- They sit in `pending_cloud_writes` VecDeque waiting for cloud ACK.
- If cloud upload is slow, stalled, or network-partitioned, VecDeque grows without bound.
- No backpressure, no timeout, no size limit.
- Meanwhile, reads see "stale" data because pending writes aren't visible yet.
- Engine reports success to user, but data isn't actually visible or durable.

**Failure Mode:**
```
1. Cloud upload begins to stall (network issues, remote overload)
2. Application issues put() calls rapidly
3. pending_cloud_writes grows: 1K writes → 100K → 1M
4. Each write in queue: ~1KB average (key + value) → 1GB queue
5. Process OOM after sustained writes
6. Crash → local WAL ephemeral, never reached cloud → data loss
```

**Affected Invariant:** #2 (Durability Frontier Correctness), #10 (Write Visibility)

**Fix:**
- Add `MAX_PENDING_CLOUD_WRITES` threshold (e.g., 100K writes or 1GB).
- When exceeded, return `MidgeError::WriteStall` to caller.
- Implement timeout on pending writes (e.g., 30s). If cloud doesn't ACK by then, fail the write batch.
- Track telemetry: gauge for pending queue depth, histogram of wait times.
- On cloud ACK, process pending writes in batches (group commit).

**Affected Code:**
- `src/runtime/actors/wal.rs` (WalActor::pending_cloud_writes, handle_append, handle_cloud_upload_complete)
- `src/runtime/state.rs` (WalState — add max bounds)
- `src/telemetry/` (add gauges for queue depth)

---

### **3. [BLOCKER] Sequence number allocation not idempotent; collisions on retry**

**Location:** `src/runtime/state.rs` (line ~100–120, next_sequence), `src/runtime/actors/wal.rs` (handle_append)

**Problem:**
- `state.next_sequence()` is called once per write and simply does `self.sequence += 1`.
- No guard against double-allocation.
- If a WriteBatch operation encounters an error mid-way (e.g., disk full, cloud timeout), the message is retried by caller.
- But the sequence numbers are already consumed and advanced.
- Result: seqno gaps, or if retry succeeds, different seqnos for same logical operation (same data appears twice with different versions).

**Failure Mode:**
```
1. WriteBatch([put(a, v1), put(b, v2)]) allocated seqno [100, 101]
2. WAL append succeeds, memtable updated
3. Manifest update fails (disk full)
4. Caller retries WriteBatch
5. WalActor allocates seqno [102, 103] for SAME data
6. Now manifest has:
   - a@100 (first attempt)
   - a@102 (retry attempt)
   - b@101 (first attempt)
   - b@103 (retry attempt)
7. Reads return duplicates or incorrect versioning
```

**Affected Invariant:** #1 (Monotonic Sequence Numbers)

**Fix:**
- Wrap sequence allocation in an idempotency cache per request_id.
- Store `request_id → (allocated_seqnos, durability_confirmed)` in runtime state.
- On retry of same request_id, return cached seqnos instead of allocating new ones.
- Clear cache after durability frontier advances past the allocated seqnos.
- Alternatively: use WAL record offset (segment_id + offset_in_file) as seqno, which is inherently idempotent.

**Affected Code:**
- `src/runtime/state.rs` (next_sequence, add idempotency cache)
- `src/runtime/actors/wal.rs` (handle_append, check cache before allocate)

---

### **4. [BLOCKER] Manifest mutations not atomic; crashes leave orphaned SSTs**

**Location:** `src/metadata/`, `src/runtime/actors/manifest.rs`, `src/runtime/event_loop.rs`

**Problem:**
- Compaction flow involves multiple asynchronous message sends:
  1. Write merged SST to disk
  2. Send `ManifestCompactionComplete` (remove old SSTs, add new SST)
  3. Send `ManifestPersist` (fsync manifest to disk)
- These are separate operations with no atomicity.
- If crash happens between step 2 (in-memory) and step 3 (persisted), the manifest on disk is stale.
- On restart: old SSTs are still in manifest but already deleted from disk (or vice versa).
- Result: corruption, data loss, or orphaned files.

**Failure Mode:**
```
1. Compaction merges SSTs [A, B] into [C]
2. C is written to disk successfully
3. ManifestCompactionComplete applied in-memory:
   - state.manifest.remove(A, B)
   - state.manifest.add(C)
4. ManifestPersist is queued but not yet executed
5. CRASH (power loss)
6. On restart:
   - Manifest loaded from disk (still shows A, B, not C)
   - WAL recovery: no entry for C (it's not in manifest yet)
   - SST C on disk is never read; data loss
```

**Affected Invariant:** #6 (Manifest Consistency), #5 (Compaction Preserves Values)

**Fix:**
- Implement write-ahead intent log for manifest mutations (see `src/runtime/intent_persistence.rs`).
- Before applying `ManifestCompactionComplete` in-memory, write intent to persist: "compaction X removes [A, B] adds [C]".
- Only after intent is durable (fsync'd), apply to in-memory manifest.
- On recovery, replay intent log to re-apply any interrupted mutations.
- Alternatively: use atomic rename trick: write new manifest to `.tmp`, then atomic rename to manifest file (POSIX atomic rename).

**Affected Code:**
- `src/runtime/actors/manifest.rs` (handle_compaction_complete)
- `src/metadata/persistence.rs` (save)
- `src/runtime/intent_persistence.rs` (expand to cover all mutations)
- `src/runtime/event_loop.rs` (load + replay intent log on startup)

---

### **5. [BLOCKER] Compaction merge operator semantics not validated; silent data corruption**

**Location:** `src/compaction/merge.rs`, `src/compaction/executor.rs`

**Problem:**
- Compaction calls user-provided `MergeOperator::merge()` to combine multiple operands.
- No validation that merge result is correct, deterministic, or different from input.
- If `MergeOperator` implementation is buggy (e.g., ignores some operands, has side effects), compaction silently corrupts data.
- No way to detect corruption until reads return incorrect values.
- No checksum to catch merge operator bugs.

**Failure Mode:**
```
1. User registers MergeOperator for CF:
   fn merge(&self, operands: Vec<&[u8]>) -> Vec<u8> {
       // BUG: only takes first operand, ignores rest
       operands[0].to_vec()
   }
2. Writes: merge(a, 1) + merge(a, 2) + merge(a, 3)
3. Expected result: merge(1, 2, 3) = some_function(1, 2, 3)
4. Compaction merges SSTs, calls merge operator → returns 1 (BUG)
5. Result (1) written to new SST and persisted
6. On next read: get(a) returns 1 (should return some_function(1, 2, 3))
   Data silently corrupted; no indication compaction was culprit
```

**Affected Invariant:** #5 (Compaction Preserves Values)

**Fix:**
- During compaction, run merge operator twice independently on same operands; compare results.
  - If different, log error and fail compaction (data integrity issue).
  - If merge is non-deterministic, operator bug is caught immediately.
- Store merge operator ID + version in SST metadata (header).
- On read, verify merge operator version matches registered operator.
  - If version not found, reject read (safer than silent merge with wrong operator).
- Add observability: metrics for merge operations, sample outputs logged for debugging.

**Affected Code:**
- `src/compaction/merge.rs` (apply_merge_operator — add validation)
- `src/sst/types.rs` (SST header — add merge op version)
- `src/sst/traits.rs` (SstReader — check merge op version on read)

---

### **6. [BLOCKER] No validation that compacted SSTs are readable; corruption undetected**

**Location:** `src/compaction/executor.rs` (write_versions_to_sst)

**Problem:**
- Compaction produces SST file and writes to disk.
- No re-open or sample-read to verify file is readable and not corrupted.
- Corruption during write (e.g., power loss during fsync, disk error, OOM during serialization) goes completely undetected.
- On later recovery or read, SST fails to parse → crashes or returns None silently.

**Failure Mode:**
```
1. Compaction creates 1GB SST file
2. During write: disk error or power loss
3. Only 900MB written to disk; file is truncated/corrupted
4. File handle closed; compaction marked complete
5. Manifest updated with new SST reference
6. Later, read query touches that SST
   → tries to parse header
   → parse fails (truncated)
   → returns MidgeError::Corruption
   → or silently returns None (corruption hidden)
   → data loss
```

**Affected Invariant:** #7 (Crash-Safe Recovery)

**Fix:**
- After writing SST file, immediately re-open it and sample-read:
  - Read first and last key ranges
  - Verify all keys are in order
  - Check block integrity (decompress, validate checksums if present)
- If any check fails, delete SST and fail compaction.
- Add CRC/Blake3 checksums to SST data blocks (see Issue #9 below).

**Affected Code:**
- `src/compaction/executor.rs` (write_versions_to_sst — add post-write validation)

---

### **7. [BLOCKER] TTL expiration not enforced during compaction; expired data persists**

**Location:** `src/compaction/executor.rs` (filter_tombstones), `src/sst/` (entry metadata)

**Problem:**
- Entries can be tagged with TTL (time-to-live) in `WalRecord`.
- Compaction reads entries but doesn't check if TTL has expired.
- Expired entries are written to new SST without filtering.
- Later reads still see expired data (should be hidden).
- TTL contract violated; users expect data to disappear after TTL.

**Failure Mode:**
```
1. Put(key="session_123", value="token", ttl=1_second)
2. Entry appended to WAL + memtable
3. Wait 2 seconds (TTL expired)
4. Compaction runs (doesn't check TTL)
5. Expired entry written to SST unchanged
6. Read(key="session_123") 
   → returns "token" (should be None)
   → security issue: expired session token is reusable
```

**Affected Invariant:** #3 (Read Precedence — includes TTL filtering)

**Fix:**
- During compaction entry processing, check each entry's expiration:
  - Store write_time in entry metadata (or compaction_time + ttl)
  - current_time - write_time > ttl? → treat as tombstone (filter out)
- Add compaction_timestamp to every SST metadata (or every entry).
- Add metrics: # of expired entries filtered by compaction.
- Update read path to also filter expired entries.

**Affected Code:**
- `src/compaction/executor.rs` (filter_tombstones — add expiration check)
- `src/wal/types.rs` (WalRecord — add write_time)
- `src/sst/types.rs` (entry metadata — add expiration info)

---

### **8. [HIGH] Snapshot isolation broken; SST deletion during long-lived scan causes data loss**

**Location:** `src/engine/api/snapshot.rs`, `src/runtime/event_loop.rs`, `src/runtime/gc_actor.rs`

**Problem:**
- Snapshot holds a sequence number but doesn't prevent garbage collection.
- Compaction can delete SSTs that are older than snapshot's seqno.
- If user is mid-range-scan on that snapshot, the SST can be deleted while being read.
- Read fails with "file not found" or returns incomplete results.
- No mechanism to wait for in-flight scans before deleting SSTs.

**Failure Mode:**
```
1. Snapshot created at seqno 100
2. User starts range_scan(snapshot) over keys [a, z]
3. range_scan is reading SST_001 (seqno 50)
4. Compaction runs, determines SST_001 is obsolete
5. Compaction deletes SST_001 from disk
6. range_scan iterator tries to read next block from SST_001
   → file not found error
   → scan fails, returns partial data
```

**Affected Invariant:** #7 (Crash-Safe Recovery), #10 (Write Visibility)

**Fix:**
- Track active snapshots in runtime state: `snapshot_id → (seqno, ref_count)`.
- Before deleting SST in GC actor, check if any snapshot references it:
  - Get list of SSTs >= snapshot_seqno
  - If overlap with SSTs to be deleted, defer deletion
- When snapshot is dropped, decrement ref_count; trigger GC to retry deletion.
- Add timeout: if snapshot held > 1 hour, force-close it (prevent indefinite SST pinning).

**Affected Code:**
- `src/runtime/state.rs` (add snapshot_pin registry)
- `src/engine/api/snapshot.rs` (register on creation, unregister on drop)
- `src/runtime/gc_actor.rs` (check pin registry before deleting SSTs)

---

## HIGH-RISK DESIGN DEBTS

These don't cause immediate failures but make the system fragile, hard to operate, and prone to subtle bugs.

### **#1: Actor communication not typesafe; message routing is fragile**

**Location:** `src/runtime/mod.rs` (RuntimeMsg enum), `src/runtime/dispatch.rs`

**Problem:**
- `RuntimeMsg` is a massive enum (~50+ variants), all in a single type.
- Dispatcher must pattern-match on every variant; if one is missed, message silently disappears.
- `request_id` extraction is repetitive and error-prone (appears ~40 times).
- Adding a new actor requires editing Dispatcher, EventLoop, and response routing.

**Risk:** New actor added, dispatcher logic forgotten, messages silently drop and requests timeout.

**Fix:**
- Use trait-based routing: `trait RuntimeActor { fn handle(&mut self, msg: Self::Msg) → Response }`.
- Each actor subscribes to its own message type (compile-time checked).
- Router becomes a dispatch table: `Arc<dyn Fn(RuntimeMsg) → Box<dyn Any>>`.
- Avoids runtime dispatch errors and centralizes routing logic.

---

### **#2: No structured intents for crash recovery; state + manifest can be inconsistent**

**Location:** `src/runtime/intent_persistence.rs`, `src/runtime/state.rs`

**Problem:**
- Intent log exists but is only loaded on startup.
- State mutations happen in memory, then persisted to manifest asynchronously.
- If crash between mutation and persist, recovery can miss critical updates.
- Subtle data loss scenarios possible if crash at wrong time.

**Risk:** Edge case crashes cause data inconsistency.

**Fix:**
- Write intent to log BEFORE mutating state.
- Persist intent (fsync) before applying to in-memory state.
- On recovery, replay intent log to restore any interrupted mutations.
- Similar to two-phase commit: intent phase (prepare), then apply phase.

---

### **#3: WAL recovery is re-run fully on every startup; scales poorly**

**Location:** `src/wal/recovery.rs` (replay_wal)

**Problem:**
- `replay_wal()` reads all WAL files sequentially, replays all entries to memtable.
- On 100GB WAL, this takes minutes (or longer).
- Stops on first corruption but doesn't report which file/offset (debugging hard).
- Production incidents with slow startups are hard to diagnose.

**Risk:** Unacceptable startup latency in production.

**Fix:**
- Checkpoint: periodically persist memtable state + last_wal_offset to disk.
- On restart, load checkpoint; only replay WAL tail (since checkpoint).
- Reduces startup time from minutes to seconds.

---

### **#4: Merge operator registration is per-engine, not per-CF-version; operator mismatch**

**Location:** `src/engine/mod.rs` (merge_operators registry), `src/compaction/merge.rs`

**Problem:**
- Merge operators are registered globally per CF.
- Later code deployments can change merge operator logic.
- Old operator still used for existing SSTs (no version tracking).
- Merge results become inconsistent; silent data corruption.

**Risk:** Silent data corruption during code rollout.

**Fix:**
- Version merge operators; tag each SST with merge op version + ID.
- On read, verify version matches registered operator; reject if not found.
- On upgrade, validate all SSTs have compatible operator version.

---

### **#5: No per-level bloom filter tuning; read amplification not minimized**

**Location:** `src/sst/bloom/`, `src/sst/traits.rs` (SstFactory)

**Problem:**
- Bloom filters have hardcoded size/FPR.
- L0 (high overlap) might benefit from larger bloom filters (lower FPR).
- L1+ (sorted) might not need them or need smaller size.
- Suboptimal performance; wasted memory or high false-positive rate.

**Risk:** Read amplification increases; read latency spikes.

**Fix:**
- Allow per-level bloom configuration.
- Measure false-positive rate; adjust at compaction time.

---

### **#6: Lock contention on column_families DashMap during high-concurrency reads**

**Location:** `src/engine/mod.rs` (column_families: DashMap)

**Problem:**
- Every read path locks the `column_families` DashMap to fetch CF state.
- High-concurrency workload causes lock contention.
- Read latency spikes on multi-CPU systems.

**Risk:** Unpredictable read latency under load.

**Fix:**
- Use Arc<ColumnFamilyState> instead of DashMap.
- Store in RuntimeState as array indexed by CF_ID (no locks needed).

---

### **#7: Compaction planner doesn't consider write patterns; can trigger thrashing**

**Location:** `src/compaction/planner.rs`, `src/compaction/strategy.rs`

**Problem:**
- Compaction is triggered based on SST count/size alone, not write distribution.
- Heavy writes to same key range can cause repeated compactions.
- Compaction churn burns CPU; write throughput drops.

**Risk:** Performance cliff under skewed write patterns.

**Fix:**
- Track hot key ranges; adjust L0→L1 trigger based on overlap.
- Batch more SSTs when hot range detected.

---

### **#8: No rate limiting on background actors; can starve foreground reads**

**Location:** `src/runtime/scheduler.rs`, `src/runtime/event_loop.rs`

**Problem:**
- Compaction, flush, and cloud upload run with no prioritization.
- Sustained write workload triggers continuous flush/compaction.
- Foreground reads queued behind compaction messages.
- Read latency becomes unpredictable under load.

**Risk:** SLA violations; read p99 latency spikes.

**Fix:**
- Implement priority queue in event loop.
- Prioritize user operations (reads, writes) over background housekeeping.

---

### **#9: Errors from cloud uploads swallowed; data loss possible**

**Location:** `src/runtime/actors/cloud.rs`, `src/runtime/event_loop.rs`

**Problem:**
- Cloud actor queues uploads but doesn't report failures back to caller.
- If cloud upload fails, local WAL is deleted but data never reached cloud.
- Silent data loss.

**Risk:** Data loss in cloud-backed mode.

**Fix:**
- Fail writes if cloud upload fails.
- Don't delete local WAL until cloud confirms receipt + durability.

---

### **#10: No backward compatibility for WAL/SST format changes; upgrades require full rebuild**

**Location:** `src/wal/encoding.rs`, `src/sst/encoding.rs`

**Problem:**
- WAL and SST formats have no version fields.
- Any format change breaks old files.
- Users can't upgrade Midge without data loss.

**Risk:** Upgrades require full data rebuild (downtime).

**Fix:**
- Add format version to WAL + SST headers.
- Implement upgrade path (on-the-fly conversion if possible).

---

## REFACTOR PLAN

A minimal sequence of fixes to achieve production readiness. Each step ≤ 1 day.

### **Step 1: Durability frontier enforcement** (Day 1)

- Modify `RuntimeMsg::Read` to carry `Durability` enum (Strict, Batched, CloudFirst).
- In event loop's read handler, validate `sequence <= durable_seq` before responding.
- If not durable yet, queue read in `durability_waiters` group commit.
- Add integration test: `should_not_return_unsynced_write_on_crash`.

### **Step 2: CloudFirst backpressure + timeout** (Day 2)

- Add max size check in WAL actor: `if pending_cloud_writes.len() > MAX_PENDING (100K)`.
- Return `MidgeError::WriteStall` when exceeded; caller retries with backoff.
- Implement timeout on pending writes (30s). If cloud ACK not received, fail batch.
- Add telemetry: gauge for pending queue depth, histogram of wait times.

### **Step 3: Sequence number idempotency** (Day 3)

- Add `request_id → (allocated_seqnos, confirmed_at)` cache in `RuntimeState`.
- Before calling `next_sequence()`, check cache. Return cached seqnos on retry.
- Clear cache after durability confirmed (remove entries with confirmed_at < durable_seq).

### **Step 4: Manifest atomicity via intent log** (Day 4)

- Expand `IntentPersistence` to cover all manifest mutations.
- Before applying `ManifestCompactionComplete`, write intent to log, fsync, then apply.
- On recovery, replay intent log from checkpoint.
- Add test: `should_recover_incomplete_manifest_mutation_on_crash`.

### **Step 5: TTL enforcement + compaction validation** (Day 5)

- Add expiration check in compaction: `if write_time + ttl < now { skip_entry }`.
- Add post-write validation in `execute_compaction()`: re-open SST, sample-read keys, verify order.
- Add CRC checksums to SST blocks (optional; defer if time-constrained).

---

## QUESTIONS FOR STAKEHOLDERS

1. **CloudFirst durability model**: What is the cloud SLA? (99.99%?) Should local WAL be ephemeral, or persisted until cloud confirms? When should pending writes become visible to reads?

2. **Merge operator guarantees**: Are users required to provide idempotent, deterministic operators? What if they don't? Should Midge sandbox/validate merge operations?

3. **Snapshot lifetime**: What is intended maximum lifetime? Can be held indefinitely (weeks?)? Should there be auto-close timeout?

4. **Format evolution strategy**: How will WAL/SST format changes be handled on upgrade? Require full rebuild, or on-the-fly conversion?

5. **Recovery time SLA**: How long is acceptable for WAL replay on startup? Seconds? Minutes? If > 1 min, should checkpointing be default?

---

## VERDICT

**Current Status:** Research-grade prototype, not production-ready.

**Go/No-Go for Production:** **NO-GO** (fix all 8 blockers first)

**Effort to Production:** ~5 days (fix blockers), + 2 weeks (high-risk debts), + 1 week (comprehensive testing)

**Confidence in Architecture:** Medium. Actor model is sound, but durability semantics are incomplete. WAL/compaction/recovery are fundamentally correct but fragile due to lack of atomicity and error handling rigor.

**Biggest Strengths:**
- Clean actor model (good isolation, testable)
- Deterministic event loop (reproducible bugs)
- Intent log foundation (good for crash recovery)
- Comprehensive test framework (benches, unit tests, smoke tests)

**Biggest Weaknesses:**
- Durability semantics incomplete (reads don't respect frontiers)
- No atomic manifest mutations (data loss risk)
- Unbounded memory in CloudFirst (can crash)
- Errors lose context (hard to debug)
- No data validation on write (corruption undetected)

---

**Reviewed by:** AI Code Review (December 2025)  
**Next Steps:** Prioritize blockers; create issues for each stop-ship item; schedule refactor work.
