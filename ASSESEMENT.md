# Midge — Technical Assessment (brutally honest)

📌 Purpose: Record the implementation review findings focused on correctness, durability, and performance risks (code-focused; ignores docs/CI/style).

---

## Executive summary ✅
- The actor single-owner model simplifies reasoning, but there are multiple **critical** correctness and durability bugs that could cause **data loss, resurrection of deletes, or client-visible incorrect behavior**.
- Highest-risk areas: **SST write atomicity & manifest updates**, **range scans / snapshot visibility**, and **CloudFirst durability/idempotency**. These must be fixed before production use.

---

## Findings (detailed)
Each finding lists: what can go wrong → why current code allows it → severity → suggested fix.

### 1) SST file atomicity & manifest updates
- What can go wrong: Manifest can refer to a partially-written or corrupted SST; reads can see corruption, or manifests point to non-durable files (data loss / silent corruption).
- Why: SST writers use `finish_to_path` default which writes directly (no write-to-temp + fsync + atomic rename + dir-fsync). Manifest is updated after writer finishes without enforced durable file visibility.
- Severity: **critical**
- Fix: Require atomically publishing SSTs: write to `{name}.tmp`, fsync file, rename atomically, fsync parent directory. Only after rename/fsync write manifest intent and persist. Add tests that simulate crash between steps.


### 2) Range scans & snapshot visibility
- What can go wrong: Range scans only consult active and immutable memtables; they do NOT include SSTs. Snapshot sequence arguments are ignored on read/range paths. Result: range scans miss flushed data and snapshots are not isolation-preserving.
- Why: `handle_range_scan()` only merges memtable data and never reads SSTs. `handle_read()` ignores the `_seq` parameter for snapshot filtering for SSTs.
- Severity: **critical**
- Fix: Implement SST-aware range scans and snapshot-aware reads using `SstStateReader::get_state_at()` / `scan_range_state()` where possible; merge SST and memtable results correctly. Add snapshot unit tests for reads and range queries.


### 3) Compaction tombstone dropping ignores snapshots (resurrection risk)
- What can go wrong: Compaction unconditionally drops tombstones; with long-lived snapshots a delete may be lost, resurrecting data for snapshots.
- Why: `filter_tombstones()` drops all tombstones without considering a snapshot horizon.
- Severity: **critical**
- Fix: Make compaction snapshot-aware; only drop tombstones older than the oldest active snapshot (provide horizon to the compactor). Add tests ensuring deletes stay invisible to snapshots.


### 4) CloudFirst semantics: idempotency and confirmations
- What can go wrong: Idempotent allocations (request_id → sequences) are not confirmed on cloud ACK; retrying a request may allocate a second sequence or the original entry may never be cleaned—leading to duplicates or unbounded idempotency state.
- Why: `state.confirm_sequences()` is called in local `sync` completion paths but not when CloudFirst ACK completes in `handle_cloud_upload_complete`/durability completion.
- Severity: **serious**
- Fix: On CloudFirst successful ack/response path, call `state.confirm_sequences(request_id)` (or equivalent) for relevant waiters and use `cloud_durable_seq` as confirmation frontier for cleanup. Add tests that append (CloudFirst), ACK, then retry same request_id and assert idempotency behavior.


### 5) WAL → memtable visibility & ordering (CloudFirst gating)
- What can go wrong: In CloudFirst the WAL is appended but memtable updates are deferred until cloud ACK; durability frontier checks and read waiters might be inconsistent, enabling reads that should be gated (or the opposite — hanging reads).
- Why: `is_sequence_durable()` uses frontiers but EventLoop and waiters interplay assumes memtable visibility is synchronized with frontiers; CloudFirst gating and applying pending writes live in separate codepaths and can drift.
- Severity: **serious**
- Fix: Ensure the durability coordinator semantics are aligned with CloudFirst: consider `cloud_durable_seq` as the visibility frontier for CloudFirst mode; when cloud ACK advances, apply pending writes in strict sequence order and only after applying confirm waiters and idempotency entries.


### 6) Blocking IO on EventLoop (Flush & Compaction)
- What can go wrong: Event loop blocks during heavy IO (SST write, compaction), causing latency spikes, queue growth, and potential starvation.
- Why: `FlushActor::handle_flush()` and `CompactionActor::run_compaction()` perform blocking work synchronously in the event loop.
- Severity: **serious**
- Fix: Offload SST write and compaction `execute_compaction()` to a worker thread pool; keep only fast state mutations on EventLoop. Use completion messages to update state/manifest.


### 7) WAL partial-appends & replay robustness
- What can go wrong: Partial WAL writes can lead to replay corruption. While replay detects truncated records, more robust detection (per-record checksum) and testing are needed.
- Why: WAL writer writes length-prefixed record and replay checks sizes (good), but tests must exercise partial write cases (chaos fs).
- Severity: **serious**
- Fix: Add per-record checksums or stronger integrity checks and add chaos tests simulating partial writes and interrupted syncs.


### 8) CloudFirst backpressure and memory growth
- What can go wrong: Pending cloud-write queue may grow (OOM) if cloud is slow; the code uses thresholds but the accounting is approximate and copies buffers into the queue, doubling memory.
- Why: Pending entries clone keys/values into `PendingCloudWrite` and account by heuristics (~1KB per write).
- Severity: **serious**
- Fix: Reduce copies (store `Bytes` or WAL offsets), tighten accounting, and ensure backpressure paths return clear errors to callers. Add stress tests simulating long cloud latency.


### 9) Testing & invariant gaps
- What can go wrong: Many correctness assumptions (snapshot-invariant, compaction invariants, CloudFirst idempotency) are not asserted in tests; regressions are likely to be missed.
- Severity: **serious**
- Fix: Add unit & integration tests for the scenarios above: CloudFirst ack/fail, range scan including SST, snapshot+compaction, partial SST crash and recovery, and blocked flush/compaction timing.


## Prioritized action plan (recommended)
1. Critical fixes (immediately):
   - Implement SST atomic publish (temp file + fsync + rename + dir fsync) and update manifest persistence to happen only after SST durability. Add tests for crash during SST write.
   - Fix range scan & read snapshot semantics to include SSTs and snapshot_seq filtering (add tests).
   - Prevent tombstone dropping that violates snapshots (make compaction snapshot-aware).

2. High priority (next):
   - CloudFirst: confirm idempotency entries on ACK; ensure correct waiter completion and cleanup logic.
   - Add end-to-end tests for CloudFirst failure modes and retries.

3. Medium priority:
   - Offload flush/compaction IO off EventLoop.
   - Improve pending CloudFirst memory usage and backpressure semantics.

4. Long term:
   - Add per-record checksums for WAL and SST validation at open.
   - Add concurrency & stress tests (chaos FS, simulated power-loss, cloud slowdowns).

---

## Suggested concrete code changes (short)
- `sst` writer: require writers implement atomic publish and fsync parent directory before returning success; update `DynSstWriter::finish_to_path` and `FsSstWriter`.
- `flush.rs`: freeze memtable on event loop then schedule `write_memtable_to_sst` on worker thread; send `FlushComplete` message back to event loop when done.
- `compaction.rs`: run `execute_compaction` off-thread; ensure output SSTs are published atomically before manifest update via `ManifestActor::add_sst()`.
- `wal.rs` + `event_loop.rs`: on CloudFirst ACK, call `state.confirm_sequences(request_id)` for waiters, update `pending_batch_min_seq` cleanup, and ensure `cloud_durable_seq` is used for cleaning idempotency entries.
- Add tests for each scenario above.

---

## Closing notes
- The codebase demonstrates careful design and many correct behaviors (e.g., WAL length-prefix checking, intent log, skiplist memtable design). However, a few critical orchestration & persistence gaps could lead to serious data correctness and production reliability problems. Address the critical fixes first (SST publishing, read visibility, compaction tombstone semantics, CloudFirst idempotency), add tests, then improve performance/operational robustness.


---

If you want, I can open focused PRs in priority order — e.g., start with the SST atomic publish + manifest safety change and tests. Say which I should implement first and I will proceed.