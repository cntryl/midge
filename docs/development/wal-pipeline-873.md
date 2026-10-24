# WAL pipeline: current state, target design, and slice plan (#873)

Historical design baseline: `develop` at `12962819` (2026-10-24). Local candidate
implementation and outstanding qualification are tracked in
[the integration log](issue-integration-0.5.0.md); the appendices retain their
original source observations.
Method: four read-only research passes over the WAL write-to-recovery lifecycle, each citing lines it read in the
source. The passes are included verbatim as appendices A to D, with their own path abbreviations. This document
holds the decisions. The tables in section 2 are the summary, the appendices are the evidence.

On 2026-10-24 the user selected ordered whole-transaction publication for #940.
The loop may prepare and queue later groups while fsync is held, and must serve
reads/control/validation against the earlier applied cut. A later BestEffort
write waits for the preceding whole applied prefix before success or visibility.
Its sequence cannot bypass an unapplied Strict unit. Issue #940 AC1 now uses a
deterministic held-fsync gate for this contract; a five-millisecond later-write
completion is not required. A separate commit-visibility map or reordered
linearization would require another design. This keeps the applied cut monotonic,
immutable snapshots and read-your-writes together, consistent with the
[RocksDB ordering tradeoff](https://github.com/facebook/rocksdb/wiki/unordered_write).

## 1. Conclusions

1. **No wholesale rewrite is justified. A staged pipeline change is.** The recovery, ledger, planner, catalog and
   fencing layers (#808, #869, #870, #872) have one coherent rule set and mostly one owner each; they stay. The
   write path does not: every strict commit blocks the single event-loop thread through two fsyncs, a nested
   encode, an LZ4 pass and a CRC, and no write-path transition has a single owner (section 2.1).
2. **Four hypotheses in the backlog are wrong or incomplete as written** and are replaced (section 6.1):
   #904's premise and requirement R1, #941's requirement R1, #947's gather target, and #905's reason and
   requirement R2. #916 is partly stale.
3. **Decision requested by #904 (R4 of this issue): accept a modified form.** Lost-write detection for the open
   segment becomes exact only below an amortized floor, with an exact edit at rotation and clean shutdown
   (section 5). Durability itself does not change. This is the one decision in this document that changes what a
   recovery can detect, so it needs explicit sign-off at the first slice PR.
4. **WAL persisted formats:** these WAL slices require no byte layout changes or version bump. The separate
   metadata FORMAT 9 work is tracked by #953. The #904 change alters when an existing
   ledger edit is written, not what a reader may assume of it (section 6.3).
5. **Eleven numbered slices, ordered** (section 6.4); slice 8 splits into four and slice 11 into three. Slices 1, 2,
   5, 6 and 10 are correctness, contract or test-debt fixes found by this review that no existing issue covers.
6. **Pipelining has five constraints the backlog did not state** (section 4.5): the durable floor must be the end
   of the fsynced group, snapshots must publish the applied sequence, `WriterEffectGroup` roots must be parked and
   drained, the spill path is a barrier, and a missing admission-closed test must exist first.

## 2. Phase 0: current-state map

### 2.1 The 17 transitions

Columns: owner, where it runs (L = runtime event-loop thread, W = WAL writer thread, C = caller thread,
U = uploader or retirement worker), caller-visible failure, pinning test. Full rows with effects and every
citation are in the appendices; this table keeps the first citation per cell.

Bare file names in sections 2 to 6 mean:

| Name | Path under `src/` | Name | Path under `src/` |
|---|---|---|---|
| `wal.rs` | `runtime/actors/wal.rs` | `transaction.rs` | `runtime/actors/wal/transaction.rs` (the engine's is written `engine/api/transaction.rs`) |
| `durability.rs`, `rotation.rs`, `transaction_state.rs`, `spill.rs` | `runtime/actors/wal/` | `wb.rs`, `write_batch.rs` | `runtime/event_loop/write_batch.rs` |
| `el_wal.rs` | `runtime/event_loop/wal.rs` | `awt.rs` | `runtime/actors/wal/tests.rs` |
| `durability_sync.rs`, `sealing.rs`, `publication.rs`, `shutdown.rs` | `runtime/event_loop/` (`sealing.rs`, `publication.rs` under `cloud_integration/`) | `handle.rs`, `state.rs`, `wal_history.rs`, `lifecycle.rs`, `lease_state.rs` | `runtime/` (`lease_state.rs` under `engine/`) |
| `writer_io.rs`, `writer_runner.rs` | `wal/fs/` | `encoding.rs`, `frame.rs`, `ledger_store.rs`, `traits.rs`, `streaming.rs` | `wal/` (`streaming.rs` under `recovery/`) |
| `streaming_wal_plan.rs` | `runtime/cloud_startup/` | `uploads.rs`, `proofs.rs`, `backend.rs` | `storage/hybrid/backend/` |

| # | Transition | Owner today | Thread | Failure the caller sees | Pinning test |
|---|---|---|---|---|---|
| 1 | admit | **Four layers**: caller stall flag and queue gate (`ingest.rs:77`, `handle.rs:633`), dispatch admission (`el_wal.rs:445`, `wb.rs:120`), drain admission (`wb.rs:593`), actor preparation and byte budget (`transaction.rs:86`, `wal.rs:712`) | C, L | Always definitely-not-applied: `WriteStall`, `NoSpace`, `Fenced`, `WriteConflict`, `ResourceLimit` | L0 ceiling and byte budget pinned; **queue-full `WriteStall` none; drain path skipping cloud-memtable admission none** |
| 2 | sequence | `RuntimeState::allocate_sequences` (`state.rs:1274`), reaching into the ledger for `Reserve` edits | L | `ResourceLimit`; before any WAL byte | `awt.rs:226`, `awt.rs:2872`, `sequence.rs:66` |
| 3 | frame | **Two encode passes in two layers**: nested payload (`transaction.rs:570`, `encoding.rs:635`) then outer frame, LZ4 and CRC32C (`writer_io.rs:225`, `encoding.rs:310`, `frame.rs:43`) | L | `ResourceLimit` or `Corruption` at prepare, sequences released; unchanged error at frame time | round-trip and frame-layout tests pinned |
| 4 | enqueue | `FsWalWriterIo::enqueue_in_group` (`writer_io.rs:333`); enqueue and ack wait are one function | L | Pre-queue: unchanged `WriteStall`/`Timeout`; post-queue ack timeout: unknown, poisons the writer | ack-timeout cases pinned; **queue-full none (appears unreachable)** |
| 5 | append | **Straddles actor and runner**: `WriterRunner` writes (`writer_runner.rs:344`), the actor settles outcome (`transaction.rs:682`) | W, L | `unchanged: true` returns the raw error; `false` returns `OutcomeUnknown`. **Every append failure fences regardless** (`transaction.rs:685`) | rollback proof and group failures pinned |
| 6 | sync | **Three owners, two protocols**: actor ordering (`durability.rs:40`), writer wait (`writer_io.rs:581`), physical fsync (`writer_runner.rs:505`), plus a ledger fsync (`durability.rs:264`). Strict bypasses the ticketed protocol (`durability.rs:64`) | W, **L (ledger fsync inline)** | Any failure fences; `OutcomeUnknown` when a unit had a WAL record | sync failures pinned; **post-submission sync `Timeout` none** |
| 7 | acknowledge | Durability is guaranteed by call order inside `append_prepared_transaction_group` (`transaction.rs:212` before `:216`); the ack function documents "immediate in every mode" (`durability_sync.rs:40`) | L, C | `Fenced` means not logged and not applied; caller `Timeout` becomes `OutcomeUnknown` (`engine/api/transaction.rs:1034`) | group ack paths pinned; **engine remap through `RuntimeHandle` none** |
| 8 | memtable publish | Apply (`transaction_state.rs:119`) then `publish_snapshot` (`event_loop/mod.rs:612`) once per group | L | Apply is infallible after preflight; a lease lapse mid-group marks the family partially applied | apply pinned; **publish ordering and group-atomic visibility none** |
| 9 | rotate | **Two owners**: `WalActor::rotate` (`rotation.rs:60`) and `WalHistory::start_epoch` (`wal_history.rs:507`) at open | L | Before the footer: retryable; from the footer on: fenced, `sealed = N` recorded | actor and event-loop matrices pinned, **both on an empty segment** |
| 10 | seal | **Two owners**: local `rotate_local_wal_transition` (`wal_transition.rs:69`), cloud `try_seal_current_cloud_segment` (`sealing.rs:40`) over one `WalTransitionProtocol` | L | Fenced both views once, or retryable abandon | transition protocol pinned; **`seal_for_cloud` branches none** |
| 11 | LEDGER edit | One write path (`ledger_store.rs:211`), about ten callers in five modules | L at runtime | `SyncOpen` failure: fenced and `OutcomeUnknown`; `Rotate` failure: fenced with `sealed = N`; `Reserve` failure: tolerated while the old reservation covers | torn, heal and semantics pinned; **no test counts ledger fsyncs** |
| 12 | recover-after-partial | **At least five owners**; the durable-floor rule is implemented twice (`streaming.rs:874`, `streaming_wal_plan.rs:408`) | startup | Strict: `Corruption` locally, `RecoveryFailed` in cloud, engine does not open | Strong for torn, sealed, missing, orphan; **salvage below the floor none** |
| 13 | replay | One engine, `replay_wal_with_options` (`streaming.rs:278`), behind three wrappers | startup | `Corruption`, `ResourceLimit`, `Timeout`, `InvalidArgument` fail the open | chain and selection pinned; unit fixtures use `durable_bytes: None` |
| 14 | shutdown | Three layers, two paths (`engine/mod.rs:618`, `shutdown.rs:18`, `lease_state.rs:306`); local seals nothing | C, L | `Busy` while a transaction is live; terminal `Timeout` replayed | clean-shutdown sync pinned; **mid-shutdown kill none; bounded reaper none** |
| 15 | upload segment | **Two owners**: runtime decides (`sealing.rs:300`), storage does the PUT and retry (`uploads.rs:103`); retry ownership is split | L, U (single thread) | `cloud_async` never sees it; `cloud_strict` sees `OutcomeUnknown` | retry, fence and exhaustion pinned; **concurrency none (there is none)** |
| 16 | publish catalog | **Three-way straddle**: pure rules (`publish.rs`, `cloud_catalog.rs`), orchestration (`catalog.rs:395`), lock and cache inside `HybridStorage` (`backend.rs:170`); runs inline on the loop (`publication.rs:11`) | L | Mismatch or refusal fences; retriable requeues; lease failure at ack requeues without fencing the protocol | in-order, stale epoch and withdraw pinned |
| 17 | retire | **Seven decision sites in four modules** (appendix C, section 2); delete retry is memory-only (`proofs.rs:55`) | U | Callerless; catalog is retired before the delete, a failed delete orphans the object | proof conditions pinned; **pending-delete persistence none** |

Decision gate result (Phase 0): not every transition has one owner and a test, so Phase 1 is a design, not a
"no rewrite justified" conclusion. The design is nevertheless incremental (section 4).

### 2.2 Findings that change the plan

These are facts the backlog did not have. Citations are in the appendices.

| # | Finding | Effect |
|---|---|---|
| F1 | A strict commit makes six cross-thread hops and four blocking waits (appendix A, section b). The ledger `SyncOpen` fsync runs **on the event loop**, after the WAL fsync wait. | #904 and #940 target the right cost |
| F2 | The ledger image comparison that #904 calls "O(ledger) read" never runs in production (`ledger_store.rs:156,295`); the real cost is a second fsync plus an O(members) clone. `wal_fsync_count` counts only the writer thread (`writer_runner.rs:564`), so "fsync count halves" cannot be measured today. | #904 AC1 needs a counter first |
| F3 | The durable-floor rule exists twice with different outcomes. Local salvage of a file shorter than the floor quarantines the **whole** open segment; cloud keeps the verified prefix (appendix B, B.2). Code-reading finding, no test. | Fix before #904 changes the floor |
| F4 | Compression covers the **whole nested payload**, and sequences are inside it (`encoding.rs:310`, `:680`). A sequence patch after off-loop compression is impossible without a format change. | #941 R1 replaced |
| F5 | The loop is the only producer and blocks on every ack, so the writer queue never holds more than one entry. The 5000-deep queue and `WriteStall` look unreachable; `writev` has nothing to gather. | #947 narrowed |
| F6 | Overlapping writes are excluded from a group because apply is deferred past fsync (`write_batch.rs:805`), which hides **write-write conflicts and `insert_only`**, not only assertions. | #905 R2 widened |
| F7 | Coalesced followers skip `prepare_cloud_transaction_memtables` (`el_wal.rs:142` vs `wb.rs:593`), so in ephemeral-cache mode they bypass the flush-staging window. Call-site reading only. | New correctness slice |
| F8 | Per-op `recompute_total_memtable_bytes` iterates every column family and immutable (`transaction_state.rs:502`, `flush.rs:274`); `publish_snapshot` builds a snapshot for every column family. | Cheap win before #906 |
| F9 | Every write-submit `Timeout`, including a provably unchanged one, becomes `OutcomeUnknown` at the engine (`engine/api/transaction.rs:1034`). Every append failure fences the actor; `unchanged` only chooses the error. | Outcome slice |
| F10 | `WalWriter` has 12 methods; production calls 9. The three unused ones have defaults that diverge from `FsWalWriterIo` (`traits.rs:106,136`, `writer_io.rs:562`). | Writer-contract slice |
| F11 | Retirement is decided at seven sites in four modules; remote delete retry is memory-only; the uploader does not re-check authority between read and PUT (safe because keys are epoch-named and `If-Absent`). | #943, retirement slice |
| F12 | #916 is partly stale: retired catalog entries are already dropped, publication is already one CAS per contiguous run, receipts already skip readback. Missing: parallel uploads and time-based batching. | #916 narrowed |
| F13 | Local shutdown seals nothing, so the next open re-scans the whole open segment (`wal_history.rs:885`); a leaked transaction pins the drop reaper and the lease forever (`lifecycle.rs:180`). | #933 confirmed |
| F14 | The invariant evidence file is stale: wrong line numbers, 16 test names that no longer exist, no `CLOUD-8` or `SST-10`. | Hygiene slice |

## 3. SOLID review (R8)

Evidence and the full per-principle analysis are in appendix D, section 1. Rule applied: no new abstraction unless
it removes a recorded violation.

| Principle | Verdict | Violations and decisions |
|---|---|---|
| S | partially holds | `wal.rs` has six reasons to change (`wal.rs:172-951`). **S1** MVCC validation and memtable apply in `transaction_state.rs`, duplicated in `spill.rs:294`: gap, split only inside a slice that edits those files. **S2** outcome classification scattered over six sites: fixed in slice 6. **S3** `begin_sync_io` mixes concerns: accepted, #904 deletes part of it. **S4** WAL vocabulary in the storage layer: accepted. **S5** stale docs in `wal.rs:12`: fixed in the next slice that edits the file. |
| O | partially holds | Provider seam open; writer seam closed (`FsWalFactoryIo` hard-coded at `wal.rs:631`, `rotation.rs:314`). **O1** accepted: no hypothesis needs a second writer, and a factory trait would remove no defect. **O2** accepted: leases are the only source of native groups. |
| L | partially holds | Five `WalWriter` implementations; **L1 to L3** defaults diverge from `FsWalWriterIo`: fixed in slice 2 by deleting the defaults. **L4** accepted (single-thread authority install). **L5, L6** gaps: no trait-level `WalAppendError` contract test, doubles skip authority and accounted overrides; covered by slice 2's failing test. |
| I | violated (trait), accepted (actor) | **I1** three test-only methods on `WalWriter`: fixed in slice 2 by deleting them. **I2** `tail`/`seal_facts`/`write_footer` stubs: accepted. **I3** wide `WalActor` surface with one consumer: accepted. |
| D | partially holds; layering rule holds | Production `src/wal/` imports nothing from `runtime/`, `engine/` or `storage/`; the only upward imports are in test modules (`writer_io.rs:1321`, `ledger_chain.rs:6`). **D1** to **D4** concrete `FsWalFactoryIo`, `HybridStorage`, `LeaseValidity`, `WalHistory` in `runtime/`: accepted, single implementation each and the dependencies point down. Semantic leaks without an offending `use` (appendix C, section 7) are recorded in F11. |

## 4. Phase 1: target design

### 4.1 Owners and boundaries

The target keeps today's thread model and sharpens who owns what. Nothing here adds a thread; slice 8c changes what
the existing writer thread does.

| Owner | Owns | May do | Must not do |
|---|---|---|---|
| **Event loop** (`WalActor` and `RuntimeState`) | Sequence allocation, admission, conflict validation, apply order, snapshot publication, acknowledgement, the `WalIoState` machine, fencing decisions | Allocate sequences, enqueue prepared work, apply a group after its durability event, edit the ledger for rotation, seal and shutdown | Wait on a condvar for fsync or append acknowledgement (target), encode or compress frames (target), read SSTs for conflict checks (slice 4) |
| **WAL writer thread** (`WriterRunner`) | Segment file bytes, positional writes, the segment digest, fsync, rollback | Encode the frame (compress, CRC), write, fsync, roll back a partial append, report a typed completion | Touch `RuntimeState`, decide fencing, edit the ledger |
| **Ledger** (`WalHistory`, `LedgerStore`) | Membership, order, reservation, durable floor | Append edits under the writer fence | Be on the per-commit critical path (slice 3): floor edits are amortized, rotation and shutdown edits stay exact |
| **Cloud publication** (uploader, publisher, retirement) | Object PUTs, catalog CAS, retirement proof, delete | Run off the loop (#907), upload concurrently (#916), delete after the catalog retires | Advance the cloud-durable frontier out of order |

Mutable WAL state has one owner: the event loop. Workers return typed outcomes through the existing
`worker_msg_tx` channel, the pattern already used by garbage collection.

### 4.2 State machines

**Append group** (the unit the writer thread sees):

```
Prepared --enqueue--> Enqueued --written--> Written --fsync--> Synced --apply--> Applied --publish--> Acked
    |                     |                    |                  |
    | admission failure   | unchanged error    | any failure       | any failure
    v                     v                    v                  v
NotCommitted          NotCommitted         Fenced + Unknown    Fenced + Unknown
(proof: no bytes)     (proof: rolled back)
```

Today `Enqueued` to `Synced` happens inside one blocking call on the loop. The target releases the loop between
`Enqueued` and `Synced`, bounded by the in-flight limit in section 4.4. Apply order equals group order, always.
Visibility is still atomic per group: `publish_snapshot` runs after the group's apply.

**Rotation and seal** keep the existing step table (`rotation.rs:3-25`): reversible until the footer is written,
irreversible after. The design changes nothing there except that the recovery-time duplicate
(`WalHistory::start_epoch`) shares its footer and ledger step with the runtime path (gap, last paragraph of section 6.4).

**Publication** keeps the in-order rule: publish only a contiguous acknowledged run, `AckSet::withdraw` on any
failure, `If-Absent` PUT with epoch-named keys (appendix C, section 4).

**Recovery** keeps the taxonomy in appendix B, section B, with one rule for the durable floor shared by local and
cloud (slice 1).

**Shutdown**: unchanged ordered steps (appendix B, D.1). Added: a bounded wait for live transactions in the drop
reaper (#933).

### 4.3 Commit outcomes

Three outcomes, defined by what the writer can prove. This table is the contract slice 6 enforces in one function.

| Outcome | Meaning | Proof | Errors that map here today |
|---|---|---|---|
| **Committed** | Applied locally, acknowledged per the write option | Group reached `Acked` | success |
| **NotCommitted** | No WAL byte names this transaction and no memtable write happened | Failure before enqueue, or after a durable rollback | admission `WriteStall`/`NoSpace`/`Fenced`/`WriteConflict`/`ResourceLimit`; sequence exhaustion; unchanged append or sync-submission errors |
| **Unknown** | Bytes may be in the log; replay may apply them | Any failure after enqueue without a rollback proof; any caller-side response timeout; any ledger-edit failure after a successful fsync | `OutcomeUnknown`; caller `Timeout` after `try_send` |

Rules the design keeps: any append, sync, or ledger failure after enqueue fences the actor, and `unchanged` only
chooses the error shown. A loop-reported `Timeout` that is provably unchanged stays a typed `Timeout`.

That last rule cannot be built where the remap lives today. `admitted_commit_timeout_error`
(`engine/api/transaction.rs:1034`) matches on the error **type** and the caller-side wait produces the same type
(`response_timeout_error`, `handle.rs:123`). Slice 6 therefore moves the remap to the one place that creates the
caller-side wait timeout, so a `Timeout` that arrives inside a loop response passes through untouched.

`cloud_strict` post-apply failures stay `OutcomeUnknown` because the data may not be cloud-durable;
`wait_cloud_durable` returns typed errors by design and this difference is documented, not changed.

### 4.4 Deadlines, continuation, and backpressure

| Wait | Deadline | After expiry |
|---|---|---|
| Caller response | `runtime_response_timeout`, 60 s default | Everything continues: the loop still appends, syncs, applies, publishes. Caller sees `OutcomeUnknown`. Unchanged by this design. |
| Append and sync acknowledgement | `storage_io_timeout`, 30 s default | Writer may still complete; later appends are rejected unchanged; actor fences |
| Cloud upload retry | 50 ms doubling to 1 s; waiters see `Timeout` after the third failed attempt | Storage keeps retrying |
| Shutdown | caller timeout; `shutdown_cloud_drain_timeout` for uploads | Terminal result replayed; leaked transaction bounded by #933 |

Backpressure bounds (values in appendix A section d and appendix C section 5) and the target behaviour:

| Bound | Today | Target |
|---|---|---|
| Loop queue, 1000 messages | `WriteStall`, no test | Keep; add the missing test (slice 5) |
| WAL writer queue, 5000 entries | Unreachable (inference, F5) | Replaced by an **in-flight group limit** of 4 (provisional; tuned in slice 8b), enforced at enqueue; reaching it blocks admission of the next group, never drops one |
| Memtable hard limit | Advisory only (`flush.rs:293`) | Keep as a hint; the L0 slot ceiling is the enforced bound. Slice 5 adds a test that states this. |
| Coalesced followers | Skip flush-staging admission (inference, F7) | Slice 5 first proves the bypass red, then gives followers the same admission as the first member |
| CloudAsync lag | Only queue fill and disk | Out of scope here (F8 recorded); #937 measures lag first |
| Group size | count only | Add a byte cap before pipelining (slice 8b), so a group cannot exceed the replay bound |

### 4.5 Constraints the pipeline must satisfy

The review found these by tracing what changes once the loop is released between `Enqueued` and `Synced`. Each is
assigned to a slice. None is in the backlog issues as written.

| # | Constraint | Why | Slice |
|---|---|---|---|
| C1 | The durable floor recorded after an fsync is the **end offset of the group that fsync covered**, not `writer.current_pos()` | Today `current_pos` equals the fsynced length only because the loop blocks per group and is the only appender (`durability.rs:264`, appendix B section C.1). With group k+1 already written when group k's fsync returns, `current_pos` would record bytes that are not durable, and an honest power loss would fail strict recovery with "shorter than its persisted fsync boundary". | 3 (interface), 8b (enforced) |
| C2 | Snapshots publish the **highest applied sequence**, not `state.sequence` | `state.sequence` means "allocated" and is advanced at prepare (`state.rs:1302`); `publish_snapshot` publishes it (`event_loop/mod.rs:640`). With groups in flight a snapshot would expose a sequence ahead of the applied data. | 8a |
| C3 | `WriterEffectGroup` roots are lexical and thread-bound; they are parked in loop-owned state while a group is in flight, and rotation, seal, lease transition and shutdown first **drain the pipeline** | A transition waits for every root to drain on the same thread (`writer_effect_groups.rs`, appendix A finding 2.1 row 6); today the root leaves before apply or a lease transition (`transaction.rs:313`) | 8a |
| C4 | The spilled-transaction path (`spill.rs`) is a **barrier**: it waits for in-flight groups, then appends and syncs as today | It is a second, ungrouped append and sync owner (appendix A, transition 5 notes) and has no ordering against in-flight groups | 8a |
| C5 | A failed group does not need to return its sequences | `release_sequences` returns only the latest allocation (`state.rs:1317`); with several groups prepared an earlier group's range cannot be returned. This is harmless only because every post-enqueue failure fences. The rule is stated so a later change that stops fencing on `unchanged` knows it must revisit it. | 8b |
| C6 | The admission-closed behaviour of a pending lease transition has a test before the pipeline relies on it | 870-bound has no pinning test (section 6.2) | 5 |

## 5. The #904 decision

**Decision: accept, modified.**

- **Rejected:** requirement R1 as written (derive the durable prefix from the last valid frame). That removes the
  persisted evidence, which is the `durable_bytes = None` classification: any CRC failure then fails strict
  recovery, and `should_open_strictly_when_power_loss_tears_unsynced_multi_page_tail`
  (`storage_root_injection.rs:811`) regresses, violating #904's own R4. The segment alone cannot tell "never
  acknowledged" from "acknowledged then damaged".
- **Rejected:** requirement R3's motivation. The full-file comparison never runs under the writer fence, which is
  every production configuration.
- **Accepted:** drop the per-group `SyncOpen` fsync by recording the floor on an amortized schedule: one
  `SyncOpen` when 1 MiB of acknowledged bytes have accumulated or 100 ms have passed since the last one
  (provisional, tuned by the slice's benchmark), **plus an exact edit at rotation, clean shutdown and salvage**.
  `N = 1` reproduces today. The time bound needs a timer; the loop already has a sync deadline for Batched mode
  (`sync_deadline_timeout`, `durability.rs:436`) that the slice reuses. If it proves unsuitable the bound is bytes
  only, and an idle tail above the floor stays tolerated until the next edit or shutdown; the contract below says
  so either way.
- **Contract after the change:** lost-write detection for the open segment is exact for bytes older than the last
  floor edit. Loss or damage between the floor and the true acknowledged end is classified as a torn tail:
  strict recovery truncates it and opens normally. Sealed segments are unaffected; their footers carry exact length
  and digest. CloudAsync already runs with a header-only floor, so this is not a new class of behaviour.
- **What the window costs:** an intact, valid tail past the floor is replayed in full; nothing acknowledged and
  valid is dropped. A CRC fault anywhere in `[floor, acknowledged end)` is salvageable and the verified prefix
  reaches the floor, so recovery truncates that frame **and every valid acknowledged frame after it**. The loss is
  bounded by the window, 1 MiB or 100 ms of commits. The check exists for operator or filesystem truncation, a
  lying fsync, media damage and engine bugs. It does not matter for honest power loss or a process crash.
- **Tests this changes:** `should_fail_strict_recovery_without_truncating_when_corrupt_length_hides_valid_suffix`
  (`streaming_wal_plan/tests.rs:1110`) records the full length as the floor and keeps pinning the exact case; a
  sibling with a lower floor pins the window and expects the truncation described above.
- **Needed first:** slice 1 (one floor rule, tested below the floor), a counter that includes ledger fsyncs
  (`wal_fsync_count` today counts the writer thread only), and a new restart test for the window edge: truncate at
  `floor - 1` fails strict, truncate within `[floor, acknowledged end)` opens and drops the tail. The failpoint
  `midge::wal::after_fsync_before_durable_frontier` (`durability.rs:331`) sits exactly between the WAL fsync and
  the floor edit and no test configures it today. Constraint C1 applies once the loop stops blocking.
- **Fallback if rejected at review:** keep per-group exactness and instead remove the other costs the review
  found (the O(members) clone per append, a held LEDGER file handle, `fdatasync` for the ledger). These do not
  remove the second fsync.

## 6. Phase 2: hypotheses, preservation, format, slices

### 6.1 Verdicts on the backlog

| Issue | Verdict | Replacement |
|---|---|---|
| #904 LEDGER edit per fsync | Partly confirmed, **modify** | Section 5, slice 3 |
| #905 overlapping writes join the group | Partly confirmed, **widen R2** | Needs a staged-write overlay (slice 4) that serves assertions, write-write conflict checks (`AbortOnWriteConflict`) and `insert_only`; the joins are slice 8d. `LastWriteWins` blind writes can join without it. |
| #906 concurrent apply with watermark | Confirmed, **larger than stated** | Apply also mutates non-concurrent state: per-op `recompute_total_memtable_bytes`, `record_delete_range`, `active_memtable_started_*`, partial-apply marking. Slice 7 makes those per-group first; slice 9 is the watermark. |
| #907 cloud mirror, seal, ack off the loop | Confirmed | Slice 11a. Correction: WAL catalog publication is serialized by `wal_catalog_mutation` inside `HybridStorage`, not by `ManifestPublicationGate` |
| #913 metadata generation GC | Not contradicted | Added constraint: never delete the generation named by the catalog's `retired_by` |
| #915 server-side checksums, multipart | Partly refuted | `x-amz-checksum-crc32c` already exists (`s3.rs:851`); multipart is absent and mainly an SST concern |
| #916 parallel uploads, bounded catalog | Partly stale | Slice 11b. Retired entries already dropped, one CAS per contiguous run, receipts skip readback. Remaining: parallel uploads, time-based batching |
| #924 per-commit overhead | Confirmed | Slice 7: per-family publish plus per-group bookkeeping (F8) |
| #933 bounded reaper | Confirmed | Independent of the slices. Citation fix: waiting is `lifecycle.rs:180`, not `:109` |
| #940 fsync completion as an event | Mechanics confirmed, **four missing prerequisites** | Slices 8a and 8b: unify the strict and ticketed sync protocols; park the `!Send` `WriterEffectGroup` root; staged-write view (slice 4); writer-side batching of sync requests is unexercised today |
| #941 encode and compress off the loop | **R1 refuted** | Sequences are inside the compressed payload. Viable: move frame encode, compress and CRC to the writer thread (slice 8c). Per-op compression would be a format change and is rejected. |
| #943 durable delete intents | Confirmed | Slice 11c. For WAL, the catalog `anchor.id` is already a durable lower bound; WAL orphans at or below it can be swept without a new journal |
| #946 conflict checks off SSTs | Confirmed | Slice 4. Only `AbortOnWriteConflict`, assertions and `insert_only` pay it |
| #947 build once, vectored writes | Narrowed | The single-entry queue leaves nothing to gather; drop `writev`, keep "frame once" inside slice 8c |

### 6.2 Preservation map (R4)

Every #808 contract and every #869, #870 and #872 regression maps to an owner and a test, or to a named gap. The
rows are in appendix D, section 2 (94 rows by count). The WAL-relevant entries of
`docs/development/invariant-catalog.md` are mapped there too: AUTH-1 to 4, TX-4, WAL-1 to 7, CLOUD-1 to 3 and 5 to 8,
RES-2, 3, 5 and 8, PROG-1 and FORMAT-1.

**Selection, stated so it is not mistaken for completeness:** appendix D skipped catalog entries it judged to be
about SST, compaction, memtable, backup, accounting or iteration only (TX-1, 2, 3, 5 to 8; SST-1 to 10; CLOUD-4;
LIFE-1, 2; BACKUP-1 to 5; RES-1, 6, 7; ACCT-1 to 6; FORMAT-2 to 5; PROG-2). Their disposition is "not WAL, out of
scope for this issue". The selection was made by one reader and has not been independently checked, so AC4 is
**partly met**: every mapped contract has an owner and a test or a named gap, but "every catalog entry" is a
claim about the selection, not about all entries. Slice 10 re-checks the skipped list when it refreshes the
evidence file.

The gaps:

| Gap | What is missing | Closed by |
|---|---|---|
| 808-FMT-SEG | No test rejects a wrong segment-header version or record version | slice 10 |
| 808-821e | No test runs the same history through the local and cloud recovery entry points | slice 1 |
| 808-822d, AUTH-3 | No WAL-specific test that mutable state stays on the loop; the cited governance test does not exist | slice 10 |
| 870-bound | No test that a pending transition closes new admission | **slice 5** (constraint C6; moved up from 10) |
| AUTH-2 (partial) | No read-availability-after-fence matrix | slice 10 |
| CLOUD-8 | Missing from `invariant-evidence.json` (the test exists) | slice 10 |
| PROG-1 | The cited recovery-progress tests do not exist | slice 10 |
| L5, L6 | Doubles skip authority and accounted overrides; no trait-level contract test | slice 2 |
| Transition-level gaps | Listed in bold in section 2.1 and in appendices A to C | the slice that touches each transition |

### 6.3 Format decision (R5)

No byte layout of the segment header, frame, record, transaction batch, footer, ledger, or catalog changes in any
slice, and no persisted version constant moves.

| Change | Persisted bytes | Action |
|---|---|---|
| #904 floor semantics | `SyncOpen` keeps its shape. Only its cadence changes. | **No version bump.** A floor at or below the true acknowledged end is accepted by the current readers (`stop_at`, `probe_open_member`, the planner clamp only compare against it; `sync_open` requires a monotone raise), and a journal written with exact floors is a valid floor journal. A bump would reject journals both versions can read. Update `format-compatibility.md` and `wal-redesign-808.md` to describe the floor semantics. |
| #941 corrected (writer-thread encoding) | none; output bytes identical | Byte-for-byte golden against the legacy encoder |
| #947 (frame once) | none | same golden |
| #916 | none unless the catalog schema changes or the mirror becomes optional | Catalog `FORMAT_VERSION` 3 to 4 only in that case, with its rejection test |
| Slice 1 (one floor rule) | none | none |

### 6.4 Slices

Ordered. Each names its behaviour change, the failing test written first, crash points where relevant, local and
cloud parity, and the existing benchmark that gates it. Per the 0.5.0 tracking rules (#935), every implementation
issue gets a numeric target comment before its branch is created; the benchmark column names the row that target
is measured on. Performance slices carry no `flea` or `solid` label because they fix no defect and refactor no
principle; labels follow `AGENTS.md`.

| # | Slice | Labels | Behaviour change | Failing test first | Crash points and parity | Benchmark gate |
|---|---|---|---|---|---|---|
| 1 | **One durable-floor rule** (#974) | `flea`, p1, `area:wal` | Local and cloud share one function for tolerate, lost-write and salvage-below-floor; local salvage of a file shorter than the floor keeps the verified prefix | `should_keep_verified_prefix_given_salvage_when_local_file_is_shorter_than_the_floor`; a parity test running one history through both entry points (closes 808-821e) | `stop_at` branch 2 gets a unit test with a real `durable_bytes`; no new crash point; this slice is the local/cloud parity work | none (recovery only) |
| 2 | **Writer contract** (#975) | `solid`, p3, `area:wal` | Delete `append_record`, `append_batch`, `sync` from `WalWriter`; remove the divergent defaults; update the four doubles | trait-level contract test: every impl returns `unchanged: true` only with proof | local and cloud share the trait; no crash point | none |
| 3 | **#904 modified** (#904) | p2, `area:wal` | Amortized floor edits, exact at rotation, shutdown, salvage; the floor is passed as an explicit end offset (C1) | ledger fsyncs counted; window-edge restart test (section 5); damaged-frame-in-window truncation test | failpoint `midge::wal::after_fsync_before_durable_frontier`; cloud unaffected (CloudAsync floor already header-only) | `tier2_subsystem_durability_commit_latency`: ledger fsyncs per strict group from 1 to under 1 |
| 4 | **Staged-write overlay and #946** (#946) | p2, `area:wal` | Validation (assertions, write-write conflicts, `insert_only`) sees staged and in-flight groups; conflict checks stop reading SSTs on the loop | `should_open_no_sst_on_event_loop_given_conflict_checked_commit_when_submitted`; overlay equals memtable-after-apply (proptest) | a flush between begin and commit; parity: cloud ephemeral-cache mode runs the same suite | `tier1_hotpath_api`, `tier4_system_strict_group_commit` |
| 5 | **Admission parity and missing pins** (#976) | `flea`, p2, `area:wal` | Coalesced followers get the same flush-staging admission; add the missing tests: queue-full `WriteStall`, post-submission sync `Timeout`, memtable hard limit stated, **870-bound (pending transition closes new admission)** | first `should_apply_flush_staging_admission_to_coalesced_followers_given_ephemeral_cache` is written to **prove the bypass red** against current code, then the fix | cloud ephemeral-cache mode only for the follower fix; the pins are local | none |
| 6 | **One commit-outcome function** (#977) | `solid`, p2, `area:wal` | `WalAppendError`, group durability and ledger failures map through one function per section 4.3; the Timeout remap moves to the caller-side wait; engine remap tested through `RuntimeHandle` | `should_keep_timeout_given_provably_unchanged_failure_when_commit_is_submitted` | failpoint `midge::wal::txn_after_commit_append_before_sync`; local and cloud strict map the same | none |
| 7 | **Apply bookkeeping per group** (#924, part of #906) | p2, `area:wal` | `recompute_total_memtable_bytes` once per group; snapshot publication per touched family | `should_build_one_read_snapshot_given_many_families_when_commit_touches_one` | none; local and cloud share the path | `tier1_hotpath_api`, `tier4_system_local_throughput_regression` |
| 8a | **Pipeline prerequisites** (#978) | p1, `area:wal` | Unify strict and ticketed sync protocols; park the `WriterEffectGroup` root in loop-owned state; drain before rotate, seal, lease transition and shutdown; snapshots publish the applied sequence; spill path is a barrier (C2 to C4) | `should_publish_applied_sequence_given_failed_group_when_snapshot_published`; `should_drain_in_flight_group_before_rotate_given_pending_group` | the local and cloud rotation matrices run again with a group parked; parity: cloud seal drains too | `tier4_system_strict_group_commit` (no regression) |
| 8b | **Pipelined commit** (#940) | p1, `area:wal` | Append and fsync completion become loop events; in-flight limit and group byte cap; floor is the fsynced group's end (C1, C5) | held-fsync read/control progress and ordered later-write completion (#940 AC1); `should_open_strictly_given_group_written_after_synced_group_when_power_is_lost`; a failed fsync still yields `Fenced` and `OutcomeUnknown` | failpoints at append, sync, ledger floor, apply; local and cloud strict pass one suite | `tier4_system_strict_group_commit` (scales to the fsync ceiling), `tier2_subsystem_durability_commit_latency` (read p99 decoupled) |
| 8c | **Writer-thread encoding** (#941 corrected, #947 reduced) | p2, `area:wal` | Frame encode, compress and CRC move to the writer thread; "frame once" | byte-for-byte golden against the legacy encoder; old decoder reads new-path bytes | the append failpoints; local and cloud share the encoder | `tier2_subsystem_durability_commit_latency` |
| 8d | **Overlapping writes join groups** (#905 widened) | p2, `area:wal` | Overlapping keys join a group when no assertions, `start_sequence` or `insert_only`; assertions and write-write conflicts are checked against the overlay inside the group | `should_coalesce_overlapping_strict_writes_given_no_assertions_when_same_window`; the existing `insert_only` rejection still rejects inside a group | failpoint `midge::wal::before_prepared_transaction_apply`; local and cloud strict | `tier4_system_strict_group_commit` |
| 9 | **Concurrent apply with watermark** (#906) | p1, `area:wal` | Memtable apply on workers, a visibility watermark, the loop allocates and acknowledges | `should_not_expose_sequence_after_watermark_given_earlier_apply_stalled_when_read` | failpoint `midge::wal::before_prepared_transaction_apply`; loom (#931) and the differential fuzzer (#951), both merged, run before merge | `tier4_system_strict_group_commit` |
| 10 | **Test and evidence debt** (#979) | p3, `area:testing` | Segment and record version rejection, event-loop ownership pin, read-after-fence matrix, refresh `invariant-evidence.json` and re-check the skipped catalog list | the tests themselves, each shown red against a mutated source line | none; local and cloud share them | none |
| 11a | **Seal, ack and catalog publication off the loop** (#907) | p2, `area:wal` | These three move to a worker; the loop handles completion events and re-checks the epoch | `should_complete_local_get_under_100ms_given_1s_mirror_delay_when_flush_publishes` | catalog contiguity campaigns unchanged; needs the #937 simulated hybrid rows (merged); the Sqrzl rows of #937 stay open and are not a prerequisite | the #937 hybrid rows |
| 11b | **Parallel uploads and time-based batching** (#916 narrowed) | p2, `area:wal` | Bounded upload pool; catalog write per interval; a lease failure at ack fences the protocol (F11) | `should_never_publish_segment_n_plus_1_before_n_given_4_concurrent_uploads` | publication in order under every completion order; parity with the #937 hybrid rows | the #937 hybrid rows |
| 11c | **Durable remote deletes** (#943, WAL part) | p2, `area:wal` | Pending remote WAL deletes survive a crash; objects at or below `anchor.id` are swept after the lease and identity checks | `should_delete_orphan_wal_object_given_crash_after_catalog_retirement_before_delete` | a new failpoint between the retiring catalog CAS and the delete, named in the slice | none |

Order and dependencies: 1 before 3; 3, 4, 5, 6, 7 and 8a before 8b; 8b before 8c and 8d; 4 before 8d; 7 before 9;
8b before 9; 11a before 11b. Slices 1, 2, 5, 6, 7 and 10 are independent of the pipeline and can ship first.

Not sliced, recorded as gaps with reasons: S1 (MVCC split), O1 (writer factory), the seven-site retirement
decision (F11), the duplicate rotation owner (`start_epoch`), local shutdown sealing, the time-based CloudAsync lag
bound (F8). Each is a candidate for its own issue if a later slice's evidence needs it; none is required for the
pipeline.

## 7. Open gaps and limits of this review

- **State machines (R3, AC3):** only the append group has a diagram (section 4.2). Rotation, seal, publication,
  recovery and shutdown are described as unchanged, with their current step tables in the appendices. This is a
  stated gap, not an omission: the design changes none of them.
- **Preservation map (R4, AC4):** partly met, see section 6.2. The skipped invariant-catalog entries were judged by
  one reader.
- **Follow-up issues (AC6):** every slice has an issue. Existing backlog issues carry the slice (#904, #946, #924, #940,
  #941, #947, #905, #906, #907, #916, #943) and received a comment with the verdict in section 6.1; new issues are
  #974 (slice 1), #975 (2), #976 (5), #977 (6), #978 (8a) and #979 (10).
- **Citations (AC2):** every row was produced by reading the cited line. When this document was assembled, fifteen
  citations from the main-body tables were re-read against the source and all matched; the independent review
  re-read eleven more and found no mismatch. A few test line numbers in appendices C and D came from `rg` only and
  are marked there.
- **Not read:** `memtable/skiplist.rs`, `ReadSnapshot::latest_state_sequence` internals,
  `HybridStorage::ensure_wal_write_admission`, `TransactionMemoryPool`, SST publication code beyond one line, and the
  recovery code behind the doc-level claim that recovery re-verifies `retired_by`.
- **Inferences, not measurements:** the unreachable writer queue (F5) and the follower admission skip (F7) come from
  call sites. Slice 5 proves the second red before fixing it; slice 8b measures the first.
- **Provisional numbers:** the floor window (1 MiB or 100 ms), the in-flight limit (4) and the group byte cap are
  starting points that each slice replaces with measurements on its own benchmark.
- `docs/development/wal-redesign-808.md` still says the current format is 8 and does not mention `SyncOpen`,
  `SalvageOpen` or `durable_bytes`; slice 3 updates it.

---

# Appendices: research passes

Each appendix is the unedited output of one research pass. Path abbreviations are defined at the top of each.
Headings are demoted two levels. Appendix D uses the placeholders `[SLICE-WRITER-CONTRACT]` and `[SLICE-OUTCOME]`;
they are slices 2 and 6 in section 6.4. Appendix B recommends #904 "MODIFY"; section 5 adopts it.


## Appendix A: Write path (admit through memtable publish)

### #873 Phase 0 current-state map: the WRITE PATH

Scope: admit, sequence, frame, enqueue, append, sync, acknowledge, memtable publish.
Mode traced: local, `DurabilityPolicy::Strict`, filesystem WAL, with and without a lease-backed
`GroupedWalWriter`. Cloud/CloudAsync/BestEffort/Batched/spilled variants are noted where they differ.
Branch `develop` @ 12962819. Read-only; nothing in the repo was touched.

#### Path abbreviations

| Short | Path |
|---|---|
| `txn.rs` | `src/runtime/actors/wal/transaction.rs` |
| `tstate.rs` | `src/runtime/actors/wal/transaction_state.rs` |
| `dur.rs` | `src/runtime/actors/wal/durability.rs` |
| `wal.rs (actor)` | `src/runtime/actors/wal.rs` |
| `spill.rs` | `src/runtime/actors/wal/spill.rs` |
| `wb.rs` | `src/runtime/event_loop/write_batch.rs` |
| `wb_tests.rs` | `src/runtime/event_loop/write_batch/tests.rs` |
| `el_wal.rs` | `src/runtime/event_loop/wal.rs` |
| `dsync.rs` | `src/runtime/event_loop/durability_sync.rs` |
| `handle.rs` | `src/runtime/handle.rs` |
| `etx.rs` | `src/engine/api/transaction.rs` |
| `wio.rs` | `src/wal/fs/writer_io.rs` |
| `wrun.rs` | `src/wal/fs/writer_runner.rs` |
| `enc.rs` | `src/wal/encoding.rs` |
| `frame.rs` | `src/wal/frame.rs` |
| `awt.rs` | `src/runtime/actors/wal/tests.rs` |

#### Extra files read (outside the starting list)

`src/runtime/event_loop/{wal.rs, durability_sync.rs, dispatch.rs, scheduler.rs, mod.rs, groups.rs, control.rs, cloud_memtable_admission.rs, cloud_integration/sealing.rs}`,
`src/runtime/{actors/wal.rs, state.rs, state/flush.rs, router.rs, runtime.rs, wal_history.rs}`,
`src/runtime/tests.rs`, `src/runtime/wal_history/tests/fenced_ledger.rs`,
`src/engine/ingest.rs`, `src/engine/startup/storage.rs`, `src/config.rs`, `src/codec.rs`,
`src/lease/{writer_effect_groups.rs, traits.rs}`, `src/wal/{ledger_store.rs, sequence.rs, format/segment.rs, recovery/streaming.rs}`,
`src/wal/fs/writer_io/tests/writer_groups.rs`, `src/storage/hybrid/backend/reservations.rs`,
test files `src/runtime/event_loop/{write_batch/tests.rs, tests.rs}`, `tests/{durability.rs, fault_injection.rs}` (heads only).
Not read: `memtable/skiplist.rs`, `ReadSnapshot::latest_state_sequence` internals, `HybridStorage::ensure_wal_write_admission`,
`TransactionMemoryPool`, config defaults for `l0_compaction_trigger` / `max_immutable_memtables`.

---

#### Thread model (used by every row)

* **Caller thread** = application thread inside `Transaction::commit_with_sequence` (`etx.rs:646`).
* **Event loop** = the single `midge-runtime` thread (`src/runtime/runtime.rs:164-167`). It owns `RuntimeState`, the `WalActor`
  (`event_loop.actors.wal_actor`, e.g. `wb.rs:472`), the memtables, and the response router.
* **WAL writer worker** = one `midge-wal-writer` thread per `FsWalWriterIo` (`wio.rs:216-218`), running `WriterRunner::run` (`wrun.rs:188`).
* The `WalActor` is **not** a thread. It is a struct the event loop calls synchronously, so "WAL actor" code runs on the event-loop thread.
  `FsWalWriterIo` methods are also called synchronously from the loop. Only `WriterRunner` runs elsewhere.

---

#### Table: the 8 write-path transitions

Legend for the failure column: "UNCHANGED" = `WalAppendError { unchanged: true }`, meaning the writer can prove no bytes remain (`traits.rs:10-19`; set by
`WalAppendError::unchanged` `traits.rs:29-34`). "UNKNOWN" = `unchanged: false`, meaning bytes may be in the file (`traits.rs:22-27`).

##### 1. admit

| Field | Finding |
|---|---|
| Owner | **No single owner; four layers.** (1) Caller thread: `IngestCoordinator::check_write_stall` (`src/engine/ingest.rs:77-88`, advisory flag from the previous response, probe round-trip `handle.rs:749-785`) and `RuntimeHandle::send_apply_transaction_and_wait` queue gate `try_send` (`handle.rs:633-651`, `map_submission_error` `handle.rs:84-107`). (2) Event loop, dispatch path: `WalCoordinator::accept_write` (`el_wal.rs:445-463`), `ensure_l0_write_admission` (`wb.rs:120-140`), `prepare_cloud_transaction_memtables` (`el_wal.rs:142`, defined `cloud_memtable_admission.rs:20-32`). (3) Event loop, drain/coalesce path: `admit_drained_transaction` = lease health + L0 only (`wb.rs:593-599`). (4) `WalActor::prepare_transaction_append` (`txn.rs:86-162`: durability availability `:107-108`, family checks `:110-125`, conflict/assertion/insert-only `:126-133`, replay bound `:619`) and `admit_wal_records` byte budget at append time (`wal.rs (actor):712-733`). |
| Effects | Caller thread: atomic flag read, optional probe message, `router.register` + `try_send`. Event loop: reads `RuntimeState`; **may do SST reads** via `ValidationSnapshots` (`tstate.rs:23-56`, `:340-373`: clones `manifest.files`, builds a `ReadSnapshot`); in cloud ephemeral-cache mode those reads go to a remote SST store on the loop (test `awt.rs:127-189` builds `RemoteSstFs` with a 5 s timeout). Cloud admission can freeze a memtable and set flush headroom (`cloud_memtable_admission.rs:86-96`). No sequence or WAL mutation yet. |
| Caller-visible failure | Always **definitely not applied, not logged**. `WriteStall("runtime request queue is full")` (`handle.rs:100`); `WriteStall("Memory budget exceeded for CF n")` (`ingest.rs:80-83`; a probe timeout counts as stalled, `handle.rs:783`); `WriteStall("column family n has no free L0 slot (u/c)")` (`wb.rs:132-136`); `Fenced` (lease, `el_wal.rs:446-449`); `WriteStall` for CloudAsync WAL admission (`sealing.rs:379-385`); `NoSpace` (cloud flush-staging window `cloud_memtable_admission.rs:59-63`, or hybrid local-WAL byte budget `reservations.rs:71-80`, raised un-fenced at `txn.rs:664`); `WriteConflict`/`InvalidArgument`/`ResourceLimit`/`RecoveryFailed` from preconditions. `Timeout` cannot occur here: the caller's response wait only starts after `try_send` succeeds (`handle.rs:654-659`). |
| Pinning test | L0: `should_stall_before_wal_when_transaction_would_exceed_hard_l0_ceiling` (`wb_tests.rs:2272`). Byte budget: `should_reject_wal_append_before_disk_growth_when_ephemeral_budget_is_exhausted` (`awt.rs:32`). Assertion vs staged writer: `should_reject_assertion_when_earlier_batch_member_writes_the_asserted_key` (`wb_tests.rs:950`). Remote-SST validation: `should_check_transaction_assertions_against_remote_sst_when_local_cache_is_empty` (`awt.rs:127`). **Queue-full `WriteStall` for `ApplyTransaction`: none.** (`src/runtime/tests.rs:746` fills the queue but tests snapshot-release, not admission.) **Drain path skipping cloud-memtable admission: none.** |
| Notes / findings | **Finding: admission differs by entry route.** Only the first transaction of a wake goes through `accept_write` + `prepare_cloud_transaction_memtables` (`el_wal.rs:132-145`). Followers pulled by `collect_coalesced_transaction_msg` or `drain_pending_writes` get only `admit_drained_transaction` (`wb.rs:565`, `:593-599`). `prepare_cloud_transaction_memtables` has a single non-test call site (`el_wal.rs:142`) and is a no-op unless the ephemeral SST cache is on (`cloud_memtable_admission.rs:24-26`). So in ephemeral-cache mode with Strict/Batched, coalesced followers appear to skip the flush-staging-window `NoSpace`/`WriteStall` admission. (`accept_write`'s CloudAsync check is moot: `drain_pending_writes` returns 0 for CloudAsync, `wb.rs:177-179`.) Verified by call-site reading only; no test. |

##### 2. sequence

| Field | Finding |
|---|---|
| Owner | **Straddles actor and state, and persists through the ledger.** `WalActor::allocate_transaction_sequences` (`txn.rs:505-539`) calls `RuntimeState::allocate_sequences` (`src/runtime/state.rs:1274-1312`), which owns `state.sequence`, and which calls `state.wal.history.reserve(through)` (`state.rs:1286-1297`), i.e. a **ledger `Reserve` edit + fsync**. Txn id: `RuntimeState::next_txn_id` (`state.rs:1390-1396`). Give-back: `release_sequences` (`state.rs:1317-1321`), called from `txn.rs:142-147`, `:151-154`, `:526-529`. |
| Effects | Event-loop thread. A txn of N ops consumes N+2 sequences: begin marker, ops, commit marker (`txn.rs:514-523`). Sequences are handed out in prepare order, so group order = sequence order. Advances `state.sequence` **at prepare time, before any WAL byte or memtable write** (`state.rs:1302`). Reservation refill is one ledger append+fsync per `RESERVATION_BLOCK = 1<<20` when less than half a block remains (`src/wal/sequence.rs:14`, `:46-52`), so it is amortised, not per commit. |
| Caller-visible failure | `ResourceLimit("sequence space exhausted")` (`state.rs:1282`, `:1308`), `ResourceLimit("transaction ID space exhausted")` (`state.rs:1391-1393`), or the ledger error if the reservation cannot cover the allocation (`state.rs:1288-1290`). All before any WAL byte names a sequence: definitely not applied. If a later prepare step fails, the range is released only when it is the latest allocation (`state.rs:1318`). |
| Pinning test | `should_reject_transaction_without_mutation_when_sequence_space_is_exhausted` (`awt.rs:226`); sequences released on rejected batch: `should_reject_resident_transaction_when_batch_payload_exceeds_wal_decode_limit` (`awt.rs:2872`, asserts `state.sequence == sequence_before` at `:2916-2919`); reservation math: `should_require_reservation_when_allocation_passes_reserved_through` (`src/wal/sequence.rs:66`), `should_reserve_once_for_a_transaction_larger_than_the_remaining_block` (`:95`); ledger repair after failed reserve/sync edit: `should_preserve_acknowledged_reservation_when_fenced_append_is_repaired_after_sync_failure` (`fenced_ledger.rs:64`). |
| Notes | **Finding: sequence allocation has no abstraction of its own**; it is a method on `RuntimeState` that reaches into the WAL ledger. `state.sequence` therefore means "allocated", not "visible" (see transition 8). After a failed group, `state.sequence` stays advanced (test `awt.rs:852-877` asserts it is unchanged *after* the failure), which is why a later `publish_snapshot` would publish a sequence ahead of applied data; harmless only because the WAL is fenced. |

##### 3. frame (encode, compress, CRC)

| Field | Finding |
|---|---|
| Owner | **Two encode passes, two owners, two layers, same thread.** Pass 1, runtime actor: `build_transaction_batch_record` (`txn.rs:570-621`) → `enc::encode_txn_batch_payload_records` (`enc.rs:635-728`) builds the nested op payload (values **raw**, test comment `enc.rs:1502-1503`). Pass 2, WAL fs writer: `FsWalWriterIo::encode_record_frame` (`wio.rs:225-235`) → `enc::encode_into` (`enc.rs:279-286`, `encode_into_inner` `:288-335`) wraps the payload as the outer `TxnBatch` record's VALUE and **LZ4-compresses the whole payload when it is at least `MIN_COMPRESSION_INPUT_BYTES` = 256** (`enc.rs:310-315`, `src/codec.rs:78`), then `frame::append_frame_encoded` (`frame.rs:69-92`) computes **CRC32C** over the compressed payload (`frame.rs:43`). Separately the writer thread folds every written byte into an **XXH3** segment digest (`wrun.rs:380-386`, `src/wal/format/segment.rs:231-251`). |
| Effects | CPU and allocation only. Alloc: nested `Vec` (`enc.rs:660`) → `Bytes` (`:727`); pooled frame buffer (`wio.rs:306-309`, `:566-568`); LZ4 output `Vec` (`enc.rs:313`); `WalRecord.clone()` of `Bytes` handles (`txn.rs:635`, cheap). Pass 1 + pass 2 + CRC run on the **event loop**; the XXH3 digest runs on the **writer thread but inside the append-ack wait** (before the ack is sent at `wrun.rs:470-471`), so it is on the loop's blocked critical path. |
| Caller-visible failure | At prepare: `Corruption("failed to encode transaction batch")` (`txn.rs:591-596`), `ResourceLimit` for payload above `WAL_MAX_VALUE_LEN` = 64 MiB or above `max_replayable_txn_bytes` (`txn.rs:597-619`, `frame.rs:9`); sequences released (`txn.rs:142-147`); definitely not applied. At frame time (inside append): `WalAppendError::unchanged(err)` (`wio.rs:479-480`, `:573-575`); the actor still **fences** on it (`txn.rs:685`) but returns the raw error (`txn.rs:688-689`). |
| Pinning test | Round trips: `should_roundtrip_transaction_batch_payload` (`enc.rs:1338`), `should_roundtrip_txn_batch_outer_record_carrying_compression_tag` (`enc.rs:1501`); frame layout: `should_match_append_frame_when_payload_is_encoded_in_place` (`frame.rs:179`), `should_detect_crc_mismatch_after_payload_corruption` (`frame.rs:199`); writer path = payload-then-frame: `should_encode_record_frame_identically_to_payload_then_frame` (`wio.rs:1179`); limit before write: `awt.rs:2872`. |
| Notes | **Finding:** because the nested payload (which contains the per-op sequence numbers, `enc.rs:680-702`) is the unit that gets compressed, a "sequence patch after off-loop compression" (#941 R1) is impossible without changing the format. See verdict #941. |

##### 4. enqueue

| Field | Finding |
|---|---|
| Owner | `FsWalWriterIo::enqueue_in_group` (`wio.rs:333-441`), invoked by `WalActor::append_prepared_transaction_batches` (`txn.rs:623-711`, call `:671-681`). Tail-ordering admission first: `admit_records` (`wio.rs:240-259`). Authority admission: `PendingWriterEffect::admit` (`wio.rs:344-345`). **Enqueue and append-acknowledge are one function with one blocking wait**; they cannot be separated or pipelined today. |
| Effects | **Event-loop thread** locks `queue` (`parking_lot::Mutex<Vec<QueuedWrite>>`), pushes **one** `QueuedWrite` per `append_batch` call (i.e. one per group, `wio.rs:566-576`), `condvar.notify_one` (`wio.rs:370-373`), then **blocks on `ack_rx.recv_timeout`** (`wio.rs:383-388`) until the writer thread acks. If the queue holds `MAX_QUEUE_DEPTH` = 5000 entries it sleeps with exponential backoff 1 ms doubling to 100 ms for up to `MAX_WAIT_ATTEMPTS` = 50 attempts (`wio.rs:35-36`, `:349-426`, `wrun.rs:120`), about 4.4 s total. |
| Caller-visible failure | Pre-queue failures are UNCHANGED: sticky writer failure (`wio.rs:339-341`, `:352-354`, `:359-361`), native-admission deadline `Timeout("WAL append deadline expired before queue submission …")` (`wio.rs:443-453`), queue full `WriteStall("WAL queue full after 50 attempts …")` (`wio.rs:432-440`). Post-queue ack timeout is UNKNOWN: `Timeout("WAL append acknowledgement timed out after …")`, and it **poisons the writer** (`write_failed` sticky, `wio.rs:390-403`). Worker gone before ack: UNKNOWN `Internal` (`wio.rs:405-412`). |
| Pinning test | Ack timeout is ambiguous and poisons: `should_fence_writer_when_append_acknowledgement_times_out` (`wio.rs:1043`), `should_prove_subsequent_append_was_not_enqueued_after_ambiguous_timeout` (`wio.rs:1062`); configured timeout honored: `should_apply_configured_timeout_when_append_waits_for_acknowledgement` (`wio.rs:1091`); pre-submission deadline is UNCHANGED and non-poisoning: `should_reject_native_append_unchanged_when_budget_expires_before_queue_submission` (`writer_groups.rs:285`). **Queue-full `WriteStall`: none.** |
| Notes | **Finding: the 5000-deep queue and the `WriteStall` at `wio.rs:432-440` appear unreachable on the current topology.** The event loop is the only producer (appends are issued only through `WalActor`, and rotation's `write_footer`, `wio.rs:515-528`, also runs on the loop), and it blocks on each ack, so the queue never holds more than one entry from it. This is inference from code; not measured. |

##### 5. append (physical write)

| Field | Finding |
|---|---|
| Owner | **Straddles the actor and the runner.** Physical: `WriterRunner::{run, wait_for_batch_or_pending_sync, write_batch, append_with_effects, rollback_partial_append, fail_writes}` (`wrun.rs:188-307`, `:344-441`, `:640-655`). Outcome and accounting: `WalActor::append_prepared_transaction_batches` post-ack section (`txn.rs:682-710`) plus `settle_wal_append` / `settle_failed_wal_append` (`wal.rs (actor):735-763`). |
| Effects | **Writer thread**: takes the leading run of queued writes that share one authority (`wrun.rs:239-255`), `coalesce_batch` (single entry is moved, multiple entries are concatenated, `wrun.rs:309-320`), activates each effect (`:280-293`), `write_at(start_pos, …)` under the writer-effect fence `run_with_effects` (`:361-373`, `:443-454`), XXH3 digest update (`:380-386`), `current_pos` store (`:302-305`), ack (`:606-613`). Failure inside the write: `truncate` + `fsync` rollback inside the fence (`:408-441`); on any batch failure the thread marks `write_failed` and **exits** (`:199-205`, `:640-655`). **Event loop**: after the ack it settles byte admission, bumps `state.wal.pending_writes`, `appended_bytes`, `pending_sync_count`, `bytes_since_sync` (`txn.rs:694-709`). |
| Caller-visible failure | `WalAppendError { error, unchanged }` is handled at `txn.rs:682-693`: **the actor fences on every append failure regardless of `unchanged`** (`:685`). `unchanged` only chooses (a) whether the local-WAL byte reservation is released (`wal.rs (actor):754-758`) and (b) the caller error: `unchanged = true` returns the raw error (NoSpace, Timeout, Internal, Fenced) and the caller **can know no bytes were written**; `unchanged = false` returns `OutcomeUnknown("the transaction is in the WAL but not applied; it may be applied on restart: …")` (`txn.rs:688-692`, `wal.rs (actor):922-930`) and the caller **cannot know**. `unchanged = true` is produced only before the write or after a successful rollback truncate **and** rollback fsync (`wrun.rs:391-397`, `:408-441`); a lost authority during write or rollback yields UNKNOWN (`wrun.rs:374-377`, `:414-440`). `Timeout` here means the fs op timed out (`wrun.rs:70-73`) or the ack wait expired (UNKNOWN). The writer never reports `Fenced` for disk problems (`wrun.rs:59-61`). |
| Pinning test | Rollback proof: `should_prove_unchanged_append_only_after_durable_rollback` (`wrun.rs:806`), `should_stop_rollback_mutations_when_validity_expires_during_io` (`wrun.rs:756`); group-level: `should_fail_all_prepared_transactions_when_batch_append_hits_no_space` (`awt.rs:1442`), `should_fail_all_event_loop_strict_transactions_when_shared_append_fails` (`wb_tests.rs:1172`), `should_answer_outcome_unknown_when_group_append_is_partly_written_and_authority_lost` (`wb_tests.rs:1553`); fence after physical commit before accounting: `should_fence_each_append_path_after_physical_commit_before_accounting` (`awt.rs:1815`). |
| Notes | One physical `write_at` per group: `should_append_multiple_prepared_transactions_with_one_physical_call` (`awt.rs:1049`). The spilled-transaction path (`spill.rs:42-154`) is a **second, independent append owner**: it appends records one at a time (`spill.rs:400-420` onward), is never grouped, and syncs without a `WriterEffectGroup` (`dur.rs:37`). |

##### 6. sync

| Field | Finding |
|---|---|
| Owner | **Three owners and two sync protocols.** (a) Actor ordering/outcome: `apply_strict_group_durability_with_group` (`dur.rs:40-62`) → `sync_internal_with_group` (`:70-77`) → `begin_sync_io` (`:143-262`) + `commit_sync_io` (`:286-313`). (b) Writer wait: `FsWalWriterIo::sync_with_group` (`wio.rs:581-649`). (c) Physical fsync: `WriterRunner::complete_pending_fsyncs` (`wrun.rs:505-578`). (d) **LEDGER edit** after the fsync: `persist_sync_boundary` (`dur.rs:264-284`) → `WalHistory::record_sync_in_group` (`src/runtime/wal_history.rs:573-622`) → `LedgerStore::append` (`src/wal/ledger_store.rs:211-243`, `write_frame` `:281-316`). The strict group path **bypasses the ticketed protocol** (`dur.rs:64-77` "never touches the durability coordinator generation"); Batched/forced syncs use `EventLoop::sync_wal_generation` (`dsync.rs:162-253`, `WalSyncTicket`). |
| Effects | Loop calls `catch_unwind(sync_in_group / sync_with_timeout)` (`dur.rs:171-190`), which pushes a `PendingSync` and `notify_one` (`wio.rs:597-618`), then **blocks on `sync_cond.wait_for`** until `completed_fsyncs` (`wio.rs:620-647`). Writer thread: `take_ready_syncs` then `file.sync(Durable)` under the effect fence (`wrun.rs:580-596`, `:542-550`), `notify_all` (`:574-576`). Then **on the loop**: the LEDGER `SyncOpen` append = open + `len` + `write_at` + **second fsync**, plus an extra whole-file `read_ledger` only for unfenced stores (`ledger_store.rs:295-306`; production lease-backed stores omit the image, `ledger_store.rs:76-81`, `wal_history.rs:241-245`), plus compaction every `COMPACT_AFTER_EDITS` = 1024 edits (`ledger_store.rs:44`, `:237-241`). Then `commit_sync_io` checks the receipt against `state.sequence`/`pending_writes` (`dur.rs:291-299`), `advance_synced_to`, zeroes pending counters (`:304-310`), and `advance_local_to(last_sequence)` (`dur.rs:59`). |
| Caller-visible failure | Any sync failure, panic, authority loss, or ledger-edit failure **fences the actor** (`dur.rs:47-57`, `:203-208`, `:215-218`, `:247-254`, `:275-281`) and the group error is mapped by `group_durability_error` (`txn.rs:341-353`) to **`OutcomeUnknown`** whenever any unit had a WAL record (`wal.rs (actor):922-930`). That includes `Timeout("WAL sync timed out after …")`, which also sets the writer's `sync_failed` sticky flag (`wio.rs:633-646`). Meaning: frames are in the file (fsync may or may not have landed), memtable has none of them, replay may apply them. A ledger-edit failure *after* a successful fsync is also `OutcomeUnknown` (`dur.rs:275-281`), so the commit is likely durable but reported unknown. A pre-fsync failure on an idle barrier is retryable and does not fence (`dur.rs:163-168`) but cannot occur with pending strict records. Sync cannot be told apart from append by the caller: both are `OutcomeUnknown`. |
| Pinning test | `should_fence_strict_transaction_when_sync_fails_after_records_are_appended` (`awt.rs:824`), `should_apply_no_strict_group_member_when_shared_sync_fails` (`awt.rs:1382`), `should_fail_all_event_loop_strict_transactions_when_shared_sync_fails` (`wb_tests.rs:1233`), `should_freeze_without_failed_unit_when_strict_sync_fails_after_wal_append` (`wb_tests.rs:1791`), lease lapses before sync: `should_answer_outcome_unknown_when_lease_lapses_before_group_sync` (`wb_tests.rs:2581`), receipt mismatch: `should_fence_as_local_failure_when_wal_state_changes_under_a_sync_receipt` (`awt.rs:3168`), ledger repair: `fenced_ledger.rs:64`; one fsync per strict group: `should_coalesce_independent_strict_transactions_with_one_durable_append` (`wb_tests.rs:417`, asserts `sync_calls() == 1`). **Post-submission sync-wait `Timeout` (`wio.rs:633-646`): none** (only the pre-submission deadline is tested, `writer_groups.rs:297`). |
| Notes | **Finding:** each strict group pays **two fsync waits on the event loop**: the WAL fsync (writer thread, loop blocked on the condvar) and the LEDGER fsync (loop thread itself). Every other loop message waits behind both. |

##### 7. acknowledge

| Field | Finding |
|---|---|
| Owner | **Three layers, and the guarantee lives in the wrong one.** Durability meaning is decided by *ordering inside* `append_prepared_transaction_group` (sync at `txn.rs:212` happens before the apply loop at `:216-280`). The event loop sends the response: `ack_applied_transaction` (`dsync.rs:48-64`), `respond` (`groups.rs:59-71`), `ResponseRouter::complete` (`src/runtime/router.rs:188-244`). The engine adds `finalize_write_durability` (`etx.rs:855-892`; local = no-op, comment `:887-891`; cloud = `SealWalForCloud` round trip `:861-881`) and `post_apply_durability_error` (`etx.rs:1019-1027`). The ack function itself documents "runtime write-path acks are immediate in every mode" (`dsync.rs:40-47`); it does not check durability. |
| Effects | Event loop: group result fan-out `finish_coalesced_transaction_batch` (`wb.rs:457-547`), `publish_snapshot` once per group (`wb.rs:483-485`), per request `settle_applied_write` (`dsync.rs:68-76`: clear barrier for Strict; queue a `ConfirmTransactionApply` waiter for Batched/CloudAsync), response via bounded(1) route (`router.rs:161`) or inline channel (`groups.rs:60-66`). Response carries `write_stall_hint` computed on the loop (`dsync.rs:61`, `wb.rs:57-74`). Caller thread wakes in `recv_timeout` (`handle.rs:698`). |
| Caller-visible failure | Success = `TransactionApplied{last_sequence, op_count, write_stall_hint}` (`ingest.rs:110-125`). Group failure fan-out (`wb.rs:515-544`): not-applied members get `error.replay()`; Batched members already applied when the group fails get `OutcomeUnknown` (`wb.rs:491-501`); a fallback request that was never attempted gets `not_attempted(..)` which turns a group `OutcomeUnknown` into **`Fenced`** meaning "not logged, not applied" (`wb.rs:540`, `:705-713`). `handle_write_error` additionally **fences the whole runtime** (`lease_healthy = false`, anomaly flag) on `Timeout`, `OutcomeUnknown`, or a fenced actor (`wb.rs:678-698`). Caller wait: `runtime_response_timeout` expiry gives `Timeout("runtime request ApplyTransaction request_id=…")` (`handle.rs:118-126`, `:698-706`), which the engine rewrites to **`OutcomeUnknown`** for any commit with writes (`etx.rs:747-749`, `:1034-1042`), because the route is dropped but the loop still applies and syncs the write. In that case the caller **cannot know** whether bytes were written or even whether it was applied. `Fenced` from the engine means not applied and no WAL record to replay (`etx.rs:622-623`). |
| Pinning test | `should_coalesce_independent_strict_transactions_with_one_durable_append` (`wb_tests.rs:417`); `should_acknowledge_applied_unit_when_lease_lapses_inside_coalesced_group` (`wb_tests.rs:1641`); `should_answer_not_applied_to_fallback_request_when_its_group_fails` (`wb_tests.rs:2685`); `should_fence_runtime_when_the_wal_actor_reports_a_fenced_writer` (`wb_tests.rs:2391`); `should_not_fence_runtime_when_no_space_comes_from_outside_the_wal_writer` (`wb_tests.rs:2352`); `should_fence_runtime_when_storage_acknowledgement_times_out` (`wb_tests.rs:296`); caller timeout keeps the write: `should_finish_accepted_transaction_given_caller_times_out_before_event_loop_resumes` (`src/runtime/tests.rs:596`); route timeout: `should_bound_routed_transaction_response_when_runtime_accepts_without_responding` (`src/runtime/tests.rs:1336`); engine remap (helper only): `should_report_unknown_outcome_when_admitted_commit_response_times_out` (`etx.rs:1050`). **No test drives the engine remap through `RuntimeHandle` (helper test and runtime test are separate).** |
| Notes | **Finding:** every `Timeout` from a write submit, including a provably-unchanged one such as `wio.rs:443-453`, is reported to the application as `OutcomeUnknown` (`etx.rs:1034-1042`). That is safe but loses information; the caller cannot recover "definitely not written". |

##### 8. memtable publish (apply + snapshot publication)

| Field | Finding |
|---|---|
| Owner | **Two parts, two layers.** Apply: `WalActor::apply_prevalidated_transaction_ops` (`tstate.rs:119-134`) → `apply_to_memtable` / `apply_delete_range_to_memtable` (`tstate.rs:470-534`), called per unit from `txn.rs:255-261` after sync. Visibility: `EventLoop::publish_snapshot` (`src/runtime/event_loop/mod.rs:612-652`), called after the whole group applies (`wb.rs:483-485`, `:644`, `el_wal.rs:87`, `:178`). `RuntimeState` also mutates here (`recompute_total_memtable_bytes`, `record_delete_range`). |
| Effects | Event-loop thread only. `put_bytes_with_seq(&self)` on the memtable through a shared reference (`tstate.rs:487-495`), then per op: `state.recompute_total_memtable_bytes()` (`tstate.rs:502`, `:531`), which **iterates every column family and immutable** (`src/runtime/state/flush.rs:274-287`); `active_memtable_started_*` bookkeeping (`tstate.rs:498-501`); `state.record_delete_range` (`tstate.rs:532`). `publish_snapshot` builds a fresh `ReadSnapshot` **for every column family** and publishes `PublishedSnapshot { sequence: state.sequence, … }` to the lock-free `SnapshotCache` (`mod.rs:617-642`). Readers (`begin_tx`) take the cache, not a message. Because publish happens once after the whole group applied, group visibility is atomic by construction, not by a tracked watermark. |
| Caller-visible failure | Apply is `expect`ed infallible after preflight (`tstate.rs:119-134`, preflight `:59-76`); an unexpected failure panics the loop thread. A lease lapse between sync and apply fails the remaining units: logged units get `OutcomeUnknown`, unlogged-and-unapplied get `Fenced`, and a partly applied unit marks its family "partially applied" so it refuses writes and flush until restart (`txn.rs:237-245`, `wal.rs (actor):856-904`, `:825-839`). |
| Pinning test | Apply: `should_apply_wal_sequence_to_memtable` (`awt.rs:940`), `should_durably_append_strict_transaction_group_once_before_memtable_apply` (`awt.rs:1165`), `should_leave_no_partial_writes_given_later_memtable_preflight_failure` (`awt.rs:1308`), `should_answer_fenced_when_lease_lapses_before_unlogged_unit_applies` (`awt.rs:2290`). **`publish_snapshot` ordering and group-atomic visibility: none** (the only direct test pins a side effect, `should_not_prune_reader_cache_when_publishing_after_plain_write`, `src/runtime/event_loop/tests.rs:3326`). |

---

#### (a) Which thread each step runs on, and what blocks the event loop

| Step | Thread | Blocks the loop? | Cite |
|---|---|---|---|
| Stall probe (if flag set) | caller → loop → caller | no (caller waits up to 250 ms) | `ingest.rs:77-88`, `handle.rs:14`, `:749-785` |
| `try_send` into queue (cap 1000) | caller | no | `handle.rs:640` |
| Dequeue, dispatch, admission, SST-backed validation | **loop** | **yes**: SST reads, remote reads in cloud mode | `scheduler.rs:434-444`, `dispatch.rs:83-86`, `tstate.rs:340-373`, `awt.rs:127-189` |
| Group collect window (`try_recv`/`recv_timeout` 5 µs slices, at most 100 µs) | **loop** | yes (sleeps) | `wb.rs:340-393` |
| Sequence alloc (+ ledger reserve fsync on refill) | **loop** | yes, rarely | `state.rs:1274-1312` |
| Nested payload encode | **loop** | yes (CPU) | `txn.rs:584-590` |
| `begin_group` / `validate_write_admission` (lease authority) | **loop** | yes; provider-store remote proof "at the group boundary" | `txn.rs:294-316`, `src/lease/traits.rs:1013-1023`, `wal.rs (actor):1052-1074` |
| Frame encode, **LZ4 compress** (payload at least 256 B), **CRC32C** | **loop** | yes (CPU) | `wio.rs:225-235`, `enc.rs:310-315`, `frame.rs:43` |
| pwrite + XXH3 digest | writer thread | **yes: loop waits on ack channel** | `wio.rs:388`, `wrun.rs:371-386` |
| **fsync (WAL)** | writer thread | **yes: loop waits on condvar** | `wio.rs:620-647`, `wrun.rs:545` |
| **LEDGER SyncOpen append + fsync** | **loop itself** | **yes (synchronous I/O)** | `dur.rs:257`, `ledger_store.rs:281-316` |
| Memtable apply, `recompute_total_memtable_bytes` per op | **loop** | yes (CPU) | `tstate.rs:470-504`, `flush.rs:274-287` |
| `publish_snapshot` (per-CF snapshots) | **loop** | yes (CPU, O(#CFs)) | `mod.rs:612-652` |
| Response send | loop | no | `groups.rs:59-71` |

Answer to the specific question: **fsync blocks the loop (it waits on a condvar for the writer thread); encode, compress and CRC are all on the loop and block it; the LEDGER fsync is a second fsync executed directly on the loop.**

#### (b) Exact strict-commit sequence, `Transaction::commit` to caller ack

Local, Strict, lease-backed (grouped) writer. `W` marks a blocking wait, `H` a cross-thread hop.

1. **Caller thread**: `commit_with_sequence` (`etx.rs:646`): `validate_assertions` against its own snapshot (`:653`, `:786-799`), build ops (`:708`), `coordinator.submit_ops` (`etx.rs:709`, `ingest.rs:36`).
2. (conditional) **H0/W0**: if `stall_flag` is set, `CheckWriteStall` round trip, 250 ms bound (`ingest.rs:77-79`, `handle.rs:754-760`).
3. `send_apply_transaction_and_wait`: `lifecycle.begin_submission`, `router.register` (bounded(1) channel), **H1** `msg_tx.try_send` to the bounded(1000) loop queue (`handle.rs:638-651`); then **W1** `recv_timeout(runtime_response_timeout)` (`handle.rs:654-659`, `:698`).
4. **Event loop** wakes from `select!` (`scheduler.rs:434-437`), `process_wake_msg` (`:282-306`) → `RuntimeDispatcher` → `WalCoordinator::apply_transaction` (`dispatch.rs:83-86`, `:164-190`; `el_wal.rs:113`).
5. Loop: `accept_write`, `ensure_l0_write_admission`, `prepare_cloud_transaction_memtables` (`el_wal.rs:132-145`); `apply_transaction_with_coalescing` (`el_wal.rs:153`, `wb.rs:264`).
6. Loop: `prepare_transaction_for_coalescing` (`wb.rs:549-589`) → `prepare_transaction_append` (`txn.rs:86-162`): preconditions (possibly SST reads), allocate sequences, build apply-ops, **encode nested payload**.
7. Loop: collect window, only if the queue was non-empty at entry (`wb.rs:303-306`, `:340-393`); each additional message goes through step 6.
8. Loop: `finish_coalesced_transaction_batch` → `append_prepared_transaction_group` (`wb.rs:469-472`, `txn.rs:185-286`) → `append_prepared_group_wal` (`txn.rs:288-336`): `begin_group` (authority), `append_prepared_transaction_batches` (`txn.rs:623-711`): `admit_wal_records`, lease check, `append_batch_in_group` → **frame/compress/CRC** → `enqueue_in_group` (`wio.rs:566-576`, `:333-441`).
9. **H2** loop → writer thread (queue push + `notify_one`, `wio.rs:370-373`); loop **W2** on the ack channel (`wio.rs:388`).
10. **Writer thread**: wake (`wrun.rs:217-256`), `write_at`, digest, `current_pos`, **H3** ack (`wrun.rs:470-471`, `:606-613`).
11. Loop resumes: `count_records`, byte-budget settle, `pending_writes` bookkeeping (`wio.rs:577`, `txn.rs:694-709`).
12. Loop: `apply_strict_group_durability_with_group` (`txn.rs:325-327`, `dur.rs:40`) → `begin_sync_io` (`dur.rs:143`): authority check (skipped when prevalidated, `dur.rs:108-138`), `sync_with_group`: push `PendingSync`, **H4** `notify_one` (`wio.rs:597-618`); loop **W3** on `sync_cond` (`wio.rs:620-647`).
13. **Writer thread**: `complete_pending_fsyncs`: fsync under effect fence, **H5** `notify_all` (`wrun.rs:505-578`).
14. Loop resumes: **W4 (inline)** `persist_sync_boundary` = LEDGER append + fsync (`dur.rs:257`, `:264-284`); `commit_sync_io`, `advance_synced_to`, `advance_local_to` (`dur.rs:286-313`, `:59`).
15. Loop: per unit `ensure_lease_validity`, `apply_prevalidated_transaction_ops` (memtable) (`txn.rs:237-261`).
16. Loop: `publish_snapshot` (`wb.rs:483-485`), per request `ack_applied_transaction` → `respond` (`dsync.rs:48-64`, `groups.rs:59-71`) → **H6** `router.complete` bounded(1) send (`router.rs:188-196`).
17. **Caller thread** wakes from `recv_timeout`, decodes (`ingest.rs:104-131`), `sequence_publisher.store`, `finalize_write_durability` (local: no-op, `etx.rs:724-737`, `:887-891`), `unregister_snapshot`, returns.
18. Afterward on the loop: `drain_pending_writes` (up to 1024), `sync_batched_wal_if_needed` (idle for strict, `el_wal.rs:196-203`).

Count: **6 hops** (H1 to H6) plus 1 optional probe round trip; **4 blocking waits** on the loop (W2 ack, W3 fsync, W4 ledger fsync inline, plus the collect window) and 1 on the caller (W1). Cloud strict (`cloud_strict`/`sync` in cloud mode) adds a second caller → loop round trip, `SealWalForCloud`, after the ack (`etx.rs:861-881`).

#### (c) Group commit

* **Mechanism:** there is no group-commit coordinator. The first `ApplyTransaction` of a wake becomes `CoalescedTransactionBatch` and the loop pulls further `ApplyTransaction`s straight off the same channel with `try_recv` (`wb.rs:274-324`, `:340-393`). `coalesced_transaction_durability` admits only Strict/Batched, non-CloudAsync, with a real writer (`wal.rs (actor):940-951`). One `append_batch` and (for all-Strict) one `sync` cover the whole group (`txn.rs:209-212`, `:294-335`); pinned by `wb_tests.rs:417`, `awt.rs:1165`.
* **`LOCAL_STRICT_GROUP_COMMIT_WINDOW`** = `Duration::from_micros(CANDIDATE_WINDOWS_US[4])` = **100 µs** (`wb.rs:19-23`). It applies **only when the queue is non-empty at group start**: `local_strict_group_commit_collect_window(queue_is_empty)` returns `None` for an empty queue (`wb.rs:25-27`, `:303-306`; pinned by `wb_tests.rs:1946`, whose argument is mislabelled `at_capacity`). So a lone writer waits **0 µs**, and real batching comes from commits accumulating in the queue while the loop is blocked in fsync.
* **Other bounds:** the target of 8 (`EXPLICIT_TRANSACTION_COALESCE_TARGET`, `wb.rs:15`) stops the wait only when the channel is momentarily empty (`wb.rs:363-367`); with a non-empty channel the group keeps growing to `max` = 1024 after a wake (`el_wal.rs:152-163`; 64 for drained writes `scheduler.rs:355`, `:296-302`; 1024 `el_wal.rs:104`, `:197`; 4096 before a batched sync `dsync.rs:296`). Pinned: `wb_tests.rs:1962`. Batched windows (25/100 µs) apply only to transactions with a `start_sequence` (`wb.rs:16-17`, `:307-313`). No byte bound on a group.
* **What breaks a group** (each ends collection; `fallback_request` / `deferred_error` run **after** the group completes):
  1. **Key/range overlap with an already-staged member** (`StagedTransactionTouches`, `wb.rs:555-560`, `:779-877`) → `Fallback`, applied as its own append + fsync after the group (`wb.rs:522-525`). Pinned: `should_fall_back_from_coalescing_when_point_range_overlap` (`wb_tests.rs:551`: Strict gives 2 appends, 2 syncs) and `wb_tests.rs:1895`. Also assertion overlap (`wb.rs:823-827`, `wb_tests.rs:950`).
  2. **Different `TransactionCoalescingKey`** (durability policy, `has_start_sequence`, `conflict_policy`, `wb.rs:772-777`, check `:556`) → pinned by `wb_tests.rs:1010`.
  3. Empty-ops (assertion-only) request (`wb.rs:555`; `wb_tests.rs:1053`).
  4. Any non-`ApplyTransaction` message: stashed in the single `pending_msg` slot (`wb.rs:445-453`; `wb_tests.rs:675`, `:731`).
  5. A member failing prepare/admission → `deferred_error` (`wb.rs:439-441`, `:292-300`).
  6. `max` reached, channel empty/disconnected, window elapsed, or lease unhealthy (`wb.rs:349`, `:356-390`, `:182-184`).
* **Why overlap is excluded today** (the comment at `wb.rs:805-827`): each member's sequence is allocated at prepare, but its memtable apply is deferred until after the group's fsync, so a later member's conflict/assertion/insert-only validation (which reads memtables + SSTs, `tstate.rs:329-373`) cannot see the earlier staged write.

#### (d) Backpressure: every bound on the write path

| Bound | Value | Enforced at | Caller sees |
|---|---|---|---|
| Loop request queue | **1000 messages** (`RUNTIME_QUEUE_CAPACITY`, `runtime.rs:13`; `bounded` `runtime.rs:49`, `:147`); counts messages, not bytes | `try_send`, `handle.rs:640` / `:89-101` | `WriteStall("runtime request queue is full")`; not enqueued |
| Caller stall hint | one `AtomicBool` per `IngestCoordinator`, set from the previous response's `write_stall_hint` (`ingest.rs:116`); probe timeout 250 ms (`handle.rs:14`) | `ingest.rs:77-88` | `WriteStall("Memory budget exceeded for CF n")`; a probe timeout is treated as stalled (`handle.rs:783`) |
| L0 slot ceiling | `l0_compaction_trigger + max_immutable_memtables + 1` (`flush.rs:219-224`); defaults not read; a txn that crosses the flush threshold is admitted, the next stalls (`flush.rs:255-272`) | loop, `wb.rs:120-140` (dispatch `el_wal.rs:138`, drain `wb.rs:593-599`) | `WriteStall("… no free L0 slot (u/c)")`; pinned `wb_tests.rs:2272` |
| Immutable-memtable queue full | `limits.max_immutable_memtables` (`flush.rs:195-199`) | **hint only**; indirectly enforced via L0 slot usage, which counts immutables (`flush.rs:239-242`) | via L0 `WriteStall` |
| Total memtable hard limit | `memtable_flush_threshold * 2` (`flush.rs:206-215`) | **advisory only**: feeds `should_hard_stall_writes` (`flush.rs:293-304`) → hint/probe/`WaitForWriteStallClear` (`wb.rs:57-74`, `control.rs:28-66`); **not** checked by `ensure_l0_write_admission` | `WriteStall` only through the caller-side flag, one response late; the loop itself does not reject at the hard limit |
| `write_pressure.stalled` | boolean (`flush.rs:380-386`) | advisory, same as above | same |
| CloudAsync WAL admission | upload-queue limits (`sealing.rs:368-385`) | `accept_write`, dispatch path only | `WriteStall` |
| Cloud flush-staging window | `max_local_bytes / 2` (`cloud_memtable_admission.rs:10-18`) | `prepare_cloud_transaction_memtables`, **first txn on dispatch only** (`el_wal.rs:142`) | `NoSpace` (txn alone exceeds window) / `WriteStall` (needs an immutable first) (`cloud_memtable_admission.rs:57-82`) |
| Local WAL byte budget | frame-size bound sum per append (`wal.rs (actor):712-733`), only with hybrid storage + ephemeral cache; settled to actual (`:735-747`) | before append, `txn.rs:664` | `NoSpace("ephemeral local disk budget cannot admit WAL append")`; un-fenced; pinned `awt.rs:32` |
| WAL writer queue | `MAX_QUEUE_DEPTH` = **5000** entries (`wrun.rs:120`); 50 attempts, backoff 1→100 ms (`wio.rs:35-36`, `:349-426`) ≈ 4.4 s | `enqueue_in_group` | UNCHANGED `WriteStall("WAL queue full …")`; apparently unreachable (see row 4); no test |
| WAL replay bound | txn payload at most `WAL_MAX_VALUE_LEN` = 64 MiB (`frame.rs:9`, `txn.rs:597-605`); decoded/frame bound at most `max_replayable_txn_bytes` when `Some` (`txn.rs:606-619`, `wal.rs (actor):685-692`; spilled `spill.rs:73-75`). `Some` is set **only on the two cloud config paths** (`storage.rs:1084`, `:1240`, both `..Self::cloud_runtime_config`); local default `None` (`src/runtime/config.rs:190`) | prepare, before sequences/bytes | `ResourceLimit`; sequences released; pinned `awt.rs:2872` |
| Group size / bytes | count only (see (c)); no byte cap | `wb.rs:349`, `el_wal.rs:152` | none |
| Sequence space | `u64::MAX - 1` | `state.rs:1281` | `ResourceLimit` |

#### (e) Deadlines and what continues after a caller gives up

| Wait | Deadline | Where | Expiry result | What continues |
|---|---|---|---|---|
| Caller response wait | `runtime_response_timeout`: default `max(60 s, storage_io_timeout + 30 s)` = 60 s with the 30 s storage default (`src/config.rs:10-11`, `:32-38`) | `receive_routed_response` `handle.rs:691-720` | `Timeout("runtime request ApplyTransaction request_id=… exceeded response timeout …")` (`handle.rs:118-126`); engine remaps to `OutcomeUnknown` (`etx.rs:747-749`) | **Everything.** `router.abandon` removes the route (`router.rs:261-281`); the queued request is not cancelled and ApplyTransaction does no route/deadline check on dequeue (`registered_request_deadline` is not used on the write path, grep of `src/runtime/event_loop`). The loop still appends, syncs, applies, publishes; the response is counted late (`router.rs:188-243`). Pinned: `src/runtime/tests.rs:596`. |
| Stall probe | 250 ms | `handle.rs:14` | treated as stalled → `WriteStall` | n/a |
| Group collect window | at most 100 µs strict; 25/100 µs batched | `wb.rs:15-23` | group closes | n/a |
| Writer queue backoff | about 4.4 s | `wio.rs:349-426` | UNCHANGED `WriteStall` | none was enqueued |
| Native admission budget (append, sync) | `storage_io_timeout`, spent at admission and in the ack wait (`wio.rs:383-387`, `:443-453`, `:603-607`) | `wio.rs` | UNCHANGED `Timeout`; non-poisoning (`writer_groups.rs:285`, `:297`) | none was enqueued |
| Append ack wait | `storage_io_timeout` (default 30 s, `config.rs:10`) | `wio.rs:388-404` | UNKNOWN `Timeout`; `write_failed` sticky (`wio.rs:394-400`) | **The writer thread may still perform the write**; later appends are rejected UNCHANGED (`wio.rs:1062`). Actor fences. |
| Sync wait | `storage_io_timeout`; same value also bounds `begin_group` (`txn.rs:298`) | `wio.rs:633-646` | `Timeout`; `sync_failed` sticky | The fsync may still complete on the writer thread; actor fences and caller gets `OutcomeUnknown` |
| Loop cloud work | `event_loop_cloud_deadline` = `runtime_response_timeout` (`mod.rs:596-598`) | cloud paths only | n/a to local write path | n/a |

Not a deadline but relevant: after any `Timeout`/`OutcomeUnknown` response the runtime flips `lease_healthy = false` (`wb.rs:682-695`); all later writes are rejected as `Fenced` until restart.

---

#### Hypothesis verdicts

* **#904 (stop committing a LEDGER edit per fsync): CONFIRMED, with one correction.** Per strict group the loop does `persist_sync_boundary` (`dur.rs:264-284`) → `record_sync_inner` commits `SyncOpen` unless `through_bytes <= durable_bytes` (`wal_history.rs:609-622`) → `LedgerStore::append` + `write_frame` = open, `len`, `write_at`, fsync (`ledger_store.rs:211-243`, `:281-316`), and compaction every 1024 edits (`:44`, `:237-241`). **Correction:** the "re-reads the entire ledger" claim holds only for unfenced stores (`image: Some`, `ledger_store.rs:295-306`); stores opened `under_writer_fence` (lease-backed) keep `image = None` and skip the read (`ledger_store.rs:76-81`, `:156`, `:176-178`, `wal_history.rs:241-245`). The cost that remains is the second fsync on the loop. The edit is also a failure mode: ledger failure after a successful WAL fsync becomes `OutcomeUnknown` (`dur.rs:275-281`).
* **#905 (overlapping writes join the group): PARTLY CONFIRMED; R2 is incomplete.** The fallback is at `wb.rs:555-560`; the constants are 100 µs strict (only with a non-empty queue) and 25/100 µs batched (only with `start_sequence`), target 8 only when the channel is empty (`wb.rs:15-23`, `:303-315`, `:363-367`). The 16-writers-one-key scenario costs one fsync each because the fallback runs after the group (`wb.rs:522-525`; `wb_tests.rs:551`). But the issue's reason is understated: the hazard is deferred apply (`wb.rs:805-827`), which also blinds `AbortOnWriteConflict` write-write checks (`txn.rs:389-399`) and `insert_only` (`txn.rs:464-479`), not only assertions. A counter workload that read-modify-writes under `AbortOnWriteConflict` would conflict (correctly) rather than share a group; joining helps only `LastWriteWins`, non-`insert_only` blind writes. R2 must cover write-write conflicts, not just assertions/`insert_only`.
* **#906 (concurrent memtable apply with visibility watermark): CONFIRMED description; the work is larger than stated.** Apply and publish are on the loop (`txn.rs:255-261`, `wb.rs:483-485`). The skiplist API is `&self` (`tstate.rs:487-495`), but apply also mutates non-concurrent state: per-op `recompute_total_memtable_bytes` over all CFs (`tstate.rs:502`, `:531`; `flush.rs:274-287`), `record_delete_range` (`tstate.rs:532`), `active_memtable_started_*` (`tstate.rs:498-501`), and partial-apply marking (`wal.rs (actor):863-869`). There is **no watermark today**: `state.sequence` is advanced at allocation (`state.rs:1302`) and published after the group (`mod.rs:639-642`); visibility is atomic only because one thread does prepare→apply→publish. R4 (fencing at apply) already exists per unit (`txn.rs:237-245`; `wb_tests.rs:1465`, `:1641`).
* **#940 (fsync completion as an event): CONFIRMED mechanics, with four missing prerequisites.** The loop does a synchronous `catch_unwind` wait (`dur.rs:171-190`), fences before ack (`dur.rs:40-58`), and the writer already batches sync requests (`wrun.rs:580-596`). Missing from the issue: (1) the strict path **does not use** the ticketed protocol that a `WalSyncComplete` handler would extend (`dur.rs:64-77` vs `dsync.rs:162-253`), so two sync paths must be unified; (2) the `WriterEffectGroup` root is lexical and `!Send` (`src/lease/writer_effect_groups.rs:423-436`) and lives in a local of `append_prepared_group_wal` (`txn.rs:294-335`), so an in-flight sync means parking it in loop-owned state while the filesystem fence stays held across other loop turns (`writer_effect_groups.rs:1-5`); (3) R3 pipelining needs a **staged-write view** for validation, the same blocker as #905, since apply must still follow sync; (4) the writer's multi-request sync batching is unexercised today because the loop is the only producer and blocks per request (`wio.rs:620-647`).
* **#941 (encode/compress off the loop): PREMISE PARTLY REFUTED; R1 as written is not format-preserving.** Verified lines `enc.rs:288-335`. But compression applies to the **outer record VALUE = the whole nested payload**, not to individual op values (`enc.rs:310-315`, comment `enc.rs:1502-1503`), and the sequences sit **inside** that payload (`enc.rs:680-702`), so patching a sequence after compression is impossible. Viable variants: (a) allocate sequences at admission on the caller (breaks loop-owned allocation + ledger reservation, `state.rs:1274-1312`); (b) move frame encode+compress+CRC to the writer thread (the `WalPos` returned by `append_batch` is ignored by the txn path, `txn.rs:679-682`, so deferring layout is feasible; `admit_records`/`count_records` need only sequences, `wio.rs:240-267`); (c) per-op value compression (format change). Work moved off the loop is two encodes plus LZ4 plus CRC (`txn.rs:584-590`, `wio.rs:225-235`).
* **#947 (frame once, vectored writes): PARTLY CONFIRMED.** Copies exist: LZ4 owned `Vec` (`enc.rs:313`), `push_tlv` copy (`enc.rs:163-170`, `:316`), nested payload built separately (`enc.rs:660`). But the outer frame is already encoded in place (`frame.rs:69-92`), and `coalesce_batch` moves a lone entry without copying (`wrun.rs:310-312`). With the loop as sole producer and blocked per ack, the queue holds at most one entry, so the multi-entry concatenation (the 4th copy) and `writev` (R2) have nothing to gather. A "frame once" change must also account for the XXH3 segment digest pass inside the ack wait (`wrun.rs:380-386`).
* **#946 (conflict checks off SSTs on the loop): CONFIRMED.** `latest_sequence_snapshot` clones `manifest.files` for the CF and builds a `ReadSnapshot` (`tstate.rs:340-373`), built lazily once per CF per validation (`tstate.rs:45-55`); preflight copies each key into a `HashSet` (`tstate.rs:63-69`); a test shows the check reading a **remote** SST store from loop resources (`awt.rs:127-189`). Scope: only for `AbortOnWriteConflict` (`txn.rs:389-399`), assertions (`:404-411`), or `insert_only` (`:464-479`); plain `LastWriteWins` writes do none of this. `latest_state_sequence` internals not verified (extra file not read).

---

#### Summary of cross-cutting findings

1. **The loop serialises everything and blocks through two fsyncs per strict group** (WAL fsync via condvar, then the LEDGER fsync inline), plus encode, compress, CRC, apply, and per-CF snapshot publish. Real group commit arises only from queue accumulation while the loop is blocked; the 100 µs window is skipped for a lone writer.
2. **No transition has a single owner.** Frame has two encode owners in two layers; sync has three owners and two protocols; acknowledge's durability guarantee is carried by call ordering inside `append_prepared_transaction_group`, while the ack function states acks are "immediate in every mode"; sequence lives on `RuntimeState` and reaches into the ledger; the spilled path (`spill.rs`) is a second append/sync owner.
3. **`unchanged` never changes the fencing decision**: every append failure fences (`txn.rs:685`); `unchanged` only selects raw-error vs `OutcomeUnknown` and byte-budget settlement. Any `Timeout`/`OutcomeUnknown` response also fences the whole runtime (`wb.rs:682-695`).
4. **Backpressure is partly advisory or dead**: the memtable hard limit is only a hint; the 5000-deep WAL queue looks unreachable; followers in a coalesced group skip cloud-memtable admission.
5. **Timeout semantics collapse**: any write-submit `Timeout`, even a provably-unchanged one, becomes `OutcomeUnknown` at the engine (`etx.rs:1034-1042`).

#### Transitions / behaviours with no pinning test

* admit: loop-queue-full `WriteStall` for `ApplyTransaction` (`handle.rs:89-101`); drain-path skipping `prepare_cloud_transaction_memtables`.
* enqueue: `WriteStall("WAL queue full …")` (`wio.rs:432-440`).
* sync: post-submission `Timeout` of the fsync wait (`wio.rs:633-646`).
* memtable publish: `publish_snapshot` ordering and group-atomic visibility.
* acknowledge: engine `Timeout → OutcomeUnknown` driven end to end through `RuntimeHandle`.
* memtable hard limit being advisory (no test asserts the loop does or does not reject at the hard limit).

## Appendix B: Segment lifecycle and recovery (rotate through shutdown)

### #873 Phase 0: segment lifecycle and recovery transitions

Scope: rotate, seal, LEDGER edit, recover-after-partial, replay, shutdown.
Branch `develop` @ 12962819. Read-only research; every `file:line` below was read with the Read tool (offset/limit). Test function names that I only saw through `rg` (not Read) are marked `(rg)`.

Vocabulary used in the failure column:
- `Open` / `Transitioning` / `Fenced` are the `WalIoState` actor states (`src/runtime/actors/wal.rs:172-192`). `Transitioning` is never a caller-visible resting state: rotate and sync assert they settle in `Open` or `Fenced` (`rotation.rs:53-56`, `durability.rs:91-94`).
- Naming hazard: state `Fenced` does NOT always surface as `MidgeError::Fenced`. `io_error()` returns `MidgeError::Fenced` only when `authority_lost` is true; a locally poisoned WAL returns `MidgeError::RecoveryFailed("local WAL is poisoned: ...")`, and a stuck `Transitioning` returns `RecoveryFailed("... transition did not complete ... restart is required")` (`wal.rs:502-536`). Tests assert both (`actors/wal/tests.rs:1790` RecoveryFailed after a local fence; `tests.rs:3437` Fenced after lease expiry).

#### 1. The six transitions

| # | Transition | (1) Owner | (2) Effects | (3) Caller-visible failure outcome | (4) Pinning test | (5) Citations |
|---|---|---|---|---|---|---|
| 1 | **rotate** | `WalActor::rotate` -> `rotate_with_ticket` (runtime/actors/wal/rotation.rs). It orchestrates `WalHistory::prepare_rotation` / `record_rotation` (runtime/wal_history.rs) and `FsWalWriterIo::write_footer` (wal/fs/writer_io.rs). **Straddles three layers.** A **second owner** performs the same physical rotation at startup: `WalHistory::start_epoch` (footer + create next + `Rotate`, bypassing the actor and the ticket protocol). | Event-loop thread, in order: (a) lease check; (b) `begin_io_transition(Rotate)` Open->Transitioning; (c) `seal_facts` waits for the writer thread to drain, takes the digest (no file IO); (d) `prepare_rotation`: `journal.heal()` (LEDGER snapshot republish if an earlier write failed), `ledger.check`, existence check of open segment, `create_segment` = write 128-byte header + file fsync + dir fsync; (e) **first irreversible step**: `mark_transition_sealed(old)`, footer frame appended and fsynced by the writer thread; (f) drop writer, `LedgerEdit::Rotate` appended + LEDGER fsync; (g) open writer on next segment (`FsWalFactoryIo::open_segment`), install, `finish_io_transition`. Roughly 5 fsyncs (header, dir, footer, LEDGER, plus whatever the writer open does). | Before the footer starts: `Open`, retryable, error is the raw cause (`Internal` for failpoints / ticket mismatch / ledger check; `Fenced` if the lease lapsed; `Timeout` from `drained_digest`). Exceptions that **fence even though nothing irreversible happened**: ticket disagreement (`rotation.rs:76-88`), `seal_facts` failure (`:124-133`), `RecoveryFailed`/`Corruption` from `prepare_next_segment` (open segment file gone, or next segment holds unrecorded records; `:141-144`). From the footer on: `Fenced` (poisoned, `RecoveryFailed` on later calls; `Fenced` variant only if authority lost), `sealed_segment = N` recorded for restart recovery. After `Rotate` is durable but before the writer is installed, the ledger already reports segment N+1 open while the actor is Fenced (`event_loop/wal_transition.rs` test asserts `open_segment == segment_id+1` at `AfterLedgerRotate`). | actor matrix `should_leave_rotation_operational_or_fenced_at_every_filesystem_boundary` (src/runtime/actors/wal/tests.rs:1747, failpoints feature); event-loop matrix `should_keep_both_transition_views_consistent_at_every_local_rotation_boundary` (src/runtime/event_loop/wal_transition.rs:299, failpoints); `should_fail_rotate_without_advancing_segment_when_next_segment_creation_fails` (tests.rs:1705); `should_fence_when_footer_write_fails` (tests.rs:3488); `should_fence_rotation_when_open_segment_is_missing_with_pending_records` (tests.rs:744); `should_fence_filesystem_wal_when_next_writer_open_fails_after_ledger_rotate` (tests.rs:1993); `should_preserve_open_segment_when_validity_expires_before_next_segment_create` (tests.rs:3404); process-abort pair `should_recover_only_accepted_write_when_rotation_loses_replacement_writer` / `..._stops_after_writer_installation` (tests/fault_injection.rs:6284, 6300). Caveat: the two matrices run on an **empty** segment (no records), so they do not exercise footer-with-records. | rotation.rs:3-25 (step table), 47-58, 60-164, 168-178, 192-204, 208-233, 237-260, 262-295, 324-358; durability.rs: n/a; wal_history.rs:434-468, 477-484, 507-555, 856-882, 967-993; writer_io.rs:511-528; wal.rs:414-472, 502-536, 539-584 |
| 2 | **seal** (protocol-level; the footer itself is inside rotate) | **Two owners.** Local: `EventLoop::rotate_local_wal_transition` (event_loop/wal_transition.rs:69), triggered only by `seal_active_wal_for_prune` (event_loop/wal_retention.rs:85). Cloud: `EventLoop::try_seal_current_cloud_segment` (event_loop/cloud_integration/sealing.rs:40), reached from the timer path `maybe_flush_cloud_async_wal` (sealing.rs:409), the `seal_for_cloud` request handler (event_loop/wal.rs:332), and shutdown. Shared state machine: `WalTransitionProtocol` + `WalSealTicket` (runtime/wal_transition.rs). | Local: `sync_local_wal_before_prune_rotation` (a full paired sync, so one more WAL fsync + SyncOpen edit), `begin_seal` ticket, `actor.rotate`, `note_sealed`, `finish_seal`, `retire_local_seal`. Cloud: `prepare_cloud_seal` (lease + upload capacity), `begin_seal`, `flush_for_cloud_upload_within` (actor enters `CloudFlush`, fsyncs the WAL once, **does not touch LEDGER**), revalidate lease, `actor.rotate`, register segment in durability coordinator + upload backlog, transfer accounting. All on the event-loop thread, so everything queued behind a seal waits (sealing.rs:16-21). | `settle_failed_seal_step`: if the actor fenced itself the protocol fences too and every queued durability waiter gets one terminal error (`fence_wal_transition`); otherwise `abandon_prepared_seal` and the seal is retryable (wal_transition.rs:27-66). Cloud failure after rotate -> `finish_failed_cloud_seal_transition`: segment kept as an owned obligation, runtime fenced, waiters failed (sealing.rs:240-270). Reversible cloud failure after the flush -> `cancel_reversible_cloud_seal` (sealing.rs:272-285). Local prune-time failure only logs, marks `persistence_anomaly`, and retains the WAL (wal_retention.rs:89-98). | `should_commit_local_rotation_as_one_protocol_transition`, `should_roll_back_prepared_seal_when_actor_stayed_operational`, `should_fence_both_transition_views_from_a_single_helper` (event_loop/wal_transition.rs:271, 246, 223); cloud matrix `CLOUD_SEAL_BOUNDARIES` (wal_transition_boundary.rs:57-69, tests in event_loop/cloud_integration/tests.rs, not read); `should_not_rotate_local_wal_when_active_segment_is_empty` (event_loop/flush_pipeline/tests.rs:1219). `seal_for_cloud`'s own branches: **none found by name**. | event_loop/wal_transition.rs:27-99; sealing.rs:29-114, 116-166, 174-232, 240-285; event_loop/wal.rs:332-443; wal_retention.rs:85-100; durability.rs:368-414, 422-433; wal_transition.rs:1-140 |
| 3 | **LEDGER edit** | Write path: `LedgerStore::append` (wal/ledger_store.rs:211) behind `WalHistory::commit` (runtime/wal_history.rs:398); validity owner: `WalLedger::apply` (wal/ledger.rs:278, pure). One write path, but ~10 callers across 5 modules (see section A). | `append`: heal if needed (snapshot republish = tmp + fsync + rename + dir fsync), clone ledger and `apply` the edit (O(members) clone), JSON-encode one frame `[len][crc32c][payload]`, `write_frame` = open LEDGER, `len()`, `write_at(len)`, **`sync(Durable)`**. Every 1024 edits it compacts synchronously on the same call (ledger_store.rs:237-241; failure tolerated, next edit retries). Runs on whichever thread called the `WalHistory` method: event loop at runtime, startup thread at recovery. Under a `WalHistoryFence` the call is wrapped by `with_writer_effect` (leader-store epoch proof). | `Err` from `commit`: invalid edit -> `Internal` (`LedgerError` mapped at ledger_store.rs:321-336), nothing written; I/O failure -> `failed=true`, next append heals first; image mismatch -> `Fenced` and the store latches `fenced` (ledger_store.rs:212-216, 221-228). A failed write "may or may not have become durable" (wal_history.rs:395-396). Caller consequence differs by edit: `SyncOpen` failure -> actor fenced + `OutcomeUnknown` (durability.rs:275-281, wal.rs:922-930); `Rotate` failure -> fenced with `sealed = N` (rotation.rs:247-254); `Reserve` failure tolerated while old reservation covers the allocation (state.rs:1286-1297). | Frame/torn/heal: `should_drop_torn_tail_when_opening` (wal/ledger_store/tests.rs:704), `should_heal_before_appending_when_earlier_append_failed` (:727, rg), `should_leave_old_or_new_ledger_when_power_fails_during_append` (:948, rg), `should_bound_ledger_size_when_many_edits_append` (:795, rg). Semantics: `should_persist_monotone_open_sync_boundary`, `should_persist_salvage_floor_with_open_cut`, `should_reject_invalid_open_sync_without_mutation` (wal/ledger/tests.rs:953, 980, 1009). Fenced repair: `fenced_ledger.rs:64` onward. **No test counts LEDGER fsyncs** (see section A). | ledger_store.rs:1-25, 203-243, 251-279, 281-316, 321-336; ledger.rs:98-142, 278-293; wal_history.rs:390-400, 609-622 |
| 4 | **recover-after-partial** | **No single owner; at least five.** (i) detection: `streaming::admit_segment` + `stop_at` (wal/recovery/streaming.rs:737, 863) for local, and `probe_open_member` (runtime/cloud_startup/streaming_wal_plan.rs:385) + `planner::admit_open` (wal/planner.rs:531) for cloud: **the durable-floor rule is implemented twice**. (ii) local repair: `RuntimeState::replay_wal_storage_within` / `truncate_tolerated_active_tail` (runtime/state/recovery.rs:585, 667), which lives in `runtime::state` yet truncates WAL files and calls `WalHistory::set_aside`. (iii) cloud repair: `commit_recovery_plan_within` + `apply_open_cut` (runtime/cloud_startup/mod.rs:92; streaming_wal_plan.rs:487). (iv) file/ledger mechanics: `WalHistory` (`sweep_orphans`, `set_aside`, `start_epoch`, `rebase`). (v) epoch start: `RuntimeState::begin_wal_epoch` (state.rs:1344) called from `EventLoop::new` (event_loop/mod.rs:273). | Order at local open: `WalHistory::open_with_fence` (LEDGER snapshot republish + orphan sweep) -> `ReplaySegments::from_ledger` -> replay (read-only) -> if torn active tail: durable truncate under writer fence -> if salvage stop: `set_aside` (copy cut segment, move later members into `salvaged-<ms>/`, cut + footer the stop segment, create next, `SetAside` edit, sweep) -> memtables installed -> **later**, in `EventLoop::new`, `start_epoch`: scan the whole open segment, write footer (or just fsync an existing footer), create next, `Rotate` edit; or `Reopen` for an empty open segment; then `Reserve` edit. Cloud variant replaces the middle with plan + `SetAside`/`Rebase`/`SalvageOpen` edits. | Strict: `Corruption` (local) or `RecoveryFailed` (cloud/plan) and **the engine does not open**; no history mutated except the LEDGER republish and orphan sweep that precede replay. Salvage: opens with `opened_in_salvage_mode`, damage quarantined, sequence floor raised. **Local and cloud return different variants for the same condition** (local lost-write `Corruption`, tests/durability.rs:777; cloud `RecoveryFailed`, streaming_wal_plan/tests.rs:1104). See section B. | See per-case table in section B. Strongest: `should_fail_strict_given_missing_acknowledged_wal_tail_when_reopening` (tests/durability.rs:735), `should_recover_post_restart_sync_commit_when_previous_crash_left_torn_wal_tail` (:783), `should_open_strictly_when_power_loss_tears_unsynced_multi_page_tail` (tests/storage_root_injection.rs:811), `should_set_aside_unreplayed_wal_when_salvage_stops_at_corrupt_segment` (runtime/state/recovery.rs:1591), `should_reject_strict_recovery_when_incomplete_active_tail_crosses_durable_boundary` (streaming_wal_plan/tests.rs:1090). | streaming.rs:634-733, 737-792, 863-950; recovery.rs:136-141, 263-276; planner.rs:531-596, 294-303; streaming_wal_plan.rs:385-468; runtime/state/recovery.rs:531-689; cloud_startup/mod.rs:92-198; wal_history.rs:507-555, 680-703, 748-802; state.rs:1344-1363; event_loop/mod.rs:270-273 |
| 5 | **replay** | `streaming::replay_wal_with_options` (wal/recovery/streaming.rs:278) is the one engine (#524). Entry wrappers: `recovery::replay_wal_with_frontiers_within` (recovery.rs:429, local), `CloudReplay::replay` (engine/startup/streaming_recovery.rs:92, cloud, with checkpoints), `recovery::validate_wal_with_policy` (recovery.rs:412, storage verification, `ValidateOnly`). Selection: `ReplaySelection` (wal/replay.rs:9). Segment list: `ReplaySegments::from_ledger` -> `planner::local` (recovery.rs:61; planner.rs:307). **Straddles** wal/recovery, wal/planner, wal/replay, runtime/state, engine/startup. | Reads each planned segment: header admit + chain link + ledger/catalog expectation, frame walk, footer check; applies a transaction unit only if `commit_sequence > family frontier` (replay.rs:25-34); split-marker transactions spill to an anonymous temp file locally. **Read-only**: it reports `tolerated_active_tail` / `salvage_stop` and leaves the repair to the caller (recovery.rs:303-311). Cloud replay checkpoints memtables to SSTs mid-replay (streaming_recovery.rs:131-151). | `Err` from `replay_paths` (`Corruption`, `ResourceLimit`, `Timeout("WAL replay deadline expired")`, `InvalidArgument` for bad limits) fails the open. Cloud replay additionally turns an unplanned `salvage_stop` into `RecoveryFailed` (streaming_recovery.rs:158-163). | `should_replay_every_member_when_files_match_the_ledger` (wal/recovery/tests/ledger_chain.rs:80, rg) and the `should_fail_strict_replay_when_*` family (:94-192, rg); `should_select_whole_unit_when_commit_exceeds_its_family_frontier` etc. (wal/replay.rs:52-84); `should_fail_closed_when_replay_family_has_no_persisted_frontier` (replay.rs:76). Note the unit-level fixtures use `durable_bytes: None` (recovery/fixture.rs:58, 76) so they do **not** exercise the production floor branches. | streaming.rs:278-321, 634-733; recovery.rs:16-20, 61-67, 379-453; replay.rs:9-36; planner.rs:305-332; streaming_recovery.rs:92-187 |
| 6 | **shutdown** | Three layers, two paths. Graceful: `Engine::shutdown` (engine/mod.rs:618) -> `RuntimeHandle::shutdown` (runtime/handle.rs:411) -> event loop `handle_shutdown` (runtime/event_loop/shutdown.rs:18) -> `LeaseState::schedule_cleanup` (engine/lease_state.rs:209). Drop: `Engine::drop` (engine/mod.rs:120) -> `LeaseState::detach_reaper` (lease_state.rs:306) -> `Runtime::drop` -> `shutdown_inner` (runtime/runtime.rs:206). Admission gate: `RuntimeLifecycle` (runtime/lifecycle.rs). | See section D. Local shutdown = one paired WAL sync (fsync + `SyncOpen`), reject held work, drain in-flight flush, join workers. **No local segment is sealed or rotated at shutdown**; the open segment stays open and `start_epoch` seals it at the next open. Cloud adds final seal + upload drain + active-memtable checkpoint, all under `shutdown_cloud_drain_timeout`. | `Busy("N transaction(s) are still active")` and retryable if a transaction is live (handle.rs:426-432); `Timeout` if the caller deadline lapses (cleanup retryable, caller deadline does not cancel IO); terminal `Timeout`/other durability error from the runtime is **replayed on every later call** (`ShutdownTerminal`, handle.rs:421-423, engine/mod.rs:603-610). Held callers get `Busy("runtime is shutting down")` (shutdown.rs:533-535). Local WAL-sync failure is recorded as `shutdown_error` but shutdown continues (shutdown.rs:28-33). | `should_fsync_wal_on_clean_shutdown_when_buffered_commits_are_pending`, `should_complete_wal_durability_waiters_when_local_shutdown_syncs` (shutdown.rs:615, 637); `should_allow_shutdown_retry_when_live_transaction_is_released` (engine/tests.rs:566); `should_reap_engine_without_blocking_drop_when_transaction_is_live` (engine/tests.rs:599); `should_release_primary_lease_given_shutdown_timeout_when_shutdown_completes` (tests/fault_injection.rs:5493, rg); `should_retain_primary_lease_when_engine_drops_with_blocked_wal_upload` (:5624, rg). **None** for "process killed mid-shutdown" (general crash campaigns only) and **none** for a leaked transaction bounding the reaper (#933 AC1/AC2). | shutdown.rs:6-152, 208-258, 421-465; handle.rs:411-488; engine/mod.rs:120-132, 602-668; lifecycle.rs:95-159, 180-191; runtime.rs:206-232; lease_state.rs:209-258, 306-351 |

Findings from the ownership column (for the doc's "two owners / straddles layers" list):
1. **Rotation is implemented twice**: runtime path (`WalActor::rotate`, writer-thread footer) and recovery path (`WalHistory::start_epoch` + free functions `seal_scanned` / `write_footer`, wal_history.rs:507-555, 856-882). They share only `prepare`-style helpers (`create_segment`) and the ledger edit.
2. **The durable-floor tolerance rule is implemented twice**: `stop_at` (streaming.rs:874-894) and `probe_open_member` (streaming_wal_plan.rs:408-421), with different resulting error variants.
3. **`runtime::state` owns WAL-file repair** (truncate, set_aside) while `wal/` owns detection; the cloud equivalent lives in `runtime::cloud_startup`. Lower layer (`wal/recovery`) reports; upper layers mutate files.
4. **State name vs error variant mismatch** (`Fenced` state yields `RecoveryFailed` unless authority was lost), see top.
5. **Stale "rename" language after #843**: `SegmentObligationState::Sealed` says "The actor renamed the active file" (runtime/wal_transition.rs:117), `settle_failed_seal_step` says "once the rename happened" (event_loop/wal_transition.rs:60-61), `commit_rotated_cloud_seal` says "proves the active file was renamed" (sealing.rs:169-172). Rotation is footer + ledger edit now (rotation.rs:3-25).
6. **Local rotation is not size-triggered**: the only local caller of rotate is the flush-driven prune path (wal_retention.rs:85-99, sole call site `rg` of `seal_active_wal_for_prune`); a local open segment grows until a flush/prune cycle, and start-up `start_epoch` re-reads the *entire* open segment (`scan_member`, wal_history.rs:885-894) to seal it, so reopen cost is O(open segment size).

#### A. LEDGER record vocabulary and the "O(ledger) read per strict group" claim

##### A.1 Format (wal/ledger_store.rs:1-25, 34-61)
`LEDGER` = 12-byte header (`MDGLEDGR` + u32 version 2) then frames `[u32 len][u32 crc32c over len+payload][JSON payload]`. First frame is `Snapshot(WalLedger)`; every later frame is `Edit(LedgerEdit)` applied with `WalLedger::apply`; an edit that does not apply on load is corruption. A torn final frame is `LedgerTail::Torn` and ignored; a bad frame before a valid frame is corruption (`next_frame`, :403-425).

Snapshots (not edits) are written by `create`, `open` (every open republishes), `compact` (every 1024 appends and on heal): tmp + fsync + rename + dir fsync (ledger_store.rs:262-279, 354-364).

##### A.2 LedgerEdit variants (ledger.rs:98-142)
| Variant | Meaning (ledger.rs) | Writer (who / when) | Fsynced? |
|---|---|---|---|
| `Rotate{sealed, open}` | Seal open member with footer facts, open next (`rotate`, :298-316) | `record_rotation_inner` (wal_history.rs:481-484) from `WalActor::record_rotation` (rotation.rs:237-260), event-loop thread, every local and cloud seal; also `start_epoch_inner` (wal_history.rs:546-549) at open when the recovered open segment holds records | Yes, own `sync(Durable)` per append |
| `Reopen{open}` | Replace a record-less open member with a new id/epoch (:318-328) | `start_epoch_inner` (wal_history.rs:535-537) at open when the open segment is empty and from an older epoch or has a footer | Yes |
| `AdvanceLogStart{to}` | Retire all members below `to` (:330-349) | `retire_below_inner` (wal_history.rs:716-733) via `WalActor::retire_wal_below` (wal.rs:978-989) from `retire_wal_below_or_retain` (wal_retention.rs:50-72), and cloud `Keep` plan | Yes |
| `SetAside{from, cut, open}` | Salvage: drop members >= `from`, optionally reseal last kept with the cut prefix, open new (:351-406; clamps `durable_bytes` to `cut.data_len`, :399-402) | `set_aside_inner` (wal_history.rs:756-802): local salvage (runtime/state/recovery.rs:639) and cloud plan (cloud_startup/mod.rs:174) | Yes |
| `Rebase{anchor, open}` | Drop every member, continue from a foreign anchor (cloud failback) (:408-421) | `rebase_inner` (wal_history.rs:656-676): cloud plan (cloud_startup/mod.rs:187), substituted-WAL retirement (streaming_recovery.rs:275) | Yes |
| `Reserve{through}` | Raise sequence reservation; must strictly increase (:463-472) | `WalHistory::reserve` (wal_history.rs:564-566): `begin_wal_epoch` (state.rs:1359) at open, and `allocate_sequences` (state.rs:1286-1287) on the **commit hot path** whenever fewer than 2^19 sequences remain of a 2^20 block (sequence.rs:14, 46-52); i.e. once per ~524k sequences | Yes |
| `SyncOpen{id, through_bytes}` | Record the open member's fsynced byte prefix (`durable_bytes`); must be >= current (:444-461) | `record_sync_inner` (wal_history.rs:609-622) from `persist_sync_boundary` (durability.rs:264-284), after every WAL fsync that advances the position, **before** waiters are acked | Yes |
| `SalvageOpen{id, valid_bytes, floor}` | After salvage authority commits `floor`, lower `durable_bytes` to `valid_bytes` and raise the reservation to `floor` (:423-442) | `prepare_open_cut` (wal_history.rs:625-637), **cloud recovery only** (cloud_startup/mod.rs:131) | Yes |

Local salvage does not use `SalvageOpen`; it cuts and seals through `SetAside{cut}`.

##### A.3 Which modes pay `SyncOpen`
`persist_sync_boundary` runs inside `begin_sync_io` (durability.rs:257), which serves: strict groups (durability.rs:40-62), the paired event-loop sync (`sync_wal_generation`, durability_sync.rs:162-253: Batched windows, `sync_current_wal` for DDL/shutdown, pre-prune sync). It does **not** run for `CloudAsync`: `flush_for_cloud_upload_within` fsyncs the writer directly (durability.rs:393-401) and never calls the history. So in `CloudAsync` mode `durable_bytes` stays at its opening value (`HEADER_LEN`, ledger.rs:89-95) until rotation. **A floor-style `durable_bytes` therefore already exists in production for CloudAsync.**

##### A.4 The #904 "re-reads the entire ledger file" claim: REFUTED for production, TRUE for unfenced stores
- `write_frame` compares the whole file to `self.image` only `if let Some(image) = &self.image` (ledger_store.rs:295-306).
- `image` is `None` when the store is opened under the writer fence: `image: (!writer_fenced).then(Bytes::new)` (ledger_store.rs:156, 189) and the struct doc says "Production stores under `WalHistoryFence` omit this image so appends do not reread and recopy the full journal for every edit" (:77-80).
- Production always supplies the fence: local non-memory open passes `startup_lease.wal_history_fence(..)` (engine/startup/storage.rs:1308-1311); cloud history is opened with `load_cloud_wal_history(.., &wal_fence, ..)` and `commit_recovery_plan_within(.., wal_fence, ..)` (storage.rs:207-208, 283-299; cloud_startup/mod.rs:133-160). Unfenced `WalHistory::open/load/genesis` are test/fixture entry points.
- What the fenced path does cost per `SyncOpen`: `self.ledger.clone()` + `apply` (O(members), each member carries `SealedFacts` with a per-family Vec), one JSON frame, `open` + `len()` + `write_at` + **fsync** (ledger_store.rs:217-229, 281-316), the leader-store epoch proof (`with_writer_effect`; native groups amortize it to one proof per strict group, test `should_reuse_one_native_fence_when_strict_group_appends_syncs_and_records_history`, actors/wal/tests/writer_groups.rs:104-169), and a synchronous compaction (tmp + fsync + rename + dir fsync) every 1024 edits on the commit path (:237-241).
- So the real per-group overhead is **a second fsync** (confirmed) plus O(members) CPU, not an O(ledger-bytes) read. The "per-commit read grows until compaction" part of the failure scenario does not apply to the fenced path. It would apply to `LedgerStore::open/create` without a fence.
- Measurement trap for #904 AC1: `wal_fsync_count` is incremented only in `writer_runner.rs:564`; `LedgerStore::write_frame` fsyncs through `file.sync` (ledger_store.rs:315) and is not counted. The existing test `should_issue_one_physical_wal_sync_when_non_empty_sync_transaction_commits` (tests/durability_sync_count.rs:76-101) already asserts `1` while two real fsyncs occur. "fsync count halves" cannot be observed with the current counter.

#### B. Recovery outcome taxonomy

##### B.1 Policy plumbing
- `RecoveryPolicy::{Strict, Salvage}` -> `ReplayPolicy::{Strict, SalvageValidPrefix}` (recovery.rs:16-20; mapping runtime/state/recovery.rs:598-603 and streaming_recovery.rs:110-115). Cloud also feeds the policy to `planner::plan` (planner.rs:355-396), where a "hole" is `Unreplayable` under strict (-> `RecoveryFailed`, planner.rs:294-299) and cut + set-aside under salvage.
- `ReplayErrorAction::{TolerateFinalActiveTail, SalvageVerifiedPrefix, Fail}` (recovery.rs:136-141).
- Base rule `replay_error_action` (recovery.rs:263-276): final active file + `IncompleteTail` -> Tolerate; Salvage policy + salvageable (`Corruption`, common/error.rs:165-167 via rg) -> Salvage; else Fail.
- `FrameError::{IncompleteTail, Corrupt, ResourceLimit}`: IncompleteTail = short header, zero-filled tail, etc. (frame.rs:346-372, 545-559).
- Floor-aware override in `stop_at` (streaming.rs:874-894), with `durable_bytes = member.durable_bytes` for ledger members and `None` for published objects:
  1. final active AND `durable = Some(d)` AND `valid_bytes >= d` AND error salvageable (**any `Corruption`**, not only incomplete tail) -> **Tolerate** (tail is unacked by definition).
  2. final active AND `valid_bytes < d` AND incomplete-tail -> **lost write**: Salvage -> SalvageVerifiedPrefix, Strict -> Fail.
  3. otherwise the base rule (so `durable = None` only tolerates `IncompleteTail`).
- A tolerated tail is only reported (`tolerated_active_tail`); the local caller then truncates durably under the writer fence (runtime/state/recovery.rs:617-624, 667-689) and `start_epoch` later seals the prefix. The comment at streaming.rs:872-873 states the design intent: "Only persisted fsync evidence licenses treating corruption as an unsynced open tail. Never guess from zeros or checksum symptoms."

##### B.2 Per-case table
| Case | Strict | Salvage | Pinning test(s) |
|---|---|---|---|
| **Torn tail** (incomplete frame at or after `durable_bytes`, open segment) | Tolerated, truncated to verified prefix, open succeeds. Cloud: `OpenEnd::Torn` -> `OpenCut` (planner.rs:560-580; streaming_wal_plan.rs:408-414) | same | `should_recover_post_restart_sync_commit_when_previous_crash_left_torn_wal_tail` (tests/durability.rs:783); `should_skip_corrupted_wal_tail_given_truncated_tail_when_recovering` (tests/durability.rs:441); `should_open_strictly_when_power_loss_tears_unsynced_multi_page_tail` (tests/storage_root_injection.rs:811); `should_ignore_only_incomplete_final_active_wal_tail_given_truncated_frame_when_recovering` (wal/recovery/tests.rs:1526); `should_truncate_only_incomplete_active_wal_tail_before_virtual_replay` (streaming_wal_plan/tests.rs:479, rg) |
| **Zero-filled tail** | Tolerated | Tolerated | `should_tolerate_zero_filled_final_active_wal_tail_given_strict_recovery` (wal/recovery/tests.rs:1702); cloud `should_recover_valid_active_wal_prefix_when_tail_is_zero_filled` (streaming_wal_plan/tests.rs:1066) |
| **Bytes below the durable floor** (acked data lost) | File shorter than `durable_bytes`: `admit_segment` rejects the segment with `Corruption("... shorter than its persisted fsync boundary")` (streaming.rs:759-760) -> open fails with `Corruption`. Incomplete frame below floor with file long enough: `Fail` via branch 2. Cloud: `OpenEnd::Damaged` -> `RecoveryFailed("active WAL failed validation ...")` (streaming_wal_plan.rs:415-442; planner.rs:563-568) | Local: `admit_segment` failure is routed through `stop_at` with `valid_bytes = 0` (streaming.rs:660-664), so base rule picks SalvageVerifiedPrefix at offset 0, `holds_no_records(0)` is true, and `set_aside(From(segment))` quarantines the **whole open segment including its valid prefix** (runtime/state/recovery.rs:626-639). Cloud keeps the verified prefix and cuts (planner.rs:560-580). **Code-reading finding, not verified by a test; local and cloud salvage disagree.** | Strict local: `should_fail_strict_given_missing_acknowledged_wal_tail_when_reopening` (tests/durability.rs:735). Strict cloud: `should_reject_strict_recovery_when_incomplete_active_tail_crosses_durable_boundary` (streaming_wal_plan/tests.rs:1090). Salvage: **none found**. `stop_at` branch 2 (streaming.rs:883-891) has **no direct unit test**: wal/recovery unit fixtures use `durable_bytes: None` (recovery/fixture.rs:58, 76). |
| **Mid-file corruption, open segment, at/after floor** | Branch 1: tolerated as tail and truncated (acked bytes are impossible there by definition) | same | `should_fail_strict_recovery_given_corrupted_length_before_valid_active_wal_suffix` (wal/recovery/tests.rs:1640, **with `durable=None`**, so it pins the evidence-less behaviour: strict fails); cloud `should_fail_strict_recovery_without_truncating_when_corrupt_length_hides_valid_suffix` (streaming_wal_plan/tests.rs:1110, ledger records full length as synced, `RecoveryFailed`) |
| **Corruption in a sealed segment / sealed segment without footer** | Fail (`Corruption`); sealed members never get tail tolerance (`ReplayFileKind::Sealed`, streaming.rs:846-855) | Salvage verified prefix; `salvage_stop` -> `set_aside(After(cut))` (wal_history.rs:756-802) | `should_fail_strict_recovery_given_corruption_in_a_sealed_wal_segment` (wal/recovery/tests.rs:1421, rg); `should_salvage_verified_prefix_given_corruption_in_a_sealed_wal_segment_when_salvage_policy_is_enabled` (:1472, rg); `should_fail_strict_replay_when_sealed_member_ends_without_footer` (recovery/tests/ledger_chain.rs:171, rg); `should_set_aside_unreplayed_wal_when_salvage_stops_at_corrupt_segment` (runtime/state/recovery.rs:1591); `should_reopen_strictly_after_salvage_sets_aside_unreplayed_wal` (:1649) |
| **Missing segment file** (ledger member, file gone) | `replay_paths` -> `stop_at` with `ReplayFailure::Record(Corruption("... is history but its file is missing"))` (streaming.rs:644-658) -> Fail `Corruption` (test accepts `RecoveryFailed` or `Corruption`, runtime/state/recovery.rs:1447). Writer-side: `FsWalFactoryIo::open_segment` refuses with `Corruption` (factory_io.rs:111-117 test); rotation with the open file gone: `RecoveryFailed`, WAL fenced (wal_history.rs:458-465; actors/wal/tests.rs:776-779). Cloud: `Corruption("... is history but its file is missing")` (streaming_wal_plan.rs:476-480) | Replay stops at the hole; later members set aside, history continues past every handed-out id (`(vec![1], 4)`), floor raised | `should_fail_strict_recovery_when_a_ledger_member_file_is_missing`, `should_stop_salvage_at_a_missing_ledger_member` (runtime/state/recovery.rs:1435, 1457); `should_refuse_writer_when_ledger_segment_is_missing` (wal/fs/factory_io.rs:106); `should_fence_rotation_when_open_segment_is_missing_with_pending_records` (actors/wal/tests.rs:744); cloud `should_fail_strict_recovery_naming_object_when_cataloged_segment_is_missing` (streaming_wal_plan/tests.rs:1135, rg) |
| **Orphan segment** (file not in LEDGER) | `WalHistory::load_inner`/`genesis_inner` always run `sweep_orphans` (wal_history.rs:236-257, 349, 680-703): non-member with id below `next_segment_id`, or at/above it but holding no record, is deleted; a non-member **with records** at id >= `next_segment_id` -> `Corruption("... holds records the WAL ledger never recorded")`. Segment files with no LEDGER at all: `RecoveryFailed("exists without a WAL ledger")` (wal_history.rs:939-943). Policy-independent | same | `should_sweep_unrecorded_next_segment_when_rotation_crashed_before_recording_it` (wal_history/tests.rs:288); `should_refuse_creating_segment_over_unrecorded_records` (:312); `should_refuse_genesis_when_segment_files_exist_without_ledger` (:220, rg); `should_capture_only_ledger_members_when_wal_directory_holds_orphans` (engine/backup.rs:742, rg) |
| **Empty (header-only) segment** | `holds_no_records(len <= 128)` (format/segment.rs:48-50). Never sealed, never owed to the cloud: same-epoch open empty segment is kept; older-epoch one is replaced by `Reopen` (wal_history.rs:528-538). Not rotated at prune time (wal_retention.rs:86-88). Header-only orphans are swept; `create_segment` rewrites a header-only file (wal_history.rs:967-993) | same | `should_count_a_header_only_segment_as_holding_no_records` (format/segment/tests.rs:184); `should_not_seal_empty_open_segment_when_reopening` (wal_history/tests.rs:593); `should_not_rotate_local_wal_when_active_segment_is_empty` (flush_pipeline/tests.rs:1219); `should_adopt_header_only_segment_when_genesis_crashed_before_ledger` (wal_history/tests.rs:249, rg) |
| **LEDGER itself torn** | Torn final frame ignored (`LedgerTail::Torn`), first `open` republishes a clean snapshot; bad frame followed by valid frame, bad snapshot, wrong magic/version -> `Corruption` / `CompatibilityError` (ledger_store.rs:403-480) | same | `should_report_torn_*`, `should_fail_when_middle_frame_is_corrupt`, `should_fail_when_frame_length_hides_later_frames`, `should_drop_torn_tail_when_opening` (wal/ledger_store/tests.rs:355-477, 704) |

Dead-code observation: `Member::durable_bytes` is `Option` only so a LEDGER image without the field loads as `None` (`#[serde(default)]`, ledger.rs:78-81; test `should_load_legacy_open_member_without_durable_byte_evidence`, ledger/tests.rs:1065). LEDGER format version is 2 and only 2 is readable (ledger_store.rs:39, 435-439), and `Member::opened` always writes `Some` (ledger.rs:89-95). Under the repo's "no back-compat" rule the `None` arm is reachable only from the recovery test fixtures, which makes the unit tests exercise a path production cannot reach.

#### C. What `durable_bytes` means and what #904 would change

##### C.1 Meaning today
`Member.durable_bytes` (ledger.rs:78-81): "Prefix fsynced before acknowledgement". Concretely it is `writer.current_pos()` read **after** the WAL fsync returns and **before** any waiter is acked (durability.rs:171-257 then 264-284, `commit_sync_io` 286-313). `current_pos` is advanced by the writer thread after each write (writer_runner.rs:302-305, 472-475) and the event loop is the only appender, so at that moment it equals the fsynced length. It is exact for Strict and Batched; it is stale (header only) for CloudAsync (A.3).

Who moves it:
- raises: `SyncOpen` only (monotone, ledger.rs:444-461; no-op skipped at wal_history.rs:609-617).
- resets: new member -> `HEADER_LEN` (ledger.rs:89-95) on `Rotate`, `Reopen`, `SetAside`, `Rebase`, genesis.
- lowers: `SalvageOpen` (min, ledger.rs:435-439), `SetAside{cut}` (min with `cut.data_len`, ledger.rs:399-402), planner clamp when a cut is planned (planner.rs:581-587).
- invariants checked on load: `>= HEADER_LEN`, and `<= sealed.data_len` for sealed members (ledger.rs:682-692).

##### C.2 Every reader and every decision keyed on it (non-test source)
1. `streaming.rs:759-760` `admit_segment`: file shorter than the floor -> `Corruption` (applies to any `Expectation::Member`, including storage verification via `ReplaySegments::from_ledger`, engine/verification.rs:304).
2. `streaming.rs:874-894` `stop_at`: tolerate-vs-lost-write decision (B.1).
3. `streaming_wal_plan.rs:385-421` `probe_open_member` (called with `member.durable_bytes`, :123): `Torn` vs `Damaged` for cloud planning.
4. `planner.rs:581-587` `admit_open`: clamps the expectation to `valid_bytes` after a planned cut.
5. `ledger.rs` `sync_open`, `salvage_open`, `set_aside`, `TryFrom<LedgerImage>`: bookkeeping/invariants.
6. `wal_history.rs:609-621` `record_sync_inner`: skip logic.
Nothing else reads it (rg over src/tests, excluding test files, found only these). Metrics fields named `*_durable_bytes` in `metadata/accounting.rs` are unrelated.

##### C.3 Effect of making it a low-frequency floor
Let `A` = true acked end of the open segment, `F <= A` the recorded floor, `V` = end of the verified prefix at recovery.

- **Torn-tail tolerance does not regress**: branch 1 needs `V >= F`; since `F <= A <= V` for an honest crash this is a superset. The registered RED case `torn-unsynced-tail` (storage_root_injection.rs:807-832) stays green.
- **Lost-write detection goes from exact to floor-based**: today any loss that leaves `V < A` is caught (file shorter than `A`, or incomplete frame below `A`). With a floor, loss or damage with `F <= V < A` falls into branch 1: strict recovery **silently tolerates it, truncates it durably** (runtime/state/recovery.rs:667-689), `start_epoch` seals the shortened prefix, and the DB opens clean (`opened_in_salvage_mode` false). The silent window is exactly the commits acked since the last floor edit. Only the **open** segment is exposed: sealed segments carry exact `data_len` + digest in their footer and ledger facts (ledger.rs:610-628; streaming.rs:794-815).
- Mid-file **corruption** (not just truncation) in `[F, A)` is also reclassified as a torn tail, because branch 1 accepts any salvageable `Corruption`.
- What the check protects against in practice: operator/filesystem truncation (the pinned test), fsync that lied or a filesystem that reordered metadata, media damage, and engine bugs. It does not protect against ordinary process crash or honest power loss (those cannot lose acked bytes).
- Precedent: CloudAsync local open segments already run with a header-only floor (A.3); their loss protection is the cloud copy, not the local floor.
- `R1` of #904 as written ("derive the durable prefix from the segment: last complete frame before first invalid one") removes the evidence entirely, which is the `durable_bytes = None` classification: only `IncompleteTail` is tolerated and any CRC failure fails strict (base rule, recovery.rs:263-276). That is stricter than today for torn unsynced tails whose later pages persisted out of order (the multi-page RED case), so it would **regress** `should_open_strictly_when_power_loss_tears_unsynced_multi_page_tail`, violating #904 R4. The segment alone cannot distinguish "never acked" from "acked then damaged".
- Edits that must stay exact regardless: `Rotate` (carries `sealed.data_len`, exact by footer), `SalvageOpen` floor, `SetAside` clamp.

##### C.4 Recommendation on #904: **MODIFY**
- **Reject** R1 as written (segment-derived prefix with no persisted evidence) and "floor = rotation only".
- **Accept** the goal (drop the per-group LEDGER fsync) via an **amortized, bounded-window floor**: write `SyncOpen` when any of {N syncs, B bytes since last edit, T ms} trips, plus a mandatory exact edit at clean shutdown (`sync_current_wal` already runs there, shutdown.rs:28-33) and exactness at rotation (footer). Make the contract explicit: "lost-write detection is exact for bytes older than the last floor edit; inside the window it is classified as a torn tail." N=1 reproduces today.
- The existing pinned lost-write test (tests/durability.rs:735) calls `engine.shutdown` **before** truncating, so an exact-at-shutdown edit keeps it green; but no test covers the crash-then-truncate variant, so the new contract needs a new failing-first test for the window edge (truncate at `F-1` fails strict; truncate within `[F, A)` opens and drops).
- **Reject R3 as a motivating item** (the full-file image comparison is already off the production path, A.4). Keep the image check only for unfenced stores.
- Cheap non-semantic savings to evaluate first, because they keep exactness: avoid the per-append `ledger.clone`, keep the LEDGER file handle open, and benchmark `fdatasync` vs full for the LEDGER file. These do not remove the second fsync.
- Option to design (unverified): lag-one floor, i.e. persist the *previous* group's end offset inside the next group's own WAL fsync (zero extra fsync); window is then the most recent group only, which is the common loss case, so likely insufficient alone.
- #904 AC1 needs a counter that includes LEDGER fsyncs (A.4). AC3 needs a **new** restart test: the failpoint `midge::wal::after_fsync_before_durable_frontier` (durability.rs:331-336) sits exactly between the WAL fsync and `persist_sync_boundary`, but `rg` finds no test that configures it. The in-process sync matrix (durability_sync.rs:892) swaps in a `PanickingSyncWriter` and an in-memory history, so it does not reach the LEDGER at all.

#### D. Shutdown

##### D.1 Graceful path, ordered (src/runtime/event_loop/shutdown.rs:18-152, preceded by engine/handle steps)
1. `Engine::shutdown`: if a prior cleanup is pending, wait on it (engine/mod.rs:620-622).
2. `RuntimeHandle::shutdown`: replay a terminal result if one exists; `lifecycle.begin_shutdown()` (state `Open -> Closing`, then take and drop the submission write lock so in-flight sends finish; new work gets `Busy("engine is shutting down")`, lifecycle.rs:143-159, 126-132); if `active_transactions != 0` return `Busy` (handle.rs:426-432; engine/mod.rs:636-638 returns it without waiting). Retryable.
3. Send `ShutdownWithResponse`; if a storage-verification barrier is active the message is deferred (shutdown.rs:7-16).
4. `handle_shutdown`:
   - `sync-current-wal`: **local only**: `sync_wal_generation` = fsync + `SyncOpen` + complete the waiters of that generation. CloudAsync returns `Ok` without syncing (durability_sync.rs:359-367). Error is recorded, not fatal (shutdown.rs:28-33).
   - `fail_shutdown_held_work`: reject deferred/held messages, flush-barrier, write-stall, durability and inline waiters with `Busy("runtime is shutting down")` (shutdown.rs:421-465).
   - drain compaction publication, then `shutdown_and_join` that actor.
   - cloud: drain flush pipeline; if `pending_writes > 0` seal the final segment (`seal_current_cloud_segment_within` = flush + rotate); drain uploads until `shutdown_cloud_drain_timeout` (`Timeout("shutdown timed out with N storage-owned and M runtime-owned cloud uploads")`); if no error so far, freeze and flush every active memtable (checkpoint). Local: only wait for an already in-flight flush (shutdown.rs:154-166); **no checkpoint**, buffered commits live only in the WAL.
   - join flush, compaction, GC, WAL-prune and storage-prune workers; `invalidate_unsettled_flush_accounting`; reject held work again; respond `Ok` or the first recorded error; `HandleOutcome::Break`.
5. `Engine::shutdown`: wait for the runtime to exit, then `schedule_cleanup` on a "midge-fencing-reaper" thread that releases heartbeat, lease, guard and storage, and wait for it (engine/mod.rs:639-668; lease_state.rs:209-258).

What is flushed/sealed/synced: local = one WAL fsync and one `SyncOpen`; nothing sealed, nothing rotated, no SST written. Cloud = final seal (footer, `Rotate`, upload), upload drain, memtable checkpoint. The next open therefore always finds an **open** local segment holding records, replays it, and `start_epoch` seals it (footer + create + `Rotate`) and then `Reserve`s.

##### D.2 Caller still holds a transaction
- `Engine::shutdown`: `Busy`, engine stays `Closing`, new transactions refused, caller may drop the transaction and call again (test `should_allow_shutdown_retry_when_live_transaction_is_released`, engine/tests.rs:566).
- `Engine::drop`: `detach_reaper` moves the runtime and fencing resources into a "midge-engine-reaper" thread (lease_state.rs:306-351). Its `drop(runtime)` runs `Runtime::shutdown_inner`: `begin_shutdown`, **`wait_for_transactions` (unbounded condvar wait, lifecycle.rs:180-191)**, send `RuntimeMsg::Shutdown` (no response), `join_until(Duration::MAX)` (runtime.rs:206-219), then release the lease. So a leaked transaction pins the reaper and the lease forever. This confirms #933's problem. Corrections to #933's citations: the waiting is `lifecycle.rs:180-191` / `runtime.rs:211`, not `lifecycle.rs:109-119` (that is `release`), and there is no `Engine::close()` in the tree, the graceful API is `Engine::shutdown(timeout)`. Pinned: `should_reap_engine_without_blocking_drop_when_transaction_is_live` (engine/tests.rs:599) proves drop does not block and the lease is held until the transaction drops; nothing pins a bounded wait.

##### D.3 Process killed mid-shutdown
Shutdown writes no LEDGER edit except the final `SyncOpen`, and local shutdown seals nothing, so every intermediate state is an ordinary crash state: open segment (maybe with a tail beyond `durable_bytes`), LEDGER at its last fsynced edit. The lease is not released and expires by TTL (tests use `expire_crashed_process_lease`, tests/fault_injection.rs:6172-6173). Cloud kill mid-drain leaves a sealed local segment plus a possibly unpublished catalog entry, recovered through the catalog/ledger lineage. No test kills the process specifically inside `handle_shutdown`; coverage is the generic crash campaigns (tests/wal_campaigns.rs:46, 68, 154) and the abort-child scenarios.

#### E. Rotation partial-failure states

Order of durable effects (rotation.rs:123-156 + wal_history.rs:434-484): header file create+fsync -> dir fsync -> footer append+fsync -> LEDGER `Rotate` append+fsync -> writer open on N+1. Crash or error points, with recovery outcome:

| # | Point | Disk state | Actor state on in-process failure | Recovery on next open | Failpoint / test |
|---|---|---|---|---|---|
| E0 | before `begin_io_transition`: lease check, fenced, ticket mismatch | unchanged | lease -> Fenced (authority_lost); ticket mismatch -> Fenced (poisoned, `RecoveryFailed`) | normal | `should_preserve_open_segment_when_validity_expires_before_next_segment_create` (tests.rs:3404) |
| E1 | after seal digest (no IO) | unchanged | Open (rolled back) | normal | failpoint `midge::wal_transition::after_seal_digest` in `LOCAL_ROTATION_BOUNDARIES` (tests.rs:1752, event_loop/wal_transition.rs:304) |
| E2 | before next-segment create | unchanged | Open | normal | `midge::wal::inject_fail_before_segment_create` (rotation.rs:324-331) and `midge::wal_transition::before_segment_create`; tests.rs:3404, 1752 |
| E3 | next file created, header not yet durable / dir entry not durable (crash) | N+1 absent, empty, or partial header (<= 128 bytes) | n/a (crash) | N+1 is a non-member with no records -> `sweep_orphans` removes it (wal_history.rs:690-700); next rotation recreates it | `should_sweep_unrecorded_next_segment_when_rotation_crashed_before_recording_it` (wal_history/tests.rs:288, process crash only; no power-loss variant of this exact point) |
| E4 | after create, before footer (`AfterSegmentCreate`) | N+1 header-only, N open without footer | Open, retryable; second `rotate` succeeds (`create_segment` rewrites header-only file, wal_history.rs:967-993) | as E3 | `midge::wal_transition::after_segment_create`; asserted at tests.rs:1798-1805 (file exists only for this boundary, retry works) |
| E5 | footer partially written or unsynced (crash) | N ends in torn footer frame | in-process: footer write error -> Fenced with `sealed=N` | final-active torn tail with `V = data_len >= durable` -> tolerated and truncated by the recovery caller, then `start_epoch` writes a fresh footer + `Rotate` | in-process: `should_fence_when_footer_write_fails` (tests.rs:3488). **Restart from a torn writer-footer: no direct test**; closest `should_keep_recovered_tail_when_power_is_lost_after_its_footer_append` (wal_history/tests.rs:533) covers `start_epoch`'s own footer |
| E6 | footer durable, ledger not yet (`AfterFooter`) | N has valid footer; N+1 header-only; ledger says N open | Fenced, `sealed=N` | replay accepts footer on open member; `start_epoch` -> `seal_scanned` only fsyncs existing footer and records it in `Rotate`; N+1 orphan swept then recreated | `midge::wal_transition::after_footer`; `should_record_footered_open_member_sealed_when_rotation_crashed_before_ledger` (wal_history/tests.rs:462), `should_reopen_with_sealed_segment_when_power_is_lost_after_footer_before_rotate` (:433) |
| E7 | `Rotate` frame partially written (crash) | LEDGER ends in torn frame | in-process: ledger write error -> Fenced `sealed=N` ("may still have become durable", rotation.rs:23-25) | `LedgerTail::Torn` ignored -> E6 | `should_drop_torn_tail_when_opening` (ledger_store/tests.rs:704); `should_leave_old_or_new_ledger_when_power_fails_during_append` (:948, rg); `should_reopen_prefix_of_history_when_power_is_lost_after_rotation` (wal_history/tests.rs:411) |
| E8 | `Rotate` durable, writer not opened (`AfterLedgerRotate` / `inject_fail_after_ledger_rotate_before_writer_create` / writer open error) | ledger: N sealed, N+1 open (header only, 0 records); actor has no writer | Fenced, `sealed=N`, ledger already reports N+1 | replay includes N sealed + N+1 empty; `start_epoch`: empty open member from an older epoch -> `Reopen` to N+2 (kept if same epoch) | tests.rs:1752 (matrix), `should_fence_filesystem_wal_when_next_writer_open_fails_after_ledger_rotate` (tests.rs:1993), `should_recover_only_accepted_write_when_rotation_loses_replacement_writer` (tests/fault_injection.rs:6284), `should_not_seal_empty_open_segment_when_reopening` (wal_history/tests.rs:593) |
| E9 | writer installed, `finish_io_transition` not reached (`midge::wal::inject_fail_after_writer_create_before_commit`) | fully rotated on disk | Fenced `sealed=N` | normal (E8 recovery, N+1 may hold zero records) | `should_recover_only_accepted_write_when_rotation_stops_after_writer_installation` (tests/fault_injection.rs:6300) |
| E10 | event-loop side after `Rotate` (note_sealed/finish_seal/retire_local_seal; cloud registration/accounting) | complete on disk | protocol + actor fenced, waiters failed once | normal | `CLOUD_SEAL_BOUNDARIES` matrix (wal_transition_boundary.rs:57-69; tests not read); cloud `inject_fail_after_wal_rotate_before_enqueue` (sealing.rs:98-103) |

Gaps: (1) the two local rotation matrices run on a segment with no records; (2) no restart test from a torn *writer* footer (E5); (3) no power-loss variant of E3 (only `process_crash`); (4) `drained_digest` timeout fences (rotation.rs:124-133) although nothing irreversible happened, and no test asserts that choice.

#### F. Verdicts on existing issues

- **#904 (LEDGER edit per fsync): PARTIALLY CONFIRMED.**
  - Confirmed: `SyncOpen` after every advancing fsync (durability.rs:264-284; wal_history.rs:609-622), a second fsync per strict group (ledger_store.rs:315), periodic synchronous compaction on the commit path (ledger_store.rs:237-241).
  - Refuted: "re-reads the entire ledger file": only when `image` is `Some`, which is never in production (ledger_store.rs:156, 189, 295-306; storage.rs:1308-1311).
  - Unsafe as stated: R1 (segment-derived prefix) regresses R4 for torn unsynced tails (C.3). Not measurable as stated: AC1 (LEDGER fsyncs are uncounted). AC3 needs a new test (failpoint exists, unused). Recommendation: MODIFY (C.4).
- **#933 (bound the detached reaper): CONFIRMED** with citation corrections (D.2); no bounded-wait test exists.
- **#926**: not mine.

#### G. #808 contracts that bear on these transitions (docs/development/wal-redesign-808.md)

- Explicit WAL history / sequence allocation (lines 39-41): the LEDGER (edit vocabulary A.2) and `Reserve`; bears on LEDGER edit, rotate, `start_epoch`.
- Replay only authorized history above family frontiers (:42): replay and `ReplaySelection` (replay.rs:25-34); bears on replay.
- Prefix-only retirement after durable authority (:43): `AdvanceLogStart` persisted before unlink (wal_history.rs:716-733); retirement is adjacent to my scope but shares the LEDGER write path.
- Fence ownership and every effect (:44, #820 :62-75): every ledger edit and segment effect runs under `WalHistoryFence::with_writer_effect` (wal_history.rs:52-85, 381-388); bears on rotate, LEDGER edit, recover-after-partial truncation (runtime/state/recovery.rs:685-688). Worker-lifetime clause bears on shutdown (join workers before lease release, shutdown.rs:208-258; lease_state.rs:306-351).
- Truthful outcomes and fail-closed salvage (:45, #821 :77-84, #822 :91-99): salvage "stops at the first damaged or missing authoritative point", persists the sequence floor before truncating/quarantining, fails closed on timeouts and read errors; maps to the taxonomy in B. `OutcomeUnknown` for post-WAL-effect failures maps to `appended_unit_error` (wal.rs:917-930) on the sync path. Note the local-salvage behaviour on a file shorter than the durable floor (B.2) should be checked against "salvage may use only the longest verified prefix".
- "A torn unsynced tail opens strictly" (acceptance evidence :117-120) is precisely the behaviour that requires the persisted floor evidence; this is the contract #904 R1 would break.
- Doc staleness: it still says "current on-disk format is 8" and does not mention `SyncOpen`/`SalvageOpen`/`durable_bytes` at all.

#### H. Extra files read (beyond the starting list)
src/runtime/wal_history.rs; src/runtime/wal_transition.rs (head); src/runtime/wal_transition_boundary.rs; src/runtime/state/recovery.rs; src/runtime/state.rs (sequence allocation, begin_wal_epoch); src/runtime/event_loop/{wal_transition.rs, durability_sync.rs, shutdown.rs, wal_retention.rs, mod.rs (lines 255-315), cloud_integration/sealing.rs}; src/runtime/handle.rs; src/runtime/runtime.rs (shutdown_inner/Drop); src/runtime/cloud_startup/{mod.rs, streaming_wal_plan.rs}; src/engine/lease_state.rs; src/engine/startup/storage.rs (lines 120-160, 270-335, 1296-1322); src/wal/frame.rs (300-560); src/wal/fs/{writer_io.rs, writer_runner.rs}; src/wal/sequence.rs; src/wal/recovery/fixture.rs; test files: src/runtime/actors/wal/tests.rs, src/runtime/actors/wal/tests/writer_groups.rs, src/runtime/wal_history/tests.rs, src/runtime/wal_history/tests/fenced_ledger.rs, src/runtime/cloud_startup/streaming_wal_plan/tests.rs, src/runtime/state/recovery.rs (test module), src/wal/ledger/tests.rs, src/wal/ledger_store/tests.rs, src/wal/recovery/tests.rs, src/engine/tests.rs, tests/durability.rs, tests/durability_sync_count.rs, tests/lifecycle.rs, tests/storage_root_injection.rs, tests/fault_injection.rs. Issues read: #873, #904, #933 (`gh issue view`).

## Appendix C: Cloud publication (upload, catalog, retire)

### #873 Phase 0 current-state map: cloud publication (upload segment, publish catalog, retire)

Branch `develop` @ 12962819. Read-only research. All paths are repo-relative to `/Users/jrepanich/Code/cntryl/midge`.
Every `file:line` below was read with the Read tool (offset/limit) in this session. Where a statement is my inference rather than a read line, it is marked **(inference)**.

Terminology used below:
- "event loop" = the runtime event-loop thread (`src/runtime/event_loop/`).
- "uploader" = the single `midge-wal-uploader` thread (`src/storage/hybrid/backend/uploads.rs:371-375`).
- "retirement worker" = per-batch `midge-wal-retirement-*` thread (`src/runtime/event_loop/wal_retention/cloud.rs:170-191`).
- "pruner" = `midge-object-pruner-*` conditional-delete thread (`src/storage/hybrid/backend/proofs.rs:744-799`).

Precursor (not one of the three, but they hang off it): **seal** the open segment. Owner `EventLoop::try_seal_current_cloud_segment` (`src/runtime/event_loop/cloud_integration/sealing.rs:40-114`), runs on the event loop with `runtime_response_timeout` as its deadline (`sealing.rs:16-27`, `src/runtime/event_loop/mod.rs:591-598`). Seal triggers: 16 MiB / 500 ms / 10 000 pending writes (`src/runtime/config.rs:16-30`, duplicated at `src/storage/cloud/config.rs:6-22`); `cloud_strict` and `wait_cloud_durable` force it (`src/runtime/event_loop/wal.rs:370-404`).

---

#### 1. Transition table

##### 1a. Upload segment

| Field | Finding |
|---|---|
| **Owner** | **Two owners, straddling runtime and storage.** (i) Runtime decides *what/when*: `EventLoop::try_drain_cloud_wal_upload_backlog_within` (`sealing.rs:300-363`) calls `CloudPersistence::enqueue_wal_segment` (`src/runtime/hybrid_persistence.rs:203-227`, which re-reads the local file and validates it, then names the object `segment_object_key(id, epoch)`). (ii) Storage does the PUT, retry and backoff: `HybridStorage::{enqueue_object_upload, process_uploads, schedule_pending_uploads, process_wal_upload}` (`src/storage/hybrid/backend/uploads.rs:103-150, 167-229, 231-311, 429-532`). Retry ownership is split: storage retries failed *attempts* itself (`uploads.rs:194-211`, `queues.rs:51-64`); runtime's `UploadBacklog` re-uploads only after a *verification or publication* failure (`ack.rs:510-557`, `coordination.rs:35-47`). The code comments call this out (`ack.rs:140-143`, `publication.rs:146-152`). |
| **Effects** | One `PUT <wal object key>` with `If-Absent` precondition, whole segment read into memory with `read_all` (`uploads.rs:442, 466-478`). If the provider returns a write receipt, bytes are compared to the receipt and no readback is made; otherwise a `stable_object_proof_from_backend` GET/HEAD readback is made and byte-compared (`uploads.rs:487-512`). A verified upload is cached in `verified_uploads` (bounded 1 024) so the runtime can skip its own readback (`uploads.rs:514-530`, `22-31`; `hybrid_persistence.rs:340-342`). Emits `StorageEvent::CloudAck` or `CloudFail` to the event queue and the external channel (`uploads.rs:317-338`). No catalog write, no local file removed. Thread: the uploader (single). Scheduling (`process_uploads`) runs on whoever ticks storage, i.e. the event loop (`ack.rs:26-40`). |
| **Failure outcome** | Storage layer: `CloudFail{failure_kind}` with `Other`/`Timeout` (retry), `Corruption` (remote object holds other bytes), `Fenced` (writer lost authority) (`src/storage/mod.rs:258-276`, `uploads.rs:665-688`). `Other`/`Timeout` stay queued with delay 50 ms doubling to 1 s (`queues.rs:22-49`); `Corruption`/`Fenced` leave the queue (`uploads.rs:200-201`, `queues.rs:200-203`). Runtime layer (`ack.rs:144-187`): first 2 failed attempts are silent (`ack.rs:12, 152-159`); from the 3rd failure on, waiters get `MidgeError::Timeout("Cloud durability unconfirmed after N failed upload attempts ... treat the outcome as unknown ...")` (`ack.rs:160-167`) while storage keeps retrying; `Corruption` -> waiters get `MidgeError::Corruption`, writer fenced; `Fenced` -> waiters get `MidgeError::Fenced`, writer fenced (`ack.rs:169-186`). **cloud_async commit**: `finalize_write_durability` returns `Ok(())` without waiting (`src/engine/api/transaction.rs:861-881`), so the commit never sees upload failure; it sees only admission failures on *later* writes: `WriteStall` from `check_cloud_wal_admission` / `accept_write` (`event_loop/wal.rs:445-463`, `sealing.rs:368-385`), or `Fenced` once lease health is lost (`event_loop/wal.rs:446-449`). **cloud_strict commit**: waits via `SealWalForCloud{wait_for_ack:true}` (`transaction.rs:862-877`); *any* error after apply, including `Timeout`, `Corruption`, `Fenced`, `WriteStall`, is rewrapped to `MidgeError::OutcomeUnknown` (`transaction.rs:727-735, 1019-1027`). `Engine::wait_cloud_durable` instead returns `Ok(false)` for `Backpressure`/`Transient` severities and raw `Err` (e.g. `Fenced`, `Corruption`) otherwise (`src/engine/mod.rs:546-564`). **Fenced writer**: see section (e). |
| **Pinning tests** | Retry/backoff: `should_double_wal_upload_retry_delay_until_its_cap_when_failures_repeat`, `should_schedule_next_attempt_by_retry_policy_when_an_upload_fails` (`src/storage/hybrid/backend/tests.rs:988, 1033`). Fenced upload not attempted: `should_end_wal_upload_without_attempting_it_when_writer_is_fenced` (`src/runtime/hybrid_persistence/tests.rs:2315`). Waiters after exhausted attempts: `should_fail_all_generation_waiters_given_exhausted_cloud_upload_attempts` (`src/runtime/event_loop/cloud_integration/tests.rs:448`), `should_leave_retry_with_storage_queue_when_upload_attempts_are_exhausted` (`:8743`). Corrupt upload fences: `should_fence_writer_when_storage_reports_a_corrupt_upload` (`:8789`). Strict commit maps to OutcomeUnknown end to end: `should_finish_cloud_strict_upload_after_reporting_unknown_outcome_when_provider_recovers` (`tests/fault_injection.rs:8334`). **None** for "uploads run concurrently" (there is no concurrency). **None** for "worker PUT after fence is harmless" (see finding F4). |
| **Citations** | `uploads.rs:103-150,167-229,231-311,361-397,429-532,612-690`; `queues.rs:22-64,139-222`; `backend.rs:74-82`; `hybrid_persistence.rs:203-227`; `sealing.rs:300-363`; `ack.rs:12,144-187`; `transaction.rs:727-735,861-881,1019-1027`. |

##### 1b. Publish catalog

| Field | Finding |
|---|---|
| **Owner** | **Three-way straddle (wal / runtime / storage), plus a fourth piece on the event loop.** Pure rules: `wal/publish.rs` (`AckSet`, `uploaded_run`, `additions`; `publish.rs:44-143, 283-354`) and `wal/cloud_catalog.rs` (`WalPublicationCatalog::append_additions`/`extend_validated`, `cloud_catalog.rs:483-514`). Stateful orchestration (load, repair, decode, admit memory, CAS primary then mirror): `runtime/hybrid_persistence/catalog.rs` (`mutate_wal_catalog_within` `catalog.rs:395-460`, `commit_catalog_with_authority` `:489-537`) driven by `CloudPersistence::publish_wal_segments` (`hybrid_persistence.rs:370-397`). The in-process lock and the receipt-gated catalog cache live **inside `HybridStorage`** (`backend.rs:170-175`, `356-389`). The decision of *which* run to publish and when is on the event loop: `handle_storage_event_cloud_ack` (`publication.rs:13-94`) + `DurabilityCoordinator::contiguous_acked_cloud_segments` (`src/runtime/durability.rs:313-327`) + the `AckSet` held in `CloudWalUploadTracker` (`coordination.rs:7-18`). |
| **Effects** | On the event loop thread, inline (`publication.rs:11-93`; issue #907 confirmed): (1) lease validation (`publication.rs:26`); (2) `verify_uploaded_wal_segment`: local `read_all` + validate; if `take_verified_upload` hits no network I/O, else a remote GET proof + byte compare (`hybrid_persistence.rs:308-354`); (3) `AckSet::record_upload` (`publication.rs:60-63`); (4) `contiguous_acked_cloud_segments` -> run of consecutive acked segments (`publication.rs:65-69`); (5) one catalog CAS for the whole run (`publication.rs:96-127`): take lock (`catalog.rs:404`), take cached primary and **decode it again from bytes** (`catalog.rs:405-416`) or GET primary (+ GET/PUT mirror sync) (`catalog.rs:418, 231-351`), `append_run`, encode the **whole catalog as one JSON object** (`catalog.rs:198-218, 500`), `PUT primary` with `If-Match` (`catalog.rs:502-511`), then `PUT mirror` with `If-Match` or sync-copy (`catalog.rs:516-535`). Each control PUT does a readback GET unless the provider returned a receipt (`src/storage/hybrid/backend/control_io.rs:188-229`); the cache is kept only if **both** PUTs had receipts (`backend.rs:376-380`). (6) re-validate lease (`publication.rs:129`), `mark_published` (`:139-143`). (7) `commit_contiguous_cloud_ack` advances `cloud_durable` (`WalFrontiers::advance_cloud_over`, `src/runtime/frontiers.rs:69-79`; call site `ack.rs:234`), completes waiters (`ack.rs:249-263`), drops the inflight entry (`ack.rs:270-282`), queues local WAL retirement (`ack.rs:284-301`). Objects written: catalog primary + mirror (2 PUTs, full body each). |
| **Failure outcome** | `PublicationError` (`hybrid_persistence.rs:34-46`): `Mismatch` (object has other bytes) -> `MidgeError::Corruption` (`:78-87`); `Refused(Refusal)` -> `Corruption` for `Conflict/BehindHead/Unchained`, `Internal` for `Unordered/SequenceMismatch`, `Fenced` for `EpochMismatch` (`publish.rs:257-271`); `Fenced`; `Retriable` (I/O, timeout, lost CAS race = `Busy`, memory pressure). Routing in `route_cloud_publication_error` (`publication.rs:153-188`): Mismatch/Refused/Fenced -> `fence_cloud_upload` (withdraw upload, `fence_wal_transition` fails every waiter with the original error replayed) (`publication.rs:194-202`, `wal_transition.rs:27-49`, `durability_sync.rs:117-146`); `Retriable` -> withdraw the first upload of the run, then either defer for shared-memory pressure without failing waiters (`ack.rs:192-216`) or fail blocked waiters and requeue to the runtime backlog (`ack.rs:484-508`, `publication.rs:178-185`). Lost-CAS: re-reads catalog; `Fenced` if a later epoch owns it, else `Busy` (`catalog.rs:451-457`). **cloud_async**: commit already returned `Ok`; the writer only sees `WriteStall` on later writes (held unpublished uploads count against queue limits, `held_uploads` `coordination.rs:303-309`, `queues.rs:177-181`). **cloud_strict**: waiter gets the replayed error, commit returns `OutcomeUnknown` (`transaction.rs:727-735, 1019-1027`). A lease failure *at ack time* does **not** fence; it requeues (`publication.rs:26-34, 129-138`; see F6). |
| **Pinning tests** | In-order rule: `should_not_publish_later_upload_when_preceding_segment_is_unacknowledged` (`cloud_integration/tests.rs:8608`), `should_publish_buffered_upload_in_order_when_preceding_segment_is_acknowledged` (`:8664`), `should_not_advance_cloud_durability_across_unacked_segment_gap` (`:4908`). Stale epoch: `should_reject_wal_catalog_publication_from_stale_epoch_after_takeover` (`src/runtime/hybrid_persistence/tests.rs:161`), `should_report_fenced_when_catalog_authority_changes_before_publication` (`:367`). Both copies converge: `should_converge_catalog_copies_after_publication` (`:202`). Run withdraw on failure: `should_withdraw_first_upload_when_publishing_its_run_fails` (`cloud_integration/tests.rs:8824`). Held-upload backpressure: `should_refuse_writes_when_unpublished_uploads_fill_the_upload_queue` (`:9039`). Wait semantics end to end: `should_report_not_covered_given_late_wal_uploads_when_wait_times_out` (`tests/cloud_core/cloud_durable_wait.rs:100`). Fenced predecessor commit is OutcomeUnknown: `should_preserve_acknowledged_history_when_predecessor_wal_resumes_after_takeover_and_cleanup` (`tests/cloud_provider_engine_qualification/authority_publication.rs:56`, asserts at `:436`; requires Sqrzl). **None** for "catalog publishes at most one write per N ms" or for "catalog object size under N segments" (no such behaviour exists). **None** for concurrent uploads (no concurrency). |
| **Citations** | `publication.rs:13-202`; `durability.rs:313-327`; `publish.rs:44-143,283-354`; `cloud_catalog.rs:273-289,483-514`; `catalog.rs:126-147,198-218,395-460,489-537`; `hybrid_persistence.rs:34-87,308-354,370-397`; `control_io.rs:188-229`; `backend.rs:170-175,356-389`; `ack.rs:192-216,220-302,484-508`; `frontiers.rs:63-79`. |

##### 1c. Retire (remote catalog authority, remote object deletion, local WAL)

| Field | Finding |
|---|---|
| **Owner** | **Six owners across four layers; this is the most fragmented transition.** (1) Scheduling + candidate pre-filter + gate: `EventLoop::prune_cloud_wal_segments_covered_by_manifest`, `next_cloud_wal_prune_candidates` (`src/runtime/event_loop/wal_retention/cloud.rs:63-151, 212-238`). (2) Authority + CAS orchestration: `CloudPersistence::prune_cloud_wal_segments_within` (`hybrid_persistence.rs:402-512`) on the retirement worker (`cloud.rs:15-51`). (3) Pure rule: `WalPublicationCatalog::{retirable_prefix, retire_prefix, oldest_prefix, require_epoch}` (`cloud_catalog.rs:397-414, 537-599, 631-640`). (4) Shared-generation proof: `SharedMetadataSnapshot::committed_retirement_authority_within` + `SharedRetirementAuthority::verify_current` (`hybrid_persistence/metadata_snapshot.rs:34-48, 77-127`). (5) Physical delete: `HybridStorage::{admit_remote_deletes_by_identity, spawn_conditional_deletes, delete_conditional_target}` (`proofs.rs:598-646, 720-811, 813-885`). (6) Completion + local retirement: `handle_storage_event_cloud_wal_prune_complete` (`ack.rs:340-390`), `retire_cloud_local_wal` (`cloud.rs:300-337`), `WalHistory::retire_below` (`src/runtime/wal_history.rs:712-733`), GC unlink `GcActor::retry_retired_wal` (`src/runtime/actors/gc.rs:321-354`). Local retirement of a *published* segment is a separate trigger from remote retirement: it happens at ack time (`ack.rs:301, 306-326`). |
| **Effects** | Remote: (a) `HEAD` each target to pin its identity (`hybrid_persistence.rs:457-461`); (b) reserve bounded delete capacity (`:469-479`, `proofs.rs:598-646`); (c) catalog CAS retiring the prefix: sets `anchor` to the last retired link, `retired_by = generation`, drains rows (`cloud_catalog.rs:562-567`), 2 full-object PUTs as in 1b (`hybrid_persistence.rs:480-495`); (d) only then `HEAD` again + authority check + `DELETE If-Match` per object on a pruner thread (`proofs.rs:826-866`); failed deletes are re-queued in an **in-memory** `pending` list (`proofs.rs:55-62, 781-788`), emit `CloudWalPruneComplete` per object (`proofs.rs:793-797`). Local: ledger `AdvanceLogStart` committed first under the writer fence, then GC unlinks files below `log_start` and releases disk-budget bytes (`wal_history.rs:716-733`, `wal_retention.rs:50-72`, `gc.rs:333-342`). Batch size 32 (`cloud.rs:7, 236`); at most one retirement worker and it owns `ManifestPublicationOwner::WalPrune` (`cloud.rs:98-107, 134-142`). |
| **Failure outcome** | Not caller-visible at all: runs callerless. A failed attempt becomes `CloudWalPruneAttemptFailed` -> `release_prune`, segment kept for retry (`cloud.rs:33-50`, `ack.rs:113-123`); uncovered prefix returns `MidgeError::Busy("oldest WAL segment is not flushed in the committed generation")` per id (`hybrid_persistence.rs:448-455`). Catalog rule refusals map through `refusal_error`: `EpochMismatch` -> `Fenced`, anything else -> `Corruption` (`catalog.rs:482-487`). After catalog CAS success a delete failure is *ignored*: authority is already retired, object becomes an orphan, `mark_persistence_anomaly` only (`ack.rs:371-386`). Under `cloud_async`/`cloud_strict` the commit path never observes retirement except indirectly: if retirement stalls, the catalog and local WAL grow and `WriteStall`/`NoSpace` follow (section d). Fenced writer: retirement worker's lease check or CAS fails with `Fenced`; the delete worker maps `Fenced` to `Unauthorized` and does not retry (`proofs.rs:838-850, 773-780`). |
| **Pinning tests** | Needs cloud_durable >= segment max: `should_not_prune_remote_wal_when_segment_is_not_cloud_durable` (`cloud_integration/tests.rs:4088`). Needs manifest coverage: `should_retain_remote_wal_when_segment_family_maximum_exceeds_shared_frontier` (`:4124`), `should_retire_remote_wal_when_segment_family_maximum_equals_shared_frontier` (`:4153`). Needs committed shared manifest base: `should_not_retire_wal_authority_without_cloud_manifest_base` (`:3391`). Prefix-only: `should_retire_flushed_oldest_segment_when_a_later_published_segment_is_unflushed` (`:276`), `should_not_spawn_retirement_worker_when_oldest_published_segment_is_unflushed` (`:251`). Shared generation must not move: `should_retain_catalog_authority_when_the_captured_shared_generation_changes` (`hybrid_persistence/tests.rs:2798`). Catalog rule: `should_retire_only_exact_catalog_entry`, `should_refuse_retirement_of_a_segment_behind_an_older_one`, `should_keep_the_chain_when_every_entry_retires` (`src/wal/cloud_catalog.rs:1019, 1034, 1053`; rg line numbers only, bodies not Read). Delete after retire uses original identity: `should_retry_a_retired_object_with_its_original_identity_after_transport_failure` (`src/storage/hybrid/backend/tests.rs:1191`; rg line only, body not Read). Local disk budget: `should_release_wal_disk_budget_only_after_local_segment_removal` (`cloud_integration/tests.rs:302`). **None found** for "pending remote deletes survive a crash" (they do not; see F5). **None found** for "retirement under lost lease deletes nothing" at the engine level (only unit-level, `proofs.rs:838-850` has no test I located). |
| **Citations** | `cloud.rs:7-51,63-151,153-210,212-238,300-337`; `hybrid_persistence.rs:402-512`; `cloud_catalog.rs:397-414,537-599,631-640`; `metadata_snapshot.rs:34-48,77-127`; `proofs.rs:55-62,598-646,720-885`; `ack.rs:306-326,340-390`; `wal_history.rs:712-733`; `gc.rs:321-354`. |

---

#### 2. (a) The cloud WAL retirement proof

Two different deletions with two different proofs. Both are written down in `docs/development/wal-redesign-808.md:25-31` ("Local retirement persists the ledger's new `log_start` before queuing file removal. Cloud retirement first commits the frontiers in a shared generation, records that generation in the catalog's `retired_by`, and then queues identity-conditional deletes").

##### Remote WAL object (the catalog row, then the object)

All must hold before the *catalog row* is retired; the object delete follows and is not itself a proof step.

| # | Condition | Decided at |
|---|---|---|
| 1 | Segment is **published in the catalog** (an `AckSet::Published` row; unpublished uploads are never candidates) | `AckSet::published` consumed in `cloud.rs:229-232`; authoritative check is the catalog itself: `oldest_prefix` must find the id as the catalog's oldest, in order (`cloud_catalog.rs:579-599`; call `hybrid_persistence.rs:431-433`) |
| 2 | **Frontier**: `segment.max_sequence <= wal.frontiers.cloud_durable()` | `cloud.rs:233` (runtime pre-filter only; the catalog does not check it because published implies cloud-durable by the in-order rule) |
| 3 | Runtime pre-check that no local family blocks the oldest published segment (`oldest_wal_blocking_family`) | `cloud.rs:219-228`. Documented as "never grants retirement; spares a worker" (`cloud.rs:213-218`) |
| 4 | **Manifest coverage in the committed shared generation**: every `(cf, max_seq)` of the entry's persisted facts is `<= through_sequence` of that family's frontier (or the family is `Dropped`), taken from the *shared* manifest not local state | `retirable_prefix` (`cloud_catalog.rs:397-414`) fed by `authority.frontiers()` in `hybrid_persistence.rs:435-444`; authority built in `metadata_snapshot.rs:77-127` |
| 5 | **Lease validity**: epoch of the authority equals the writer's (`hybrid_persistence.rs:413-417`); `validate_epoch` on the leader store; the committed metadata pointer still equals the generation the manifest was read from (`metadata_snapshot.rs:34-48`, re-run before the retiring CAS in the validate closure `hybrid_persistence.rs:484-487`) |
| 6 | **Catalog epoch**: `retire_prefix` requires `writer_epoch == fencing_epoch` (`cloud_catalog.rs:543, 631-640`) |
| 7 | **Generation monotonicity**: generation is not NONE and not below `retired_by` (`cloud_catalog.rs:544-549`) |
| 8 | **Expected rows unchanged** from what planning saw (`cloud_catalog.rs:550-561`) |
| 9 | **Catalog CAS** accepted by the provider (`If-Match` on primary), under the in-process lock (`catalog.rs:395-460`) |
| 10 | Physical delete: object `HEAD` identity equals the identity pinned *before* the CAS (`proofs.rs:826-837`); write authority closure passes (`proofs.rs:838-851`); `DELETE If-Match identity` (`proofs.rs:860-866`) |

Recovery re-verifies #4 against the catalog's `retired_by` (doc `wal-redesign-808.md:29-30`; I did not re-read the recovery code, so treat as doc-level).

Where it is "decided" (every place): `cloud.rs:219-233` (pre-filter), `hybrid_persistence.rs:412-461` (authority, prefix, coverage count, pinned identities), `cloud_catalog.rs:537-599` (rule), `catalog.rs:395-460` (CAS), `metadata_snapshot.rs:34-48,77-127` (shared generation), `proofs.rs:813-885` (identity + lease before delete), and `ack.rs:340-390` (what the runtime does with the result). **Seven sites in four modules for one decision.**

##### Local WAL segment

| # | Condition | Decided at |
|---|---|---|
| 1 | Cloud-durable: ack committed, i.e. catalog published and frontier advanced (`ack.rs:220-302`, `retire_acked_local_wal_segments` `ack.rs:306-319`) **or** catalog prune completed (`ack.rs:354-361`) | Queue only: `queue_local_retirement` (`cloud.rs:305-308`) |
| 2 | Only a leading run of ledger-sealed members retires (`LocalRetirementQueue::retire_point` `coordination.rs:200-207`; `cloud.rs:309-317`) | prefix rule |
| 3 | Not under a storage-verification barrier (`ack.rs:288-300`) | deferral |
| 4 | Ledger `AdvanceLogStart` committed under the writer fence (`wal_history.rs:712-733`) | durable authority |
| 5 | GC unlinks only ids `< log_start` and releases disk-budget bytes after removal (`gc.rs:333-342`) | physical |

Note the asymmetry: **local retirement does not require manifest coverage** (it needs only cloud publication), because the catalog entry becomes the recovery authority for that data. Manifest coverage gates only the *remote* retirement.

---

#### 3. (b) Catalog rewrite cost (#916)

- Format: one JSON object `wal/publication-catalog.v1.json` plus a mirror (`src/cloud_layout.rs:18-20` via `cloud_catalog.rs:25-27`); format version 3 (`cloud_catalog.rs:24`). The object holds `format_version, fencing_epoch, anchor, next_segment_id, seq_floor, retired_by, segments: Vec<CatalogEntry>` (`cloud_catalog.rs:273-289`). Each `CatalogEntry` = `epoch, prev: ChainLink, facts: SealedFacts, size_bytes, crc32c` (`cloud_catalog.rs:31-39`); `ChainLink` = `id, last_seq, digest:[u8;16]` (`link.rs:13-24`); `SealedFacts` = `link, record_count, data_len, max_seq_per_cf: Vec<(u32,u64)>` (`segment.rs:216-225`). The `digest` serializes as a 16-element JSON number array **(inference from serde derive)**, so an entry holds two `ChainLink`s.
- **Size estimate (arithmetic, not measured)**: roughly 300-400 bytes per entry for one column family plus ~14 bytes per extra family, so ~300-400 KB per 1 000 unretired entries. #916's "about 100 KB for 1,000 segments" looks about 3-4x low.
- **Is it a full-object PUT per publish? Yes.** `AdmittedEncoding::new` serializes the whole catalog (`catalog.rs:198-218`), `commit_catalog_with_authority` PUTs it to primary then the same bytes to mirror (`catalog.rs:500-535`). Retirement does the same (`hybrid_persistence.rs:480-495`) and so does fencing at startup (`hybrid_persistence.rs:239-258`). With a cache hit, the loop also **re-decodes the full catalog from the cached bytes on every mutation** (`catalog.rs:405-416`, three-pass `decode` `catalog.rs:53-70`), CPU on the event loop proportional to catalog size.
- Without provider write receipts (cache not retained, `backend.rs:376-380`): per publish = GET primary + GET mirror (+ possible mirror repair PUT) + PUT primary + readback GET + PUT mirror + readback GET (`catalog.rs:418, 231-351`; `control_io.rs:188-229`).
- **How often**: once per `CloudAck` that completes a contiguous run (`publication.rs:90-127`). With a single uploader, runs are almost always length 1. `cloud_strict` seals one segment per commit when no inflight segment already covers the sequence (`wal.rs:370-404`), so one catalog write pair per strict commit. Concurrent strict commits coalesce into the same sealed segment, giving group commit for free **(inference from `wal.rs:370-404`)**. `cloud_async` publishes one pair per segment (16 MiB / 500 ms / 10k writes).
- **Is the catalog already bounded?** Retired entries are already dropped from the object: `retire_prefix` drains rows and keeps only an `anchor` link (`cloud_catalog.rs:562-567`), and `next_segment_id` keeps ids monotonic (`cloud_catalog.rs:7-14`). So #916 R4's "compact retired entries" is **already done**. The remaining growth is *unretired* entries. Policy bound: `eventual_flush_segment_gap = 128` forces a family flush so retirement can proceed (`storage/cloud/config.rs:7-10`, `runtime/state/flush.rs:462-468`). Hard bound on held-but-unpublished uploads: 1 024 entries / 1 GiB (`backend.rs:74-75`). The catalog is unbounded only if retirement stalls (no committed shared generation, or an unflushed family; see `cloud.rs:219-228`) **(inference)**.

---

#### 4. (c) Upload concurrency and the in-order rule

- **Single thread today.** One `std::thread` named `midge-wal-uploader` drains an `mpsc::sync_channel` of capacity 32 (`uploads.rs:361-385`, `backend.rs:76`, spawn `backend.rs` struct comment says "Single background worker for WAL uploads"). It processes one segment at a time to completion, including the PUT and a possible readback, with `callback_timeout` = 30 s per call (`backend.rs:82`, `uploads.rs:429-532`). `schedule_pending_uploads` marks entries `InFlight` and `try_send`s; `Full` puts the entry back to `Pending` and stops the pass (`uploads.rs:277-288`).
- **In-order publication rule** (the thing a pool must preserve), as implemented:
  - Runtime tracks `inflight: segment_id -> max_sequence` (`durability.rs:301-309`).
  - `contiguous_acked_cloud_segments` sorts inflight ascending and calls `uploaded_run`, which stops at the first owed segment not in the `AckSet` and errors on a `max_sequence` mismatch (`durability.rs:313-327`, `publish.rs:283-303`).
  - The catalog side independently enforces it: `additions` requires every added entry to chain onto the head (`prev == head`, `max_sequence > head.last_seq`, epoch non-decreasing), is idempotent for identical prefix, and returns `BehindHead/Unchained/Conflict` otherwise (`publish.rs:319-354`).
  - A failed or withdrawn upload is removed from the `AckSet` so no later run publishes past it (`publish.rs:92-98`, `ack.rs:484-508`).
  - `cloud_durable` advances only through the published run's last segment, and only the first `CloudAck` of an out-of-order set triggers publication of the buffered ones (`publication.rs:81-88`, tests `:8608`, `:8664`).
- **A parallel pool (#916) must preserve:** (1) the uploader keeps reporting per-segment `CloudAck`; publication stays single-writer on the event loop (or one publisher task); (2) `AckSet::withdraw` semantics on any failure; (3) the `If-Absent` PUT with epoch-named keys (`hybrid_persistence.rs:215-224`) so a re-upload after retry is idempotent and a stale epoch cannot clobber; (4) the `verified_uploads` cache bound (`uploads.rs:518-529`); (5) the `held_uploads` accounting so unpublished uploads still count against queue capacity (`coordination.rs:303-309`, `queues.rs:177-181`); (6) fence check before each attempt (`uploads.rs:241-251`) and **ideally also before each PUT inside the worker** (today it is only at schedule time; see F4); (7) terminal-event-per-segment semantics (`queues.rs:263-275`, test `should_emit_one_upload_terminal_event_when_wal_ack_logging_panics`).
- Bounded work already present that a pool can reuse: `UploadQueue` limits 1 024 / 1 GiB (`backend.rs:74-75`, `queues.rs:139-181`); worker channel 32; 30 s callback timeout.

---

#### 5. (d) Backpressure on uploads

| Layer | Bound | Value | What the caller sees |
|---|---|---|---|
| Seal triggers (pacing, not rejection) | min segment bytes / max delay / max pending writes | 16 MiB / 500 ms / 10 000 (`runtime/config.rs:16-30`; `durability.rs:388-407`; user-configurable via `CloudWritePolicy` `storage/cloud/config.rs:5-22`) | Nothing |
| Runtime backlog non-empty (older sealed segments not yet admitted to the storage queue) | boolean | n/a | `MidgeError::WriteStall("older CloudAsync WAL segments are still awaiting upload admission")` from seal (`sealing.rs:134-138`); `WriteStall("CloudAsync WAL recovery uploads are still draining")` from write admission (`sealing.rs:379-383`) |
| Storage upload queue | entries, bytes (queued + held-unpublished) | 1 024 entries, 1 GiB (`backend.rs:74-75`); `is_stalled` uses `>=` for entries+held and bytes+held (`queues.rs:177-181`) | `MidgeError::WriteStall("CloudAsync WAL upload queue remains at capacity: entries=.../...")` (`uploads.rs:67-84`) via `accept_write` for **every** subsequent write (`event_loop/wal.rs:445-463`), counted as `WriteAdmissionRejection::CloudWal`. Seal path: `ensure_wal_upload_capacity` -> same `WriteStall` + `record_write_stall_cloud` (`uploads.rs:53-65`, `sealing.rs:159`) |
| Worker channel | entries | 32 (`backend.rs:76`) | Nothing (entry stays `Pending`, `uploads.rs:285-288`) |
| Event queue | entries/bytes | 2 048 / 8 MiB (`backend.rs:77-79`) | Log only (`uploads.rs:335-337`); terminal events are kept in a separate map (`queues.rs:245`) |
| **Time-based CloudAsync lag** | **none exists** | I grepped for `lag` in non-test `src/` and found no lag bound | An unreachable cloud lets `cloud_async` commits keep returning `Ok` until the queue above (or local disk) fills |
| Local disk budget (only when `ephemeral_sst_cache` is enabled) | `max_local_bytes` default 2 GiB; watermarks 90/95/98 % (`storage/hybrid/policy.rs:5-47`) | admission uses free bytes only: `bytes > free_bytes` -> `RejectNoSpace` (`actor.rs:177-188`) | `MidgeError::NoSpace("ephemeral local disk budget cannot admit WAL append")` before apply (`reservations.rs:71-82`, called from `runtime/actors/wal.rs:712-733`); a normal pre-apply error, not wrapped to OutcomeUnknown. Watermark crossings emit `StorageEvent::BackpressureOn`, which sets `write_stalled` and pauses flushes (`reservations.rs:144`, `ack.rs:124-127`). Bytes are released only after the local file is unlinked (`gc.rs:337-342`) |
| Shared maintenance memory | catalog decode/encode reservations | `DEFAULT_COMPACTION_MEMORY_LIMIT` pool (`catalog.rs:190-196`) | Not caller-visible: a contended catalog publication defers and keeps waiters (`ack.rs:192-216`, test `should_preserve_cloud_waiter_when_catalog_admission_is_temporarily_blocked`, `cloud_integration/tests.rs:8043`, rg line only) |

Under `cloud_strict` a `WriteStall` raised by the seal itself is returned through `SealWalForCloud` (`wal.rs:399-402`) and then rewrapped by the commit to `OutcomeUnknown` (`transaction.rs:727-735`), even though in that case the WAL append was applied and the writer only failed to start the upload. Under `wait_cloud_durable` the same stall becomes `Ok(false)` (`engine/mod.rs:552-559`).

Pinning: `should_refuse_writes_when_unpublished_uploads_fill_the_upload_queue` (`cloud_integration/tests.rs:9039`), `should_back_off_runtime_wal_admission_when_storage_queue_is_full` (`:6749`), `should_reject_wal_upload_when_entry_or_byte_capacity_is_exhausted` (`src/runtime/hybrid_persistence/tests.rs`, seen just below `:2315`; exact line not Read). WAL disk admission: `should_release_wal_disk_budget_only_after_local_segment_removal` (`cloud_integration/tests.rs:302`).

---

#### 6. (e) Fencing: how a lost lease/epoch surfaces

Fencing inputs: `RuntimeFence::check_health` (heartbeat flag, DDL ambiguity, monotonic validity; `fencing.rs:14-37`), `validate_within` (also asks the leader store; `fencing.rs:42-81`), `HybridStorage::check_write_authority` (monotonic validity closure installed at open; `backend.rs:365-369`, `src/engine/startup/storage.rs:619-640`), catalog `fencing_epoch` + provider CAS (`cloud_catalog.rs:631-640`, `catalog.rs:451-457`).

| Transition | Surfaces as | Writer's state after |
|---|---|---|
| **Upload** | (1) `schedule_pending_uploads` runs `check_write_authority` before send; on `Fenced` emits `CloudFail{Fenced}` and the upload leaves the queue without any PUT (`uploads.rs:239-251`, `queues.rs:200-203`). (2) Runtime maps to `MidgeError::Fenced("cloud WAL segment N upload stopped: ...")` and, if it still owns the segment, `fence_cloud_upload` -> `fence_wal_transition`: WAL actor + protocol fenced together, every queued waiter failed once with that error (`ack.rs:172-186`, `publication.rs:194-202`, `wal_transition.rs:27-49`). **Gap:** the worker does no authority check between the file read and the PUT (`uploads.rs:442-478`), so an upload already dispatched when a takeover lands still writes its `If-Absent` object; the code treats that as a harmless orphan (`uploads.rs:239-240`) | WAL actor fenced; later writes fail `check_lease_health`/`ensure_ready` with `Fenced` (`event_loop/wal.rs:446-449`, `wal_transition.rs:172-197`) |
| **Publish catalog** | Stale epoch: `append_additions` returns `EpochMismatch` (`cloud_catalog.rs:488-493`) -> `PublicationError::Fenced` (`hybrid_persistence.rs:67-76`) -> `fence_cloud_upload` (`publication.rs:168-170`). Lost CAS while a later epoch owns catalog: `MidgeError::Fenced("cloud WAL catalog advanced to fencing epoch N, writer epoch M rejected ...")` (`catalog.rs:451-457`). Authority check fails before PUT: `ControlWriteFailure::Authority` (`control_io.rs:185-187`). Lease validation fails at ack **before** verify/publish or **after** publish: **no fence**, the upload is requeued and waiters are failed with `MidgeError::Fenced("... writer lease validation failed ...")` context-preserved by `cloud_ack_error` (`publication.rs:26-34, 129-138`, `ack.rs:425-448`) | For catalog-detected fences: actor+protocol fenced as above. For lease-validation-detected fences: `lease_healthy` flag flips false (`fencing.rs:71-75`), blocking later writes via `check_health`, but the WAL protocol is not explicitly fenced (F6) |
| **Retire** | Retirement worker: `verify_current` fails -> `Fenced` ("shared metadata pointer changed before WAL retirement", `metadata_snapshot.rs:42-46`) or lease error (`:35-37`); catalog `EpochMismatch` -> `refusal_error` -> `Fenced` (`catalog.rs:482-486`). Result is queued as `CloudWalPruneAttemptFailed` for every candidate and the segment is retained (`cloud.rs:45-49`, `ack.rs:113-123`); nothing is deleted. Delete worker: `Fenced` -> `Unauthorized`, **not** retried (`proofs.rs:838-850, 773-780`). No caller sees it | Writer is not fenced by a retirement failure; it learns of the takeover on its next write/ack. Retained WAL objects become the successor's orphans or history |

Commit-caller view for a fenced writer: `cloud_async` commit already returned `Ok`; subsequent commits get `Fenced` (pre-apply). `cloud_strict` commit that was in flight gets `OutcomeUnknown` (the original `Fenced` is flattened) (`transaction.rs:727-735, 1019-1027`), pinned by `authority_publication.rs:436` (requires Sqrzl). `wait_cloud_durable` on a runtime fenced for authority returns `Err(Fenced)`, but a runtime fenced for any non-authority reason returns `Err(RecoveryFailed("local WAL is poisoned ..."))` (`wal_transition.rs:186-191`, test `cloud_integration/tests.rs:6086`); `wait_cloud_durable`'s doc only promises `Fenced` (`engine/mod.rs:528-533`).

---

#### 7. (f) Layer ownership and layering violations

Who owns what today:

| Concern | `wal/` | `storage/hybrid` | `runtime/` |
|---|---|---|---|
| Catalog format, rules, chain/epoch, retire/append | `cloud_catalog.rs`, `publish.rs` (pure, tested) | - | - |
| Segment byte validation | `cloud_segment.rs` | - | calls it (`hybrid_persistence.rs:212-217, 324-329`) |
| Catalog load/repair/CAS/encode/admit/cache | - | holds the lock + cache (`backend.rs:170-175, 356-389`) and raw `write_control_object_with_authority` (`control_io.rs:142-232`) | `runtime/hybrid_persistence/catalog.rs` (the logic) |
| Which run to publish, ack state (`AckSet`), frontier | the `AckSet` *type* (`publish.rs:44`) | - | owns the instance (`coordination.rs:7-18`) and `DurabilityCoordinator` (`durability.rs:313-327`); `WalFrontiers` in `runtime/frontiers.rs` |
| Upload queue, backoff, bounded memory | - | `uploads.rs`, `queues.rs` | `UploadBacklog` retry (`coordination.rs:35-47`) |
| Disk budget admission | - | `actor.rs`, `reservations.rs`, `policy.rs` | calls it from `actors/wal.rs:712-733` |
| Retire scheduling and gate | - | - | `wal_retention/cloud.rs` |
| Remote delete workers | - | `proofs.rs:598-885` | - |
| Seal policy defaults | - | `storage/cloud/config.rs:6-22` | duplicated in `runtime/config.rs:16-44` |

**`use` violations of "lower layers must not depend on higher layers":** I searched `src/wal` and `src/storage` for imports of `crate::runtime`, `crate::engine`, `crate::wal`, `crate::metadata`, `crate::sst`, `crate::lease`, `crate::compaction`.
- `src/wal` -> `crate::runtime`: only `src/wal/fs/writer_io.rs:1321` and `:1327`, which are inside `mod tests` (opens at `writer_io.rs:713`). Test-only. No production violation found in `src/wal`.
- `src/storage` -> higher layers: production hit is **`src/storage/hybrid/backend/file_publication.rs:111`** (`crate::sst::fs::engine_sst_error`, storage depends on the SST module for an error mapper; SST publication, adjacent to my scope). Other hits (`control_io.rs:432` `crate::lease::LeaseValidity`, `remote_sst.rs:716/726` `crate::metadata`, `crate::runtime::read_resources`, and `storage/providers/request_budget_tests/catalog_memory.rs:6-9,302,351` importing `runtime::hybrid_persistence` and `wal::cloud_catalog`) are all in test modules.

**Semantic layering leaks without an offending `use`** (this is the real finding):
1. `HybridStorage` claims to be format-neutral ("this module never imports those formats", `backend.rs:10-11`; `hybrid_persistence.rs:1-5`), yet it owns the WAL **catalog** mutation lock and the receipt-gated catalog cache (`backend.rs:170-175`), and `pub(crate)` entry points named for WAL (`queue_cloud_wal_prune_complete`, `ensure_wal_upload_capacity`, `uploads.rs:33-65`).
2. `StorageEvent` (a `storage` type) has WAL-specific variants `CloudAck`, `CloudFail`, `CloudWalPruneComplete`, `CloudWalPruneAttemptFailed` (`src/storage/mod.rs:329-352`), and the pruner reuses `CloudWalPruneComplete{segment_id: request_id}` for generic delete targets (`proofs.rs:793-797`).
3. `CloudPersistence` is a `Deref<Target=HybridStorage>` wrapper in `runtime` (`hybrid_persistence.rs:118-134`): runtime logic reaches storage internals through the deref, so the boundary exists in naming only.
4. Rules live in `wal/` but the state machine that applies them is split between `runtime::event_loop::coordination` (AckSet, prunes, backlog, local retirement queue) and `runtime::hybrid_persistence::catalog` (CAS). `wal/publish.rs` is thus a leaf of pure functions that has two stateful callers in different runtime subtrees.
5. Two retry owners for one upload (section 1a); two copies of the seal policy defaults.

---

#### 8. Hypothesis verdicts

- **#916 (parallel WAL uploads, bounded catalog)**: **Partly confirmed, partly stale.**
  - Confirmed: single uploader thread (`uploads.rs:371-375`); ack path runs on the event loop and rewrites the full catalog twice, primary then mirror (`publication.rs:118-127`, `catalog.rs:500-535`, `hybrid_persistence.rs:480-495`).
  - Refuted/stale: "segment bytes read back twice on providers without receipts" is only the no-receipt case. With receipts the uploader's readback is skipped (`uploads.rs:487-497`) and the runtime skips its own via `take_verified_upload` (`uploads.rs:22-31`, `hybrid_persistence.rs:340-342`); control PUT readback is skipped on a receipt (`control_io.rs:192-209`).
  - R1 (contiguous publication via the existing chain rule) is already in place (`durability.rs:313-327`, `publish.rs:283-354`).
  - R2 (one catalog write per round covering every newly uploaded contiguous segment) is **already how publication works** (one CAS per contiguous run, `publication.rs:96-127`); what is missing is a time-based batching interval, and it only pays off once uploads can complete out of order.
  - R4 "compact retired entries" is **already done** (`cloud_catalog.rs:562-567`); the remaining unbounded part is unretired entries, bounded in practice by `eventual_flush_segment_gap=128` and the 1 024-entry held limit. The issue's 100 KB per 1 000 entries estimate looks 3-4x low (my arithmetic from the serde structs, not measured).
  - Optional mirror: the mirror is part of cache validity (`backend.rs:376-380`) and repair (`catalog.rs:261-351`), so dropping it changes recovery behaviour; needs its own decision.
  - AC1 (4-concurrent upload mock test) has no existing test; none to extend.
- **#907 (mirror/seal/ack off the loop)**: **Confirmed.** Seal (`sealing.rs:15-27`), ack verify plus catalog CAS (`publication.rs:11-93`) and metadata mirror (`metadata_mirror.rs:4-46`) run inline with `event_loop_cloud_deadline` (`mod.rs:596-598`). One correction to the issue's risk text: WAL catalog publication is **not** serialized by `ManifestPublicationGate` (that gate covers Flush/WalPrune/Compaction, `coordination.rs:381-426`); it is serialized by the `wal_catalog_mutation` mutex inside `HybridStorage` (`backend.rs:170-172`) plus provider CAS. Retirement is already off the loop (`cloud.rs:166-191`).
- **#913 (metadata generation GC)**: **Not contradicted by what I read; one added constraint.** I found only creates of `metadata/generations/` keys in non-test `src/` (`metadata_snapshot.rs:346`, `ddl.rs:1628`, `lease/mod.rs:430`); no delete sites seen. New constraint from my area: the catalog's `retired_by` records the shared generation that authorized retirement (`cloud_catalog.rs:283-286`, `540-549`), and recovery verifies it covers the retirement point (`wal-redesign-808.md:29-30`). GC must therefore never delete the generation named by `catalog.retired_by` (or any later needed one). #913 R2 does not mention this.
- **#943 (durable delete intents + orphan sweeper)**: **Confirmed.** `GcActor` queues are in-memory (`gc.rs:20-48`); remote WAL deletes retry from an in-memory `PruneWorkerRegistry.pending` (`proofs.rs:55-62, 781-788`); a crash between the catalog retirement CAS and the DELETE strands the object (`hybrid_persistence.rs:480-509`, `ack.rs:371-386`). I saw no LIST-based sweeper outside providers/simulation. Design note **(inference)**: for WAL, the catalog `anchor.id` is already a durable lower bound, so any WAL object whose segment id is `<= anchor.id` is safe to delete after the lease/identity checks, which could make WAL orphans cheap to sweep without a new intent journal. Objects above the anchor from fenced writers still need #943's safety window.
- **#915 (server-side checksums + multipart)**: **Partly refuted for WAL; premise wrong about checksums.** The issue says no `x-amz-checksum` usage exists under `src/storage/`; `src/storage/providers/s3.rs:851, 1363-1364, 1472` implements opt-in `x-amz-checksum-crc32c` PUT receipts, and the WAL upload and control writes already consume receipts (`uploads.rs:469-497`, `control_io.rs:192-209`). No multipart anywhere (`rg -i multipart src` returned nothing), so that half stands. For WAL, whole-segment `read_all` bounds memory at the segment size (16 MiB seal threshold by default), so multipart is mainly an SST concern. SST publication code (`file_publication.rs`) was not read in this pass beyond line 111.

---

#### 9. Findings worth carrying into the design doc

- **F1. Strict commit error flattening.** Every post-apply failure of `cloud_strict` becomes `OutcomeUnknown` (`transaction.rs:727-735, 1019-1027`), including `Corruption` and `Fenced` which are definitively not retryable. Meanwhile `wait_cloud_durable` returns typed errors (`engine/mod.rs:546-564`). Two callers of one runtime message, two error contracts.
- **F2. Retirement has seven decision sites in four modules** (section 2). A single `RetirementProof` type is a natural phase 1 target; candidate pre-filters at `cloud.rs:219-233` duplicate checks the catalog layer does authoritatively.
- **F3. Split retry ownership of upload** (storage backoff vs runtime backlog), documented in comments but a real seam for a worker pool.
- **F4. No authority re-check between read and PUT in the uploader** (`uploads.rs:241-251` vs `442-478`); safe only because keys are epoch-named and `If-Absent` (`hybrid_persistence.rs:215-224`). Needed for #916/#943 reasoning about orphans.
- **F5. Remote delete retry state is memory-only** (`proofs.rs:55-62`); crash after catalog retirement leaks the object.
- **F6. Lease failure at ack requeues instead of fencing** (`publication.rs:26-34, 129-138`), unlike catalog-detected `Fenced` (`publication.rs:168-170`). Observable result is the same for writers (`lease_healthy` flips, `fencing.rs:71-75`), but the WAL transition protocol is left unfenced and waiters see `Fenced` while the segment is requeued.
- **F7. Catalog is re-decoded and fully re-PUT on every mutation**, twice (primary, mirror), on the event loop; the cache removes GETs but not the encode/decode CPU or the PUT bytes (`catalog.rs:405-416, 500-535`).
- **F8. No time-based CloudAsync lag bound.** Only queue-fill and disk admission stop a writer running ahead of an unreachable cloud.

#### 10. Extra files read (beyond the starting list)

`src/runtime/hybrid_persistence.rs`, `src/runtime/hybrid_persistence/catalog.rs`, `src/runtime/hybrid_persistence/metadata_snapshot.rs` (1-150), `src/runtime/durability.rs` (230-430), `src/runtime/event_loop/cloud_integration/ack.rs`, `src/runtime/event_loop/wal_retention/cloud.rs`, `src/runtime/event_loop/wal_retention.rs` (50-73), `src/runtime/event_loop/wal.rs` (340-500), `src/runtime/event_loop/coordination.rs` (1-60, 100-430), `src/runtime/event_loop/wal_transition.rs` (1-80), `src/runtime/event_loop/fencing.rs`, `src/runtime/event_loop/durability_sync.rs` (1-200), `src/runtime/event_loop/mod.rs` (560-630), `src/runtime/wal_transition.rs` (172-197), `src/runtime/wal_history.rs` (703-735), `src/runtime/actors/wal.rs` (700-775), `src/runtime/config.rs` (10-46), `src/runtime/state/flush.rs` (455-470), `src/storage/hybrid/backend.rs`, `src/storage/hybrid/backend/queues.rs`, `reservations.rs`, `control_io.rs` (142-232, 420-440), `src/storage/hybrid/actor.rs` (120-200), `src/storage/mod.rs` (258-290, 329-352), `src/storage/cloud/config.rs`, `src/engine/api/transaction.rs` (640-760, 855-892, 1019-1042), `src/engine/startup/storage.rs` (619-640), `src/wal/format/{link,segment}.rs` (structs only), `docs/development/wal-redesign-808.md`, test files cited in tables.

## Appendix D: SOLID review, preservation map and format inventory

### #873 research: SOLID review (R8), preservation map (R4), format inventory (R5)

Source: branch `develop` at 12962819 (clean). Read-only. Every `file:line` below was seen either through the Read tool or as the matching `rg -n` line of the definition. Line counts are `wc -l` (they include inline test modules unless stated).

Two scope notes found while reading:

- The issue lists `src/runtime/actors/wal/` but the actor itself is `src/runtime/actors/wal.rs` (1084 lines: `WalActor`, `WalIoState`, outcome classification, disk-budget admission). It is the largest WAL owner in the directory and is included below.
- `src/wal/recovery*`, `planner*` and `ledger*` import nothing from `runtime/`, `engine/` or `storage/` in production code. The ledger *history* logic (`WalHistory`, 1055 lines) lives in `src/runtime/wal_history.rs`, not in `src/wal/`, so "local history" is split across two layers by design (store/format low, history/fence high).

---

#### 1. SOLID verdicts

Rule applied: no new abstraction unless it removes a recorded violation. Proposed slice names are placeholders; the design doc owns the final names. `[SLICE-WRITER-CONTRACT]` = collapse the `WalWriter` surface to what production calls (see I1/L1). `[SLICE-OUTCOME]` = one typed commit-outcome function replacing scattered error rewrapping (R3 "commit outcomes").

| Principle | Verdict |
|---|---|
| S | partially holds |
| O | partially holds (provider seam open, writer seam closed) |
| L | partially holds |
| I | violated (trait), accepted (actor surface) |
| D | partially holds; AGENTS layering rule holds |

##### S: single responsibility

Line counts and responsibilities, `src/runtime/actors/wal*` (non-test unless noted):

| File | Lines | Responsibilities (reasons to change) |
|---|---:|---|
| `wal.rs` | 1084 (tests mod declared at 1009-1010) | (1) `WalIoState` durable-state machine `Memory/Open/Transitioning/Fenced` (172-192, 414-536). (2) disk-budget admission/settlement against `HybridStorage` (712-763). (3) commit-outcome classification `fail_unapplied_transactions`/`is_authority_failure`/`appended_unit_error` (856-930). (4) config setters and counters (594-700, 781-799). (5) lease/leader-store wiring and writer-authority construction (767-778, 1024-1083). (6) coalescing eligibility (932-951). Six reasons. |
| `transaction.rs` | 731 | admission/preparation (86-166), sequence allocation (505), WAL batch build (541-620), append incl. group admission (288-336, 623-711), conflict preconditions (379-492). |
| `transaction_state.rs` | 535 | MVCC write/assertion conflict validation (184-329) and memtable publication (416-535). No WAL effect at all; reason to change is MVCC/memtable semantics. |
| `spill.rs` | 505 | spilled-transaction validate (218), admit (348), append (390-473), apply (475). Re-implements conflict checks in `ensure_no_write_conflict_for_op` (294) next to `transaction_state.rs:184`. |
| `durability.rs` | 470 | fsync transition (143-262), ledger sync-boundary persist (264-284), receipt commit (286-313), cloud-flush seal (345-434), batch-timing policy `should_sync_batch`/`sync_deadline_timeout` (436-470). Flush timing and fsync I/O are separate reasons. |
| `rotation.rs` | 359 | rotation protocol and its reversible/irreversible step table (1-25, 47-295); writer re-creation (309-319); three failpoint boundaries (324-358). One reason, well documented. |
| `authority.rs` | 94 | adapts `LeaseValidity`/`LeaderStore` to `WriterAuthority` (11-94). One reason. |
| `apply_op.rs` | 222 | maps a transaction op to a `WalRecord`/memtable op. One reason. |

Other owners: `src/wal/fs/writer_io.rs` 1519 (enqueue/backpressure `333-441`, sync `581-649`, trait impls `457-554`; tests from `713`), `writer_runner.rs` 1089 (batching, positional write, rollback `408-441`, fsync completion `505-580`; tests from `685`), `ledger.rs` 714 (pure state machine, tests at `713`), `ledger_store.rs` 483 (journal I/O only), `planner.rs` 823 (pure, no I/O by design, `1-31`), `recovery.rs` 553, `publish.rs` 674 (pure ack/publication planner `21-356`, tests from `367`), `cloud_catalog.rs` 1156 (document model, encode/decode/validate; tests from `733`), `storage/hybrid/backend/uploads.rs` 691 (queue scheduling `231`, worker `361-534`, ack events `535-691`), `proofs.rs` 976 (proofs `141-395`, CAS `397-530`, delete admission/workers `555-960`, plus unrelated SST accessors `128-139`).

Answer to the issue's question ("admission, sequencing, flush timing, rotation outcomes: separate reasons to change?"): yes. In `src/runtime/actors/wal/` there are at least five independent reasons to change: admission/validation (`transaction.rs`, `transaction_state.rs`, `spill.rs`), sequencing/batch build (`transaction.rs:505-620`), flush/sync timing (`durability.rs:436-470`), rotation outcomes (`rotation.rs`), and the durability state machine plus outcome classification (`wal.rs`). They are already separated by file (rotation, durability, spill, authority) and share one `WalActor` struct that is the single owner of `WalIoState`, which is what the transition invariant at `wal.rs:27-44` requires. The pure planner/ledger/publish/catalog owners each have one reason to change.

Violations and decisions:

- S1. `transaction_state.rs` (MVCC conflict validation + memtable apply) and the conflict logic duplicated in `spill.rs:294-346` are not WAL concerns. DECISION: recorded as a gap. Moving them is a behavior-preserving file/module split with no new abstraction (AGENTS allows mechanical splits without an issue), but it is not needed by the write-to-recovery design; do it only inside a slice that already edits `spill.rs`/`transaction.rs`.
- S2. Outcome classification is spread over `wal.rs:856-930`, `transaction.rs:683-692`, `transaction.rs:341-353`, `durability.rs:276-281`, `spill.rs:467-469`, `wal.rs:754-761`. Each site decides separately how `WalAppendError.unchanged` becomes `OutcomeUnknown` vs the raw error vs `RecoveryFailed`. DECISION: fixed in `[SLICE-OUTCOME]` (this is the R3 "commit outcomes" owner; it replaces code, adds one function, no trait).
- S3. `durability.rs:143-262` `begin_sync_io` mixes authority check, state transition, failpoints, panic containment (`181-190`), counters, an env-var trace read per sync (`234`), and ledger persistence. DECISION: accepted. It is one transition; the separately-pinned boundaries are the failpoints. Revisit only with #904 (it deletes `persist_sync_boundary`, `264-284`).
- S4. `proofs.rs:128-139` holds `remote_sst_backend`/`ephemeral_sst_cache_enabled`, unrelated to proofs; WAL vocabulary (`spawn_wal_upload_worker` `uploads.rs:361`, `queue_cloud_wal_prune_*` `33-45`) sits in the storage layer even though it imports no WAL types. DECISION: accepted (out of Phase 0 owners' reasons; `docs/development/architecture.md` boundary "HybridStorage ... does not import WAL formats" still holds by import).
- S5. `wal.rs:12-13,262-264` module and field docs are stale ("ALWAYS uses FsWalWriter", "Stamped on every WAL record" for the epoch). Records carry no epoch since #843 (`traits.rs:40-41`, `encoding.rs:11`). DECISION: fixed in whichever slice next edits `wal.rs` (doc-only).

##### O: open/closed

Can a new writer or storage provider be added without editing WAL core?

- Storage provider: yes. A provider implements the storage/cloud backend traits below the WAL; `uploads.rs` and `proofs.rs` import no `crate::wal`/`crate::runtime` (checked with `rg "crate::(wal|runtime)"`, zero hits). The catalog and publication logic (`wal/publish.rs`, `wal/cloud_catalog.rs`) is provider-neutral. No WAL edit needed.
- Writer: no. The actor hard-codes the only writer factory: `use crate::wal::FsWalFactoryIo` at `wal.rs:51`, constructed at `wal.rs:631` (open) and `rotation.rs:314` (`segment_writer`), imported at `rotation.rs:34`. `WalIoState::Open` stores `fs: Arc<dyn Fs>` plus the writer (`wal.rs:174-177`), so a non-filesystem writer would also change the state enum. There is no factory trait; `FsWalFactoryIo::open_segment` (`factory_io.rs:59`) is an inherent method.
- Grouping seam: `GroupedWalWriter` (`traits.rs:46-70`) is expressed in `crate::lease::WriterEffectGroup`, so it is open to implementors only if they use lease groups; `writer_group_capability` (`traits.rs:78`) returns `None` by default and `FsWalWriterIo` returns `Some` only while its authority has groups (`writer_io.rs:462-468`). Actor callers branch on the capability at `transaction.rs:294-300,671-681` and `durability.rs:181-189`, so adding a *new kind* of grouped writer needs no actor edit, but adding a *non-grouped* writer already works.

Decision: O1 (hard-coded `FsWalFactoryIo`): accepted. There is one writer implementation and no planned second one in #904/#916/#941/#947 (all keep `FsWalWriterIo`); a factory trait would be an abstraction that removes no behavior defect. Revisit if a second writer is proposed. O2 (lease-shaped group trait): accepted, lease is the only authority that can produce native groups.

##### L: Liskov substitution

All `WalWriter` impls (found with `rg "impl.*WalWriter for"`; 5 total):

| Impl | Location | Notes |
|---|---|---|
| `FsWalWriterIo` | `src/wal/fs/writer_io.rs:457` (+ `GroupedWalWriter` at 531) | The only production writer. Overrides every method. |
| `FsyncFailingWriter` | `src/runtime/actors/wal/tests.rs:653` | Delegates append to inner; `sync` always errs; `write_footer` errs; no `append_*_accounted` override, so failures map to `unchanged: false`. |
| `UncertainAppendWriter` | `src/runtime/actors/wal/tests/admission.rs:395` | `append_record` always errs (Timeout or NoSpace); relies on default `append_record_accounted` (`traits.rs:96-98`) so it can only produce `unchanged: false`. |
| `PanickingSyncWriter` | `src/runtime/event_loop/durability_sync.rs:525` | `append_*` return `Ok(0)`; `sync` panics once. |
| `CountingSyncWriter` | `src/runtime/event_loop/shutdown.rs:548` | `append_*` return `Ok(0)`; `sync` counts. |

`WalAppendError { unchanged }` (`traits.rs:16-19`) means "operation-specific proof that no bytes remain". `FsWalWriterIo` honours it precisely: `unchanged: true` only before queue submission or after a durable rollback proof (`writer_io.rs:340,353,360,429,433`; `writer_runner.rs:292,335,339,357,376,396,404`), and `unchanged: false` after a partial write whose rollback did not complete or was not synced (`writer_runner.rs:417,425,432,439`) and after an acknowledgement timeout (`writer_io.rs:401`). The trait defaults are conservative (`unknown`, `traits.rs:96-98,121-123`). The three actor consumers read it consistently: budget refund only on `unchanged` (`wal.rs:754`), `OutcomeUnknown` wrapping when not unchanged (`transaction.rs:683-692`), `wal_may_have_changed |= !unchanged` (`spill.rs:468`).

Divergences found:

- L1. Default `append_batch` for an empty slice returns `Ok(0)` (`traits.rs:108-112`, `last_pos = 0`); `FsWalWriterIo` returns `current_pos()` (`writer_io.rs:562-564`). A double inheriting the default returns a position that is not a valid WAL position.
- L2. Default `sync_with_timeout` discards the timeout (`traits.rs:136-139`); `FsWalWriterIo` honours it (`writer_io.rs:633-646`). All four doubles inherit the ignoring default, so no double can model a deadline expiry.
- L3. Default `append_batch` is not atomic (loops `append_record`, `traits.rs:106-113`), contradicting its doc ("a single write"); `FsWalWriterIo` enqueues one buffer (`writer_io.rs:576`).
- L4. `GroupedWalWriter::begin_group` on `FsWalWriterIo` returns `Internal` if the authority has no groups (`writer_io.rs:536-537`), while the capability can change at runtime through `set_write_authority` (`writer_io.rs:458-460`); the actor guards with the capability check, so this is only a TOCTOU between capability and call. Not reachable on the event-loop thread (authority is set from the same thread).
- L5. No double overrides `set_write_authority` (`traits.rs:74`), so actor-level fencing is bypassed in every double test; and no double implements `append_*_accounted`, so the `unchanged: true` branch of every consumer is exercised only through real `FsWalWriterIo` over a failing `Fs` (`writer_runner.rs:806`, `writer_io/tests/writer_groups.rs:285,619`, `runtime/actors/wal/tests/admission.rs:434`).
- L6. No trait-level contract test exists for "every impl returns `unchanged: true` only with proof"; the contract is pinned per implementation only.

Decisions: L1-L3 fixed in `[SLICE-WRITER-CONTRACT]` by deleting the defaulted non-accounted methods (see I1), which removes the divergent defaults instead of fixing them (supports the "delete dead API" rule: production never calls `append_record`, `append_batch` or `sync`, see I1). L4 accepted (single-thread authority install, documented here). L5 recorded as a gap (add `set_write_authority`/accounted overrides to the shared test double only inside the slice that touches the contract). L6 recorded as a gap, to be covered by that slice's failing test written first.

##### I: interface segregation

`WalWriter` (`traits.rs:72-163`) has 12 methods:

| Method | Default | Production callers |
|---|---|---|
| `set_write_authority` | no-op (74) | `wal.rs:302,325,488,776,1030`, `rotation.rs` via factory |
| `writer_group_capability` | `None` (78) | `transaction.rs:297,675`, `durability.rs:183` |
| `append_record` | required (88) | none in production; only tests (`state/recovery/authority_tests.rs:164,192`, `factory_io.rs:151,178` which are in its test module from `factory_io.rs:100`) |
| `append_record_accounted` | maps to `unknown` (96) | `spill.rs:467` |
| `append_batch` | loop (106) | none in production |
| `append_batch_accounted` | maps to `unknown` (121) | `transaction.rs:680` |
| `sync` | required (130) | none in production |
| `sync_with_timeout` | calls `sync` (136) | `durability.rs:188,397` |
| `current_pos` | required (142) | `wal.rs:665,744`, `durability.rs:270` |
| `tail` | required (144) | `wal.rs:961` |
| `seal_facts` | required (154) | `rotation.rs:185` |
| `write_footer` | required (162) | `rotation.rs:219` |

So the actor depends on 9 of 12; `append_record`, `append_batch`, `sync` are required/defaulted surface used only by tests. Every double must stub `tail`, `seal_facts`, `write_footer` through the `unsealable` helper module added only for them (`traits.rs:165-187`). The write path (append), the durability path (sync), the sealing path (seal_facts/write_footer/tail) and the authority path (set_write_authority/groups) are four different callers' views of one trait.

Does the runtime actor expose more than callers need? `WalActor` is `pub struct` (`wal.rs:241`, re-exported `runtime/actors/mod.rs:21`) with about 56 `pub`/`pub(crate)`/`pub(super)` fns across `wal.rs`, `durability.rs`, `transaction.rs`, `spill.rs`, `rotation.rs`. The event loop names 28 distinct methods through `wal_actor.` outside `src/runtime/actors/` (most used: `is_cloud_async` 25, `is_fenced` 11, `append_single_op` 4 and `append_single_range_delete` 1, both defined in the `#[cfg(test)]` module `test_append.rs`). Setup (`set_leader_store`, `set_lease_validity`, `set_read_resources`, `set_max_replayable_txn_bytes`, `attach_counters`), write path, sync-timing queries, rotation and retirement are all on one type. There is exactly one consumer, the event loop.

Decisions: I1 (fat `WalWriter`, test-only methods): fixed in `[SLICE-WRITER-CONTRACT]`: delete `append_record`, `append_batch`, `sync` and the non-accounted/accounted duplication, leaving `append_batch_accounted`-shaped single append, `sync_with_timeout`-shaped single sync; update the four doubles and the test callers. This removes a recorded violation by deleting surface, not adding a trait. I2 (`tail`/`seal_facts`/`write_footer` stubs): accepted, because splitting a `SegmentSealer` trait is an abstraction that does not remove I1's defect once the dead methods are deleted. I3 (wide actor surface): accepted, single in-process consumer, no polymorphism need, splitting would add traits with one implementor.

##### D: dependency inversion

What the WAL/runtime modules name concretely:

- `crate::wal::FsWalFactoryIo`: `wal.rs:51,631`, `rotation.rs:34,314` (see O1).
- `crate::storage::HybridStorage` (concrete): `wal.rs:245` field, `694` setter, used at `720-763` for `ephemeral_sst_cache_enabled`, `admit_local_wal_bytes` (`storage/hybrid/backend/reservations.rs:71`), `settle_local_wal_admission` (`:92`). Three-method need, no trait.
- `crate::lease::LeaseValidity` (concrete struct): `wal.rs:265`; but `crate::lease::LeaderStore` is a trait (`wal.rs:270`) and `Fs` is a trait (`wal.rs:175`), `WalWriter` and `WriterAuthority` are traits.
- `crate::lease::WriterEffectGroup` (concrete) in `wal/traits.rs:51,59,68` and `wal/writer_authority.rs:4`.
- `RuntimeState`/`WalHistory`: every actor method takes `&mut RuntimeState` and mutates `state.wal.pending_writes`, `state.sequence`, frontiers (`transaction.rs:703-708`, `durability.rs:304-308`). Same layer.

AGENTS.md layering rule ("lower layers should not depend on higher layers"), `rg "crate::(runtime|engine|storage)" src/wal`:

- Production code in `src/wal/`: zero imports of `runtime/`, `engine/` or `storage/`. Non-test dependencies are `common`, `io`, `types`, `config` (`planner.rs:33`), `metadata::frontiers` (`replay.rs:4`, `recovery.rs:434`), `memtable` (`recovery.rs:10`), `telemetry` (`writer_io.rs:48`, `factory_io.rs:14`, `writer_runner.rs:150`, `frame.rs:618,714`, `recovery/streaming.rs:37,220,251,304,349,527,642`), `lease` (`traits.rs:51,59,68`, `writer_authority.rs:4`, `writer_io.rs:337,532,543,550,560,584`), `cloud_layout` (`wal/mod.rs:103`).
- Test-only upward imports (inside `#[cfg(test)]`, not violations of production layering): `src/wal/fs/writer_io.rs:1321,1327` (test module begins `713`) and `src/wal/recovery/tests/ledger_chain.rs:6` use `crate::runtime::wal_history`.
- `src/storage/hybrid/backend/uploads.rs` and `proofs.rs` import only `super::*`, `common`, `storage` (checked with `rg "^use "`): no WAL or runtime dependency.
- Transitive: `wal -> lease -> storage` (`lease/mod.rs:241,280,306,315,398`). `wal` does not import `storage` directly. Not a violation of the rule as written, but `wal` is not independent of `storage` build-wise.

Decisions: layering rule: holds, no decision needed. D1 (concrete `FsWalFactoryIo`): accepted with O1. D2 (concrete `HybridStorage` for the three-method disk budget): accepted; the dependency points down (runtime -> storage), has one implementation, and the budget tests use the real storage (`admission.rs:438-449`). D3 (`WalHistory` in `runtime/`, test-only imports from `wal/` tests into `runtime/`): accepted, tests only. D4 (`LeaseValidity` concrete in actor): accepted, same reason as D2.

---

#### 2. Preservation map

Legend: Owner = module and `file:line` of the defining symbol on `develop`. Test = exact fn and file (found with `rg "fn <name>"`). `GAP` rows state what was searched. Drift note: `docs/development/invariant-evidence.json` is stale relative to develop (symbols like `WalActor::admit_wal_records@wal.rs:706` are now `wal.rs:712`; the JSON lacks `CLOUD-8` and `SST-10`, which `invariant-evidence.md` has; 16 test names it cites no longer exist). All test names below were re-found with `rg`, not copied from the evidence file.

##### 2a. #808 contracts (`docs/development/wal-redesign-808.md`)

| ID | Statement | Owner | Pinning test | Owner file:line |
|---|---|---|---|---|
| 808-C1 | Explicit WAL history and sequence allocation: `wal/LEDGER` holds membership, order, linked identities, durable sequence reservation; directory contents do not establish history | `WalLedger`, `WalHistory` | `should_raise_reservation_when_reserving` (src/wal/ledger/tests.rs:839); `should_allocate_wal_segment_ids_above_previous_runs_when_all_sealed_segments_were_pruned` (tests/lifecycle.rs:2776) | src/wal/ledger.rs:278 (`apply`), 463 (`reserve`), 517 (`next_segment_id`); src/runtime/wal_history.rs:564 (`reserve`) |
| 808-C2 | Replay only authorized history above family flush frontiers; SST contents never prove coverage | planner + replay selection | `should_select_whole_unit_when_commit_exceeds_its_family_frontier` (src/wal/replay.rs:52); `should_not_read_ssts_to_decide_wal_replay_or_retention` (src/runtime/state/tests/frontier_authority.rs:116); `should_replay_only_the_chain_prefix_before_the_first_hole` (src/wal/planner/tests.rs:570) | src/wal/planner.rs:355 (`plan`); src/wal/replay.rs:25 (`should_apply`); src/wal/recovery/streaming.rs:278 |
| 808-C3 | Retire only a prefix after durable authority advances (local `log_start` before file removal; cloud `retired_by` before conditional delete) | `WalHistory::retire_below`, cloud retirement | `should_retire_local_wal_from_durable_frontiers_without_opening_sst_payloads` (src/runtime/event_loop/flush_pipeline/tests.rs:1304); `should_retain_remote_wal_until_the_family_frontier_is_committed_to_shared_metadata` (src/runtime/hybrid_persistence/tests.rs:2686) | src/runtime/wal_history.rs:712; src/wal/ledger.rs:522 (`log_start`); src/runtime/hybrid_persistence.rs:402 |
| 808-C4 | Fence ownership and every effect: stale cache or worker cannot alter authority or apply an effect after takeover | `WalHistory` fence, `WriterAuthority`, group coordinator | `should_fence_stale_history_before_successor_wal_effects` (src/runtime/wal_history/tests.rs:45); `should_finish_an_admitted_writer_effect_before_successor_acquisition` (src/lease/fs_leader_store.rs:1686); `should_hold_filesystem_writer_fence_around_background_append` (src/wal/fs/writer_io.rs:1235) | src/runtime/wal_history.rs:60 (`with_writer_fence`), 381 (`under_writer_fence`); src/wal/writer_authority.rs:30; src/lease/writer_effect_groups.rs:227 |
| 808-C5 | Truthful outcomes (`OutcomeUnknown`), salvage only verified history | `WalActor` outcome classification; planner | `should_not_replay_commit_reported_as_retryable_when_wal_sync_fails` (tests/fault_injection.rs:2902); `should_keep_acknowledged_writes_when_salvage_replay_hits_a_transient_io_error` (tests/wal_replay_io_failure.rs:73) | src/runtime/actors/wal.rs:856 (`fail_unapplied_transactions`), 922 (`appended_unit_error`) |
| 808-CAT | Cloud history is the published chain in catalog v3 with anchor and monotonic segment allocation; publication adds only a contiguous run | `WalPublicationCatalog`, `AckSet` | `should_reject_catalog_whose_first_entry_does_not_link_the_anchor` (src/wal/cloud_catalog.rs:913); `should_append_run_past_head_when_catalog_publishes_a_prefix` (src/wal/publish.rs:536); `should_stop_run_when_a_middle_segment_is_not_uploaded` (src/wal/publish.rs:455) | src/wal/cloud_catalog.rs:24 (`FORMAT_VERSION`), 650 (`validate`); src/wal/publish.rs:283 (`uploaded_run`), 319 (`additions`) |
| 808-FRONT | A flush publishes the SST and advances its family frontier in one manifest edit | `Manifest::prepare_edit` | `should_leave_every_field_unchanged_when_a_nested_batch_has_an_invalid_transition` (src/metadata/manifest/tests.rs:23); `should_retain_frontiers_when_compaction_or_reclamation_removes_every_sst` (src/metadata/manifest/tests.rs:183) | src/metadata/manifest.rs:102 |
| 808-RETL | Local retirement persists new `log_start` before queuing file removal; failed removal never makes retired history replayable | `WalHistory::retire_below` | `should_not_resurrect_deleted_key_when_retired_local_wal_copy_survives_restart` (tests/fault_injection/local_wal_gc.rs:105) | src/runtime/wal_history.rs:712 |
| 808-RETC | Cloud retirement commits frontiers in a shared generation, records it in `retired_by`, then queues identity-conditional deletes; recovery verifies the shared generation covers the retirement point | `CloudPersistence`, catalog, proofs | `should_retain_catalog_authority_when_the_captured_shared_generation_changes` (src/runtime/hybrid_persistence/tests.rs:2798); `should_reject_stale_remote_wal_target_identity_when_guarded_delete_runs` (src/runtime/hybrid_persistence/tests.rs:1884) | src/runtime/hybrid_persistence/catalog.rs:111 (`retire_prefix`); src/runtime/hybrid_persistence/metadata_snapshot.rs:77; src/storage/hybrid/backend/proofs.rs:598 |
| 808-FMT | Unsupported persisted formats rejected before mutation, no migration; manifest/FORMAT 8, catalog 3 | `validate_format_marker`, catalog decode, ledger open | `should_reject_open_given_future_format_version_when_starting` (src/metadata/format.rs:271); `should_reject_open_given_format_version_older_than_eight` (src/metadata/format.rs:249); `should_report_another_format_version_as_incompatible` (src/wal/cloud_catalog.rs:982); `should_fail_when_version_is_unsupported` (src/wal/ledger_store/tests.rs:539) | src/metadata/format.rs:13, 78; src/wal/cloud_catalog.rs:325; src/wal/ledger_store.rs:39 |
| 808-FMT-SEG | Same rule for segment header version (`SEGMENT_VERSION`) and record version (`VERSION`) | `SegmentHeader::decode`, `encoding` | **GAP**: only `should_reject_header_when_crc_mismatches` (src/wal/format/segment/tests.rs:66) and `should_error_when_magic_invalid` (src/wal/encoding.rs:1288) exist; searched `src/wal` for `version|magic` test names, none rejects a wrong segment-header version or record version. | src/wal/format/segment.rs:120-124; src/wal/encoding.rs:36 |
| 808-820a | Takeover is based on persisted ownership and chained history, not a stale cache's view | `WalHistory`, planner `lineage` | `should_preserve_successor_strict_commits_when_predecessor_reopens_old_cache` (tests/fault_injection/wal_authority_regressions.rs:87); `should_rebase_strictly_when_the_catalog_set_this_history_aside` (src/wal/planner/tests.rs:345) | src/wal/planner.rs:54 (`lineage`); src/runtime/wal_history.rs:748 (`set_aside`) |
| 808-820b | Worker lifetime explicit: ownership not released until the worker is confirmed stopped; hung worker cannot block the event loop | writer runner + group coordinator | `should_keep_transition_fenced_when_an_activated_job_outlives_its_root` (src/lease/fs_leader_store/group_tests.rs:58); `should_release_queued_native_tickets_when_worker_panic` (src/wal/fs/writer_io/tests/writer_groups.rs:535); `should_retain_primary_lease_when_engine_drops_with_blocked_wal_upload` (tests/fault_injection.rs:5624) | src/wal/fs/writer_runner.rs:658 (`Drop`); src/wal/fs/writer_io.rs:697 |
| 808-820c | #811: successor acquires a greater epoch after completed release then power loss; fail closed on power loss during acquisition | `FsLeaderStore` | `should_allow_successor_after_power_loss_follows_completed_release` (src/lease/fs_leader_store.rs:1912); `should_fail_closed_when_power_loss_interrupts_acquisition_lock` (src/lease/fs_leader_store.rs:1979); `should_allow_successor_when_power_loss_follows_completed_lease_release` (tests/wal_campaigns.rs:172) | src/lease/fs_leader_store.rs:1006 (`release_if_owner`) |
| 808-821a | Salvage uses only the longest verified prefix, stops at first damaged/missing point | planner | `should_replay_only_the_chain_prefix_before_the_first_hole` (src/wal/planner/tests.rs:570); `should_not_replay_segments_after_invalid_segment_when_cloud_salvage_skips_one` (src/runtime/cloud_startup/streaming_wal_plan/tests.rs:736) | src/wal/planner.rs:355 |
| 808-821b | Persist the sequence floor before truncating, quarantining or setting aside | `WalHistory::prepare_open_cut`/`set_aside`, `apply_open_cut` | `should_preserve_sequence_floor_when_salvage_is_interrupted_before_authority_commit` (src/runtime/cloud_startup/streaming_wal_plan/tests.rs:1597) | src/runtime/wal_history.rs:625, 748; src/runtime/cloud_startup/streaming_wal_plan.rs:487 |
| 808-821c | Timeouts, I/O failures, resource limits, uncertain authority fail closed without destructive salvage | cloud startup plan | `should_leave_wal_history_unchanged_when_salvage_lacks_family_authority` (src/runtime/state/recovery/authority_tests.rs:148); `should_refuse_missing_family_authority_when_replaying_under_salvage_policy` (src/wal/recovery/streaming/tests.rs:1010) | src/runtime/cloud_startup/mod.rs:92 |
| 808-821d | Corruption in a local prefix already retired by cloud authority does not pin later history | cloud startup plan | `should_ignore_corrupt_local_copy_of_retired_prefix_when_later_history_remains` (src/runtime/cloud_startup/streaming_wal_plan/tests.rs:2232) | src/wal/planner.rs:355 |
| 808-821e | Equivalent histories give the same outcome classes through local and cloud recovery | `replay_wal_with_options` vs cloud inspector | **GAP** (end-to-end): only `should_classify_identically_across_walkers_when_given_fuzzed_wal_bytes` (src/wal/recovery/tests.rs:1879), which compares two frame walkers on one active file. Searched tests/ and src/ for `parity|local_and_cloud|across_modes`; no test runs the same history through the local and cloud recovery entry points. | src/wal/recovery/streaming.rs:278; src/runtime/cloud_startup/streaming_wal_plan.rs:70 |
| 808-822a | `OutcomeUnknown` when a failure follows WAL effects; never retryable `NotApplied` | `WalActor` | `should_not_replay_commit_reported_as_retryable_when_wal_sync_fails` (tests/fault_injection.rs:2902); `should_retain_wal_admission_when_failed_append_has_no_rollback_proof` (src/runtime/actors/wal/tests/admission.rs:434) | src/runtime/actors/wal.rs:856, 922; src/runtime/actors/wal/transaction.rs:683-692 |
| 808-822b | Admit whole transaction (incl. replay envelope) before first WAL effect; budget rejection leaves WAL usable | `WalActor` | `should_keep_wal_open_when_spilled_transaction_exceeds_remaining_wal_budget` (src/runtime/actors/wal/tests/admission.rs:525); `should_reject_transaction_when_no_space_hits_before_batch_append_and_remain_usable` (tests/fault_injection.rs:266) | src/runtime/actors/wal.rs:685 (`ensure_replayable_transaction`), 712; src/runtime/actors/wal/spill.rs:348 |
| 808-822c | One bounded replay rule; abandoned split transaction cannot block checkpoint replay | streaming replay | `should_checkpoint_after_abandoned_split_transaction_when_newer_epoch_records_follow` (src/wal/recovery/streaming/tests.rs:748) | src/wal/recovery/streaming.rs:257, 278 |
| 808-822d | Mutable runtime state stays on the event loop; workers return results | event loop + actors | `should_route_manifest_file_list_mutation_through_runtime_owner` was cited by evidence but **does not exist** (rg over src, tests). GAP for WAL-specific ownership: `tests/solid_governance.rs` searched for a WAL equivalent, none found. | src/runtime/actors/wal/transaction.rs:703-708 (actor mutates `RuntimeState` on the loop thread) |
| 808-823 | Catalog rows for WAL-2..5/CLOUD-3/5 reflect the design; `cloud_async` takeover contract documented; stale validator reference removed | docs | Documentation row, no test. `rg "validate-tests\|validator"` over invariant-catalog.md and wal-redesign-808.md finds the only mention at wal-redesign-808.md:109 (the instruction to remove it). Not a code GAP; verify in the design doc pass. | docs/development/invariant-catalog.md:67-73, 111-118 |
| 808-ACC | Final qualification: every redesign selector green; nightly WAL campaign budget | campaign harness | tests/wal_campaigns.rs (15 `should_*` tests, e.g. `should_keep_recovered_tail_when_power_is_lost_after_a_process_crash` at :202); examples/wal_redesign_baselines.rs | tests/wal_campaigns.rs; examples/wal_redesign_baselines.rs |

##### 2b. Original #808 regression inventory (`wal-redesign-regressions-808.json`, 18 + 5 supplemental, all found at their JSON paths)

| ID | Statement | Owner | Pinning test | Owner file:line |
|---|---|---|---|---|
| best-effort-delete | Best-effort delete does not resurrect after clean shutdown / crash image | memtable/flush + WAL skip | `should_not_resurrect_best_effort_deleted_key_when_reopening_after_clean_shutdown` (tests/lifecycle.rs:2607); `..._when_recovering_a_crash_image` (tests/lifecycle.rs:2620) | src/engine/api/write_options.rs:149 |
| failed-local-unlink | Failed removal of older flushed segment does not resurrect a deleted key | local retirement | `should_not_resurrect_deleted_key_when_older_flushed_segment_removal_fails` (tests/lifecycle.rs:2734) | src/runtime/wal_history.rs:712 |
| retired-local-cloud-copy | Retired local WAL copy neither resurrects nor hides a delete | local GC | `should_keep_deleted_key_absent_without_compaction_when_local_wal_removal_fails` (tests/fault_injection/local_wal_gc.rs:110); `should_not_resurrect_deleted_key_when_retired_local_wal_copy_survives_restart` (:105) | src/runtime/event_loop/wal_retention.rs:8 |
| empty-range-pin | Empty range does not pin cloud WAL | cloud retirement | `should_retire_cloud_wal_when_empty_range_has_no_replayable_effect` (tests/wal_retention_regressions.rs:98) | src/runtime/hybrid_persistence.rs:402 |
| dropped-family-pin | Dropped / discarded family does not pin cloud WAL | cloud retirement | `should_retire_cloud_wal_when_flushed_family_is_dropped` (:111); `should_retire_cloud_wal_when_unflushed_family_is_discarded` (:124) (tests/wal_retention_regressions.rs) | src/runtime/hybrid_persistence.rs:402 |
| predecessor-cache | Predecessor reopening an old cache cannot erase successor strict commits | planner lineage | `should_preserve_successor_strict_commits_when_predecessor_reopens_old_cache` (tests/fault_injection/wal_authority_regressions.rs:87) | src/wal/planner.rs:54 |
| out-of-order-publication | A later upload is not published while an earlier segment is unacknowledged | `AckSet` | `should_not_publish_later_upload_when_preceding_segment_is_unacknowledged` (src/runtime/event_loop/cloud_integration/tests.rs:8608) | src/wal/publish.rs:283 |
| restarted-segment-id | Segment ids allocate above previous runs when all sealed segments were pruned | ledger | `should_allocate_wal_segment_ids_above_previous_runs_when_all_sealed_segments_were_pruned` (tests/lifecycle.rs:2776) | src/wal/ledger.rs:517 |
| noncorruption-salvage | Transient I/O during salvage does not drop acknowledged writes | planner/salvage | `should_keep_acknowledged_writes_when_salvage_replay_hits_a_transient_io_error` (tests/wal_replay_io_failure.rs:73) | src/wal/recovery/streaming.rs:278 |
| interrupted-salvage-floor | Interrupted salvage keeps its sequence floor | cloud startup | `should_preserve_sequence_floor_when_salvage_is_interrupted_before_authority_commit` (src/runtime/cloud_startup/streaming_wal_plan/tests.rs:1597) | src/runtime/cloud_startup/streaming_wal_plan.rs:487 |
| substitute-upload-collision | Salvage substituting an intact local segment still permits strict writes | planner/ledger | `should_allow_strict_writes_when_salvage_substitutes_intact_local_segment` (tests/fault_injection/wal_authority_regressions.rs:259) | src/runtime/wal_history.rs:748 |
| retryable-fsync-failure | fsync failure is never retryable-NotApplied | `WalActor` | `should_not_replay_commit_reported_as_retryable_when_wal_sync_fails` (tests/fault_injection.rs:2902) | src/runtime/actors/wal/durability.rs:143 |
| partial-spilled-admission | Spilled transaction over the remaining WAL budget does not fence the WAL | `WalActor` | `should_keep_wal_open_when_spilled_transaction_exceeds_remaining_wal_budget` (src/runtime/actors/wal/tests/admission.rs:525) | src/runtime/actors/wal/spill.rs:348 |
| abandoned-split-transaction | Abandoned split transaction does not block checkpoint | streaming replay | `should_checkpoint_after_abandoned_split_transaction_when_newer_epoch_records_follow` (src/wal/recovery/streaming/tests.rs:748) | src/wal/recovery/streaming.rs:257 |
| failed-allocation-listing | Next segment id does not regress when WAL directory listing fails | ledger/state | `should_not_regress_next_segment_id_when_wal_directory_listing_fails` (src/runtime/state/recovery/timeout_tests.rs:511) | src/wal/ledger.rs:517 |
| inverted-epoch-history | Older-epoch segments not skipped when a regressed segment id holds a newer epoch | recovery | `should_not_skip_older_epoch_segments_when_a_regressed_segment_id_holds_newer_epoch` (src/wal/recovery/tests.rs:1903) | src/wal/recovery.rs (segment admission) |
| torn-unsynced-tail | Strict open accepts a torn unsynced multi-page tail | planner open cut | `should_open_strictly_when_power_loss_tears_unsynced_multi_page_tail` (tests/storage_root_injection.rs:811) | src/wal/planner.rs:492 (`admit_tail`) |
| recovered-tail-not-durable | Recovered tail stays after power loss following a process crash | open cut | `should_keep_recovered_tail_when_power_is_lost_after_a_process_crash` (tests/wal_campaigns.rs:202) | src/runtime/cloud_startup/streaming_wal_plan.rs:487 |
| lost-buffered-delete | Flushed delete not resurrected when power loss drops its WAL record | frontier + replay | `should_not_resurrect_flushed_delete_when_power_loss_drops_its_wal_record` (tests/wal_campaigns.rs:239) | src/wal/replay.rs:25 |

##### 2c. #869 (fenced Ledger repair and compaction through production fence)

| ID | Statement | Owner | Pinning test | Owner file:line |
|---|---|---|---|---|
| 869-1 | Failed LEDGER append sync, repair/retry, power-loss reopen preserves acknowledged reservation | `LedgerStore`, `WalHistory` | `should_preserve_acknowledged_reservation_when_fenced_append_is_repaired_after_sync_failure` (src/runtime/wal_history/tests/fenced_ledger.rs:64) | src/wal/ledger_store.rs:211 (`append`), 281 (`write_frame`) |
| 869-2 | Failed append then failed repair refuses next reservation without publishing it | `WalHistory::reserve` | `should_refuse_new_reservation_when_fenced_repair_publication_fails` (src/runtime/wal_history/tests/fenced_ledger.rs:95) | src/runtime/wal_history.rs:564 |
| 869-3 | Automatic compaction interrupted before/after publication preserves reservation | `LedgerStore::compact` | `should_preserve_acknowledged_reservation_when_fenced_automatic_compaction_is_interrupted` (src/runtime/wal_history/tests/fenced_ledger.rs:129); `should_keep_ledger_when_power_fails_during_compact` (src/wal/ledger_store/tests.rs:905) | src/wal/ledger_store.rs:262 |
| 869-4 | Successor takeover after predecessor append failure rejects predecessor repair before changing successor bytes | fence + store | `should_reject_stale_repair_without_changing_successor_reserved_history` (src/runtime/wal_history/tests/fenced_ledger.rs:169); `should_not_republish_stale_ledger_after_successor_changes_it` (src/wal/ledger_store/tests.rs:319) | src/runtime/wal_history.rs:60, 381 |

##### 2d. #870 / #872 (amortized writer fencing with counted effect groups)

| ID | Statement | Owner | Pinning test | Owner file:line |
|---|---|---|---|---|
| 870-overlap | Same-epoch independent effects share one native guard and overlap | group coordinator | `should_overlap_independent_effects_when_the_writer_epoch_matches` (src/lease/fs_leader_store.rs:1130); `should_share_one_fence_when_scoped_and_original_authorities_overlap` (src/lease/fs_leader_store/group_tests.rs:178) | src/lease/writer_effect_groups.rs:227 (`admit_root`) |
| 870-transition | Lease transition closes admission, waits for every root and queued/running job, then rereads state | group coordinator | `should_keep_a_transition_waiting_until_a_delayed_worker_discards_its_ticket` (src/lease/fs_leader_store/group_tests.rs:309); `should_wait_for_an_admitted_writer_effect_before_release` (src/lease/fs_leader_store.rs:1734); `should_preserve_other_transition_intents_when_one_waiter_cancels` (src/lease/writer_effect_groups.rs:617) | src/lease/writer_effect_groups.rs:284 (`transition`) |
| 870-bound | Admission into a continuous group is bounded so a renewed group cannot starve a foreign transition (`!pending` at :237/:246, `creator_live`) | group coordinator | **GAP**: no test asserts that new same-epoch admission is refused/blocked while `transition_waiters != 0` and readmitted afterwards. Searched `src/lease` for `starv|admission|readmit|pending` test names and read group_tests.rs:309, :58, :293 (they prove the transition waits, not that new admission is closed). | src/lease/writer_effect_groups.rs:237, 246 |
| 870-proof | Authority proof invalidated before final native unlock; no idle proof across unlock or takeover | native guard | `should_reject_cached_authority_when_a_process_crash_revokes_the_native_guard` (src/lease/fs_leader_store/group_tests.rs:232); `should_allow_takeover_when_the_last_logical_root_has_finished` (:293) | src/lease/fs_leader_store.rs:698 |
| 870-stages | Explicit bounded ownership across strict append, fsync and ledger publication; running worker keeps ownership after caller timeout | actor + writer | `should_reuse_one_native_fence_when_strict_group_appends_syncs_and_records_history` (src/runtime/actors/wal/tests/writer_groups.rs:104); `should_keep_each_physical_budget_fresh_when_a_group_spans_several_stages` (src/lease/fs_leader_store/group_tests.rs:155) | src/runtime/actors/wal/transaction.rs:288-336 |
| 870-order | Append/rollback ordering and ledger edit serialization preserved | writer runner | `should_keep_successful_prefix_when_later_queued_authority_rejects_its_partition` (src/wal/fs/writer_io/tests/writer_groups.rs:455); `should_prove_unchanged_append_only_after_durable_rollback` (src/wal/fs/writer_runner.rs:806) | src/wal/fs/writer_runner.rs:344, 408 |
| 872-creator | Creator exit closes independent admission; admitted ops finish counted children | coordinator | `should_finish_descendant_jobs_when_the_creating_root_has_left` (src/lease/fs_leader_store/group_tests.rs:21); `should_continue_explicit_writer_group_when_an_independent_creator_has_left` (src/wal/fs/writer_io/tests/writer_groups.rs:361) | src/lease/writer_effect_groups.rs:237 |
| 872-tickets | Authority captured and worker tickets reserved before enqueue; every coalesced member retained through rollback; tickets drained on worker failure/panic | `PendingWriterEffect`, runner | `should_release_queued_request_tickets_when_native_worker_fail` (src/wal/fs/writer_io/tests/writer_groups.rs:524); `should_release_queued_native_tickets_when_worker_panic` (:535); `should_report_unchanged_batch_when_a_child_validation_fails_after_its_noop_callback` (:619) | src/wal/writer_authority.rs:62 (`admit`); src/wal/fs/writer_runner.rs:274 |
| 872-deadline | Cancellation, per-stage deadlines, monotonic validity and native guard validity checked before physical effects | writer io + coordinator | `should_charge_native_admission_against_append_acknowledgement_budget` (src/wal/fs/writer_io/tests/writer_groups.rs:123); `..._sync_acknowledgement_budget` (:134); `should_reject_a_physical_stage_when_the_original_caller_deadline_expires` (src/lease/fs_leader_store/group_tests.rs:500); `should_skip_physical_effect_when_cancellation_arrives_during_native_guard_validation` (:553) | src/wal/fs/writer_io.rs:362, 443 (`check_append_budget`) |
| 872-retry | Retryable pre-effect timeout vs stale-actor fencing | `WalActor` | `should_remain_retryable_when_native_strict_group_admission_times_out` (src/runtime/actors/wal/tests/writer_groups.rs:213); `should_fence_strict_actor_when_successor_precedes_native_group_admission` (:172); `should_keep_native_sync_retryable_when_budget_expires_before_queue_submission` (src/wal/fs/writer_io/tests/writer_groups.rs:297) | src/runtime/actors/wal/transaction.rs:294-312 |
| 872-panic | Panics in admission, callback, release do not leak the fence | coordinator | `should_release_the_native_fence_when_a_writer_callback_panics` (src/lease/fs_leader_store/group_tests.rs:135); `should_recover_admission_when_the_backend_panics_after_native_lock_acquisition` (:436); `should_recover_release_when_the_backend_panics_after_native_unlock` (:523) | src/lease/writer_effect_groups.rs |
| 872-orphan | Orphan queued/active jobs and exact-identity rejection | coordinator | `should_release_a_queued_job_fence_when_the_job_is_discarded` (src/lease/fs_leader_store/group_tests.rs:89); `should_reject_a_job_before_effect_when_its_authority_does_not_match` (:110) | src/lease/writer_effect_groups.rs |
| 872-scope | Scoped cancellation keeps the incoming scope | scoped store | `should_retain_the_incoming_scope_when_a_queued_job_is_cancelled` (src/lease/fs_leader_store/group_tests.rs:204) | src/lease/scoped_leader_store.rs |
| 872-crash | Crash-releasing native locks keep the fencing protocol | `FsLeaderStore` | `should_preserve_native_fencing_protocol_when_crash_releases_effect_lock` (src/lease/fs_leader_store.rs:2464) | src/lease/fs_leader_store.rs |

##### 2e. Invariant catalog entries concerning WAL, LEDGER, recovery, retirement, cloud WAL, fencing

Skipped as clearly SST/compaction/memtable/backup/accounting-only: TX-1,2,3,5-8, SST-1..10, CLOUD-4, LIFE-1/2, BACKUP-1..5, RES-1,6,7, ACCT-1..6, FORMAT-2..5, PROG-2.

| ID | Statement | Owner | Pinning test | Owner file:line |
|---|---|---|---|---|
| AUTH-1 | Mutations require current writer authority; expired validity or superseded epoch fenced before the next mutation | `RuntimeFence` | `should_reject_expired_validity_when_cached_health_is_true` (src/runtime/event_loop/fencing.rs:244); `should_reject_metadata_pointer_cas_that_lands_after_successor_acquires` (src/lease/cloud/tests.rs:2605) | src/runtime/event_loop/fencing.rs:14, 42 |
| AUTH-2 | Loss of authority stops mutations, leaves reads and diagnostics available; unknown authority distinct from lost | `RuntimeFence` | `should_poison_lease_health_when_ownership_is_lost` (src/runtime/event_loop/fencing.rs:180); `should_not_poison_lease_health_when_leader_store_read_fails_transiently` (:153); `should_report_timeout_without_poisoning_when_lease_validation_times_out` (:167). **GAP (partial)**: the evidence file records that no public matrix proves every read/diagnostic stays available after every fencing cause; searched tests for a read-after-fence matrix, none. | src/runtime/event_loop/fencing.rs:42 |
| AUTH-3 | Event loop owns mutable runtime state; actors/workers return results | event loop | `should_route_manifest_file_list_mutation_through_runtime_owner` named by the evidence does not exist (rg, src and tests). **GAP**: the nearest executable pin is governance-style only; no WAL-specific test (the `WalActor` mutates `RuntimeState` on the loop thread at transaction.rs:703-708). | src/runtime/actors/wal/transaction.rs:703; src/runtime/event_loop/flush_pipeline/publication.rs:263 |
| AUTH-4 | Cloud DDL/control metadata fenced by the lease epoch; ambiguous CAS keeps writes fenced | DDL registry | `should_reject_paused_create_cas_after_successor_fences_ddl_registry` (src/runtime/ddl.rs:1418). `should_recover_ambiguous_ddl_once_when_reopening_after_crash_before_remote_cas_submission` cited by the evidence was not found (rg); out of WAL scope, recorded for catalog hygiene. | src/runtime/ddl.rs |
| TX-4 | A committed transaction is visible all at once; recovery applies the complete transaction or none | WAL batch + replay | `should_apply_txn_batch_atomically_during_recovery` (src/wal/recovery/tests.rs:900); `should_recover_only_committed_transaction_given_split_wal_records_when_commit_marker_is_missing` (src/wal/recovery/tests.rs:1023). The evidence-cited `should_maintain_atomicity_given_concurrent_reads_when_transaction_commits` no longer exists. | src/runtime/actors/wal/transaction.rs:505, 541; src/wal/recovery/streaming.rs:278 |
| WAL-1 | Write option defines the acknowledgment boundary (sync/buffered/best_effort/cloud_async/cloud_strict) | write options + `WalActor` | `should_issue_one_physical_wal_sync_when_non_empty_sync_transaction_commits` (tests/durability_sync_count.rs:76); `should_map_every_local_write_option_to_expected_wal_policy_given_local_storage_when_committing` (src/engine/api/write_options.rs:181); `should_not_advance_cloud_durability_across_unacked_segment_gap` (src/runtime/event_loop/cloud_integration/tests.rs:4908) | src/engine/api/write_options.rs:149; src/runtime/actors/wal/transaction.rs:166 |
| WAL-2 | Admit whole transaction and replay envelope before first WAL effect; ambiguous failure is `OutcomeUnknown` | `WalActor` | `should_retain_wal_admission_when_failed_append_has_no_rollback_proof` (src/runtime/actors/wal/tests/admission.rs:434); `should_keep_wal_open_when_spilled_transaction_exceeds_remaining_wal_budget` (:525); `should_not_replay_commit_reported_as_retryable_when_wal_sync_fails` (tests/fault_injection.rs:2902) | src/runtime/actors/wal.rs:712, 856 |
| WAL-3 | Recovery applies only complete transactions under one bounded rule; abandoned split does not block; torn tail cut to verified prefix and synced before append resumes | recovery + planner | `should_checkpoint_after_abandoned_split_transaction_when_newer_epoch_records_follow` (src/wal/recovery/streaming/tests.rs:748); `should_cut_a_torn_open_member_without_keeping_a_copy` (src/wal/planner/tests.rs:533); `should_open_strictly_when_power_loss_tears_unsynced_multi_page_tail` (tests/storage_root_injection.rs:811); `should_plan_every_ledger_member_after_its_anchor` (src/wal/planner/tests.rs:170). Evidence-cited `should_keep_valid_prefix_given_truncated_wal_tail_when_reopening_in_strict_mode` no longer exists. | src/wal/recovery/streaming.rs:257, 278; src/runtime/cloud_startup/streaming_wal_plan.rs:487 |
| WAL-4 | Strict fails closed; salvage keeps longest verified authorized prefix, persists the sequence floor first | planner + `WalHistory` | `should_fail_strict_but_salvage_valid_prefix_given_corrupted_first_wal_frame_when_reopening` (tests/durability.rs:872); `should_replay_only_the_chain_prefix_before_the_first_hole` (src/wal/planner/tests.rs:570); `should_open_strictly_without_set_aside_segments_when_salvage_stops_after_its_catalog_commit` (src/runtime/cloud_startup/streaming_wal_plan/tests/crashes.rs:300) | src/wal/planner.rs:355; src/runtime/wal_history.rs:748; src/runtime/cloud_startup/mod.rs:92 |
| WAL-5 | Active writer epoch fences every ledger edit and WAL effect; stale caches/workers cannot change history after takeover; unsupported versions rejected before mutation | `WalHistory`, fs writer | `should_refuse_append_when_ledger_changes_without_changing_length` (src/wal/ledger_store/tests.rs:300); `should_reject_strict_recovery_when_active_wal_has_lower_writer_epoch` (src/runtime/cloud_startup/streaming_wal_plan/tests.rs:1040); `should_stop_at_the_first_segment_whose_header_breaks_the_chain` (src/wal/planner/tests.rs:403). Format-rejection half: see 808-FMT / 808-FMT-SEG (**GAP** for segment/record version) | src/runtime/wal_history.rs:60, 381; src/wal/fs/writer_runner.rs:344 |
| WAL-6 | Local and cloud durability frontiers only advance; reset only through explicit recovery path | `Frontiers` | `should_not_lower_frontiers_when_an_older_sequence_arrives` (src/runtime/frontiers.rs:108); `should_lower_cloud_frontier_only_through_recovery_reset` (:141) | src/runtime/frontiers.rs:47, 53, 59, 83 |
| WAL-7 | WAL retirement needs each sealed segment family maximum covered by its durable flush frontier; unknown family authority retains WAL; replay selects whole transactions by commit sequence | retention + replay | `should_retain_local_wal_when_sst_metadata_has_no_flush_frontier` (src/runtime/event_loop/flush_pipeline/tests.rs:1272); `should_retire_local_wal_from_durable_frontiers_without_opening_sst_payloads` (:1304); `should_retain_a_newer_flushed_family_until_the_oldest_family_is_dropped` (src/runtime/hybrid_persistence/tests.rs:2742); `should_replay_complete_batch_when_frontier_precedes_commit` (src/wal/recovery/streaming/tests.rs:854) | src/wal/replay.rs:25; src/runtime/event_loop/wal_retention.rs:8; src/runtime/hybrid_persistence.rs:402 |
| CLOUD-1 | Cloud WAL segments are immutable epoch-scoped objects; object presence grants no authority; late fenced uploads are orphans | catalog | `should_reject_epoch_scoped_simulated_cloud_wal_without_catalog` (src/engine/tests.rs:949); `should_validate_catalog_authority_before_exposing_replay_sources` (src/runtime/cloud_startup/streaming_wal_plan/tests.rs:601); `should_preserve_acknowledged_history_when_predecessor_wal_resumes_after_takeover_and_cleanup` (tests/cloud_provider_engine_qualification/authority_publication.rs:56) | src/wal/cloud_catalog.rs:650; src/runtime/cloud_startup/cloud_recovery/mod.rs:348 |
| CLOUD-2 | `CloudAck` only after immutable upload and exact readback; lease validated and catalog published before frontier advances | uploads + event loop publication | `should_readback_remote_wal_before_upload_worker_emits_ack` (src/runtime/hybrid_persistence/tests.rs:2122); `should_reject_cloud_ack_given_writer_fenced_after_upload_was_enqueued` (src/runtime/event_loop/cloud_integration/tests.rs:4867) | src/storage/hybrid/backend/uploads.rs:429; src/runtime/hybrid_persistence.rs:308, 370; src/runtime/event_loop/cloud_integration/publication.rs:217 |
| CLOUD-3 | Committed cloud metadata generation is authority; catalog/metadata format versions checked before mutation | metadata + catalog | `should_ignore_stale_manifest_json_during_strict_cloud_recovery` (src/engine/tests.rs:1287); `should_report_another_format_version_as_incompatible` (src/wal/cloud_catalog.rs:982) | src/runtime/cloud_startup/cloud_recovery/metadata.rs:30; src/wal/cloud_catalog.rs:325 |
| CLOUD-5 | Cloud recovery combines intact local WAL with validated catalog chain; `cloud_strict` survives cache loss; `cloud_async` unpublished WAL only from same cache | cloud startup | `should_recover_cloud_backlog_after_complete_local_disk_loss` (tests/cloud_provider_engine_qualification/operational.rs:34); `should_recover_local_cloud_async_wal_when_child_aborts_before_upload` (tests/fault_injection.rs:7077); `should_replay_the_tail_after_the_catalog_head_from_the_cache` (src/wal/planner/tests.rs:262) | src/runtime/cloud_startup/streaming_wal_plan.rs:70; src/wal/planner.rs:54, 492 |
| CLOUD-6 | Cloud WAL retirement only from a committed shared generation covering every family maximum; catalog records it before deletion; deletion conditional on exact identity | cloud retirement | `should_reject_stale_remote_wal_target_identity_when_guarded_delete_runs` (src/runtime/hybrid_persistence/tests.rs:1884); `should_retry_a_retired_object_with_its_original_identity_after_transport_failure` (src/storage/hybrid/backend/tests.rs:1191); `should_queue_a_retired_object_until_the_conditional_delete_worker_is_available` (:1225) | src/runtime/hybrid_persistence.rs:402; src/runtime/hybrid_persistence/metadata_snapshot.rs:77; src/storage/hybrid/backend/proofs.rs:598 |
| CLOUD-7 | Provider failures, deadlines, conditional-write conflicts remain typed | cloud dispatcher | `should_preserve_precondition_failed_kind_when_cloud_error_crosses_storage_backend` (src/storage/cloud/tests.rs:1826); `should_cancel_native_metadata_get_when_operation_deadline_expires` (src/storage/cloud/tests/native_metadata_deadline.rs:188) | src/storage/cloud/dispatcher.rs |
| CLOUD-8 | Cloud-durable frontier covers a sequence only after its covering upload was acknowledged and published; only advances | engine + frontiers | `should_report_not_covered_given_late_wal_uploads_when_wait_times_out` (tests/cloud_core/cloud_durable_wait.rs:100); `should_cover_commit_sequence_given_cloud_async_write_when_its_upload_lands` (:70). **Doc GAP**: `invariant-evidence.json` has no `CLOUD-8` entry (only `invariant-evidence.md:593`). | src/engine/mod.rs:505, 534; src/runtime/frontiers.rs:59 |
| RES-2 | Resource admission happens before the WAL/publication side effect when rejection must leave the request unapplied | `WalActor` | `should_reject_transaction_when_no_space_hits_before_batch_append_and_remain_usable` (tests/fault_injection.rs:266); `should_preserve_transaction_when_oversized_range_delete_is_rejected` (tests/storage_layer.rs:320) | src/runtime/actors/wal/transaction.rs:86; src/runtime/actors/wal.rs:712 |
| RES-3 | Saturation is typed backpressure or write stall; never drops an acknowledged write | writer queue + L0 admission | `should_preserve_acknowledged_state_when_no_space_tears_wal_frame` (tests/fault_injection.rs:320); `should_stall_before_wal_when_transaction_would_exceed_hard_l0_ceiling` (src/runtime/event_loop/write_batch/tests.rs:2272) | src/wal/fs/writer_io.rs:333-441 (`WriteStall` at 434); src/runtime/event_loop/write_batch.rs:120 |
| RES-5 | Foreground operations use the caller's remaining deadline; a timeout is not cancellation | router + writer | `should_preserve_manual_caller_budget_when_queued_compaction_outlives_response_wait` (src/runtime/tests/compaction_deadline_tests.rs:37). WAL-append/sync side: `should_charge_native_admission_against_append_acknowledgement_budget` (src/wal/fs/writer_io/tests/writer_groups.rs:123) | src/wal/fs/writer_io.rs:388-404 |
| RES-8 | Shutdown/drop does not release writer authority while a durability worker could still mutate storage | engine shutdown + writer drop | `should_retain_writer_lease_until_blocked_flush_worker_exits` (tests/fault_injection.rs:5335); `should_retain_primary_lease_when_engine_drops_with_blocked_wal_upload` (tests/fault_injection.rs:5624) | src/runtime/event_loop/shutdown.rs:18; src/wal/fs/writer_io.rs:697 |
| PROG-1 | Recovery progress advances only on completed bounded work (verified WAL bytes/frames, non-empty remote ranges) | recovery progress | **GAP**: the four tests the evidence cites (`should_remain_healthy_when_actual_cloud_recovery_completes_read_only_work` etc., `tests/stress_workload_watchdog.rs`) do not exist; `tests/stress_workload_watchdog.rs` is absent and `rg "WorkProgress|recovery_progress" tests src --glob '*test*'` found no replacement. | src/telemetry/recovery_progress.rs; src/wal/recovery/streaming.rs:37, 642 |
| FORMAT-1 | Persistent formats carry explicit versions and integrity checks; future formats distinguishable from corruption | format modules | `should_reject_open_given_future_format_version_when_starting` (src/metadata/format.rs:271); `should_reject_future_v9_fixture_given_unsupported_version_when_reopening` (tests/storage_layer.rs:1805). WAL-specific halves: see 808-FMT-SEG (**GAP**). | src/metadata/format.rs:78; src/wal/frame.rs:528 |

##### 2f. GAP summary (rows containing GAP)

1. 808-FMT-SEG, no test for wrong segment-header version or record version.
2. 808-821e, no end-to-end local/cloud recovery parity test.
3. 808-822d, no WAL-specific event-loop-ownership test; cited governance test missing.
4. 870-bound, no test that a pending transition closes new admission.
5. AUTH-2 (partial), no read-availability-after-fence matrix.
6. AUTH-3, same cited test missing (shares fix with 808-822d).
7. CLOUD-8, missing from `invariant-evidence.json` (test exists).
8. PROG-1, cited tests do not exist.
9. L6 (SOLID), no trait-level `WalAppendError` contract suite.
10. L5 (SOLID), test doubles skip authority and accounted-append overrides.

Rows 1, 2, 4, 9 need a failing test written first in their slice; 3/6 and 8 and 5 are catalog/evidence hygiene plus at most one new test each; 7 is doc-only. Doc drift (not counted as separate GAPs): evidence JSON line numbers and four test names (WAL-3, TX-4, AUTH-3, AUTH-4) are stale.

---

#### 3. Format inventory

`docs/development/format-compatibility.md` (read in full): FORMAT 8 current, no migration, older and future markers rejected with `CompatibilityError` (lines 85-93); catalog v3 (135-169); fixtures at `tests/fixtures/compatibility/v8_populated_v4_sst_db` (225-235).

| Persisted WAL-related format | Constant / definition | Value | file:line |
|---|---|---|---|
| Database `FORMAT` marker | `CURRENT_FORMAT_VERSION` (`MIN_SUPPORTED_FORMAT_VERSION` equals it); marker file `FORMAT`, prefix `midge-format-version=` | 8 | src/metadata/format.rs:13, 16, 17, 19 |
| Segment header | `HEADER_LEN`, `HEADER_MAGIC`, `SEGMENT_VERSION`; reserved bytes 84..124; CRC32C in last 4 bytes; decode check of version | 128 bytes, `MWSEGHDR`, version 2 | src/wal/format/segment.rs:35, 36, 37, 41, 120-124 |
| Segment footer frame | `FOOTER_MAGIC`, `FOOTER_VERSION`; xxh3-128 digest, record count, per-family max seq | `MF`, 1 | src/wal/format/segment.rs:42, 43 |
| Frame | `[u32 len][u32 crc32c(payload)][payload]`; `WAL_FRAME_HEADER_LEN`, `WAL_MAX_RECORD_LEN`; CRC computed at `frame.rs:43`, checked at `:131` | 8 bytes; 64 MiB max | src/wal/frame.rs:4, 5, 43, 131 |
| Record payload | `MAGIC` `MW`, `VERSION`, TLV tags (OP 1, CF_ID 2, SEQ 3, KEY 4, VALUE 5, EXPIRATION 6, RANGE_END 7, TXN_ID 8, COMPRESSION 9); SEQ written by `put_u64(buf, tags::SEQ, record.seq)` | version 2 | src/wal/encoding.rs:35, 36, 43-54, 295 |
| Transaction batch payload | `TXN_BATCH_MAGIC` `TB`, `TXN_BATCH_VERSION`, fixed prefix `TXN_BATCH_MIN_PREFIX_LEN` (txn_id, begin_seq, commit_seq at fixed offsets) | version 1 | src/wal/encoding.rs:37, 38, 110 |
| Ledger journal | `MAGIC` `MDGLEDGR`, `VERSION`, `HEADER_LEN` 12, frames `[len][crc32c][JSON Frame]`, `Frame::{Snapshot,Edit}`, `LEDGER_FILE`, `LEDGER_TEMP_FILE`, `COMPACT_AFTER_EDITS` | version 2, JSON payload | src/wal/ledger_store.rs:35-44, 57-61 |
| Ledger edits | `LedgerEdit::{Rotate, Reopen, AdvanceLogStart, SetAside, Rebase, Reserve, SyncOpen, SalvageOpen}` (serde-serialized, so variant names and field names are persisted) | n/a (shape is the format) | src/wal/ledger.rs:101-141 |
| Cloud WAL catalog | `FORMAT_VERSION`; primary `wal/publication-catalog.v1.json`, mirror `wal/publication-catalog.v1.mirror.json` (`v1` is the object name, not the version) | 3 | src/wal/cloud_catalog.rs:24; src/cloud_layout.rs:18-21 |
| Cloud WAL object key | epoch-scoped, `WAL_PREFIX` + `epochs/{epoch}/{segment file name}`; parser `parse_segment_object_key` | n/a | src/wal/mod.rs:100-107 |
| Manifest family frontiers (WAL coverage) | per-family `flushed_through` rows (FORMAT 8) | via FORMAT 8 | docs format-compatibility.md:68-75 (code: src/metadata/manifest.rs:102 `prepare_edit`) |

##### Do the four hypotheses change a persisted byte layout?

- **#904 (stop committing a LEDGER edit per fsync): yes, a persisted-format change, but only to the ledger journal contents, and only if `SyncOpen` is removed or its semantics change.** `LedgerEdit::SyncOpen` (`ledger.rs:133`) and `SalvageOpen` (`:137`) are serialized variants of the JSON `Frame::Edit` (`ledger_store.rs:57-61`); `durable_bytes` is derived state of `WalLedger::apply` (`ledger.rs:278`). Removing the variant, changing what `apply` does with an existing variant, or deriving the durable prefix from the segment instead changes how existing journals load, so `ledger_store.rs:39` `VERSION` (2 -> 3) and `metadata/format.rs:13` `CURRENT_FORMAT_VERSION` (8 -> 9) must move together, with `tests/fixtures/compatibility/v8_populated_v4_sst_db` and the future-format fixture updated (format-compatibility.md:232-235). The segment bytes, frame, record and catalog layouts do not change. Keeping `SyncOpen` only for salvage cuts (#904 R2) still changes the semantics of the existing variant (a missing per-sync edit is no longer "boundary not durable"), so a version bump is required either way. Pre-1.0 break is allowed with no migration (memory: breaking changes OK; format-compatibility.md:85-93).
- **#941 (sequence placeholder patching in encoded frames): no change to persisted bytes, provided final bytes equal the current encoder's.** The sequence is TLV tag 3 (`encoding.rs:295`) inside the payload and is covered by the frame CRC32C (`frame.rs:43`), so a patch must write the u64 and recompute the CRC after patching (the issue's R2). Transaction batches carry `begin_seq`/`commit_seq` at fixed offsets (`encoding.rs:110`) and per-op `seq`; those are also patch targets. Layout, tags, lengths, versions stay identical; the compression decision (`compress_wal_value`, `encoding.rs:310-318` per the issue) must not depend on sequence. Needs a test that old decoder reads new-path bytes (the issue's AC3) and that bytes are equal to the legacy encoder's for the same inputs (byte-for-byte golden), not only that they decode.
- **#947 (vectored writes): no change.** Segment layout is positional and contiguous: `write_at(start_pos, bytes)` at `writer_runner.rs:371` with `start_pos` from `current_pos` (`:350`). Vectored writes (`write_vectored`) over the queued buffers produce the same bytes at the same offsets if the buffers are written in queue order without gaps; the digest update at `writer_runner.rs:380-385` requires the same ordering. Not a format change; the risk is partial-write rollback (`rollback_partial_append`, `:408-441`) which must still truncate to `start_pos`.
- **#916 (cloud WAL catalog, format 3 -> 4): conditional.** Parallel uploads (R1), batched publication (R2), publication off the loop (R3) and receipts (R5) do not change the catalog document (`cloud_catalog.rs:24` stays 3; ordering is enforced by `publish.rs:283`). Catalog size bound by compacting retired entries (R4) is already structural: retirement removes the oldest entries and moves `anchor` (format-compatibility.md:156-157), so that part needs no schema change unless entries are summarized. Making the mirror object optional or asynchronous (R4) changes the object-set contract at `cloud_layout.rs:18-21` and the recovery rule "mirror used only when the primary is missing or malformed" (format-compatibility.md:138-141); that is a recovery-authority change that needs a catalog `FORMAT_VERSION` bump (3 -> 4, `cloud_catalog.rs:24`) only if the document schema (fields such as `retired_by`, `seq_floor`, `anchor`) changes. Bytes of an unchanged-schema document stay identical.

Format decision summary for R5: no format change is required by the pipeline design itself. #941 and #947 are byte-identical relocations. #904 requires a ledger `VERSION` and FORMAT bump (the only forced break). #916 requires a catalog bump only if its schema changes (mirror removal alone changes the object set, not the document).
