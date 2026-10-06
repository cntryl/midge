# System Invariant Catalog and Gap Analysis

This catalog consolidates the system-level properties Midge must preserve
across writes, reads, background work, recovery, and provider operations. It
extends the narrower [storage invariants](storage-invariants.md) page. It does
not attempt to list every local helper precondition or Rust type invariant.

The initial inventory was recorded during the #707/#708 work. This update
refreshes its row-level references against the delivered startup/flush and
compaction deadline fixes, #723 native scratch-capacity repair, #724 version
documentation, and #715 accounting/benchmark/reader source measured at
c8f0de80. References now bind recovery seek-index source commit
c1e5709e376d29090bb331d37f27fa941d763b96 before this documentation commit.
The changed replay-coverage locations and source hash are refreshed;
existing invariant claims and historical qualification objects are retained.
The measured c8 campaign and its original report stay unchanged; the new
coverage index controls do not claim fresh native/full-hour qualification.
The retained nine-cell outcome, original readback digest and #730/#731
helper/reader limits are mapped below. Full-hour acceptance is separate and
uses the final merged catalog source; its run identities and artifact
readbacks are tracked on [GitHub issue #711](https://github.com/cntryl/midge/issues/711).
No fresh Rust test execution is claimed by this documentation preparation.

Each ID links to [exact source and assertion evidence](invariant-evidence.md).
The accompanying [machine-readable inventory](invariant-evidence.json) names
production symbols, test symbols, assertion markers, fixture preconditions
and uncovered boundaries. The lightweight validator checks references and
source postimage drift; it does not prove an invariant or run Rust tests.

## Catalog

### Authority and serialized state

| ID | Invariant | Evidence |
|---|---|---|
| AUTH-1 | Mutations require current writer authority. A writer whose monotonic lease validity expires or whose epoch is superseded must be fenced before it submits another state-changing operation. A provider request already submitted may have an ambiguous outcome; Midge cannot claim it was cancelled. | [Exact evidence](invariant-evidence.md#auth-1) |
| AUTH-2 | Loss of writer authority stops new mutations while leaving reads and diagnostics available. Unknown authority caused by a timeout or transient provider failure remains distinguishable from proven lease loss. | [Exact evidence](invariant-evidence.md#auth-2) |
| AUTH-3 | The event loop owns authoritative mutable runtime state. Actors and storage workers return results; they do not independently publish manifest, memtable, sequence, or health state. An operation becomes authoritative only at its serialized publication boundary. | [Exact evidence](invariant-evidence.md#auth-3) |
| AUTH-4 | Cloud DDL and control metadata changes are fenced by the active lease epoch. An ambiguous compare-exchange keeps writes and maintenance fenced until recovery reconciles the prepared operation. Column-family IDs remain retired after drop and are not reused. | [Exact evidence](invariant-evidence.md#auth-4) |

Owners: `src/lease/`, `src/runtime/event_loop/fencing.rs`,
`src/runtime/event_loop/manifest.rs`, `src/runtime/state/`, and
`src/engine/startup/`. Evidence includes lease tests, DDL tests, the #662/#663
regressions, and the authority schedules recorded in
[`discovery-0.3.1.md`](discovery-0.3.1.md).

### Transactions, MVCC, and reads

| ID | Invariant | Evidence |
|---|---|---|
| TX-1 | A transaction belongs to one column family and captures one committed sequence horizon at begin. Later commits do not change its snapshot. A registered snapshot pin protects every version and SST it may still read within that runtime. Cloud transactions require current lease authority; they return Fenced after lease loss because pins do not retain remote SSTs against a successor. | [Exact evidence](invariant-evidence.md#tx-1) |
| TX-2 | A read selects the highest eligible sequence at or below its snapshot horizon. The transaction sees its own accepted intents in operation order; a later point write can supersede an earlier point or range delete. | [Exact evidence](invariant-evidence.md#tx-2) |
| TX-3 | Point tombstones, range tombstones, and expired values suppress older values consistently across memtables, SSTs, flush, compaction, restart, forward scans, and reverse scans. Range bounds are binary-safe, start-inclusive, and end-exclusive. | [Exact evidence](invariant-evidence.md#tx-3) |
| TX-4 | A committed transaction is visible all at once. Its operations receive one ordered sequence range and recovery either applies the complete transaction or none of it. No reader may observe a partially applied multi-key commit. | [Exact evidence](invariant-evidence.md#tx-4) |
| TX-5 | `LastWriteWins` and `AbortOnWriteConflict` have distinct documented semantics. Strict conflict checks happen before WAL publication. `assert_value` checks the frozen start snapshot and rejects any later point or covering range mutation at the runtime serialization point, regardless of conflict policy. | [Exact evidence](invariant-evidence.md#tx-5) |
| TX-6 | Rollback or dropping an uncommitted transaction discards its client-side intents; it does not publish them to the WAL or memtable. | [Exact evidence](invariant-evidence.md#tx-6) |
| TX-7 | Query scans honor effective prefix/range bounds, direction, and result limit. An iterator surfaces read errors as errors and enters a failed state; corruption is not converted to normal exhaustion. | [Exact evidence](invariant-evidence.md#tx-7) |
| TX-8 | Midge does not promise serializable isolation, predicate locking, or atomic transactions spanning column families. Lost updates remain possible under the default last-writer-wins policy unless callers use strict conflict handling or assertions. | [Exact evidence](invariant-evidence.md#tx-8) |

Owners: `src/engine/api/transaction.rs`, `src/runtime/read_snapshot.rs`,
`src/runtime/snapshot_pins.rs`, `src/engine/api/query.rs`, and
`src/engine/api/iterator.rs`. Evidence includes `tests/transactions.rs`,
`tests/engine_api.rs`, `tests/column_families.rs`, and the transaction oracle in
`tests/discovery_model.rs`. The public contract is in
[`transaction-durability-contract.md`](../user-guides/transaction-durability-contract.md)
and [`transactions-and-mvcc.md`](../transactions-and-mvcc.md).

### Commit durability and WAL recovery

| ID | Invariant | Evidence |
|---|---|---|
| WAL-1 | The selected write option defines the acknowledgment boundary. On local storage, `sync()` waits for local fsync; in-memory storage has no crash-persistent medium. `buffered()` waits for append and visibility but may be lost before fsync; `best_effort()` skips WAL and requires later SST publication to survive restart; `cloud_async()` does not promise cloud durability at return; `cloud_strict()` returns only after cloud authority covers the sequence. | [Exact evidence](invariant-evidence.md#wal-1) |
| WAL-2 | A commit timeout is not proof that a mutation did not happen. Once accepted, runtime work may finish after the caller stops waiting; callers must use idempotency or readback before retrying an ambiguous write. | [Exact evidence](invariant-evidence.md#wal-2) |
| WAL-3 | Recovery processes sealed WAL segments in sequence order, then the active WAL. It never applies an incomplete frame or partial transaction. A valid truncated active tail may be retained as a prefix, but the file is truncated and synced to that prefix before append resumes. | [Exact evidence](invariant-evidence.md#wal-3) |
| WAL-4 | Strict recovery fails closed on corrupt or ambiguous durable state. Salvage may retain only a validated prefix and must report degraded recovery; it must not silently skip a replay hole and apply later state as if history were contiguous. | [Exact evidence](invariant-evidence.md#wal-4) |
| WAL-5 | Writer epochs and recovered sequence floors prevent a stale writer or set-aside WAL record from becoming current history or having its sequence reused. Epoch-regressed sources participate in the salvage-hole and floor plan before quarantine. | [Exact evidence](invariant-evidence.md#wal-5) |
| WAL-6 | Local and cloud durability frontiers only advance. Recovery may reset a frontier only through its explicit recovery path, then re-prove the prefix from manifest or validated WAL authority. | [Exact evidence](invariant-evidence.md#wal-6) |
| WAL-7 | Pruning a WAL record requires proof that authoritative SST state covers its exact logical effect, including tombstones and TTL representation. If coverage or identity is uncertain, retain the WAL. | [Exact evidence](invariant-evidence.md#wal-7) |

Owners: `src/wal/recovery.rs`, `src/wal/encoding.rs`,
`src/runtime/actors/wal/`, `src/runtime/frontiers.rs`,
`src/runtime/event_loop/wal_retention/`, and
`src/runtime/cloud_startup/streaming_wal_plan/`. Evidence includes
`tests/durability.rs`, `tests/fault_injection.rs`, focused recovery-plan tests,
and sanitized recovery fuzz targets. The crash and prefix contract is described
in [`recovery-internals.md`](recovery-internals.md).

### Manifest, SST, flush, and compaction

| ID | Invariant | Evidence |
|---|---|---|
| SST-1 | The manifest's published file set is authoritative. Raw SST presence, a local cache file, an upload, or a temporary output does not make a file visible to reads or recovery. | [Exact evidence](invariant-evidence.md#sst-1) |
| SST-2 | A published SST is immutable. Readers and recovery rely on its name, size, any recorded checksum, and validated metadata describing the same bytes. A changed or missing authoritative object is corruption or unavailable authority, not a reason to guess. | [Exact evidence](invariant-evidence.md#sst-2) |
| SST-3 | Key and sequence bounds used to skip files are trusted only when complete and validated. Unknown or overlapping L1+ bounds stay in a conservative read path until a complete repair publishes a disjoint replacement. Endpoints are treated conservatively where equality could hide tombstone coverage. | [Exact evidence](invariant-evidence.md#sst-3) |
| SST-4 | Flush either publishes the complete new SST state through the manifest or leaves prior state authoritative. Durable intent bridges output creation and manifest publication; an orphan is not exposed as committed data. | [Exact evidence](invariant-evidence.md#sst-4) |
| SST-5 | Compaction writes and proves the complete output set, persists intent, and switches the manifest atomically before deleting inputs. Recovery resolves a crash to the complete old set or complete new set, never a partial mixture. Cleanup is idempotent and retains inputs or intent when publication is ambiguous. | [Exact evidence](invariant-evidence.md#sst-5) |
| SST-6 | Compaction preserves the logical state visible to active snapshots. It retains versions and tombstones newer than the oldest snapshot and drops older tombstones only with the required bottommost/coverage proof. Snapshot pins prevent this runtime's GC from unlinking files still in use. Cloud snapshots are invalidated with Fenced after lease loss; pins do not coordinate reclamation with a successor. | [Exact evidence](invariant-evidence.md#sst-6) |
| SST-7 | Compaction output identity and sequence are assigned before worker execution. A placeholder or incomplete plan is not publishable. Resource exhaustion or cancellation before publication retains the authoritative inputs. | [Exact evidence](invariant-evidence.md#sst-7) |
| SST-8 | Manifest journal edits are replayed without gaps. Checkpoint horizons advance only over edits included in that snapshot; a crash between snapshot rename and journal truncation remains idempotent. | [Exact evidence](invariant-evidence.md#sst-8) |
| SST-9 | Newly created database and storage directories are durably linked before Midge admits writes, so a synced file is not lost behind a volatile parent-directory entry. | [Exact evidence](invariant-evidence.md#sst-9) |

Owners: `src/metadata/`, `src/runtime/intent_persistence.rs`,
`src/runtime/actors/flush/`, `src/runtime/actors/compaction/`,
`src/runtime/event_loop/`, `src/runtime/sst_read_view.rs`,
`src/compaction/`, and `src/io/durable_dir.rs`. Evidence includes
`tests/storage_invariants.rs`, `tests/fault_injection.rs`,
`tests/durability.rs`, manifest/journal unit tests, and the compaction
publication crash matrix in
[`cardinality-independent-architecture.md`](cardinality-independent-architecture.md).

### Cloud storage and remote authority

| ID | Invariant | Evidence |
|---|---|---|
| CLOUD-1 | Cloud WAL segments are immutable, epoch-scoped objects. Object existence alone grants no replay or durability authority; startup replays only entries in a validated publication catalog. Late uploads from a fenced writer remain ignored orphans. | [Exact evidence](invariant-evidence.md#cloud-1) |
| CLOUD-2 | `CloudAck` is emitted only after immutable upload and exact byte readback. The runtime validates the active lease and publishes the WAL catalog before advancing the cloud durability frontier or completing strict durability waiters. | [Exact evidence](invariant-evidence.md#cloud-2) |
| CLOUD-3 | The committed cloud metadata generation is the authority for mirrored local control files. Mutable metadata copies and unreferenced uploads do not override the committed descriptor. Primary and recovery mirror repair must preserve conditional publication ordering. | [Exact evidence](invariant-evidence.md#cloud-3) |
| CLOUD-4 | Cloud startup inventories each manifest-authoritative SST by its exact object key and recorded size. Required recovery proofs and subsequent SST readers validate content, identity and format when those paths consume it; metadata-only inventory does not eagerly verify every body or block. Definitive missing/size-mismatched objects fail closed or enter the explicitly supported salvage path; unrelated objects are not substituted. | [Exact evidence](invariant-evidence.md#cloud-4) |
| CLOUD-5 | Cloud recovery combines intact local WAL with validated remote catalog history and preserves sequence/epoch holes. A `cloud_strict()` acknowledgment remains recoverable after local-cache loss from uploaded WAL or later published SST state. | [Exact evidence](invariant-evidence.md#cloud-5) |
| CLOUD-6 | Cloud WAL deletion is conditional on exact coverage and identity proofs, current lease/metadata authority, and a final proof check immediately before provider deletion. Unknown, changed, or ambiguously covered bytes are retained. | [Exact evidence](invariant-evidence.md#cloud-6) |
| CLOUD-7 | Provider failures, deadlines, and conditional-write conflicts remain typed outcomes. Provider adapters must preserve range, pagination, precondition, and timeout semantics rather than silently falling back to weaker operations. | [Exact evidence](invariant-evidence.md#cloud-7) |

Owners: `src/wal/cloud_catalog.rs`, `src/wal/cloud_segment.rs`,
`src/lease/cloud.rs`, `src/storage/hybrid/`,
`src/runtime/hybrid_persistence/`, and `src/runtime/cloud_startup/`.
Evidence includes `tests/cloud_core.rs`, provider contract and engine
qualification suites, cloud recovery tests, and the native Sqrzl evidence in
[`roadmap-0.3.1.md`](roadmap-0.3.1.md). The boundary of emulator evidence is
defined in [`cloud-qualification-policy.md`](cloud-qualification-policy.md).

### Column-family lifecycle and backup/restore

| ID | Invariant | Evidence |
|---|---|---|
| LIFE-1 | Dropping a column family is serialized with writes and publication. The safe API refuses to discard active committed data; the destructive API makes that choice explicit. The drop frontier and retained SST names allow reclamation to resume after restart. | [Exact evidence](invariant-evidence.md#life-1) |
| LIFE-2 | Dropped-family SSTs remain authoritative for snapshots that predate the drop until those snapshots release their pins. Reclamation begins only after durable manifest publication removes the files. | [Exact evidence](invariant-evidence.md#life-2) |
| BACKUP-1 | Backup captures one consistent durable frontier: mutations are fenced while WAL is synced and durable files are pinned, then bulk copy proceeds from that pinned set. The inventory identifies every object by relative path, size, and checksum. | [Exact evidence](invariant-evidence.md#backup-1) |
| BACKUP-2 | Backup destination, source, restore artifact, and restore target must be path-disjoint after resolving aliases. Publication does not replace an existing destination or target. | [Exact evidence](invariant-evidence.md#backup-2) |
| BACKUP-3 | Restore validates the artifact inventory and all bytes, then strictly verifies the staged database before atomic publication. Opening the restored database acquires a new writer lease rather than copying the source lease. | [Exact evidence](invariant-evidence.md#backup-3) |
| BACKUP-4 | Each restore attempt owns a unique stage. A crash-left or foreign stage cannot authorize deletion of another attempt's data or permanently block retry. A failed restore never publishes a partial target. | [Exact evidence](invariant-evidence.md#backup-4) |
| BACKUP-5 | Backup/restore support is limited to local and matching CloudSimulated layouts. In-memory and provider-backed Cloud databases are outside this API contract. | [Exact evidence](invariant-evidence.md#backup-5) |

Owners: `src/engine/backup.rs`, `src/engine/backup/paths.rs`,
`src/engine/verification.rs`, `src/engine/mod.rs`, and
`src/runtime/hybrid_persistence/`. Evidence includes `tests/column_families.rs`,
`tests/backup_paths.rs`, `tests/backup_publish_races.rs`, and the restore abort
qualification in [`discovery-0.3.1.md`](discovery-0.3.1.md).

### Resource bounds, retries, and progress

| ID | Invariant | Evidence |
|---|---|---|
| RES-1 | Engine-managed memory, disk staging, storage queues, readers, merge heads, and transaction spill state are bounded by configured byte/count budgets or fixed capacities. A reservation remains charged for as long as its bytes or resource owner remain live and is released with that owner. | [Exact evidence](invariant-evidence.md#res-1) |
| RES-2 | Resource admission happens before the corresponding WAL or publication side effect when rejection must leave the request unapplied. Rejection preserves previously accepted transaction intents. | [Exact evidence](invariant-evidence.md#res-2) |
| RES-3 | Saturation is reported as typed backpressure or a write stall; it does not silently drop an acknowledged write, discard authoritative data, or turn an expected resource limit into a worker panic. | [Exact evidence](invariant-evidence.md#res-3) |
| RES-4 | Retryable work is scheduled with bounded deadlines/backoff and does not busy-spin. Permanent corruption, authority loss, and space exhaustion retain authoritative state and surface the blocking error. | [Exact evidence](invariant-evidence.md#res-4) |
| RES-5 | Foreground provider operations consume the caller's remaining deadline where documented. If the caller times out after runtime acceptance, background work can continue and the result is ambiguous. A timeout is not cancellation. | [Exact evidence](invariant-evidence.md#res-5) |
| RES-6 | The hard L0 ceiling accounts for published files and every active, queued, or in-flight generation that could publish another file. At the ceiling Midge stalls writes before WAL admission and schedules pressure recovery even when ordinary background compaction is disabled. | [Exact evidence](invariant-evidence.md#res-6) |
| RES-7 | Compaction work and scratch are bounded independently of total target-level cardinality. Under successful storage operations and admitted writes no faster than service, maintenance has a decreasing progress measure; persistent I/O failure or true resource exhaustion may stop progress explicitly. | [Exact evidence](invariant-evidence.md#res-7) |
| RES-8 | Shutdown or engine drop does not release writer authority while an accepted durability worker could still mutate storage. Teardown may continue in a reaper after the caller's shutdown wait expires. | [Exact evidence](invariant-evidence.md#res-8) |

Owners: `src/common/resource_budget.rs`, `src/runtime/transaction_spill/`,
`src/runtime/read_resources/`, `src/storage/hybrid/`,
`src/runtime/event_loop/scheduler.rs`, `src/runtime/retry_schedule.rs`,
`src/compaction/`, and `src/engine/mod.rs`. Evidence includes
`tests/cloud_admission.rs`, `tests/stress_workload_progress.rs`,
`tests/stress_workload_watchdog.rs`, transaction spill tests, and the deterministic
work-bound evidence in
[`cardinality-independent-architecture.md`](cardinality-independent-architecture.md).

### Completed-work recovery progress

| ID | Invariant | Evidence |
|---|---|---|
| PROG-1 | Startup recovery progress advances only when bounded work completes: verified bytes, frames, coverage decisions, or successful non-empty remote ranges. Request submission, a held callback, an error, or an empty response is not progress. Cached verification counts as progress even when it performs no new I/O. | [Exact evidence](invariant-evidence.md#prog-1) |
| PROG-2 | Progress is credited only to the active recovery caller and phase generation. A phase transition, completed scope, unrelated workload event, or background thread cannot keep an old recovery watchdog alive. | [Exact evidence](invariant-evidence.md#prog-2) |

Owners are `src/telemetry/recovery_progress.rs`, WAL frame/replay, metadata
journal/inventory and private cloud-recovery filesystem views. Actual
producer/held-request watchdog cases are mapped at PROG-1; constructed
consumer scope filtering is separately labeled at PROG-2. Completed bounded
work is distinct from startup completion. These delivered regressions do not
supply the separate qualification of all fourteen one-hour workloads
at the final accounting revision.

### Metadata accounting and measurement boundaries

| ID | Invariant | Evidence |
|---|---|---|
| ACCT-1 | Each runtime owns one bounded metadata/publication accounting identity. Retained metrics handles contain counters only, so they can survive owned shutdown and a distinct reopened engine without retaining the original runtime, storage or writer lease. | [Exact evidence](invariant-evidence.md#acct-1) |
| ACCT-2 | Metadata cost carries an explicit immutable origin and fixed persistent or memory-only medium. Retrying an accepted ordinary flush during shutdown preserves its original origin and logical clock; recovery, bootstrap, DDL, compaction-before-GC, administration and newly forced shutdown work remain separate. | [Exact evidence](invariant-evidence.md#acct-2) |
| ACCT-3 | Issued metadata payload, successful write-return bytes and durability-confirmed snapshot/journal bytes are distinct. Snapshot durability alone is not checkpoint completion; required journal truncation and barriers must succeed. Failed operations retain observed attempted/returned bytes without invented durable credit. | [Exact evidence](invariant-evidence.md#acct-3) |
| ACCT-4 | Each matching publication attempt is recorded once. Full logical flush-publication time spans the first accepted publish submission through successful matching immutable installation, including retry wait and metadata work. Failed/build-only/stale completions cannot fabricate or duplicate committed SST bytes. | [Exact evidence](invariant-evidence.md#acct-4) |
| ACCT-5 | Operations seal their bounded ledger once; a late payload/error/namespace/barrier completion marks accounting incomplete or escaped rather than silently disappearing. Counter overflow, owner mismatch, decreasing interval values or incomplete histograms invalidate a measured interval; histogram deltas subtract bucket counts, never percentiles. | [Exact evidence](invariant-evidence.md#acct-5) |
| ACCT-6 | Checkpoint qualification separates measured ordinary-local/persistent costs from warmup, forced origins and both settled owners. Invalid/missing attempts stay invalid, all nine fixed-cell attempts are required, and actual final-owner persistence integrity is checked without adding later costs to measured ratios. Source/selector/PID/build/archive provenance and native trust diagnostics are retained. Endpoint capture preserves the original cell deadline, one capped allowance and zero Persistent active gauge; rejected warmup samples are excluded but the final accepted query and all end sampling stay inside the measured clock. Sampling supplies no progress heartbeat. | [Exact evidence](invariant-evidence.md#acct-6) |

Owners are `src/metadata/accounting.rs`, `src/metadata/accounted_fs.rs`,
`src/metadata/store.rs`, `src/runtime/state/accounting.rs`, actual flush and
recovery publication paths, and the fixed checkpoint workload/reader. The
source inventory separates real public ACK, seeded encoded WAL, actual
delegated filesystem observations and constructed recording/readback tests.
Pre-state costs are explicitly uncovered and filesystem payload bytes are
not physical-device bytes. The source-bound nine-cell campaign is recorded
at ACCT-6: B/C meet only the conditional policy-investigation predicate.
Cadence remains unchanged. Full-hour acceptance uses the final merged catalog source and is tracked on GitHub issue #711.

### Persisted formats and operator-facing contracts

| ID | Invariant | Evidence |
|---|---|---|
| FORMAT-1 | Persistent formats use explicit versions and integrity checks. Supported historical formats, corruption, and unsupported future formats remain distinguishable; malformed compressed data is never retried as raw data. | [Exact evidence](invariant-evidence.md#format-1) |
| FORMAT-2 | New entries satisfy the encoded-size and range-bound admission rules before transaction mutation. Historical supported data remains readable only within the decoder's stated limits; compaction must preserve it or fail without deleting its source. | [Exact evidence](invariant-evidence.md#format-2) |
| FORMAT-3 | Read-only verification does not repair or mutate storage. The path-only CLI reports local-path scope and cannot establish provider-backed cloud authority. Its JSON schema and exit codes are separate versioned contracts. | [Exact evidence](invariant-evidence.md#format-3) |
| FORMAT-4 | Configuration rejects incompatible durability modes and invalid storage/provider options before writes are accepted. Public error classes distinguish invalid input, unavailable authority, timeout, resource pressure, corruption, and incompatibility. | [Exact evidence](invariant-evidence.md#format-4) |
| FORMAT-5 | A release's API and persisted-format claims are limited to the support matrix, fixtures, migration instructions, and qualification evidence for that release. Midge's 0.x line does not promise blanket compatibility. | [Exact evidence](invariant-evidence.md#format-5) |

Owners: `src/metadata/format.rs`, `src/wal/encoding.rs`, `src/sst/`,
`src/engine/verification.rs`, `src/bin/`, and `src/engine/api/options/`.
Evidence includes `tests/storage_layer.rs`, `tests/verification_cli.rs`,
`tests/fuzz_harness.rs`, and format fixtures. Contracts are in
[`format-compatibility.md`](format-compatibility.md),
[`verification.md`](../user-guides/verification.md), and
[`support-matrix.md`](support-matrix.md).

## Gap analysis

### Coverage and traceability

The existing [`storage-invariants.md`](storage-invariants.md) is accurate as a
nine-item storage summary, but it is not a system-wide catalog. Writer authority,
transaction isolation, cloud publication authority, backup/restore, resource
admission, timeouts, column-family lifecycle, and format/API contracts were
spread across other documents. There was no single index connecting those
contracts to their owners and representative evidence. This page closes that
navigation gap; it does not add new runtime guarantees.

Test coverage is broad and includes model histories, fault injection, actual
process aborts, provider adapters, and fuzzing. The test suites do not provide a
formal proof of every schedule. The final 0.3.1 evidence records 1,024 logical
histories / 117,380 scheduled actions, named process aborts, bounded race
schedules, provider campaigns, and focused properties. Its own interpretation
states that these are bounded schedules rather than exhaustive concurrency
proofs. Future invariant changes should continue to name the production entry
point, exact test, assumed preconditions, and untested boundary.

Every ID now names exact production/test references, assertion markers,
fixture preconditions and uncovered scope in the accompanying evidence map.
`ruby tools/validate-invariant-catalog.rb` checks unique IDs, complete mapping,
missing/dangling evidence references, source/test symbols, assertion markers
and captured source hashes. This catches reference drift; it does not infer
semantic sufficiency from a passing assertion or replace actual test runs.

#715 instrumentation and readback controls establish their tested contracts.
Historical 32911805 A/r1 smoke 37270512400 validates transport/construction
only, with zero stalls/waits and native too-few-samples diagnostics. Its
later nine-run 37270845568 remains invalid after two active metadata end
snapshots, despite exact data verification and later owner settlement.
At measured c8f0de80, smoke 37273556125 and all nine attempts of 37274074026
pass original-archive readback. B/C have three payload misses each; A has
one time miss and does not qualify. The conditional investigation predicate
is met while cadence stays unchanged. Exact strict ACK/reopen/shutdown
records do not upgrade native too-few-samples confidence or measure physical
device bytes. Final fourteen hours follow the final merged #711 source.

### Known behavioral and assurance limits

| Gap | Impact and current boundary | Suggested follow-up |
|---|---|---|
| Checkpoint write amplification | At measured c8f0de80, all nine fixed-cell attempts of 37274074026 pass original-archive readback. B/C each miss the predeclared snapshot-payload target in 3/3 repeats (13.023–13.534% / 80.261–80.262%); A has one time miss and does not qualify. The conditional policy-investigation predicate is met; cadence stays unchanged. [Recorded outcome](checkpoint-write-amplification.md#recorded-hosted-outcome) and [original readback](evidence/checkpoint-715-readback.json) retain exact identities and native diagnostics. | Any later cadence proposal still requires matched measurements and allocation, journal/frontier, forced checkpoint and crash controls. Fs payload is not physical/device amplification. Full-hour acceptance uses the final merged catalog source, with run identities and artifact readbacks tracked on GitHub issue #711. |
| Transaction spill lookup scaling | The accepted #681 design retains matching-run point probes and does not implement the original eight-source sorted spill view or upfront auxiliary scratch reservation. Correctness and configured-budget checks passed, but the original stronger read-work bound was not shipped. | Add deterministic lookup-work/read-source counters to any future scaling claim and measure resident plus heavily spilled transactions. |
| Cloud replay locality | The accepted #682 design bounds retained readers/blocks but clears them at checkpoints. Release cold-recovery evidence improved ranges and bytes substantially; it is not a same-host latency comparison or production SLO. | Keep the configured budget as the safety gate and rerun the native cold-recovery workload when changing cache retention. |
| Compaction caller deadline | Delivered #714 captures one immutable manual origin through queueing, multi-CF work, cooperative compute/output staging, native operations and publication phases. Later waiters cannot replace the owner. Background None keeps its ordinary callerless policy. A caller can still time out while accepted work retains storage/lease ownership. | Preserve typed Timeout, ambiguous CAS recovery and safe joins; do not interpret deadline expiry as provider cancellation or claim preemption of a blocked local syscall. |
| Startup aggregate deadline | Delivered #713 adds optional open_timeout (default None), one original deadline, scoped native/local recovery and prepared-runtime acceptance. Expired startup cannot admit a late runtime, and genuinely uncertain lease acquisition stays LeaseIndeterminate while cleanup retains ownership. | Preserve None/Complete compatibility and typed Timeout through strict/salvage; local calls are checked cooperatively rather than forcibly interrupted. |
| Directory-entry durability for deletion | Unix object deletion does not sync the parent directory. A crash can resurrect obsolete bytes. Manifest/catalog authority keeps them inactive, so the classified risk is a storage leak rather than data loss; bounded cleanup is not guaranteed by this behavior. | Revisit if a hard storage-reclamation bound becomes a supported contract. |
| Provider environment scope | Sqrzl qualifies modeled provider protocol behavior. It cannot establish a deployment's IAM, quotas, network policy, lifecycle rules, availability, or capacity. | Treat environment-specific qualification as a deployment acceptance item, not as repository CI evidence. |
| Documentation freshness | #724 aligns identified present-tense current-version claims to Cargo.toml 0.3.1 and labels the superseded historical roadmap section. Historical and migration 0.3.0 references remain intentional. | Continue distinguishing current contracts, historical examples and final exact-source qualification; do not rewrite history as current evidence. |

### Tracked follow-ups

The delivery history and remaining qualification work are tracked separately:

- [#711](https://github.com/cntryl/midge/issues/711): this row-level reference
  inventory and validator; documentation adds no new runtime guarantee.
- [#712](https://github.com/cntryl/midge/issues/712): delivered by #724 for
  current-version documentation and historical status wording.
- [#713](https://github.com/cntryl/midge/issues/713): delivered aggregate
  optional startup recovery deadline and retained cleanup ownership.
- [#714](https://github.com/cntryl/midge/issues/714): delivered manual
  compaction origin propagation; related #723 repairs native canonical
  mount lookup for safe bounded overlap scratch admission.
- [#715](https://github.com/cntryl/midge/issues/715): accounting/readback
  source controls and the measured c8f0de80 nine-cell outcome are mapped
  here. B/C meet the conditional investigation predicate; no cadence change
  is accepted. Merged PR #728 delivers measurement; this mapping completes
  its catalog criterion. Full-hour correctness/recovery acceptance uses the
  final merged catalog source, with receipts tracked on GitHub issue #711.

Recovery-progress/watchdog and explicit flush admission/wake fixes have been
delivered. Their bounded fixtures remain distinct from final full-hour
qualification. The accepted transaction-spill and replay-cache designs, deletion's storage-leak
boundary, provider-environment qualification, and bounded-schedule assurance
limits remain documented scope boundaries rather than new defects.

The multi-process shared-writer, serializable isolation, cross-column-family
transactions, provider-backed cloud backup, and salvage-as-normal-production
workflow are explicit non-goals or unsupported contracts, not missing
implementation coverage. The supported boundaries are recorded in
[`support-matrix.md`](support-matrix.md),
[`transaction-durability-contract.md`](../user-guides/transaction-durability-contract.md),
and [`backup-and-restore.md`](../user-guides/backup-and-restore.md).

## Sources reviewed

- [`architecture.md`](architecture.md),
  [`recovery-internals.md`](recovery-internals.md), and
  [`storage-invariants.md`](storage-invariants.md)
- [`transaction-durability-contract.md`](../user-guides/transaction-durability-contract.md),
  [`transactions-and-mvcc.md`](../transactions-and-mvcc.md), and
  [`api-guide.md`](../user-guides/api-guide.md)
- [`format-compatibility.md`](format-compatibility.md),
  [`cardinality-independent-architecture.md`](cardinality-independent-architecture.md),
  [`cloud-qualification-policy.md`](cloud-qualification-policy.md), and
  [`support-matrix.md`](support-matrix.md)
- [`roadmap-0.3.1.md`](roadmap-0.3.1.md) and
  [`discovery-0.3.1.md`](discovery-0.3.1.md)
- Integration evidence in `tests/durability.rs`, `tests/fault_injection.rs`,
  `tests/transactions.rs`, `tests/discovery_model.rs`, `tests/cloud_core.rs`,
  `tests/cloud_provider_engine_qualification.rs`, `tests/backup_paths.rs`,
  `tests/column_families.rs`, `tests/storage_layer.rs`, and
  `tests/verification_cli.rs`; inline tests under the owning source modules.
