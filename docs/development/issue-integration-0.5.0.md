# Local 0.5.0 issue integration

The user requested this workflow on 2026-10-24: one local branch,
`work/0.5.0-issue-integration`, with small commits for the issues and one PR
after the batch is complete. This supersedes the earlier per-issue PR train
for this batch. Keep implementation commits local; do not create individual
PRs or close issues as though local work had merged.

The branch starts at `b4d08ebe6a01d2a66eae1b4d4c155e081032f485`, including
the documentation changes already proposed by PR #980. That pre-existing PR
has a failed Core run at the base revision: the simulated cold-cache takeover
test returned `precondition failed: remote SST version changed` in
[run 40083997084](https://github.com/cntryl/midge/actions/runs/40083997084).
This is a baseline observation, not a reproduced or diagnosed integration
defect. Preserve it when interpreting broad validation.

## Current review boundary

The implementation-first batch now contains the approved runtime candidates
for the 0.5.0 issues. The chronological notes below retain earlier partial
checkpoints; their older "pending implementation" descriptions are superseded
by the later slices and the final invariant-evidence map. No issue is closed
by a local commit, and the batch has not been pushed or opened as a PR.

The final runtime/test source checkpoint is
`0bb7f09df6eaadd92ebba6870434acc30ba3a69d`. Following evidence-map and tracking
commits are documentation only. Validation at that source checkpoint:

- `cargo fmt --check`: passed.
- `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings -D clippy::pedantic`:
  passed with zero warnings.
- `cntryl-tools validate-tests`: 4,495 declarations compliant, zero structure
  violations. This is source validation, not 4,495 executed tests.
- `cargo check --locked --manifest-path fuzz/Cargo.toml --bins`: passed;
  the two metadata replay targets report an unused shared fixture helper.
- Active behavioral regressions and measurements added during the
  implementation-first pass remain unexecuted.

The implementation retains Midge's ordered whole-unit applied prefix, exact
WAL durable floors, storage-enforced epoch fencing and conservative physical
reclamation. Provider-duration metadata serialization is replaced by exact
owned captures and conditional pointer publication. Both resident and spilled
validation use the existing detached read stage; spill append/apply remains a
drained compatibility barrier. Flush and compaction names share an independently
prefetched durable cursor, and disjoint moves preserve immutable SST identity.

The remaining milestone obligations are review and evidence, plus explicitly
gated decisions. #904 has accounting preparation while its weaker floor cadence
remains unapproved and unimplemented. #939 consumes the commit-path measurements
before its design decision; it has no speculative production sharding change.
The absolute #929 matrix, #937 provider runs, relative A/B targets, runtime/crash
regressions, assurance compute budgets and Sqrzl requalification remain due.
Conditional #916 interval and #918 hash-index choices require measurements.
Deferred #922/#925/#928/#958 retain their existing gates.

Formatting, type/lint checks, source-map consistency and test-structure checks
are implementation checks. They do not establish a runtime pass, latency bound,
crash outcome or release qualification. Earlier executed results in this log
apply to their recorded slice/source, not the completed aggregate branch.

## Working rules

- On 2026-10-24 the user requested an implementation-first pass over every
  0.5.0 issue, followed by review and tests. Subsequent candidate commits may
  carry unexecuted behavioral regressions; preserve them, use compile checks
  to keep interfaces coherent, and defer broad qualification and performance
  measurements to the review pass. Do not equate implemented with qualified.
- Work in dependency order, preserving a failing behavioral regression before
  each durability fix and recording the green commands with its commit.
- Prefer a self-contained passing commit for each issue or bounded slice.
  Keep red evidence in this log and commit messages; unresolved regressions
  remain active rather than being ignored or inverted.
- Keep all changes within issue scope. Use Cargo and existing cntryl-tools /
  cntryl-stress facilities; no new runners, scripts or workflow changes.
- Record performance targets and alternating A/B evidence before accepting
  optimizations. A missing measurement is not a passing result.
- The #904 detection-window decision still needs explicit implementation
  sign-off. Keep measurement/design gates on #922/#925/#928/#939/#958; the
  user's branch workflow does not settle their technical decisions.
- Local completion, merged delivery and release/provider qualification are
  separate states. The final PR must list remaining evidence and decisions.

## Scope snapshot

The 45 open issues at batch start are:

| Group | Issues |
| --- | --- |
| Correctness and assurance prerequisites | #893, #974, #975, #976, #977, #979 |
| Commit pipeline | #904, #946, #924, #978, #940, #941, #947, #905, #906 |
| Cloud pipeline and publication | #907, #916, #914, #915, #943, #913 |
| Background work | #909, #910, #908, #912, #942, #911, #948, #949 |
| Storage formats | #926, #918, #953, #952 |
| Other hardening | #923, #927, #933 |
| Measurement and tracking | #929, #932, #935, #937 |
| Gated evaluations/designs | #922, #925, #928, #939, #958 |

The authoritative requirements are the issue bodies and
[roadmap](roadmap-0.5.0.md), including the slice dependencies from
[the WAL design](wal-pipeline-873.md). A finding refuted by a production-path
regression is triaged explicitly rather than implemented to match its text.

The user added an Azure protocol upgrade on 2026-10-24. It is tracked as #981
and stays in this branch and the eventual single batch PR.
Its broader qualification run also exposed the native S3 preparation-stage
test gap tracked by #982; that issue remains open for a separate local commit.

## Progress

### #974: implemented locally

Local replay and cloud planning now share the open-tail classification rule.
Salvage keeps the verified prefix below a recorded floor; strict recovery
rejects missing acknowledged bytes even at a clean frame boundary. Cloud
salvage durably repairs that floor before a later strict reopen. Local strict
admission still rejects a known-short file before replay/checkpoint effects.
The adapters retain their native strict error variants: local `Corruption`,
cloud `RecoveryFailed`.

Red evidence on the starting source: the public salvage regression returned
no first value, and the parity test found cloud strict recovery accepting a
missing acknowledged frame. Both tests then passed with the implementation.

Validation recorded during the slice:

- Public WAL-floor restart tests: 2 passed, including post-salvage writes and
  strict reopen without sequence reuse.
- Local WAL recovery suite with failpoints: 76 passed before the additional
  footer/floor case; that case also passed in the broad run below.
- Cloud streaming WAL planning suite: 47 passed, including five damage cases
  under both policies and durable salvage followed by strict reopen.
- Storage-root injection and WAL crash campaigns with failpoints: 20 + 15 passed.
- Full failpoint unit run: 3,396 passed, 1 failed. The shutdown-flood test
  observed 1.988651875 seconds against its one-second assertion; a focused
  rerun passed in 0.12 seconds. This is an unresolved broad-run timing result,
  not a claim that the full suite is green or that it was reproduced on the
  unchanged base. Its memory-mode workload does not use the changed recovery
  path.
- Workspace/all-target/all-feature pedantic Clippy and cntryl-tools test
  structure validation passed. No ignored tests or relaxed assertions.

### #893: implemented locally

The delayed DDL fixture now holds its admitted provider CAS behind a
test-owned condition variable until both negative-readback fencing assertions
finish. Releasing it preserves the committed-operation reconciliation checks.
Dropping the backend also releases the worker if an assertion fails. The
fixture models an admitted provider operation surviving the caller deadline;
its delegated filesystem write does not re-admit against that expired deadline.

A forced early-commit negative control failed with `ColumnFamilyCreated`
instead of `Fenced`, demonstrating the scheduling assumption in the old
fixture. This was a controlled ordering change, not a claim of reproducing
the original intermittent failure on unchanged source. The control is removed
from the final patch. The separate exact ambiguous-CAS readback success test
also passes.

Validation: `cargo test --locked --features failpoints --lib
runtime::event_loop::cloud_integration::tests:: -- --nocapture` passed the
DDL regression and 164 other tests, but failed
`should_reconcile_wal_prune_when_catalog_retirement_commits_before_timeout`
at its second-pass bookkeeping assertion. That test passed alone in 3.13
seconds. The changed backend is used only by the DDL regression; this is
another unresolved broad-run observation, not a diagnosed baseline defect.
Workspace/all-target/all-feature pedantic Clippy, rustfmt and cntryl-tools
test validation passed.

### #975: implemented locally

Removed the test-only `append_record`, `append_batch` and `sync` surface from
`WalWriter`. Accounted appends and timeout-aware sync are now required methods,
with explicit physical unchanged-proof and empty-batch semantics. All four
test doubles implement those methods; the delegating fsync-failure writer
preserves the inner append proof. Existing tests use the production methods
and retain their configured sync budgets.

The trait contract regression compares complete physical segment bytes after
the worker joins, covering single and batch append rollback through both the
real writer and the delegating double. Empty batches keep the nonzero append
position and bytes. A deliberately defective double that wrote a record and
then returned `unchanged: true` failed the byte comparison. That negative
control is removed. The timed-out queued-append test also asserts absence of
an unchanged proof.

Validation: the WAL-filtered unit run passed 393 tests. The final two-record
batch contract case passed, along with 14 durability coordinator, 3 shutdown
and 19 runtime recovery tests. Workspace build, all-target/all-feature pedantic
Clippy, rustfmt, cntryl-tools test validation and documentation validation passed.

### #976: implemented locally

The follower regression failed on the starting source: a Strict transaction
whose standalone flush staging exceeded its window returned
`TransactionApplied`. Coverage includes Strict/Batched, coalesced/fallback
routes and standalone `NoSpace` / blocked-generation `WriteStall` admission.

The request-queue and pending-transition cases already have active tests.
Mutation checks confirmed that they reject a queue error changed from
`WriteStall` to `Timeout`, and pending transition intents ignored at admission.
New tests also reject a submitted fsync falsely acknowledged after timeout
and a memory hint mistakenly enforced before the L0 slot ceiling. All four
controls failed at their intended assertions and were removed.

The combined-growth regression also failed after adding only individual
follower checks: two individually admissible 20 KiB transactions produced an
unflushable generation in a 128 KiB staging window. This confirmed extension
of the same finding was added to #976. Coalescing now tracks prepared growth
per family and ends a group before combined growth exceeds that window.
Ordinary follower admission then freezes the earlier generation as needed.
Smaller transactions retain one coalesced append; oversized individual
transactions and blocked generations keep their normal rejection types.

Validation: 9 cloud-memtable admission tests, 47 write-batch tests and 24 WAL
writer tests passed, along with the queue and transition tests after removing
their mutations. All-target/all-feature pedantic Clippy and cntryl-tools
test/documentation validation passed. The final small-group control passed
under both policies, retaining one append when combined growth fits; Clippy
also passed on that final source. No timeout or assertion was relaxed.

### #977: implemented candidate; tests deferred

Append, group-sync and ledger failure paths use the common commit-error
classifier. Byte admission still requires the physical unchanged proof, and
existing actor fencing is preserved. Only an expired caller response wait
for an admitted write becomes `OutcomeUnknown`; a proved unchanged `Timeout`
inside a response now passes through the engine. Assertion-only requests
retain typed response-wait timeouts. Post-apply cloud durability remains
unknown on failure.

Added a production writer pre-enqueue failpoint and public engine restart
regressions for resident and spilled commits. The old helper-only engine
remap tests were replaced by this path coverage; the existing routed-wait
test now expects the truthful unknown outcome. `cargo check --locked --tests
--features failpoints` passed. Execution of the regressions, broad tests,
adversarial review and performance measurements is deferred per the user's
implementation-first request.

### #981: implemented locally; Azure Sqrzl checks complete

The Azure signers previously pinned `2025-01-05`; SAS requests inherited their
operation version from the token. Microsoft lists `2026-10-06` as the current
GA SDK default with limited EUAP exceptions in its
[versioning documentation](https://learn.microsoft.com/en-us/rest/api/storageservices/versioning-for-the-azure-storage-services).
The new pin is shared by Shared Key, OAuth and managed identity signers. SAS
requests select the operation protocol through `api-version`, preserving the
raw signed fields while replacing old protocol overrides. Qualification setup
requests use the new pin too.

Red evidence: both new credential-path tests failed before the change. Bearer
signing returned `2025-01-05`, and an actual SAS GET retained an older
`api-version`. The SAS regression exercises GET, PUT, DELETE, HEAD and LIST,
including encoded signatures and stale or repeated protocol parameters.

Qualification uses the existing Cargo/Sqrzl facilities and existing compose
configuration. The running Sqrzl image is
`ghcr.io/sqrzl/sqrzl-emulator@sha256:a10c5413dba980b852cac5e718c7a92a6642c6ec60ebb6a8222f2dca7d69f7c4`.
No workflow or release configuration is changed.

Validation on the final Azure implementation:

- `cargo test --locked --lib --features sqrzl-tests,failpoints azure --
  --test-threads=1`: 81 passed, including all Azure provider/signing tests,
  the shared-key rejection case and the Azure conditional-write contract.
  The contract checks matching and stale ETags, same-length replacements,
  conditional deletes, metadata-bearing reads and missing objects.
- The Azure engine cache-loss test passed separately in 2.05 seconds.
- The added published-WAL recovery test passed in 3.70 seconds. It uses the
  existing native segment fixture after a quiescent owner, then deletes the
  whole local cache and verifies both remote SST and WAL-only values through
  a fresh engine. This is not a claim of a real Azure crash campaign.
- The 22-case provider-engine run before adding the WAL test passed 21 tests
  and failed the native S3
  late-runtime-preparation deadline case (`trace.prepared`: 0, expected 1),
  in 569.81 seconds. The focused exact case then passed in 7.48 seconds.
  The missed qualification stage is tracked by
  [#982](https://github.com/cntryl/midge/issues/982); the full gate is not green.
  Optional live S3 and child-driver entries in this suite do not establish
  separate live-provider evidence.
- All-target/all-feature pedantic Clippy, rustfmt, cntryl-tools test and
  documentation validation passed. No test was ignored or relaxed.

The Cargo qualification commands above use `SQRZL_SECRET_ACCESS_KEY` from the
local emulator configuration. The Azure engine commands are
`cargo test --locked --features sqrzl-tests,failpoints --test
cloud_provider_engine_qualification <test-name> -- --exact --nocapture`, with
`should_recover_engine_from_sqrzl_azure_after_local_cache_loss` and
`should_replay_published_azure_wal_after_complete_local_cache_loss` respectively.

The user explicitly selected Sqrzl-only qualification. Live Azure account
access is outside this task's scope and is not a pending acceptance gate.
Local authentication fixtures and emulator recovery establish the tested
contracts; they do not claim target-region acceptance or live identity
permissions. The separate broad-suite failure remains tracked by #982.

No issue is claimed merged or production-qualified by local commits.

## Implementation-first candidates

### #942: range tombstone collection

Complete persisted range endpoints now permit the bottommost overlap proof
without selecting every file in the family. Missing bounds conservatively
block the point/span proof, and any incomplete file blocks the narrower range
proof unless the whole family is selected. Runtime plans explicitly use the
existing original-source snapshot retention: old readers keep their pinned
SSTs and memtable Arcs, while GC remains excluded from the capture/register
window. Standalone compaction plans retain the existing sequence-horizon
behavior unless that source-retention proof is explicitly supplied.

Added complete/disjoint and legacy/incomplete proof regressions. Compile
check with failpoints passed; test execution, public snapshot/retirement
qualification and performance evidence remain in the review pass.

### #924: implemented candidate; qualification deferred

The read cache now has a slot per family and a release/acquire applied cut.
Ordinary commits publish only their touched families. Unchanged source views
retain their original Arc, including across another family's manifest edits.
Capturing a transaction TTL time clones one shared source bundle instead of
copying immutable-memtable vectors and path storage. Capture-to-pin exclusion
and family lifecycle dispatch remain in place. The explicit applied cut is
introduced separately by #978 before any asynchronous WAL pipeline is enabled.

Memtable mutation updates total bytes by the observed size delta, avoiding a
full family scan after every operation. Delete-range tracking and generation
start mutation still run exclusively on the event loop. An active regression
uses 100 families, verifies source Arc retention and visible data at the
published cut, and reconciles incremental memory accounting against a full sum.
The code compiles with failpoint test targets; the regression, lifecycle
interleavings and A/B targets remain for qualification.

### #978: applied-cut prerequisite implemented; pipeline remainder pending

Runtime state now separates allocated and completely applied sequences.
Resident groups advance the applied cut only after each whole unit applies;
spilled units advance it only after their final operation succeeds. Recovery
initializes the cut from replay. Both snapshot publication and fallback
BeginTransaction use this boundary; assertion-only requests return it too.
An active regression injects a strict group sync failure after a successful
unit and requires the published cut and reads to remain at the applied unit.
Compile checks pass; execution is deferred.

Strict ticket unification, loop-owned parked roots and transition drains are
still required with #940/#946. No append/fsync concurrency has been enabled
by this prerequisite commit.

### #949: merge-heap slice implemented; writer identity pending

The merge heap owns each retained event and compares its existing key rather
than allocating a second key. Advancing a nonempty input replaces the top via
PeekMut and repairs the heap once. Sequence, event-kind and input tie ordering
are preserved. Reservation accounting drops the eliminated duplicate key.
The failpoint test targets compile. Existing compaction order/coverage tests
remain active; execution and CPU A/B measurements are deferred. Finalized
writer identity and the #915 upload handoff remain required for the issue.

The following #949 slice now returns finalized logical bounds, point/range
counts, length and CRC32C from the filesystem SST writer. Streaming CRC is
computed while atomic staging consumes the exact encoded bytes, including
footer; no summary is returned before sync/rename/directory sync and scratch
cleanup succeed. Compaction carries that summary into its publication sink,
removing production post-write summary and checksum passes. Writers without
the capability retain the conservative verification fallback. Upload still
checks the exact source against the recorded identity; #915 owns streaming
transfer and provider receipts.

Active regressions cover buffered/streaming empty, range-only and multiblock
files, compare finalized metadata and CRC with independent reads, require no
output reads during finalization, and reject failed directory synchronization.
`cargo check --locked --tests --features failpoints` passes for the combined
working tree. Behavioral execution and write-to-upload mutation qualification
remain pending.

### #911: compaction checkpoint slice implemented; remaining cadence pending

Compaction publication now captures immutable manifest contents and the exact
applied edit cut only when the existing byte/edit threshold requests a
checkpoint. The existing publication worker encodes and installs it under the
manifest writer lock; journal edits newer than the captured cut survive.
Active recovery regressions cover a concurrent newer tail and a crash after
snapshot rename before journal truncation. Test targets compile in the shared
working tree; behavioral execution remains pending. Ahead-of-use name
reservation and the remaining publication cadence paths still need review and
implementation before the issue can be considered complete.

### #946/#905: same-group overlay implemented; asynchronous handoff pending

A sequenced staged-write overlay now supplies same-group point/range write
conflicts, assertions and insert-only existence/TTL decisions. It retains Bytes
handles and borrowed preflight/deduplication keys. Blind overlapping writes can
share one append/sync; mixed operation-ordering barriers are preserved.
Active tests cover overlapping strict units across restart, write conflicts,
staged range deletion and insert-only, and require coalescing while retaining
failure outcomes. Failpoint test targets compile; no tests or A/B runs have
executed for this candidate.

Off-loop snapshot validation with intervening-write revalidation, earlier
in-flight groups, pipeline receipts, parked roots and concurrent apply remain
pending. This slice does not enable an asynchronous pipeline.

### #948: overlap partition slice implemented; Fragment narrowing pending

Plans capture level+2 bounds and a configurable soft overlap-byte target
(default 320 MiB in LeveledCompactionConfig; zero disables). Output merging
introduces non-mutating boundary events at grandparent starts, so range-only
coverage can also be fragmented legally. Indexed weighted endpoints measure
overlap in logarithmic time per key group. Whole user-key groups remain
indivisible. One oversized grandparent, an inseparable overlapping set, or
unknown legacy bounds can exceed the soft target; the roller avoids splitting
every key inside that same oversized span. Exclusive range ends may coincide
with the following grandparent's first key without covering that key.

Active tests cover feasible point layouts, repeated-key reduction, contiguous
range coverage, and one oversized grandparent. Compilation reaches this code;
the latest shared check stops in concurrent cloud-publication interface edits.
Execution, A/B write amplification and narrowing the existing Fragment fallback
remain pending. The fallback is retained conservatively for legacy or oversized
source/target layouts while the new partition policy is qualified.

### #927 and #915 receipt slice: implemented candidates

Cold-start SST inventory uses a bounded group of concurrent probes, retains
manifest order, and advances progress only after verified outcomes. Native
provider executors use configurable bounded full-jitter retry delays. Immutable
SST retries retain the same IfAbsent key, body, deadline and memory admission,
reconciling ambiguous effects by pinned content verification. Generic catalog
and unconditional mutation replay stays disabled.

Immutable publication now uses the existing validated provider receipt path;
a matching digest over the exact sent bytes avoids range readback. Missing
receipts retain identity-pinned verification. The source still has to match the
writer's finalized size and CRC before upload. Active regressions cover receipt
success/fallback/mismatch, ambiguous immutable creation, bounded startup probes
and failed progress. Code compiles through these paths; execution, provider
qualification and latency/request targets are pending.

#915's streaming multipart stage, bounded part reservations and removal of
whole-file admission remain uncoded. #907 off-loop catalog publication and
#916 parallel upload scheduling are separate following slices.

### #953: FORMAT 9 codec and append-only intents implemented candidate

Local and cloud metadata bodies share one MessagePack map frame with CRC32C.
The decoder uses borrowed input plus a structural extent/depth preflight to
reject oversized announced bodies/collections before allocation. Snapshots and
journal edits use the codec; intent changes append predecessor-chained suffix
edits and compact on bounded byte/edit thresholds. Database FORMAT 8 is
explicitly rejected by the FORMAT 9 candidate, with no JSON fallback.

Active property/corruption, intent replay/torn-tail and 20,000-file footprint
regressions are included, and the existing manifest fuzz target reaches the
new snapshot decoder. Direct JSON unit fixtures were updated. Library compile
checks pass through this slice; execution/fuzzing and recording actual byte
counts remain pending. The populated FORMAT 9 and future FORMAT 10 golden
fixtures plus external compatibility/CLI assumptions still need updating
before the compatibility gate can pass. Existing FORMAT 8 bytes stay as an
explicit rejected fixture. #914's new committed cloud chain is not part of
this codec commit.

### #912: TTL compaction candidate implemented

Compaction captures the engine's observed clock at plan launch. The newest
expired version and its selected older versions are removed only with complete
point-overlap coverage and retained original snapshot sources (or no active
snapshot for standalone callers). Expiration never selects an older non-TTL
value as a replacement. Uncertain coverage keeps the expired masking value.

Writers, flush publication, metadata conversion and independent SST summaries
carry an advisory earliest expiration. Existing periodic maintenance uses it
to pay cold TTL debt below normal size/count thresholds: bounded ordinary
closures drain shallower levels before a bottom-level rewrite can prove no
resurrection. Files without recorded TTL debt avoid periodic rewrites. This
may promote extra files in an expired family; write-amplification measurements
remain required.

Active raw-output tests cover captured clock, overlap proof and snapshot
retention conditions. Public local/simulated-cloud tests require physical
manifest reclamation, a held pre-expiry snapshot, no older-value resurrection
on restart, and idle reclamation without new writes/manual compaction.
`cargo check --locked --tests --features failpoints` passes on the combined
working tree. Test execution, crash campaigns and space measurements remain
pending.

### #907/#916: WAL publication worker and upload pool implemented candidates

WAL verification/catalog CAS runs on an owned worker. Completion matches its
operation and epoch, revalidates authority, and advances only a verified
contiguous prefix. In-flight verification retains upload pressure credit until
its acknowledgement is owned. Shutdown explicitly drains publication; a timed
out admitted worker remains owned and joins before lease resources can release.
Immutable WAL uploads use a fixed configurable pool (default four) with a
256 MiB retained-memory cap. Body/provider-copy/fallback-readback reservations
are acquired before reading, and backend requests retain their admission.

Active regressions hold the publication worker while local writes/read progress,
fence stale completion, reorder four upload acknowledgements, and exercise
pool failures/retries and memory bounds. Combined failpoint test targets compile
without warnings; execution and provider/crash qualification are pending.
Metadata mirror and remaining seal-provider offload are still required by #907.
No publication batching interval is selected before #916's measurements justify
one. Mirror authority/repair semantics remain synchronous and unchanged.

The next #946 slice moves resident conflict/assertion/insert-only SST queries
to a fixed worker with bounded pending jobs and retained memory. Captured source
pins have a separate internal ID domain. Completion carries the applied cut and
writer epoch; intervening applied writes invalidate positive and negative
results and trigger a fresh read pass, including writes already flushed to SST.
Final loop admission evaluates current TTL and the staged overlay before any
sequence allocation or WAL effect. Shutdown cancels queued caller routes while
retaining ownership of active read work and its pins.

Active regressions hold a read worker while blind writes and flush progress,
then require a stale negative proof to retry and detect the flushed conflict.
Another regression isolates transaction/validation pin IDs. Failpoint test
targets compile. Spilled validation still needs off-loop handoff; the complete
operation-kind interleaving matrix and earlier in-flight group overlay remain
pending with #940. No test execution or performance evidence is claimed.

### #914: separate pointer and takeover claim implemented candidate

Metadata commitment now CASes an independent pointer. Acquisition claims that
same identity with epoch/owner before activating lease validity, preserving the
predecessor's committed generation. A delayed predecessor either commits before
the claim and is preserved or loses its old pointer identity; learning a new
identity does not authorize its old epoch. Heartbeat changes no longer share
the metadata CAS object.

The initialization protocol distinguishes a new pending namespace from an
initialized authority: crashes after lease CAS or pointer claim can resume
safely, while an initialized missing pointer and the prior embedded layout
fail closed. Active takeover/initialization regressions cover these boundaries.
Failpoint test targets compile. Versioned shared-codec delta objects, cached
common-path request counts, retention on their exact dependency closure and the
crash campaign remain pending.

### #923: read-path contention candidate implemented

Only the final transaction release takes the shutdown wait mutex; its
condition-check/notification ordering still excludes a lost wakeup. Lease
mutations publish an immutable ArcSwap tuple of epoch, deadline and terminal
state before notifying waiters, so ordinary read-authority snapshots avoid the
renewal mutex. Existing before/after read and per-row checks remain intact.
Normal unpin updates one generation count instead of scanning all active
snapshots. Last-generation release tracks every filename across distinct live
generations; GC still samples the authoritative active-pin union under the
capture/registration exclusion boundary.

Active regressions cover nonfinal release with the wait mutex held, final
release racing wait setup, coherent authority reads with renewal locked, and
10,000 snapshots across generations sharing a filename. Failpoint test targets
compile. Existing read-fencing and #341/#342 tests are retained unchanged;
execution and the named 32-client A/B target/evidence remain pending acceptance.

The #940/#978 receipt prerequisite now records the pre-I/O end offset, actual
appended sequence, pending counts, epoch and segment. Ticketed completion binds
the barrier generation and consumes only that captured prefix. It no longer
uses a writer's later mutable position as evidence for an earlier fsync.
Active real-writer recovery coverage appends a later unsynced frame between
fsync and acknowledgement, truncates it, and requires strict recovery of the
earlier prefix. A rejection matrix covers wrong generation/epoch/segment.
Failpoint test targets compile without warnings; behavioral execution remains
pending. This remains synchronous groundwork for the ordered pipeline.

The #923 fast path also covers `LeaseValidity::remaining`, which the actual
before/after read-authority wrapper calls. A valid published deadline returns
without the renewal mutex. Expiry takes the writer lock to recheck the current
deadline before fencing, so a timely concurrent renewal is preserved. The
held-renewal regression now exercises both the tuple snapshot and remaining
validity. The current combined check is blocked by concurrent multipart
provider interface edits; this follow-up has not executed its tests.

### #940: ordered visibility decision reconciled

The user selected Midge's existing ordered whole-transaction publication on
2026-10-24. The loop can prepare/queue later work during fsync and serve reads,
control and validation against the earlier applied cut. Later BestEffort writes
wait for that prefix before success/visibility. A different commit-visibility
map or reordered linearization is outside this batch. GitHub #940 R3/R4/AC1
were updated and read back: deterministic held-fsync progress replaces the
contradictory five-millisecond later-write completion requirement. The WAL
design and public contract agree; issue implementation is still in progress.

### #933: bounded transaction-retention candidate implemented

OpenOptionsBuilder exposes a Drop transaction-wait window (default30seconds).
The owned reaper closes admission, waits that window, then terminally fences
outliving transactions. Runtime read wrappers enforce that state before/after
reads and iterator rows, and commits reject new admission with Fenced.
The terminal state survives runtime closure. Explicit Engine::shutdown(timeout)
keeps its graceful Busy/retry behavior.

The runtime and every admitted worker still drain before fencing resources
release. A blocked unfenceable physical effect can therefore retain the lease
beyond the client wait window; the timeout does not abandon ownership. Logging
records the live count and this terminal disposition.

Active local/simulated-cloud regressions retain reads, cached scans and an
uncommitted writer across Engine drop, then require successor recovery plus
Fenced old operations. Another holds an admitted Strict WAL unit at its sync
boundary: the client becomes fenced but successor acquisition remains excluded
until the unit drains, after which the acknowledged value recovers. Existing
graceful/retry and outliving-transaction tests remain active. Combined failpoint
test targets compile; execution and broader blocked-provider campaigns remain
pending.

The following #940 slice adds nonblocking lower-writer sync submission. Typed
prefix/completion identities bind barrier, epoch, segment, sequence range and
end offset. The pending handle polls once and retains native root ownership
through settlement; the worker broadcasts completion only after child effects
settle. Async sync admission is bounded at128 queued requests. The synchronous
compatibility path waits on the same receipt and keeps its admission/deadline
behavior. Active held-worker/native-root and wrong-prefix regressions are
included; failpoint test targets compile without warnings. No behavioral tests
have executed for this slice. Event-loop pipeline enablement, append completion
events and writer-thread encoding are still being implemented.

### #915: bounded native streaming candidate implemented

Native immutable-file publication now streams owned file handles in bounded
5 MiB parts, reserves retained part/provider workspace, and checks the final
source CRC before committing the object. S3 multipart and Azure block sessions
carry checked part/session identities; GCS uses generation-conditional resumable
upload and validates its whole-object CRC because that protocol has no checked
intermediate-chunk receipt. Same-deadline aborts retain uncertainty as residue,
never authoritative metadata. Conditional final commitment preserves IfAbsent.

Active native HTTP regressions cover64MiB publication, corruption and changed
finalized source across S3, Azure, GCS JSON/XML. Full failpoint test targets and
restricted provider library configurations compile. Tests, missing/wrong receipt
fallback, unknown final results, abort/retry campaigns and Sqrzl remain pending
qualification. No actual provider request/peak-memory performance result is
claimed from compile checks. Provider protocol details and limits are recorded
in cloud-file-publication-915.md.

TTL maintenance also advances the next periodic check toward the earliest
recorded expiration, bounded between1second and the normal30second interval.
Flush publication shortens a later scheduled check. Due or uncertain TTL debt
retries at that bounded cadence instead of busy-spinning behind an occupied
worker/publication. This makes the existing idle-reclamation regression's
bounded wait meaningful; the previous30second-only schedule would not do so.
Failpoint test targets compile for this follow-up; execution is still pending.

The #953 compatibility slice now includes populated/empty binary FORMAT9
fixtures and a futureFORMAT10 fixture. Historical FORMAT8 bytes remain rejected
under strict and salvage. Bounds mutations edit binary fields and recompute
framing checksums so semantic verification remains tested; CLI/startup/fault
fixtures use FORMAT9 and production intent encoding.

Frame/journal CRC storage uses big-endian checksum bytes so CRC32C of a complete
valid frame does not collapse to the little-endian appended-checksum residue.
An active equal-length frame identity regression guards that distinction.
The focused production-encoding/reopen fixture regression passed1/1 (0.12s)
with `cargo test --locked --features failpoints --lib
metadata::codec::tests::should_reopen_populated_fixture_contents_when_encoded_as_format_nine
-- --nocapture`. Its earlier little-endian intermediate materialization result
is superseded. Full codec/fuzz/compatibility qualification remains pending.
The later #918 SSTV5 slice must preserve this V4 artifact as rejection evidence
and add the corresponding current V5 fixture.

The next #914 slice stores control-metadata changes in versioned
`manifest/{ordinal}/{operation_uuid}` objects using the shared binary codec.
Objects identify epoch, operation and exact parent identity; the committed
pointer selects the head. Recovery follows its bounded snapshot/edit closure,
checks ordinal/parent/epoch lineage and reconstructs exact control files. LIST's
largest uploaded ordinal has no commitment authority. Snapshot resets bound the
closure (provisional16objects/64MiB,16KiBedit policy).

Active checkpoint/recovery and orphan-high-ordinal regressions are included.
Library compilation passes; behavioral execution is pending. Cached common-path
request reduction, #913/#943 retention and crash/provider qualification remain
required; this commit does not claim the two-request target.

### #909: job reservations and immediate follow-up candidate

Compaction admission now owns an explicit job reservation through publication.
It binds input identities/order and source/target publication ranges. Conflicting
ranges are excluded even with disjoint input names; unknown bounds reserve
conservatively, one L0 job per family is enforced, and repair/fragment jobs
remain exclusive. Publication rechecks captured identity and L0 ordering fields.
The production slot limit remains1 until the bounded pool/coordinator is wired.

Every successful publication requests the next normal picker turn, including
deep-level debt with no critical L0 pressure. Active coverage includes range
conflicts, unknown bounds, disjoint levels/families and stale input ordering.
A real compute/publication/restart regression requires a second deep-level job
before a deliberately one-hour timer, without another flush. The shared check
currently stops in concurrent metadata-cache and !Send fixture updates; no test
execution is claimed. Configurable multi-job scheduling, shared memory admission
and per-family flush concurrency remain pending.

The remaining #907 metadata mirror now captures an immutable local control-file
cut and submits provider work to an owned worker. Operation/epoch completion
retains the caller's original deadline and continuation. DDL response, flush
cursor/backoff, GC deletion and SST-name coverage change only after matching
successful unfenced completion. Manual compaction plans/waiters remain pending
through name reservation rather than being rejected or falsely completed.
Read and resident validation progress is permitted under the mirror owner.
Shutdown retains and drains admitted work.

Five active regressions cover held-provider local progress, stale names,
original flush ownership, abandoned callers and failure backoff. Shared failpoint
test targets compile after the related fixture/interface updates. Behavioral,
crash, latency and provider qualification remain pending. Sealing itself keeps
its required local append/fsync/rotation barrier; remote upload/publication is
off loop. Publication independence between flush and compaction remains #910.

The #914 cached common path now reuses receipt-validated pointer identity under
monotonic writer validity and the takeover claim. Reduced manifest authority
and fixed-size SHA fingerprints avoid retaining another SST inventory or full
control-body cache. An active receipt-backend regression requires exactly an
edit PUT plus pointer CAS with zero reads, then reconstructs the committed
intents. Failpoint test targets compile; that regression has not executed.
Unknown outcomes, replacement identities, cache invalidation and provider/crash
qualification remain gates. The two-request count is a coded assertion for the
supported receipt path, not a measured provider result.

#909 compute now runs on a bounded reusable worker pool instead of creating an
OS thread per job. Owned task tickets join terminal work, pool teardown closes
admission and joins all workers, and worker panics remain terminal results
without killing the reusable worker. Active regressions cover physical thread
reuse, a held worker with a full queue, and post-panic progress. The existing
single production slot is preserved until the multi-job coordinator/shared
budget handoff is ready. The latest shared compile is in concurrent name-
prefetch/interface edits; these tests have not executed.

#940's ordered resident Strict fsync stage now parks one exact-prefix physical
sync, native root and transition ticket on the loop. Reads/control/validation
can progress against the prior applied cut; later writes and transitions queue
within bounds until the whole prefix settles. Deadlines fence logical progress
but retain root, worker and memory ownership until physical termination. The
same ticket/coordinator path commits receipt then applies/publishes in order.
Three existing thread fixtures construct EventLoop on its owning thread to
preserve the lexical root's !Send bound.

An active physical-fsync gate regression checks old-cut read/control progress,
blocked later BestEffort success, ordered visibility and stale-snapshot behavior.
Failpoint test targets compile cleanly; no behavioral test execution is claimed.
Async next-group append, writer encoding/copy work, concurrent apply, spilled
proof/sync handoff and oversized Strict compatibility remain unfinished. #904's
explicit recovery-window decision is still pending.

The writer publishes its completed-write prefix before append acknowledgement,
then releases its queue/sync locks before notifying the caller. This closes the
window where immediate typed sync admission could reject an append the caller
had already been told completed. The held-fsync stage regression exercises that
handoff; its execution remains pending with the rest of the implementation pass.

#913 adds bounded metadata-generation retirement under the same publication
lock used from head capture through body decoding. The selected chain closure
and current plus three older ordinals remain protected; every deletion checks
decoded lineage, fresh lease/head authority and the original object identity.
Each pass permits four LISTs and eight candidates. Active regressions cover
snapshot retention/restart, required delta history, pointer replacement and
capture exclusion. Shared test targets compile; these tests have not executed.
An idle continuation is still required so the bounded cursor progresses after
the final write or a cooldown, and is being implemented separately.

#911 now prewarms ordinary flush-name blocks of sixteen and replenishes when
eight remain. Only matching successful mirror completion admits the bound;
output launch consumes admitted names without a new journal append or mirror.
The cursor is captured before raising the durable counter, preserving every
unconsumed name. Active tests check first-block consumption and replenishment
against the durable selected manifest. They compile but have not executed.
Compaction's WAL-sequence jumps still use the owned asynchronous fallback;
this slice does not claim every job start finds an ahead-reserved block.

#941/#906 now have numeric provisional targets recorded in their issues and
`performance-targets.md`, before the corresponding implementation completes.
The aggregate reference is `172c941201f42e0206ca912d080e65c23ff4133c`; concurrent
apply additionally requires a pinned completed #941/#947 checkpoint for an
isolated comparison. Three alternating pairs and raw artifacts are required;
no gain or qualification is claimed by preregistration.

#913's idle continuation now submits an owned no-op mirror every thirty
seconds through the existing gate and worker. Idle wakeups drain the bounded
retention cursor without another metadata mutation. An active 25-snapshot
regression exercises cooldown and multiple idle batches with an injected short
interval rather than resetting cooldown state. Failpoint test targets compile
cleanly; execution remains pending.

#941 parks sequenced raw append, then exact append completion and Strict sync,
before the existing ordered commit/apply handoff. The writer owns nested
encoding, LZ4, outer framing, CRC and XXH3; the loop owns sequence allocation
and bookkeeping. Admission charges simultaneous raw and encoded/compressor/
coalescing ownership through physical termination, including abandoned waiters.
Active regressions hold encoding/fsync, compare real writer bytes over six
compression sizes and mixed TTL/insert/delete/range operations, and reject
insufficient raw admission budgets. Test targets compile; execution and A/B
qualification remain pending. Large/spilled/non-filesystem paths retain their
drained synchronous compatibility barriers. Next-group append is still ordered
behind the active group; this slice does not claim a multi-group pipeline.

#918/#952 add SSTV5 varint entries, optional TTL headers and sixteen-entry
restart directories with continued-version markers. Point seeks use binary
restart selection; forward/reverse scans decode intervals lazily. Historical
V4 bytes remain rejection evidence. Active tests cover decode bounds, equal-key
versions spanning intervals, TTL/deletes, reverse bounds and malformed framing.
Three production decoder fuzz targets each have eight real writer-produced
seeds, with additional footer/range and binary metadata/journal seeds.

The existing Cargo fixture/corpus tests passed 2/2 (latest 0.39s); the new
FORMAT9/SSTV5 fixture verifies and Engine reads its data. All fuzz binaries
compile through the existing fuzz package. Restart/conformance regressions,
fuzz time budgets, performance and system footprint qualification remain
pending. Redundant unsafe lease auto-trait declarations were removed and a
compiler trait-bound assertion retains the requirement.

#947 borrows passthrough and incompressible WAL values until final framing,
removing the intermediate owned-value allocation/copy. Compressed values keep
the existing owned LZ4 output. Active tests check borrowed pointer identity,
reference LZ4 bytes/replay and a legacy-owned versus borrowed frame/CRC matrix.
Test targets compile (the shared snapshot still reported unfinished #908
warnings); these tests and allocation/A/B measurements have not executed.
The preregistered construction target is one allocation and 4096 copied value
bytes on the incompressible 4 KiB fixture; the existing rotation row is
compressible and cannot establish that saving.

#943 persists known-retired SST deletion obligations in the manifest decision
and checkpoints, then hydrates them after a cold restart. Deletion requires
the original length/CRC, bounded reads pinned to one provider identity, fresh
writer authority and a conditional DELETE. Missing proof and replacement
objects remain retained. Terminal confirmation journals the original identity
and refuses active or replaced metadata. Active tests cover durable queue
replay, cold hydration, replacement races, writer fencing and snapshot pins.
Test targets compile; execution/crash qualification remains pending. General
SST orphan and catalog-anchor WAL rediscovery are still separate code gaps.

#908 adds opt-in caller-side admission slowdown below existing L0/memory
ceilings, an estimated pending-compaction byte band/hard limit and a shared
flush/compaction output rate schedule. Defaults preserve immediate admission.
The original response deadline covers slowdown and submitted work; expiry or
fencing during the delay leaves no queued mutation. Worker-side rate charging
covers bounded scratch/final write chunks, bypasses under hard pressure and
remains bypassed through shutdown drain. New metrics separate delay and charged
bytes from hard stalls. Active public regressions cover soft L0 success/reopen,
hard L0 rejection, pending-byte rejection and unchanged defaults; internal
regressions cover pre-admission expiry and shared real-writer rate/identity.
The latest shared check is in concurrent #909/#906 declarations; execution and
sustainable-load A/B qualification remain pending.

#906 uses a bounded two-worker pool for captured memtable effects within one
synced resident group. Runtime bookkeeping, source-size accounting, range
tracking and one whole-group applied cut remain loop-owned. Reads/control stay
at the prior cut while effects finish out of order. Failure, panic, cancellation
or authority loss fences subsequent mutation globally and retains ownership
until every admitted worker settles; restart replays the durable group whole.
Active regressions hold the first unit while a later unit applies, verify old
and new snapshots plus exact memory accounting, and lose authority before a
cross-family BestEffort attempt and restart. The shared compile reaches only
concurrent #909 test renames; behavioral, loom/differential/crash and A/B
qualification remain pending. A related #941 regression preserves NotAttempted
for an append whose physical rollback proves unchanged NoSpace.

#943/#913 now have an optional bounded inventory-page capability. Native
S3/OCI, Azure and GCS XML/JSON perform one non-retrying request per page, bounded
to 32 keys, a 64 KiB response and a 4 KiB continuation. Unsupported inventory
retains/defer work rather than collecting and truncating a whole prefix.
Metadata retention preserves its ordinal/page cursor across four-LIST/eight-
probe attempts, so many orphan UUIDs at one ordinal cannot starve later pages.
Active HTTP regressions count requests across continuation/503 responses and
reject repeated tokens; a 25-orphan ordinal regression preserves current
recovery while draining bounded batches. Test targets compile (only #909 WIP
warnings); behavioral/provider execution remains pending.

#979 adds active wrong segment/record version regressions with otherwise valid
framing CRCs, a physical-fsync versus loop-owned frontier/accounting handoff
regression, and retained read/scan/diagnostic coverage after WAL I/O fencing
while the read lease remains live. The catalog now distinguishes retained
local reads from lease-dependent cloud reads and records admitted concurrent
memtable effects above the visible cut. Test targets compile; execution and
required mutation-based red evidence remain pending. The evidence-map audit
is a separate follow-up and will retain unresolved assurance gaps explicitly.

#937 registers an opt-in Tier 5 Sqrzl diagnostic target beside the shared Tier
2 simulator workload and local Strict floor. Continuous lag samples exclude
the final drain and require tick/gap health; native costs count existing HTTP
attempt completion events through shutdown join, including retries and all
engine request bodies. Missing/incomplete observations fail as inconclusive.
The simulator's resident-WAL byte scope remains explicitly distinct. Both
benchmark targets compile; no performance/provider execution or exact-SHA
result artifact is claimed. The latest failpoint test-target check also passes,
with only the concurrent #909 work's temporary unused-method warnings.

#979's map audit reviews all 65 catalog IDs, resolves 234 active test symbols,
refreshes actual source/assertion anchors and removes 29 stale row references.
It adds CLOUD-8 and SST-10 and preserves historical qualification provenance.
PROG-1/2 remain explicit active-Cargo coverage gaps pending the next test slice.
`cntryl-tools validate-docs` passes (66 Markdown files), as does diff checking.
Postimage hashes describe the captured working-tree sources based on
`2494eff5`; concurrent sibling edits mean a final anchor/hash refresh is still
required at the completed code checkpoint. This audit is not test execution.

#910's design is recorded in `metadata-publication-910.md`: arrival-ordered
validated local edits, immutable epoch/cut captures, optimistic exact-identity
pointer CAS and monotonic covered-cut acknowledgement. The provider-duration
gate stays until that protocol and GC capture protection are wired. Releasing
it around the current live-Fs/full-image publishers would be unsafe. This is
the remaining publication implementation, not a delivered independence claim.

#909's prerequisite now has explicit per-job cores behind a bounded coordinator,
configurable compaction/flush counts (1..=64, default 1), shared memory admission,
reserved-range-aware picking and per-worker scratch proof scopes. Exact owned
compute results and bounded polling tolerate early/lost notifications; completed
jobs retain their inputs and reservations through publication. Per-family flush
slots bind completion/retry/release to exact IDs, and queued name reservations
retain every job's generation/deadline. Publication callbacks reselect their job
and ignore unrelated stale errors without settling active ownership or files.
Test targets compile cleanly. Held-two-worker/fairness/budget production-loop
regressions are the next slice; execution, crash and A/B qualification remain
pending. The global publication gate remains for #910's protocol change.

#943's general SST sweep now captures immutable per-family startup allocation
floors before replay/prewarm and binds them to the acquired epoch. Only canonical
older generations with complete exclusion from local/selected manifests,
snapshots, intents, retired obligations, active jobs and pending names can be
probed. Class-specific inventory owners feed bounded pages and an eight-probe
conditional-deletion pass on the existing worker. Unknown proof retains data;
late predecessor PUTs can recreate only an unreachable leak for a later pass.
Active tests cover floor/prewarm separation, pin/intent/pending-job protection,
malformed references, bounded page tails, replacement identity and rediscovery.
Combined test targets compile; execution/crash/provider gates remain pending.
Catalog-anchor WAL rediscovery is the next isolated slice.

#979 adds five active default-Cargo progress regressions through existing real
remote-WAL, manifest-authoritative HEAD and journal recovery fixtures. Held or
failed transport cannot earn completed-work credit. The existing listener's
caller/thread/generation scopes now run against real replay, including nested
non-recovery scope restoration; uninvoked synthetic event helpers are removed.
`cargo check --locked --all-targets --features failpoints,stress-soak` passes
cleanly (latest 3.13s). Behavioral execution and mutation red evidence remain
pending; the PROG rows will be refreshed at this test checkpoint.

#940/#905's next group consumes only the leading deferred resident writes.
An older layout/spill/rotation lookahead remains ahead of newer writes, while
eligible queued units share one physical sync after the earlier group settles.
Active regressions count the real writer's fsyncs and require a rotation to
precede the newer write. The same all-target failpoint/stress-soak compile
passes cleanly; execution is pending. This improves ordered queued coalescing
without claiming simultaneous physical append during the held fsync.

#943 now rediscovers WAL objects in the admitted catalog's permanently closed
ID range through bounded pages and pinned header reads. Canonical identity,
valid header lineage, fresh same-epoch anchor, shared `retired_by` authority and
lease validation precede conditional deletion. This is unreachable-key proof,
kept distinct from full original membership/digest proof for known retirement.
Above-anchor/future/malformed candidates remain retained. Active regressions
cover lost queues, late retired PUTs, replacement/epoch races and bad headers.
Test targets compile cleanly; execution and provider/crash gates remain pending.
Two growing metadata functions were split into focused helpers for the hard
line-count policy; no lint suppression was added.

The PROG map refresh is pinned to `fb06795f`: five real-fixture Cargo tests
replace the old inactive listener claims, 239 active test symbols and 1196
assertion/helper anchors are mapped, and all 241 `should_...` tokens anywhere
in JSON resolve. `cntryl-tools validate-docs` passes (67 Markdown files).
Historical qualification remains separate, and final source postimages must
still be refreshed after the remaining code changes.

#904's safe preparation observes ledger file/directory sync attempts and
successful receipts at real Fs boundaries, exposing them beside writer barrier
syncs and an explicitly scoped accounted sum in runtime metrics and existing
Strict/rotation rows. Segment initialization/repair and other filesystem work
are excluded from that sum; it is not a complete physical-device count.
Active regressions cover ledger checkpoints, failed/after-effect syncs and a
public Strict commit/reopen. Exact floors, encoding and recovery semantics stay
unchanged. All-target compilation passes; behavior and measurements remain
pending. The metric builder uses conservative empty defaults while always
installing actual runtime health; focused helpers keep hard function limits.

#978/#906 now fence the global applied prefix after partial legacy/spilled
apply as well as concurrent apply. The aggregate loop-owned bit makes admission
constant-time across every family, including direct actor preparation. A
cross-family BestEffort cannot advance the cut past hidden partial entries.
Active coverage fails a real spill after its first effect, requires rejection
without visibility advance, and reopens to the whole durable unit. Additional
held-group spill and oversized-resident regressions pin drained compatibility
barriers. All-target failpoint/stress-soak compilation passes cleanly (7.47s);
execution remains pending. These paths remain synchronous after the drain.

The partial-spill public child-process regressions now require same- and
cross-family Strict/BestEffort writes to be fenced and retained snapshots to
stay at the complete prior cut. Restart still proves the whole durable unit.
Delayed resident validation now covers point conflicts, insert-only, assertions
and range deletes across a real intervening flush, plus an earlier held Strict
group. These are active regressions; execution remains pending.

#910's generic captured-cut protocol now compiles with the cloud features:
opaque epoch/ordinal captures, exact body digests, retained intent obligations,
same-epoch CAS reconciliation and monotonic pointer/cache observations.
Unknown effects retain their capture after the task handle drops. Provider
PUT/CAS work runs outside the coarse cache mutex. Runtime flush, compaction
and auxiliary mirror tasks now receive captured cuts. Flush ownership also
binds completion to the exact physical phase, epoch, family and identity;
duplicate or early results cannot settle a retained pipeline.

GC's local SST/WAL effects and known-retired cloud workers use the same local
capture mutex for the final protection check and physical effect. They defer
while captures remain unresolved and retain the original durable obligation.
General SST/WAL discovery and metadata-generation retention have the same
protection edge. Active generic overlap/CAS/epoch/unknown-effect and physical
GC regressions are coded, not executed. Short local edit/capture gate turns
and real background overlap/recovery regressions are the current integration
slice; an independent-flush latency qualification is not yet claimed.

#909 now has active real-loop two-worker publication/restart, independent job
re-pick, overlapping reservation rejection, oldest-prefix L0 ownership and
shared memory failure/isolation regressions, plus a public per-family held
flush build/restart test. Compilation passes; behavioral execution and A/B
qualification remain pending.

#948 narrows the legacy source-fragmentation fallback: exceeding a soft work
target alone does not force a source rewrite when selected target overlap
fits the configured overlap budget. Exact/under-budget and disabled-policy
picker cases accompany a real whole-merge cloud publication/cold-recovery
regression. Unsafe/incomplete layouts retain conservative handling.

The latest all-target failpoint/stress-soak compilation passes cleanly
(12.57s shared snapshot). The pedantic check still finds three hard function
length errors in the active trivial-move slice plus warnings; it is not a
zero-warning validation result. No behavioral, crash, provider or A/B pass is
inferred from compilation. #904's live issue now records its accounting
candidate and the still-unapproved cadence change explicitly.

#910's short local turns are now committed: background provider work retains
its admitted cut while the gate reopens. Completion reacquires the exact local
owner before applying its edit; a bounded retained result waits behind a
legacy retirement turn, and duplicates cannot add new ownership. Active
regressions hold a real compaction manifest PUT for two seconds, require a
later flush within 100ms, verify point/scan/cold recovery after both settle,
and pause a real checkpoint between rename and journal truncation. These are
coded assertions, not measured or executed results. Inline compatibility
publication and legacy WAL-retirement serialization remain explicit.

#926 now performs eligible disjoint moves through atomic logical-level edits
and a distinct move intent. It preserves names, bytes and checksums without
opening SST readers/writers or creating physical replacement/retirement
obligations. Exact live sources, L0 order and target conflicts are revalidated
before the edit. Failure/uncertain publication retains the original objects.
Eight active transition/zero-work/failure/restart/target-conflict regressions
compile; execution and provider/performance qualification remain pending.

Live issues #909/#910/#926/#948 now record these implementation candidates and
their unexecuted gates, and all five updated issue bodies (#904 included) were
read back with the candidate section present and state still OPEN.

The final coding gaps under active implementation are bounded streamed-spill
validation (#946) and a shared ahead-of-use flush/compaction name cursor
(#911). Spill append/apply stays a drained compatibility barrier under #978;
moving its SST validation off-loop first requires proportional decoder-memory
ownership. No fixed large budget increase or per-key proof cache is accepted.
The compaction cursor must consume a name once, retain it across provider wait,
and restart beyond the durable exclusive bound without changing WAL epoch
semantics. Neither gap is represented as completed or qualified here.

Those two remaining runtime gaps are now coded. #911 shares a per-family
prefetched durable name cursor across flush and compaction, preserving the
accepted generation/deadline through reservation and short-turn contention.
#946 streams resident/spilled SST validation through the existing worker;
decoded fields retain real reservations, range traversal is bounded, and
constant-size TTL/error summaries are revalidated against the exact live cut
and source generations. The moved source keeps its original transaction pool
and disk ownership rather than being charged again against the 8MiB capture
queue. One consumed-completion workspace prevents aggregate decoder buildup.
Five decoder and thirteen dependent active regressions cover budget/corruption,
conflict/insert/assert/range, TTL/error order, deadlines, epoch loss, flush,
restart and legal large sources. Execution remains deferred.

Final #910 integration charges captures and cold selected-metadata reads to the
existing shared maintenance pool, retains physical PUT/GET charges after caller
timeout, and reuses exact staged object identity for small-pool lost-CAS
readback. Selected `ManifestPublished` receipts authorize an intent clear;
`OutputDurable` alone does not. Temporary memory/count admission retains exact
flush/compaction ownership and the original absolute deadline. The requested
gate-deferral counter and a labeled CloudSimulated compaction-backpressure row
are implemented. Independent-flush fixtures now cover held PUT, duplicate/lost
successful CAS callbacks and cold recovery before release, excluding exact
unselected output names from recovered authority. A process-kill campaign and
actual latency/provider results remain separate outstanding evidence.

The final full issue audit found no further clearly missing approved runtime
behavior. Explicit recovery-contract and measurement gates remain as stated
above. Live #911/#946 now describe their candidates and keep their issues OPEN.

Final static validation made single-invariant test names and setup/action/assert
boundaries explicit without removing atomic proof assertions. The last #910
correction prunes obsolete receipt accounting before any new byte admission:
a last source dropped under a read-only registry guard cannot leave the next
capture blocked by already-finished ownership. Two active tests cover capture
and selected-body admission in exactly one-cut-sized pools. The invariant map
refresh uses the final source checkpoint, including those tests and renamed
proofs; its links and hashes establish source traceability, not execution.
