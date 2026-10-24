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
