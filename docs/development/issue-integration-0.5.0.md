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
