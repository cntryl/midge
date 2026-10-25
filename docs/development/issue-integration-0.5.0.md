# Local 0.5.0 issue integration

## Current local follow-up checkpoint (2026-10-25)

The active local integration branch is `work/0.5.0-issue-integration`, based
on `b5d1bc7b38efea90bde2e243d49d2978e2dcdf7d`. The issue-scoped commits
currently cover these slices:

| Commit | Issue slice | Focused evidence | Remaining issue gates |
| --- | --- | --- | --- |
| `a53da5fc` | #948 narrows Fragment fallback to enabled grandparent output cuts | Regression was red before the fix; work partition 11/11 and picker 2/2 passed; Jev favored the change (0.82) | Tier 2 write-amplification measurement |
| `8e8230c6` | #915 covers lost completion reconciliation at the same immutable upload key | Focused CloudStorage → HybridStorage production-path test passed (1/1) | Provider-specific checksum/multipart qualification and performance evidence |
| `4e0499f2` | #940 routes Strict spilled transactions through ticketed asynchronous fsync and ordered apply | Held-fsync progress/visibility, post-sync recovery and sync-failure tests passed (3/3); spill workspace has its own retained bounded budget | Crash campaign, stale/duplicate receipt and rotation/spill/shutdown cases, and performance measurement |
| `4cfbacb3` | #918 directly checks the legacy SST rejection boundary | Focused V4 fixture reader test passed (1/1) | Tier 4 footprint and #935 decode measurements |
| `f8c14a1c` | #914 removes hidden reads from the metadata request-count test double | The zero-read/two-PUT test was red on two inner GETs, then passed (1/1) with actual mock GET/range history and conditional-header assertions; Jev favored the test design (0.98) | Cold-start before/after and #937/#935 performance evidence |
| `19926136` | #978 asserts the spill-specific global applied cut after partial apply | Cross-family rejection plus fresh snapshot cut/key assertions passed (1/1); whole-spill restart recovery passed (1/1) | Pipeline/rotation matrices, AC1–3, crash and performance evidence |
| `a08f0616` | #905 covers a conflicting follower queued behind held Strict fsync | The follower waits without append or visibility, then conflicts after predecessor apply with no partial effects (1/1); Jev favored the evidence (0.65) | Hot-key throughput target and records-per-append/group-window measurements |
| `4d7a429f` | #907 moves CloudAsync WAL prefix sync, footer append, and footer sync onto ticketed writer-worker stages; event-loop ownership retains rotation and ledger publication | Three focused failpoint regressions passed (3/3), including control progress during both sync barriers, lease loss after prefix sync, and footer-transition failure; `cargo fmt --check` and `git diff --check` passed | Injected-provider latency/A-B evidence and completion ordering/reconciliation cases (#935/#937); no full suite or benchmark run yet |
| `83f705bb`, `1b49519a`, `6c7bc9af` | #904 pins intact synced-tail replay, bounds the floor while later Strict appends queue, and classifies damage around the floor | Post-fsync/pre-frontier Strict reopen regression passed (1/1); adjacent WAL recovery-floor tests passed (2/2); two-held-fsync event-loop floor-bound regression passed (1/1); floor-aware damage regression proves strict failure below the floor and physical truncation above it, including later valid frames (1/1); formatting and diff checks passed | Separate fsync/A-B accounting and explicit cadence sign-off |
| `508df52b` | #916 proves WAL catalog storage shrinks with a 10,000-segment history's unretired backlog | Encoded catalog sizes decreased at 10,000, 5,000 and 1,000 entries; anchor, head, next ID, remaining identities and round-trip decode held; full `wal::cloud_catalog::tests` module passed (23/23) | AC2 cloud-strict A/B/provider measurements and any justified publication interval decision |
| `efc76aa4` | #923 proves production orphan GC protects one SST shared across two pinned snapshot generations until the final release | Real CloudSimulated compaction and scheduled sweeps preserved both snapshot values and the shared SST after first release, then deleted it after final release; cold reopen and focused test passed (1/1, 91.02 s) | AC1 latency pairs and AC4 measured 10,000-snapshot unpin work |
| `f37cc4b1` | #927 proves a failed SST inventory probe retained under salvage does not advance the watchdog's `sst_inventory` progress | Jev selected the failed-probe regression (0.74); corrected stage-specific progress test passed (1/1); `cargo fmt --check` and `git diff --check` passed | 10,000-SST cold-start A/B, late/unknown mutation and identity/provider qualification |
| `58926c85` | #927 verifies salvage reconciliation when inventory results arrive in C/A/B order | Jev selected direct C/A/B completion-iterator coverage over a test-only worker hook (confidence 1.0); only missing B is removed and A/C order/identity are preserved; full `cloud_startup::cloud_recovery::tests` module passed (4/4) | 10,000-SST cold-start A/B, late/unknown mutation and identity/provider qualification |

Jev judgments informed the design reviews; implementation source and behavior
tests remain the correctness evidence. Parallel Jev-reviewed slices found
#947's proposed framing optimizations and #911's checkpoint failure handling
already present; their remaining throughput measurements stay open. The
#910 callback-admission retry is also implemented and its focused adapter
suite passed 4/4. The #926 implementation was already
present, so its eight focused ordering, proof, replay and recovery tests were
executed and passed without a code change. #949's implementation was likewise
already present; its focused writer/merge coverage is recorded in its issue,
while the compaction CPU-per-MB comparison remains open. Re-evaluation against
`4e0499f2` confirmed cloud seal needs a `WalSealTicket`-aware worker handoff
and loop-owned rotation. The async seal path now retains a ticket across
prefix sync, footer append and footer sync, then performs rotation and ledger
updates on the event loop after exact completion checks. Normal seal work no
longer blocks the loop on these barriers. Shutdown retains its bounded
synchronous final seal after admission stops. The focused regressions establish
the core transition and progress behavior; provider latency, reordered or
duplicate completion and caller-timeout reconciliation remain open acceptance
gates.

The live 0.5.0 milestone still has 28 open issues. #912 remains deliberately
deferred to preserve clock-correction recovery. No issue is closed by these
local commits, and no aggregate gate, benchmark campaign, PR or merge is
claimed at this checkpoint. The historical checkpoints below describe earlier
branches and qualifications; this section is the current local status.

The #916/#923/#927 acceptance audits found testable correctness gaps separate
from their remaining measurements. The new catalog history test measures
encoded size against 10k/5k/1k unretired entries. The shared-SST GC test holds
two actual snapshot source generations and waits for scheduled sweeps before
and after final release. The failed-probe test observes only `sst_inventory`
completion events, excluding legitimate local `sst_crc` work. A second
#927 regression feeds the actual reconciliation function a deterministic
C/A/B completion stream with a definitive missing middle entry; only that
entry is removed and original A/C manifest order is restored. Jev selected
this direct seam over adding a test-only worker hook (confidence 1.0). These
focused tests passed, but they do not complete provider or A/B acceptance.

For #904, Jev returned the choice field without selecting among the bounded
options on two code-grounded prompts (confidence 0.78 and 0.80); that output is
not a design decision. The issue explicitly requires implementation sign-off
before weakening exact SyncOpen cadence, so this branch preserves the exact
floor contract. The AC3 regression exercises an intact synced tail after the
post-fsync/pre-frontier failure point and proves Strict recovery replays the
value. A separate two-held-fsync event-loop regression covers AC5: queued later
appends cannot advance the persisted floor beyond the earlier exact sync
receipt. AC4 now verifies strict failure for damage below the floor and
truncation to the verified prefix for damage above it, including removal of a
later valid frame. Separate fsync/A-B accounting and explicit cadence sign-off
remain open; no cadence change is authorized.

The user requested this workflow on 2026-10-24: one local branch,
`work/0.5.0-issue-integration`, with small commits for the issues and one PR
after the batch is complete. This supersedes the earlier per-issue PR train
for this batch. The user's subsequent instruction authorizes pushing the
combined batch, reviewing it and merging one PR. Do not create individual
PRs or close issues whose acceptance evidence remains incomplete.

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
by a local commit. The batch is tracked in
[PR #983](https://github.com/cntryl/midge/pull/983), targeting `develop`.
That PR is the canonical final verification record: it records the final
aggregate and serial fault results, renewed production-source Sqrzl campaign,
exact-head hosted checks and merge/issue disposition. The checkpoints below
preserve earlier failed and partial results rather than replacing their source
boundaries with the final outcome.

The implementation-pass runtime/test source checkpoint was
`0bb7f09df6eaadd92ebba6870434acc30ba3a69d`. Its initial evidence map and checks
are historical; the final reference map is pinned to `ad89586c`, after the
review fixes recorded below.
Validation at that historical source checkpoint:

- `cargo fmt --check`: passed.
- `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings -D clippy::pedantic`:
  passed with zero warnings.
- `cntryl-tools validate-tests`: 4,495 declarations compliant, zero structure
  violations. This is source validation, not 4,495 executed tests.
- `cargo check --locked --manifest-path fuzz/Cargo.toml --bins`: passed;
  the two metadata replay targets report an unused shared fixture helper.
- `cntryl-tools validate-docs`: 71 Markdown documents passed. The final
  invariant inventory resolves all 65 IDs, 320 active test symbols, 356 row
  references and 2,144 assertion/helper anchors. All 204 source/supporting-doc
  SHA256 postimages verify; historical qualification objects are unchanged.
- Behavioral regressions added during the implementation-first pass were
  unexecuted at that checkpoint. Current PR qualification is recorded below.

### PR review and qualification in progress

The first aggregate behavioral run was stopped after confirmed worker-gate
deadlocks; it is failed/aborted evidence, not a passing suite. Initial native
Sqrzl CI also failed. Review is resolving the failures with active regressions:

- WAL upload fallback verification must use range identity for both HEADs
  and conditional range reads. Filesystem content-checksum HEAD identity is
  distinct. The regression proves initial ACK, immutable retry ACK, exact
  bytes and unchanged range identity.
- Compaction capture retries must select their exact pending compute owner
  before settling errors. The regression first lost a held sibling's input
  guard, then passed with that sibling's publication and cold recovery intact.
- Detached validation must retain the registered caller deadline. Its
  existing spill-deadline regression failed before the routing fix and passed
  with the original deadline preserved.
- Successful cloud replay must publish the installed applied-sequence cut.
  Real recovered point reads failed before this fix; recovery cancellation,
  checkpoint and cache-loss qualification continue against the corrected cut.
- Startup, publication, GC, flush and reusable compaction workers must
  inherit the existing test failpoint gate. A real timed-open/runtime request
  regression failed at lease acquisition before propagation and passed after
  it, without changing any deadline or default-build behavior.
- Fixtures are being updated for the separate binary committed pointer,
  owned manifest chains, whole-group concurrent visibility, bounded parallel
  inventory and metadata-only disjoint SST moves. Runtime contracts and
  restart assertions remain the basis of acceptance.

Native qualification uses only a dedicated local Sqrzl container, pinned to
`sha256:a10c5413dba980b852cac5e718c7a92a6642c6ec60ebb6a8222f2dca7d69f7c4`.
No live Azure qualification or production rollout is included. A green HEAD
request does not qualify authentication, conditional writes or recovery.

### Jev design review

The user requested Jev evaluation for complex decisions. Narrow review requests
resolved to `jev-1.14.0` and evaluated the selected-proof handoff, memory
ownership, spill-pressure assertions and startup scheduling separately. These
judgments prioritize investigation; source evidence and executed regressions
remain the acceptance gates.

- Selected terminal proof: retain compact evidence from the exact selected
  control cut, preserving lineage, horizon and already-owned guard checks.
  A local terminal phase alone cannot authorize clearing an intent.
- Compact representation: use a fixed UUID and an exact sorted digest slice.
  Review found that the earlier tree/string estimate did not derive the
  first-node allocation bound. Actual retained payload, private decoder space,
  partial clears and real callback tails require independent verification.
- Deferred receipt pruning: source Drop can encounter a held registry lock.
  Its receipt storage must retain its own adequate charge through pruning.
  Jev preferred adding a derived receipt allowance (confidence 0.89); that
  preference did not establish a bound for private standard-library nodes.
  Once review verified that stable Rust is not pinned to one implementation,
  Jev favored exact sorted receipt storage (confidence 0.98). The implementation
  charges its retained and replacement payload through the final prune.
- Typed decode admission: dense small intent variants invalidate a universal
  `8E + 4096` private-workspace argument. That observation does not establish
  that the old full `16E - credit - compact` charge overflowed on the same
  input. Jev favored narrow allocation-free shape preflight and admission in
  the existing pool. Actual decoder payload, nested fields, phase overlap,
  original deadlines and ordinary small-pool progress remain required proof.
- Spill pressure: assert the actual shared disk-capacity rejection, all ledger
  classes and released physical spill files. Accepted WAL occupies the same
  fixed pool, so a constant spill-file-size floor cannot represent total
  pressure. Retry staging only after the measured blocking WAL retires.
- Startup preparation: preserve real 600 ms aggregate timeout coverage and
  qualify the exact late-preparation race with the existing controlled shared
  scope. Reaching preparation before a real deadline cannot be guaranteed by
  an observer barrier. Controlled phase scheduling is separate evidence from
  elapsed-time qualification.
- Takeover: check healthy arrangement before the actual expiry wait, then
  check expired monotonic authority and real successor/restart outcomes.
  Runtime fencing also owns the health flag; stopping heartbeat workers does
  not freeze that shared flag. The confirmed fixture finding is #984.
- Fault-free timed fixtures: hold the existing read gate before constructing
  deadlines and through owned cleanup, excluding unrelated global fault
  configuration. Keep numeric latency/deadline assertions unchanged. Jev
  favored this scope; focused and full parallel execution determine whether
  the observed timing failures are resolved.
- Publication latency: Jev favored separating the active 100 ms scheduling
  probe, using existing controlled storage, from real-filesystem ownership and
  recovery schedules. Actual receiver timestamps disproved observer delay as
  the cause; physical build, upload and checkpoint stages slowed under shared
  host contention. No durable sync, checkpoint cadence or numeric deadline was
  changed to meet the timing assertion. Hardware latency remains a separate
  open #910 gate.
- Scoped tracing: retain a second ordinary non-global recorder so an unrelated
  callsite registration cannot cache the sole scoped recorder's thread-local
  absence. The controlled regression first performs an actual unscoped flush,
  then requires the original scoped API spans. Production tracing and
  dependencies are unchanged; #985 records the reproduced failure and fix.

One hypothetical GET-gate review lead was dropped. Fresh lease validation still
performs a GET before publication; source inspection instead found that the
fragment fixture had never published its starting manifest remotely. The fixture
now proves that real baseline before holding the original provider operation.
No extra production GET, cache defeat or phase framework was introduced.

### Executed Sqrzl qualification

Production source `1d1a2403ef3ef06ed90eaedca88d75a7b8f6b782` passed the
following commands against the dedicated pinned Sqrzl container. These results
qualify that historical source only. Subsequent runtime fixes and renewed
qualification are recorded below; exact-head CI remains a separate merge gate.
The configured local emulator credential is omitted here.

| Command | Result |
| --- | --- |
| `cargo test --locked --lib --features sqrzl-tests storage::providers::qualification -- --test-threads=1` | 8 passed, 4.08 s. Optional live S3 configuration was absent; no live-provider claim. |
| `cargo test --locked --test cloud_provider_engine_qualification --features sqrzl-tests,failpoints -- --test-threads=1` | 23 passed, 502.95 s. |
| `cargo test --locked --test fault_injection --features sqrzl-tests,failpoints wal_authority_regressions::should_preserve_successor_strict_commits_when_predecessor_reopens_old_cache -- --exact` | 1 passed, 18.72 s. |
| `MIDGE_DISCOVERY_PROFILE=sqrzl cargo test --locked --test discovery_model --features sqrzl-tests` | 6 passed, 177.09 s; 128 resident/spilled histories across the configured protocol front doors. |

The engine campaign retained original memory/disk limits and verified all
762 and 1,523 records in its two complete-local-disk-loss profiles. Its actual
receipt files identify the production revision and `verification_complete`.
The image remains
`ghcr.io/sqrzl/sqrzl-emulator@sha256:a10c5413dba980b852cac5e718c7a92a6642c6ec60ebb6a8222f2dca7d69f7c4`.
Local command/context logs are `/tmp/midge-050-native-final-context.md` and
`/tmp/midge-050-native-final-*.log`; engine receipt artifacts are retained under
`/tmp/midge-050-native-final-artifacts`.

The broad local run at this checkpoint passed 3,632 library tests and failed
five; it also exposed one scoped-span fixture failure. Those are failed
aggregate results. Accepted-worker settlement, fault-free deadline isolation
and the scoped-dispatch correction passed their focused regressions; the full
observability target then passed 59/59. The remaining publication-latency
fixture separates its active controlled-storage 100 ms scheduling probe from
real-filesystem held-owner and recovery checks. Shared-host physical disk
latency is an open performance gate; neither a quiet serial control nor the
reference inventory qualifies it. Final aggregate and exact-head CI results
must be recorded before merge.

The publication fixture correction is code checkpoint
`d55a621776d9c6c9478a9d63e502703660076115`. Its default-parallel group passed
7/7 in 18.00 s. The three controlled-storage acknowledgment measurements were
7.96, 8.19 and 8.76 ms. The three real-filesystem schedules took 176.61, 182.65
and 184.44 ms, with exact ownership, pre-release cold recovery and post-release
authority assertions intact. These results qualify separate scheduling and
recovery claims; they do not satisfy #910's real-hardware 100 ms acceptance.
The evidence map is refreshed against this code checkpoint; historical
qualification objects retain their original source and execution boundaries.

The existing #953 footprint regression was executed unchanged at `d55a6217`:

```sh
cargo test --locked --lib --features failpoints \
  metadata::codec::tests::should_reduce_manifest_bytes_when_encoding_twenty_thousand_bounded_files \
  -- --exact --nocapture
```

It passed (1/1, 0.31 s), encoding the same 20,000-file manifest as 6,377,947
JSON bytes and 4,479,374 binary bytes, then decoding all 20,000 entries. The
stdout artifact is `/tmp/midge-050-manifest-20000-footprint.log`. This records
format footprint, not runtime performance or #952's fuzz compute budget.

The subsequent full local command,
`cargo test --locked --workspace --features failpoints --no-fail-fast`, finished
at code checkpoint `d55a6217`. Every binary, integration and doc-test target
passed. The library passed 3,637 and failed three assertions that original
compaction inputs had already been physically deleted. The operation's phase,
caller outcome, intent and authority checks had passed. This remains failed
aggregate evidence, recorded in `/tmp/midge-050-workspace-final-candidate.log`.
These fixtures use background captured publication. An expired final mirror
can retain an unresolved cut that correctly protects physical objects; joining
existing workers does not retry a retained delete obligation. Seed mirror/name
continuations must also finish under their original authority before the
fixture rebinds its provider. The correction observes actual deferred work
without extending the original caller deadline, changing its outcome or
permitting another publication after expiry.

Commit `c5226995` passes the full phase/deadline group 13/13 (21.72 s). Success
cases observe real GC retries and joins. The final-clear timeout case first
receives a genuine successful `ManifestPublished` result and proves the selected
retired identities. A scoped observer holds the existing manifest writer lock
only until actual local and remote inputs disappear under the original 5 s
caller clock; this establishes its stated live-GC prerequisite before admitting
the deliberately unresolved clear cut. It adds no global callback, backend or
new publication. Existing caller/phase, earlier-authority retention, exact row,
resource-release and no-new-upload assertions remain active. Jev evaluated
this ordering using the corrected captured-cut source facts.

The explicit serial fault command then passed 181 and failed 16; that is
failed evidence in `/tmp/midge-050-final-serial-faults.log`. It exposed stale
physical-compaction and acquisition-format fixtures, missing integration of
the existing before-enqueue timeout seam into sequenced submission, and two
implementation defects requiring another production checkpoint:

- #986: startup intent repair installed validated SST authority while its
  applied visibility cut remained at the old base. Commit `63c9d9d3` raises
  visibility only to the installed manifest proof after complete repair;
  it does not change the allocation-floor helper. The original public salvage
  and strict checkpoint-failure regressions pass 2/2 (0.42 s).
- #943: failed compaction handed unpublished output names to ordinary GC,
  which correctly requires a durable retired-object identity. The active
  partial-upload regression requires retaining every input while reclaiming
  the exact non-authoritative output. Its repair must retain the original
  prepared proof and use conditional owned deletion, preserving uncertainty
  and replacement protection.

Jev evaluated the installed-cut boundary and real before-enqueue seam
independently, favoring those narrow repairs. The full `commit_timeout` target
passes 3/3 (2.95 s): both resident/spilled unchanged failures preserve `Timeout`
and restart absence, while an admitted late response remains `OutcomeUnknown`.
The prior Sqrzl runs still describe production revision `1d1a2403`; renewed
native qualification is required after these additional production fixes.

Commit `e86f2c6f` performs failed-output cleanup on the existing joined compute
task using its exact prepared provider proofs. A separate bounded cleanup
obligation retains uncertain residue and checks current write authority;
it does not renew the caller's publication deadline or weaken ordinary GC's
durable-original requirement. The real replacement-race regression passes,
and the held-cleanup case verifies caller expiry, retained writer ownership,
shutdown and exact restart. Physical-compaction fault qualification passes
28/28 (35.34 s); the L0 pressure case passes 1/1 (1.37 s). Overlapping anchors
force physical rewrites and all original acknowledged rows remain checked.

The existing checkpoint no-space seam is also preserved at the captured
snapshot's actual pre-rename staging boundary (`6de9598f`, #911). Its regression
proves a real checkpoint is due, retains the old snapshot and durable journal
on failure, and verifies all original rows plus the latest anchor on restart.
Both acquisition-lock crash cases use the actual V4/Pending acquisition prefix
and pass 2/2 (0.34 s); production acquisition creates the separate pointer.

The renewed local native campaign's production source is
`f27419f92d5c1cf06524e6aac882cfb6b7ee12b7`; later changes through the prior
code/reference checkpoint `1c74f03a818ba15611bce675e71a6e0811760d19` are tests
and documentation. The following static results belong to that checkpoint.
Strict all-target/all-feature pedantic Clippy passes with zero warnings
(20.84 s); all-target/all-feature check and build pass (22.91 s and 59.83 s).
Formatting passes, 4,542 test declarations are structurally compliant and all
72 Markdown documents validate. These static results do not establish runtime
qualification. Final aggregate, serial fault and renewed Sqrzl execution, and
the separate exact-head hosted merge gates, are recorded on PR #983.

The fresh aggregate at `d771b179` passed 3,639 library tests and failed two
fault-free fixture waits during unrelated global fault scopes. All phase and
overlap cases passed. Commit `1c74f03a` acquires the existing read scope before
shutdown/debt setup and clocks, carries its worker context onto the owning
thread, and settles already-accepted local compaction work before final actor
assertions. Exact focused cases pass 1/1 each (1.67 s and 0.16 s). Original
10 s/1 s/100 ms/3 s bounds and authority/manifest assertions remain unchanged.
The failed aggregate remains attributed to its source; final full-suite hosted
verification is recorded separately on PR #983.

### Final hosted review correction: per-family flush retry order

At PR head `9a87959f`, hosted Core passed the entire workspace test step and
then passed 197/198 serial fault tests. The remaining unchanged public
`should_preserve_flush_identity_order_when_oldest_publication_retries` case
failed with a correctly rejected regressing manifest frontier. Readiness
filtering had excluded an oldest `RetryPending` immutable during its 10 ms
backoff, allowing a younger queued generation from the same family to publish
a higher frontier first. The local 512 KiB younger commit often exceeded that
backoff and hid the interval. This confirmed defect is tracked in #987.

Commit `4c7f25f64d5adac54ace38deb4ac7f2061ee2220` selects each family's oldest
outstanding generation before phase/readiness/actor eligibility. Dispatch,
due-work and retry-deadline queries use the same family heads. Other families
remain independently schedulable; clocks, identities, deadlines and the
nonregressing-frontier guard are unchanged. Jev favored this narrow decision,
and independent source review confirmed the append/remove queue ordering.

The deterministic real-worker regression was RED with frontier2 over the held
older generation (expected0, 0.20 s), then GREEN (0.32 s) with actual other-family
publication, exact retained identities, original caller deadline and public
Engine reopen. The original public retry/restart case passes unchanged
(0.48 s), the flush pipeline group passes 40/40, and runtime state passes 86/86.
Strict all-target/all-feature pedantic Clippy passes (19.86 s).

That is the FIFO code checkpoint. Earlier local pinned-Sqrzl
campaigns remain attributed to `f27419f9` and their actual clean discovery
head `9a87959f`; they are not relabeled as results for the FIFO correction.
The existing hosted provider jobs each run provider contracts, full engine,
takeover and discovery commands against Sqrzl. Their final exact-head results
and observed container digest are recorded on PR #983. The workflows are
unchanged; their `latest` image and the separately pinned local campaign are
distinct evidence boundaries.

### Final callback-admission and fixture qualification

At hosted head `b826de1d`, all three provider jobs passed their complete native
campaigns for the FIFO runtime. Core passed 3,642 library tests and failed one
128 KiB metadata capture accounting case. Its source charge was 6,586 bytes;
the prior physical completion adapter still correctly owned another 65,536
bytes when an exact GET needed its own adapter. The local admission refusal
was incorrectly converted into a provider protocol error, losing the existing
owning-pool contention identity. This confirmed liveness finding is tracked
under #910; releasing the old charge early or enlarging the pool is invalid.

Commit `ca5608695457780be0570c7877f7176950a27e09` preserves a typed, definitely
unsubmitted local read admission error across cloud/storage boundaries.
Only an exact GET blocked by its own reservation pool can retry, under the
same absolute deadline. Its accepted provider timeout is sampled after actual
adapter admission. Static oversize, foreign-pool and genuine protocol failures
remain terminal; PUT/CAS and whole-publication retries are unchanged.

The deterministic regression holds an actual generic completion adapter's
64 KiB tail, reproducing the hosted refusal before the fix (RED, 0.06 s).
After release, exactly one GET is accepted with a budget no greater than the
original deadline's remaining allowance; no extra PUT occurs. The original
128 KiB cap and physical ownership remain unchanged. Expiry, static oversize,
same-pool release races and foreign/protocol classification are covered.
Metadata-cut tests pass 17/17 (1.63 s), existing read-deadline tests pass 4/4
(0.07 s), and strict all-target/all-feature pedantic Clippy passes (4.92 s).
Accounting assertions boundedly observe the real final callback tail before
requiring exact source-only or zero usage; they do not release that tail.
Jev evaluated the narrow admission decision, followed by source/test review.

Two additional fixture slices preserve their original bounds and assertions.
`755c3a03` scopes all four fault-free publication-order scenarios before timed
worker/provider waits and records/reloads three seeded L0 identities before
real move journals reference them. Those groups pass 4/4 and 5/5.
`8a22aa4b` similarly scopes the two direct manual-flush scenarios through owned
cleanup (2/2, 3.85 s). These repairs do not alter runtime behavior or move/replay
validation. Their preceding failed local aggregates retain their source/log
boundaries; the final full-suite result belongs to hosted exact-head CI.

The final production checkpoint is `ca560869`. The later test-only checkpoint
`812900422b207cd5adcd41b23c17bcd59b416a31` also repairs two pressure scenarios
that treated a compute notification as an already available native ticket.
They wait for that same owned ticket under the original combined 10 s budget,
then dispatch without an implicit fairness turn before the intermediate
publication assertions. The original explicit next fair turn still admits
the second job. Exact one/two publication counts, critical intermediate L0,
active successor and final admission/policy assertions remain unchanged.
Both persisted seed layouts are positively reloaded before launch. Target and
neighbor tests pass 1/1 each; Jev evaluated the ownership/observation ordering.
`ad89586c` applies the same owned-ticket observation to the shared phase fixture:
the sole compute notification and ready ticket share one original 8 s bound,
then the notification is dispatched once. All 13 phase cases pass (21.19 s),
with their caller 5 s budgets unchanged. Production already polls pending
compute tickets every millisecond; these direct fixtures now observe that
ownership boundary rather than assuming an advisory wakeup is terminal.
This is the final code/reference checkpoint; production bytes remain at
`ca560869`. Final
aggregate, serial fault and complete hosted native results, including the
observed Sqrzl image digest, are recorded on PR #983. Earlier successful
campaigns remain attributed to their actual sources.

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
campaigns, assurance compute budgets and qualification of the eventual release
SHA remain due. The executed Sqrzl results above qualify their recorded runtime
source; they do not replace continuous-lag benchmark or release evidence.
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

The user subsequently deferred destructive TTL reclamation (#912) to the next
batch to preserve clock-correction recovery. Its implementation is excluded
from the current PR; the issue remains open.

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

Added complete/disjoint and legacy/incomplete proof regressions. The public
Engine snapshot/reclaim regression passed and inspects published SST contents
before and after the old snapshot is released; it also verifies a disjoint SST
remains selected and readable. The production planner plus real-SST execution
test for incomplete unselected bounds passed. For mutation sensitivity, forcing
range GC eligible despite incomplete bounds failed at the expected planner
assertion; restored code passed. The required cloud WAL-retirement regression
passed with failpoints enabled. These focused results cover AC1-AC4; performance
evidence remains separate.

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

### #912: destructive TTL reclamation deferred

The user chose to preserve recovery after correcting an erroneously advanced
clock. #912 remains open for the next batch. This PR removes its destructive
expired-value drop, captured compaction-clock/source-retention capabilities,
expiration-driven picker, early maintenance timer and TTL exception to trivial
moves. Point/range tombstone GC and the post-start conflict-history horizon
remain independent and active.

Compaction preserves the latest raw TTL value and its expiration. Reads apply
their snapshot read time; reopening after clock correction can expose that
same value again without resurrecting an older non-TTL version. The original
TTL and clock-skew recovery tests remain unchanged. An active physical-output
regression also preserves expired values across snapshot horizons and proves
both expiration masking and corrected-time reads.

Writers, flush publication, metadata conversion and independent SST summaries
still carry the optional advisory earliest expiration. It is inert metadata;
it selects no TTL compaction and authorizes no deletion. Keeping that coherent
field avoids an unrelated binary layout change. The two new physical/idle
reclamation candidate tests are removed with the explicitly excluded feature.
Future destructive reclamation requires a recovery-safe retention design or
an explicitly approved contract change before implementation and measurement.

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

The #912 expiration-driven maintenance follow-up is also deferred by the
clock-correction decision above. Ordinary compaction retains its existing
periodic cadence and size/count/repair admission; advisory expiration alone
does not schedule a rewrite.

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
snapshot retention/restart, required delta history, pointer replacement,
capture exclusion, and predecessor fencing across a real lease takeover. The
new AC2 regression failed when the epoch checks were removed, then passed with
them restored. `cargo test --lib metadata_retention -- --nocapture` passes all
seven tests; AC3's GC-enabled crash campaign remains open.

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
interval rather than resetting cooldown state; its execution is recorded in
the focused retention test run above.

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

The seed-equality test and FORMAT9/SSTV5 fixture verification pass, including
Engine reads of fixture data. The three new decoder targets each passed a
61-second smoke and the documented 1,800-second release fuzz budget with no
crash artifacts. The seed-equality and lease auto-trait tests both passed on
2026-10-25; no `unsafe impl` remains in `src/lease`. Restart/conformance,
performance and system-footprint qualification remain pending. Existing
footer/range and metadata/journal fuzz targets were not part of this budget
campaign.

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
The public `cloud_core` held-snapshot regression now exercises the scheduled
CloudSimulated sweep: after compaction retires the predecessor SST, the pinned
snapshot continues reading it while a real sweep increments retained-object
metrics without attempting DELETE; after releasing the final pin, a subsequent
sweep reclaims the remote SST. Removing the sweep deletion gate makes this
test fail (two DELETE attempts while pinned); restoring the gate passes in
61.47 seconds. This supplies AC3's public read/sweep/reclaim evidence for
CloudSimulated only. AC6's sweep-enabled crash campaign remains open, as do
native-provider qualification and the general SST orphan and catalog-anchor
WAL rediscovery evidence.

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
public Strict commit/reopen. The focused post-fsync/pre-frontier test now also
proves intact synced bytes replay when the exact floor update fails. Exact
floors, encoding and recovery semantics stay unchanged. The focused test passes;
separate fsync measurements and remaining recovery-window cases are pending.
The metric builder uses conservative empty defaults while always installing
actual runtime health; focused helpers keep hard function limits.

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

### #979: mutation evidence for added assurance regressions

At source `736beb83391663eb154b69f53088c39d3e81215b`, each of the nine
assurance regressions added for #979 failed under a relevant source mutant.
The two WAL version tests rejected flipped header and payload version guards.
The runtime-owner test rejected premature applied-cut publication; the live
lease read test rejected unconditional cloud read fencing. Dropping positive
recovery progress events independently failed the remote replay, inventory,
journal replay and caller-scope tests. Crediting progress at range-read start
failed the held-I/O test, and ignoring the phase-generation check failed the
caller-scope isolation test. All mutants were reverted in an isolated
temporary worktree.

After restoration, the two version tests, runtime-owner test, live-lease read
test and all five recovery-progress tests passed. This closes #979 AC1; the
previously recorded map refresh and test-symbol resolution satisfy AC2. The
mutation checks establish only the named regressions' detection of these
specific defects.

## Cloud publication and reclamation follow-up (2026-10-25)

Local branch `work/cloud-publication-reclamation` groups #916, #913 and #943
under the cloud publication/reclamation work, with #907 as the related
publication prerequisite. This patch adds tests and evidence only; it does not
claim #907's remaining performance gates. The first #916 commit adds a
four-segment event-loop regression: a transient failure leaves the
prefix closed while later ACKs arrive in reverse order; the test reads the
authoritative remote catalog to verify no later segment is published, then
checks all four entries after the retry ACK. The focused test passes. Existing
worker callback reordering and provider-cloud retry/recovery tests remain
separate complementary layers.

The public #943 scheduled-sweep snapshot regression now cold-reopens after the
old SST is reclaimed and verifies the selected metadata still recovers the
current value. It passes on CloudSimulated. The metadata retention suite (7
tests), orphan suites (18 tests), hybrid-persistence suite (108 tests),
four-ACK prefix test, retry campaign, and focused failpoint authority tests
also pass. `cargo fmt --check` passes.

Branch `work/cloud-reclamation-crash-campaigns` adds a sweep-specific crash
campaign for #943 AC6. It compacts a real SST, copies those bytes to a valid
unselected sequence-zero SST identity, and aborts a child process immediately
before the scheduled sweep's identity-conditional delete. The orphan remains
after the crash; a cold successor reads the selected value, retries the sweep,
and reclaims the orphan. The test observes positive LIST/probe/delete metrics
within the shared SST/WAL per-turn budgets. A generic delete failpoint first
proved too broad because startup cleanup could fire it; the dedicated sweep
boundary removed that false positive. The focused failpoint test passes.

The same branch adds a #913 AC3 checkpoint-retention crash slice. A real
CloudSimulated engine publishes 20 strict, flushed metadata versions; a child
process reaches the validated identity-conditional delete boundary for the
oldest version and aborts. The object remains after the crash. The successor
recovers and reads every committed key, resumes collection, removes the old
version, and reads every key again. Jev scored this evidence strong for the
deletion-interruption slice; existing unit tests separately cover pointer
change before delete and replacement identity. Pointer-advance checkpoint
selection is implicit in the published history and production collector path,
not asserted as a generation identifier. This does not cover publication racing
collection, uncertain authority reads in a crash campaign, or ambiguous delete
responses. Two focused collector regressions additionally hold a real pending
metadata capture and corrupt the selected pointer: retention performs no
deletes while capture ownership is pending or the authority read is uncertain,
and the exact committed pointer restores readable history. Removing both
pending-capture guards makes the capture regression fail because the candidate
is deleted. These unit checks complement but do not replace the remaining AC3
process-crash cases.

Branch `work/metadata-retention-crash-acceptance` completes those AC3 cases.
One child aborts after immutable chain upload but before pointer CAS; restart
reads the last committed checkpoint and keeps the higher unselected object
unselected. A second child injects an `Indeterminate` selected-authority read
and aborts at the collector's error boundary; an eligible old version survives,
then a successor reopens every committed value and resumes cleanup. Together
with the merged post-pointer-advance delete crash, these tests cover in-flight
publication, checkpoint recovery, uncertain authority and safe reclamation.
Jev favored closing #913 against its written AC1-AC3. This branch is not yet
merged; issue closure is pending its exact-head gates.

These checks do not complete #907's injected-latency/A-B gates or #916's cost
and catalog-size measurements. #913's metadata-generation GC crash acceptance
remains open. No live-provider qualification is claimed.
