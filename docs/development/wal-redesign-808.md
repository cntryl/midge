# WAL redesign regression inventory (#808)

The original baseline inventories the 14 audit classes as 17 executable tests,
plus two supplemental allocation/history cases and three cases the crash
simulator found (`torn-unsynced-tail`, #817; `recovered-tail-not-durable`,
#831; `lost-buffered-delete`, #835). It supplies a baseline runner and CI
coverage. Subsequent serial PRs implement the stages below. The registry's
`baseline` column records the current expected result: completed stages are
GREEN, while unresolved stages keep their original RED assertion markers.
The original baseline commit and captured RED evidence remain the historical
reference. Refs #808.

The earlier simulator and shipping adapter changes were removed after review
found false-pass and false-failure risks. Filesystem fault injection and crash
state fidelity, provider late effects and ABA identities, ordered recovery
oracles, stable replay identities, and engine campaigns must be qualified in
subsequent serial PRs before serving as acceptance evidence. The removed draft
is retained in Git history at `33cc1a72e10282eba32e8cfb090aa2fea7535f77`.
The crash-device-dependent #811 fixture follows the ownership work; #811 stays
open. Production changes are described by the implemented stage contracts below.

## Implementation sequence

The redesign lands as serial pull requests. `fixing_pr_stage` in the registry
names the stage whose pull request turns a case GREEN:

| Stage | Scope | Issues |
|---|---|---|
| 1 | Injected storage root: every engine file effect goes through one `Fs`, then the qualified crash simulator | #813, #814, #815, #816, #827, #825; simulator #817 (crash filesystem), #830 (object store, scheduler), #831 (oracle, local campaigns), #837 (fixture re-targeting, campaign budgets), #835 (lease control), #838 (cloud campaigns) |
| 2 | Segment ledger, linked segment format, sequence reservations, contiguous catalog publication | #818: #841 (contiguous publication, upload retries), #848 (replay takes segment lists), #842 (segment ledger, prefix-only retention), #843 (linked segments, reservations), #844 (chained catalog, recovery planner) |
| 3 | Per-family flush frontiers; replay and retention cut over from SST coverage proofs | #819 |
| 4 | Ownership, lineage and storage-enforced fencing | #820 |
| 5 | One salvage rule | #821 |
| 6 | Typed commit outcomes, admission envelope, one transaction rule | #822 |
| 7 | Invariant catalog and durability contract | #823 |

## Stage 3 flush authority (#819)

FORMAT 8 requires a canonical frontier row for every known family, including
permanently retained dropped IDs. A flush journals its SST allocation, `AddSst`
and `AdvanceFlushFrontier` in one atomic batch. Replay selects complete
transactions by commit sequence against these frontiers; it still validates
skipped records. Missing family authority is corruption under both recovery
policies. SST contents and tombstone presence do not decide WAL retention.

Local retirement first persists the new ledger `log_start`, then queues physical
deletes. Cloud retirement uses the frontiers in an immutable, committed shared
generation, commits that generation's ordinal in catalog `retired_by`, and then
queues identity-conditional deletes. Shared ordinals are independent of manifest
journal edit IDs. Startup verifies the committed generation covers `retired_by`
before hydrating metadata or admitting WAL history. Simulated cloud engines use
the same shared-store lease and generation authority as provider engines.

Physical cloud cleanup has a bounded retry queue. It reserves capacity before
retiring catalog entries and retains the original object identity across
transient failures. Retries are process-local: a crash or an unconfirmed catalog
CAS reply can leave remote objects below the committed anchor. Recovery ignores
those objects; ambiguous CAS results never authorize deletion. Persistent orphan
reclamation is outside this stage.

Decisions that refine the #808 target design:

- The WAL ledger is a local journal, `wal/LEDGER`, in every mode. Cloud startup
  replaces the mirrored manifest files with the shared generation, so a
  holder's unpublished tail cannot live in the manifest. The catalog still holds
  the shared published prefix.
- Retirement is prefix-only: it advances `log_start` and never deletes a segment
  behind a retained one.
- There is no `control_through` frontier: column-family DDL is recorded only in
  the manifest, never in the WAL.
- Contiguous catalog publication lands first in stage 2 (#841). A verified
  readback only marks a segment uploaded; the pure `wal::publish` selects the
  longest contiguous run of uploads past the catalog head, and one catalog CAS
  publishes it in order. A refusal no retry can fix fences the writer, never
  an overwrite: a remote object with other bytes, a catalog entry that
  conflicts, or a run the catalog cannot append in order. A failed WAL upload
  stays queued and retries with exponential backoff capped at one second until
  it succeeds or the writer is fenced; after three failures each further
  failure fails the waiting callers with an unknown outcome (`Timeout`) while
  the retries go on. Uploads verified but not yet published count against the
  upload queue's limits, so writes stall behind a segment that keeps failing.
  The queue is WAL-only.
- Segments are linked (#843). A header names the segment's id, writer epoch,
  first sequence and the link of the segment before it; a footer seals the
  segment with its own link: id, last sequence and an xxh3-128 digest of the
  bytes before the footer. Recovery admits only the chain the ledger (or the
  catalog) names, so the writer epoch is a segment fact and records no longer
  carry it.
- Rotation takes the digest from the writer's running hash first, then
  creates the next segment linked to it, then writes and syncs the footer,
  then records `Rotate`. A failed create leaves the segment open and
  writable; a crash after the footer leaves a footered segment the ledger
  still calls open, which recovery accepts as sealed.
- Recovery seals the tail it replayed before the event loop serves anything:
  a footer and fsync when it holds records, `Reopen` with a new segment when
  it holds none, so no sealed segment is empty. Then a `Reserve` edit makes
  the next block of sequences durable before any is handed out, and the
  sequence continues past every earlier reservation, so a commit whose
  unsynced record a power loss dropped never has its sequence reused. No
  record holds the skipped sequences, so once the cloud holds every record
  before the open segment its frontier extends to the open segment's first
  sequence.
- Genesis records the holder identity and the first open segment. A fresh
  cloud cache starts at the catalog's `next_segment_id`, past every id any
  writer published, linked to the catalog head.
- Salvage drops a suffix of the ledger with `SetAside{from, cut, open}`, after
  copying the dropped files into `wal/salvaged-<millis>/`, truncating the cut
  segment to its replayed prefix and sealing that with a footer. The new open
  segment links the cut. Nothing is renamed.
- The catalog is a chain (#844, extended in #819), like the ledger. Its `anchor` is
  the link of the last segment it retired, or the genesis link. Each entry,
  `{epoch, prev, facts, size_bytes, crc32c}`, links the one before; epochs
  never fall; `next_segment_id` passes the head and never falls. The head is
  the newest entry's link, or the anchor. The object keys keep their `v1`
  names. Stage 3 uses `format_version: 3`, adds complete sealed segment facts
  (including per-family sequence maxima), and records `retired_by` as the shared
  generation authorizing its retired prefix. Any other version is a
  `CompatibilityError`, never repaired from the mirror.
- The catalog owns its shape. Its mutators are pure and refuse whole
  (`CatalogRefusal`): `fence_to` hands it to a later epoch, `append_run` must
  chain onto the head (`publish::Refusal::Unchained`), `retire_prefix`
  retires only the oldest entries, moves the anchor and keeps
  `next_segment_id`, and `set_aside_suffix` drops a suffix and raises
  `seq_floor`. Every mutation takes one read-modify-CAS path,
  `mutate_wal_catalog_within` (#858). A rule refusal writes nothing:
  `refusal_error` maps another epoch to `Fenced` and anything else to
  `Corruption`. A lost compare-and-swap re-reads the primary without
  repairing it: `Fenced` when a later epoch owns it, `Busy` otherwise.
  Metadata bootstrap refuses any catalog that is not pristine, because a
  catalog whose entries all retired is still history.
- Recovery planning is pure (`wal::planner`, #844). Startup only probes the
  segments (object HEAD, checksum and inspection), cuts the open member and
  assembles the sources; the planner decides and derives lineage from the
  catalog and ledger itself. Replay reads through `ReadFs`, whose `ReadFile`
  handles expose only reads and length queries. `lineage` places a cache's
  ledger against the catalog:
  - **Fresh:** no ledger. The catalog replays and a new history starts after
    its head.
  - **Joined:** the ledger's chain holds the catalog head, and the catalog
    never handed out the id of the ledger's first member after that head: a
    successor may have published that id and salvage set it aside since.
    The catalog replays, then the cache's tail.
  - **Behind:** the catalog head precedes the ledger's anchor and is not in
    its chain: the catalog lost history this cache retired. When the
    catalog's floor covers the ledger's anchor, the catalog set that history
    aside itself and the cache rebases, even in strict recovery; otherwise it
    is a hole.
  - **Diverged:** anything else: another writer extended the catalog. The
    cache belongs to a writer another one succeeded. It sets its local WAL
    aside, replays only the catalog and continues past it, since an
    unpublished tail is not history once another writer has taken over.
- `plan` admits the catalog's entries, then a Joined cache's tail, to one
  `format::Chain` from the catalog's anchor. The first position that is
  missing, damaged or unchained is the hole. Strict recovery refuses it.
  Salvage replays the prefix before it, sets the rest aside (the catalog
  suffix and the local members from the hole) and lifts the sequence floor
  above all of it. Replay checks each published object against its entry
  (`Expectation::Published`): the header's epoch and previous link, and the
  link its footer seals. Ledger members at or below the catalog's anchor are
  retired history: they are neither replayed nor owed to the catalog, and
  startup retires them locally. The plan carries one history action,
  `Genesis`, `Keep`, `SetAside` or `Rebase`, that startup only executes.
  `planner::local` plans a local WAL from its ledger alone.
- A cataloged segment whose link differs from this ledger's member at the
  same id is a fork, never Joined: the local tail is set aside and strict
  recovery opens on the catalog's history, as #843's failback does.
- `commit_recovery_plan_within` makes a cloud plan durable in one order.
  First one catalog compare-and-swap drops the suffix from the hole and
  raises the floor over everything set aside; then the ledger carries out
  the history action. The failpoint
  `midge::recovery::after_salvage_floor_before_quarantine` sits between
  them. A crash there leaves either the same hole in the cache's own tail,
  which the next salvage open completes, or a catalog the ledger no longer
  joins: Behind it, with a floor covering the gap, when the catalog dropped
  history the cache had already retired, or past it when the catalog dropped
  segments the cache still holds. The next open rebases onto that catalog,
  strictly, and the floor already covers what was set aside.
- The #844 discussion proposed treating a cataloged id in
  `[log_start, open)` that is not a ledger member as set-aside history. That
  rule is not implemented literally: after an idle-restart `Reopen`, it would
  retire a successor's strict-acknowledged segment. The catalog CAS before
  the ledger edit and the Behind and takeover rules replace it. Under catalog
  v2 its precondition, a catalog that published a segment past a local-only
  one, cannot occur, because `append_run` requires each new segment to link
  the catalog head.
- A transaction is single-family: its frames carry the family, and a batch
  spanning families is refused before any sequence is allocated.
- Files the ledger does not name are orphans. The writable open sweeps them,
  and fails closed on one holding data above every id the ledger handed out.
- An idle column family pins every later local segment until a flush covers
  its own: prefix-only retirement accepts that growth. Cloud acknowledgements
  that arrive out of order keep later local copies until the prefix catches up.

## Named regressions

[The registry](wal-redesign-regressions-808.json) assigns each test to its fixing
stage and records a specific assertion for each expected RED. Unresolved
regressions remain active and fail ordinary test execution. The two retired
local/cloud-copy regressions remain active after #805. The other baseline
failures remain assigned to the ledger/authority, outcome/transaction,
frontier/retention, or salvage stages in #808.

The retryable-fsync fixture requires the injected sync failure to fail the
commit with its specific cause before checking restart behavior. The existing
unit test that expects `NoSpace` describes the current implementation; the
outcome stage must update both contracts together when typed outcomes ship.

The non-corruption salvage fixture runs the engine on a crash device (#837).
Each WAL read of a salvage open fails in turn with a plain I/O error, on its
own copy of the device, so the fixture does not depend on how many reads
recovery makes before replay. A healthy control verifies every acknowledged
write, and every faulted read must fire. Both `Ok` and `Err` from a faulted
open must preserve those values on a later healthy open.

The failed-unlink fixture injects a `remove_file` error on the older retired
segment through the same device, so it runs on every platform (#837).

The segment ledger (#842) turns `restarted-segment-id` and
`failed-local-unlink` GREEN. Ids come from the ledger, which never hands out
one it recorded, and the ledger records a new log start before a file is
unlinked, so a file whose removal fails is never replayed. The matching
campaign detections became controls.

The chained catalog and the recovery planner (#844) turn `predecessor-cache`
GREEN in stage 2. A segment id alone cannot tell a predecessor's stale tail
from a successor's history once the successor's catalog entries retire. The
catalog's retained anchor link and `next_segment_id` can: the predecessor's
ledger does not hold the catalog head, which links the successor's history,
so its cache is Diverged. It sets its unpublished tail aside and replays only
the successor's history. The case needs the `sqrzl-tests` feature and runs in
`integration-s3.yml` and the baseline runner with `--with-sqrzl`.

Linked segments (#843) turn `inverted-epoch-history` and
`recovered-tail-not-durable` GREEN. Replay follows the ledger's chain, so a
segment written by an older epoch is never skipped as stale behind a newer
one, and recovery seals and syncs the tail it served. The buffered-crash
campaign detection became a control.

Three fixtures lost their scenario under the ledger and were re-targeted to
reproduce the same defects:
- `substitute-upload-collision` now crashes before the ledger retires the
  local copy, so the intact local member is still history when the remote
  copy is damaged.
- `lost-buffered-delete` fails the WAL sync that the post-flush prune would
  otherwise win the race with.
- The #811 stale-lease-lock detection now powers off after a clean shutdown,
  since every open's ledger publication makes the earlier unlink durable.

## Baseline execution

Run from a clean checkout, with receipts outside the checkout:

```sh
cargo run --locked --all-features --example wal_redesign_baselines -- --baseline \
  --output /tmp/midge-wal-baseline
```

This checks expected RED/GREEN states for applicable cases. An unselected
Sqrzl case is explicitly recorded as skipped; `coverage_complete` is false. No baseline receipt sets `acceptance_passed`.
A registry with no executed cases fails. To require the provider regression:

```sh
docker compose up -d sqrzl
SQRZL_SECRET_ACCESS_KEY=easy-peasy cargo run --locked --all-features --example wal_redesign_baselines -- \
  --baseline --with-sqrzl --output /tmp/midge-wal-provider-baseline
```

Explicit provider selection requires the local endpoint and credential. Missing
setup fails before running that test and cannot count as RED evidence. Each
Cargo/test process tree has a 300-second deadline, configurable with
`--timeout-seconds`. Timeout is never a matching RED result.

Linux CI executes every case, the provider case with Sqrzl, and retains
receipts and complete test logs. No case depends on the host platform; Windows
and macOS CI compile every fixture target. This is baseline maintenance, not a
complete cross-platform redesign campaign.

## Campaigns

`tests/wal_campaigns.rs` drives the engine through seeded workloads on the
crash device and judges every recovery with the prefix oracle (#831).
Control campaigns must find nothing. CI runs their per-PR seed budget with the
integration tests. The nightly `WAL campaigns` workflow runs 100 times as many
seeds, from a base that moves each run. Detection campaigns keep fixed seeds,
so every run proves the same findings (#837).

A restarted process takes a crashed one's lease over at once: the test
advances the lease clock it reads past the old lease's expiry, and it acquires
the next epoch through the normal path (#835). A crashed process reaches
neither its device nor the object store again, so it cannot release its lease
or write after its crash; requests it had in flight can still land late.

Cloud campaigns (#838) run the same harness against simulated-cloud and
provider engines whose objects live in a simulated object store, sealing an
asynchronous WAL segment every two writes so several upload at once:

- **Promises.** `cloud_strict` writes, clean shutdowns and, in provider mode,
  completed flushes are cloud-durable. `cloud_async` writes are promised
  nothing until one of those covers them.
- **Two histories.** A recovery from a surviving cache is judged as locally:
  the engine may not lose what it has served. A takeover by a process with a
  fresh cache, in provider mode only, keeps just what the cloud promised, so
  it is judged against a cloud history that only such takeovers rebase. A
  recovery from a surviving cache still removes the unpromised WAL commits it
  lost from the cloud history.
- **Faults.** The faulting control fails WAL and SST uploads and deletes
  before or after their effect, loses their answers, delays them and lands
  them late, waits for the store to settle before some crashes, and retries
  opens the store's faults refuse, as an operator would. Any finding fails
  it: since #841 it no longer tolerates WAL holes at fresh-cache takeovers.
- **Upload attempts.** A deterministic control fails segment 1's first three
  upload attempts while later segments upload past it, lets the store settle,
  then takes over with a fresh cache. Before #841 this reproduced
  `out-of-order-publication`: the upload queue dropped segment 1 after three
  attempts, segment 2 was published past it before the runtime re-admitted
  segment 1, and the takeover recovered the later writes without the earlier
  ones. Publication now waits for a contiguous run and the upload
  keeps retrying, so the control must find nothing.
- **Images** record how the engine stopped, so a replay judges a fresh-cache
  takeover against the cloud history, and keep the store's held-back
  requests, so a replay can land them.
- **Simulated cloud** keeps its lease and shared metadata in the cache, so a
  fresh cache would lose its whole cloud; it has no fresh-cache takeovers.

A finding is saved as a crash image below `MIDGE_CAMPAIGN_IMAGES`, and both
workflows upload those images as artifacts. To run the nightly budget, then
replay a saved image:

```sh
MIDGE_CAMPAIGN_PROFILE=nightly MIDGE_CAMPAIGN_SEED_BASE=1000000 \
  MIDGE_CAMPAIGN_IMAGES=target/wal-campaigns \
  cargo test --all-features --test wal_campaigns -- --test-threads=1
MIDGE_CAMPAIGN_REPLAY=target/wal-campaigns/<case>-seed-<seed> \
  cargo test --all-features --test wal_campaigns should_replay_saved_image_when_one_is_named
```

## Acceptance execution

Omit `--baseline` and select Sqrzl to require all GREEN:

```sh
SQRZL_SECRET_ACCESS_KEY=easy-peasy cargo run --locked --all-features --example wal_redesign_baselines -- \
  --with-sqrzl --output /tmp/midge-wal-acceptance
```

Acceptance requires clean, stable source, exactly one executed test per
selector, no skipped case, and every test GREEN.

RED evidence requires a failed libtest summary and the selected test's specific
assertion inside its failure block. Compiler warnings, missing selectors,
compilation/setup failures, and unrelated panics cannot satisfy it. Empty
registries, duplicate selectors, and missing RED markers are rejected before
execution. Receipts record source SHA, dirty-source fingerprint, exact commands,
logs, deadlines, coverage completeness, and source stability. Output paths
inside the checkout are rejected before any test runs so receipts cannot
invalidate their own source identity.

## Scope and remaining qualification

The inventory adds no persisted layout, durability policy or public API. The
full #808 exit criteria still require the reviewed simulator, adapter
injection, all fixing stages, engine campaigns, removal audit, qualified
develop-to-main promotion, and exact-main readback. Merging this inventory
must not close #808 or #811.
