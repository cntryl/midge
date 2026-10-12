# WAL redesign regression inventory (#808)

This change inventories the 14 original audit classes as 17 executable tests,
plus two supplemental allocation/history cases and three cases the crash
simulator found (`torn-unsynced-tail`, #817; `recovered-tail-not-durable`,
#831; `lost-buffered-delete`, #835). It supplies a baseline runner and CI
coverage. It does not implement the redesign or complete its simulator
stage. Refs #808.

The earlier simulator and shipping adapter changes were removed after review
found false-pass and false-failure risks. Filesystem fault injection and crash
state fidelity, provider late effects and ABA identities, ordered recovery
oracles, stable replay identities, and engine campaigns must be qualified in
subsequent serial PRs before serving as acceptance evidence. The removed draft
is retained in Git history at `33cc1a72e10282eba32e8cfb090aa2fea7535f77`.
The crash-device-dependent #811 fixture follows the ownership work; #811 stays
open. Existing production storage and lease protocols are unchanged.

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
  cloud cache starts one past the catalog's last segment, linked to the
  catalog head. A ledger whose open segment is below that was succeeded by
  another writer, which alone publishes there; `Rebase` keeps its local WAL
  aside and continues past the catalog, linked to the catalog head.
- Salvage drops a suffix of the ledger with `SetAside{from, cut, open}`, after
  copying the dropped files into `wal/salvaged-<millis>/`, truncating the cut
  segment to its replayed prefix and sealing that with a footer. The new open
  segment links the cut. Nothing is renamed.
- Until #844 chains the catalog, the catalog's first entry has no anchor to
  check; every later cloud segment must carry the epoch its entry names and
  link the one before.
- A transaction is single-family: its frames carry the family, and a batch
  spanning families is refused before any sequence is allocated.
- Files the ledger does not name are orphans. The writable open sweeps them,
  and fails closed on one holding data above every id the ledger handed out.
- An idle column family pins every later local segment until a flush covers
  its own: prefix-only retirement accepts that growth. Cloud acknowledgements
  that arrive out of order keep later local copies until the prefix catches up.

## Named regressions

[The registry](wal-redesign-regressions-808.json) assigns each test to its fixing
stage and records a specific assertion for each expected RED. An ignored test
is an unresolved defect, not a waived acceptance requirement. The two retired
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
campaign detections became controls. `predecessor-cache` moved to stage 4:
a segment id cannot tell a predecessor's stale tail from a successor's
history once the successor's catalog entries retire, so it needs ownership
(#820). A cache whose ledger is behind the catalog
belongs to a writer another one succeeded: it sets its local WAL aside,
replays only the catalog, and continues past it, since an unpublished tail is
not history once another writer has taken over.

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
