# WAL redesign status and acceptance (#808)

The regression inventory in [wal-redesign-regressions-808.json](wal-redesign-regressions-808.json)
tracks the original audit failures and later crash-simulator findings. This page
describes the landed authority model and the remaining behavior required to
close #808. It does not change the test registry, tests, or workflows.

## Landed authority model

The current on-disk format is 8. WAL membership and order are recorded in the
local `wal/LEDGER` in every storage mode. Linked segment identities and durable
sequence reservations make the ledger's named history and allocation floor
explicit; directory contents do not establish WAL history.

Cloud history is the published chain in the version 3 WAL catalog. The catalog
retains its chain anchor and monotonic segment allocation, and publication adds
only a contiguous run. A local ledger records the cache's history; the shared
catalog records the published cloud prefix.

Per-family flush frontiers are part of manifest state. A flush publishes the
SST and advances its family's frontier in one manifest edit. Replay eligibility
uses the authorized WAL chain and those family frontiers; SST contents are not
used to prove WAL coverage.

Retirement is prefix-only and follows durable authority. Local retirement
persists the ledger's new `log_start` before queuing file removal. Cloud
retirement first commits the frontiers in a shared generation, records that
generation in the catalog's `retired_by`, and then queues identity-conditional
deletes. Recovery verifies that the shared generation covers the catalog's
retirement point. Failed or delayed physical deletion can leave storage behind,
but cannot make retired history replayable.

Unsupported persisted formats are rejected before mutation. There is no
backward-compatibility or migration path. The manifest is format 8 and the
cloud WAL catalog is format 3.

## Five safety contracts

| Contract | Current status | Remaining issue |
|---|---|---|
| Explicit WAL history and sequence allocation | Local membership is in `wal/LEDGER`; cloud published history is a chained catalog. | Complete ownership and takeover qualification in #820. |
| Replay only authorized history above family frontiers | Implemented by the ledger/catalog chain and format 8 manifest frontiers. | #819 is complete; retain its production-path regressions in final qualification. |
| Retire only a prefix after durable authority advances | Local `log_start` and cloud `retired_by` order retirement before physical deletion. | #819 is complete; qualify remaining crash and provider cases in final qualification. |
| Fence ownership and every effect | A stale cache or worker must not alter authority or apply an effect after takeover. | #820, including the lease-release power-loss case tracked by #811. |
| Return truthful outcomes and salvage only verified history | `MidgeError::OutcomeUnknown` already represents uncertain commit results. | #821 covers salvage; #822 covers outcomes, admission, replay limits, and event-loop ownership. |

Stage 3, #819, is complete in [PR #866](https://github.com/cntryl/midge/pull/866).
The remaining implementation queue is serial: ownership and fencing (#820),
safe salvage (#821), outcomes and transaction admission (#822), then the
invariant catalog and durability contract (#823). Issue #820 is a required
dependency of #808 even though it is labeled `area:lease`. Issue #811 remains
open until #820's acceptance evidence passes.

Issue #832 is a separate filesystem-contract issue. It covers consistent
`NotFound` classification for a missing rename source, consistent
`AlreadyExists` classification for `create_new` on an existing file, and the
engine behavior for vanished salvage SSTs and spill-name collisions. It is not
a dependency of the WAL redesign.

## Remaining issue contracts

### #820: ownership, lineage, and effect fencing

Takeover must be based on persisted ownership and chained history, not on a
stale cache's view or whether the successor happened to publish WAL. Enforce
the current epoch and expected authority state at every ledger edit and shared
effect. A delayed worker completion or storage operation from a predecessor
must not cross the ownership boundary. Track worker lifetime explicitly: do
not release ownership or reuse its state until the worker is confirmed stopped,
and do not let a hung worker block the event loop.

Include #811's case: a completed filesystem lease release followed by power
loss must permit a successor to acquire a greater epoch. Also cover power loss
while acquisition is still in progress. Keep #811 open until this evidence
passes.

### #821: one fail-closed salvage rule

Salvage may use only the longest verified prefix of the authorized history. It
stops at the first damaged or missing authoritative history point; it does not
skip a hole and continue with later records. Persist the sequence floor before
truncating, quarantining, or setting aside any history. Timeouts, I/O failures,
resource limits, and uncertain authority fail closed without destructive
salvage. Predecessor-tail ownership decisions belong to #820.

Cover the cloud local-retirement wedge: corruption in a local prefix already
retired by cloud authority must not pin or prevent recovery of later valid
history. Exercise equivalent histories through local and cloud recovery, with
the expected outcome classes preserved across modes.

### #822: outcomes, complete transaction admission, and bounded replay

Use the existing `MidgeError::OutcomeUnknown` contract; do not add another
outcome error type. A failure after WAL effects may have begun must not be
reported as retryable `NotApplied`. Admit a whole transaction, including its
replay resource envelope, before the first WAL effect. Apply one bounded replay
rule across local and cloud paths so an abandoned split transaction cannot
block checkpoint replay. Keep mutable runtime state owned by the event loop;
workers return results for event-loop transitions to apply.

### #823: invariant catalog and durability contract

Update only WAL and cloud-catalog rows superseded by this design. Preserve
still-valid durability and cloud invariants, and remove references to eliminated
coverage-proof behavior. Document explicit format rejection with no migration
path and the `cloud_async` takeover contract: unpublished writes can be
recovered from the same surviving cache, but are not promised to another
holder's takeover and must not override the shared published chain. Remove the
stale validator reference. This issue does not include CI or workflow changes.

## WAL redesign acceptance evidence

- **Ownership:** a successor acquires after power loss following completed
  release; a stale predecessor cache cannot replace successor history; delayed
  writer effects and unauthorized ledger edits are fenced; workers cannot
  outlive or block the ownership transition.
- **Salvage:** a transient read failure preserves acknowledged data; interrupted
  salvage cannot reuse sequences; a torn unsynced tail opens strictly; recovery
  from a substitute segment permits later strict writes; corruption in a
  retired local prefix does not pin later cloud-retired WAL.
- **Outcomes and transactions:** fsync failure is never reported as retryable
  `NotApplied`; whole-transaction budget rejection leaves the WAL usable; an
  abandoned split transaction does not block checkpoint replay.
- **Final qualification:** every redesign selector is green; provider coverage
  is executed with no skips; the nightly WAL campaign budget passes.

## Independent storage-contract qualification (#832)

This issue is separate from #808 and is not a redesign dependency. Its evidence
must show matching `RealFs`, `CrashFs`, and `MockFs` classifications for a
missing rename source and `create_new` on an existing file, plus engine behavior
for vanished salvage SSTs and spill-name collisions.

The final baseline-runner acceptance command must select Sqrzl and omit
`--baseline`, so expected-red inventory mode cannot count as completion:

```sh
docker compose up -d sqrzl
```

Then run:

```sh
SQRZL_SECRET_ACCESS_KEY=easy-peasy cargo run --locked --all-features --example wal_redesign_baselines -- \
  --with-sqrzl --output /tmp/midge-wal-acceptance
```

Run the nightly campaign budget with the repository's campaign harness:

```sh
MIDGE_CAMPAIGN_PROFILE=nightly MIDGE_CAMPAIGN_SEED_BASE=1000000 \
  MIDGE_CAMPAIGN_IMAGES=target/wal-campaigns \
  cargo test --all-features --test wal_campaigns -- --test-threads=1
```

An unselected provider case, a skipped selector, an empty selector set, a
compile/setup failure, or a campaign timeout is not acceptance evidence.
