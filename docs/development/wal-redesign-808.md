# WAL redesign regression inventory (#808)

This change inventories the 14 original audit classes as 17 executable tests,
plus two supplemental allocation/history cases and two cases the crash
simulator found (`torn-unsynced-tail`, #817; `recovered-tail-not-durable`,
#831). It supplies a baseline runner
and CI coverage. It does not implement the redesign or complete its simulator
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
| 1 | Injected storage root: every engine file effect goes through one `Fs`, then the qualified crash simulator | #813, #814, #815, #816, #827, #825; simulator #817 (crash filesystem), #830 (object store, scheduler), #831 (oracle, local campaigns), #837 (fixture re-targeting, campaign budgets), #835 (cloud campaigns, lease control) |
| 2 | Segment ledger, linked segment format, sequence reservations, contiguous catalog publication | #818 |
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
- Contiguous catalog publication lands with the ledger in stage 2.
- Rotation does not pre-open the next segment, because the next header links
  the previous segment's final digest.

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
