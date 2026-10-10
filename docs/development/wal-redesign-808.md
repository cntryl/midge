# WAL redesign regression inventory (#808)

This change inventories the 14 original audit classes as 17 executable tests,
plus two supplemental allocation/history cases. It supplies a baseline runner
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

The non-corruption salvage fixture injects I/O failure at the shipping replay
spool allocation boundary. A callback inside the error branch proves the fault
fired. A healthy control verifies every acknowledged transaction member. Both
`Ok` and `Err` from the faulted open must preserve those values on a later
healthy open. No temporary-directory environment variables are used. The hook
is disabled in production builds and is removed with replay spooling in #808.

## Baseline execution

Run from a clean checkout, with receipts outside the checkout:

```sh
cargo run --locked --all-features --example wal_redesign_baselines -- --baseline \
  --output /tmp/midge-wal-baseline
```

This checks expected RED/GREEN states for applicable cases. Unsupported
platform cases and an unselected Sqrzl case are explicitly recorded as skipped;
`coverage_complete` is false. No baseline receipt sets `acceptance_passed`.
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

Linux CI executes all applicable cases with Sqrzl. macOS CI executes the native
partition, including the immutable-file unlink fault, and records the provider
skip. Windows CI compiles all portable fixture targets. Linux and macOS retain
receipts and complete test logs. Their union covers every registry entry; this
is baseline maintenance, not a complete cross-platform redesign campaign.

## Acceptance execution

Omit `--baseline` and select Sqrzl to require all GREEN:

```sh
SQRZL_SECRET_ACCESS_KEY=easy-peasy cargo run --locked --all-features --example wal_redesign_baselines -- \
  --with-sqrzl --output /tmp/midge-wal-acceptance
```

Acceptance requires clean, stable source, exactly one executed test per
selector, no skipped case, and every test GREEN. Because the unlink fixture is
macOS-specific, complete current registry acceptance runs on macOS. The
redesign must supply a portable adapter-based version before claiming the
planned cross-mode campaign coverage.

RED evidence requires a failed libtest summary and the selected test's specific
assertion inside its failure block. Compiler warnings, missing selectors,
compilation/setup failures, and unrelated panics cannot satisfy it. Empty
registries, duplicate selectors, and missing RED markers are rejected before
execution. Receipts record source SHA, dirty-source fingerprint, exact commands,
logs, deadlines, coverage completeness, and source stability. Output paths
inside the checkout are rejected before any test runs so receipts cannot
invalidate their own source identity.

## Scope and remaining qualification

This PR adds no persisted layout, durability policy, public API, simulation
oracle, or production adapter path. Its sole shipping-path addition is a
feature-gated replay spool I/O failpoint. The full #808 exit criteria still
require the reviewed simulator, adapter injection, all fixing stages, engine
campaigns, removal audit, qualified develop-to-main promotion, and exact-main
readback. Merging this inventory must not close #808 or #811.
