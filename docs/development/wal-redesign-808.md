# WAL redesign #808: simulator stage

The acceptance specification is [#808](https://github.com/cntryl/midge/issues/808).
The baseline for the redesign is `fee88107cc5c76da2c4c9b7a1df486c8dce650a5`,
after squash merges of [#805](https://github.com/cntryl/midge/pull/805) and
[#806](https://github.com/cntryl/midge/pull/806). These prerequisites closed #792
and #799. This document records infrastructure and baseline failures; it does
not qualify the redesign or authorize closing #808.

## Adapter model

`CrashFs` implements the shipping `Fs`/`File` interfaces. Files have inode
identities; rename and unlink leave existing handles attached to their inodes.
File sync persists bytes, directory sync persists immediate child names, and
ancestor barriers remain necessary. A crash invalidates old process handles.
The supported failure selections discard volatile effects, retain OS pages
for a process-only failure, or deterministically persist torn writes and
unbarriered directory snapshots. This is a portable conservative model, not an
emulation of one host filesystem's journal or physical block allocation.
Its space counter includes data retained by durable names and open handles.

`ScheduledFs` lets the controller execute the storage effect independently of
its callback. `ObjectStore` uses the same scheduler and implements
`StorageBackend`, with version identities changing on every replacement.
Mutation preconditions are checked when the effect lands. Losing a response
does not revert the effect; duplicating a response does not rerun it.
The request reservation remains owned through completion or response loss.
Both adapters run the existing shared backend contracts.

The production event loop passes its filesystem into the WAL actor through a
rooted subdirectory view. The crash test exercises that actor's actual strict
append and the existing replay core. The adapter work has not yet replaced
native filesystem construction in every startup, lease, flush, or compaction
path. Full engine fault campaigns depend on completing those seams.

## Oracle and counterexamples

The independent oracle checks complete observations against ordered logical
transactions with `MustSurvive`, `MaySurvive`, and `MustNotApply` obligations.
Optional transactions apply atomically, rather than permitting independent
choices for their keys. It models deletes, half-open range deletes, absolute
TTL expiry, creation and dropping of families, and nonreuse of family IDs.
Candidate exhaustion returns an explicit inconclusive error.

Counterexamples carry the source SHA, model version, seed, logical transactions,
scheduling actions, effect/completion trace, observed state, and resource
counters. Current-model decoding rejects other versions. The deletion shrinker
requires a fresh replay for each candidate; invalid schedules cannot count as
reproduced failures. Campaign generators and a runnable engine replay driver
remain to be wired into this infrastructure.

## Audit registry

[The machine-readable registry](wal-redesign-regressions-808.json) retains all
14 audit scenario classes and assigns unresolved cases to the planned fixing
PR stage. Multiple tests cover best-effort restart, dropped-family variants,
and the retired cloud/local-copy regression. The latter is GREEN after #805;
the other 13 classes reproduced RED on the baseline. Supplemental RED tests
cover a failed allocation listing and epoch history ordered by reused IDs.
They belong to stage 2.

Unresolved cases have explicit `#808 PR N` ignore reasons for this initial
simulator stage. All must become active before final acceptance. The unlink
fixture currently requires macOS `chflags`; a portable scheduled-I/O variant
is still required. The native predecessor-cache case uses the repository's
Sqrzl configuration and requires its emulator. No production cloud account is
required.

Run and retain every named case regardless of random campaign budgets:

```text
python3 tools/run-wal-redesign-regressions.py --baseline --output /tmp/midge-808-baseline
python3 tools/run-wal-redesign-regressions.py --output /tmp/midge-808-acceptance
```

The second command requires GREEN even for ignored tests. Both commands
verify that each selector executes exactly one test and require its specific
failure marker for RED evidence, rejecting compilation or setup failures, and record source identity and dirty-source fingerprints.
An unsupported platform is incomplete qualification, never a passing receipt.
Use a clean immutable source revision for acceptance evidence.

## Current local evidence

- Formatting and strict all-target, all-feature Clippy pass.
- The 43 focused adapter, oracle, scheduler, and shipping-writer checks pass.
- The pure object-storage scheduler campaign passes 512 seeds × 256 operations.
  This exercises provider effects and responses; it is not an engine WAL campaign.
- `cargo machete` reports no unused dependencies.
- The invariant-reference validator fails on both this branch and unchanged
  baseline (242 errors on the baseline). The docs validator emits the same 463
  broken-anchor errors on both. These failures are recorded, not waived.
- The full serial unit suite and remaining qualification are still in progress.

## Remaining stage 1 exit work

- Finish filesystem, object-store, and lease injection through engine startup,
  flush, compaction, and recovery, keeping the shipping transition paths.
- Add a simulated lease using the production lease interface and scheduler.
- Provide engine counterexample replay and shrinking commands.
- Run local/CloudSimulated and Sqrzl engine campaigns at the fixed PR budgets;
  install campaign CI gates. Broaden the pure scheduler histories with the
  shipping authority protocol as its owners become available.
- Complete immutable-diff review and all required checks before opening this
  stage for merge. The remaining five redesign stages and qualified main
  promotion follow only after that merge and readback.
