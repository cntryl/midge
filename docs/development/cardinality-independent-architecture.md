# Cardinality-independent LSM architecture

Midge has no fixed supported key-count ceiling and no compaction fan-in limit
based on the number of non-overlapping target files crossed. The checked
contract is about retained work and memory as SST cardinality grows. Catalog
storage remains proportional to SST count, and elapsed I/O remains
proportional to the bytes and files that actually intersect an operation.

This is an architectural bound, not a promise that databases of different
sizes have identical wall-clock latency.

## Assumptions

The tight read bounds apply after every L1+ SST has complete key bounds and the
level satisfies the non-overlap invariant. Newly flushed and compacted SSTs set
`key_bounds_complete`. Legacy files remain readable in a conservative fallback
bucket until a one-file-at-a-time maintenance pass verifies and durably records
their complete bounds. A verified overlap quarantines the level instead of
optimizing through a false invariant.

The L0 bound assumes writes use the current admission path. A database opened
with historical state already above the ceiling remains readable, stalls new
writes, and schedules recovery compaction. Storage operations must continue to
succeed, and admitted write rate must stop or remain below compaction service
rate, for the progress guarantee to apply.

FORMAT 3, SST V4, the public API, compaction intents, and manifest edit schemas
are unchanged. The only manifest compatibility addition is the optional
`key_bounds_complete` metadata field; absence means untrusted.

## Checked work bounds

Let:

- `H = l0_compaction_trigger + max_immutable_memtables_per_cf + 1`;
- `L` be the configured number of levels, including L0;
- `n_i` be the number of complete-bound SSTs in lower level `i`;
- `S` be the number of selected L0 source files in one compaction.

| Operation | Retained or opened SST work | Metadata selection work |
| --- | --- | --- |
| Snapshot capture | zero `FileMeta` clones; one `Arc<SstReadView>` clone | constant after a catalog publication |
| Point read | at most `H + 2 * (L - 1)` candidate readers | `O(sum(log n_i))`, plus returned candidates |
| Forward or reverse range scan | at most `H + (L - 1)` active SST cursors | `O(sum(log n_i) + intersecting files)` |
| L0 compaction | at most `S + 1` merge heads | target span metadata is linear in overlap count |
| Inner-level compaction | at most two merge heads | target span metadata is linear in overlap count |

L0 files may overlap arbitrarily, so the read view orders them by recency and
must consult each published file. Admission prevents valid post-migration state
from growing that set beyond `H`. Complete L1+ files are ordered by full key
coverage. Binary search finds a point or range boundary; equality at adjacent
boundaries may conservatively select both neighbors.

A range scan gives every selected L0 file its own cursor and uses one chained
cursor for each lower level. The chained cursor opens one ordered file at a
time in both directions. Work may still grow with the number of files and rows
inside the requested range, but the scan does not retain one active cursor per
lower-level SST.

Compaction represents source files and the complete target-level span
separately. L0 retains one head per bounded selected source plus one chained
target head. Inner-level compaction retains one chained source head and one
chained target head. Point versions and range-tombstone start/end events share
that stream. Every reader, retained event, merge container, boundary key, and
output buffer is charged to the derived compaction resource pool; genuine byte
exhaustion fails closed.

## L0 admission and progress

L0 slot usage counts published L0 files, every queued or in-flight immutable
generation capable of publishing another L0 file, and a non-empty active
generation. A transaction that consumes the last reserved generation may
complete, but the next transaction is rejected with `WriteStall` before WAL
admission. Disabling background compaction does not disable this ceiling.

The single worker chooses work in this order:

1. Critical L0 debt, round-robin across affected column families.
2. The globally deepest overfull inner level.
3. Ordinary soft-L0 work.

For a logical interval at level `i`, assign rank equal to the number of levels
remaining below it. Compaction never moves an interval upward. Every successful
job advances at least one source interval to a strictly deeper level, while a
deleted interval disappears. With finite levels and no admitted writes, the
sum of interval ranks strictly decreases. `compact_all()` returns success only
after the production debt predicate is clear; debt with no valid plan is an
invariant error.

Stalled writers are reconsidered only after L0, memory, disk, and cloud
pressure all clear. The architecture deliberately retains one global worker;
parallel compaction is a throughput optimization, not part of the cardinality
proof.

## Publication authority

Compaction publication keeps the existing complete-set ordering:

1. Finish and durably sync every output.
2. Persist the vector-valued output-durable intent.
3. Mirror any subset and then all outputs where cloud storage requires it.
4. Atomically switch the manifest from the complete input set to the complete
   output set.
5. Publish the refreshed in-memory snapshot.
6. Garbage-collect inputs.
7. Durably clear the intent.

Recovery interprets a crash before the manifest switch as old-set authority
and a crash at or after the switch as new-set authority. It never constructs a
partially visible replacement set. The test matrix exercises all eight cut
points, including partial and complete mirroring, by reopening persisted state
and replaying the production intent logic.

## Deterministic proof evidence

`should_prove_read_work_bounds_across_synthetic_manifest_cardinalities` builds
the real immutable index at 1, 1,000, 100,000, and 1,000,000 complete lower-level
intervals, plus the default hard L0 ceiling of 15. Test-only counters measure
metadata comparisons, per-snapshot `FileMeta` clones, modeled candidate reader
opens, and active SST cursor slots. The existing actual-reader regression also
confirms that an equality-boundary point read opens two readers rather than an
entire level.

`should_keep_compaction_work_bounded_across_ten_thousand_targets` feeds a
10,000-file target span to the real streaming input constructor and observes
two merge heads with resource ownership below the configured pool. The
partitioned-output resource test streams logical input several times larger
than its pool and asserts the recorded peak never exceeds that pool.

`should_prove_every_small_abstract_manifest_invariant`
enumerates all 4,096 occupancy states across three column families and four
levels. It uses the production admission, picker, and debt predicate, applies
each selected publication, and requires a strictly decreasing rank until every
family is clear. A separate hot-family schedule must repeat `0, 1, 2, 0, 1, 2`.

`should_replay_every_compaction_publication_crash_point_to_complete_authority`
reopens and replays output durability, intent durability, partial mirror,
complete mirror, manifest switch, snapshot publication, input GC, and intent
clear states. Every recovered manifest is exactly the old set or the complete
new set.

The earlier counterexamples remain useful guards: a 100,000-file point lookup
formerly visited the whole lower level, a 65-target compaction formerly hit the
total-input resource limit, and disabled compaction formerly allowed continuing
L0 growth. The corresponding regressions now enforce indexed selection,
streamed target spans, and pre-WAL admission.

## Commands

```text
cargo test --lib runtime::sst_read_view::tests::should_prove_read_work_bounds_across_synthetic_manifest_cardinalities -- --nocapture
cargo test --lib compaction::tests::should_keep_compaction_work_bounded_across_ten_thousand_targets -- --nocapture
cargo test --lib runtime::cardinality_proof_tests -- --nocapture
cargo test --lib runtime::state::tests::should_replay_every_compaction_publication_crash_point_to_complete_authority -- --nocapture
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings -D clippy::pedantic
cargo fmt --all -- --check
cargo check --release --workspace --all-features
cntryl-tools validate-tests
```

## Scale calibration and limitations

The million-interval model tests metadata shape, not a million on-disk SSTs and
not a 500-million-key latency run. The existing
[bounded compaction qualification](bounded-compaction-qualification.md)
measured and crash-verified 261.2 million logical entries with 238 remaining L0
files at its largest rung. Linear calibration of that observed metadata density
to 500 million logical entries is roughly 456 files, so the synthetic one-million
interval model is more than 2,000 times larger in catalog shape. This comparison
is calibration only; no 500-million-key timing is claimed.

The proof does not remove unavoidable linear costs:

- Manifest persistence and the shared catalog require `O(total SSTs)` storage.
- A catalog rebuild after a manifest edit performs `O(total SSTs)` work before
  later snapshots can share it.
- Legacy fallback and quarantined overlap levels are intentionally
  conservative and can exceed the tight candidate bounds until repaired.
- Range I/O scales with intersecting files and returned data.
- Compaction I/O and elapsed time scale with the complete source and target
  byte volume, even though active heads and owned memory remain bounded.
- Storage exhaustion, corruption, cancellation, invalid ranges, or a persistent
  provider failure may stop progress and return the exact blocking error.
