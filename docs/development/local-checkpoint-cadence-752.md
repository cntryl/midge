# Bounded local checkpoint candidate (#752)

Status: candidate under correctness and performance qualification. Not accepted
for merge, issue closure or release by the measurements below.

## Refreshed baseline

Source `d5bcb607b75b1fa5cf5e4f4f910d7e49f9ebe12b`, unchanged checkpoint cadence.
[Transport smoke 37805299364](https://github.com/cntryl/midge/actions/runs/37805299364)
and [nine-cell run 37806015565](https://github.com/cntryl/midge/actions/runs/37806015565)
completed. Independent Rust readback verified originating REST metadata, sealed
provider archives, executable/build receipts, all fixed workload construction,
compaction and exact acknowledged-data verification before/after reopen.
Full nine-row evidence is [checkpoint-752-baseline-readback.json](evidence/checkpoint-752-baseline-readback.json).
Raw capture remains `/tmp/midge-752-baseline-captured`; retain provider artifacts
for reproduction rather than treating this summary as the raw input.

| Cell | Repeat | Snapshot/SST bytes | Checkpoint/publication time | Checkpoint p95 lower ms | Gate miss |
| --- | ---: | ---: | ---: | ---: | --- |
| A | 1 | 0.82% | 24.32% | 7.25 | True |
| A | 2 | 0.96% | 15.02% | 1.75 | False |
| A | 3 | 0.95% | 15.38% | 1.75 | False |
| B | 1 | 13.19% | 25.31% | 1.75 | True |
| B | 2 | 13.28% | 19.12% | 1.00 | True |
| B | 3 | 13.28% | 19.37% | 1.00 | True |
| C | 1 | 80.23% | 27.47% | 2.25 | True |
| C | 2 | 80.26% | 20.91% | 1.00 | True |
| C | 3 | 80.27% | 19.73% | 1.00 | True |

B and C qualify in all three repeats. A misses once and does not qualify.
The native one-sample statistical diagnostic remains untrustworthy for generic
throughput inference; the explicitly preregistered fixed-cell accounting gate
is independently valid. No unfavorable repeat or admission rejection is removed.

## Candidate contract

Only an ordinary local flush without a required snapshot may defer. Its durable
batch includes the SST, next SST identity and `BumpWalSeq` persisted frontier;
all three share a framed record and synced marker. No journal format or public
API change is introduced. `next_wal_seq` has no runtime mutation and is not a
new deferred frontier.

Trial triggers are 16 journal records since the checkpoint or 16 KiB of journal.
The existing interval-16 probe motivates the first trial; these limits require
matched engine/recovery measurements before acceptance. A flush is eligible
only when its conservative framed-record upper bound is at most 4 KiB, checked
before metadata cloning/JSON allocation. Name escaping is bounded at six bytes
per source byte and hex bounds at two; a 1536-byte allowance covers fixed fields,
frontier edits, envelope, framing and the marker. Larger metadata forces a
checkpoint before and after its append; its existing allocation path is not
claimed to become globally bounded by this change.

A successful eligible append may cross the byte trigger by one bounded record,
so the eligible deferred tail is below 20 KiB and at most 16 records. A failed
checkpoint forgets cached authority. Before another ordinary append, checkpoint
failure propagates and prevents additional tail growth. This bounds the added
deferred tail, not all pre-existing journal data or forced oversized records.
No new metadata cache is added. Unknown position, changed file lengths or a
caller behind durable authority cannot defer; a stale caller is fenced for reload.
Duplicate SST publication still checkpoints rather than deferring unjournaled
progress.

## Forced authority inventory

- Required/cloud flush publication retains its snapshot requirement.
- Manifest actor persistence retains DDL, administrative and compaction-before-GC checkpoints.
- Startup storage, streaming recovery, intent replay and cloud recovery retain forced checkpoints.
- Cloud shutdown retains its forced cloud checkpoint. Local shutdown already syncs WAL and drains flush publication; it does not require a new manifest snapshot for durability.
- No WAL pruning, SST GC, cloud frontier, lease, recovery policy or watchdog is weakened.

## Acceptance still required

Deterministic checkpoint failure/retained-journal controls, real acknowledged
SST-only crash recovery, stale caller, staging rename/directory durability and
truncation failures, shutdown/cloud/GC controls, lint and hosted CI must pass.
Then run the same transport smoke and all nine candidate repeats; compare all
matched rows to this baseline, retaining every noisy/failing result. Qualifying
cells require at least 50% lower attributable checkpoint bytes/time; stable
throughput and public flush p95 must not regress more than 10%. Report startup
replay overhead and actual journal limits. Full final merged-source acceptance
requires the fourteen original hour-long Tier 5/6 correctness/recovery cases.
Probe/short-run gains do not close #752.

## Local verification

- Payload regression: original cadence failed with 1,948,222 ordinary and forced snapshot bytes; candidate passed the >=90% reduction assertion and reconstruction after every publication.
- Fixed-cardinality manifest regression: 128 SST entries, 128 frontier updates; >=90% checkpoint-payload reduction and exact replay after every edit passed.
- 77 runtime-state tests passed; one explicit measurement test remained ignored.
- 124 all-feature metadata-filtered controls passed, including stale/concurrent checkpoints, synced-marker errors, directory sync, partial-tail repair and rename-before-truncation recovery.
- Local/cloud public manifest checkpoint and compaction control passed.
- Real child-process abort after two acknowledged local flushes recovered 16 exact committed rows from published SSTs with fixture WAL removed.
- Worst-width numeric fields/key-bound record-size controls passed.

Hosted exact-head validation and candidate measurements remain separate from local proof.
