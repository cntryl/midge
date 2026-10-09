# Write-pressure attribution (#753)

This investigation closes a measurement boundary before choosing an optimization.
The merged baseline is `c6a5b1ca3e3a004e26480261db0fdcb6ae714658`, after #752's
checkpoint change and #782's observation fix. The new counters compile only for
tests and `internal-testing`; ordinary production builds retain no new counters.
Existing stall transition counters retain their original meaning.

## Declared campaign and stopping criteria

Before collecting results, the campaign is fixed at three independent fresh
processes per local write-heavy 1/2/4/8/16-client and S3 emulator pressure
1/2/4-client cell: 24 ten-minute intervals. Every case keeps its original limits,
workload shape, no-progress bound, six exact verification checks and two successful
shutdowns. No failed repeat may be removed. The source, executable, initial state,
provider artifact hashes and all native diagnostics must be retained.

Every caller-observed WriteStall must reconcile with one commit rejection and
exactly one origin: bounded transaction queue, L0, cloud generation or cloud WAL
admission. Unattributed or duplicate counts fail attribution acceptance. Counts
are per rejected admission, not per blocked family, retry sleep, waiter wakeup,
state transition or post-apply durability failure. Unknown outcomes remain terminal.
Snapshots are taken with all measured callers joined; they are not an atomic
multi-counter view while writes are concurrently running. Counters are per engine.

A mechanism is dominant only if it accounts for at least half of the measured
rejections in all three repeats of an affected cell. Task durations, sampled
maintenance debt/occupancy and native CPU profiles must support its explanation.
The diagnostic task stops after reconciling the entire matrix and identifying a
supported mechanism or publishing an explicit null result. It does not require
open-ended improvement of an undeclared production SLO.

If a production optimization is justified, its gate is declared now: improve
acknowledged throughput by at least 20% in the affected fixed cells and avoid a
throughput regression greater than 10% in every other cell, with exact data,
original budgets/deadlines and successful-call latency preserved. A selected
change needs focused RED/GREEN tests and the fourteen original full-hour cases at
its final merged source. A separately tracked follow-up must name the mechanism,
finite target and regression scenario. Diagnostic completion is not an optimization.

## Observations and their limits

`WriteAdmissionSnapshot` is available only through the unsupported `__internal`
diagnostics interface. The commit count is recorded only when submission returns
WriteStall before an applied sequence. Origin counters record the first rejecting
gate; the queue counts transaction messages and excludes control requests and
disconnected channels. Rejected admissions do not enter success populations.
The readback checks native parameters against both cumulative endpoints, and
reconciles the origin sum with caller-reported rejections.

Existing build/publication durations count completed worker tasks, including work
that began before an interval boundary. They are not exact interval occupancy and
must not be summed with inclusive checkpoint or replay phases. One-second optional
maintenance samples retain request/completion timestamps, queue debt, flush and
compaction gauges, errors and cumulative timers. They never advance the watchdog.
Sampled gauge fractions are observations at sample times, not exact busy-time
integrals; errors and missed short tasks remain visible. Polling adds at most one
runtime request per sample, with a 250-ms caller bound, and is part of this baseline.

The separate profiling job runs plain and 99-Hz user-space CPU sampled 60-second
fresh-process trials on the same hosted ARM machine at local 16 and S3 4 clients.
Each retains its own native receipts and correctness checks. Profiles include
setup, ingestion and verification, so startup symbols must be separated from write
and maintenance symbols. Plain/profile trial differences quantify sampling
perturbation only for those trials; they are not an optimization comparison or a
statistical claim. Emulator costs do not qualify production-provider latency.

## Execution

Use the existing reusable workflow on the committed branch:

```sh
gh workflow run bench-tier5.yml --repo cntryl/midge --ref BRANCH \
  -f profile=standard -f investigation=comparisons
```

Read every original archive and verify the 24 planned cells with
`tools/readback-investigation.rb ROOT SHA 600`. Read each profile trial separately
with the same script and a 60-second minimum. Publish all outcomes, counter
reconciliation, completed-task duration deltas, sampled gauges, native profile
attribution and any instrument failure before issue closure.
