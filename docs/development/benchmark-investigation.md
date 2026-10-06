# Tier 4–6 measurement and profiling

This campaign addresses #752–#755 through benchmark instrumentation and
controlled comparisons. Production checkpoint cadence, exact WAL coverage,
reader/block limits, retry semantics and watchdog deadlines remain unchanged.
The exploratory probes do not satisfy acceptance for a production policy.

## Caller latency

Tier 5/6 now report three populations, with sample counts:

- `attempt_latency_*_us`: every database call, including rejected and terminal
  attempts, excluding reporter work and subsequent retry sleep.
- `successful_call_latency_*_us`: only successful database calls, with the same
  call boundary. Success includes read-only completions; it is not a claim of
  strict durable write acknowledgement when a workload selects async writes.
- `inter_ack_latency_*_us`: worker-loop start to first success, then previous
  success to next success. Includes reports, rejected calls, actual sleep and
  worker scheduling. Attempts can select different operations, so this does
  not measure repeated execution of one stable transaction intent.

The legacy `transaction_latency_*_us` names retain their original all-attempt
meaning, recorded explicitly in report parameters. Do not compare those old
percentiles with the new successful/inter-ACK population as equivalent data.
HDR histograms use microseconds, rounded down and floored at one microsecond;
merge histograms, not percentiles. Empty populations report zero alongside
zero samples; that is missing successful work, not zero-latency success.

Requested backoff stays in `saturation_backoff_ms`. `actual_sleep_ns` measures
elapsed monotonic time around sleep. Both are summed worker-time: overlapping
clients can exceed stage wall time. The unfinished interval at loop exit is
retained as `censored_inter_ack_samples` and `censored_inter_ack_ns`; it never
enters the successful histogram. Final forced snapshot publication is outside
the interval. Accounting overflow makes `valid=false` sticky.

Client/status JSON contains a versioned `latency` object. Stage CSV retains
existing columns and appends explicit populations/counts, actual sleep and
censored intervals. Runtime stall reasons have separate stage-local `*_delta`
parameters with validity flags. Legacy reason parameters remain cumulative
end-of-stage values. A counter reset invalidates its delta; never sum those
cumulative values across stages.

## Independent write-pressure cells

The original Tier 5 ramp remains a growing engine across concurrency stages.
Set `MIDGE_STRESS_COMPARISON_CLIENTS` to one of the workload's original client
counts to execute one stage in a fresh process/database/remote namespace.
The configured duration applies to this single stage. Values, transaction
shapes, durability, flush thresholds and workload behavior stay identical.

Comparisons start from the same empty write workload (no warmup operations).
Read workloads retain their deterministic 512-row seed. Actual initial scan
cardinality is checked before measurement. Each `stage-00-prestate.json`
records the initial rows, prior-stage count, runtime state, SST layout, logical
local file bytes and source. `resolved-options.json` records the resolved
memory pools and thresholds. Empty initial state controls workload age; it
does not establish a matched steady-state maintenance backlog.

Run a local cell directly:

```sh
MIDGE_STRESS_COMPARISON_CLIENTS=4 MIDGE_TIER5_DURATION_SECS=600 \
  STRESS_PROFILE=smoke STRESS_NO_PROGRESS_TIMEOUT_SECS=60 \
  STRESS_TIMEOUT_SECS=1500 \
  bash tools/run-stress-with-receipts.sh tier5_stress_workloads \
  tier5_write_heavy_local
```

`bench-investigation.yml` runs three fresh hosted ARM processes per local
1/2/4/8/16-client and emulated S3 1/2/4-client cell. Smoke measures 60 seconds;
standard measures 600 seconds. Native statistical diagnostics remain intact.
The use of native `STRESS_PROFILE=smoke` follows the existing operational
workloads: it does not shorten the explicitly configured measured interval or
upgrade statistical confidence. Original exact data/reopen/flush/shutdown
checks remain mandatory. Every attempt, including failures, retains archives.

## Exploratory checkpoint policies

```sh
cargo run --release --example benchmark_investigation \
  --features internal-testing -- checkpoint /tmp/checkpoint-probe.json
```

The probe uses real `ManifestStore` journal framing, fsync, snapshot staging
and truncation on a private filesystem. Three repeats cover fixed synthetic
SST cardinalities 16/64/256 and snapshot edit intervals 1/16/64, with 512 real
monotonic WAL-sequence metadata edits per row. Every edit is independently
reloaded before optional snapshot publication. The 16 KiB journal threshold
is checked after append; a single framed edit can overshoot it. It is an
experimental trigger, not a production capacity limit.

Receipts distinguish snapshot/journal issued bytes, actual checkpoint time,
peak journal bytes and replay time. Initial setup is outside counters.
`cadence_elapsed_ns` subtracts measured reload time from loop wall time, but
still includes validation/serialization/metadata observations. Reloading every
edit warms metadata caches. Timings are diagnostic, not isolated engine flush
latency or physical-device write amplification. Synthetic SST metadata has no
acknowledged data. This does not replace the original A/B/C engine campaign,
forced-publication/crash/frontier controls or final hour acceptance in #752.

## Recovery locality probe

```sh
cargo run --release --example benchmark_investigation \
  --features internal-testing -- recovery /tmp/recovery-probe.json
```

The probe builds sixteen real uncompressed checksummed SSTs with 128 keys each
and uses the unchanged `ReplayCoverage` through the filesystem-backed remote
adapter. Three repeats vary grouped/interleaved keys, 128 KiB/2 MiB shared
budgets, and retained proof versus explicit proof discard every 256 probes.
Every exact point proof must succeed. Original SST bytes have a retained
XXH3-128 fixture fingerprint to check equality across comparison rows.

Receipts include candidate/node visits, verified bytes, reader/block work,
completed decodes/key allocations, remote ranges/bytes/errors, inclusive
coverage time, wall time and peak/final resource charges. Fixture preparation
is outside measured replay. Every variant releases all retained charges.
Explicit discard models the cache boundary only; it does not execute durable
Engine checkpoint publication. Filesystem adapter timing does not establish
native network-provider latency or a production recovery objective. These
controlled cases can identify locality costs but cannot attribute all the
original hour campaign's time without additional native profiles.

## Delivery boundaries

The measurement PR may close #755 after native JSON/CSV/status and final-hour
readback. #752 remains a production-policy investigation; #753 and #754 require
measured attribution or a documented null result against declared targets.
Ship selected engine changes in subsequent subsystem PRs, with focused
red/green durability controls and final-source qualification. Preserve the
original nine checkpoint repeats and fourteen-hour negative/positive archives.

The registered Tier 5 workflow can dispatch the reusable investigation before
the new workflow exists on the default branch:

```sh
gh workflow run bench-tier5.yml --ref BRANCH \
  -f profile=standard -f investigation=all
```

`investigation=none` retains the original nine-case ramp. Investigation
`profile=full` optionally measures each independent cell for 3,600 seconds;
the ordinary Tier 5/6 full campaigns remain separate acceptance evidence.

## Literal readback

Retain each original uploaded ZIP, validate its CRC and extract into a separate
artifact directory. Then run:

```sh
ruby tools/readback-investigation.rb EXTRACTED_ARTIFACT_ROOT SOURCE_SHA SECONDS
```

The tool requires real successful completions and checks all six exact-value
controls, both shutdowns, actual measured duration, source identity and native
finalization. The runner builds once, retains its Cargo artifact receipt and
SHA-256, then executes that exact artifact. Hashing happens before the watchdog
starts. Workload status retains that checksum and checks for clean tracked source
at setup, outside the measured stage. Direct Cargo invocations without the wrapper
remain usable diagnostics but do not supply the executable fingerprint.
Earlier captures without that fingerprint remain diagnostic archives, not
complete executable-provenance evidence. It compares native observations and sample counts with CSV/status,
and checks stage-local stall deltas against recorded runtime endpoints. Check
that the receipt count equals the entire planned matrix; partial downloads do
not establish a completed campaign. Stage wall time includes initial runtime
and prestate capture, worker setup, and ending inventory/report work; it excludes
the final flush/reopen verification. New measurement overhead is part of the new
baseline and does not establish a speedup against older reports.

Probe receipts are written after each completed row with `complete=false`
until the planned matrix finishes. A later failure retains earlier results and
job logs, but cannot qualify the whole matrix. Recovery range counters attach
after `ReplayCoverage` installs its normal observer and forward normal progress;
a regression requires actual nonzero completed ranges and bytes.
