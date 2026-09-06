# Operational cloud qualification

Midge serves Fitz and Cassie from one owner process. This campaign tests the
ability to recover and maintain cloud data larger than the configured local
working disk. It does not add independent readers, distributed compaction, or
a new persisted coordination protocol.

## Repeatable native-provider campaign

The ignored integration test uses the native S3 provider against the pinned
Sqrzl image in `compose.yml`. It creates an acknowledged seed through
the public API, then publishes a deterministic catalog-authorized WAL fixture
in bounded objects. Fixture records have independent reproducible values and
valid framing, epochs, lengths, and checksums. This produces a controlled
backlog without storing a complete expected-value ledger in memory.

Each profile runs these phases in fresh child processes:

1. Delete the complete local engine directory. Recover until the first durable
   checkpoint, then exit immediately through an exact recovery failpoint.
2. Delete that local directory again. Wait for the crashed owner's lease to
   expire, recover the entire fixture, and verify every source key and value.
3. Run acknowledged writes, point reads, short scans, flushes, and compaction.
   Temporarily fail SST uploads; observe retained working-space charges and
   successful publication after the failure is removed.
4. Delete the working directory again, reopen, and verify every source value
   and every acknowledged workload value, including the final durable marker.

The shipping lease duration remains in effect. An external child watchdog
makes a hung recovery fail the campaign. Only `LeaseHeld` retries during open;
other recovery failures surface immediately. Expected pre-append write stalls
and post-outage publication polling share one fixed workload deadline derived
from `MIDGE_QUALIFICATION_TIMEOUT_SECONDS`. Progress does not reset that deadline;
the child watchdog still bounds the complete phase, including recovery. While
the campaign runs, its engine request timeout is extended to the configured
profile timeout when that exceeds the engine default. Bulk `compact_all`
requests share the fixed workload deadline and report maintenance metrics while
waiting; a large legitimate compaction is not limited by the default API wait.
The reporter cannot cancel a blocked scoped request; the external child
watchdog bounds its join. Shipping defaults and provider I/O timeouts are
unchanged. While
waiting, five-second status reports show total SST count, completed maintenance,
queues, local pressure, and the age of the last observed progress. An unchanged
completion counter can mean a large compaction is still running; the reports
also show flush-publication age separately so unrelated compaction cannot hide
a waiting flush. They do not claim per-block progress or replace the fixed timeout.

Run the default two profiles (6 MiB WAL / 2 MiB local, then 12 MiB / 4 MiB):

```sh
docker compose up -d sqrzl
MIDGE_QUALIFICATION_ARTIFACT_DIR=/tmp/midge-cloud-evidence \
  cargo test --test cloud_provider_engine_qualification \
  --features sqrzl-tests,failpoints \
  operational::should_recover_cloud_backlog_after_complete_local_disk_loss \
  -- --ignored --exact --nocapture
```

All sizes are independently configurable. An example larger profile is:

```sh
MIDGE_QUALIFICATION_LOCAL_BYTES=2147483648 \
MIDGE_QUALIFICATION_WAL_BYTES=5368709120 \
MIDGE_QUALIFICATION_MEMORY_BYTES=268435456 \
MIDGE_QUALIFICATION_VALUE_BYTES=32768 \
MIDGE_QUALIFICATION_SEGMENT_BYTES=33554432 \
MIDGE_QUALIFICATION_TIMEOUT_SECONDS=7200 \
MIDGE_QUALIFICATION_ARTIFACT_DIR=/tmp/midge-cloud-evidence \
  cargo test --release --test cloud_provider_engine_qualification \
  --features sqrzl-tests,failpoints \
  operational::should_recover_cloud_backlog_after_complete_local_disk_loss \
  -- --ignored --exact --nocapture
```

These are test profiles, not engine capacity limits. The small profiles also
exercise a single WAL object larger than local capacity. The larger example
splits its aggregate backlog into 32 MiB objects to remain within the Sqrzl
server's request-body limit. Indivisible records and transactions still need
to fit the engine's configured working capacity.

## Evidence and its limits

Each phase writes JSON plus a child-process log. `pressure.json` records
metrics during the injected upload failure and after recovery. Set
`MIDGE_QUALIFICATION_REVISION` to label the exact tested revision; the Cloud
Qualification workflow sets it to the tested SHA and uploads artifacts even
when the campaign fails.

The reports contain encoded cloud WAL bytes, source and verified record counts,
recovery duration, observed peak local file bytes, process peak RSS, checkpoint
count, and the existing runtime metrics. Recovery duration includes any lease
takeover wait. RSS includes the process, runtime, and allocator, not just the
engine's configured data allocations. An interrupted phase reports zero
verified records because full-state verification has not run yet. An additional
`*-opened.json` report preserves recovery observations before query verification;
it also reports zero verified records and `verification_complete: false`.

Local file bytes are sampled every 5 ms and synchronously at publication and
checkpoint failpoints. The sum covers reachable filenames' logical lengths;
it excludes filesystem block overhead and unlinked files still held open.
The configured budget is also enforced by Midge's
reservation ledger. This is evidence about file residency and accounting on
the test host; it is not an OS-enforced filesystem quota or proof that sampling
observes every transient filesystem allocation.

`remote_range_*` runtime counters record submitted SST range calls,
returned bytes, failures, and cumulative elapsed time. Warm cache hits add no
provider requests. They exclude HEAD calls, startup WAL replay, WAL retirement,
and control metadata. Separate counting-provider regressions check exact
remote request accounting and independent-engine isolation.

Schema version 2 also records `costs` in each child report. `costs.http` groups
HTTP attempts by method, separating ranged GETs. These process-wide totals
include all native-provider executors in the child: lease operations, metadata,
WAL, SST, background maintenance, retry attempts, and listing pages. The
`*-opened.json` snapshot is taken before query verification, so its totals
isolate the recovery interval from the later workload. Fixture preparation in
the parent process is excluded. These totals are not per-engine metrics for a
process containing multiple engines.

`request_body_bytes` counts body bytes offered to each HTTP attempt, not bytes
acknowledged by the server. `response_body_bytes` counts consumed body chunks,
including partial/error responses; headers, TLS overhead, and unread bytes are
excluded. `http_errors` counts status codes at least 400, including expected
not-found responses. Transport errors and cancelled attempts have separate
counts. A deadline cancelling a live attempt still produces its observation.
The opt-in `midge::cloud_io` tracing target emits these observations at DEBUG,
without object keys, URLs, headers, credentials, or payload contents.

`costs.recovery_phases` aggregates the `midge::recovery` tracing target. Phase
times are inclusive: `coverage` and `recovery_checkpoint` are part of
`wal_replay`, which is part of `replay_and_repair` and `open`. Do not sum nested
phase durations. Failed lease-acquisition/open attempts are included, while
the campaign's sleep between acquisition attempts is only in `recovery_ms`.
Coverage also reports probe count, reader-open attempts, and successfully
verified SST bytes. A hard process exit can leave a phase incomplete, so the
interrupted report contains only observations emitted before that exit.

Coverage retains at most one SST reader using its existing byte budget. It
releases the reader before checking a different SST and before checkpoint
construction. Full-object verification remains bound to an immutable read
view, and each data probe retains conditional identity and block validation.
This reduces repeated metadata reads without persisting a new proof format or
expanding the cache with the SST inventory.

Sqrzl is the self-contained native-provider qualification environment described
in [the cloud qualification policy](cloud-qualification-policy.md). Its
results do not establish AWS latency, S3 request cost, or a production recovery
time objective. Deployment-specific runs should retain the same correctness
and resource observations and add provider-wide request/byte telemetry.

## Responsibility boundaries

- WAL retirement owns reusable proof progress; raw WAL/SST readers expose
  narrow cursors and retain format validation. Object identity and committed
  metadata remain the authority for deletion.
- The hybrid storage ledger owns capacity and rejection history. Runtime
  metrics expose a snapshot without creating another admission policy.
- The owner event loop selects the next existing maintenance task. Flush and
  compaction continue to own construction and publication.
- Read admission owns bounded open coordination. Remote filesystem views
  report actual range I/O through a lower-layer observer contract; storage
  does not depend on runtime types.

Recovered L0 inventory can exceed normal write-admission thresholds. Point
reads check complete manifest key bounds before opening each candidate, retain
newest-first ordering, and consult files with unknown or invalid bounds.
This keeps unrelated cold tables from consuming the reader pool while allowing
recovery to finish independently of the steady-state L0 threshold.

Full scans chain contiguous groups of strictly disjoint L0 intervals, retaining
one active reader per group. Overlapping or uncertain files keep their existing
source precedence and conservative reads. Retained SST reader metadata therefore
follows the number of these groups, plus independently read uncertain files. A recovered
backlog with many overlapping or uncertain intervals may still require
maintenance before a scan fits its memory budget. Scans separately collect the
selected range tombstones; that retained state grows with the selected tombstone
history. This campaign's independently generated source keys exercise disjoint
recovered intervals and do not establish a bound for aggregate tombstone state.

Automatic persistent-engine memory allocation leaves a quarter of the memory
remaining after transaction and compaction pools for SST reads before sizing
the two memtable generations. This changes small-budget defaults within the
configured total. Explicit memtable sizes remain unchanged when they leave
read capacity; configurations leaving no SST read capacity fail during option
construction. Cold-open bookkeeping shares one bounded coordination allowance,
so longer object names reduce available concurrency instead of wasting fixed
per-owner slices.

Retirement progress is process-local. Manifest changes involving overlapping
or uncertain coverage can require revalidation; frequent compaction churn may
therefore increase cleanup cost. The optimization does not weaken exact
coverage checks or promise a durable resume cursor across process restarts.

Cloud maintenance yields retirement proof work at acknowledged progress
boundaries. Its cooperative work slice leaves provider timeouts intact and
allows other ready maintenance to run before another proof turn. Mandatory
identity preflight, an indivisible proof step, and final catalog publication
still use the outer operation deadline, so the slice is not a hard total
latency guarantee. Slow-provider regressions verify that work can advance
even when a successful request takes longer than the slice.
Retained proof memory is deducted from compaction execution and publication
allowances so alternating workers cannot each claim the full maintenance pool.


## Enforced Linux resource profiles

The separate [resource-bound qualification runner](resource-bound-qualification.md)
uses a finite disk-backed ext4 filesystem and a memory-limited engine container.
It keeps Sqrzl, the controller, OS resource sampling and evidence outside those
limits, and adds deterministic multi-family overwrite/tombstone/TTL workloads,
external SIGKILL, and kernel disk exhaustion with an external acknowledgment
ledger. These enforced profiles complement the native sampled campaign above.
