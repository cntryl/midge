# Operational cloud qualification

Midge serves Fitz and Cassie from one owner process. This campaign tests the
ability to recover and maintain cloud data larger than the configured local
working disk. It does not add independent readers, distributed compaction, or
a new persisted coordination protocol.

## Repeatable native-provider campaign

The ignored integration test uses the native S3 provider against the pinned
Sqrzl image in `docker-compose.yml`. It creates an acknowledged seed through
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
retry within a separate deadline.

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
verified records because full-state verification has not run yet.

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

Retirement progress is process-local. Manifest changes involving overlapping
or uncertain coverage can require revalidation; frequent compaction churn may
therefore increase cleanup cost. The optimization does not weaken exact
coverage checks or promise a durable resume cursor across process restarts.
