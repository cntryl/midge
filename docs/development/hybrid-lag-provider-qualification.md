# Hybrid lag provider diagnostic candidate

Issue #937's Sqrzl rows share the Tier 2 lag workload: 2048 fixed transactions
with 24-byte keys and 128-byte values. One-client rows preserve the existing
baseline; a four-client `cloud_strict` row loads the configured upload pool.
Rows cover `cloud_async`,
`cloud_strict` and the equivalent local `sync` floor. Cloud options use the
production defaults with compaction enabled. Native provider execution remains
a qualification step; this document records the implemented measurement shape,
not results or a cloud latency claim.

```text
cargo check --locked --features failpoints,stress-soak --bench tier2_subsystem_hybrid_durability_lag --bench tier5_hybrid_durability_lag
cargo bench --locked --bench tier2_subsystem_hybrid_durability_lag -- --no-progress
cargo bench --locked --features stress-soak --bench tier5_hybrid_durability_lag -- --no-progress
```

The Tier 5 target is opt-in and its four rows have diagnostic trust. It uses
the existing Sqrzl namespace setup, `MIDGE_STRESS_SQRZL_ENDPOINT` (default
`http://127.0.0.1:9000`) and configured `SQRZL_SECRET_ACCESS_KEY`. Unavailable
Sqrzl or namespace setup fails the row; no provider result is substituted.
The Tier 2 simulator rows retain gate trust.

The sampler reads the event-loop-published durability-frontier snapshot and a
separate pending-upload gauge every 10 ms during the commit window. These direct
reads do not queue a full runtime metrics request; frontier values may briefly
lag while the event loop processes a message. It records local-durable and
visible sequence lag and the sampled maximum pending-upload count. The sampler
stops before the final drain; drain-time zero lag cannot establish sample
health. A valid cloud row requires nonzero ticks, at least one quarter of
nominal ticks and no inter-sample gap above 40 ms. It reports achieved mean
period, maximum gap and tick count.

Cloud rows also report the count and p50/p99/maximum interval between sampled
increases of `wal_cloud_durable_seq`. This is a sampler-quantized frontier
interarrival observation, not a catalog compare-and-swap completion timestamp;
multiple publications between ticks can be coalesced. The configured nominal
sampling period is recorded with the row, and the achieved period and maximum
gap bound how to interpret the observed intervals. The metric must not be used
as an exact per-publication latency claim. A cloud row with fewer than two
sampled advances is marked inconclusive instead of silently omitting the
interval distribution.

The default cloud upload policy records four workers and a shared 256 MiB
upload-memory budget for retained bodies, provider copies and fallback
verification workspace. Upload admission separately bounds the pending source
WAL queue at 1,024 entries and 1 GiB, with a 32-entry worker channel. These are
configured bounds, not observed resident-memory usage. The pending-upload
observation is a count, not bytes; no live queued-byte gauge is exposed by this
benchmark.

Probe waits every 64 asynchronous commits provide a separate time-lag signal.
Probe timeouts, incomplete sampling or failed shutdown make the row inconclusive.
Commit p50/p99 and sampled lag p50/p99/max remain separate observations.

Native request/byte costs aggregate existing `midge::cloud_io` completion
events. The window begins after column-family setup and ends after cloud drain
and shutdown join settle admitted workers. It counts every completed engine HTTP
attempt, including retries, lease/control work and background maintenance. It
records PUTs, total request-body bytes, response-body bytes, transport failures
and cancellations. No URL, object key, credential or body content is retained.
Missing fields, zero native observations or unsettled attempts invalidate the
cost row. Body-byte counts exclude HTTP headers and are not billing estimates.
An unwinding measurement poisons subsequent windows in that process, since an
engine reaper could still emit completions. Repeat in a fresh process rather
than attributing those effects to another row.

The simulator's byte observation remains resident WAL object bytes; native
bytes include every engine request body. Their units are declared, and those
different scopes must not be compared as a provider cost improvement. Report
each provider's absolute observations with its native scope; no cross-revision
comparison is required.
The local floor has no provider cost scope. Shutdown costs are included in
native cost totals, while commit latency and throughput exclude drain/shutdown.

Exact-SHA Tier 2 and Sqrzl artifacts, machine/service identity, commands and
sampler-health results are recorded in
[`issue-937-frontier-sampler-2026-10-28`](evidence/issue-937-frontier-sampler-2026-10-28/README.md).
The Tier 2 simulator rows passed with gate trust. The strict Sqrzl rows passed
the sampler-health guard; the four-client and async throughput rows are noisy
and remain diagnostic. Pre-fix invalid rows are retained as historical reports.
Preserve future failed and inconclusive attempts under the same evidence
procedure. No matched SlateDB run or external parity comparison is available.

On 2026-10-26, a one-sample local smoke of the new four-client simulator row
was inconclusive: its maximum sampler gap was 53 ms against the existing 40 ms
limit. That failed attempt remains retained separately. On 2026-10-28, the
five-sample Tier 2 four-client row passed with a 30 ms maximum gap, and the
five-sample Tier 5 four-client row passed with an 18 ms maximum gap. The 40 ms
guard is unchanged. The healthy rows provide absolute current-source
observations; no performance-improvement claim is made.

The current #916 work adds measurement fields only. It has no accepted numeric
latency target yet, so its cloud-strict observations remain diagnostic and do
not establish a performance-improvement claim.
