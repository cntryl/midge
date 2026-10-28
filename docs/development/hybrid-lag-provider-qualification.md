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
different scopes must not be compared as a provider cost improvement. Actual
provider A/B comparisons use matching native scope and three alternating pairs.
The local floor has no provider cost scope. Shutdown costs are included in
native cost totals, while commit latency and throughput exclude drain/shutdown.

Qualification still owes exact-SHA mock and Sqrzl artifacts, machine/service
identity, commands, sampler health, all pair deltas and median. Preserve failed
and inconclusive attempts under the existing evidence procedure. No matched
SlateDB run or external parity comparison is available.

On 2026-10-26, a one-sample local smoke of the new four-client simulator row
was inconclusive: its maximum sampler gap was 53 ms against the existing 40 ms
limit. The guard remains unchanged. This host run is retained as a failed
attempt, not as row evidence; repeat on a runner that meets the existing sample
health checks before beginning the three alternating A/B pairs.
