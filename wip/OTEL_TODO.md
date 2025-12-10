# OTEL TODO

This file tracks the work needed to make Midge fully OpenTelemetry-ready with tracing and metrics. Keep updates brief and check off items as they land.

## Core tasks
- [ ] Add `telemetry.rs` module exposing `init_otel()`, `meter()`, `tracer()` with Prometheus endpoint + OTLP (HTTP/gRPC) exporters and console subscriber for debug.
- [ ] Wire telemetry initialization from the public API (e.g., `midge::telemetry::init_otel()`), no-op safe if called multiple times.
- [ ] Define shared meters/counters/histograms with lock-free handles (Prometheus static collectors) and avoid allocations on hot paths.
- [ ] Add context propagation helpers so spawned tasks inherit the current span.

## Subsystem instrumentation
- [ ] WAL: spans for append/rotate/fsync/replay; counters (`wal_append_total`, `wal_fsync_total`, `wal_segment_rotate_total`); histogram for flush latency; structured fields (seq, cf_id, key_len, segment_id).
- [ ] Memtable: instrument apply paths (non-hot) with spans and error logs; avoid per-key logging; gauge for memtable bytes.
- [ ] SST: spans for write/flush/compaction; counters (`sst_flush_total`, `sst_compaction_total`, `sst_block_load_total`); block cache hit/miss counters; histogram for flush/compaction latency.
- [ ] Block cache: counters for hit/miss; gauges for capacity/size; optional hit ratio metric (derived) without extra allocations.
- [ ] Runtime/actors: spans for message dispatch/handlers; counter for `runtime_messages_total` per variant; gauge for queue depth; histogram for request latency.
- [ ] Cloud: spans for upload/ack/fail; counters (`cloud_upload_total`, `cloud_upload_retry_total`); histogram for upload latency; structured error logging.

## Safety/perf constraints
- [ ] Do not instrument hot inner loops (no per-key spans or logs in tight loops).
- [ ] Prefer metrics first, tracing second, logging third; avoid string concatenation in logs.
- [ ] Ensure instrumentation respects layer rules in `docs/DEPENDENCY_ANALYSIS.md`.

## Testing & validation
- [ ] Add minimal integration test that initializes telemetry and emits a sample metric/span (behind `logging`/`otel` feature gate if needed).
- [ ] Document how to run with Prometheus/OTLP exporters and how to view traces locally.
