# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and version numbers use [Semantic Versioning](https://semver.org/spec/v2.0.0.html) formatting.

Midge is currently in the 0.1 release line. Compatibility expectations for pre-1.0 releases are defined in [docs/development/stability-policy.md](docs/development/stability-policy.md).

## [Unreleased]

### Changed

- Strict WAL acknowledgement, remote DDL authority calls, and direct manifest mirroring now
  share a deadline derived from when the caller began waiting, instead of
  restarting a full `storage_io_timeout` on every round trip. Deployments on
  degraded providers may now see `MidgeError::Timeout` naming the storage step
  where earlier releases blocked longer. Callerless flush and maintenance work
  retain their own retry lifecycles; compaction publication does not yet have one
  aggregate deadline across all provider operations.
- A cloud WAL acknowledgement whose callers have all abandoned their requests now
  continues as callerless durability work. Once a sealed WAL segment is accepted,
  publication failures requeue it so an inflight frontier gap cannot strand later
  strict waits. A newer dependent waiter's remaining budget prevents an expired
  older waiter from prematurely failing it. Background CloudAsync publication
  remains callerless as before.
- Timed-out column-family reclamation remains retained and is retried by idle
  maintenance until authoritative manifest publication succeeds; physical SST
  deletion begins only afterward. The retry deadline pauses under online
  verification rather than spinning the event loop and receives a bounded
  fairness slot under sustained request load.
- Cloud WAL pruning now runs off the event loop behind the metadata-publication
  gate. It proves an exact committed metadata snapshot and its referenced SSTs
  before conditionally retiring catalog authority, retains WAL on missing,
  mismatched, or timed-out proof, and rotates candidates so one unverifiable low
  segment cannot starve later safe cleanup.
- Ambiguous remote DDL compare-exchange outcomes retain the durable prepare and
  fence writes, flushes, and compactions until the operation ID is positively
  observed in authority state.
- Draining the CloudAsync WAL upload backlog validates the writer lease once per
  pass rather than once per segment. Durable fencing is unchanged: the
  publication catalog's epoch check and compare-exchange remain authoritative.

### Added

- `RuntimeMetricsSnapshot::abandoned_runtime_requests_total` and
  `RuntimeMetricsSnapshot::late_runtime_responses_total` report callers that
  stopped waiting and responses that arrived with no caller left. They diagnose
  aggregate runtime-timeout behavior across routed and inline transaction paths;
  because they are process-wide and late responses include errors, they do not
  identify the outcome of an individual timed-out mutation.

### Fixed

- A response arriving after its caller gave up no longer blocks the event loop on
  the tombstone mutex that timing-out callers contend for.
- Pending requests are now failed reliably when the event loop panics: the
  submission gate is closed before the pending table is drained, so a caller
  submitting concurrently is failed rather than left to wait out its full
  response timeout.

- **Breaking:** cloud provider enum variants now contain private-field typed
  AWS, Azure, GCS, OCI, and generic S3-compatible configurations. Cloud
  locations normalize surrounding prefix slashes, and `OpenOptions::build`
  performs automatic side-effect-free structural validation. See the
  [migration guide](docs/operations/migration-guide.md).
- **Breaking:** database FORMAT 3 now requires SST V4. V4 uses a fixed,
  self-identifying checksummed footer, mandatory checksummed block trailers,
  exact block-handle validation, and explicit TTL presence. FORMAT 1/2 and SST
  V1-V3 require logical export with the old binary and import into a new
  database; there is no in-place migration or legacy fallback. See the
  [FORMAT 3 migration](docs/operations/migration-guide.md).
- **Breaking:** cloud WAL recovery now trusts only publication catalog v1 and
  epoch-scoped objects. Prefixes containing the older segment-only layout, or
  epoch-scoped objects without a valid catalog, fail startup rather than
  guessing publication authority. There is no in-place migration; export with
  a compatible binary and import into a new prefix as described in the
  [cloud WAL migration](docs/operations/migration-guide.md).
- Synchronous runtime operations now have a bounded response wait. The default
  is 60 seconds with the default storage I/O timeout and is configurable with
  `runtime_response_timeout`; when it expires, Midge returns
  `MidgeError::Timeout` without cancelling work already accepted by the runtime.
  Treat a timed-out mutation as outcome-unknown until runtime and recovery
  evidence establish its result.
- `drop_column_family` now refuses to discard committed data still present in
  the active memtable and returns `MidgeError::Busy`. Callers must flush and
  retry, or explicitly opt into data loss with
  `drop_column_family_discarding_unflushed`.
- Scan iterators retain stable SST handles for the scan lifetime on supporting
  filesystem backends and expose explicit active, exhausted, and failed
  states. Terminal read errors remain sticky instead of becoming clean
  exhaustion; path-only backends fail visibly if the backing path disappears.
- Provider-backed cloud storage now defaults to one bucket/container and one
  database prefix. Advanced deployments can route WAL, SST, and control
  objects separately with `CloudStorageTopology` and `OpenOptions::cloud_multi`.
- Option construction now rejects invalid memtable limits, zero transaction
  pools, invalid cloud write policies, invalid lease skew tolerances, and a
  runtime response timeout that does not enclose the storage I/O timeout.
  Scans with an explicit start key greater than the end key now return
  `MidgeError::InvalidArgument`.

### Added

- `Transaction::assert_value` provides an opt-in, ABA-safe value precondition.
  It checks the frozen transaction snapshot and rejects any later point or
  covering range mutation before commit serialization, regardless of the
  transaction's ambient conflict policy. Assertion reservations share the
  bounded transaction memory pool and can return `MidgeError::ResourceLimit`.
- Public `EngineMetrics` and `StorageVerifier` facades, including bounded
  runtime-metrics capture, plus explicit `IteratorState` reporting.
- Lease-loss notification and clock-safety controls through `on_lease_loss`,
  `lease_clock_skew_tolerance`, and `ttl_clock`.
- Read-only cloud location/topology preflight with an overall deadline,
  topology deduplication, bounded range reads, and serializable redacted
  readiness reports. Preflight does not qualify write, CAS, fencing, or delete
  permissions; Sqrzl remains authoritative for mutation semantics.
- Initial release of Midge embedded LSM database
- Actor-based concurrency model for deterministic execution
- Cloud-native storage support (S3, Azure Blob, GCS, OCI)
- Three storage modes: Memory, Local, Cloud
- Explicit durability guarantees (sync, buffered, best_effort)
- Snapshot isolation with MVCC
- Column family support
- Range queries with prefix scans
- Bloom filters (SST-level and block-level)
- Block cache with LRU/TinyLFU/CLOCK-Pro policies
- Leveled compaction strategy
- WAL with configurable durability policies
- Comprehensive metrics and telemetry
- Tiered benchmarking suite (Tier1-4)
- YCSB workload support
- Cross-platform support (Linux, Windows, macOS)
- Startup recovery metrics API: `Engine::get_recovery_metrics()`
- Runtime recovery metrics snapshot path (`GetRecoveryMetrics`) for WAL and intent-log replay visibility
- Integration coverage for recovery metrics API, including deterministic `intent_log.yaml` replay fixture

### Fixed

- Durability acknowledgements now remain tied to their covering persistence
  barrier: concurrent strict commits may share one physical fsync, but no
  caller succeeds before that barrier completes successfully.
- Cloud WAL upload, publication, takeover, and recovery now preserve writer
  fencing and fail closed on ambiguous or stale publication state.
- Shutdown retains writer fencing while accepted durability work is still
  draining, including after the caller's shutdown deadline expires.
- WAL replay, manifest publication, flush/compaction cleanup, and column-family
  DDL now preserve committed state across their failure and recovery paths.
- TTL expiry is a nondestructive visibility decision until compaction can prove
  that physical reclamation is safe for every active snapshot.

### Upgrade and rollback

- Rollback is unsupported within a database or cloud prefix after the new
  persisted formats have been written. Preserve the old database or prefix and
  use its compatible binary as the rollback target. Follow the logical
  export/import procedures in the [migration guide](docs/operations/migration-guide.md)
  before switching traffic.

### Documentation

- Architecture guide
- Recovery and durability guide
- Performance tuning guide
- Cloud setup guide
- API guide
- Testing guide
- Benchmarking guide
- README example for recovery metrics usage
- Recovery internals observability section for startup replay counters
- support matrix, format compatibility policy, and release policy docs
- operator runbook and release checklist
- Consolidated durability documentation around the canonical transaction durability contract
- Trimmed duplicated positioning/readiness documentation and refreshed storage-mode overview language
- Defined Sqrzl as the authoritative self-contained cloud qualification
  environment, with manual real-cloud testing used to validate emulator fidelity
  and deployment assumptions.

### Removed

- Unsupported legacy SST codec identifiers and the nonshipping compression
  fast-accept heuristic. Unknown or removed codec identifiers now fail closed.
- The mandatory three-location `CloudStorageBuckets` API.
- Orphaned internal `SeqnoAllocActor` source file that was not compiled into the runtime actor module
- Empty/redundant integration test files and duplicate engine initialization coverage

## [0.1.0] - TBD

### Added
- Initial pre-release version

[Unreleased]: https://github.com/cntryl/midge/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/cntryl/midge/releases/tag/v0.1.0
