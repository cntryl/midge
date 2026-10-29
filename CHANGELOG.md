# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and version numbers use [Semantic Versioning](https://semver.org/spec/v2.0.0.html) formatting.

Compatibility expectations for pre-1.0 releases are defined in [docs/development/stability-policy.md](docs/development/stability-policy.md).

## [Unreleased]

## [0.5.0] - 2026-10-29

### Added

- Cloud-durable sequence observation and `Engine::wait_cloud_durable`,
  per-operation latency histograms, WAL sync phase timings, and nonblocking
  durability-frontier sampling. Cloud-durable historical reads remain deferred.
- Optional `alloc-mimalloc` for binaries and benchmarks; the library installs
  no process allocator. Release and benchmark profiles use thin LTO.
- Loom models of the lock-free skiplist's insert, append and read races under
  `cfg(midge_loom)`, and a documented miri run of its unit tests, both part of
  the local gate for memtable changes (#931).
- A differential fuzz target and proptest, `engine_differential`, that replay
  random multi-key histories with snapshots, TTL, range deletes, flushes,
  compactions, reopens and power-loss crashes against a `BTreeMap` model, plus
  a concurrent-writer mode reconciled by commit sequence (#951).

### Changed

- **Breaking persisted format: database FORMAT 9 and SST V5.** Manifest and
  intent persistence use bounded binary frames with CRC32C. SST V5 uses compact
  entry headers, independent restart keys every 16 entries, and checked restart
  continuation metadata. Older database and SST formats are rejected without
  mutation; this release has no in-place upgrade from 0.4.x.
- Cloud metadata layout v2 publishes immutable manifest snapshots/edits through
  a separately fenced committed pointer. Lease v4 and initialization sentinel
  v3 replace the prior lease-embedded pointer. WAL publication catalog v3,
  segment v2 and exact acknowledged recovery floors remain unchanged.
- Commit/WAL and background maintenance pipelines preserve ordered whole-unit
  visibility while moving admitted work off the event loop. Cloud cleanup
  retains snapshot, compaction, intent and in-flight publication dependencies.
- Point reads stop at a deciding source after preserving L0 sequence order;
  SST handles remain open, reader caches are sharded, and range-tombstone
  coverage is indexed. Memtable and SST scans stream with bounded buffers.
- SST publication streams large immutable files through provider multipart or
  resumable protocols. Checksums must match exact source bytes and pinned
  identities; missing receipts retain identity-pinned readback.
- Cloud mode caches fetched remote SST ranges on local disk, keyed by the
  object's pinned identity and checksummed, in a quarter of a local storage
  budget of at least 32 MiB. That quarter is carved out before WAL, spill,
  flush and staging admission, which now see three quarters of
  `local_storage_budget`. A reader open fetches the object's last 64 KiB once
  instead of one request per metadata block, so `remote_range_bytes_total`
  counts the prefetched tail (#938).
- Range scans no longer insert the SST blocks they read into the block cache
  unless they are bounded by a `limit` of at most
  `Query::SMALL_SCAN_FILL_LIMIT` rows, so a one-pass scan leaves the
  point-read working set cached; `Query::cache_fill(CacheFill::Normal)` or
  `CacheFill::Bypass` overrides the choice (#944). `Query` gains the public
  `cache_fill` field.

### Fixed

- Reject WAL records that carry a `COMPRESSION` tag without a `VALUE` field.
- Report an admitted write whose commit outcome cannot be established as
  `OutcomeUnknown`, so callers do not mistake a timeout for proof of no write.
- Keep SST cache identities structural and validate restart continuation links
  during point lookup, including user-key histories spanning restart intervals.
- Preserve progress under local/cloud compaction pressure and reconcile lost
  conditional cloud mutation responses without blindly replaying effects.

### Compatibility and rollback

- Rust MSRV is 1.97. Database FORMAT 9/SST V5 and cloud metadata layout v2 are
  required. See [the migration guide](docs/operations/migration-guide.md) for
  the 0.4.x-to-0.5.0 application-level export/import procedure.
- **Binary rollback against a 0.5.0 store is unsupported.** Restore the
  preserved 0.4.x database/prefix and matching binary; reconcile writes made
  after cutover separately. Never rewrite a FORMAT marker to force an open.
- Exact WAL recovery floors and raw TTL retention are preserved. Destructive
  TTL reclamation and weaker WAL detection floors are not enabled. The SST
  redesign belongs to 0.5.1, whose in-place upgrade is a future requirement,
  not a capability delivered here.
- Qualification and measured performance are recorded per source and topology.
  Sqrzl evidence is not a live-cloud or RocksDB/Pebble parity claim. Performance
  baselines, hybrid latency/lag/cost evidence and cold-start measurements are
  deferred to 0.5.1; existing noisy or incomplete rows remain inconclusive.
  Release publication requires the final candidate to pass the release checklist.

## [0.4.1] - 2026-10-16

### Fixed

- Bound cloud WAL catalog decode, publication, retirement and startup salvage
  allocations using the retained containers and provider payload ownership
  rather than a multiple of the encoded catalog size (#886–#888). Native S3,
  Azure and GCS regressions cover incident-sized catalogs within Fitz's
  unchanged maintenance pool. Temporary decode admission refusal preserves
  receipt-confirmed cached authority; ambiguous writes still invalidate it.
  Retirement also admits the copied rows and nested family frontiers kept
  through conditional publication before releasing their original catalog.
  Persisted formats, epoch fencing and conditional retirement are unchanged.

### Compatibility and rollback

- No supported public API or persisted-format change from 0.4.0. Database
  FORMAT 8, SST V4, cloud catalog v3 and lease metadata v3 remain required.
  Older stores still require logical export/import into a fresh prefix.
- Rollback to 0.4.0 is supported after clean shutdown and preservation of the
  original store, subject to its catalog memory limits. It reintroduces the
  catalog admission failures fixed here and is unsuitable for affected stores.

## [0.4.0] - 2026-10-14

### Changed

- **Breaking on-disk format: database FORMAT 8 (#843–#866).** FORMAT 8
  requires SST V4 and durable per-family flush frontiers. WAL segments are
  linked and named in `wal/LEDGER`; the ledger records segment facts and
  durable sequence reservations. Manifest journal records require their edit
  envelope, key bounds use the canonical hex encoding, and legacy WAL
  checkpoint fields are removed. Current binaries reject database formats
  1 through 7 with `CompatibilityError`; there is no in-place upgrade.
- **Breaking cloud metadata formats (#844, #866).** Cloud WAL publication
  catalog v3 is a chained history whose sealed-segment summaries include
  record counts and per-family maximum sequences. Retirement is authorized by
  a committed shared manifest generation. Cloud lease metadata advances to
  version 3. These formats have no migration path; move cloud databases by
  logical export and import into a new prefix.
- **Breaking Rust integration API (#868).** Public storage integration
  capabilities changed: custom `CloudBackend`, `LeaderStore`, and filesystem
  implementations must adopt the focused capability interfaces and scoped
  acquisition/lock contracts. The public metrics fields used for WAL
  checkpoints were also replaced by the per-family flush-frontier view (#844,
  #866), and `MidgeError::OutcomeUnknown` distinguishes commits whose outcome
  cannot be confirmed (#866).
- **Breaking backup contents (#844).** A backup captures `wal/LEDGER` and the
  segments the ledger names, read inside the capture barrier, instead of
  everything under `wal/`. Orphaned segment files, `wal/salvaged-*/`
  directories and `.salvage-retained` copies are no longer captured, and a
  ledger that cannot be read fails the backup.
- **WAL writer fencing (#872).** Related same-epoch append, sync, and ledger
  effects share counted fencing ownership, reducing repeated lock admission
  and authority reads while preserving lease-transition exclusion.
- Empty range deletes are rejected with `InvalidArgument` (#866).

### Fixed

- Honor metrics-only telemetry configuration so benchmark and qualification
  callers can install their own tracing subscribers (#882).
- Preserve coherent, version-pinned control reads when a concurrent writer
  replaces the object afterward, avoiding spurious corruption during cold-cache
  takeover. Stale compare-and-swap attempts remain rejected (#880).
- Recovery seals and syncs the WAL tail it replayed before serving any read,
  so a power loss after a process crash no longer loses values recovery
  already returned (#843).
- A sequence number returned for a commit whose unsynced WAL record a power
  loss dropped is never handed out again (#843).
- Replay follows the WAL ledger's linked chain, so segments an older writer
  epoch wrote are no longer skipped as stale behind a regressed segment id
  (#843).
- A transaction that spans column families is refused before any sequence
  is allocated (#843).
- A predecessor that reopens its old cache after a successor took over no
  longer replays its unpublished WAL tail over the successor's
  `cloud_strict()` commits, even after the successor's catalog entries
  retire (#844).
- A crash during cloud salvage no longer lets the next open replay cataloged
  segments past the hole: one catalog compare-and-swap sets the suffix aside
  and raises the sequence floor before the WAL ledger changes (#844).
- Point and bounded-prefix reads remain correct when duplicate SST successor
  boundaries span many retained versions; unverified trie hints fall back to
  the complete sparse-index search (#775).
- Cancellation remains attached to cloud lease acquisition, filesystem writer
  locks recover safely after process death, and cloud conditional-read headers
  are preserved through provider requests (#868).

### Upgrade and rollback

- Upgrading from `0.3.2` to `0.4.0` requires a logical export and import into
  a new local database or cloud prefix. FORMAT 8 rejects older database
  formats, and cloud catalog v3 rejects other catalog versions; there is no
  in-place migration.
- Preserve the original database directory and cloud prefix for rollback.
  Rollback requires restoring that copy and running `0.3.2`; older binaries
  must not open data written by `0.4.0`.
- Applications implementing custom storage backends must update their
  implementations for the source-breaking integration interfaces. Applications
  with exhaustive `MidgeError` matches must handle `OutcomeUnknown`.

## [0.3.2] - 2026-10-07

### Added

- Optional `OpenOptionsBuilder::open_timeout(Duration)` bounds one startup
  attempt across lease acquisition, recovery, and runtime admission. The
  default remains unbounded; timed-out work retains ownership until it settles.
- Metadata publication and recovery work diagnostics, with controlled benchmark
  profiles for checkpoint cost, write pressure, and cold recovery.

### Fixed

- Invalidate cloud transactions after lease loss: point reads, new scans, and
  the next advance of an active iterator return `MidgeError::Fenced`. Reads
  recheck authority after blocking I/O, before exposing its result. A successor
  can reclaim remote SSTs pinned only by the predecessor's process.
- Preserve renewed monotonic lease validity when a watchdog resumes with an
  older deadline, and preserve bounded cloud lease-read retries.
- Surface terminal fencing to stalled admission and flush callers. Explicit
  cloud flush barriers progress safely through retry and compaction pressure.
- Carry the initiating manual compaction deadline through queueing, computation,
  name reservation, upload, and metadata publication; retain data and authority
  when a late result cannot establish safe cleanup.
- Route only real compaction caller responses, retain optional WAL cleanup
  during shutdown, and report clean filesystem lease release accurately.
- Bound cloud WAL coverage and warm recovery key reconstruction without
  weakening exact SST coverage or conservative replay fallback.
- Preserve committed YCSB key inventories and count successful completed work
  for benchmark and soak progress.

### Upgrade and rollback

- Database FORMAT 4, SST V4, CLI JSON schema version 1, and cloud control formats
  are unchanged from `0.3.1`. Rust API additions preserve existing call sites
  and error variants; cloud reads after lease loss now explicitly return
  `Fenced`. Applications must discard those transactions and reopen under a
  healthy writer. See the [migration guide](docs/operations/migration-guide.md).
- No offline export/import is required for a `0.3.1` database. Stop writes,
  complete shutdown, preserve a verified database/prefix copy, and qualify
  recovery on a separate copy before cutover.
- Rollback is supported with constraints: restore the preserved pre-upgrade
  copy and use `0.3.1`. Salvage-mutated databases are excluded; writes after the
  preserved copy require separate reconciliation. The older binary also
  restores the lease, progress, and read-authority defects fixed here.

### Known risks

- Midge remains pre-1.0 and single-process. Sqrzl continuously qualifies provider
  protocols; deployment-specific credentials, network policy, quotas, and
  capacity still require application qualification.
- Full-manifest checkpoint cost and sustained write-pressure/cold-recovery
  profiling remain follow-up work in #752, #753, and #754. This release does not
  change the checkpoint policy or promise production latency bounds.

## [0.3.1] - 2026-10-02

### Fixed

- Reject expired writer authority synchronously and recheck it after blocking
  preparation before subsequent WAL, metadata, catalog, DDL, and cleanup work.
- Treat epoch-regressed cloud WAL as a salvage replay hole; persist the complete
  unreplayed sequence floor before quarantining sources or retiring catalog entries.
- Make interrupted restore attempts retryable using unique attempt-owned stages,
  and reject overlapping database, backup artifact, and restore paths before mutation.
- Carry CloudAck memory-contention provenance with its failed reservation so
  concurrent catalog work cannot erase retry classification.

### Changed

- Index resident transaction intents, merge spilled point sources with a heap,
  and sweep range tombstones during forward and reverse scans.
- Retain bounded SST reader and decoded-block locality during cloud WAL coverage
  probes. Proof failures continue to fall back conservatively to WAL replay.

### Upgrade and rollback

- Public Rust signatures, error variants, CLI JSON schema version 1, database
  FORMAT 4, SST V4, and cloud control formats are unchanged from `0.3.0`.
  Stop writes, complete shutdown, preserve a verified copy, and test recovery
  on a separate copy before upgrading. See the [migration guide](docs/operations/migration-guide.md).
- Rollback is supported with constraints: use the preserved pre-upgrade database
  and `0.3.0`. Salvage-mutated databases are excluded from rollback claims;
  writes after the preserved copy require separate reconciliation.

### Known risks

- Midge remains pre-1.0 and single-process. Spilled point reads still probe
  matching spill runs; recovery reader caches are cleared at checkpoints.
  Qualify application-specific memory, transaction, and cloud workloads.
- Full-manifest checkpoints still amplify cumulative bytes as SST cardinality
  grows. #670 was measured and deferred; no checkpoint policy change ships.

## [0.3.0] - 2026-09-29

### Added

- Versioned `midge verify --json` success and error output as schema version 1,
  with operator guidance and full-shape compatibility regressions.

### Changed

- Provider-backed cloud metadata now commits immutable control-file generations
  through a version 2 lease descriptor. The version 2 DDL registry is fenced to
  the successor's lease epoch before recovery or serving requests. Legacy
  provider-backed prefixes cannot be opened in place by this release.
- Default block-cache LRU admits concurrent shard reads and samples repeat-hit
  recency; its first hit after admission still refreshes recency. TinyLFU and
  CLOCK-Pro retain per-hit updates. Cache accounting charges eight additional
  bytes per resident entry.

### Fixed

- Persist newly created database directories before startup writes and reject
  unsafe rooted directory layouts.
- Check complete manifest and SST key bounds against decoded SST entries during
  storage verification, preventing a healthy verdict for hidden persisted keys.
- Keep prior SST coverage values charged until replacement during cloud WAL
  replay and avoid copying WAL payloads for coverage probes.
- Bootstrap a fresh provider-backed cloud database with a complete committed
  metadata generation before recovery. Interrupted pre-commit attempts can
  retry with the same empty cache; unproved local or remote history still fails
  closed.
- Keep the writer lease until cloud WAL uploads and prune workers exit, even
  when shutdown times out while an upload remains blocked. Retry a failed
  lease release using its original epoch after local write authority is closed.

### Upgrade and rollback

- Local database FORMAT 4 and SST V4 are unchanged. Stop writes, complete
  shutdown, and preserve a verified database copy before upgrading from
  `0.2.0`; use `midge verify` and application recovery tests on a separate copy.
  Consumers of earlier unversioned CLI JSON must recognize schema version 1.
- Provider-backed cloud databases with legacy lease and mutable metadata
  require offline logical export using `0.2.0` and import with `0.3.0` into a
  new empty prefix and fresh local cache. Preserve application metadata needed
  to reconstruct TTL expiration. See the [migration guide](docs/operations/migration-guide.md).
- Rollback is supported with constraints: restore the preserved pre-upgrade
  local database or original cloud prefix with `0.2.0`. Binary rollback against
  a `0.3.0` cloud prefix is unsupported; writes after cutover need separate
  reconciliation.

### Known risks

- Midge remains pre-1.0 and single-process. The block-cache change improves
  cache-local read scaling, but paired synthetic Engine diagnostics did not
  establish an end-to-end latency gain and recorded higher sampled p99 in
  some workloads. Review the [support matrix](docs/development/support-matrix.md)
  and qualify deployment-specific cloud configuration and capacity.

## [0.2.0] - 2026-09-27

### Added

- Consistent-cut backup and restore.
- Automatic repair of underfull overlapping L1+ SST levels. Reads stay
  conservative until the replacement set is durably published.

### Changed

- Narrowed the crate surface to canonical re-exports and the prelude. The
  previously doc-hidden implementation modules and test hooks are now private
  or available only through the non-default `internal-testing` feature; they
  were not stable API.
- Database FORMAT 4 stores manifest key bounds as hex strings. FORMAT 3
  databases remain readable and are upgraded to FORMAT 4 in place on writable
  open; both formats use SST V4.
- Large same-level overlap repairs use bounded, non-authoritative local scratch
  runs. Capacity or I/O failures leave the original SST set authoritative.

### Fixed

- Hardened WAL sealing, lease fencing, recovery, manifest publication, and
  compaction cleanup across failure and restart boundaries.
- Avoided rebuilding SST read metadata for manifest changes that do not alter
  the file set.

### Upgrade and rollback

- Stop writers, complete shutdown, and preserve a verified copy of the entire
  database directory and relevant cloud prefix before upgrading from `0.1.1`.
  Test the upgrade against a separate copy first. A writable `0.2.0` open
  upgrades a FORMAT 3 database marker to FORMAT 4 in place.
- Rollback is supported with constraints: restore the pre-upgrade copy and use
  the prior binary. Directly opening a database that `0.2.0` has writable-opened
  with `0.1.1` is unsupported. Writes made after the preserved copy was taken
  are not present in that rollback copy.

### Known risks

- Midge remains pre-1.0 and single-process. Review the support matrix for the
  qualified topology and runtime guarantees before production use.

## [0.1.1] - 2026-09-13

### Fixed

- WAL fsync and rotation now use one explicit transition protocol across the
  filesystem writer, durability coordinator, sequence frontiers, sealed-segment
  ownership, accounting, health, and waiter completion. Assertion-only and
  empty synchronous transactions can no longer advance only the actor's
  generation and strand the following buffered batch.
- A WAL whose active file was sealed but whose replacement writer could not be
  installed now enters a deterministic fenced state. Subsequent durable writes
  are rejected before sequence allocation or memtable mutation, the sealed
  segment is retained, and restart recovery replays the accepted durable prefix.
- CloudAsync sealing retains every successfully rotated segment in inflight and
  upload-backlog ownership when coordinator or later bookkeeping fails. Pending
  accounting is settled, affected waiters fail exactly once, and restart
  recovery resumes the retained upload obligation.
- WAL transition preflight failures restore the reversible actor state, local
  disk admission failures before the first spilled WAL frame remain retryable,
  and stale-writer authority failures fence further durable work.
- Runtime configuration rejects local/cloud WAL policy switches before applying
  any other requested fields, preserving the atomic update contract.

### Changed

- Development and stress qualification now consume the published
  `cntryl-stress` crate instead of a repository-local path dependency. This does
  not change the runtime dependency graph of `cntryl-midge`.

### Upgrade and rollback

- This patch does not change the public API, error variants, WAL records,
  manifest data, SST format, or database format. After a clean shutdown,
  existing `0.1.0` databases and cloud prefixes can be opened directly with
  `0.1.1`.
- Rollback to `0.1.0` is supported after a clean `0.1.1` shutdown because this
  patch writes the same persisted formats. Preserve the database directory and
  cloud prefix before changing binaries as required by the general migration
  procedure.

### Known risks

- Midge intentionally sacrifices in-process write availability after an
  ambiguous WAL transition. The actor reports degraded/fenced health and
  requires restart recovery rather than risking an acknowledgement that the
  surviving persistence state cannot prove.
- A failure after durability is completely committed but before the operation
  returns can still be reported as a false negative. Retrying must remain
  idempotent; the implementation does not report success before durability.

## [0.1.0] - 2026-09-10

### Changed

- The derived compaction memory pool now uses 20% rather than 10% of the total
  engine memory budget (still capped at 256 MiB). This keeps bounded remote SST
  publication live under the default four-file merge fan-in; the total engine
  budget is unchanged, so automatic memtable and read-cache capacity adjust
  downward accordingly.
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

- Cloud compaction partitions now leave enough of the shared maintenance pool
  for live merge inputs. Transient SST-upload failures no longer leave L0
  recovery repeatedly failing publication until writers time out.
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

### Rollback

- This is the first published release. Rollback to an unpublished build has no
  supported in-place compatibility contract; preserve a backup or export data
  before changing binaries.

### Known risks

- Midge remains pre-1.0 and single-process. Production suitability is limited
  to the capabilities and qualification conditions in the support matrix; API,
  operational, and on-disk compatibility may change in a future 0.x minor release.

[Unreleased]: https://github.com/cntryl/midge/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/cntryl/midge/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/cntryl/midge/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/cntryl/midge/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/cntryl/midge/releases/tag/v0.1.0
