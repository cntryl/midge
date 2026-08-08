# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and version numbers use [Semantic Versioning](https://semver.org/spec/v2.0.0.html) formatting.

Midge is currently in the 0.1 release line. Compatibility expectations for pre-1.0 releases are defined in [docs/development/stability-policy.md](docs/development/stability-policy.md).

## [Unreleased]

### Changed
- **Breaking:** database FORMAT 3 now requires SST V4. V4 uses a fixed,
  self-identifying checksummed footer, mandatory checksummed block trailers,
  exact block-handle validation, and explicit TTL presence. FORMAT 1/2 and SST
  V1-V3 require logical export with the old binary and import into a new
  database; there is no in-place migration or legacy fallback.
- Scan iterators retain stable SST handles for the scan lifetime on supporting
  filesystem backends and expose explicit active, exhausted, and failed
  states. Terminal read errors remain sticky instead of becoming clean
  exhaustion; path-only backends fail visibly if the backing path disappears.
- Provider-backed cloud storage now defaults to one bucket/container and one
  database prefix. Advanced deployments can route WAL, SST, and control
  objects separately with `CloudStorageTopology` and `OpenOptions::cloud_multi`.

### Added
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
