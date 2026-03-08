# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and version numbers use [Semantic Versioning](https://semver.org/spec/v2.0.0.html) formatting.

Midge is currently in the 0.1 release line. Compatibility expectations for pre-1.0 releases are defined in [docs/development/stability-policy.md](docs/development/stability-policy.md).

## [Unreleased]

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

### Documentation
- Architecture guide
- Recovery and durability guide
- Performance tuning guide
- Cloud setup guide
- API guide
- Testing guide
- Benchmarking guide

## [0.1.0] - TBD

### Added
- Initial pre-release version

[Unreleased]: https://github.com/cntryl/midge/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/cntryl/midge/releases/tag/v0.1.0
