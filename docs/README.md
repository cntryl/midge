# Midge Documentation

## Structure

Documentation is organized into three categories:

### `dev/` — Code-Level Guidelines

Standards and conventions for writing production code, tests, and benchmarks.

- `architecture-guidelines.md` — Core architectural principles
- `bench-guidelines.md` — Benchmark design and execution standards
- `code-guidelines.md` — Production code standards
- `test-guidelines.md` — Test organization and naming conventions

### `features/` — User-Facing Features

How to use Midge: APIs, configuration, and operational patterns.

- `basics.md` — Getting started
- `overview.md` — High-level architecture
- `cloud-backend-swap.md` — Switching cloud providers
- `cloud-configuration-matrix.md` — Cloud setup matrix
- `hybrid-storage.md` — Local + cloud hybrid mode
- `durability-modes-explained.md` — Sync modes and guarantees
- `durability-profiles.md` — Pre-configured durability profiles
- `cloud-integration/provider-detection.md` — Detecting supported cloud providers

### `internal/` — System Internals

Deep dives into how Midge works: invariants, algorithms, and data structures.

- `glossary.md` — Terms and acronyms
- `invariants/lsm-invariants.md` — LSM structural and behavioral guarantees
- `invariants/wal-invariants.md` — WAL durability and replay guarantees
- `invariants/compaction-invariants.md` — Compaction safety guarantees
- `invariants/manifest-invariants.md` — Manifest sequencing and durability guarantees
- `lock-ordering.md` — Deadlock prevention hierarchy
- `merge-iterator.md` — Merging iterator algorithm
- `manifest.md` — Manifest file format and handling
- `lock-file.md` — Database lock file details
- `performance.md` — Internal performance tuning and guidance
- `benchmarks-ycsb.md` — YCSB benchmark guidance
- `file-formats/` — On-disk formats (SST, WAL, TLV registry)

## Navigation

- New to Midge? Start with `features/basics.md` and `features/overview.md`
- Contributing code? Read `dev/` guidelines first
- Debugging an issue? Check `internal/invariants.md` and `internal/lock-ordering.md`
- Performance tuning? See `internal/performance.md` and `dev/bench-guidelines.md`
