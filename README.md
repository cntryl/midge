# Midge

[![CI](https://github.com/cntryl/midge/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/cntryl/midge/actions/workflows/ci.yml) [![crates.io](https://img.shields.io/crates/v/cntryl-midge.svg)](https://crates.io/crates/cntryl-midge) [![docs.rs](https://docs.rs/cntryl-midge/badge.svg)](https://docs.rs/cntryl-midge) [![license](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Midge is an embedded LSM-tree storage engine written in Rust.

Quickstart

Install:

```bash
cargo add cntryl-midge
```

Examples:

- See `examples/basic_usage.rs` and `examples/smart_config.rs` for usage.

Run tests & validator:

```bash
cargo test
python scripts/validate_tests.py --summary
```

License: Apache-2.0 (see `LICENSE`)


- **High-Level Configuration**: Automatic parameter derivation from performance goals (Latency, Throughput, Cost)
- **Column Families**: Logical partitioning of keyspace with independent configuration
- **Transactions**: ACID guarantees with snapshot isolation
- **Snapshots**: Point-in-time consistent views for backups and analytics
- **Compaction Filters**: User-defined logic to drop or transform keys during compaction
- **Range Tombstones**: Efficient deletion of key ranges
- **Bloom Filters**: Reduce read amplification with probabilistic filters
- **Write-Ahead Log (WAL)**: Durability with configurable sync modes
- **Sparse Indexing**: Efficient SST navigation with block-level indexes
- **Rate Limiting**: I/O throttling for background operations
- **Metrics**: Built-in telemetry for monitoring and debugging
- **TTL (Time To Live)**: Automatic key expiration with compaction-based cleanup
  - ✅ Write-time TTL support via `put_with_ttl()` and `insert_with_ttl()`
  - ✅ Batch operations support TTL via `Mutation` API
  - ✅ WAL and SST persistence of expiration metadata
  - ✅ Automatic filter configuration via `MidgeOptions.ttl_seconds`
  - ✅ Compaction-based cleanup removes expired entries from disk

### Experimental/Planned Features

- **Cloud Storage**: Multi-cloud backend support (S3, Azure, GCS)
- **Adaptive Autotuning**: Runtime parameter adjustment based on observed metrics

## Documentation

- Architecture notes and design rationale: `docs/THE_BIG_IDEA.md`
- For examples, see the `examples/` directory (`basic_usage.rs`, `metrics_usage.rs`, `smart_config.rs`).

Additional documentation is available in the `docs/` folder where present; some in-repo docs are work-in-progress.

## Quick Start

Midge provides two APIs for initialization:

1. **High-Level Config API** (Recommended) - Answer 3 questions, get optimized parameters
2. **Low-Level MidgeOptions** - Manual control over every parameter

### High-Level Configuration API ⭐ Recommended

The easiest way to get started is with the high-level Config API. Just answer three questions:

```rust
use cntryl_midge::MidgeEngine;
use cntryl_midge::config::{ConfigBuilder, Goal, Durability};
use bytes::Bytes;

// Build configuration by answering 3 questions:
// 1. What's your performance goal?      → Goal::Latency
// 2. What durability do you need?       → Durability::Steady
// 3. How much memory is available?      → 256 MB
let config = ConfigBuilder::new("./my_db")
    .goal(Goal::Latency)              // Optimize for low latency
    .durability(Durability::Steady)   // Balanced durability (20ms sync)
    .memory_budget_mb(256)            // 256 MB total budget
    .build()?;

// All other parameters (block size, cache, bloom filters, compaction threads,
// WAL buffer, etc.) are automatically derived from your goals!
let engine = MidgeEngine::open_with_config(config)?;

// Use the engine
engine.put(Bytes::from("key1"), Bytes::from("value1"))?;
let value = engine.get(b"key1")?;
```

**Why use the Config API?**

- ✅ **Simple**: 3 questions instead of 20+ parameters
- ✅ **Optimized**: Automatically balances cache, memtables, and compaction
- ✅ **Safe**: Built-in validation prevents unsafe configurations
- ✅ **Adaptable**: Optional runtime autotuning adjusts to workload changes
- ✅ **Inspectable**: Full transparency into derived parameters via `config.plan()`

**Configuration Presets:**

- `Goal::Latency` - Optimize for low p99 latency (<10ms point queries)
- `Goal::Throughput` - Optimize for high MB/s throughput
- `Goal::Cost` - Minimize memory and CPU usage

**Durability Levels:**

- `Durability::Strict` - Fsync on every write (process crash + power loss safe)
- `Durability::Steady` - Fsync at intervals (balanced performance/durability)
- `Durability::CloudPersisted` - Local fsync + verified cloud copy

**Workload Profiles** (optional tuning):

- `WorkloadProfile::WriteHeavy` - >70% writes (larger memtables)
- `WorkloadProfile::ReadMostly` - >70% reads (larger cache)
- `WorkloadProfile::RangeScan` - Frequent range scans (larger blocks)

**Cloud-Native Example:**

```rust
use cntryl_midge::config::{ConfigBuilder, CloudMode, Goal, Durability};

// Configure for cloud-backed storage (S3, GCS, Azure)
let config = ConfigBuilder::new("./local_cache")
    .goal(Goal::Throughput)
    .durability(Durability::CloudPersisted)
    .cloud_mode(CloudMode::Hybrid)      // Auto-detects AWS/GCP/Azure
    .cloud_bucket("my-bucket")
    .build()?;

let engine = MidgeEngine::open_with_config(config)?;
// Writes are automatically persisted to cloud storage!
```

See `examples/smart_config.rs` and `examples/basic_usage.rs` for configuration and usage examples.

### Low-Level MidgeOptions API

For fine-grained control, use the traditional MidgeOptions API:

```rust
use cntryl_midge::{MidgeEngine, MidgeOptions};
use bytes::Bytes;

// Open database with manual configuration
let options = MidgeOptions::default();
let engine = MidgeEngine::open(options)?;

// Write operations
engine.put(Bytes::from("key1"), Bytes::from("value1"))?;
engine.delete(Bytes::from("key2"))?;

// Write with TTL (expires after 60 seconds)
engine.put_with_ttl(
    Bytes::from("session:abc"),
    Bytes::from("data"),
    60
)?;

// Read operations
if let Some(value) = engine.get(&Bytes::from("key1"))? {
    println!("Found: {:?}", value);
}

// Transactions
let mut txn = engine.begin_transaction();
txn.put(Bytes::from("key3"), Bytes::from("value3"), None);
txn.insert(Bytes::from("key4"), Bytes::from("value4"), None);
txn.commit()?;

// Range scans
use cntryl_midge::Query;
let results = engine.scan(Query::new().prefix(Bytes::from("user:")))?;
for (key, value) in results {
    println!("{:?} => {:?}", key, value);
}
```

## Testing

Run the full test suite:

```bash
cargo test
```

Run benchmarks:

```bash
cargo bench
```

Developer commands (use when contributing):

```bash
# Format and lint
cargo fmt
cargo clippy --all-targets -- -D warnings

# Build & test
cargo build
cargo test

# Ensure package contents for publish
cargo package --list
cargo publish --dry-run
```

Contributing & workflow:

- Create a topic branch and open a PR against `main`.
- CI runs on push/PR and enforces clippy + tests; ensure your branch is green.
- Run `cargo test` and `cargo clippy --all-targets -- -D warnings` locally before pushing.

### Writing Tests

**This project enforces strict test quality guidelines.** All tests are validated automatically.

**Quick start:**

- Use `should_*` naming (never `test_*`)
- Add AAA comments for tests >5 lines: `// Arrange`, `// Act`, `// Assert`
- One test = one behavior (split different inputs into separate tests)
- Use VS Code snippets: Type `should` and press Tab

**See:**

- 📋 Quick reference and validator: `testutils/validate_tests.rs` — run `cargo run --bin validate_tests -- --summary` or `cargo run --bin validate_tests -- --file src/your_file.rs`
- 📚 Contributor guidance: `.github/copilot-instructions.md` (quick contributor notes + validation rules)

**Validation:**

```bash
# Run the test suite and meta-tests:
cargo test

# Run the test validator:

# Rust-backed validator (cargo)
cargo run --bin validate_tests -- --summary
cargo run --bin validate_tests -- --file src/your_file.rs

# Python validator (no Rust compile required)
python scripts/validate_tests.py --summary
python scripts/validate_tests.py --file src/your_file.rs
```

## Status

**Production Readiness:** 🟡 Beta

- ✅ **1478 tests passing** (unit + integration + doctests; run `cargo test` locally)
- ✅ CI enforces `cargo clippy --all-targets -- -D warnings` and `cargo test`
- ✅ Crash recovery matrix validation (5-scenario smoke test + 1K/10K proof tests)
- ✅ YCSB benchmark suite (Workloads A, B, C implemented and validated)
- ✅ Core LSM functionality complete
- ✅ Column family support
- ✅ Configuration system with automatic parameter derivation
- ✅ Autotuning metrics integration
- 🚧 Additional features in development

## Publishing 🔖

Checklist to prepare for a crates.io release:

- Bump `version` in `Cargo.toml` and update changelog/release notes (if present).
- Ensure `readme` is set in `Cargo.toml` (this repository includes `README.md`).
- Verify `license` field matches the `LICENSE` file (Apache-2.0).
- Run the validation commands locally:
  - `cargo clippy --all-targets -- -D warnings`
  - `cargo test`
  - `cargo package --list` (inspect files that will be published)
  - `cargo publish --dry-run`
- After dry-run successful, push a release commit and run `cargo publish` when you're ready.

> Tip: CI runs on push/PR to `main` and will fail if clippy or tests fail — make sure the branch is green before publishing.

## License

Midge is licensed under the [Apache-2.0](LICENSE) license.

