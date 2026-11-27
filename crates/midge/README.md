# cntryl-midge

Midge is an embedded Rust key-value engine built on a log-structured merge tree.
This crate provides the application API for transactions, column families,
ordered scans, backup and restore, and storage verification. It also owns the
runtime that coordinates writes, compaction, and recovery.

## Use Midge

Add `cntryl-midge` to your application with `cargo add cntryl-midge`. For a
local checkout, use `crates/midge` as the Cargo dependency path.

```rust
use std::time::Duration;

use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = Engine::open(OpenOptions::local("./db").build()?)?;
    let cf = engine.get_column_family("default").expect("default column family");
    let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    tx.put(b"hello".to_vec(), b"world".to_vec(), None)?;
    tx.commit(WriteOptions::sync())?;
    engine.shutdown(Duration::from_secs(5))?;
    Ok(())
}
```

Use `OpenOptions::in_memory()` for ephemeral data, `OpenOptions::local(path)`
for disk-backed storage, and `OpenOptions::cloud(...)` for provider-backed
storage. Default features enable AWS S3, Azure Blob Storage, Google Cloud
Storage, and OCI. For memory or local storage only, set
`default-features = false` in your dependency. Use `telemetry` for tracing
integration or `telemetry-otlp` for OTLP export.

The `midge` binary verifies an existing database with
`midge verify [--json] <db-path>`.

## Work on the crate

From the repository root, run the [quick-start example](examples/documented_quick_start.rs)
and engine API integration tests.

```sh
cargo run --locked -p cntryl-midge --example documented_quick_start
cargo test --locked -p cntryl-midge --test engine_api
```

The `internal-testing` feature exposes implementation details for repository
tests, benchmarks, and fuzz targets. Applications should use the crate-root
exports or the prelude.

See the [API guide](../../docs/user-guides/api-guide.md) and
[transaction durability contract](../../docs/user-guides/transaction-durability-contract.md).
