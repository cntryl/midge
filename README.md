[![CI](https://github.com/cntryl/midge/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/cntryl/midge/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/cntryl-midge.svg)](https://crates.io/crates/cntryl-midge)
[![license](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

# Midge

Midge is a high-performance, embedded LSM-tree key/value engine for Rust with an explicit transaction API.

## Should I use this?

Use Midge if you want:

- An embedded database (no separate server process) you ship with your Rust app.
- LSM-tree storage with range scans, delete-range tombstones, and column families.
- A transaction-scoped API (all reads/writes happen inside explicit transactions).
- Explicit durability choices per commit (e.g. `WriteOptions::sync()`).

## Quick start ✅

Add to your `Cargo.toml`:

```toml
[dependencies]
cntryl-midge = "1"
```

Example (minimal):

```rust
use cntryl_midge::prelude::*;

fn main() -> Result<(), MidgeError> {
    // Open a local engine directory
    let engine = Engine::open(OpenOptions::local("./db").build())?;
    let cf = engine.create_column_family("cf1")?;

    // Write
    let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    tx.put(b"key".to_vec(), b"value".to_vec(), None)?;
    engine.commit(tx, WriteOptions::sync())?;

    // Read
    let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    let value = tx.get(b"key")?;
    println!("got: {:?}", value);

    Ok(())
}
```

Run the test suite:

- `cargo test`
- Optional style check: `python ./scripts/validate_tests.py --summary` (reports some legacy violations today)

## Common operations ✨

Here are short, copy-pasteable snippets for common tasks (transactional):

- Put (write a key)

```rust
// write within a ReadWrite transaction and commit
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.put(b"key".to_vec(), b"value".to_vec(), None)?;
engine.commit(tx, WriteOptions::sync())?;
```

- Get (read a key)

```rust
let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
let value = tx.get(b"key")?; // returns Option<Bytes>
if let Some(v) = value { println!("value: {:?}", v); }
```

- Delete (single key)

```rust
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.delete(b"key".to_vec())?;
engine.commit(tx, WriteOptions::sync())?;
```

- Delete range (exclusive end)

```rust
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.delete_range(b"start".to_vec(), b"end".to_vec())?;
engine.commit(tx, WriteOptions::sync())?;
```

- Scan (range / iterate)

```rust
let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
let mut iter = tx.scan(&Query::new())?;
while let Some((k, v)) = iter.next() {
    println!("k={:?} v={:?}", k, v);
}
```

## Documentation

### Getting Started
- **[API Guide](docs/API_GUIDE.md)** - Complete guide to using Midge (OpenOptions, transactions, WriteOptions, queries)
- **[Cloud Setup](docs/CLOUD_SETUP.md)** - Configuring S3, Azure, GCS, Cloudflare R2, and other cloud providers
- **[Recovery & Durability](docs/RECOVERY.md)** - Durability guarantees, crash scenarios, and recovery behavior

### Architecture & Design
- **[The Big Idea](docs/THE_BIG_IDEA.md)** - Philosophy, design decisions, and core principles
- **[Architecture](docs/ARCHITECTURE.md)** - Technical implementation guide for contributors

### Contributing
- **[Contributing Guide](CONTRIBUTING.md)** - How to contribute code, tests, and documentation

### More
- **[Testing](docs/TESTING.md)** - Test conventions and workflows
- **[Benchmarks](docs/BENCHMARKS.md)** - Benchmark tiers, rules, and how to run them
- **[Performance Tuning](docs/PERFORMANCE_TUNING.md)** - High-level tuning guidance
