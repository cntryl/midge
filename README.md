[![CI](https://github.com/cntryl/midge/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/cntryl/midge/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/cntryl-midge.svg)](https://crates.io/crates/cntryl-midge)
[![license](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

# Midge

The only embedded Rust database that runs the same code against local disk, S3, Azure Blob, and GCS — no server, no surprises.

Built for Rust services and edge daemons that need reliable embedded storage without the ops burden of a separate database.

## Quick start

```toml
[dependencies]
cntryl-midge = "0.1"
```

```rust
use cntryl_midge::prelude::*;

let engine = Engine::open(OpenOptions::local("./db").build())?;
let cf = engine.create_column_family("cf1")?;

// Write
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.put(b"hello".to_vec(), b"world".to_vec(), None)?;
engine.commit(tx, WriteOptions::sync())?;

// Read
let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
let value = tx.get(b"hello")?; // Option<Bytes>
```

That's it. Everything — local disk, cloud storage, column families — follows the same pattern.

## Is Midge a good fit?

**Good fit if you're building:**

- A Rust service or daemon that needs embedded storage without running a separate database
- An app that runs locally in dev and in the cloud in production, with no storage code changes
- Something where you need to _know_ exactly when data is durable

**Probably not the right fit if:**

- You need multi-process access to the same store
- You're not writing in Rust (no stable non-Rust client yet)
- You need the absolute highest throughput — RocksDB will beat Midge in a benchmark

## Why Midge?

**It stores data wherever you need it.** Local disk for development, S3 or Azure Blob in production — same API, same code, swap the open options. Most embedded databases are stuck on the local filesystem.

**Transactions are explicit and obvious.** Every read and every write happens inside a transaction you own. No hidden flushes, no background writer surprises, no wondering when your data lands. You call `commit`, it commits.

**Durability is your choice.** Use `WriteOptions::sync()` when you need a guarantee. Use `WriteOptions::default()` when you want throughput. The control is yours and the behavior is documented.

**It's fast enough.** Up to 160 MB/s on local disk, 46 MB/s on cloud storage. Not the fastest embedded engine in a benchmark — but predictable under real workloads, which matters more.

**It's designed to be trustworthy.** 1,500+ tests including deterministic crash recovery scenarios and enforced test structure validation. The v1 API follows semver — no surprises in patch releases. CI runs the full test suite on every commit across Linux, macOS, and Windows. Built for infrastructure, not prototypes.

## Common operations

**Put**

```rust
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.put(b"key".to_vec(), b"value".to_vec(), None)?;
engine.commit(tx, WriteOptions::sync())?;
```

**Get**

```rust
let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
let value = tx.get(b"key")?; // Option<Bytes>
```

**Delete**

```rust
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.delete(b"key".to_vec())?;
engine.commit(tx, WriteOptions::sync())?;
```

**Delete range**

```rust
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.delete_range(b"start".to_vec(), b"end".to_vec())?;
engine.commit(tx, WriteOptions::sync())?;
```

**Scan**

```rust
let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
let mut iter = tx.scan(&Query::new())?;
while let Some((k, v)) = iter.next() {
    println!("{:?} = {:?}", k, v);
}
```

## Documentation

**[Full Documentation Hub →](docs/)**

### Getting Started

- **[Quick Start](docs/user-guides/quick-start.md)** — 5-minute hello-world example
- **[Overview](docs/user-guides/overview.md)** — What is Midge, when to use it
- **[API Guide](docs/user-guides/api-guide.md)** — Complete API reference
- **[FAQ](docs/user-guides/faq.md)** — Common questions and answers

### Operations

- **[Performance Tuning](docs/operations/performance-tuning.md)** — Optimize for your workload
- **[Cloud Setup](docs/operations/cloud-setup.md)** — Deploy to S3, Azure, GCS
- **[Durability](docs/user-guides/durability.md)** — Crash recovery and guarantees

### Contributing

- **[Contributing Guide](CONTRIBUTING.md)** — How to contribute
- **[The Big Idea](docs/development/the-big-idea.md)** — Design philosophy
- **[Architecture](docs/development/architecture.md)** — Technical internals
- **[Testing](docs/development/testing.md)** — Test conventions
- **[Benchmarks](docs/development/benchmarks.md)** — Performance benchmarking
