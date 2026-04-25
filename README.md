[![CI](https://github.com/cntryl/midge/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/cntryl/midge/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/cntryl-midge.svg)](https://crates.io/crates/cntryl-midge)
[![license](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

# Midge

Midge is an experimental embedded Rust LSM engine for engineers who want to evaluate explicit durability and recovery behavior without standing up a separate database.

It is not production-ready. The current goal is narrower: make the crate understandable and failure-tested enough that experienced engineers are willing to try it in controlled environments.

## Quick start

```toml
[dependencies]
cntryl-midge = "0.1"
```

```rust
use cntryl_midge::prelude::*;
use cntryl_midge::Bytes;

let engine = Engine::open(OpenOptions::local("./db").build())?;
let cf = engine.create_column_family("cf1")?;

let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.put(b"hello".to_vec(), b"world".to_vec(), None)?;
tx.commit(WriteOptions::sync())?;

let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
let value = tx.get(b"hello")?;
```

## What Midge Is Good For

- evaluating an embedded Rust store with explicit write durability choices
- local-disk and cloud-backed experiments that use the same API
- systems work where restart behavior and crash boundaries matter

## What Midge Is Not Claiming Yet

- production readiness
- stable long-term API guarantees
- a polished operational story for broad deployment

## Why Evaluate It

- explicit `sync()`, `buffered()`, `best_effort()`, and cloud durability semantics
- deterministic recovery-oriented tests for WAL, flush, and compaction paths
- public observability and verification APIs such as `get_recovery_metrics()`, `get_runtime_metrics()`, `get_storage_layout()`, and `verify_storage()`

## What To Read Before Trying Midge

- [Storage invariants](docs/development/storage-invariants.md)
- [Storage architecture overview](docs/development/architecture.md)
- [Durability guarantees](docs/user-guides/durability.md)
- [Transaction and durability contract](docs/user-guides/transaction-durability-contract.md)
- [Recovery process](docs/development/recovery-internals.md)
- [Testing and trust matrix](docs/development/testing.md)

Those documents define what `commit()` means, how restart recovery works, and which tests back the guarantees.

## Common operations

**Put**

```rust
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.put(b"key".to_vec(), b"value".to_vec(), None)?;
tx.commit(WriteOptions::sync())?;
```

**Get**

```rust
let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
let value = tx.get(b"key")?;
```

**Delete**

```rust
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.delete(b"key".to_vec())?;
tx.commit(WriteOptions::sync())?;
```

**Delete range**

```rust
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.delete_range(b"start".to_vec(), b"end".to_vec())?;
tx.commit(WriteOptions::sync())?;
```

**Scan**

```rust
let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
let query = Query::new().prefix(Bytes::from_static(b"user:"));
let mut iter = tx.scan(&query)?;
while let Some((k, v)) = iter.next() {
    println!("{:?} = {:?}", k, v);
}
```

## Observability

```rust
let recovery = engine.get_recovery_metrics()?;
println!("WAL records replayed: {}", recovery.wal_recovery_records_replayed);
println!("WAL bytes replayed: {}", recovery.wal_recovery_bytes_replayed);
println!("Intent replay runs: {}", recovery.intent_log_replay_runs);
println!("Intent entries replayed: {}", recovery.intent_log_entries_replayed);

let runtime = engine.get_runtime_metrics()?;
println!("Health: {:?}", runtime.health);
println!("Current sequence: {}", runtime.current_sequence);
println!("Write stalled: {}", runtime.write_stalled);
```

Use these snapshots to confirm what recovery ran at startup and what the runtime is doing now.

## Documentation

- [Full documentation hub](docs/)
- [Quick start](docs/user-guides/quick-start.md)
- [API guide](docs/user-guides/api-guide.md)
- [Transactions and MVCC](docs/transactions-and-mvcc.md)
- [Stability policy](docs/development/stability-policy.md)
- [Testing](docs/development/testing.md)
- [1.0 production contract](docs/development/one-dot-zero-contract.md)
- [1.0 readiness scorecard](docs/development/one-dot-zero-readiness-scorecard.md)
- [Support matrix](docs/development/support-matrix.md)
- [Production runbook](docs/operations/production-runbook.md)

## Current Position

The intended status for Midge right now is:

> Experimental but safe enough for serious engineers to try.

Not:

> Production ready.
