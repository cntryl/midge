[![CI](https://github.com/cntryl/midge/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/cntryl/midge/actions/workflows/ci.yml)

# Midge

Midge `0.1.0` is an embedded Rust 2021 LSM key-value engine. It uses explicit
transactions and explicit write durability policies. The crate MSRV is Rust
`1.97`.

The supported storage constructors are `OpenOptions::in_memory()`,
`OpenOptions::local(path)`, `OpenOptions::cloud(...)`, and
`OpenOptions::cloud_simulated(...)`. The real cloud providers are optional,
pre-1.0 integrations. `CloudSimulated` is a local filesystem simulation for
cloud lifecycle tests; it is not a cloud service.

Midge is single-process embedded storage. The 0.x API and operational contract
may change. Cloud-backed production use is not endorsed by this release line.

## Quick start

```rust
use std::time::Duration;
use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};

let mut engine = Engine::open(OpenOptions::local("./db").build()?)?;
let cf = engine.get_column_family("default").unwrap();
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.put(b"hello".to_vec(), b"world".to_vec(), None)?;
tx.commit(WriteOptions::sync())?;
engine.shutdown(Duration::from_secs(5))?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

The executable canonical example is [examples/documented_quick_start.rs](examples/documented_quick_start.rs).

## Reading paths

- Users: [overview](docs/user-guides/overview.md) → [quick start](docs/user-guides/quick-start.md) → [API guide](docs/user-guides/api-guide.md).
- Durability: [transaction durability contract](docs/user-guides/transaction-durability-contract.md) → [recovery internals](docs/development/recovery-internals.md).
- Contributors: [architecture](docs/development/architecture.md) → [invariants](docs/development/storage-invariants.md) → [testing](docs/development/testing.md).
- Operators: [operator runbook](docs/operations/operator-runbook.md) and [cloud setup](docs/operations/cloud-setup.md).

See [docs/README.md](docs/README.md) for the complete current inventory.
