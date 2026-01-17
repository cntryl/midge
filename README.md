[![CI](https://github.com/cntryl/midge/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/cntryl/midge/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/cntryl-midge.svg)](https://crates.io/crates/cntryl-midge)
[![license](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

# Midge

Midge is a small, fast, embedded LSM-tree key/value engine for Rust. It provides a simple, explicit transaction API so you can open an engine, write keys, and read them back safely.

## Quick start ✅

Add to your `Cargo.toml` (check crates.io for the latest version):

```toml
[dependencies]
cntryl-midge = "<latest-version>"
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
let mut iter = tx.scan(&Query::new()).unwrap();
while let Some((k, v)) = iter.next() {
    println!("k={:?} v={:?}", k, v);
}
```
