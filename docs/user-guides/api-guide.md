# API guide

The stable package-facing surface is the set of types re-exported at the crate
root. The [Rust API documentation](https://docs.rs/cntryl-midge) and executable
[quick-start example](../../examples/documented_quick_start.rs) are the most
precise references.

## Open and close

```rust
let mut engine = Engine::open(OpenOptions::local("./db").build()?)?;
let cf = engine.get_column_family("default").unwrap();
// ... use transactions ...
engine.shutdown(Duration::from_secs(30))?;
```

`OpenOptions` also provides `in_memory`, `cloud`, and `cloud_simulated`. Build
options before opening. Public builder controls include `goal`,
`memory_budget`, `workload`, `with_memtable_size_limit`,
`with_memtable_flush_threshold`, `transaction_memory_pool_size`,
`block_cache_policy`, `cloud_write_policy`, `background_compaction`,
`recovery_policy`, and `storage_io_timeout`.

## Transactions

```rust
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.put(b"key".to_vec(), b"value".to_vec(), None)?;
tx.delete(b"old".to_vec())?;
tx.delete_range(b"prefix/".to_vec(), b"prefix0".to_vec())?;
tx.commit(WriteOptions::sync())?;

let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
let value = read.get(b"key")?;
let query = Query::new().start_key(Bytes::from_static(b"a"))
    .end_key(Bytes::from_static(b"z"));
let mut rows = read.scan(&query)?;
while let Some((key, value)) = rows.next().transpose()? {
    println!("{key:?} = {value:?}");
}
```

`delete_range` uses an inclusive start and exclusive end and is only a
transaction operation. A read-only transaction cannot write. A transaction is
bound to one column family. `rollback()` explicitly abandons buffered writes;
dropping an uncommitted transaction has the same write-discard effect.

## Durability and cloud

`WriteOptions::sync`, `buffered`, and `best_effort` describe local behavior.
For cloud-backed storage, `cloud_async` returns after the local barrier while
upload continues, and `cloud_strict` waits for the cloud upload. Cloud provider
configuration is exposed through `CloudProviderConfig` and feature-specific
credential source types; see [cloud setup](../operations/cloud-setup.md).

## Errors and diagnostics

Operations return `MidgeResult<T>`. The public error variants include `Io`,
`NotFound`, `InvalidArgument`, `Corruption`, `NotSupported`, `RecoveryFailed`,
`WriteConflict`, `Timeout`, and resource/backpressure errors. Runtime,
recovery, read-amplification, storage-layout, and verification snapshots are
available through the methods documented on `Engine`.

For crash boundaries and recovery handling, use the [durability contract](transaction-durability-contract.md)
and [operator runbook](../operations/operator-runbook.md).
