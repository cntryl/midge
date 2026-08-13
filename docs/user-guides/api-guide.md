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

## Column-family lifecycle

`create_column_family` accepts non-empty UTF-8 names up to 255 bytes. Names
containing NUL and the reserved name `default` are rejected.

`drop_column_family` is the safe default: it waits for in-flight flush and
compaction publication, but returns `MidgeError::Busy` while committed data
remains in the active memtable. Flush that column family and retry to avoid an
implicit active-memtable discard; the drop still makes all data in the column
family inaccessible and eligible for reclamation. If discarding committed,
unflushed writes is deliberate, call the explicitly destructive
`drop_column_family_discarding_unflushed` method. Writes ordered after either
drop request are not pulled across its WAL barrier and fail against the
dropped column family.

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

A scan iterator reports `IteratorState::Active`, `Exhausted`, or `Failed`.
Normal end-of-stream moves it to `Exhausted`. A storage or decode error moves
it to `Failed`, and later `next()` calls replay that same terminal error; they
do not turn corruption into an apparently clean end-of-stream. Propagate the
error or break the loop after observing it. On filesystems that support stable
persistent handles, holding a scan also holds the SST handles needed by that
snapshot, so unlinking or replacing a path after the scan begins cannot switch
the iterator to different bytes. Backends without that capability retain
path-based reads and surface a sticky read error if the path disappears.

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
Provider payloads are private-field typed configurations, so invalid
cross-provider credential combinations cannot be assembled through the public
API. `validate()` is side-effect-free; `preflight(CloudPreflightOptions)` is an
explicit read-only deployment check returning a serializable redacted report.

## Errors and diagnostics

Operations return `MidgeResult<T>`. The public error variants include `Io`,
`NotFound`, `InvalidArgument`, `Corruption`, `NotSupported`, `RecoveryFailed`,
`WriteConflict`, `Timeout`, and resource/backpressure errors. Runtime,
recovery, read-amplification, storage-layout, and verification snapshots are
available through the methods documented on `Engine`.

For crash boundaries and recovery handling, use the [durability contract](transaction-durability-contract.md)
and [operator runbook](../operations/operator-runbook.md).

## Persisted compression

Current SST blocks use raw, LZ4, Zstd level 3, or Zstd level 9 encoding. Every
block has a mandatory checksummed trailer, and unknown or removed codec codes
are rejected without a raw-byte fallback. Compression is intentionally skipped
for inputs smaller than 256 bytes because framing and codec overhead dominate
at that size; this is a size policy, not a heuristic acceptance fast path.
