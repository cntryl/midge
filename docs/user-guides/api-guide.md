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
`recovery_policy`, `storage_io_timeout`, `runtime_response_timeout`,
`on_lease_loss`, `lease_clock_skew_tolerance`, and `ttl_clock`.

## Runtime response deadlines

Every Engine operation that submits a `RuntimeMsg` and waits for its response is
bounded. Transactions, column-family changes, flushes, compaction, metrics,
storage-layout capture, and post-start configuration use
`runtime_response_timeout`. The default is 60 seconds. When
`storage_io_timeout` is raised without an explicit runtime override, Midge
derives the enclosing deadline as at least 30 seconds longer than the storage
deadline; an explicit runtime timeout must be greater than
`storage_io_timeout`.

APIs that already accept a timeout, including `shutdown`, `verify_storage`,
`wait_for_write_stall_clear`, and `get_runtime_metrics_with_timeout`, honor the
caller's operation-specific budget. Fire-and-forget messages do not wait for a
response. Engine drop hands teardown to a reaper which may continue waiting for
durability workers so writer fencing is not released early.

Engine open and recovery happen before the event loop accepts runtime messages,
so `runtime_response_timeout` is not an aggregate startup deadline. Individual
provider callbacks use `storage_io_timeout`; embedders that need to bound total
startup should retain an outer process or startup watchdog.

A configured runtime-response `MidgeError::Timeout` identifies the request kind
and request ID. The timeout removes the caller's response route but does not
cancel work already accepted by the runtime; a mutating operation can still
complete later. Use recovery and runtime diagnostics to determine its outcome
before retrying.

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
tx.assert_value(b"guard".to_vec(), Some(b"expected".to_vec()))?;
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

`assert_value(key, expected)` is an opt-in compare-and-set guard. It compares
against the transaction's frozen start snapshot and rejects any later point or
covering range mutation before commit serialization, regardless of
`ConflictPolicy`; pass `None` to require absence. Pending writes in the same
transaction do not change the asserted snapshot value. Assertions share the
bounded transaction memory pool, so callers must handle a possible
`MidgeError::ResourceLimit` and must not proceed as though a failed assertion
registration installed a guard. See the
[transaction durability contract](transaction-durability-contract.md)
for the exact isolation and durability boundaries.

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

Cloud WAL recovery uses the catalog-authoritative, epoch-scoped layout described
in the [migration guide](../operations/migration-guide.md).
An older segment-only prefix is not opened or migrated automatically. Preserve
the old prefix, export it with a compatible binary, and import into a new prefix.

`on_lease_loss` registers a process-local notification for the transition to a
fenced writer. Begin orderly shutdown from the callback without blocking it;
the engine remains readable but rejects new writes. Shutdown timeouts bound the
caller's wait without releasing fencing ahead of still-running durability work.

## Errors and diagnostics

Operations return `MidgeResult<T>`. The public error variants include `Io`,
`NotFound`, `InvalidArgument`, `Corruption`, `NotSupported`, `RecoveryFailed`,
`WriteConflict`, `Timeout`, and resource/backpressure errors. Runtime,
recovery, read-amplification, storage-layout, and verification snapshots are
available through the methods documented on `Engine`. `Engine::metrics()`
returns a cloneable `EngineMetrics` facade, including
`get_runtime_metrics_with_timeout`; `Engine::storage_verifier()` returns a
`StorageVerifier` for online and offline integrity checks. The existing direct
`Engine` methods remain available.

For crash boundaries and recovery handling, use the [durability contract](transaction-durability-contract.md)
and [operator runbook](../operations/operator-runbook.md).

## Persisted compression

Current SST blocks use raw, LZ4, Zstd level 3, or Zstd level 9 encoding. Every
block has a mandatory checksummed trailer, and unknown or removed codec codes
are rejected without a raw-byte fallback. Compression is intentionally skipped
for inputs smaller than 256 bytes because framing and codec overhead dominate
at that size; this is a size policy, not a heuristic acceptance fast path.
