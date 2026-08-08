# Quick start

This walkthrough uses only the re-exported API and mirrors the executable
[`documented_quick_start`](../../examples/documented_quick_start.rs) example.

```rust
use std::time::Duration;
use cntryl_midge::{Bytes, Engine, OpenOptions, Query, TransactionMode, WriteOptions};

let mut engine = Engine::open(OpenOptions::in_memory().build()?)?;
let cf = engine.get_column_family("default").unwrap();

let mut write = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
write.put(b"user:1".to_vec(), b"alice".to_vec(), None)?;
write.put(b"user:2".to_vec(), b"bob".to_vec(), None)?;
write.commit(WriteOptions::sync())?;

let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
assert_eq!(read.get(b"user:1")?, Some(Bytes::from_static(b"alice")));
let query = Query::new().prefix(Bytes::from_static(b"user:"));
let rows = read.scan(&query)?.try_collect()?;
assert_eq!(rows.len(), 2);
drop(read);
engine.shutdown(Duration::from_secs(5))?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Choose storage

Use `OpenOptions::local(path)` for restart-persistent local files,
`OpenOptions::in_memory()` for tests that do not need persistence, and
`OpenOptions::cloud_simulated(cache, bucket, prefix)` for a local simulation of
cloud lifecycle behavior. `OpenOptions::cloud(cache, location)` is
feature-gated provider-backed storage and remains pre-1.0. Provider-backed
cloud mode normally uses one unversioned bucket/container and one database
prefix. Do not age-expire current objects. If versioning is enabled, bound
cleanup of noncurrent versions. Use `OpenOptions::cloud_multi` only when
separate IAM, ownership, or lifecycle boundaries are valuable; see the
[cloud setup guide](../operations/cloud-setup.md).

## Choose commit durability

Every write transaction must pass `WriteOptions` to `commit`:

- `sync()` waits for local synchronous WAL durability.
- `buffered()` uses the local batched WAL policy; it has a bounded crash window.
- `best_effort()` provides no crash durability before data is flushed.
- `cloud_async()` and `cloud_strict()` select asynchronous or waited-for cloud
  upload behavior when using cloud-backed storage.

## Finish cleanly

Release transactions and call `engine.shutdown(timeout)`. A shutdown error or
timeout should be recorded and investigated. The [durability contract](transaction-durability-contract.md)
explains what each acknowledgement means.
