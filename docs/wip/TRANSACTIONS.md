# Transactions & Snapshots

This short doc shows: how to choose per-transaction isolation and how to use the new `Snapshot::get` convenience method.

## Isolation levels

Midge supports two isolation modes per transaction:

- `Snapshot` (default): Each transaction reads a consistent point-in-time snapshot using an engine sequence number. Reads are tracked for conflict detection; concurrent commits that modify the keys read by the transaction may cause commit to fail (detect read-write conflicts).

- `ReadCommitted`: Transaction reads the latest committed value and _does not_ track reads for conflict detection. Read-then-commit races that would otherwise be detected by Snapshot may not conflict under ReadCommitted.

When opening a transaction, you can pass the desired isolation via `begin_transaction_with_options`.

Example (explicit ReadCommitted):

```rust
use cntryl_midge::MidgeEngine;
use cntryl_midge::IsolationLevel;

let engine = MidgeEngine::open(MidgeOptions::default())?;
let cf = engine.default_column_family();

let mut txn = engine
  .begin_transaction_with_options(&cf, None, 1024 * 1024, IsolationLevel::ReadCommitted)
  .unwrap();

txn.put(b"key", b"value")?;
let commit_result = txn.commit();
```

If no `IsolationLevel` is passed, the engine uses `IsolationLevel::Snapshot` by default.

## Snapshot convenience: `Snapshot::get`

A snapshot is a point-in-time view of the DB that you can use to perform consistent reads across multiple keys or to take consistent backups.

We added a convenience method on `Snapshot` to simplify snapshot reads from tests and other code paths:

```rust
let snapshot = engine.snapshot();
let cf = engine.default_column_family();
let maybe_value = snapshot.get(&engine, &cf, b"some-key")?; // Option<Bytes>
```

This is equivalent to calling `engine.get_at(cf, key, snapshot.seq())`, but more concise in tests and examples.

## Next Steps & Notes

- Consider adding `Snapshot::scan` to add a similar convenience for range scans.
- Document `ReadCommitted` semantics strongly: read-tracking is disabled for this isolation level and certain types of conflict detection will not apply.
- Tests already validate Snapshot vs ReadCommitted semantics for read/write conflicts in `tests/transaction_isolation.rs`.

