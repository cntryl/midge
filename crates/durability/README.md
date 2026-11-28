# cntryl-midge-durability

Durability owns the persisted records and writer authority that Midge uses to
recover a database.

- `wal` owns write-ahead log framing, encoding, segment handling, and replay.
- `metadata` owns the manifest, journal, intent log, and persisted format checks.
- `lease` owns writer leases, fencing, and renewal.

It uses [Core](../core/README.md) for shared primitives,
[Storage](../storage/README.md) for object transport, and
[Table](../table/README.md) for SST access. The [engine](../midge/README.md)
coordinates these mechanisms to enforce transaction durability and publication
ordering. The default `cloud-all` feature enables cloud provider support
through Storage.

## Verify changes

Run from the repository root.

```sh
cargo test --locked -p cntryl-midge-durability
```

For changes to persisted bytes or replay, check [format compatibility](../../docs/development/format-compatibility.md)
and [recovery internals](../../docs/development/recovery-internals.md). Keep
fixture-based and crash/recovery tests with the behavior they verify.
