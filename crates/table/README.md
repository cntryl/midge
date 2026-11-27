# cntryl-midge-table

Table owns the in-memory and on-disk sorted data structures used by Midge.
`SkipListMemtable` holds recent records in memory. The `sst` module owns the
sorted string table format, readers and writers, Bloom filters, trie indexes,
and block caches. Its filesystem adapters use the I/O and codecs from
[Core](../core/README.md).

Table has no default features. The [engine](../midge/README.md) uses these
primitives to compose read snapshots and decide when to flush or compact.

## Verify changes

Run from the repository root.

```sh
cargo test --locked -p cntryl-midge-table
cargo test --locked -p cntryl-midge-table --test block_cache_lifecycle
```

For SST changes, consult the [format compatibility guide](../../docs/development/format-compatibility.md).
For the lock-free skiplist, the [testing guide](../../docs/development/testing.md)
lists the Loom and Miri checks.
