# WAL and ledger sync observations

The #904 preparation keeps every existing exact SyncOpen floor. It adds
observations at the ledger's real filesystem sync boundaries; no cadence,
recovery decision or persisted encoding changes.

| Runtime metric | Scope |
| --- | --- |
| `wal_fsync_count` | Existing successful writer-runner barrier syncs |
| `wal_ledger_file_sync_attempts` / `wal_ledger_file_syncs` | Ledger append and checkpoint file calls attempted / returned success |
| `wal_ledger_directory_sync_attempts` / `wal_ledger_directory_syncs` | Ledger checkpoint directory calls attempted / returned success |
| `wal_accounted_syncs` | Successful writer barrier plus ledger file and directory calls |

The accounted sum excludes segment initialization/repair and other filesystem
syncs, including manifest, SST and lease work. It is not a complete device or
process-wide fsync count. These are issued `Fs` durability calls, not measured
device flushes; simulated filesystems exercise the same call boundaries.
An after-effect failure increments attempts but cannot claim a successful
receipt. A memory-only journal reports zero filesystem calls.

Existing 1/16/64-writer and 4 KiB rotation benchmarks report ledger attempts,
successful file/directory calls and the accounted sum alongside the existing
writer metric. Delta windows exclude startup snapshots. Unsuccessful ledger
sync receipts invalidate a cost measurement. The old `physical_fsyncs` and
`commits_per_fsync` observations retain their writer-only scope, declared in
row parameters. No measurement or saving has been recorded by adding counters.

Active regressions exercise actual ledger publication, append, compaction,
file/directory failure boundaries and a public Strict commit/reopen path.
Behavioral execution remains pending in the implementation-first batch.

Amortized ledger floors remain unapproved. Before considering that proposal,
complete any broader physical-call accounting needed by its claimed scope,
run the named A/B and recovery campaigns, and obtain explicit approval of the
lost-write detection tradeoff. These observations do not grant that approval.
