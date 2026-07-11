# Durability Guide

Midge's canonical transaction and write acknowledgment contract lives in
[transaction-durability-contract.md](transaction-durability-contract.md).

Read that page for:

- what each `WriteOptions` mode waits for before `commit()` returns
- which modes are local-only and which modes are cloud-only
- which writes survive restart after local crashes or cloud cache loss
- how visibility, local durability, cloud durability, and SST publication differ
- how `RecoveryPolicy::Strict` and `RecoveryPolicy::Salvage` should be interpreted

In particular:

- `WriteOptions::sync()` and `WriteOptions::buffered()` are local-only; cloud-backed storage rejects them.
- `WriteOptions::cloud_async()` and `WriteOptions::cloud_strict()` are cloud-only. Non-cloud storage rejects them.
- `WriteOptions::cloud_strict()` is not a stronger local mode. In cloud-backed mode it waits for the runtime to `seal`, rotate, `upload`, and acknowledge the WAL segment covering the committed sequence.
- Empty cloud-backed `cloud_strict()` transactions are allowed without inventing a WAL record.

## Manifest Journal Sync Boundary

Manifest journal edits and their durability markers are written before one
required filesystem sync. Midge does not provide a configuration or benchmark
escape hatch for this durability boundary.

## Verification Reading Order

Before evaluating durability behavior, read:

1. [transaction-durability-contract.md](transaction-durability-contract.md)
2. [../development/storage-invariants.md](../development/storage-invariants.md)
3. [../development/architecture.md](../development/architecture.md)
4. [../development/recovery-internals.md](../development/recovery-internals.md)
5. [../development/testing.md](../development/testing.md)
