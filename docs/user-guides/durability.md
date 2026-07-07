# Durability Guide

Midge's canonical transaction and write acknowledgment contract lives in
[transaction-durability-contract.md](transaction-durability-contract.md).

Read that page for:

- what each `WriteOptions` mode waits for before `commit()` returns
- which writes survive restart after local crashes or cloud cache loss
- how visibility, local durability, cloud durability, and SST publication differ
- how `RecoveryPolicy::Strict` and `RecoveryPolicy::Salvage` should be interpreted

In particular, `WriteOptions::cloud_strict()` is a cloud-backed durability mode,
not a stronger local mode. Non-cloud storage rejects it. In cloud-backed mode it
waits for the runtime to seal, rotate, upload, and acknowledge the WAL segment
covering the committed sequence. Empty cloud-backed transactions are allowed
without inventing a WAL record.

## Benchmark-Only Fsync Skips

`MIDGE_SKIP_MANIFEST_FSYNC=1` is a benchmark-only escape hatch for manifest
journal writes and fsync markers. It is honored only when
`MIDGE_ALLOW_MANIFEST_SKIP_FSYNC=1` is also set.

Treat these as a double opt-in for controlled benchmark runs only. They weaken
manifest durability and must not be used to evaluate crash safety, recovery
behavior, or production-like durability.

## Verification Reading Order

Before evaluating durability behavior, read:

1. [transaction-durability-contract.md](transaction-durability-contract.md)
2. [../development/storage-invariants.md](../development/storage-invariants.md)
3. [../development/architecture.md](../development/architecture.md)
4. [../development/recovery-internals.md](../development/recovery-internals.md)
5. [../development/testing.md](../development/testing.md)
