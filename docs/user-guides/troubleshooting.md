# Troubleshooting

Start by recording the Midge version, Rust toolchain, storage mode, feature
flags, options, and the complete error. Preserve the database path and WAL
before attempting recovery.

## Open or recovery failure

Stop competing processes and verify ownership, permissions, free space, and the
configured path. Copy the database for diagnosis. Use `Engine::verify_path` on
the copy when appropriate and inspect recovery metrics after a successful open.
Do not delete the directory, WAL, manifest, or lock as a generic recovery step.

`MidgeError::Io` represents an underlying I/O error. Other relevant variants
include `Corruption`, `RecoveryFailed`, `CompatibilityError`, `NoSpace`, and
`Fenced`. The variant and message determine the next investigation.

## Commit errors or stalls

Check whether the transaction is read-only, whether keys/ranges are valid, and
whether the conflict policy rejected a concurrent write. A `WriteStall` or
`ResourceLimit` means the caller must apply backpressure and inspect memory,
flush, and compaction progress. A timeout is not proof that the operation had
no effect; use the relevant recovery and runtime snapshots.

## Cloud problems

Confirm the provider feature, endpoint, credential source, prefix, local cache,
and network access. Reproduce with `CloudSimulated` first, then run the Sqrzl or
provider qualification suite. `cloud_async` and `cloud_strict` have different
acknowledgement boundaries; consult the [durability contract](transaction-durability-contract.md).

## Logging and evidence

Midge uses the `tracing` ecosystem. Configure the application's subscriber and
retain logs with runtime/recovery/storage-layout snapshots. The crate does not
require a particular global logging initializer.
