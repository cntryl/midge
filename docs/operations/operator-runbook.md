# Midge operator runbook

Midge 0.3.3 is an embedded, single-process Rust LSM engine. This is a local-first
operator checklist for evaluation and controlled deployments; it is not a
production endorsement.

## Before opening a database

- Use a dedicated database path with stable ownership and enough free space.
- Choose `OpenOptions::local(path)` for local storage, or
  `OpenOptions::cloud_simulated(cache, bucket, prefix)` for filesystem-backed
  cloud behavior. Provider-backed cloud storage is supported pre-1.0,
  continuously qualified through Sqrzl, and requires the matching feature and
  deployment-specific provider configuration.
- Choose a `WriteOptions` policy for every transaction commit. Use `sync()` when
  the commit must survive a local process or machine crash after acknowledgement.
- Keep the database path private to one active engine writer. Midge is not a
  multi-process lock coordinator.

## Normal operation

Keep the returned runtime, recovery, and storage-layout snapshots with the
operator logs when investigating an incident. Use `engine.flush_cf(...)` only
when an explicit flush is needed, and call `engine.shutdown(timeout)` during
orderly termination. A timeout or error is an operational event to investigate;
do not treat dropping the engine as an acknowledgement of shutdown.

## Recovery

On open, Midge validates metadata and replays recoverable WAL state according to
the configured `RecoveryPolicy`. Preserve the complete database directory and
its WAL when recovery reports an error. Copy it for diagnosis, record the exact
error, and use `StorageVerifier::verify_path` on a copy when verification is appropriate.
Do not delete a database directory, WAL, manifest, or lock as a generic recovery
step.

If a database is intentionally disposable, stop all users, preserve a copy if
needed, and recreate it from the application's source of truth. That is a data
replacement procedure, not recovery.

## Storage verification

Use `midge verify --json <db-path>` for a read-only check of a local-mode
database directory. The command cannot access provider credentials or verify
remote cloud objects. For provider-backed storage, run the open-engine
`StorageVerifier::verify_storage(timeout)` with the configured engine and
interpret `authoritative: false` as a limit on the coverage of that pass. The
versioned report and exit-code meanings are documented in the
[storage verification guide](../user-guides/verification.md).

Verification does not repair a database. Preserve the full directory and WAL
before investigating an error, and run pre-upgrade and post-upgrade checks on a
separate copy before changing production traffic.

## Lease loss and fencing

Midge is fail-closed on lease loss. Cloud transactions are invalidated:
new transactions, point reads, scan creation and existing lazy iterator advances
return `MidgeError::Fenced`; writes remain fenced as well. Readers check monotonic
lease expiry even before notification and recheck after blocking I/O. Remote SST
retention is not shared with the successor, which may reclaim old files.
Diagnostics and orderly shutdown remain available. Local and in-memory snapshot
reads retain their existing behavior. Configure `OpenOptionsBuilder::on_lease_loss` for an
exact-once process-local notification and begin orderly shutdown from the
application; the callback must not block the lease thread. Polling
`Engine::is_primary_lease_healthy` remains useful for health reporting.

The default wall-clock skew allowance is 15 seconds (half the 30-second lease
TTL). Increasing it delays legitimate takeover by the same amount; values over
one TTL are rejected. Filesystem mutation locks are intentionally never broken
automatically because an NFS/SMB rename already in flight cannot be cancelled.
If a process crashes while holding `.midge_leader.lock`, fence and stop every
possible writer, preserve the database, and remove the lock only as an explicit
operator recovery action. Never remove it merely because its timestamp is old.

## Cloud-specific notes

`cloud_async()` acknowledges local visibility and lets cloud upload proceed in
the background. On a same-cache restart, Midge merges and replays intact local
WAL with remote WAL before opening. This does not make `cloud_async()` durable
against local cache loss: `cloud_strict()` waits for the required cloud upload,
while an asynchronous write whose local bytes and unacknowledged upload are both
lost cannot be recovered. Test cache-loss and restart behavior with the exact
provider and feature set before relying on it.

See [cloud setup](cloud-setup.md), the [durability contract](../user-guides/transaction-durability-contract.md),
and [troubleshooting](../user-guides/troubleshooting.md) for bounded procedures.
