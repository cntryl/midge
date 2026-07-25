# Midge operator runbook

Midge 0.1.0 is an embedded, single-process Rust LSM engine. This is a local-first
operator checklist for evaluation and controlled deployments; it is not a
production endorsement.

## Before opening a database

- Use a dedicated database path with stable ownership and enough free space.
- Choose `OpenOptions::local(path)` for local storage, or
  `OpenOptions::cloud_simulated(cache, bucket, prefix)` for filesystem-backed
  cloud behavior. Real cloud providers remain pre-1.0 and require the matching
  feature and provider configuration.
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
error, and use `Engine::verify_path` on a copy when verification is appropriate.
Do not delete a database directory, WAL, manifest, or lock as a generic recovery
step.

If a database is intentionally disposable, stop all users, preserve a copy if
needed, and recreate it from the application's source of truth. That is a data
replacement procedure, not recovery.

## Cloud-specific notes

`cloud_async()` acknowledges local visibility and lets cloud upload proceed in
the background. `cloud_strict()` waits for the required cloud upload. A lost
local cache is therefore recoverable only when the remote state and provider
qualification support it. Test cache-loss and restart behavior with the exact
provider and feature set before relying on it.

See [cloud setup](cloud-setup.md), the [durability contract](../user-guides/transaction-durability-contract.md),
and [troubleshooting](../user-guides/troubleshooting.md) for bounded procedures.
