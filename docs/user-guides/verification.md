# Storage verification

`midge verify --json <db-path>` performs a read-only verification pass over a
local-mode database directory. Its `verification_scope` is `local_path`: the
CLI has no provider configuration and cannot determine whether the supplied
path is a cloud cache. Use this command only with a local-mode database path.
It does not open an engine, acquire provider credentials, or inspect cloud
objects. Do not use it to establish the authority or completeness of
provider-backed cloud storage. For an open engine, use
`Engine::storage_verifier().verify_storage(timeout)`; a cloud-mode report sets
`authoritative` to `false` because the pass does not claim full
remote-authority coverage.

Verification does not repair storage. Preserve the database and its WAL if the
command reports corruption or an inaccessible path. See the
[operator runbook](../operations/operator-runbook.md) and
[migration guide](../operations/migration-guide.md) before using verification
as part of an upgrade.

## JSON schema version 1

Every JSON success and error object has an integer `schema_version` field.
Every object also has `verification_scope: "local_path"` to identify the
coverage of the path-only CLI.
Version 1 success objects keep report fields at the top level:

```json
{
  "schema_version": 1,
  "verification_scope": "local_path",
  "manifest_epoch": 1,
  "manifest_files_verified": 1,
  "sst_files_verified": 1,
  "bytes_verified": 437,
  "data_blocks_verified": 1,
  "wal_boundary": null,
  "wal_recovery_records_replayed": 0,
  "wal_recovery_bytes_replayed": 0,
  "intent_entries_loaded": 0,
  "authoritative": true,
  "health": "Healthy"
}
```

The report fields describe the manifest epoch and files checked, bytes and
blocks verified, WAL boundary and replay counts, loaded intent count, whether
the pass covered authoritative storage, and the resulting engine health.
`health` is one of `Healthy`, `Degraded`, `SalvageMode`, `WriteStalled`, or
`Corrupt`. The `authoritative` field describes the storage coverage of the
verification pass; it is not a general health indicator. The CLI assumes its
path is local-mode and does not infer the backend from the path. Cloud-backed
checks require the open-engine verifier and its storage configuration.

Errors use this version 1 shape:

```json
{
  "schema_version": 1,
  "verification_scope": "local_path",
  "status": "error",
  "error_kind": "corruption",
  "message": "Compatibility error: unsupported on-disk format version"
}
```

`error_kind` is one of `usage`, `storage`, `corruption`, or `internal`.
Messages are diagnostic text; automation should branch on `schema_version`,
`status`, `error_kind`, and the process exit code rather than matching message
text. With `--json`, both reports and errors are written to standard output and
standard error is empty.

The CLI JSON contract is separate from the Rust `StorageVerificationReport`
type. The Rust type's `Serialize` implementation is not a versioned wire
format. In schema version 1, adding fields is additive and consumers should
ignore unknown fields. Renaming or removing fields, changing field types or
meanings, or changing the error object requires a new schema version. The
process exit-code mapping is a separate stable contract:

| Exit code | Meaning |
|---:|---|
| 0 | The verification report is healthy. |
| 1 | The report is degraded, in salvage mode, or write-stalled; or verification stopped on a retryable/backpressure condition. |
| 2 | The command or arguments are invalid. |
| 3 | The path or required storage data is inaccessible, missing, transiently unavailable, or fenced. |
| 4 | The report health is `Corrupt`, or storage is corrupt or uses an incompatible persisted format. |
| 5 | An unexpected internal defect prevented verification. |

When verification completes with a health report, JSON contains the report,
including when its `health` is `Corrupt` and the exit code is 4. When parsing
arguments or verifying storage fails, JSON contains the error object.
