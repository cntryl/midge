# Storage Invariants

These are the storage invariants Midge must preserve to remain safe enough for external evaluation. Each item is intentionally short: if one of these stops being true, the corresponding tests should fail and the crate should not be presented as trustworthy.

## 1. SST files are immutable after publish

Rationale:
Once a file is manifest-visible, readers and recovery treat its contents as stable durable state.

Owned by:
`src/runtime/actors/flush.rs`, `src/runtime/actors/compaction.rs`, `src/metadata/manifest.rs`

Validated by:
`tests/storage_invariants.rs` (`should_preserve_published_sst_bytes_given_later_flush_then_restart`)
and `tests/engine_api.rs` (`should_publish_compacted_ssts_in_manifest_when_compaction_completes`).

## 2. WAL records replay in sequence order

Rationale:
Newer updates, tombstones, and transactional visibility depend on deterministic replay order.

Owned by:
`src/wal/recovery.rs`

Validated by:
`tests/durability.rs` (`should_replay_all_records_given_multiple_wal_segments_when_recovering`)
and `src/wal/recovery.rs`.

## 3. Partial WAL records are never applied

Rationale:
Crash recovery may salvage a valid prefix, but it must never materialize a torn write.

Owned by:
`src/wal/recovery.rs`

Validated by:
`tests/durability.rs` (`should_keep_valid_prefix_given_truncated_wal_tail_when_reopening_in_strict_mode`
and `should_drop_partial_wal_entry_given_manual_tail_append_when_reopening_in_salvage_mode`).

## 4. Tombstones and range tombstones override older values

Rationale:
Deletes are part of the durable state model, not best-effort metadata.

Owned by:
`src/wal/recovery.rs`, `src/engine/api/iterator.rs`, `src/runtime/event_loop/read_path.rs`

Validated by:
`tests/engine_api.rs` (`should_skip_deleted_keys_given_tombstones_when_scanning`
and `should_respect_range_tombstones_given_delete_range_when_scanning`).

## 5. Flush publishes new SST state atomically or not at all

Rationale:
An SST file existing on disk is not enough; manifest publication defines authority.

Owned by:
`src/runtime/actors/flush.rs`, `src/runtime/event_loop/mod.rs`, `src/runtime/intent_persistence.rs`

Validated by:
`tests/fault_injection.rs` (`should_ignore_orphan_sst_when_flush_intent_log_save_hits_no_space`
and `should_retry_flush_given_transient_publish_failure_when_reopening`).

## 6. Compaction does not delete input SSTs before replacement state is durable

Rationale:
Compaction must reduce files without ever creating a window where neither the old nor new file set is authoritative.

Owned by:
`src/runtime/actors/compaction.rs`, `src/runtime/event_loop/mod.rs`, `src/metadata/manifest.rs`

Validated by:
`tests/fault_injection.rs` (`should_retain_input_ssts_given_compaction_failure_before_manifest_publish`
and `should_not_delete_input_ssts_given_compaction_gc_failure_after_manifest_publish`).

## 7. Manifest-visible state is authoritative over raw file presence

Rationale:
Recovery must be able to distinguish “durable output exists” from “published state changed.”

Owned by:
`src/metadata/manifest.rs`, `src/runtime/intent_persistence.rs`

Validated by:
`tests/durability.rs` (`should_not_expose_sst_without_manifest_entry_given_orphan_file_when_recovering`).

## 8. Recovery either restores a trustworthy state or reports degraded health explicitly

Rationale:
Silent ambiguous recovery is worse than an explicit recovery failure or salvage-mode open.

Owned by:
`src/wal/recovery.rs`, `src/engine/mod.rs`, `src/runtime/state.rs`

Validated by:
`tests/durability.rs` (`should_fail_strict_open_when_wal_is_corrupt` and
`should_open_in_salvage_mode_when_wal_is_corrupt`).

## 9. Database root and storage directory entries are durable before writes

Rationale:
A synced WAL or SST file is unreachable if a power loss drops the directory
entry for the database root or one of its storage directories. Startup must
persist every newly created directory entry before admitting writes.

Owned by:
`src/io/durable_dir.rs`, `src/io/real.rs`, `src/engine/startup/storage.rs`,
`src/runtime/state.rs`

Validated by:
`src/engine/startup/tests.rs`
(`should_make_new_database_root_chain_durable_before_startup_writes`) and
`src/io/durable_dir.rs`
(`should_resync_parent_when_durable_directory_is_removed_and_recreated`).
