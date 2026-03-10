# Storage Invariants

These are the storage invariants Midge must preserve to remain safe enough for external evaluation. Each item is intentionally short: if one of these stops being true, the corresponding tests should fail and the crate should not be presented as trustworthy.

## 1. SST files are immutable after publish

Rationale:
Once a file is manifest-visible, readers and recovery treat its contents as stable durable state.

Owned by:
`src/runtime/actors/flush.rs`, `src/runtime/actors/compaction.rs`, `src/metadata/manifest.rs`

Validated by:
`tests/engine_compaction.rs`, `tests/failure_injection.rs`

## 2. WAL records replay in sequence order

Rationale:
Newer updates, tombstones, and transactional visibility depend on deterministic replay order.

Owned by:
`src/wal/recovery.rs`

Validated by:
`tests/durability_recovery.rs`, `src/wal/recovery.rs`

## 3. Partial WAL records are never applied

Rationale:
Crash recovery may salvage a valid prefix, but it must never materialize a torn write.

Owned by:
`src/wal/recovery.rs`

Validated by:
`src/wal/recovery.rs`, `tests/durability_wal.rs`

## 4. Tombstones and range tombstones override older values

Rationale:
Deletes are part of the durable state model, not best-effort metadata.

Owned by:
`src/wal/recovery.rs`, `src/engine/api/iterator.rs`, `src/runtime/event_loop/read_path.rs`

Validated by:
`tests/engine_iterators.rs`, `tests/engine_compaction.rs`

## 5. Flush publishes new SST state atomically or not at all

Rationale:
An SST file existing on disk is not enough; manifest publication defines authority.

Owned by:
`src/runtime/actors/flush.rs`, `src/runtime/event_loop/mod.rs`, `src/runtime/intent_persistence.rs`

Validated by:
`tests/failure_injection.rs`

## 6. Compaction does not delete input SSTs before replacement state is durable

Rationale:
Compaction must reduce files without ever creating a window where neither the old nor new file set is authoritative.

Owned by:
`src/runtime/actors/compaction.rs`, `src/runtime/event_loop/mod.rs`, `src/metadata/manifest.rs`

Validated by:
`tests/chaos_compaction.rs`, `tests/failure_injection.rs`

## 7. Manifest-visible state is authoritative over raw file presence

Rationale:
Recovery must be able to distinguish “durable output exists” from “published state changed.”

Owned by:
`src/metadata/manifest.rs`, `src/runtime/intent_persistence.rs`

Validated by:
`tests/failure_injection.rs`

## 8. Recovery either restores a trustworthy state or reports degraded health explicitly

Rationale:
Silent ambiguous recovery is worse than an explicit recovery failure or salvage-mode open.

Owned by:
`src/wal/recovery.rs`, `src/engine/mod.rs`, `src/runtime/state.rs`

Validated by:
`tests/failure_injection.rs`
