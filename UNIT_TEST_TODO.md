# UNIT TEST TODO

Purpose: Create a prioritized list of unit test targets (invariants, edge cases, negative cases) across the `src/` modules. This file is intended to guide targeted unit test coverage improvements and to document gaps in the test surface.

---

How to use:
- Follow naming convention: `should_{action}_when_{context}`
- Use AAA structure in tests (Arrange / Act / Assert).
- Use existing test modules (or create relevant `#[cfg(test)] mod tests { ... }`) and place tests close to the code they validate when possible.
- Run: `cargo test` and `cargo test --package <lib>` for finer scope.

---

Priority Legend:
- P0 — Critical invariants that can cause crashes or data corruption
- P1 — Important invariants that cause incorrect behavior, but less likely to corrupt data
- P2 — Nice-to-have coverage: negative tests and additional edge cases

---

Summary of focused modules & high-level invariants

| Module | File(s) | Invariants & Missing Tests | Suggested Test Names | Priority | Status |
|---|---|---|---|---|---|
| Core engine & writes | `src/core/engine/operations/writes.rs` | Sequence numbers are allocated uniquely & monotonically. Concurrent writers do not produce duplicate sequences or races. | `should_allocate_unique_sequences_for_concurrent_writes` | P0 | ✅ Done |
| Memtable / skiplist | `src/core/memtable/core.rs`, `src/core/data_structures/skiplist.rs` | Drain returns all versions in newest-first order; drain_with_meta preserves internal key encoding; insertion ordering and rotate behavior work under heavy concurrent writes. | `should_return_versions_in_sequence_descending_order_within_key`, `should_encode_internal_keys_correctly_in_drain` | P0 | ✅ Done |
| Merge resolution / merge operators | `src/core/engine/operations/maintenance.rs` | Merge resolution returns a single resolved entry per user key; merge resolution respects MC/seq ordering and returns entries in internal-key order for downstream; negative tests: if merge operator panics or returns invalid data, behavior must be safe. | `should_preserve_all_versions_when_no_merge_operands`, `should_flush_entries_in_internal_key_order` | P0 | ✅ Done |
| WAL & recovery | `src/wal/` | WAL write ordering and durability; writes use `fetch_add()` for seq assignment; WAL replay produces same sequence ordering and recovered memtable state; WAL segment roll-over and header correctness. | `should_preserve_sequence_numbers_exactly_after_replay`, `should_recover_records_from_rotated_wal_segment` | P0 | ✅ Done |
| Flush pipeline & persistence | `src/core/persistence/flush.rs` | `flush_memtable_to_sst` preserves internal-key semantics; bloom/filter metadata saved; range tombstones applied; sst_seq allocation; concurrency with background flusher; test for immediate flush failing or partial writes. | `should_flush_1000_rapid_overwrites_to_same_key_without_ordering_violation` | P0 | ✅ Done |
| SST Format (DataBlockBuilder) | `src/sst/format.rs` | `DataBlockBuilder::add()` requires strictly increasing keys and strict internal-key ordering when enabled; `add_unchecked()` bypasses checks (verify intended usage & negative test). | `should_reject_duplicate_keys_when_adding`, `should_allow_add_unchecked_to_bypass_ordering_validation` | P0 | ✅ Done |
| Index / Sparse Index | `src/sst/sparse_index.rs`, `src/sst/format.rs::IndexBlockBuilder` | Sparse index must store the last key of blocks and use the same internal-key semantics as data blocks. FS writer and index builder must not duplicate keys or violate ordering. Tests should cover internal-key on-disk mode too. | `should_use_internal_key_comparator_when_internal_keys_enabled`, `should_store_block_handles_in_index` | P0 | ✅ Done |
| TLV encode/decode | `src/sst/encoding.rs` | TLV encode/decode roundtrip; linear and zig-zag encodings; decode must be robust to truncated or malformed data (no panic except MidgeError). Add negative tests for truncated sections and oversized varints. | `should_roundtrip_encode_decode_for_all_entry_types`, `should_return_error_when_truncated_before_key_delta` | P0 | ✅ Done |
| Block handles & readers | `src/sst/reader_common.rs`, `src/sst/fs/reader.rs`, `src/sst/mem/reader.rs` | Parsed footers, index parsing, meta index, bloom & tombstone logic; ensure `SstMetadata::from_bytes` handles boundary cases (empty files, missing metadata). | `should_return_error_when_bytes_empty`, `should_decode_block_when_paranoid_true_and_valid_checksum` | P1 | ✅ Done |
| FS / Dyn writer | `src/sst/fs/writer.rs`, `src/sst/writer_common.rs` | Validate `last_key_in_block` semantics (internal-key or raw), ensure `finish_bytes` reusable; verify index generation uses internal keys when `use_internal_keys=true`. Ensure writing with tiny blocks doesn't break TLV offsets & index. | `should_store_internal_index_keys_when_use_internal_keys_true` | P1 | ✅ Done |
| Bloom & filter | `src/sst/bloom.rs` | Bloom builder/lookup correctness; misbehavior when false-positive rates are high but still correct acceptance/rejection semantics; Check that bloom filter encodes/decodes properly. | `should_bloom_filter_false_positive_rate_with_bounds`, `should_encode_decode_bloom_filter_block` | P2 | Pending |
| Range tombstones | `src/sst/range_tombstone.rs` | Range tombstones encode/decode; cover range tombstone interactions with scan/get/scan_range; ensure covered entries are filtered out at read-time. | `should_roundtrip_preserve_all_fields`, `should_return_true_when_key_in_range_and_seq_valid` | P1 | ✅ Done (30+ existing tests) |
| Cloud-backed sst | `src/sst/cloud/` | Cloud-backed writer/reader invariants for uploads, multi-part upload completeness, partial upload recovery. | `should_upload_and_read_back_cloud_sst`, `should_recover_from_partial_cloud_upload` | P1 | Pending |
| Manifest & Archive | `src/sst/manifest_cache.rs`, `src/sst/cloud` | Invariants around manifest entries, consistency, serialization roundtrip; cloud lifecycle states when compaction/archival occurs. | `should_manifest_roundtrip_and_validate_archival_state` | P2 | Pending |

---

Detailed test descriptions & rationale (expanded)

1) TLV & Data block encoding/decoding
- Reason: Many issues originate from block-encoding mismatches (observed TLV decode overflow in tests).
- Tests:
  - `should_encode_and_decode_roundtrip` — Build a `DataBlockBuilder` with a set of entries, finish, parse with `TlvBlockIterator::new()`, and assert parsed entries equal the inputs.
  - `should_return_error_on_truncated_tlv_block` — Truncate data bytes in multiple places (beginning, middle, end) and ensure `decode` returns error rather than panic.
  - `should_handle_large_key_delta_and_restarts` — Use large keys and restarts (shared prefix) to verify restart logic correctness.

2) Index / Sparse Index behavior
- Reason: Key ordering & internal key format mismatches can corrupt the index, causing block lookup to fail or return wrong blocks.
- Tests:
  - `should_store_internal_index_keys_for_internal_format` — Use `FsDynWriter::new` with `use_internal=true`, write many entries and verify `IndexBlockBuilder` index keys are internal via decode, and that `SparseIndex` find_block returns correct block for internal-key lookups.
  - `should_not_duplicate_index_keys_for_rapid_overwrites` — Reproduce hot-partition scenario; write many overwrites to the same user key, ensure sparse index keys are monotonic, and fail only if duplicates or ordering isn't preserved.

3) DataBlockBuilder ordering checks
- Reason: `add()` performs ordering checks — ensure they exist, and `add_unchecked()` bypass works only if intended.
- Tests:
  - `should_reject_out_of_order_keys` — Create a builder and call `add()` with decreasing keys and verify error condition or panic is triggered in controlled way.
  - `should_allow_add_unchecked_when_caller_validates` — Use `add_unchecked` with keys that would break ordering but are valid in index-builder path and verify no runtime checks fail.

4) Merge operator & maintenance
- Reason: Merging must produce a single stable entry per user key and must preserve ordering across versions.
- Tests:
  - `should_resolve_merge_operands_correctly_when_many_versions` — Create entries with same user key and merge operands and verify the merged result matches expected merge operator behavior.
  - `should_not_produce_duplicate_internal_keys_after_merge_resolution` — Run merge resolution and assert there are no duplicate keys produced after the merge (internal ordering preserved).

5) WAL & sequence allocation
- Reason: Unique sequent numbers are fundamental. Confirm `fetch_add()` use ensures uniqueness across concurrent writes.
- Tests:
  - `should_allocate_unique_seq_when_concurrent_writes` — Start multiple threads that write concurrently and check allocated sequences are contiguous and unique.

6) Memtable behavior
- Reason: Concurrent insert and drain ordering; memtable rotate and flush boundaries.
- Tests:
  - `should_drain_versions_newest_first` — Insert multiple versions of the same user key and verify `drain_with_meta_internal` yields entries with sequence order descending.
  - `should_preserve_bounds_on_rotate` — Fill a memtable, rotate it, then flush and verify SSTs have contiguous key bounds.

7) Range tombstones, bloom & filters
- Tests:
  - `should_filter_keys_covered_by_range_tombstone_in_scans` — Insert keys, add tombstone range and verify get/scan excludes them.
  - `should_encode_decode_bloom_filter_block` — Finish SST with bloom, parse metadata, and assert the bloom is present and works as expected.

8) FS writer, finish_bytes, and index generation
- Reason: `finish_bytes` writing must produce well-formed SST with matching metadata; tests for `finish_bytes()` are essential (not just write-to-path).
- Tests:
  - `should_write_index_keys_as_internal_when_internal_enabled` — Use `finish_bytes` for internal format and inspect metadata for internal-key flag and index entry decode.
  - `should_return_valid_sst_bytes_after_finish_bytes` — Validate bytes produced are parseable by `SstMetadata::from_bytes` and `SstMemReader::from_bytes`.

9) Cloud-backed path & manifest
- Tests:
  - `should_upload_and_read_back_cloud_sst` — Use cloud mock, write to cloud, and verify reader gets the same entries.
  - `should_manifest_roundtrip_and_validate_archival_state` — Build manifest entries and decode them to validate archival state.

---

Negative tests / malformed inputs
- TLV decode with malformed entries (abrupt truncation and invalid varints).
- Index block with missing value (should error decode).
- Footer too small (SST file truncated) should return invalid data errors.
- Block type mismatches (reads where block type header differs from expected) should be rejected.

---

Suggestions for test implementation order / approach
1. Fix any recurring panics or test issues (e.g., TLV overflow) before adding more tests.
2. Add property tests if possible (quickcheck-style) for `DataBlockBuilder` (encode/decode roundtrip over many random inputs) to catch boundary conditions.
3. Start with P0 tests on `src/sst/format.rs` (`DataBlockBuilder`, `IndexBlockBuilder`), `src/core/persistence/flush.rs`, `src/core/engine/operations/maintenance.rs`, and `src/wal`.
4. Add concurrency tests related to sequence assignment and WAL allocation using `std::thread`/`crossbeam` with deterministic seeds.
5. Ensure `validate_tests.rs` passes for new tests and that they follow the repo's naming/AAA conventions.

---

Maintenance & process
- Each new test must include inline clarifying comments (Arrange / Act / Assert).
- When adding tests that reproduce previous bugs, tag them with `#[should_panic]` only if panic is intended; otherwise assert expected error type.
- Keep tests deterministic: e.g., when using randomness for edge-case property tests, use deterministic seed.

---

Next steps (developer checklist):
- [x] Prioritize P0 tests and allocate to engineers (owner, estimated time).
- [x] TLV encode/decode tests — `src/sst/encoding.rs` — ✅ Done
- [x] DataBlockBuilder ordering tests — `src/sst/format.rs` — ✅ Done  
- [x] IndexBlockBuilder internal-key tests — `src/sst/format.rs` — ✅ Done
- [x] Merge resolution tests — `src/core/engine/operations/maintenance.rs` — ✅ Done
- [x] Memtable drain ordering tests — `src/core/memtable/core.rs` — ✅ Done
- [x] Sequence allocation tests — `src/core/engine/operations/writes.rs` — ✅ Done
- [x] FS writer internal-key index test — `src/sst/fs/writer.rs` — ✅ Done
- [ ] WAL recovery tests — `src/wal/` — Pending
- [ ] Block handles & readers tests — `src/sst/reader_common.rs` — Pending (P1)
- [ ] Range tombstone tests — `src/sst/range_tombstone.rs` — Pending (P1)
- [ ] Bloom filter tests — `src/sst/bloom.rs` — Pending (P2)
- [ ] Cloud-backed SST tests — `src/sst/cloud/` — Pending (P1)
- [ ] Manifest tests — Pending (P2)

---

Appendix — Quick command snippets

Run all tests:

```powershell
cargo test --workspace
```

Run single file tests (example):

```powershell
cargo test --test <test_name>  # or use package/layer filtering
```

Run a specific test in a module (e.g. fs writer test):

```powershell
cargo test --package cntryl-midge fs::writer::should_store_internal_index_keys_when_use_internal_keys_true
```

---

If you'd like, I can now iterate and add specific test stubs for the highest-priority items (P0). Tell me which module/test you'd like to start with.  

