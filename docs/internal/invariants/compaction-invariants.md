# Compaction Invariants (Draft)

- Output SSTs are strictly sorted and non-overlapping within a level (L1+).
- Tombstones are preserved until their key range is fully shadowed in lower levels.
- Snapshot visibility is maintained: no key/version needed by any snapshot is dropped.
- Level 0 may overlap; compaction output must remove intra-L0 overlap.
- Sequence number ordering is preserved; newer versions are never placed below older ones of the same key.
