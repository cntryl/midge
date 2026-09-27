# Architecture drift findings (2026-09-27)

These findings were checked against the current production source before issue
filing. They describe behavior and its failure mode rather than relying on
earlier issue or commit history.

1. **Underfull L1+ overlap has no maintenance trigger.**
   `SstReadView::build_level` quarantines a complete-bound level when key
   ranges strictly overlap or three files meet at one boundary. The compaction
   picker only selects L1+ work when its byte target is exceeded. A small
   recovered or published overlap therefore stays on conservative linear
   reads indefinitely. A regression should create an underfull overlapping
   component, run maintenance, and prove that a replacement restores indexed
   reads without dropping a version or tombstone.
2. **Manifest metadata edits rebuild the read view.**
   `ManifestRuntimeState::DerefMut` invalidates the cached SST view before any
   mutable access, including sequence reservations and journal horizons that
   leave the file set unchanged. A regression should show pointer reuse for a
   metadata-only edit and a fresh view after a file-set edit; governance should
   reject direct mutable manifest access.
3. **Current architecture and test guides contain stale claims.**
   The storage and testing guides cite integration test files that do not
   exist. The architecture diagrams describe a production snapshot path and
   cloud acknowledgment steps that no longer match their implementations.
   A regression should reject nonexistent referenced test paths in the two
   current guides; source-backed text review should cover the diagrams and
   related ownership claims.

The first two findings are independent root causes but belong to the same
runtime and compaction maintenance change. The third is evidence repair and
does not imply a persisted-format or public API change.

Triage found no open duplicate issues before filing. These confirmed findings
are tracked as #632 (overlap repair), #633 (manifest read-view ownership), and
#634 (architecture and evidence guides).
