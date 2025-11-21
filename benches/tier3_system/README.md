# Tier 3 — System LSM Benchmarks

Goal: Measure end-to-end engine behavior including WAL, memtable, flush, reopen, and compaction.

Runtime: 3–15 seconds

What belongs here:
- Full engine operations (memtable → flush → read)
- L0→L1 compaction
- Reopen and manifest behavior

CI: Nightly
