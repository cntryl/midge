# Tier 2 — Subsystem Benchmarks

Goal: Measure isolated components in short duration.

Runtime: < 1-3 seconds

What belongs here:
- SST writer/reader
- WAL write batches
- Block iteration
- Memtable insert full flow
- Bloom filter build

CI: Run on PRs and nightly jobs.
