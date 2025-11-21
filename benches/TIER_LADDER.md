# Midge Benchmark Tier Ladder

This document describes the official 6-tier LSM benchmark ladder used by the Midge project.

## Tier mapping

- tier1_hotpath: Hotpath microbenchmarks (encode/decode, iterator next, skiplist ops)
- tier2_subsystem: Subsystem bench (SST writer, WAL batching, memtable insert)
- tier3_system: Full LSM pipeline bench (memtable→flush→reopen→compaction)
- tier4_integration: YCSB-like integration workloads across threads (workload A/B/C/D/E/F)
- tier5_soak: Long-running soak/stress tests
- tier6_capacity: Capacity tests for multi-hour soak and durability

## Usage

- CI should run tiers 1–3 for each PR, tiers 4 nightly, tiers 5–6 weekly or pre-release.
- Bench files should follow the directory naming convention and contain a top-level comment describing the scope and runtime target.
