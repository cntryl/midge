# Tier 1 — Hotpath (microbench)

Goal: Measure CPU-bound inner loops quickly.

Runtime: milliseconds

What belongs here:
- encode/decode
- iterator `.next()` microbench
- skiplist memtable ops
- bloom filter probe
- key hashing / lexkey ops

CI: Run on every PR
