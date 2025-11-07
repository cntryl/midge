# PLAN — Manifesto, Performance Targets, and Requirements (Single Resource)

This document is the canonical, short-form plan for Midge. It unifies the manifesto principles, the performance targets we aim to meet, and a high-level requirements map. The detailed, testable requirements live in `wip/REQUIREMENTS.md`. The execution backlog lives in `wip/TODO.md`.

## Manifesto (evidence > assertion)

- Every claim must have a proof artifact (bench log, test, whitepaper, or reproducible script).
- Keep user-facing knobs simple (prefer ≤ 3 core knobs) and derive the rest transparently.
- Favor lock-free/sharded designs for scale, and explicit backpressure over hidden stalls.
- CI enforces test discipline (naming, AAA structure, single-behavior tests).

## Performance targets (targets we will measure)

- WAL throughput: 3–5 GB/s (batched, vectorized I/O); fsync p99 < 150 µs
- Memtable inserts: 5–10M ops/s; point read p99 < 10 µs
- SST flush throughput: ≥ 2 GB/s; compaction WA ≤ 5×
- Cached point reads: 8–10M QPS; p99 < 20 µs
- Range scans: ≥ 3 GB/s sequential
- Concurrency: near-linear scaling to 16+ threads on hot paths
- Startup/recovery: < 500 ms for a 100 GB DB (with checkpoints)

Acceptance for each target is a reproducible benchmark with raw logs, environment manifest, and summary statistics (p50, p95, p99), stored under `infra/proofs/`.

## Requirements overview (what must be true)

- WAL & Durability guarantees — ordered, crash-safe, group-commit, strict/balanced/weak profiles
- Memtable & indexing — lock-free inserts, sequence monotonicity, snapshot consistency
- SST & Compaction — deterministic merges, tombstone correctness, tiered/leveling strategy
- Read path & caching — checksum-verified reads, block cache policy, read-amplification bounds
- Concurrency & backpressure — safe flush/compaction overlap, write stall rules, fairness
- Error handling & recovery — torn-write detection, partial replay, corruption quarantine
- Cloud integration — idempotent uploads, reconciliation tools, consistent restore
- Multi-CF — isolation, independent compaction, shared budgets with guardrails
- Observability — actionable metrics, health surface, background error propagation
- Configuration invariants — compact, explainable, and linted

The authoritative, testable details are in `wip/REQUIREMENTS.md`.

## Execution

- Build to the requirements in `wip/REQUIREMENTS.md`.
- Track work and acceptance criteria in `wip/TODO.md`.
- Keep `wip/` minimal: this `PLAN.md`, plus `REQUIREMENTS.md` and `TODO.md`.
