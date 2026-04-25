<!--
    Concise Copilot instructions for the Midge repo. Keep this file short
    and focused: architecture pointers, developer workflows, project
    conventions, and where to look for authoritative examples.
-->

# GitHub Copilot Instructions — Midge (concise)

Purpose: help AI coding agents be productive quickly by describing
the project's architecture, conventions, and developer workflows.

STOP!

- did you validate tests? `cntryl-tools validate-tests`
- did you fix all clippy warnings? `cargo clippy --all-targets`

- **Big picture**: Midge is an embedded LSM-tree engine in Rust. Key
  runtime pieces live under `src/`: `engine/` (main API), `storage/`
  (cloud/local backends), `wal/`, `sst/`, `compaction/`, `metadata/`
  (manifest), `iterators/`, `runtime/` (background actors), and
  `common/` (foundation types). See `docs/development/the-big-idea.md` for
  architecture overview.

- **Layer rule (critical)**: lower-level modules must not depend on
  higher-level modules. Foundation is `common/` (zero deps), then
  `io/`, `storage/`, `wal/`, `sst/`, `metadata/`, up to `engine/`
  (public API). Run test validation after layer changes.

- **Build & test commands**: use these exact commands during edits:

  - `cargo build --workspace`
  - `cargo test` (runs unit + integration tests)
  - `cntryl-tools validate-tests` (test naming/structure validation)
  - `cargo bench` / `cargo bench --bench <name>` (Criterion benches)

- **Where examples live**:

  - Bench patterns: `benches/` (see `benches/criterion_helper.rs`).
  - Integration tests: top-level `tests/` (uses `should_{action}_when_{context}`).
  - Automation tooling: `cntryl-tools` for validation, inventory, and benchmark summaries; `scripts/test_watchdog.py` remains the repo-specific hang detector.

- **Test conventions (required)**:

  - Name: `should_{action}_when_{context}`. cntryl-tools validate-tests checks this.
  - Structure for non-trivial tests: include `// Arrange`, `// Act`,
    `// Assert`. Only one `// Act` per test. Small tests (<5 lines)
    may omit full AAA.

- **Bench rules (required)**:

  - Precompute all data outside `b.iter(|| ...)`.
  - No allocations or RNG inside hot loop; use deterministic seeds.
  - Use `group.sampling_mode(SamplingMode::Flat)` and
    `group.throughput(...)` (see `benches/criterion_helper.rs`).
  - Use `black_box` on inputs/outputs.

- **Integration points**:

  - Cloud backends: `src/storage/cloud/` (multiple providers, use test
    mocks in benches).
  - Storage modes: Memory, Local, Cloud—each with distinct source of
    truth (see `docs/development/the-big-idea.md`).
  - WAL & SST formats: `src/wal/` and `src/sst/` — be careful with
    on-disk compatibility and readers used in recovery tests.

- **Quick rules for edits**:
  - Run `cargo clippy --all-targets` and fix warnings before committing.
  - If touching low-level code, run unit tests and targeted benches.
  - For benches: precompute data, no allocations/RNG in hot loop, use
    `SamplingMode::Flat` + `black_box`.

If anything here is unclear or missing examples you want included,
tell me which area to expand (tests, benches, architecture, or CI).
