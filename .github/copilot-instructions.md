<!--
    Concise Copilot instructions for the Midge repo. Keep this file short
    and focused: architecture pointers, developer workflows, project
    conventions, and where to look for authoritative examples.
-->

# GitHub Copilot Instructions — Midge (concise)

Purpose: help AI coding agents be productive quickly by describing
the project's architecture, conventions, and developer workflows.

- **Big picture**: Midge is an embedded LSM-tree engine in Rust. Key
    runtime pieces live under `src/`: `api/`, `core/`, `engine/`,
    `memtable/`, `compaction/`, `wal/`, `sst/`, `manifest/`, and
    `cloud/`. See `docs/DEPENDENCY_ANALYSIS.md` for layer rules.

- **Layer rule (critical)**: lower-level modules must not depend on
    higher-level modules. When changing layers, consult
    `docs/DEPENDENCY_ANALYSIS.md` and run the validation tests.

- **Build & test commands**: use these exact commands during edits:
    - `cargo build --workspace`
    - `cargo test` (runs unit + integration tests)
    - `cargo run --bin validate_tests -- --summary` (enforces test rules)
    - `cargo bench` / `cargo bench --bench <name>` (Criterion benches)

- **Where examples live**:
    - Bench patterns: `benches/` (see `benches/criterion_helper.rs`).
    - Integration tests: top-level `tests/` (uses `should_{action}_when_{context}`).
    - Automation scripts: `scripts/` (Python preferred).

- **Test conventions (required)**:
    - Name: `should_{action}_when_{context}`. Meta-tests enforce this.
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
    - Cloud backends: `src/cloud/` (MockCloud used in benches and tests).
    - WAL & SST formats: `src/wal/` and `src/sst/` — be careful with
        on-disk compatibility and readers used in recovery tests.

- **Quick rules for edits**:
    - Run `cargo clippy --all-targets` and fix warnings before committing.
    - If touching low-level code, run unit tests and targeted benches.
    - For benches, follow the repo's bench checklist in `TODO.md`.

If anything here is unclear or missing examples you want included,
tell me which area to expand (tests, benches, architecture, or CI).
- [ ] Correct throughput
- [ ] No thread spawns
- [ ] No allocations
- [ ] Uses Midge types
- [ ] Fast (<3s)
