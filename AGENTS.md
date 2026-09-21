# Repository Guidelines

## Project Structure & Module Organization

Midge is a Rust embedded LSM key-value engine. Core code lives under `src/`, with major subsystems split by responsibility: `storage/` for local/cloud backends, `wal/` for write-ahead logging, `sst/` for table format and readers, `metadata/` for manifests, `runtime/` for actor/event-loop coordination, and `engine/` for the public API. Integration tests live in `tests/`; benchmarks live in `benches/`; design and operations docs live in `docs/`; helper scripts live in `scripts/`; fuzz targets live in `fuzz/`.

## Build, Test, and Development Commands

- `cargo build --workspace`: compile the crate and workspace targets.
- `cargo test`: run unit and integration tests.
- `cargo test --test cloud_core -- cloud_persistence_hardening --nocapture`: run a focused integration suite.
- `cargo fmt --check`: verify Rust formatting.
- `cargo fmt`: apply standard Rust formatting.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings -D clippy::pedantic`: enforce zero-warning lint policy.
- `cargo bench`: run registered Criterion benchmarks.
- `cntryl-tools validate-tests`: check test naming and structure when available.

## Coding Style & Naming Conventions

Use Rust 2021 style and `rustfmt` defaults. Prefer clear subsystem boundaries; lower layers should not depend on higher layers. Keep storage/WAL/SST durability code conservative: retain data when unsure. Test names should follow `should_{action}_when_{context}` or similarly descriptive `should_...` patterns. Use explicit `// Arrange`, `// Act`, and `// Assert` sections for non-trivial tests.

## Testing Guidelines

All new behavior should include tests. Put public API coverage in `tests/`; use inline `#[cfg(test)] mod tests` for focused internal logic. Use deterministic failpoints and crash/recovery tests for durability-sensitive changes. For cloud, WAL, SST, and manifest changes, prefer TDD regression tests that first demonstrate unsafe deletion, corrupt recovery, stale metadata, or incorrect frontier movement.

## Commit & Pull Request Guidelines

Recent history uses concise imperative or conventional commit subjects, for example `fix: prune cloud-covered remote wal segments`, `feat: enable feature-based testing...`, and `Harden cloud WAL cleanup proof validation`. Prefer `<type>: <summary>` for routine work (`fix`, `feat`, `refactor`, `test`, `docs`, `perf`, `chore`). PRs should explain what changed, why, risk level, linked issues, and exact verification commands run. Note any durability, recovery, or API compatibility impact explicitly.

## Tracking Review Findings

Track code review and audit findings as GitHub issues in `cntryl/midge` rather than fixing them ad hoc. Write findings to a local file first, triage them (drop false positives, merge duplicates, check open issues), and file only confirmed findings. Each issue should state the failure scenario, the regression test that would demonstrate it, and labels: one `priority:p0`–`priority:p3`, an `area:*` subsystem label, and `flea` for bugs or `solid` for design/SOLID work. Work one branch and PR at a time, each covering a logical group of related issues in the same subsystem, and drive it to merge before starting the next; durability fixes follow the TDD guidance above. Mechanical fixes with no design judgment (clippy, unused dependencies, formatting, behavior-preserving file splits) may go straight to a PR without an issue.

## Security & Configuration Tips

Do not commit credentials or real cloud configuration. Use local filesystem-backed cloud stores and mock providers in tests unless an explicit integration environment is required. Treat storage leaks as acceptable when the alternative is unsafe deletion.
