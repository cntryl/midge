# Repository Guidelines

## Agent Task Scope

These rules apply to every agent, including delegated agents:

- Stay focused on the user's requested task. Keep changes directly related to that task; do not expand into unrelated cleanup, refactoring, or infrastructure work.
- Do not create scripts, custom runners, tools, or automation unless the user explicitly requests them. Use the existing Cargo, `cntryl-tools`, and `cntryl-stress` commands and facilities.
- Do not modify GitHub Actions, CI, release, benchmark, or other workflows unless the user explicitly requests workflow changes.
- Use existing test fixtures and framework facilities. Keep any necessary Rust test helpers scoped to the invariant or behavior being tested; do not turn task work into a tooling or framework project.

## Project Structure & Module Organization

Midge is a Rust embedded LSM key-value engine. Core code lives under `src/`, with major subsystems split by responsibility: `storage/` for local/cloud backends, `wal/` for write-ahead logging, `sst/` for table format and readers, `metadata/` for manifests, `runtime/` for actor/event-loop coordination, and `engine/` for the public API. Integration tests live in `tests/`; benchmarks live in `benches/`; design and operations docs live in `docs/`; fuzz targets live in `fuzz/`.

## Build, Test, and Development Commands

- `cargo build --workspace`: compile the crate and workspace targets.
- `cargo test`: run unit and integration tests.
- `cargo test --test cloud_core -- cloud_persistence_hardening --nocapture`: run a focused integration suite.
- `cargo fmt --check`: verify Rust formatting.
- `cargo fmt`: apply standard Rust formatting.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings -D clippy::pedantic`: enforce zero-warning lint policy.
- `cargo bench`: run registered `cntryl-stress` benchmarks.
- `cntryl-tools validate-tests`: check test naming and structure when available.

## Coding Style & Naming Conventions

Use Rust 2021 style and `rustfmt` defaults. Prefer clear subsystem boundaries; lower layers should not depend on higher layers. Keep storage/WAL/SST durability code conservative: retain data when unsure. Test names should follow `should_{action}_when_{context}` or similarly descriptive `should_...` patterns. Use explicit `// Arrange`, `// Act`, and `// Assert` sections for non-trivial tests.

Never suppress `clippy::too_many_lines` with `#[allow]`, `#[expect]`, configuration, or command-line overrides. Split oversized functions into focused helpers while preserving behavior and assertions.

## Testing Guidelines

Keep engine tests and test runners in Rust and run them through Cargo. Do not add Python files to this repository. Use `cntryl-tools` as the first choice for additional static analysis rather than adding repository-specific analysis scripts.

All new behavior should include tests. Put public API coverage in `tests/`; use inline `#[cfg(test)] mod tests` for focused internal logic. Use deterministic failpoints and crash/recovery tests for durability-sensitive changes. For cloud, WAL, SST, and manifest changes, prefer TDD regression tests that first demonstrate unsafe deletion, corrupt recovery, stale metadata, or incorrect frontier movement.

Tests must cover real invariants and observable behavior through the production code paths they protect. Assert the contract and failure outcome, including restart or recovery when relevant. Tests that merely inspect source text, exercise runner bookkeeping, or mirror implementation details are not substitutes for invariant and behavior coverage.

Do not use `#[ignore]` on tests. Unresolved bugs must have active failing regression tests until they are fixed; do not skip them, invert their assertions, or hide them behind a feature flag. Feature selection may separate genuine environment dependencies such as Sqrzl, provided the corresponding workflow runs those tests and fails if the dependency is unavailable.

## Benchmark Guidelines

Write idiomatic Rust benchmarks using `cntryl-stress`, registered and run through the repository's existing `cargo bench` integration. Follow the established `cntryl-stress` patterns for workloads, measurements, and reporting. Keep each benchmark focused on the performance behavior being measured; do not add bespoke benchmark runners, scripts, reporting tools, or workflow changes.

## Commit & Pull Request Guidelines

Recent history uses concise imperative or conventional commit subjects, for example `fix: prune cloud-covered remote wal segments`, `feat: enable feature-based testing...`, and `Harden cloud WAL cleanup proof validation`. Prefer `<type>: <summary>` for routine work (`fix`, `feat`, `refactor`, `test`, `docs`, `perf`, `chore`). PRs should explain what changed, why, risk level, linked issues, and exact verification commands run. Note any durability, recovery, or API compatibility impact explicitly.

Use the GitHub CLI (`gh`) for all GitHub operations, including reading and updating issues, pull requests, milestones, releases, and checks.

## Tracking Review Findings

Track code review and audit findings as GitHub issues in `cntryl/midge` rather than fixing them ad hoc. Write findings to a local file first, triage them (drop false positives, merge duplicates, check open issues), and file only confirmed findings. Each issue should state the failure scenario, the regression test that would demonstrate it, and labels: one `priority:p0`–`priority:p3`, an `area:*` subsystem label, and `flea` for bugs or `solid` for design/SOLID work. Work one branch and PR at a time, each covering a logical group of related issues in the same subsystem, and drive it to merge before starting the next; durability fixes follow the TDD guidance above. Mechanical fixes with no design judgment (clippy, unused dependencies, formatting, behavior-preserving file splits) may go straight to a PR without an issue.

## Jev-Assisted Reviews

Jev is TypeSafe AI's System One decision API. It returns typed `choice`, `score`, and `noul` judgments; it is useful for prioritizing review questions, not for proving a defect or generating a patch. Keep each request narrow: include the relevant source facts, the invariant or contract, a concrete failure scenario, and bounded choices. Split broad reviews into subsystem-sized questions. If a result is weak or unclear, rephrase with more exact code-path details and sharper alternatives.

Call `POST https://api.typesafe.ai/v1/systemone` with `{ "model": "jev-latest", "state": ..., "questions": ... }` and bearer authentication. `questions` is a map keyed by caller-chosen names; each value has a `type` (`choice`, `score`, or `noul`) and the fields for that type. Read the API key from the existing local TypeSafe configuration (`~/.config/typesafe/jev.key` in the standard development environment). Never print, log, paste into prompts, or commit the key. Send only the code context needed for the judgment.

Treat Jev output as an investigation lead. Verify every nontrivial result against the implementation, relevant tests, documented contracts, and measurements where performance is involved. Drop unsupported or duplicate suggestions; only confirmed findings belong in the issue tracker.

## Security & Configuration Tips

Do not commit credentials or real cloud configuration. Use local filesystem-backed cloud stores and mock providers in tests unless an explicit integration environment is required. Treat storage leaks as acceptable when the alternative is unsafe deletion.
