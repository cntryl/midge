# Release Checklist

Use this checklist before publishing a release candidate or stable release.

- [ ] `cargo clippy --all-targets -- -D warnings`
- [ ] `cargo test`
- [ ] `cargo test --test external_adopter_smoke`
- [ ] `cargo test --test durability_wal`
- [ ] `cargo test --test failure_injection`
- [ ] `cargo test --test chaos_compaction`
- [ ] `cargo test --test engine_iterators`
- [ ] `cargo test --test compatibility_fixtures`
- [ ] `cntryl-tools validate-tests`
- [ ] production contract docs present:
      `docs/development/one-dot-zero-contract.md`,
      `docs/development/support-matrix.md`,
      `docs/development/format-compatibility.md`,
      `docs/development/release-policy.md`,
      `docs/operations/production-runbook.md`,
      `docs/operations/release-checklist.md`
- [ ] `cargo run --bin midge -- verify --json tests/fixtures/compatibility/v2_empty_db` validated where applicable
- [ ] changelog updated
- [ ] migration guide updated
- [ ] rollback statement added to release notes
- [ ] supported/experimental matrix reviewed
- [ ] known-risk summary attached

Do not call a release production-ready unless qualification evidence is attached and the support matrix says the targeted topology is in scope.
