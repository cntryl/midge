# Release Checklist

Use this checklist before publishing a release candidate or stable release.

- [ ] `cargo fmt --check`
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings -D clippy::pedantic`
- [ ] `cargo clippy --workspace --all-targets --no-default-features -- -D warnings -D clippy::pedantic`
- [ ] Rust 1.97 MSRV checks pass
- [ ] Linux, macOS, and Windows tests pass
- [ ] each provider-only feature check passes (`cloud-aws`, `cloud-azure`, `cloud-gcp`, `cloud-oci`)
- [ ] `cargo test --workspace --all-features`
- [ ] `cargo test --workspace --all-features --doc`
- [ ] `cargo check --example documented_quick_start --all-features`
- [ ] `cargo machete`
- [ ] `cntryl-tools validate-benchmarks --config .cntryl/repository.toml`
- [ ] `cargo package --locked`
- [ ] `docker build --file Dockerfile.tests --tag midge-tests:release .`
- [ ] `cargo test --test external_adopter_smoke --features failpoints`
- [ ] `cargo test --test durability_wal`
- [ ] `cargo test --test failure_injection --features failpoints`
- [ ] `cargo test --test chaos_compaction --features failpoints`
- [ ] `cargo test --test engine_iterators`
- [ ] `cargo test --test compatibility_fixtures`
- [ ] `cntryl-tools validate-tests`
- [ ] default `cargo tree --edges normal` and `cargo check --release` exclude `fail`
- [ ] Sqrzl `/healthz` responds successfully
- [ ] Sqrzl provider and provider-engine qualification suites are explicitly
      selected with `--ignored` and pass (an unreachable emulator fails hard)
- [ ] every registered fuzz target completed a bounded smoke run
- [ ] current contract docs present:
      `docs/user-guides/transaction-durability-contract.md`,
      `docs/development/support-matrix.md`,
      `docs/development/format-compatibility.md`,
      `docs/development/release-policy.md`,
      `docs/operations/operator-runbook.md`,
      `docs/operations/release-checklist.md`
- [ ] `cargo run --bin midge -- verify --json tests/fixtures/compatibility/v3_populated_v4_sst_db` validated where applicable
- [ ] changelog updated
- [ ] migration guide updated
- [ ] rollback statement added to release notes
- [ ] supported/experimental matrix reviewed
- [ ] known-risk summary attached

Do not call a release production-ready unless qualification evidence is attached and the support matrix says the targeted topology is in scope.
