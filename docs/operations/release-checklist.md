# Release Checklist

Use this checklist before publishing a release candidate or stable release.

## Run the automated qualification

In GitHub Actions, select **Release Qualification**, choose the candidate branch
or tag, and click **Run workflow**. The default needs no inputs. From the CLI:

```bash
gh workflow run release-qualification.yml --ref <candidate-ref>
```

The workflow must first be present on the default branch to appear as a manual
Actions entry. Each run checks the commit selected at dispatch, records its SHA,
and uploads a `release-result-<sha>-<attempt>` summary. Run it again after changing
the candidate, and qualify the resulting `main` commit before publication.
PRs that change the qualification workflows, Core CI workflow, or test Dockerfile
also run this workflow to validate the qualification process itself. Those runs
test the proposed merge commit; use the manual run on the actual candidate for
release evidence.

The required jobs run in parallel:

| Job | Evidence |
| --- | --- |
| Linux lint, build and tests | Existing CI: test structure, formatting, all-feature Clippy/check/build, workspace tests and serial fault injection |
| macOS tests | Default-feature workspace and fault-injection tests, run serially without an emulator dependency |
| Sqrzl and clean all-feature tests | Emulator health and resolved image digest, then the full all-feature suite in a clean Docker build, including provider contracts, engine recovery, discovery histories and doc tests |
| MSRV, features and package | Declared MSRV from Cargo metadata, no-default-feature Clippy, isolated provider-feature checks, unused dependencies, production dependency tree without `fail`, optimized build, quick start, CLI verification/format rejection and crate packaging |
| Corpus replay and bounded fuzzing | Every registered fuzz target builds and runs, preserved failure inputs replay, parser targets run for 30 seconds each and the engine differential runs for five minutes with AddressSanitizer |

**Qualification result** fails if any required job failed, was cancelled, or was
skipped. Inspect failures and artifacts; a retry does not erase the previous
result. Container logs/emulator identity, package metadata/crate, and fuzz
corpus/failure artifacts are retained for 30 days. Attach the run link to the
release evidence and preserve artifacts longer when the release needs them.

`extended_fuzz_minutes` optionally adds 30, 60, or 240 minutes of differential
fuzzing after the bounded checks. It defaults to zero. The extended campaign
disables AddressSanitizer to explore more histories; the mandatory bounded
campaign retains it. If requested, an extended-campaign failure fails
qualification. No fixed multi-hour campaign is required for routine releases;
use longer campaigns when changed storage/recovery paths or fuzz findings
justify them, and review execution/coverage progress in the logs.

Performance baselines use the existing Cargo benchmarks and benchmark workflows
on a suitable quiet runner. They are reviewed separately and keep their
environment/sampler guards. An incomplete or noisy performance row cannot be
reported as accepted evidence from a green qualification run. The workflow
does not decide issue acceptance, supported topologies, promotion, or publication.

## Review promotion and release evidence

- [ ] **Release Qualification** passed on the candidate SHA; run link and artifacts reviewed
- [ ] development work is merged into `develop` with passing current-head CI
- [ ] the `develop` → `main` promotion PR comes from this repository's `develop` branch and has passing CI, Cloud, and GitHub-managed CodeQL checks on its current head
- [ ] the promotion uses a merge commit and post-merge `main` checks pass
- [ ] the release tag points to a commit on `main`

- [ ] `cargo fmt --check`
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings -D clippy::pedantic`
- [ ] `cargo clippy --workspace --all-targets --no-default-features -- -D warnings -D clippy::pedantic`
- [ ] Rust 1.97 MSRV checks pass
- [ ] Linux and macOS tests pass
- [ ] each provider-only feature check passes (`cloud-aws`, `cloud-azure`, `cloud-gcp`, `cloud-oci`)
- [ ] `cargo test --workspace --all-features`
- [ ] `cargo test --workspace --all-features --doc`
- [ ] `cargo check --example documented_quick_start --all-features`
- [ ] `cargo machete`
- [ ] `cargo package --locked`
- [ ] with the local Sqrzl fixture running, `docker build --network=host --file Dockerfile.tests --tag midge-tests:release .`
- [ ] `cargo test --test fault_injection --all-features -- --test-threads=1`
- [ ] `cntryl-tools validate-tests`
- [ ] default `cargo tree --edges normal` and `cargo check --release` exclude `fail`
- [ ] Sqrzl `/healthz` responds successfully
- [ ] Sqrzl provider and provider-engine suites run with the `sqrzl-tests`
      feature and pass (an unreachable emulator fails hard)
- [ ] known differences found by manual real-cloud integration testing are
      recorded, reproduced in Sqrzl where applicable, and covered by Midge tests
- [ ] every registered fuzz target completed a bounded smoke run
- [ ] current contract docs present:
      `docs/user-guides/transaction-durability-contract.md`,
      `docs/development/support-matrix.md`,
      `docs/development/cloud-qualification-policy.md`,
      `docs/development/format-compatibility.md`,
      `docs/development/release-policy.md`,
      `docs/operations/operator-runbook.md`,
      `docs/operations/release-checklist.md`
- [ ] `cargo run --bin midge -- verify --json <database>` passes on a database the release candidate wrote, and prior-format fixtures under `tests/fixtures/compatibility/` are rejected with `CompatibilityError`
- [ ] changelog updated
- [ ] migration guide updated
- [ ] rollback statement added to release notes
- [ ] supported/stable matrix reviewed
- [ ] known-risk summary attached

Do not call a release production-ready unless qualification evidence is attached and the support matrix says the targeted topology is in scope.
