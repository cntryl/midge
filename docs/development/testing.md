# Testing

Midge has unit tests in `src/` and integration tests in `tests/`. For early-adopter trust work, the important question is not raw test count, but which guarantees are actually proven.

## Running Tests

```bash
cargo test
```

Run a specific integration file:

```bash
cargo test --test durability -- durability_recovery
```

Run the trust-critical smoke suite:

```bash
cargo test --test fault_injection --features failpoints -- --test-threads=1 external_adopter_smoke
```

Validate naming and AAA structure:

```bash
cntryl-tools validate-tests
```

Run the repository and packaging qualification gates:

```bash
cargo test --workspace --all-features --doc
cargo check --example documented_quick_start --all-features
cargo machete
cargo package --locked
docker compose up -d sqrzl
docker build --network=host --file Dockerfile.tests --tag midge-tests:local .
```

Pull requests into `develop` and `main` run `ci.yml` on Ubuntu. It validates
test conventions first, then runs formatting, strict Clippy, `cargo check`,
`cargo build`, and `cargo test`. Tests enable failpoints, with the dedicated
fault-injection target run serially because its failpoints are process-global.
Sqrzl tests are selected by the `sqrzl-tests` feature and run in
`integration-s3.yml`, `integration-azure.yml`, and `integration-gcs.yml`.
Unresolved regression tests remain active and failing; tests must not use ignore
attributes.

CI also runs on pushes to both branches, weekly on the default `develop`
revision, and by manual dispatch. A manual dispatch can select `main` for
release qualification. Each provider integration runs the complete existing
Sqrzl suite: provider contracts (including OCI), engine qualification, cloud WAL
takeover, and logical histories for S3, Azure, GCS XML and GCS JSON. The files
repeat these commands deliberately; there are no test exclusions. Some deeper
operational scenarios are implemented only for S3, so running the full existing
suite does not establish equivalent operational coverage for Azure and GCS.

Each provider workflow has one job and uses path filters on pull requests
and pushes to both branches. Changes to a provider implementation or resolver
run that provider's workflow; other Rust source, test fixtures, Cargo
configuration, and emulator configuration conservatively run all three.
Unrelated docs and benchmark changes do not trigger these workflows. Daily
schedules and manual dispatch run the complete existing cloud suite without
change filtering. GitHub manages Sqrzl as a service container; there are no
result uploads or extra coordination jobs.

These workflows use workflow-level path filters. Their checks must not be
unconditionally required by branch protection: a path-filtered workflow can
leave its required check pending for unrelated changes.

Promotion pull requests into `main` run CI, the applicable provider integrations,
and GitHub-managed CodeQL.
Reviewers verify that the PR head is this repository's `develop` branch.
Documentation and packaging checks run in the tagged publish workflow.

To check provider features independently when changing their dependencies:

```bash
cargo check --workspace --all-targets --no-default-features --features cloud-aws
cargo check --workspace --all-targets --no-default-features --features cloud-azure
cargo check --workspace --all-targets --no-default-features --features cloud-gcp
cargo check --workspace --all-targets --no-default-features --features cloud-oci
```

These commands check compilation only. The provider integrations run transport contracts
and cache-loss recovery for S3-compatible storage, Azure Blob, GCS XML, GCS JSON,
and OCI's S3-compatible configuration. Sqrzl S3 tests exercise the shared AWS
protocol implementation; OCI tests use `OciObjectStorageConfig` with a local
endpoint override, not the native OCI API.

The scheduled fuzz workflow builds every registered target and runs bounded
smokes. Local smoke commands should use the same time and per-input bounds.

Sqrzl provider-engine tests require the `sqrzl-tests` feature. The
provider integration workflows use GitHub-managed Sqrzl services and run tests
through Cargo, without start, wait, or cleanup steps. Once selected,
an unreachable emulator is a hard failure; there
is no runtime skip that can be confused with a passing qualification.
The provider contract includes a zero-byte object PUT/HEAD/GET/DELETE lifecycle
because empty authority documents occur in the WAL and metadata paths and some
real providers require an explicit `Content-Length: 0`. Provider unit tests also
reject redirects and undocumented mutation success statuses.

Sqrzl is the authoritative continuous cloud qualification environment for this
repository. Manual real-cloud integration testing validates emulator fidelity
and deployment-specific assumptions; it is not a credential-bearing CI
dependency. Any provider difference found manually should become a deterministic
Sqrzl scenario and Midge regression test. See the
[cloud qualification policy](cloud-qualification-policy.md).

## Trust Matrix

Use this matrix when updating guarantees or reviewing whether Midge is safe enough to evaluate.

| Guarantee | Representative tests |
|---|---|
| restart after committed writes restores state | `tests/durability.rs` — `should_restore_committed_write_given_local_restart_when_sync_commit_returned` |
| truncated WAL tail keeps valid prefix only | `tests/durability.rs` — `should_keep_valid_prefix_given_truncated_wal_tail_when_reopening_in_strict_mode` |
| corrupted durable WAL prefix fails strict recovery | `tests/durability.rs` — `should_fail_strict_but_salvage_valid_prefix_given_corrupted_first_wal_frame_when_reopening` |
| flush failure does not publish orphan SST state | `tests/fault_injection.rs` — `should_ignore_orphan_sst_when_flush_intent_log_save_hits_no_space` |
| flush restart recovers from WAL after interrupted publish | `tests/fault_injection.rs` — `should_retry_flush_given_transient_publish_failure_when_reopening` |
| compaction crash before publish keeps input SSTs authoritative | `tests/fault_injection.rs` — `should_retain_input_ssts_given_compaction_failure_before_manifest_publish` |
| compaction crash after publish keeps data visible and cleanup idempotent | `tests/fault_injection.rs` — `should_not_delete_input_ssts_given_compaction_gc_failure_after_manifest_publish` |
| iterators honor tombstones and latest-version resolution across SST boundaries | `tests/engine_api.rs` — `should_respect_range_tombstones_given_delete_range_when_scanning`, `should_return_latest_value_across_levels_given_overwrite_in_newer_sst_when_scanning` |
| strict vs salvage recovery is explicit | `tests/durability.rs` — `should_fail_strict_open_when_wal_is_corrupt`, `should_open_in_salvage_mode_when_wal_is_corrupt` |
| pre-ledger (format 3 and older) and future-format fixtures fail with `CompatibilityError` | `tests/storage_layer.rs` — `should_reject_populated_release_v3_fixture_given_unsupported_format_when_reopening`, `should_reject_future_v9_fixture_given_unsupported_version_when_reopening` |

## External-Adopter Gate

Before inviting external evaluators, run at least:

```bash
cargo test --test fault_injection --features failpoints -- --test-threads=1 external_adopter_smoke
cargo test --test durability -- durability_wal
cargo test --test fault_injection --features failpoints -- --test-threads=1 failure_injection
cargo test --test fault_injection --features failpoints -- --test-threads=1 chaos_compaction
cargo test --test engine_api -- engine_iterators
cargo test --test storage_layer -- compatibility_fixtures
```

This is the minimum “safe enough to try” gate for local-disk evaluation.

## Where To Add Tests

- `src/**`: unit tests for WAL parsing, corruption detection, sequence ordering, and other local invariants
- `tests/**`: end-to-end recovery, flush publication, compaction publication, iterator correctness, and restart behavior

Prefer extending an existing file when the guarantee already has a home. Add a new file only when it becomes a clearer public test category.

## Test Through the Real Entry Point

Behavioral tests must enter through the same public or crate-level boundary as
production. Do not populate private queues and call an internal drain method
directly. For example, a write-coalescing test should submit concurrent
`Transaction::commit` requests (or the runtime `submit` boundary) and assert
the durable result plus the coarse mechanism metric. Calling a hypothetical
`drain_as_leader` directly only proves the helper works, not that production
can reach it.

Review heuristic: would the test still prove the behavior if the internal
method were deleted and its implementation inlined into its caller? If not,
move the test to the real entry point or explicitly classify it as a local unit
invariant rather than mechanism-reachability evidence.

`tests/transactions.rs` (`should_preserve_runtime_coalescing_when_threads_write_concurrently`) is the current #120 example: it
drives concurrent commits through the engine and compares logical operations
with physical WAL appends. The deleted caller-side leader/follower grouping is
not assigned counters because it is no longer a production mechanism.

## Acceptance Evidence Before Merge

Every pull request must translate each linked issue criterion into a checked
acceptance-audit entry. Each entry names the exact evidence, identifies the
production entry point (or honestly labels a local invariant), and records
whether the implementation follows the requested approach or intentionally
uses a different resolution. A renamed pre-existing test is not new evidence,
and a partial assertion must not be presented as satisfying a stronger value,
failure, recovery, or performance contract.

The `PR Acceptance / Acceptance evidence` check validates this structure and
rejects unchecked criteria. It cannot decide whether engineering evidence is
correct; that remains an adversarial reviewer responsibility. Its purpose is
to make omissions and changed interpretations visible before merge rather than
discovering them in a later issue sweep.

## Mechanism Observability

Coarse counters are deliberate operator and testing contracts, not dumps of
private state. `wal_append_count` versus logical commits demonstrates runtime
transaction coalescing. `durability_waiters_fanned_out_total` counts waiters
completed through keyed durability events and is distinct from write
coalescing. V4 point reads expose `sst_bloom_checks_total` and
`sst_bloom_rejects_total` alongside `sst_data_blocks_read_total`;
`tests/observability.rs` (`should_reject_absent_key_with_bloom_without_reading_data_block`) proves through the public engine path that a
persisted bloom is consulted and a definite rejection avoids a data block
read. Add counters only for shipping mechanisms. Do not recreate a sparse-index
counter after that dormant implementation was removed.

`abandoned_runtime_requests_total` and `late_runtime_responses_total` are a pair
and must be read together. The first counts callers that stopped waiting for a
runtime response; the second counts responses that arrived with no caller left to
receive them. A runtime-response `MidgeError::Timeout` removes the caller's
route but does not cancel work the runtime already accepted. Both counters cover
routed requests and the inline transaction path, which does not use the router's
pending table. Tests and diagnostics may use the pair to measure aggregate
timeout and late-response behavior, but must not correlate their deltas with one
request or infer whether a timed-out mutation succeeded: unrelated requests and
error responses contribute to the same totals.

When two implementations of one concept deliberately coexist, keep a shared
corpus differential test that proves agreement. Do not preserve an alternate
implementation solely to satisfy this convention: delete nonshipping variants
instead. The PR8 compression work removes the fast-accept alternate, so no
differential test should reintroduce it.

## Shared Test Infrastructure Review

Treat failpoint registries, shared statics, temporary-directory helpers, and
chaos/fault-injection adapters like production code. Reviews must confirm:

- shared locks recover from poison instead of cascading one panic;
- every test reusing a failpoint name uses the same mutex/gate as existing users;
- new fixture behavior has a regression through a real test entry point.

## Crash-Trigger Evidence

Every subprocess crash test must prove that its intended boundary fired. Use
`tests/common/crash.rs` to configure the named failpoint (or record a named
logical boundary), sync the per-scenario trigger sentinel, and validate both
the OS-level abort and exact sentinel in the parent. A trailing child
panic is only a failure fallback; it must never satisfy the parent assertion.
The shared validator includes child stdout and stderr when the child exits at
the wrong boundary.

Counted or randomized crash points must also print their selected seed or
offset on every run and accept an environment override so CI failures are
replayable. Keep each production path's failpoint names unique: direct and
spilled transaction commit paths, for example, must never consume the same
global counter.

Crash fixture call sequence using the integration test's `crash` helper:

```text
crash::configure_abort_failpoint("midge::component::boundary", "scenario");
// Parent:
crash::run_child_expect_abort(&mut command, "scenario", "midge::component::boundary", db_path);
```

## Naming

Use:

- `should_<behavior>_given_<context>_when_<condition>`

The test name should state the promised behavior, not just the API call being made.

## AAA Structure

For non-trivial tests, use a visible Arrange / Act / Assert structure.

```rust
#[test]
fn should_preserve_valid_prefix_given_truncated_wal_tail_when_recovering() {
    // Arrange

    // Act

    // Assert
}
```

## Notes

- Legacy tests still exist and are being tightened gradually.
- Prefer durable local-disk coverage for trust-critical tests unless the behavior is specifically cloud-only.
- Informational tests that only print status are not sufficient for trust guarantees; each guarantee should have an asserted outcome.

The [#808 regression inventory](wal-redesign-808.md) documents the named WAL
audit baseline runner and its acceptance boundary. Its Sqrzl-only case,
`predecessor-cache` in `tests/fault_injection/wal_authority_regressions.rs`, runs
in `integration-s3.yml`; the baseline runner selects it with `--with-sqrzl`. All
regressions are active in their Cargo targets, including unresolved failures.

## Watchdog contention qualification

The Cargo-native watchdog executable can repeat its complete suite while CPU
contenders run in the parent process. Use a clean, committed checkout and retain
the evidence outside it:

```sh
MIDGE_WATCHDOG_CPU_WORKERS=4 MIDGE_WATCHDOG_REPEATS=3 \
MIDGE_WATCHDOG_EVIDENCE_DIR=/tmp/midge-watchdog-contention \
cargo test --locked --all-features --test stress_workload_watchdog
```

Each repeat runs all 20 checks, including fresh warmup/measured/cooldown inputs,
four deliberate no-progress failures with zero completed units, and independently
bounded preparation and execution children. Receipts and child logs are grouped
under `run-01`, `run-02`, and `run-03`. CPU contenders stop and join on exit.
Healthy recovery runs must exceed the actual three-second native idle budget;
held and progressing controls use that same budget. These fixtures qualify
watchdog signaling and supervision under CPU contention, not disk latency or
engine capacity. Production watchdog settings remain unchanged.
