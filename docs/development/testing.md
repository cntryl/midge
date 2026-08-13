# Testing

Midge has unit tests in `src/` and integration tests in `tests/`. For early-adopter trust work, the important question is not raw test count, but which guarantees are actually proven.

## Running Tests

```bash
cargo test
```

Run a specific integration file:

```bash
cargo test --test durability_recovery
```

Run the trust-critical smoke suite:

```bash
cargo test --test external_adopter_smoke --features failpoints
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
cargo test --test repository_gates
cargo package --locked
docker build --file Dockerfile.tests --tag midge-tests:local .
```

GitHub keeps the required checks separated by cost and responsibility. `CI`
is the fast Ubuntu core suite. `Repository Qualification` covers docs,
examples, benchmarks, packaging, and repository contracts; `Platform` covers
macOS and Windows; `Compatibility` covers Rust 1.97 and provider-only
features; `Cloud Qualification` runs the Sqrzl emulator; and `Docker
Qualification` builds the test image. The extended workflows run manually,
on their schedules, and after a successful main-branch core run where
appropriate, so a pull request's core check stays focused.

Provider features are checked independently so one provider cannot hide a
dependency on another provider's implementation:

```bash
cargo check --workspace --all-targets --no-default-features --features cloud-aws
cargo check --workspace --all-targets --no-default-features --features cloud-azure
cargo check --workspace --all-targets --no-default-features --features cloud-gcp
cargo check --workspace --all-targets --no-default-features --features cloud-oci
```

The `cloud-oci` command is compile-only verification of the generic
S3-compatible alias. It does not qualify OCI wire behavior, conditional writes,
or provider error responses; see `docs/operations/cloud-setup.md`.

The scheduled fuzz workflow builds every registered target and runs bounded
smokes. Local smoke commands should use the same time and per-input bounds.

The scheduled/manual `Testing Governance` workflow owns the expensive,
informational checks. Its coverage-tier diff compares unit-only and
integration-only coverage for storage-critical modules; its mutation pilot
targets compaction, lease, WAL, metadata, and runtime code. Neither job is a
required pull-request check. Review the reports weekly and triage every item as
one of: wire the mechanism through production, remove it, strengthen a real
entry-point test, or record why it is intentionally test-only/accepted.

Sqrzl provider-engine tests are explicitly ignored in ordinary test runs. The
scheduled/manual `Cloud Qualification` workflow starts Sqrzl and selects those
ignored tests. Once selected, an unreachable emulator is a hard failure; there
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
| restart after committed writes restores state | `tests/durability_recovery.rs`, `tests/durability_wal.rs` |
| truncated WAL tail keeps valid prefix only | `src/wal/recovery.rs`, `tests/durability_wal.rs` |
| corrupted durable WAL prefix fails strict recovery | `src/wal/recovery.rs`, `tests/durability_wal.rs` |
| flush failure does not publish orphan SST state | `tests/failure_injection.rs` |
| flush restart recovers from WAL after interrupted publish | `tests/failure_injection.rs` |
| compaction crash before publish keeps input SSTs authoritative | `tests/chaos_compaction.rs`, `tests/failure_injection.rs` |
| compaction crash after publish keeps data visible and cleanup idempotent | `tests/chaos_compaction.rs`, `tests/failure_injection.rs` |
| iterators honor tombstones and latest-version resolution across SST boundaries | `tests/engine_iterators.rs`, `tests/engine_compaction.rs` |
| strict vs salvage recovery is explicit | `tests/failure_injection.rs`, `tests/durability_wal.rs` |
| released-format fixtures open and future-format fixtures fail with `CompatibilityError` | `tests/compatibility_fixtures.rs`, `src/metadata/format.rs` |

## External-Adopter Gate

Before inviting external evaluators, run at least:

```bash
cargo test --test external_adopter_smoke
cargo test --test durability_wal
cargo test --test failure_injection --features failpoints
cargo test --test chaos_compaction --features failpoints
cargo test --test engine_iterators
cargo test --test compatibility_fixtures
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

`tests/runtime_transaction_coalescing.rs` is the current #120 example: it
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
`tests/read_path_diagnostics.rs` proves through the public engine path that a
persisted bloom is consulted and a definite rejection avoids a data block
read. Add counters only for shipping mechanisms. Do not recreate a sparse-index
counter after that dormant implementation was removed.

When two implementations of one concept deliberately coexist, keep a shared
corpus differential test that proves agreement. Do not preserve an alternate
implementation solely to satisfy this convention: delete nonshipping variants
instead. The PR8 compression work removes the fast-accept alternate, so no
differential test should reintroduce it.

The compile-enforced manifests in `tests/coverage_manifests.rs` exhaustively
classify cloud credential sources, compression algorithms/policies, recovery
policies, and durability policies. Adding an enum variant requires updating
the corresponding manifest; an explicit intentionally-untested reason is
acceptable for scheduled real-provider cases. The mappings are compile-time
review contracts, while the named suites remain the behavioral proof.

## Shared Test Infrastructure Review

Treat failpoint registries, shared statics, temporary-directory helpers, and
chaos/fault-injection adapters like production code. Reviews must confirm:

- shared locks recover from poison instead of cascading one panic;
- every test reusing a failpoint name uses the same mutex/gate as existing users;
- governance checks discover production failpoint call sites mechanically,
  rather than maintaining a drift-prone allowlist;
- new fixture behavior has a regression through a real test entry point.

The repository failpoint contract test scans production call sites and is the
authoritative inventory; do not replace it with a hand-maintained path list.

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

```rust,ignore
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
