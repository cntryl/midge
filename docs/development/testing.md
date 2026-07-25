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
cntryl-tools validate-benchmarks --config .cntryl/repository.toml
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

The scheduled fuzz workflow builds every registered target and runs bounded
smokes. Local smoke commands should use the same time and per-input bounds.

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
