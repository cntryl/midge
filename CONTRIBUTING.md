# Contributing to Midge

Follow the [cntryl contribution standards](https://github.com/cntryl/.github/blob/main/CONTRIBUTING.md).
Link substantial changes to an issue and agree on scope with a maintainer before
implementation. Report security issues through [SECURITY.md](SECURITY.md).

## Workspace layout

The root Cargo manifest defines the workspace. Its default package is
`cntryl-midge`, so commands such as `cargo test --test verification_cli`
and `cargo run --example documented_quick_start` work from the root.

| Directory | Responsibility |
| --- | --- |
| [`crates/midge`](crates/midge/README.md) | Public engine API, CLI, runtime, compaction, and end-to-end tests and benchmarks |
| [`crates/core`](crates/core/README.md) | Shared types, codecs, I/O primitives, and telemetry |
| [`crates/table`](crates/table/README.md) | Memtables, SSTs, and table iterators |
| [`crates/storage`](crates/storage/README.md) | Local and cloud storage backends |
| [`crates/durability`](crates/durability/README.md) | WAL, manifests, intent logs, and recovery |
| [`fuzz`](fuzz/README.md) | Cargo fuzz targets and retained corpora |

Keep tests and benchmarks with the crate that owns the behavior. For local
Cargo path dependencies, point `cntryl-midge` at `crates/midge`.

## Check changes

Use the Rust version in the [workspace manifest](Cargo.toml) or newer.
Install the repository validator with
`cargo install --git https://github.com/cntryl/tools --locked`,
then run these checks from the repository root.

```sh
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings -D clippy::pedantic
cargo test --locked --workspace --features failpoints
cntryl-tools validate-tests
```

Use `should_...` test names and Arrange, Act, and Assert sections for
non-trivial tests. Use deterministic failpoints and restart tests when changing
durability behavior. The [testing guide](docs/development/testing.md) covers
fault injection, Sqrzl, and release checks. The
[benchmark guide](docs/development/benchmarks.md) covers workloads and how to
compare results.

Open work PRs against `develop` with the linked issue, the reason for the change,
and the exact checks run. Describe durability, recovery, or API compatibility
effects when they apply. Maintainers promote qualified changes to `main`.
