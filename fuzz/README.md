# cntryl-midge-fuzz

This private Cargo workspace contains Midge's `cargo-fuzz` targets. Targets
exercise WAL and metadata replay, SST block decoding, and differential engine
behavior. The `corpus/` directory contains retained inputs for the matching
targets.

From the repository root, use a nightly Rust toolchain and install
`cargo-fuzz` with `cargo install cargo-fuzz --locked`.

```sh
rustup run nightly cargo fuzz list
rustup run nightly cargo fuzz build
```

See the [fuzz qualification workflow](../.github/workflows/fuzz-qualification.yml)
for the sanitizer and corpus replay runs.
