# Fuzz Testing for Midge

This directory contains fuzz testing targets for critical parsing components
of the Midge storage engine.

## Prerequisites

Fuzz testing with `cargo-fuzz` requires:

1. **Nightly Rust toolchain** (for sanitizers)
2. **Linux or macOS** (libfuzzer doesn't support Windows)

```bash
# Install nightly
rustup install nightly

# On Linux/macOS, run fuzz tests
cargo +nightly fuzz run fuzz_tlv_reader -- -max_len=4096
```

## Fuzz Targets

| Target | Description | Critical Level |
|--------|-------------|----------------|
| `fuzz_tlv_reader` | TLV parser (foundation of all serialization) | 🔴 Critical |
| `fuzz_wal_decode` | WAL record decoder (crash recovery) | 🔴 Critical |
| `fuzz_sst_metadata` | SST sparse index and data blocks | 🔴 Critical |
| `fuzz_block_decode` | SST block decoder (all block types) | 🟡 High |
| `fuzz_bloom_filter` | Bloom filter decoder | 🟡 High |
| `fuzz_internal_key` | Internal key encoding/decoding | 🟢 Medium |

## Running Fuzz Tests

### Quick Run (10 minutes)
```bash
cargo +nightly fuzz run fuzz_tlv_reader -- -max_total_time=600 -max_len=4096
```

### Full Corpus Build (overnight)
```bash
for target in $(cargo fuzz list); do
    cargo +nightly fuzz run $target -- -max_total_time=3600 -max_len=65536
done
```

### CI Integration
```bash
# Run each target for 5 minutes in CI
cargo +nightly fuzz run fuzz_tlv_reader -- -max_total_time=300
cargo +nightly fuzz run fuzz_wal_decode -- -max_total_time=300
cargo +nightly fuzz run fuzz_sst_metadata -- -max_total_time=300
```

## Reproducing Crashes

When a crash is found, it's saved in `fuzz/artifacts/<target>/`:

```bash
# Reproduce
cargo +nightly fuzz run fuzz_tlv_reader fuzz/artifacts/fuzz_tlv_reader/crash-xxxxx

# Minimize
cargo +nightly fuzz tmin fuzz_tlv_reader fuzz/artifacts/fuzz_tlv_reader/crash-xxxxx
```

## Coverage

Generate coverage reports to find untested code paths:

```bash
cargo +nightly fuzz coverage fuzz_tlv_reader
# Coverage data in fuzz/coverage/fuzz_tlv_reader/
```

## Windows Alternative

On Windows, use property-based testing with `proptest` instead:

```bash
# Run proptest-based tests (works on stable Rust, all platforms)
cargo test --test proptest_parsers
```

See `tests/proptest_parsers.rs` for property-based test equivalents.
