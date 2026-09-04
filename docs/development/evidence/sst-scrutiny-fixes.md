# SST scrutiny fixes: red → green

Baseline: `68821ee84a2c5d26ad32123905a7d4b84751f571`.
The pre-existing benchmark maintenance edits are separate from these fixes.

## Regressions and changes

| Defect | Recorded red result | Fix |
| --- | --- | --- |
| Valid trie deeper than 256 levels hides an empty key | Standalone SST and strict Engine post-flush point reads returned `None` | Iterative leftmost/prefix traversal; fall back to the complete binary index when the trie supplies no usable candidate |
| Writer accepts values that exceed its reader's decoded-block ceiling | Writer admission and transaction admission unexpectedly succeeded for a 64 MiB value | Check full key + value + V4 header before transaction staging/spilling and in both SST writer entry paths; cap data block targets at the decoded limit |
| Zstd buffer capacity exceeds the reserved decoded size | Declared/reserved 512 bytes; actual retained allocation 65,536 bytes | Decode known-size frames using their exact declared size, keeping the existing bounded fallback for frames without a content size |
| Sequence-zero values lose to the `Absent` sentinel | Basic `writer.add` roundtrip returned `None` | Treat absence explicitly and share state precedence between encoded point lookup and scan merging |

The oversized-entry policy rejects a new operation with `ResourceLimit` before
it enters the transaction. Previously staged operations remain valid. The check
accounts for the 26-byte V4 header, its 8-byte extended-length addition for long
keys, and a full key at a block restart. TTL uses the same fixed header. No SST
format or reader ceiling is changed. This does not repair previously emitted
SSTs containing oversized compressed entries.

Trie node graph validation remains in place, including cycle and unreachable-node
rejection. Iteration removes the artificial traversal depth limit without making
invalid graphs acceptable. The sequence-zero fix preserves tombstone precedence
at equal sequence numbers and existing TTL/snapshot semantics.

The Zstd result establishes decoded-buffer capacity accounting; it does not claim
that codec contexts, transport buffers, all cache readers, or total process RSS
are now covered by the compaction pool.

## Recorded red

Before modifying production source:

```sh
cargo test --lib sst::fs::regression_tests -- --test-threads=1
# 4 executed, 4 failed
cargo test --test sst_regressions -- --test-threads=1
# 2 executed, 2 failed
```

The preceding scrutiny also reproduced a successful strict Engine commit and
flush followed by a corruption error for an oversized value. The permanent
regression instead asserts rejection before acknowledgement, which is the chosen
contract. The deep-trie regression captures the post-flush read, verifies storage,
reopens, and checks both reads; successful verification/reopen cannot mask a
failed post-flush assertion.

## Green coverage

The original six regressions pass. Expanded coverage checks zero-sequence values,
empty values, TTL expiration, tombstone ties, snapshot sequence zero, both scan
directions, a 300-level prefix traversal, exact admission boundaries around the
extended key header, and the largest readable entry through sorted and unsorted
writers. The Engine admission test covers both put and insert, including committing
valid work in the same transaction after rejecting an oversized operation.

Focused validation:

```sh
cargo test --lib sst:: -- --test-threads=1
# 565 passed
cargo test --features failpoints --test sst_regressions \
  --test engine_compaction --test sst_reads_integration \
  --test storage_verification_hardening --test compression_compatibility \
  --test compatibility_fixtures --test compaction_snapshot_publication
# 42 passed
```

Raw red/green logs and gate results are retained locally under
`target/sst-fixes/`. The permanent tests are in `src/sst/fs/regression_tests.rs`,
`src/sst/encoding.rs`, and `tests/sst_regressions.rs`.

The first workspace run caught an architecture check on the new test file's
direct `finish_to_path` calls. The tests now use the existing
`finish_writer_to_path` filesystem helper; the allowlist and production behavior
were unchanged. All 30 architecture tests then passed. A complete workspace
rerun uses `--no-fail-fast` so every target is attempted.

## Final repository gates

- `cargo fmt --check`: passed.
- `cargo build --workspace`: passed.
- `cargo test --workspace --all-features --no-fail-fast`: **2,665 passed, 0 failed, 11 ignored**, exit 0 (including doctests).
- `cargo clippy --workspace --all-targets --all-features -- -D warnings -D clippy::pedantic`: passed.
- `cntryl-tools validate-tests`: 2,660 compliant, zero violations.
- `git diff --check`: passed.

The final workspace log is `target/sst-fixes/workspace-tests-final.log`; the
machine-readable result is `target/sst-fixes/final-summary.json`. Ignored tests
retain their existing repository conditions; live-provider qualification and
performance benchmarks were not part of this SST fix verification.
