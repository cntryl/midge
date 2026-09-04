# PR #278 review fixes: red → green

Baseline: `d796b3103acffdbe36bb19c53336f65914807948`.
This follow-up addresses the three correctness findings and five cleanup
findings from the SST/cloud review. It does not complete the remaining cloud
storage remediation phases.

## Correctness regressions

The initial focused run executed all three new regressions and failed all three
before the corresponding production changes:

| Finding | Recorded red | Green behavior |
| --- | --- | --- |
| New-write admission rejects readable legacy raw SST entries during compaction | `execute_compaction` returned `ResourceLimit` for a six-byte key and a 64 MiB value | The budgeted compaction writer can preserve legacy entries. Oversized output blocks remain raw even when LZ4 or Zstd is configured, keeping the existing compressed decoder ceiling intact. |
| Range deletion bypasses admission | Rejection occurred only after an existing staged operation had been spilled; `!writes.has_spills()` failed | Both endpoints and the complete encoded singleton range are checked before ordinal changes, reservations, staging, or spill creation. Previously staged work remains valid. |
| A running engine with background compaction disabled never recovers critical L0 pressure | The maintenance call scheduled zero compactions rather than one | Startup, post-flush publication, and periodic maintenance share the pressure-recovery scheduler. Critical L0 pressure can trigger compaction while ordinary background work remains disabled. |

Reproduce the three focused regressions:

```sh
cargo test --lib --all-features should_compact_legacy_oversized_uncompressed_entry_without_losing_readability
cargo test --lib --all-features should_reject_oversized_range_before_staging_or_spilling
cargo test --lib --all-features should_schedule_live_compaction_at_hard_l0_ceiling_when_background_disabled
```

The live-maintenance regression also consumes real worker completions and
publication, then verifies that L0 admission is available in the same runtime.
Additional tests retain the ingest, DDL, publication, and unsettled-authority
gates. A public API regression writes and flushes 32 generations with background
compaction disabled, then shuts down, reopens, and verifies every value. The
existing backpressure test now checks the published L0 ceiling throughout two
ceilings' worth of accepted generations and allows automatic pressure recovery;
its previous expectation of a permanent stall required updating.

The legacy fixture bypasses new admission only while constructing pre-fix SST
bytes in test code. Its production compaction runs cover raw, LZ4, and Zstd
output policies and verify the value, sequence, TTL, and retained input file.
A separate low-budget test proves the compaction exception does not bypass
resource reservations and that failed admission releases the writer's budget.
Ordinary sorted and unsorted writers continue rejecting oversized new entries.
Previously emitted oversized compressed blocks remain corruption errors.

Range boundary coverage includes either oversized endpoint, a combined size
that exceeds the limit, exact encoded boundaries, and arithmetic overflow. The
public API test commits and flushes valid staged work after rejecting a range
deletion that would otherwise cover that key.

## Cleanup findings

| Finding | Resolution and coverage |
| --- | --- |
| Repeated mock forwarding | One test-only forwarding macro replaces 54 forwarding methods across four test modules; fault-injecting overrides remain explicit. Existing cloud recovery and failure-injection suites exercise the wrappers. |
| Repeated provider length tests and incorrect GCS headers | One shared contract tests short, long, malformed, negative, correct, and absent Content-Length. S3 and Azure provide ETag fixtures; GCS also provides its generation header. All three adapter tests execute. |
| Repeated target-block clamping | Sorted and unsorted writers use one clamp helper, retaining the existing boundary regressions. |
| Per-proof identity allocation | Proof validation uses the borrowed conditional-identity selector directly. It no longer constructs conditional-header strings and a vector just to check presence. |
| Missing iterative trie bounds guard | Checked node access restores graceful handling of missing nodes. The new test first reproduced an out-of-bounds panic and then passed; existing deep-trie, cycle, and unreachable-node tests remain in place. |

## Verification

Raw behavioral red/green logs and the repository gate results are retained
locally in `target/pr278-review/`. The first behavioral red is
`red-correctness.log` (three executed, three failed); the separate trie red is
`green-correctness-red-guard.log` (three correctness cases passed, trie case
failed). Compiler errors during test development are not counted as red proof.

The focused green run passed eight tests, including all three provider contract
tests, the three correctness regressions, range boundaries, and the trie guard.
The public SST regression suite passed all four tests. The final workspace log
also explicitly contains all ten focused correctness, guard, boundary, resource,
and public regressions, plus the three shared provider contract cases.

Final gates:

- `cargo fmt --check`: passed.
- `cargo build --workspace`: passed.
- `cargo test --workspace --all-features --no-fail-fast`: **2,674 passed,
  zero failed, 11 ignored**, including doctests.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings -D clippy::pedantic`:
  passed.
- `cntryl-tools validate-tests`: **2,669 compliant, zero violations**.
- `cntryl-tools check-module-sizes --config .cntryl/repository.toml`: passed.
- `cntryl-tools validate-docs --config .cntryl/repository.toml`: passed.
- `cargo test --lib --features sqrzl-tests storage::providers::qualification -- --ignored --test-threads=1`:
  **4/4 passed, zero skips**.
- `cargo test --test cloud_provider_engine_qualification --features sqrzl-tests,failpoints -- --ignored --test-threads=1`:
  **7/7 passed, zero skips**.
- `python3 scripts/validate_pr_acceptance.py /tmp/midge-pr278-review-body.md`:
  passed.
- `git diff --check`: passed.

The workspace's 11 ignored cases are the four provider and seven engine cases
executed explicitly above. Qualification used the repository's pinned image,
`ghcr.io/sqrzl/sqrzl-emulator@sha256:876e017f850e53f3f4172cae459982cfe9435584fcb8e4640120b2e5fabfc624`.

The first full run found only the old permanent-stall expectation. After updating
that test and fixing a Clippy unit-pattern warning in the new API test, the full
workspace and Clippy reruns passed. `workspace-final.log`, `clippy-final.log`,
and `final-summary.json` hold the final evidence; `gates.json` deliberately
retains the initial failures as historical results.

These are correctness and compatibility proofs, not performance measurements.
The prior hotpath benchmark run is separate; this review follow-up makes no
throughput, latency, RSS, or complete memory/disk-boundedness claim. Cloud object
proof mismatch and its stale-mutation consumer regression remain distinguished
from unproven engine-level data loss.
