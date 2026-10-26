# Issue #948 current-source diagnostic

This is a one-sample diagnostic of the integrated source after commit
`a53da5fc` narrowed the source-fragment fallback. It is not a performance gate:
the stress artifact marks the row `trust_class=invalid` because one sample does
not meet the registered minimum. Host load was elevated during the run.

- Engine source: `c6b00073fd20bdd726a17ce8437fcb8bd6812365`.
- Benchmark source SHA-256: `957a655c8aea573f71ef4ba16d82a05c21d71c0545443674f356484cf61c73d6`.
- Command: `cargo bench --locked --bench tier4_system_compaction_grandparent_cuts -- --profile release --samples 1 --warmup-samples 0 --no-progress --timeout-secs 600 --output-dir /tmp/midge948-current-diagnostic`.
- The fixture contained 384,221,780 bytes across three overlapping L2 files; the largest was 133,994,180 bytes.
- The run rewrote 494,770,046 compaction-output bytes (6.247 bytes per logical input byte), ran four compactions, and ended with five SSTs.
- The measured throughput was 6.83 MB/s, but is invalid and must not be compared with gate-quality results.

The rewritten-byte diagnostic is close to the prior base result (about 489 MB)
and below the earlier candidate result (about 857 MB). This suggests the later
fragment-fallback change may address the output-amplification regression, but a
single diagnostic cannot establish that. The three-pair alternating A/B gate
must be rerun against the exact current source before AC2 is accepted.

Raw output: [`current-c6-single-sample.json`](current-c6-single-sample.json)
and [`current-c6-single-sample.md`](current-c6-single-sample.md).

## Post-fix alternating A/B continuation

The current-source A/B compares base `e9a0d86b7be0c1b298ef8f546326c5893aedba04`
with candidate `c6b00073fd20bdd726a17ce8437fcb8bd6812365`, which includes
`a53da5fc`. Both used the same benchmark SHA above; the older base checkout was
given the benchmark registration stanza and this identical benchmark source.
Before each run I recorded `uptime`; the benchmark environment artifact on this
macOS host did not report a load-average observation.

Accepted pair 1:

| Run | Median throughput (logical B/s) | RSD | Output bytes | Compaction runs | Final SSTs | Pre-run 1-minute load |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A1 base | 13,048,769.74 | 1.85% | 488,951,998 | 4 | 4 | 3.34 |
| B1 current | 13,105,075.84 | 2.51% | 494,770,046 | 4 | 5 | 3.24 |
| Pair delta | **+0.43%** | — | **+1.19%** | — | — | — |

The raw rows passed the harness with gate trust. This is one accepted pair,
not a completed three-pair gate.

An attempted A2 was excluded: it had 20.1% throughput RSD and the 1-minute
load rose to 16.85 by completion. Its raw result is preserved as
[`A2-base-c6-noisy.json`](A2-base-c6-noisy.json) and
[`A2-base-c6-noisy.md`](A2-base-c6-noisy.md). No matching B2 was run under
that load. Repeat A2 after the host settles, then continue with A2/B2 and
A3/B3. Do not infer AC2 from the accepted first pair or the excluded attempt.

Accepted pair artifacts: [`A1-base-c6.json`](A1-base-c6.json),
[`A1-base-c6.md`](A1-base-c6.md),
[`B1-candidate-c6.json`](B1-candidate-c6.json), and
[`B1-candidate-c6.md`](B1-candidate-c6.md).
