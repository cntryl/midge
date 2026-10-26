# Issue #949 compaction CPU comparison

The registered `tier2_subsystem_compaction` benchmark was run in release mode
for three alternating base/candidate pairs. Each invocation used five measured
samples and one warmup. The exact same benchmark source was copied into both
detached worktrees; its SHA-256 during measurement was
`b69d258de01684d3641621519a46ab01a68f18e1586b24777c4643826e6d57e6` in all
three locations. After the six runs, strict Clippy identified lossy-cast and
raw-pointer style lints. The tracked benchmark now uses explicit split-half
integer-to-float conversion and raw-pointer syntax; its SHA-256 is
`887ca85476eb1c32fb3043eb170dd52b834941c2b27599ac1a8800e14a5b8439`. These
changes preserve the measured values exactly for this fixture's input and
duration ranges; the measured source remains byte-identical across base and
candidate.

## Source and machine

- Base: `8883610fae287e0473064049e47f5477e7354db2`
- Candidate: `ef67da29ad7e4caba7bb0de384dd0b0b0b2fb361`
- Machine: Apple M5, 10 cores, 24 GiB memory, macOS aarch64, APFS on SSD
- Rust: `rustc 1.99.0 (b940084d7 2026-09-28)`
- Power: AC
- Workload: eight independently flushed SSTs; each has 100,000 unique 16-byte
  keys and 128-byte deterministic high-entropy values; one production
  `compact_all` measured over 115,200,000 logical input bytes
- CPU measurement: process user plus system CPU time around `compact_all`,
  which includes the compaction worker threads. Wall time and output bytes are
  reported separately in each raw artifact.
- Invocation: `cargo bench --locked --bench tier2_subsystem_compaction -- --no-progress`
- Cargo used separate target directories for the two worktrees. Every raw
  artifact's `environment.git_commit` matches its intended source SHA.
- `uptime.txt` records machine load around each run; load averages ranged from
  1.36 to 2.96 on this 10-core machine.

## Results

The primary metric is the median of five samples for process CPU nanoseconds
per logical input MiB. Lower is better. All six artifacts have acceptable
quality and the metric RSD is below the 10% Tier 2 limit.

| Pair | Base p50 ns/MiB | Candidate p50 ns/MiB | Candidate reduction | Base RSD | Candidate RSD |
| --- | ---: | ---: | ---: | ---: | ---: |
| A1/B1 | 7,814,112 | 6,715,701 | 14.06% | 0.63% | 0.31% |
| A2/B2 | 7,626,961 | 6,757,763 | 11.40% | 0.72% | 0.24% |
| A3/B3 | 7,602,103 | 6,711,633 | 11.71% | 0.43% | 0.76% |
| Median | — | — | **11.71%** | — | — |

All runs rewrote 110,788,948 output bytes. Candidate wall-time p50 was also
lower in each pair; this is a secondary observation, not the primary gate.
This result qualifies #949 AC2 for the preregistered workload and source
comparison. It does not isolate the CPU contribution of the heap change from
the finalized writer identity change; the comparison covers those two #949
changes together.
