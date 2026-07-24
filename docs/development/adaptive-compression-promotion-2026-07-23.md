# Adaptive compression promotion record

The selective Adaptive cascade was not promoted. Production retains exhaustive
smallest-output selection. The independent correctness fixes remain: both
built-in Adaptive policies use the inclusive `compressed/original <= 0.95`
eligibility threshold, and raw input is copied only when raw fallback is
actually emitted.

## Candidate and method

- Baseline source commit: `87ea77b`.
- Candidate: try codecs in declared order, skip `None`, continue after errors
  and unsupported codecs, retain the smallest qualifying output, and return
  early when a qualifying output also meets
  `min(custom_min_ratio, fast_accept_ratio)`.
- Oracle: exhaustive smallest-output selection with the same inclusive
  eligibility rules.
- Corpora: 16 deterministic blocks for each combination of 16 KiB and 64 KiB
  with repeated, structured, mixed, prefix-plus-random-tail, uniform, and
  adversarial low-cardinality data.
- Host: Apple M5, macOS aarch64, Rust 1.97.0, optimized benchmark build.

## Threshold screen

Thresholds from `0.10` through `0.80` were screened mechanically. Every row
used 20 measured samples and recorded throughput, logical and physical bytes,
codec selections, attempts, and per-workload oracle regret.

| Fast accept | Median throughput | Attempts | Added logical bytes | Every storage row <= 0.5% |
|---:|---:|---:|---:|:---:|
| 0.10 | 720.14 MB/s | 320 | 0.127% | yes |
| 0.20 | 723.24 MB/s | 320 | 0.127% | yes |
| 0.30 | 739.22 MB/s | 288 | 0.188% | yes |
| 0.40 | 739.34 MB/s | 288 | 0.188% | yes |
| 0.50 | 735.05 MB/s | 288 | 0.188% | yes |
| 0.60 | 982.81 MB/s | 272 | 3.452% | no |
| 0.70 | 1,086.45 MB/s | 256 | 4.650% | no |
| 0.80 | 1,339.88 MB/s | 244 | 6.260% | no |

`0.30` was selected for promotion testing. It and `0.40` were within 2%, so
the lower ratio won the required tie-break.

The selected threshold's deterministic block storage results were:

| Corpus | Logical bytes | Candidate / oracle bytes | Regret / logical |
|---|---:|---:|---:|
| repeated 16 KiB | 262,144 | 1,360 / 384 | 0.372% |
| structured 16 KiB | 262,144 | 2,238 / 1,278 | 0.366% |
| mixed 16 KiB | 262,144 | 162,871 / 162,871 | 0.000% |
| prefix + random tail 16 KiB | 262,144 | 67,694 / 66,830 | 0.330% |
| uniform 16 KiB | 262,144 | 262,224 / 262,224 | 0.000% |
| low cardinality 16 KiB | 262,144 | 88,505 / 88,505 | 0.000% |
| repeated 64 KiB | 1,048,576 | 4,432 / 400 | 0.385% |
| structured 64 KiB | 1,048,576 | 5,326 / 1,294 | 0.385% |
| mixed 64 KiB | 1,048,576 | 543,595 / 543,595 | 0.000% |
| prefix + random tail 64 KiB | 1,048,576 | 267,390 / 263,454 | 0.375% |
| uniform 64 KiB | 1,048,576 | 1,048,656 / 1,048,656 | 0.000% |
| low cardinality 64 KiB | 1,048,576 | 342,992 / 342,992 | 0.000% |

## Tier 2 paired captures

Five alternating candidate/oracle captures used 20 measured samples per row.
The capture-level geometric-mean speedups across all 12 corpora were 2.901x,
1.830x, 1.808x, 1.791x, and 1.796x. The geometric mean across all 60 paired
rows was 1.986x, above the required 1.25x.

The first oracle capture contained cold-start variance. The other four captures
still cleared the throughput requirement independently.

## Complete-system gate

The fixed-work local Throughput benchmark ran in two detached `87ea77b`
worktrees. The baseline worktree contained only the identical benchmark
scaffold; the candidate worktree contained the selective selector. Each
workload wrote deterministic 16 KiB records across four explicit flushes,
completed a full compaction, shut down cleanly, and recorded ingest,
flush/compaction, total time, and final SST bytes. Five captures alternated
baseline-first and candidate-first ordering.

| Workload | Median total speedup | Worst capture | Baseline / candidate SST bytes | Regret / logical |
|---|---:|---:|---:|---:|
| repeated | 0.996x | 0.993x | 19,958 / 54,262 | 0.409% |
| structured | 0.990x | 0.986x | 35,681 / 71,996 | 0.433% |
| mixed | 0.986x | 0.950x | 5,048,173 / 5,048,173 | 0.000% |
| prefix + random tail | 0.996x | 0.994x | 2,138,393 / 2,171,670 | 0.397% |
| low cardinality | 1.019x | 0.989x | 2,800,509 / 2,801,173 | 0.008% |

The capture-level geometric means were 0.983x, 1.001x, 1.007x, 1.002x, and
1.004x. Across all 25 total-time pairs the geometric mean was 0.999x, far
below the required 1.10x. One mixed-data capture was 5.02% slower, also just
outside the per-workload limit.

The candidate passed complete-SST storage: every workload remained below 0.5%
of logical input.

## Fixed-codec and decompression gate

One isolated 20-sample fixed-row capture found all decompression rows within
2.5% and all Zstd compression rows within 4.0%. LZ4 compression was 8.8% slower
at 16 KiB and 7.3% slower at 64 KiB in that capture, outside the 5% limit.
Those functions were unchanged, so this is likely host/order noise, but the
promotion rule is deliberately conservative and the result was not waived.

## Compatibility

The current implementation strictly verified and reopened the generated
`87ea77b` compressed format-2/SST-V3 fixture, including point reads, a full
scan, and the frozen data digest. It also reopened mixed legacy/current SSTs,
reopened a completed compaction, and produced byte-identical SST files from
identical ordered input.

For the reverse direction, an isolated `87ea77b` `midge verify --json` binary
reported a current-written database `Healthy` and authoritative. It verified
one SST, 26 checksummed data blocks, and 1,607,950 bytes without a format or
codec error.

## Outcome

The candidate passed block storage and Tier 2 aggregate throughput, but failed
the complete-system throughput gate and one fixed-codec capture. Production
therefore remains exhaustive. The threshold screen, candidate/oracle rows,
complete-system probe, frozen fixtures, compatibility tests, and this record
remain as diagnostic evidence for future work.
