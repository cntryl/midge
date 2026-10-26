# Issue #947 public Strict commit allocation comparison

## Source and method

- Reference: `90eb0b5c684267f45605e1f2d42580b4da45cbf9`
- Candidate: `c1e6e9423fdd55f7f2766c3d83220c8eadc96f7e`
- The same public-API test source was copied into both detached worktrees. SHA-256: `62f1269b4e8922f4e7390ea596aa27e90737ffbfdfcf87d0dee18ed1888c7e87`.
- Machine: Apple M5, 10 cores, macOS aarch64, Rust `1.99.0 (b940084d7 2026-09-28)`. Across the six runs, one-minute load ranged from 1.81 to 2.77, five-minute load from 3.17 to 3.38, and fifteen-minute load from 2.92 to 2.99.
- Each invocation warms a local engine, verifies that every 4 KiB fixture exceeds the WAL compression probe's 128-distinct-byte threshold, then counts process allocations while committing 16 prebuilt values via public `Engine` transactions and `WriteOptions::sync()`. It reads every committed value back after measurement.
- Invocation: `CARGO_TARGET_DIR=<separate-target> cargo test --release --locked --test wal_strict_commit_allocations -- --nocapture`
- Runs alternated reference/candidate. The counter covers allocations across transaction creation, put admission, WAL encoding, writer completion and strict acknowledgement, including any concurrent allocations from that engine process. It is not an isolated encoder count. `uptime.txt` records load immediately before and after each invocation.

## Results

| Pair | Reference allocations / 16 commits | Candidate allocations / 16 commits | Fewer allocations | Reduction per commit |
| --- | ---: | ---: | ---: | ---: |
| A1/B1 | 2,318 | 2,298 | 20 | 1.250 |
| A2/B2 | 2,318 | 2,299 | 19 | 1.188 |
| A3/B3 | 2,320 | 2,301 | 19 | 1.188 |
| Median | 2,318 | 2,299 | **19** | **1.188** |

All six runs passed and all committed values read back byte-for-byte. The median observed reduction is 1.188 allocation events per commit, exceeding the one-allocation target. The current isolated encoder test independently observes zero allocations for the borrowed raw-value path versus one allocation per value in the legacy construction.

The copy reduction is established from the exact source path rather than a runtime byte-copy counter: the reference calls `Bytes::copy_from_slice` to create an owned 4 KiB raw value, then copies that value into the final frame; the candidate borrows the input through the TLV append and copies it only into the final frame. Thus the code removes one 4,096-byte intermediate copy for each raw 4 KiB value. The independent legacy encoder test confirms exact frame bytes, CRC and replay compatibility. A real `FsWalWriterIo` regression additionally appends the raw 4 KiB record, obtains seal facts, and matches the writer identity digest against an independent digest over the persisted header and frame bytes.

## Correctness checks

The following focused checks passed on the integration head: borrowed-vs-legacy frame/CRC encoding; WAL put replay; real partial append with durable rollback; failed append without rollback proof; lease authority loss during sync; deadline expiry during append and rollback; writer frame construction; and append position accounting. The `wal_frame_recovery` libFuzzer target also completed a bounded 41-run smoke from 20 corpus inputs with no crash. A separate default 1,000-run attempt was interrupted at 64 iterations as sanitizer RSS rose to 339 MiB; that interrupted attempt is not qualification evidence.

No throughput gain is claimed by this allocation comparison.
