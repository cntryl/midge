# Cloud snapshot authority

Cloud snapshots remain usable only while their engine holds its lease. Pins are
process-local: a successor may compact and reclaim SST objects needed by an old
holder. After lease loss, existing cloud point reads and scan creation return
`Fenced`; an active lazy iterator reports a sticky `Fenced` error on its next
advance. New cloud transactions are rejected. Diagnostics and shutdown remain
available. Local and in-memory snapshot reads retain their existing behavior.

The direct read path shares the heartbeat flag, captured writer epoch and
monotonic lease validity with the runtime. It checks before and after a point
read, scan construction and each active iterator advance. This includes writes
buffered by the transaction, misses, empty active scans and storage errors. If
lease authority disappears during I/O, `Fenced` takes precedence over its result.
An iterator already exhausted or failed keeps its terminal state.

## Regression scope

- `src/engine/tests/cloud_read_fencing.rs` expires actual monotonic validity with
  the heartbeat stopped and still healthy. It checks cached, own-write, missing,
  forward/reverse and empty reads, sticky failure and new transaction rejection.
  The same local snapshot is a positive readable control.
- `src/runtime/handle/read_authority_tests.rs` checks heartbeat-only rejection
  and lease loss inside a synchronous read callback, including a row, missing
  SST error and end-of-range. This is a guard test, not native I/O scheduling.
- `tests/cloud_provider_engine_qualification/authority_publication/read_fencing.rs`
  uses native signed S3 against pinned Sqrzl with independent local caches. It
  seeds strict acknowledged rows, confirms original SST PUT/GET success, freezes
  point/scan state, denies renewal transport, waits for real lease loss and lets
  a higher-epoch successor overwrite, flush and compact. Every original remote
  SST must have a successful DELETE observation. Predecessor reads report
  `Fenced`; successor ordered rows and point values are exact before and after
  same-process reopen. It does not claim coordinated reader retention,
  exhaustive failover schedules or independent-process recovery.

Run the focused native regression with a running configured Sqrzl:

```sh
SQRZL_SECRET_ACCESS_KEY=easy-peasy cargo test \
  --test cloud_provider_engine_qualification --features sqrzl-tests,failpoints \
  read_fencing:: -- --ignored --test-threads=1
```

The emulator pin includes the coherent object-read fix for
[sqrzl/sqrzl-emulator#12](https://github.com/sqrzl/sqrzl-emulator/issues/12) and
its bounded/conditional read follow-ups. Midge's lease parser still rejects
malformed or ambiguous documents conservatively. Neither read fencing nor an
emulator update proves cancellation of an already submitted provider mutation.
