# Retained runtime-panic stderr

These byte-for-byte stderr payloads come from native ARM workflow run
`37986974698` at source `0a20bca8ce3194f337a0a5d6d58e0214d8716541`,
artifact `11643378177`. Both processes exited with status zero and emitted
`complete: true` native receipts, six passing exact checks and two shutdowns.

- `baseline-r3-plain.stderr.txt`: unsampled native process.
- `timers_off-r1-cpu.stderr.txt`: sampled native process; sampling disabled before
  the retained worker-thread panic and native perf data was still produced.

The campaign regression uses these unmodified payloads with successful process
and correctness receipts. It retains all sixteen outcomes and must reject the
five panic-bearing trial names from that original campaign.
