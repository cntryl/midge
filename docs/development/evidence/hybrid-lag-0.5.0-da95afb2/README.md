# Tier 2 simulated hybrid-lag baseline evidence

Source: `da95afb262346f0f1c9d985a781b1d5bfa81b4a9` (2026-10-26).
Each successful row used
`cargo bench --locked --bench tier2_subsystem_hybrid_durability_lag -- --workload <row> --no-progress --require-quiet-env`.
The simulator is in-memory and is not live provider evidence. The host was an
Apple M5, 10-core macOS arm64 machine on AC power. Each artifact uses five
measured samples; the harness observes power source only.

| Row | Selected runs | Median throughput | Run spread | Other measurements |
| --- | --- | ---: | ---: | --- |
| `cloud_async`, one client | 1, 2, 3 | 2,396.76 transactions/s | 3.30% | p99 lag 192 sequences; p99 probe wait 23.37–24.63 ms; 1.3125 requests and 0.05176 puts per commit; 1.8777–1.8778 uploaded bytes per committed byte; max sampler gap 16 ms |
| local `sync()` floor, one client | 1, 2, 4 | 111.68 transactions/s | 13.45% | Selected runs acceptable individually; run 3 noisy and excluded |
| `cloud_strict`, one client | none | Inconclusive | — | Two isolated attempts failed the sampler-health guard at 68 ms and 64 ms (40 ms maximum) |

The full three-row suite also failed twice when a cloud row exceeded the same
40 ms sampler-gap guard (52 ms and 50 ms). The per-row attempts isolated a
clean `cloud_async` row and a repeatedly inconclusive `cloud_strict` row. No
threshold was changed and no result was emitted for failed attempts. The local
floor's selected run medians vary by 13.45% despite acceptable within-run
quality; treat that spread as visible host variation, not an A/B conclusion.

Raw JSON SHA-256:

| Row/run | Quality | SHA-256 |
| --- | --- | --- |
| async 1 | acceptable | `ef4dc561b78c6c732d873f3d5faa75226d2eec7358d83b50d7922c9e1c677e85` |
| async 2 | acceptable | `42268b452d29400199ed70aec74de499971ca503859ba7f141f0f844777b8a3a` |
| async 3 | acceptable | `3f7b03976f7bd4253acd6cd1fb969a9ccb1623d753067a75b4ffd123049378c2` |
| local sync 1 | acceptable | `40910fc2198a26dcf59dce6ff4946e7821a40aaacf6cbdcf7c6e0f167e84ba31` |
| local sync 2 | acceptable | `dec982797261c33bf061227c296711c1e70f2abb68c789b722fb2296e046c6d5` |
| local sync 3 | noisy; excluded | `4984723df7b9c3ddaf49a9a1c9104e180fe4bc2b6a2327878a55eb257a63f51f` |
| local sync 4 | acceptable | `001a4f01bcf6276ecf1233998d2fd85e60c8d05114e914902554a5c900811157` |

All JSON artifacts record the same exact source revision, gate trust,
correctness outcome, sampler observations and benchmark parameters. Uptime
snapshots are retained for each isolated attempt and successful run. Failure
logs record both complete-suite failures and both isolated cloud-strict
failures; those attempts emitted no JSON artifact.
