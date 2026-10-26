# Issue #922 miss-only Bloom probe evidence

The `point_miss_probe` Tier 2 diagnostic ran on exact source
`da44cda50f568215d3cd05378ed6d71146ea251d` in GitHub Actions run
[40333630452](https://github.com/cntryl/midge/actions/runs/40333630452).
The fixture has three overlapping L0 SSTs with 256 keys each and runs 8,192
point misses. The raw `cntryl-stress.v2` artifact is
[`tier2-read-amplification-da44cda5.json`](tier2-read-amplification-da44cda5.json).

| Observation | Value |
| --- | ---: |
| Point misses | 8,192 |
| Bloom checks per miss | 2.98828125 |
| Bloom rejects per miss | 2.9765625 |
| Candidate SST checks per miss | 2.98828125 |
| Candidate data-block checks per miss | 2.98828125 |

Every benchmark query was a verified miss. The source calls the block Bloom
filter once per index-selected candidate data block, so this workload observes
about one Bloom probe per table across three candidate tables. It does not show
multiple block probes within one table or a measured index-memory bottleneck.
The result therefore does not justify scheduling the table-level filter and
partitioned-index implementation; issue #922 remains deferred pending a
large-table probe or index-memory evidence that demonstrates a bottleneck.

This was a smoke profile with one measured sample and zero warmups. The harness
classified its latency as `untrustworthy` and trust as `invalid`; do not use it
for latency or performance claims. The fixed work counters are exact for the
8,192 completed queries and the benchmark's correctness assertions passed.
The runner reported a 2.00 load average on four cores. This is diagnostic gate
evidence only, not an A/B result or feature qualification.

Jev selected keeping the feature deferred (confidence 0.85). Source inspection
confirms the observed one-probe-per-candidate-block behavior; Jev output is
advisory and does not prove performance impact.
