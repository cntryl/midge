# Benchmark L0 maintenance regression evidence

Verified on 2026-09-04, starting from `68821ee84a2c5d26ad32123905a7d4b84751f571`.
This is execution qualification on a noisy host, not a performance comparison.

## Failure and fix

Three benchmark fixtures disabled background compaction while producing enough
SST files to exhaust bounded L0 admission. Waiting for a write stall could not
reclaim those slots. No engine limits, durability rules, or stall deadlines were
changed.

The rotation and complete-system strict group-commit workloads now enable
background compaction and report that setting in their benchmark parameters.
The compression-policy workload retains separate ingest and maintenance timing:
it explicitly compacts every four flushes, identically for every policy, and
charges that work to `flush_compaction_ns`. All sixteen flushes and 8,192 records
remain. The final compaction and shutdown remain in place.

These maintenance changes affect the measured workload. Previous timing results
are not directly comparable; establish new baselines before judging performance.

## Red

Before editing, reran one failing workload from each suite using the previously
built executables, with `--profile smoke --workload '*::<function>' --json`:

| Function | Exit | Failure |
| --- | ---: | --- |
| `tier2_durability_commit_sync_local_4k_rotation` | 101 | rotation benchmark write stall did not clear |
| `latency_policy_mixed` | 101 | no free L0 slot (14/14) |
| `tier4_complete_local_strict_group_commit` | 101 | strict system write stall did not clear |

The preceding complete benchmark campaign independently reproduced all five
latency-policy shape failures, plus the rotation and strict-system failures.

## Green

```sh
cargo bench --features failpoints \
  --bench tier2_subsystem_durability_commit_latency \
  --bench tier4_system_compression_policy \
  --bench tier4_system_strict_group_commit \
  --no-run --message-format=json
```

Ran each of the three resulting executables with `--profile smoke --json` and
no workload filter. Smoke disables timing-quality rejection, but retains the
workloads and their correctness assertions.

| Suite | Workloads completed | Exit |
| --- | ---: | ---: |
| Durability commit latency | 4/4 | 0 |
| Compression policy | 15/15 | 0 |
| Complete-system strict group commit | 1/1 | 0 |

Checked report workload identities against the registered inventories. All 20
workloads emitted measured samples with nonzero completed operations and zero
failure, timeout, validation-error, duplicate, and dropped counters. The strict
system workload also passed point-read and scan digest verification after reopen.

Additional checks passed: `cargo fmt --check`, `cargo build --workspace`,
`cargo clippy --workspace --all-targets --all-features -- -D warnings -D clippy::pedantic`,
and `cntryl-tools validate-tests`. Changes are confined to benchmark fixtures;
the full workspace test suite was not rerun for this fix.

Local raw logs, exact commands, exit codes, and the report coverage audit are in
`target/bench-execution/l0-fix/` (`red.json`, `green.json`, `coverage.json`, and
per-suite logs). The original 40-target campaign is preserved separately in
`target/bench-execution/20260904T194039Z/`; its 37 passing suites were not rerun.
