# Performance tuning

Midge has no universal latency or throughput promise. Results depend on data,
hardware, workload, storage mode, cache warmth, compaction, and durability
policy. Measure the workload that matters and keep the command, features,
dataset, and variance with every result.

Start with `Goal::{Latency, Throughput, Economy}`, `MemoryBudget`, and
`WorkloadProfile`. Public overrides include memtable limits, transaction pool
size, block-cache policy, cloud write policy, storage I/O timeout, and
background compaction. Record the resolved options.

`sync`, `buffered`, and `best_effort` measure different durability contracts;
do not compare them as equivalent writes. A write stall or resource limit is a
signal to apply caller backpressure and inspect flush/compaction progress.

Use the registered Criterion benches for comparisons, then confirm important
claims with end-to-end tests and restart/recovery checks. The benchmark
[contract](../development/benchmarks.md) describes the repository's evidence
requirements. Avoid turning one machine's timing into a product guarantee.
