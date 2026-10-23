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

Use the registered `cntryl-stress` benches for comparisons, then confirm
important claims with end-to-end tests and restart/recovery checks. The
benchmark [contract](../development/benchmarks.md) describes the repository's
evidence requirements. Avoid turning one machine's timing into a product
guarantee.

## Build profile

The crate's `release` and `bench` profiles enable thin link-time optimization
and a single codegen unit. Downstream applications inherit their own profile,
so set the same in the application's `Cargo.toml` when measuring or deploying:

```toml
[profile.release]
lto = "thin"
codegen-units = 1
```

Keep `panic = "unwind"`. Midge isolates worker panics with `catch_unwind` on
the WAL fsync path, flush workers, compaction workers, and runtime shutdown so
a panic fences the writer instead of aborting the process; `panic = "abort"`
removes that isolation.

A process has exactly one global allocator, and the binary owns it, so the
library installs none. The optional `alloc-mimalloc` feature installs
`mimalloc` in the `midge` command-line binary only. An application that embeds
Midge chooses its own allocator in its binary crate:

```rust
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
```

Measure against the system allocator on the workload that matters before
adopting it; on the Tier 1 and YCSB A rows the result is mixed, not a uniform
win.

## File handles

Each cached SST reader holds one open file descriptor for its lifetime, so the
process's descriptor budget is the reader cache's capacity plus the WAL,
manifest, and lease handles. The reader cache is bounded by a quarter of the
block-cache size in reader metadata and evicts idle readers first; a reader
evicted while a read is in flight keeps its handle until that read finishes.
Size the block cache, and the process `ulimit -n`, so the live SST inventory
fits with headroom.
