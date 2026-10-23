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

Each cached SST reader holds one open file descriptor for its lifetime. The
reader cache is bounded in bytes, not in readers: it keeps reader metadata
within a quarter of the block-cache size and evicts idle readers only when that
budget is exhausted, so the number of cached readers, and therefore of open
descriptors, is that budget divided by a reader's metadata reservation (about
1 KiB for a small SST, more for large indexes). Readers evicted while a read or
scan still references them keep their descriptor until it finishes, and
compaction opens its own readers outside the cache. Add the WAL, manifest, and
lease handles, and keep the process `ulimit -n` above that total for the live
SST inventory; a descriptor shortage surfaces as an I/O error on reader open.
