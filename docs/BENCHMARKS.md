# Benchmarks

## Cold vs warm cache

- **Cold cache**: Each run starts with caches empty (block cache, OS page cache, and any internal read-ahead). This emphasizes IO and data-structure rebuild costs.
- **Warm cache**: Caches are pre-filled with the same data before timing. This emphasizes CPU work and data-structure traversal rather than IO.
- **Why it matters**: Cold runs highlight worst-case latency and steady-state recovery behavior; warm runs show best-case hot-path performance. Compare both when evaluating regressions.

## Bloom build cost

- Bloom filter construction is proportional to the number of keys and requires hashing each key.
- The cost is dominated by: key iteration, hashing work, and writing the bitset.
- Large blooms (e.g., 1M keys) can dominate total subsystem time. Treat these as build-time costs that amortize over many reads.

## Cloud vs local durability

- **Local durability**: WAL/fsync costs are mostly device latency; once fsync completes, data is stable on local storage.
- **Cloud durability**: Costs include network latency, request batching, and provider consistency guarantees. Latency variance is higher and tail latency is more important.
- Benchmarks that target durability should separate these modes because they reflect different sources of latency and failure recovery behavior.
