# SST Index Design (Midge LSM KV)

## Purpose

This document defines the indexing strategy for Midge SSTs, optimized for predictable lookup latency, efficient range scans, and minimal I/O amplification across LSM workloads.

## Design Goals

* **Fast point lookups** with a single data-block read on the happy path
* **Efficient range scans** via tight block skipping
* **Compact memory footprint** for index + bloom metadata
* **Simple, robust implementation** that supports crash safety and future extensions

---

## Index Structure

### 1. Single-Level Block Index

* Each SST contains a **flat index**: one entry per data block.
* An index entry stores:

  * `min_key`
  * `max_key`
  * `offset` of the block in the file
  * `length` (optional; footer can store compressed block sizes)
* The index resides in a **trailing metadata block**; the footer points to it.
* Lookups use **binary search** on `min_key` to find a candidate block, followed by a bounds check on `max_key`.

This structure is:

* Cache-friendly
* Small (hundreds to a few KB)
* Sufficient for files up to ~10–100MB (Midge's sweet spot)

### 2. Fence Pointers (Min/Max Keys)

All blocks carry:

* `min_key` (first key encoded in block)
* `max_key` (last key)

Benefits:

* Range scans skip blocks aggressively.
* Iterators can quickly decide when to advance across SST boundaries.
* Compaction can cheaply reason about overlap.

### 3. Per-SST Bloom Filter

* Stored as a dedicated metadata block preceding the index.
* Built from **full set of keys** in the SST.
* Your bloom design (blocked layout, single hash, double-hashing) provides excellent hot-path characteristics.
* False positives are capped and stable even with many SSTs.

Usage:

1. Query bloom
2. If "not present," skip entire SST (no index, no I/O)

**Optional future enhancement:** per-block blooms (slower SST creation, but reduces false positives for pathological workloads)

### 4. Zone Maps (Optional/Future)

If analytical / range-heavy use cases strengthen:

* Store per-block min/max statistics for numeric fields (lex-ordered keys still benefit but are limited).
* Can dramatically reduce read-amp for time-series layouts.

This is off by default, minimal impact on today's write path.

### 5. Sparse Key Sampling (Index Thinning)

The block index stores sampled `min_key`s, short prefix-compressed.

* Allows binary search on a small in-memory array.
* After locating the correct block, a tiny linear probe inside ~32–128 entries.

This keeps index blocks tiny while preserving negligible lookup cost.

---

## Consistency & Recovery

* Index and bloom blocks are **checksummed** (CRC32C in Midge).
* Footer stores:

  * offsets
  * block types
  * checksums
* SST creation writes:

  1. Data blocks
  2. Bloom filter block
  3. Index block
  4. Footer (atomic append)

On recovery:

* Seek to footer, validate checksums, then map index + bloom into memory.
* No multi-phase commit required.

---

## Example SST Layout

```
[SST File]
|-- Header
|-- DataBlock[0]
|-- DataBlock[1]
|-- ...
|-- BloomFilterBlock
|-- BlockIndexBlock
|-- Footer (offsets + checksums)
```

---

## Future Directions

* **Per-block bloom filters** (tight read-path for high SST counts)
* **Zone Maps** (for analytic and wide range scans)
* **Range Tombstone Indexing**:

  * Store tombstones in separate blocks
  * Fence-pointer–based acceleration
* **Compressed in-memory block index** via prefix-elision and varint encoding

---

## Out of Scope

* Secondary indexes
* Pluggable index backends
* Multi-level indexes (unnecessary for SST sizes Midge targets)

---

This design is aligned with convergence in modern LSM engines (Pebble, RocksDB, TiKV) and tailored for Midge's core KV use case. For implementation details, see `src/sst/` and related modules.
