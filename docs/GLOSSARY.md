## 📘 Glossary of Terms & Acronyms

### Core Architecture

| Acronym        | Full Form                     | Description                                                                                                           |
| -------------- | ----------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| **LSM**        | _Log-Structured Merge_ (Tree) | A write-optimized data structure that buffers writes in memory and periodically merges them to on-disk sorted tables. |
| **WAL**        | _Write-Ahead Log_             | An append-only log ensuring durability — changes are first written here before being applied to in-memory structures. |
| **SST**        | _Sorted String Table_         | Immutable, sorted on-disk files produced by compaction containing key-value pairs and index metadata.                 |
| **MemTable**   | _Memory Table_                | In-memory skiplist or balanced tree that holds recent writes before being flushed to disk as SSTs.                    |
| **Manifest**   | —                             | A metadata file tracking all SSTs and levels in the database, ensuring crash recovery and compaction consistency.     |
| **Compaction** | —                             | The background process of merging SSTs to reduce overlap, reclaim space, and improve read performance.                |

### Concurrency & Versioning

| Acronym      | Full Form                           | Description                                                                                                                 |
| ------------ | ----------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| **MVCC**     | _Multi-Version Concurrency Control_ | A mechanism that maintains multiple versions of data to allow readers and writers to operate concurrently without blocking. |
| **SeqNo**    | _Sequence Number_                   | A monotonically increasing identifier for ordering operations and supporting snapshot isolation.                            |
| **Txn**      | _Transaction_                       | A set of operations applied atomically to maintain database consistency.                                                    |
| **Snapshot** | —                                   | A read-only, point-in-time view of the database, implemented via sequence numbers.                                          |

### Indexing & Filtering

| Acronym          | Full Form | Description                                                                         |
| ---------------- | --------- | ----------------------------------------------------------------------------------- |
| **Bloom Filter** | —         | A probabilistic structure for quick existence checks; reduces unnecessary disk I/O. |
| **Sparse Index** | —         | Stores sampled key offsets to enable efficient SST block lookups.                   |
| **Block Index**  | —         | Metadata at the end of an SST mapping key ranges to physical file offsets.          |

### File Format & I/O

| Acronym   | Full Form                 | Description                                                                               |
| --------- | ------------------------- | ----------------------------------------------------------------------------------------- |
| **TLV**   | _Type-Length-Value_       | A binary encoding format describing structured data with minimal overhead.                |
| **CRC**   | _Cyclic Redundancy Check_ | A checksum used to verify data integrity of WAL and SST blocks.                           |
| **MMap**  | _Memory-Mapped I/O_       | A file access method that maps disk blocks directly into process memory for faster reads. |
| **I/O**   | _Input / Output_          | Read/write operations to persistent or memory storage.                                    |
| **FSync** | _File Synchronize_        | System call ensuring that OS page cache data is physically persisted to disk.             |

### Caching & Storage Layers

| Acronym         | Full Form             | Description                                                                      |
| --------------- | --------------------- | -------------------------------------------------------------------------------- |
| **LRU**         | _Least Recently Used_ | A cache eviction policy that discards the least recently accessed entries first. |
| **Block Cache** | —                     | Caches SST data blocks to reduce disk reads.                                     |
| **Page Cache**  | —                     | Kernel-level cache that stores disk pages in memory.                             |

### Cloud & Durability

| Acronym        | Full Form                      | Description                                                           |
| -------------- | ------------------------------ | --------------------------------------------------------------------- |
| **S3**         | _Simple Storage Service_ (AWS) | Object storage backend for durable blob storage.                      |
| **GCS**        | _Google Cloud Storage_         | GCP’s S3-equivalent blob storage service.                             |
| **Azure Blob** | —                              | Microsoft Azure’s object storage backend.                             |
| **CID**        | _Content Identifier_           | A hash-based identifier used in immutable storage or object catalogs. |

### Compression & Encoding

| Acronym           | Full Form                             | Description                                                              |
| ----------------- | ------------------------------------- | ------------------------------------------------------------------------ |
| **Snappy**        | —                                     | A fast compression library used for SST blocks.                          |
| **LZ4**           | —                                     | High-speed compression algorithm balancing compression ratio and speed.  |
| **ZSTD**          | _Zstandard_                           | Modern compression format offering tunable compression levels and speed. |
| **CRC32 / CRC64** | _Cyclic Redundancy Check (32/64-bit)_ | Checksums for integrity verification of stored data.                     |

### Internal Utilities & Testing

| Acronym   | Full Form                        | Description                                                                |
| --------- | -------------------------------- | -------------------------------------------------------------------------- |
| **Fuzz**  | _Fuzz Testing_                   | A testing technique that feeds randomized input to uncover edge-case bugs. |
| **Bench** | _Benchmark_                      | A performance test suite measuring throughput, latency, and scaling.       |
| **YCSB**  | _Yahoo! Cloud Serving Benchmark_ | Standard benchmark for evaluating key-value store performance.             |
| **CI**    | _Continuous Integration_         | Automated build and test pipelines.                                        |
| **CLI**   | _Command-Line Interface_         | Tooling for managing database operations and running benchmarks.           |

### Optional Advanced Concepts

| Acronym    | Full Form              | Description                                                              |
| ---------- | ---------------------- | ------------------------------------------------------------------------ |
| **MVBT**   | _Multi-Version B-Tree_ | B-Tree variant supporting versioned entries (used conceptually in MVCC). |
| **RUM**    | _Read-Update-Merge_    | Pattern describing LSM’s core lifecycle of reads, updates, and merges.   |
| **TxnLog** | _Transaction Log_      | Sequence of operations capturing transactional state.                    |
| **GC**     | _Garbage Collection_   | The process of reclaiming obsolete data or tombstones.                   |
| **TTL**    | _Time To Live_         | Expiration mechanism that automatically deletes data after a duration.   |
