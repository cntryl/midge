awesome—let’s turn “build Midge from scratch” into a crisp, end-to-end implementation plan you can execute. i’ve grouped it into four buckets you asked for, plus deliverables, milestones, and acceptance criteria so you can track progress.

# Basic LSM Requirements (MVP)

## Core data path

- **Write-Ahead Log (WAL)**

  - Append-only, preallocated segments (e.g., 64–256 MB).
  - TLV record format with per-record CRC32C.
  - Rotation on size and time; fsync on configurable policies (`always`, `every_n_records`, `every_ms`, `on_close`).
  - Recovery: scan last good record boundary; tolerate torn/truncated tail.

- **Memtable**

  - Concurrent skiplist (lock-light) or B-tree; single writable + N immutable.
  - Sequence numbers per mutation; monotonic `u64`.
  - Backpressure when memtable bytes > `memtable_limit_bytes`.

- **SSTables**

  - Immutable files with:

    - Sorted data blocks (e.g., 4–32 KB) with prefix/suffix varint encoding.
    - Per-block checksums; optional compression (Snappy-like first).
    - Index block, filter block (Bloom), footer with magic/version.

  - Sparse index for fast lookups + block cache (LRU + admission).
  - Table format finalized as `SST v1` with TLV headers for forward compat.

- **Compaction**

  - Levelled (L0–L6) to start:

    - Ingest immutables → flush to L0.
    - Size-tiered in L0, leveled below.

  - Compaction picker: score by size, overlaps, read-amp budget.
  - Throttling: limit concurrent compactions; write stall when L0 count high.

- **Point lookups & iteration**

  - Read path: memtable → immutables → L0..Ln (merge iterators).
  - Iterators with forward/backward, bound-aware, prefix-seek.

- **Crash safety**

  - Ordering: append WAL → fsync (policy) → apply to memtable → flush → install manifest edit.
  - Recovery: replay WAL newer than last persisted snapshot; rebuild version state.

## Process & packaging

- **Locking**

  - `LOCK` file TLV (as we discussed): session UUID, pid, host, acquired/renewed at, ttl.
  - Renewal thread; on renewal failure → downgrade to **read-only**.

- **Manifest**

  - Append-only manifest (edits TLV): add/remove table, level changes.
  - Periodic snapshotting to reduce replay time.

- **Config**

  - `cntryl_midge::Options` with sensible defaults, TOML/ENV overrides.

- **Public API (Rust, sync)**

  ```rust
  pub trait Kv {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    fn delete(&self, key: &[u8]) -> Result<()>;
    fn scan(&self, range: Range<&[u8]>) -> Result<Box<dyn Iterator<Item=(Vec<u8>, Vec<u8>)>>>;
    fn flush(&self) -> Result<()>;
  }
  ```

- **Observability**

  - `slog` w/ context; counters/histograms via `metrics` crate (Prometheus).
  - Stats endpoints: levels, compaction queue, memtable bytes, block cache.

# Advanced LSM Requirements (v1+)

## Read/write features

- **Range tombstones** (inclusive/exclusive bounds).
- **Merge operators** (e.g., counters, sets) with idempotent semantics.
- **TTL / expiration** (per-key and default).
- **Snapshots & consistent iterators**

  - Read sequence pinning (read-your-snapshot).
  - File-level “checkpoint” export (hardlinks/manifest snapshot).

- **Transactions (optional, later)**

  - Optimistic, snapshot-based; write set validated at commit.

- **Compaction enhancements**

  - Subcompactions (parallelize by key ranges).
  - Compaction filters (TTL drop, custom prune).
  - Compaction guard rails: IO budget, write amplification targets.

- **Filters**

  - Partitioned Bloom filters (better cache locality).
  - Optional prefix Bloom for common prefixes.

- **I/O & caching**

  - Pluggable compressor (Snappy, LZ4, Zstd).
  - Direct I/O option; readahead windows; mmap option for index/filter blocks.
  - Two-tier cache: block cache + file handle cache with admission.

- **Space management**

  - File deletion queue with “safe to remove” epochs.
  - Blob files (value log) for large values (split KV: index in SST, value in blob).

## Reliability & tooling

- **Fuzzing** (WAL parser, SST parser, iterators).
- **Jepsen-style fault scripts** (kill -9, power-loss fsync tests).
- **SST dump/verify tool** and offline compactor.
- **Online backup** (checkpoint + MANIFEST copy).

# Midge-Specific Requirements

## TLV everywhere

- WAL records, MANIFEST edits, LOCK, and file footers start with a compact TLV header:

  - `(type:u8, len:varint, value:bytes)`; little-endian inside values.
  - Common field IDs registry to avoid collisions; unknown fields skipped.

## Lock/Read-only semantics (recap)

- Background renew @ `ttl/2`.
- On 3 consecutive renew failures:

  - Set in-proc `Mode::ReadOnly`.
  - Stop WAL appends, freeze memtable, allow reads and iteration.
  - Attempt periodic reacquire if configured.

## Process model

- **Sync API** by default (your preference).
- Bounded worker pool for flush/compaction (no Tokio required).
- Clean `Drop` ordering: stop writers → finish compactions → flush → close files.

## Namespacing / Column-family-like isolation

- Provide **logical Column Families (CFs)**:

  - Separate memtables and flushing; shared WAL sequence space to start.
  - Independent options per CF (block size, Bloom on/off).
  - Manifest tracks CFs → tables.

- Key encoding helpers (varint length-prefixed: `[cf_id][user_key]`).

## Integration with Fitz & Portia

- Efficient streaming snapshot for **streams** and **queues** backends.
- JSON-Patch friendly WAL entry (optional) for Portia materializations.
- Route-aware iterators (prefix/range helpers for `notice://`, `stream://`, etc).

## Security & integrity

- CRC32C per block + file trailer digest (XXH3 or Blake3).
- Optional at-rest encryption pluggable (AES-GCM) via key provider interface.

# Cloud Integration Requirements

## “HTTP-only” provider adapters (no heavy SDKs)

- **AWS S3**

  - REST + SigV4 signing (single dependency: your signer).
  - Multipart uploads for >64 MB; ETag validation; backoff & retries with jitter.

- **Azure Blob**

  - Shared Key / MSI (Managed Identity) via OAuth2 to get tokens; REST PutBlock/PutBlockList.

- **GCS**

  - JSON API with OAuth2 SA; resumable uploads.

- **Uniform interface**

  ```rust
  pub trait CloudBlob {
    fn put(&self, path:&str, bytes:&[u8]) -> Result<()>;
    fn get(&self, path:&str) -> Result<Vec<u8>>;
    fn head(&self, path:&str) -> Result<BlobMeta>;
    fn list(&self, prefix:&str) -> Result<Vec<BlobMeta>>;
    fn delete(&self, path:&str) -> Result<()>;
  }
  ```

  - Retries: exponential backoff w/ capped jitter; idempotent writes via temp name + rename.

## Cloud-native modes

- **Hybrid WAL:** WAL to cloud (append blobs or chunked objects), local SSTs.
- **Remote SST tiering:** cold levels (e.g., L5/L6) in object storage, cache on read.
- **Distributed lease (optional)**

  - Replace local `LOCK` with cloud lease row / blob metadata for containerized multi-tenant.
  - Same TTL heartbeat semantics.

## Credentials & policy

- Default credentials only (no legacy keys in code):

  - AWS: IMDS/ECS creds; Azure: MSI; GCP: ADC.

- Per-provider time sync guard (reject if clock skew > 5 min; surface clear error).

## Observability & limits

- Per-provider metrics (latency, throttle, 4xx/5xx).
- Bandwidth and request-rate caps to control cloud costs.
- Cost flags (e.g., “avoid small objects” → pack SSTs to min 32 MB).

# Milestones, Deliverables & Acceptance Criteria

## Phase 0 — Foundations (1–2 weeks)

- **Deliverables:** `tlv.rs` hardened (fuzz harness), `crc32c`, atomic file replace, `lock.rs`.
- **Accept:** lock acquire/renew/release; failover to read-only; unit tests cover expiry, takeover.

## Phase 1 — WAL + Memtable (2–3 weeks)

- **Deliverables:** WAL writer/reader w/ recovery; memtable (skiplist); basic `put/get/delete`.
- **Accept:** Crash-recovery test passes; 1M puts, full readback integrity.

## Phase 2 — SST Flush + Read Path (2–3 weeks)

- **Deliverables:** SST format v1; flush pipeline; iterator; block cache.
- **Accept:** Random point lookups @ p50 < 200 µs (local SSD), sustained inserts 100k+/s on dev HW.

## Phase 3 — Compaction & Stalls (2–3 weeks)

- **Deliverables:** Levelled compactor; picker; throttle/stall; manifest edits.
- **Accept:** Stable write throughput under load (no unbounded L0 growth); read-amp < 20 at steady state.

## Phase 4 — Advanced Features (4–6 weeks)

- **Deliverables:** range tombstones, TTL, merge operators, snapshots/checkpoints, partitioned Bloom.
- **Accept:** End-to-end tests for deletes/TTL/merge; checkpoint restore < 60s for 100 GB dataset.

## Phase 5 — Cloud Integrations (3–5 weeks)

- **Deliverables:** S3/Azure/GCS adapters; hybrid WAL; remote tiering; default creds.
- **Accept:** WAL to cloud with 99.9% p95 < 50 ms overhead (batched); cost sanity (no tiny object spam).

## Phase 6 — Hardening & Tooling (ongoing)

- **Deliverables:** SST dump/verify; offline compactor; Prometheus metrics; docs.
- **Accept:** Fuzzing 24h clean run for parsers; chaos tests (kill -9) green; perf dashboards.

# Testing Matrix (high level)

- **Unit:** TLV codecs, WAL boundaries, iterators, merge ops, Bloom queries.
- **Property tests:** key ordering, iterator correctness under interleavings.
- **Integration:** crash-recovery, compaction correctness, TTL expiry.
- **Performance:** YCSB A/B/C/D/F; mixed read/write; cache hit ratios.
- **Cloud:** retry paths, auth renewal, multipart edge cases, clock skew simulation.
- **Failure injection:** fsync failures, ENOSPC, throttling (429/503), partial object upload.

# Configuration (initial defaults)

- `memtable_bytes = 64MB`
- `wal_segment_bytes = 128MB`
- `block_size = 16KB`
- `bloom_bits_per_key = 10`
- `target_file_size_base = 32MB` (L1 doubles per level)
- `max_background_flushes = 2`, `max_background_compactions = 2`
- `wal_fsync = every_ms(10)` (prod can tune)
- `lock_ttl_ms = 5000`, renew every 2500 ms, 3 retries before read-only

# Non-Goals (for now)

- Distributed consensus/replication.
- Full SQL/secondary indexing engine.
- Cross-process transactions.
- Async public API (we keep it sync as requested).

if you want, i can turn this into a **SPEC.md** you can drop into `/docs/` (including TLV field registries, on-disk diagrams, and error codes), or jump straight into `lock.rs` + `wal/` scaffolding with tests.
