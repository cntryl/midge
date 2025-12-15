## Core Architecture
- Concurrency model: Actor-serialized per-partition (single actor sequences all writes/metadata changes). Background threads perform IO (WAL fsync/upload) and compaction; actor schedules and coordinates them.
- Throughput limits: Single-actor dispatch and seqno assignment, WAL append/fsync (local or cloud), and memtable ingestion are the main rate-limiting steps. With per-batch fsync, throughput == fsync throughput; with group commit, actor CPU and WAL batching dominate.
- Failure/recovery: Actor persists writes to WAL (local). On restart, WAL replay rebuilds memtable and actor state. Cloud WAL is an async durability path; manifest/compaction state reconciled at startup.

## Write Path Detail
- 1000-message batch flow (typical): Client -> partition API -> partition actor receives `WriteBatch(1000)` -> actor reserves contiguous seqno range -> single batched WAL append -> wait for persistence per durability level -> batched memtable apply -> advance visible seqno and ack -> background uploader uploads WAL and schedules flush/compaction.
- Latency targets (guidance): memory-ack sub-ms–few ms; local-ack (fsync) P50 1–10ms, P99 depends on media (5–50ms typical); cloud-ack P99 ~50–200ms unless optimized.
- Serialization points that limit throughput: actor message handling, WAL append/fsync, seqno assignment (per-message vs range), and memtable insert if not batched.
- Atomicity: Prefer assigning a contiguous seqno range and writing a single batched WAL record; actor advances visible seqno only after WAL persistence target and memtable apply, making the WriteBatch atomic.

## Read Path Detail
- Reads should avoid serialized actor hops for hot reads: use atomic snapshots and lock-free reads from memtable and SSTs when possible.
- Snapshots: actor exposes committed seqno; readers use a snapshot token that maps to memtable snapshot + SST manifest at that seqno for consistent consumer positions.
- Cache hierarchy: memtable (hot, ~sub-ms), local SSTs + block cache (ms), cloud (cold, tens–hundreds ms). Tail reads target sub-ms–low-ms; historical cold reads can be 10–200ms.

## Compaction & Retention
- DeleteRange/time-based retention: typically implemented as range tombstones compacted into SSTs; efficient deletion only if SSTs are time-bucketed so whole SSTs can be dropped; otherwise rewrite is required.
- Compaction blocking: compaction runs in background, but if compaction/flush can't keep up memtables will accumulate and eventually throttle writes; IO contention from compaction can also raise tail latencies.
- Deterministic compaction planning (time-buckets) reduces rewrite work and makes performance predictable at cost of layout flexibility.
- Compaction duration: can be seconds to minutes; system must throttle or trigger backpressure before backlog causes write stalls.

## Durability Model
- Node fail with local-ack writes in flight: local WAL persistence survives process restart but not total node/disk loss if WAL isn't replicated or uploaded. Broker-level replication is required for cross-node durability.
- Cloud WAL vs broker replication: cloud uploads are an eventual backup unless broker protocol waits for cloud-ack. Local-ack is safe only if the broker's replication semantics ensure N replicas persist the write before leader ack.
- Implicit assumption: if design favors local fsync + async cloud upload, it's assuming broker-level replication for primary durability; cloud-ack is an optional stronger mode with higher latency.

## Critical Risks
1. Actor single-threaded serialization becoming CPU-limited at 100k–500k msgs/sec per partition without aggressive batching or zero-copy paths.
2. WAL fsync latency and IO bandwidth (local or network) limiting sustainable throughput and raising P99.
3. Seqno assignment per-message (vs range allocation) causing extra serialization and overhead.
4. Memtable insertion inefficiencies or cache thrashing when applying large batched writes on the actor thread.
5. Compaction backlog leading to memtable pressure and write stalls if resource isolation and throttling are insufficient.
6. Cloud upload latency variability undermining cloud-ack SLOs.
7. Time-range deletes requiring heavy SST rewrites unless SSTs are aligned to retention buckets.

## Actor Protocol Example
For `WriteBatch(1000)` (preferred, efficient flow):
- Sequence:
  1. Client -> Actor: `WriteBatch(1000)`.
  2. Actor atomically reserves seqno range `[S+1..S+1000]`.
  3. Actor creates a single batched WAL record covering the range and calls `WAL.append(batch)`.
  4. WAL persistence: actor waits per durability (none/memory, local fsync, or cloud upload) before acking.
  5. Actor applies batch to memtable in one batched operation.
  6. Actor advances visible seqno to `S+1000`, notifies listeners, and returns ack.
- Seqno: assigned as a contiguous range at receive time.
- WAL and memtable ops: both batched for the whole write batch; atomic commit boundary is: WAL persistence (as configured) + memtable apply + visible seqno advance.

## Open Questions
- Are contiguous seqno range reservations implemented today, or must they be added? Range allocation is key to reducing per-message overhead.
- Does WAL support atomic single-record append for multi-message batches, or are multiple records required per message?
- Is the read path fully lock-free w.r.t. concurrent writes (atomic memtable publish and memory barriers)?
- What exact P50/P99 SLOs are expected for memory/local/cloud acks to guide design choices (group commit vs per-batch fsync)?
- How are compaction resources prioritized and throttled to avoid write path stalls under sustained load?
- Are SSTs organized by time buckets to permit cheap DeleteRange by SST dropping?
- How are prolonged cloud WAL upload failures handled wrt acks and broker replication correctness?

---

If you'd like, I can add a short checklist of microbenchmarks and failure tests (actor throughput, batched WAL fsync vs per-message, compaction backlog stress tests) to validate the load-bearing assumptions.
