# Architecture Diagrams

This document is a visual companion to [architecture.md](architecture.md). It uses Mermaid diagrams to show the main ownership boundaries and data flows without replacing the invariant and recovery docs.

## Module Boundaries

```mermaid
flowchart TB
    API["Public API<br/>Engine, Transaction, OpenOptions"]
    Engine["engine<br/>facade, startup/recovery, ingest"]
    Runtime["runtime<br/>protocol, lifecycle, event loop, state, snapshots"]
    HybridRuntime["runtime::hybrid_persistence<br/>WAL/SST proof and prune policy"]
    Actors["runtime::actors<br/>WAL, flush, compaction, manifest, GC, cloud, eviction"]
    Memtable["memtable<br/>ordered in-memory state"]
    WAL["wal<br/>frames, segments, recovery, actor slices"]
    SST["sst<br/>readers, writers, bloom, cache, trie, reader I/O slices"]
    Metadata["metadata<br/>manifest, journal, format marker"]
    Storage["storage<br/>filesystem, hybrid queues/proofs/uploads, cloud, providers"]
    IO["io<br/>real and mock filesystem abstraction"]
    Compaction["compaction<br/>planning, merge, execution"]
    Lease["lease<br/>single-writer fencing"]
    Telemetry["telemetry<br/>metrics and health"]
    BenchSupport["benches/bench_support<br/>benchmark-local helpers"]
    TestSupport["tests/common<br/>integration-test support"]

    API --> Engine
    Engine --> Runtime
    Engine --> Lease
    Runtime --> Actors
    Runtime --> HybridRuntime
    HybridRuntime --> WAL
    HybridRuntime --> SST
    HybridRuntime --> Metadata
    HybridRuntime --> Storage
    Runtime --> Memtable
    Runtime --> WAL
    Runtime --> SST
    Runtime --> Metadata
    Runtime --> Storage
    Actors --> Compaction
    Actors --> WAL
    Actors --> SST
    Actors --> Metadata
    Actors --> Storage
    WAL --> IO
    SST --> IO
    Metadata --> IO
    Storage --> IO
    Runtime --> Telemetry
    BenchSupport -. exercises .-> API
    BenchSupport -. mocks .-> Storage
    TestSupport -. exercises .-> API
    TestSupport -. mocks .-> Storage
```

## Runtime Ownership

The runtime owns mutable engine state. API calls cross into the runtime through messages; actors mutate state only through the event loop.

```mermaid
flowchart LR
    Engine["Engine facade"]
    Handle["RuntimeHandle<br/>request channel"]
    Router["ResponseRouter<br/>oneshot responses"]
    Loop["EventLoop<br/>owns RuntimeState"]

    subgraph ActorSet["Actor set"]
        WalActor["WalActor"]
        FlushActor["FlushActor"]
        CompactionActor["CompactionActor"]
        ManifestActor["ManifestActor"]
        GcActor["GcActor"]
        CloudActor["CloudActor"]
    end

    State["RuntimeState<br/>memtables, manifest, WAL state, health"]
    SnapshotCache["SnapshotCache<br/>published ReadSnapshots"]
    Workers["Worker callbacks<br/>compaction and storage events"]

    Engine --> Handle
    Engine -->|begin_tx bypass| SnapshotCache
    Handle --> Loop
    Loop --> ActorSet
    ActorSet --> Loop
    Loop <--> State
    Loop -->|publish after writes, flushes, CF lifecycle| SnapshotCache
    Loop --> Router
    Router --> Engine
    Workers --> Loop
```

Actor-local mutable state must remain transient and rebuildable. Authoritative recovered state belongs in `RuntimeState`, the manifest, WAL, SSTs, and the publication intent log.

## Write Path

Write visibility and write durability are related but not identical. The selected `WriteOptions` determines where commit waits before responding.

```mermaid
sequenceDiagram
    participant Caller
    participant Engine
    participant Runtime
    participant WAL as WalActor
    participant Memtable as RuntimeState memtable
    participant Durability as DurabilityCoordinator

    Caller->>Engine: Transaction::commit(write_options)
    Engine->>Runtime: RuntimeMsg::ApplyTransaction
    Runtime->>WAL: allocate sequence and append records

    alt best_effort
        WAL-->>Runtime: skipped WAL
    else buffered or sync or cloud
        WAL-->>Runtime: appended local WAL records
    end

    Runtime->>Memtable: apply committed operations
    Runtime->>Durability: update local/cloud waiters

    alt WriteOptions::sync
        Runtime->>WAL: fsync/group sync
        WAL-->>Runtime: local durable frontier advanced
    else WriteOptions::buffered
        Runtime-->>Engine: return after local append and visibility
    else cloud strict
        Runtime-->>Engine: wait until CloudAck covers sequence
    end

    Engine-->>Caller: commit result
```

## Read Path

Reads use an immutable snapshot assembled from the current memtable, immutable memtables, and manifest-visible SSTs. Manifest visibility, not raw file presence, controls which SSTs participate.

```mermaid
flowchart TB
    Caller["Caller"]
    BeginTx["Engine::begin_tx"]
    Cache["SnapshotCache<br/>lock-free published boundary"]
    Register["RuntimeMsg::RegisterSnapshot<br/>SST pin tracking"]
    Snapshot["ReadSnapshot<br/>visible sequence + CF state"]
    Fallback["Event-loop read handlers<br/>fallback: Read, RangeScan, CaptureReadSnapshot"]
    Active["Active memtable"]
    Immutable["Immutable memtables"]
    Manifest["Manifest-visible SST metadata"]
    SSTReaders["SST readers<br/>cache, block bloom, binary/trie index"]
    Merge["Sequence/tombstone resolution"]
    Result["Visible key/value result"]

    Caller --> BeginTx
    BeginTx --> Cache
    Cache --> Snapshot
    BeginTx -. cache miss or stale cache .-> Fallback
    BeginTx --> Register
    Fallback -. fallback path .-> Snapshot
    Snapshot --> Active
    Snapshot --> Immutable
    Snapshot --> Manifest
    Manifest --> SSTReaders
    Active --> Merge
    Immutable --> Merge
    SSTReaders --> Merge
    Merge --> Result
```

## Flush Publication

Flush is staged so a crash cannot make an orphan SST authoritative by accident.

```mermaid
stateDiagram-v2
    [*] --> ActiveMemtable
    ActiveMemtable --> ImmutableMemtable: freeze
    ImmutableMemtable --> SstOutput: write SST
    SstOutput --> IntentOutputDurable: record flush intent
    IntentOutputDurable --> ManifestPublished: publish file metadata
    ManifestPublished --> IntentCleared: clear intent
    IntentCleared --> [*]

    SstOutput --> RecoverFromWal: crash before durable output
    IntentOutputDurable --> ReplayIntent: crash before manifest publish
    ManifestPublished --> FinalizeCleanup: crash before cleanup
```

## Compaction Publication

Compaction follows the same authority boundary: inputs stay authoritative until the manifest publishes their replacement set.

```mermaid
flowchart LR
    Inputs["Manifest-visible input SSTs"]
    Plan["Compaction plan"]
    Execute["Merge and write replacement SSTs"]
    Intent["Compaction intent<br/>output durable"]
    Publish["Manifest batch<br/>remove inputs, add outputs"]
    Cleanup["Delete obsolete inputs"]

    Inputs --> Plan
    Plan --> Execute
    Execute --> Intent
    Intent --> Publish
    Publish --> Cleanup

    Intent -. crash .-> RecoveryA["Recovery can publish replacement<br/>or keep inputs authoritative"]
    Publish -. crash .-> RecoveryB["Recovery finalizes cleanup idempotently"]
```

## Recovery Sequence

Startup rebuilds trusted state in an order that keeps the manifest as the authority boundary and WAL as the durable prefix for unpublished writes: load the manifest and intent log, replay WAL, then replay publication intents.

```mermaid
flowchart TB
    Open["Engine::open"]
    Lease["Acquire primary lease"]
    Format["Validate format marker"]
    Manifest["Load manifest and journal"]
    Intent["Load publication intent log"]
    WalReplay["Replay WAL durable prefix"]
    PublishReplay["Replay flush/compaction intents"]
    Health["Classify health and recovery metrics"]
    Runtime["Start runtime event loop"]

    Open --> Lease
    Lease --> Format
    Format --> Manifest
    Manifest --> Intent
    Intent --> WalReplay
    WalReplay --> PublishReplay
    PublishReplay --> Health
    Health --> Runtime
```

## Hybrid Cloud Mode

The runtime owns format interpretation and publication/prune policy. Hybrid storage sees only explicit object keys, bytes, provider identities, bounded queues, and conditional operations.

```mermaid
flowchart TB
    Runtime["Runtime"]
    Proofs["runtime::hybrid_persistence<br/>format validation and coverage"]
    WalActor["WalActor"]
    Hybrid["HybridStorage<br/>raw bounded object I/O"]
    LocalWal["Local WAL segment"]
    CloudWal["Cloud object<br/>wal/{segment}.wal"]
    CloudAck["StorageEvent::CloudAck"]
    CloudFail["StorageEvent::CloudFail"]

    FlushActor["FlushActor"]
    LocalSst["Local SST cache"]
    CloudSst["Cloud object<br/>sst/{name}"]
    Backpressure["BackpressureOn/Off"]

    Runtime --> WalActor
    WalActor --> LocalWal
    WalActor --> Proofs
    Proofs --> Hybrid
    Hybrid --> CloudWal
    CloudWal --> CloudAck
    CloudWal --> CloudFail
    CloudAck --> Proofs
    Proofs --> Runtime
    CloudFail --> Runtime

    Runtime --> FlushActor
    FlushActor --> LocalSst
    LocalSst --> Proofs
    Proofs --> Hybrid
    Hybrid --> CloudSst
    Hybrid --> Backpressure
    Backpressure --> Runtime
```

## Cloud WAL Durability State

```mermaid
stateDiagram-v2
    [*] --> LocalAppend: transaction append
    LocalAppend --> Visible: apply to memtable
    Visible --> Sealed: rotate WAL segment
    Sealed --> PendingUpload: enqueue_wal_segment
    PendingUpload --> InProgress: process_uploads
    InProgress --> UploadedOrphan: immutable upload success
    UploadedOrphan --> CatalogPublished: lease-fenced catalog CAS
    CatalogPublished --> CloudDurable: exact readback and lease recheck
    InProgress --> Retry: upload failure below retry budget
    Retry --> InProgress
    InProgress --> Failed: retry budget exhausted
    CloudDurable --> Acked: CloudAck
    Failed --> FailedEvent: CloudFail
    Acked --> [*]
    FailedEvent --> [*]
```

## Source Reading Map

```mermaid
flowchart LR
    Start["Start here"]
    EngineMod["src/engine/mod.rs"]
    RuntimeMod["src/runtime/mod.rs"]
    EventLoop["src/runtime/event_loop/"]
    ActorsDir["src/runtime/actors/"]
    WalRecovery["src/wal/recovery.rs"]
    Manifest["src/metadata/manifest.rs"]
    StorageMod["src/storage/mod.rs"]
    Tests["tests/"]

    Start --> EngineMod
    EngineMod --> RuntimeMod
    RuntimeMod --> EventLoop
    EventLoop --> ActorsDir
    ActorsDir --> WalRecovery
    ActorsDir --> Manifest
    ActorsDir --> StorageMod
    StorageMod --> Tests
```

## Related References

- [architecture.md](architecture.md)
- [recovery-internals.md](recovery-internals.md)
- [storage-invariants.md](storage-invariants.md)
- [testing.md](testing.md)
