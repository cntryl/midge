# Cloud durable read retention design

Status: proposed design for issue #958. This document selects a design for
review; it does not claim implementation or acceptance criteria are complete.

## Source findings

- `EventLoop::plan_compaction` sets `CompactionPlan::snapshot_horizon` from
  `oldest_active_snapshot_sequence()` in `src/runtime/event_loop/compaction.rs`.
- `select_newest_version` in `src/compaction/executor/partition.rs` emits only
  the newest point version for a key group. The snapshot horizon controls
  tombstone eligibility; it does not cause the older point value to be emitted.
- `Engine::begin_tx` captures a `ReadSnapshot`, acquires its SST view, and
  registers the source pin while holding the snapshot acquisition guard in
  `src/engine/mod.rs`. `SnapshotPinRegistry` excludes GC from the capture to
  registration window. These pins protect readers that already exist, not a
  durable read opened after compaction discarded its source.
- `ReadSnapshot` already combines a fixed set of memtables and SST sources;
  transaction reads filter versions by `start_sequence`. A separate immutable
  SST generation per cloud frontier is therefore not required to express the
  read once the compaction inputs retain the versions the frontier needs.

## Proposed representation

While cloud durable reads are enabled, compaction writes a version-retaining
chain per key rather than collapsing the key to its newest version:

1. Let `F` be the cloud-durable sequence captured for the compaction plan.
2. Retain every point version with sequence greater than `F`. A later cloud
   frontier may advance through any of these versions before another
   compaction runs; discarding an intermediate version would make that later
   frontier unreadable.
3. Retain the newest point version at or below `F`, if one exists. This is the
   point value (or point tombstone) visible at the captured durable cut.
4. Versions below that floor may be dropped after normal tombstone and
   bottommost proofs permit it. Existing readers continue to use and pin their
   originally captured source generation.

The frontier is monotonic. If it advances while a compaction is running, the
compaction's retained suffix remains sufficient: it contains every version
above its captured `F`, including all versions the newer frontier can cross.
Compaction must not publish outputs built from a stale plan that omits that
suffix. A recovered or authority-fenced frontier that would move backwards
must fail closed until recovery establishes a frontier for which retained
history is sufficient.

Range tombstones follow the same cut. Tombstones above `F` remain available
for a later frontier; tombstones at or below `F` may be collected only when
the compaction proves their covered point history is no longer needed by the
current frontier and ordinary readers are protected by their source pins.
An above-frontier range tombstone cannot mask a value in a read at `F`.
Expiration is evaluated at the read snapshot's captured read time, as in the
existing `ReadSnapshot` behavior; the durable sequence does not rewind wall
clock time. This read-time TTL rule must be checked alongside #912/#942 before
implementation.

This design adds retained history to ordinary compaction outputs. It does not
create a new object or SST generation for each frontier. Jev weakly preferred
a separately materialized source view (confidence 0.48); the existing fixed
`ReadSnapshot` plus sequence filtering provides that view, while this retained
chain supplies its missing historical versions. That source-based assessment
is the reason for preferring the smaller representation, and remains subject
to an implementation prototype and adversarial review.

## Enablement and safe reads

- Persist an opt-in capability in the database's creation metadata, off by
  default. The first implementation may enable it only when creating a new
  store. Opening an existing store with the option newly requested must return
  an explicit unsupported/unsafe error unless a persisted retention proof
  demonstrates that every needed version is still present. Never infer safety
  from the current frontier scalar alone.
- Disabling is allowed after new durable reads are stopped and every live
  durable-read pin is released. Compaction can then collapse retained chains
  under the ordinary policy. Report retained-history metrics until those
  obsolete versions have actually been removed; a setting change alone does
  not mean retained bytes are zero. Re-enabling after disablement requires a
  new proof or a new store.
- Add a read-only `begin_tx_at_cloud_durable(cf)` operation. Acquire the
  snapshot/GC acquisition guard before capture. In one event-loop operation,
  capture the cloud-durable sequence and the `ReadSnapshot` source view. Pin
  the captured SST generation before releasing acquisition exclusion. Return
  only after the pin is registered. The transaction's start sequence is the
  captured frontier, not a later frontier read from a separate atomic.
- Apply the existing cloud lease read fence when acquiring and using the
  transaction. Authority loss returns the existing fenced error; it cannot
  silently fall back to local latest reads or return an incomplete durable
  view.

## Metrics and release

Expose counts and bytes for point versions retained solely to serve the
durable-read history, and a separate count/byte observation for retained range
tombstones if their accounting can be made exact. Define the accounting owner
as the currently published source generation so compaction input/output
overlap is not double-counted. Decrease the published count only when an output
generation is committed and no longer-needed input generation is unpinned and
eligible for GC. A frontier advance makes the old floor eligible for a later
compaction; it does not itself release bytes held by a live source pin.

Implementation must include AC1's repeated compaction scenario, AC2's disable
and zero-after-reclamation check, AC3's late-enable rejection, AC4's measured
retention and release under held uploads, and AC5's interleaving/property
coverage. In particular, tests must cover frontier advance during compaction,
range deletes above and below the frontier, TTL at the captured read time,
snapshot capture racing with SST replacement/GC, and loss of cloud authority.

## Open implementation proofs

Before implementation is accepted, prove these details against the production
compaction and publication code:

1. The retention floor travels with the compaction plan and every partition;
   no merge, repair, trivial move, or output split collapses a retained chain.
2. The plan's frontier cannot regress across lease epoch recovery, and stale
   compaction output cannot publish after a newer frontier requires a different
   retention floor.
3. Range tombstone fragmentation and point tombstone GC preserve the same
   cut for every key, including keys not present in the current partition.
4. Persisted enablement metadata is created atomically with the store and
   cannot be lost during manifest rewrite or recovery.
5. Retained-byte metrics follow published generations, pins, and physical
   reclamation rather than counting planned compaction output as already freed.
