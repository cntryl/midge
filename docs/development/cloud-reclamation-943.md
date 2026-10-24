# Durable cloud reclamation candidate for 0.5.0

Removing an SST through a manifest edit records its original `FileMeta` in
`retired_ssts` in the same decision. A checkpoint retains this map after its
journal tail is truncated. The owning cloud publication therefore persists the
physical deletion obligation before acknowledging the removal. A cold cache
hydrates these obligations from the selected metadata chain.

Startup imports the map into the existing reclamation continuation. A local
removal may have crashed before its mirror, so restart first republishes that
authority; it cannot infer remote unreachability from the local manifest alone.
Live snapshot pins and compaction inputs retain the obligation. Protected work
has a retry deadline, in addition to the snapshot-release wakeup.

The worker requires the original checksum and length. It HEADs a candidate,
then computes CRC32C through 256 KiB exact-range reads pinned to that identity.
A replacement length, checksum mismatch, missing checksum or failed range read
retains the object. The final DELETE validates writer authority and names the
pinned provider identity. A bare HEAD never grants deletion authority. Unknown
DELETE outcomes keep the durable obligation; a later confirmed absence can
finish it. Both the worker and its queues remain owned through shutdown.

After remote deletion and local cleanup succeed, the loop journals a
`CompleteSstDelete` carrying the original `FileMeta`. The manifest validates it
against the retired identity and refuses active-file or replacement-identity
confirmations. Failed confirmation persistence retains the retry and reloads
uncertain metadata. The next cloud mirror can discard the tombstone; an older
cloud tombstone merely causes a safe, idempotent absence check after restart.
The binary FORMAT 9 candidate includes this field and journal record type 13;
no separately released format is migrated. Empty maps are omitted from snapshots.

Active regressions cover checkpoint/journal recovery, cold cloud obligation
hydration, original-only bounded reads, replacement retention, missing checksums,
final writer fencing, stale completion identities and snapshot pin release.
The implementation-first pass has not executed these tests or crash campaigns.

General orphan discovery remains disabled. A proposed conservative discovery
policy would freeze each family's durable allocation floor immediately after
takeover and hydration, before successor prewarming or reservation. Only names
below that immutable floor could be candidates. The current manifest, live
snapshot sources, compaction inputs, every durable publication intent, pending
mirror continuations and admitted effects must be captured under publication
and pin-acquisition exclusion. Current-epoch unpublished uploads stay protected
until a later takeover establishes a floor. A delayed predecessor upload can
recreate an unreachable object, so repeated bounded discovery is still needed.

The durable catalog anchor can separately rediscover retired WAL objects; that
discovery scan is also outstanding. Neither metadata retention nor the durable
SST queue enables these broader scans. Public hybrid snapshot-across-compaction,
delayed predecessor effects, failure campaigns and request-budget qualification
remain #943 acceptance gates.
