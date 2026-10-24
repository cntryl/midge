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

General SST orphan discovery freezes each family's durable allocation floor
during recovery, before successor replay output, prewarming or reservation.
The runtime binds this immutable floor to its acquired epoch. A moving
`next_sst_seqs` counter never expands orphan authority. Missing family bounds,
unrecognized names and an epoch mismatch retain objects.

The periodic mirror continuation captures current manifest names, durable
retired proofs, live snapshot sources, all publication intents, concurrent
compaction inputs/prepared names/output generations and pending mirror
compaction/flush work. Snapshot acquisition exclusion closes the source-capture
to pin-registration gap. The manifest publication gate stays owned through the
worker. Before deletion, the worker also reads and verifies the current selected
metadata closure and intent bytes, and freshly validates both lease and pointer.
Uncertain reference reads retain the candidate. Maintenance memory bounds the
captured set and decoded shared references.

Only canonical names strictly below the frozen floor and absent from every
protected source are candidates. Successor admissions start at or above that
floor. A new compaction cannot reference an absent source; existing jobs and
pending cuts were captured. A later successor claims the selected metadata
before serving work. Consequently these keys cannot become selected after the
final check. This is distinct from a known-retired identity: the sweeper can
HEAD and conditionally delete a permanently unreachable key, while the known
queue still requires its original checksum and never upgrades a replacement.
Current-epoch unpublished uploads stay protected until a later takeover
establishes a floor. A delayed predecessor PUT can recreate only an unreachable
leak; repeated bounded inventory cycles revisit it.

Each interval retains at most one 32-key inventory page and performs at most
eight candidate probes/deletes. Unprobed keys remain with the cursor until the
next interval. Unsupported bounded-page inventories retain/defer; full-prefix
LIST is never a fallback. Native LIST pages make one physical request with
retries disabled and a 64 KiB response limit. Engine runtime metrics report
`cloud_orphan_list_attempts`, `cloud_orphan_probes`,
`cloud_orphan_delete_attempts`, `cloud_orphan_retained` and
`cloud_orphan_inventory_unavailable`. Probe/delete transport failures retain
the durable discovery source; their existing provider retry/deadline policy
remains applicable. Known-retired work admits at most two workers and eight
identities per worker, retaining the rest for scheduled retry.

The durable catalog anchor can separately rediscover retired WAL objects; that
discovery scan is still outstanding. Public hybrid snapshot-across-compaction,
delayed predecessor effects, failure campaigns and request-budget qualification
remain #943 acceptance gates.
