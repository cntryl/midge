# Cloud metadata protocol candidate for 0.5.0

The metadata pointer is a separate checksummed binary object at
`metadata/committed-pointer.v1`. Heartbeats replace the lease document without
changing this pointer's conditional identity. Immutable metadata bodies become
authoritative only when the pointer selects their exact generation; LIST order
and an uploaded ordinal never establish commitment.

## Epoch claim and takeover

Before activating a newly acquired lease's monotonic validity, acquisition
CAS-claims the metadata pointer with its writer epoch, holder and unique owner
token. The claim preserves the last committed generation. A pointer with an
equal epoch and different owner, or a higher epoch, cannot be claimed.

Both a generation publication and an epoch claim compare-exchange the same
pointer identity. A delayed predecessor mutation has two possible orders:

| Order | Required result |
| --- | --- |
| Predecessor CAS, then successor claim | The claim reads and retains that committed predecessor history. |
| Successor claim, then predecessor CAS | The predecessor's original conditional identity is rejected. |
| Stale writer reads the successor identity | Its epoch and owner still disagree; it cannot submit a publication using that identity. |
| Pointer claim loses its response | Exact pointer readback may reconcile the same epoch and owner; a successor claim cannot count as success. |
| Generation CAS loses its response | Exact pointer readback must select that generation under the same epoch and owner. Otherwise retain uncertainty. |

A successful claim is required before the successor serves recovery or write
work. Acquisition still owns lease cleanup when its request fails. Learning
that a deadline expired does not cancel an admitted provider effect: pointer
identity exclusion remains necessary after timeout. Committed predecessor
history remains valid across epoch changes; the pointer's current writer epoch
is separate from the epoch that produced an immutable predecessor body.

New acquisitions use lease document version 4 and the version 3 initialization
sentinel. A fresh sentinel stays pending until the empty pointer is claimed;
only then does activation make the sentinel permanent and enable validity.
A crash after the lease CAS or pointer claim therefore remains an explicit
uncommitted initialization state which a later epoch can finish. An active
sentinel with a missing pointer is never repaired as empty, because that could
discard committed history. Legacy lease/sentinel versions cannot use this
repair path.

## Bytes and identity

Pointer bodies use the same bounded MessagePack/CRC32C frame codec as local
manifest snapshots and journal payloads. Conditional writes use the pointer's
pinned ETag/generation identity. Validated provider checksum receipts must match
the exact sent bytes; missing receipts or unknown results require an exact
metadata-bearing readback. A bare ETag does not prove payload integrity.

The permanent initialization sentinel and lease presence checks continue to
fail closed when initialized authority disappears. Embedded metadata pointers
from the old lease layout are rejected. FORMAT 8 data is rejected rather than
migrated by FORMAT 9 binaries.

## Versioned snapshots and edits

The default writer publishes one object at
`manifest/{ordinal:020}/{operation_uuid}` for a metadata change. The shared
codec records writer epoch, stable operation identity, exact predecessor
reference and control-file suffix edits. A standalone snapshot records the
complete file presence map. The pointer carries the exact ordered closure,
bounded to 16 objects and 64 MiB encoded bytes. Recovery checks contiguous
ordinals, predecessor identities and nondecreasing writer epochs before
applying the oldest snapshot and following edits. It never chooses a LIST
candidate by ordinal.

At 16 retained objects or 16 KiB of edit history the next publication starts
a new snapshot. An individual edit above 16 KiB also becomes a snapshot.
The operation UUID prevents an uncommitted orphan from blocking a later
attempt to publish the same ordinal. Upload receipt validation or exact
readback must complete before pointer CAS.

## Remaining implementation and qualification

Writer admission caches the claimed pointer identity only inside the acquired
monotonic lease window. A validated pointer receipt updates that identity;
unknown effects require exact readback, and a failed CAS clears the cache.
A reduced authority view and fixed SHA-256 control-file fingerprints avoid
rereading the predecessor bodies on the common path. The active transport
regression counts one edit PUT and one pointer CAS, with no reads, when both
receipts and the writer cache are available. It remains to be executed during
the batch qualification pass. Cold starts, checkpoint publications, missing
receipts and retention runs can require additional requests.

Retention (#913) operates only on the new `manifest/` versions. It retains the
head and three preceding ordinals, plus every object in the pointer's exact
snapshot/edit closure regardless of age. A publication holds the shared
metadata lock through its admitted operation and retention. Metadata retirement
readers hold that same lock from pointer capture through complete body decoding
and final pointer verification. The lock therefore excludes same-runtime
pending publication and body-fetch races. A decoded retirement proof no longer
needs its old encoded objects and must still verify the fresh pointer before
retiring WAL history.

Takeover claims preserve the predecessor's selected closure before a successor
recovers. A future publication may only extend that exact closure or replace it
with a complete snapshot; it never adopts an orphan found by LIST. A candidate
below the retained ordinal window and absent from the selected closure cannot
become selected after the final check. This also covers a delayed predecessor
PUT or DELETE: UUID keys and pointer identity exclusion prevent it from changing
successor-selected history. Startup fetches occur after acquisition and before
the runtime starts its collector. Old-epoch cleanup proofs fail their writer
checks after takeover.

Collection is attempted at most every 30 seconds following a successful mirror,
with at most four ordinal-prefix LISTs and eight candidate probes per attempt.
Each LIST is a single non-retrying bounded page. Native S3/OCI, Azure and GCS
requests include the provider's page-size control; responses are capped at
64 KiB and 32 returned keys, with bounded continuation tokens. The cursor stays
on an ordinal until all its pages are consumed. Unsupported bounded-page stores
retain data rather than falling back to complete-prefix enumeration.
Each probe pins a bounded exact identity, decodes and checks its ordinal,
operation and writer epoch, then freshly validates the lease and selected
pointer before an identity-conditional DELETE. Unknown reads and conditional
failures retain data. The scan cursor and counters live with the publication
owner; restart resets the cursor and rediscovers obligations by canonical
ordinal prefix. Legacy `metadata/generations/` cleanup is deliberately omitted
because the new layout replaces that protocol. A separate idle deadline submits
an owned no-op metadata mirror every 30 seconds. It uses the same publication
gate, worker and fresh pointer checks, so cooldowns and bounded batches continue
making progress without another metadata write. Its active idle regression
injects a short interval in the same owners and drains more than eight obsolete
snapshots through production idle wakeups without resetting cooldown state.

General orphan collection (#943) remains separate: SST deletion additionally
needs live snapshot sources, compaction inputs, durable obligations and admitted
effects, with final authority revalidation under capture-to-pin exclusion. The
metadata-only dependency proof does not authorize an SST sweep.

Active tests cover stale writers learning successor identities, delayed
predecessor CAS, initialization crashes, bounded snapshot/edit replay and an
unselected high ordinal. The implementation-first pass defers
execution, the simulator crash campaign, heartbeat/publication interleavings
and provider qualification. Those remain acceptance gates.
