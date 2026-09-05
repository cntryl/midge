# Ephemeral local storage for cloud persistence

Cloud-backed Midge must be able to operate a terabyte-scale object-store
dataset using a small, explicitly bounded local working disk. A 20 GiB local
budget must not imply a 20 GiB database-size limit. Local space is for active
WAL, transaction spill, and flush/compaction staging; published SST data lives
in object storage. This is an architectural requirement, not evidence of a
completed terabyte-scale provider qualification.

## Startup and reads

Startup loads the manifest and recovery metadata and checks remote SST
existence and size without downloading the manifest's SST inventory. Catalog
memory and the number of metadata requests remain proportional to SST count.
Bloom filters, indexes, and data blocks are loaded when an SST is actually
opened for a read or maintenance operation.

SST V4 already separates checksummed footer, metadata, index, bloom, trie,
tombstone, and data blocks. A remote immutable filesystem view uses conditional
range reads to access these existing blocks. One reader pins one provider
object identity, including across its repeated file opens. A provider must
return the requested range, length, and identity; a whole-object response is
rejected and its response buffering is capped. Arbitrary local SST files are
not trusted as a cache of that identity.

One quarter of the configured shared block-cache allowance is reserved for
reader metadata. Idle readers are evicted under pressure; active readers keep
their reservations until released. A read that cannot fit its required
metadata fails with a resource error. The remaining allowance holds data
blocks. A scan cannot leave every previously visited SST reader resident.

## Working disk and compaction

Cloud mode exposes `OpenOptionsBuilder::local_storage_budget(bytes)`. WAL
append, transaction spill, flush, and compaction staging use shared admission
accounting. Failed or ambiguous writes retain their charge; confirmed removal
of a local file releases its resident charge. Published SSTs are evicted after
remote publication instead of accumulating a second database copy locally.
Flush admission covers a conservative encoded output bound and its simultaneous
cloud-readback verification file before writing either. A failed publisher
keeps its reservation across retries. Failed cleanup retains capacity charges,
and startup accounts for any recovery scratch that could not be removed.

Before WAL append, cloud transaction admission checks the complete operation
set against a flush window of half the local disk budget, including both the
encoded SST bound and its verification copy. Active generations are frozen
before combining them with another transaction would exceed that window;
automatic flush also starts when their charge reaches one quarter of the disk
budget. An atomic transaction which cannot fit by itself returns `NoSpace`
before WAL append. These conservative limits apply to values, point deletes,
range tombstones, and spilled transactions; increasing the memory budget does
not increase this disk allowance.

Compaction streams remote inputs and drains each completed output partition to
object storage before producing the next. Its staging reservation covers the
scratch and finalized partition. An indivisible output which cannot fit is
rejected. Input files remain authoritative until the complete replacement set
is published. An interrupted prepublication upload may leave an unreachable
remote object; it never authorizes deletion of an input. The existing per-CF
SST filename allocation counter is durably advanced and mirrored before the
worker starts, preventing a replacement process from reusing an orphan name.

WAL recovery currently stages the required replay set. It preflights that set
and existing working files against the selected disk budget before downloading
anything or replacing previous recovery scratch. A recovery backlog larger
than the available working budget returns `NoSpace`; streaming one replay
segment at a time is not implemented by this change. This limit concerns
uncheckpointed recovery work, not the size of the remote SST database.

## Integrity and recovery

Metadata-only startup cannot promise a full database scrub. Data-block
corruption is reported when that block is read; it must never become a
successful missing-key result. The footer and every fetched SST block retain
their existing CRC checks. Conditional reads prevent mixing object versions
within a reader.

Unfinished publication intents require stronger validation: verify their exact
file size and recorded complete-file checksum through bounded range reads.
This work may read all bytes of the relevant outputs, but must not stage all
outputs locally. When migrating an existing local cache, verify the remote
publication proof before discarding a resident copy. An empty local cache does
not trigger that migration scan.
Explicit salvage recovery can retain an individually verified local SST when
its remote copy is unavailable. Only those named, verified copies bypass remote
reads, and they continue to count against the local budget.

WAL retirement preserves its exact coverage and integrity proof. Relevant SSTs
may require a streamed complete-file checksum before a WAL can be retired;
unrelated complete key ranges are excluded. This bounds buffers and local
residency, but does not make all maintenance I/O proportional to point reads.
Persisting provider identities alongside publication checksums would allow a
separate optimization of that verification cost.

## Qualification

Use deterministic filesystem-backed cloud and counting/conditional provider
tests for range sizes, transferred bytes, corruption, version replacement,
cache loss, bounded metadata retention, and publication recovery. Measure peak
working disk during writes and compaction, including temporary files and
failed uploads. Final disk size alone is insufficient evidence of a bound.

The filesystem cloud simulator retains control metadata locally. Its cold
SST/WAL cache recovery tests preserve that metadata; they do not establish
recovery after losing the entire local disk. Native cloud startup restores
control metadata from object storage, and requires separate full-disk-loss
qualification with a real provider.

Live qualification must separately measure a cold-cache open, random reads,
scans, sustained writes/compaction, interrupted publication, and recovery on a
20 GiB working disk with a remote dataset substantially larger than that disk.
Record the provider, exact revision, peak disk and memory, request counts,
transferred bytes, and recovery time before making operational scale claims.
