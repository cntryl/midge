# Logical SST level moves (#926)

`FileMeta.level` is the logical level used by reads, layout planning and recovery.
The level in an immutable filename records its creation name. A move keeps that
name, size, checksum, key/sequence bounds and creation order unchanged. It never
renames or copies a local/cloud object.

The optimization starts from the existing compaction source selection. It is
eligible only for a merge plan with no target inputs, complete checksummed source
metadata, disjoint selected ranges, and no conflicting current target coverage.
For L0, every remaining overlapping file must prove its smallest sequence is
strictly newer than the selected file's largest sequence. Missing bounds or
sequence authority cannot establish this proof. Complete range bounds include
range-tombstone endpoints. Overlapping sources require a merge. Sources above
the output-size or grandparent-overlap target and sources due for TTL cleanup
continue through physical compaction.

The job registry retains ownership through the existing three publication
phases. `MoveSstsPublish` records the original source identities; the
`ManifestEdit::MoveSsts` journal record revalidates live identity, order, full
target coverage and L0 sequence order immediately before changing the logical
levels atomically. This edit never emits `RemoveSst` or creates a retirement
obligation. The cloud worker mirrors the captured control cut with no SST output
receipts, uploads or copies. Source names remain protected by the move intent.

Recovery applies the journal before reconciling the move intent. A pre-edit
intent retains the original levels; an already applied edit retains the new
levels and original names. Partial or conflicting authority is rejected.
Publication uncertainty fences further compaction and retains the live objects.
No failed move uses rewrite-output cleanup on the reused source names.

Active regressions cover zero compaction readers/writers, point and scan
visibility through restart, hot B/cold A L0 overlap, unknown/range bounds,
late target conflicts, changed source order, atomic idempotent replay and
failures immediately before/after the durable move edit. Compilation is complete;
behavior execution and the #935 ingest A/B write-amplification measurement remain
pending. No throughput or write-amplification delta is claimed yet.
