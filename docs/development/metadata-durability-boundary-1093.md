# Metadata durability boundary and FORMAT 9 upgrade (#1093)

**Status:** protocol proposal for review. No new format, migration command, or
recovery behavior is implemented by this document. The selected product
direction is a crash-safe, explicit FORMAT 9 upgrade. Ordinary `Engine::open`
must never upgrade a database implicitly.

This design addresses ambiguous append recovery in both the manifest journal
and intent log. FORMAT 9 stores framing, length, payload and checksums in the
same append stream. If an interrupted write is zero-extended to its advertised
extent, the bytes can have the shape of a complete frame with a bad checksum.
That does not prove an interruption: damage to multiple fields of a durable
final record can have the same shape. Strict recovery must continue to reject
ambiguous FORMAT 9 evidence. This protocol follows the direction recorded in
[issue #1093](https://github.com/cntryl/midge/issues/1093) and the
[crash-safe upgrade decision](https://github.com/cntryl/midge/issues/1093#issuecomment-7077156336).

## Contracts

1. A selected authority head identifies the exact immutable bytes that form
   durable manifest and intent history. Bytes outside that selection are not
   committed, even when a partial append happens to fill its announced length.
2. A durable data object is published before a head may refer to it. The head
   is the acknowledgement boundary: return success only after the head is
   durably selected, or after exact readback proves that selection.
3. Corruption inside the selected closure is an error. Recovery never falls
   back to a previous head or silently drops a selected edit.
4. FORMAT 9 recovery keeps its current strict rules. A v9 source is eligible
   for upgrade only after the existing strict readers validate it. In
   particular, CRC failure in a complete-extent final frame and ambiguous
   multi-field damage remain hard errors.
5. Upgrade preserves the original v9 files and selected cloud generation.
   Before the authority switch, retry revalidates the current v9 source.
   After the switch, only the selected v10 authority may be opened. Rollback
   after v10 writes begin uses a pre-upgrade backup.
6. Checkpoint and compaction select new generations. Old referenced bytes
   remain available until safe collection.

## Proposed v10 wire and authority layout

FORMAT 10 keeps the existing `FORMAT` filename and uses the exact marker
`midge-format-version=10\n`. It introduces a dedicated v10 metadata codec and
an immutable generation namespace. The v9 codec remains available only for
strict reads and the explicit migration path.

A generation contains a checkpoint base plus zero or more immutable append
segments for each stream:

```text
metadata/generations/<generation-id>/manifest.base
metadata/generations/<generation-id>/manifest/<segment-id>
metadata/generations/<generation-id>/intent.base
metadata/generations/<generation-id>/intent/<segment-id>
metadata/HEAD
```

`HEAD` is the sole local selector. Its bounded, checksummed `MDGEHD10` frame
contains:

- protocol version and monotonically increasing head sequence
- generation ID and digest of the predecessor head for stale-writer checks
- manifest base object name, exact byte length, CRC32C and SHA-256, plus its
  checkpoint edit ID
- the ordered manifest segment references, each with exact length, CRC32C,
  SHA-256, predecessor digest and inclusive edit-ID range
- intent base object name, exact byte length, CRC32C and SHA-256, plus its
  last sequence and checksum
- the ordered intent segment references, each with exact length, CRC32C,
  SHA-256, predecessor checksum and inclusive sequence range
- the highest manifest edit ID and intent sequence represented by the
  selected closure.

Each immutable segment contains one complete logical append operation. A
manifest segment carries its edit envelope(s) and terminal fsync marker. An
intent segment carries its suffix edit. The selected head makes the whole
segment durable. Decoders verify the segment frame, checksum, stream identity,
predecessor, identity range and semantics before applying it. The terminal
manifest marker remains an internal consistency check. The selected head
provides independent evidence that distinguishes a committed append from an
interrupted one.

The names in `HEAD` are canonical generation-relative names, never arbitrary
paths. A local reader resolves only those names and verifies every declared
length and checksum. Unreferenced files and partial staging files have no
authority.

### Bounded closure and checkpointing

Use the existing shared metadata limits as the initial v10 bound: at most 16
selected immutable metadata objects and at most 64 MiB of encoded metadata in
one selected closure. Cap the encoded head at 64 KiB. A writer must checkpoint
before an append would exceed either closure limit. It must reserve the
required encoded and decode workspace before constructing a replacement
closure. If admission or durable publication fails, keep the current head and
all referenced objects. Report the failure without acknowledging the append.

A manifest checkpoint writes a new complete manifest base. Intent compaction
writes a new complete intent base. Both are staged under a fresh generation
identity, verified, then selected with one head replacement. Per-closure
bounds alone do not bound cumulative retained generations. Require a retention
and admission policy before v10 release. Keep at most four complete closures:
the selected generation and up to three predecessors. The v9 migration source
and active capture pins occupy predecessor slots. Four closures at up to
64 MiB and 16 immutable objects each yield aggregate caps of 256 MiB and 64
objects. If a pin or migration source would exceed either cap, block another
checkpoint or compaction until the retained closure can be reduced. A backup
is a separate copied artifact and does not hold live database objects after
capture finishes.

Unselected objects left by an interrupted append or migration count against
the same retained-byte and object budgets. The pending cloud migration pins
both its source and candidate closures through takeover and finalization.
Under the writer lease, a bounded collector may remove only objects it
proves are outside the selected closure and every active pin. If a bounded
scan cannot prove the state or reclaim enough space, retain the objects and
block new metadata publication with a resource error. Never fall back to
unbounded listing or delete candidates on age alone.

## Append and checkpoint lifecycle

For local storage, the existing manifest writer lock serializes head updates.
The engine's primary lease excludes another Midge writer. An append follows
this order:

1. Read and validate the selected head. Validate the requested edit and
   reserve the bounded segment/head workspace.
2. Write a new uniquely named immutable segment. Sync the segment and each
   affected directory, including the parent of any newly created directory.
   Verify the bytes that were persisted.
3. Construct a head whose predecessor digest equals the head read in step 1,
   and whose selected closure includes the new segment.
4. Stage `HEAD` with the existing write, file-sync, atomic-rename,
   parent-directory-sync lifecycle. The staged replacement is the commit
   point.
5. Return success only after step 4 completes. If it reports an error after
   rename, target visibility only proves current namespace selection. It does
   not prove the directory entry is crash-durable. Reread and validate the
   candidate, successfully sync the parent directory, then reread and validate
   the candidate again before acknowledging it. Exact predecessor selection
   means the append did not commit and may be retried. Another head fences
   this attempt. If readback or directory sync cannot prove durability, return
   an unknown outcome and preserve both generations.

Checkpointing and intent compaction use the same selector protocol: write and
verify the replacement base first, then stage one head that refers to the new
base and any retained suffix. A crash before selection leaves the old head and
old bytes authoritative. A crash after selection leaves the new base
authoritative. Old bytes are not truncated as part of the switch.

For provider cloud, the lease store's committed metadata pointer remains the
remote selector. The v10 pointer closure must identify the exact immutable
head, bases and segments with the existing object identity, length and
checksum checks. Cloud metadata capture, chain decoding, retention, orphan
sweeping and cache hydration must recognize the v10 closure as one versioned
protocol. Extend the v9 filename allowlist explicitly for v10 objects. The
pointer CAS is the remote acknowledgement boundary. Validate the candidate
closure before CAS. On a lost response, read the selected pointer and accept
only the exact candidate under the active lease epoch and owner. CloudSim uses
the same remote-pointer rule. Its local cache is derived state and must be
rehydrated from the selected generation after interruption or cache loss.

## Explicit v9 upgrade

The proposed maintenance entry point is an explicit pre-open operation, for
example `Engine::upgrade_format_9(options, timeout)`. It acquires the existing
primary lease and returns without starting a runtime. The exact public name
and report type need API review. `Engine::open` never invokes migration
implicitly. A verified pre-upgrade backup is required for rollback after v10
writes begin. The upgrade operation does not require a backup artifact when
the v9 source validates.

### Local upgrade

1. Acquire the local primary lease before reading or changing authoritative
   metadata. Capture the v9 marker and source bytes under the manifest writer
   lock. Hash the exact source snapshot, journal and intent bytes.
2. Run current FORMAT 9 Strict validation: snapshot decode and semantic
   validation, journal replay through the last verified durability marker,
   and intent sequence/predecessor/checksum validation. Strict success alone
   is insufficient because current replay can accept some malformed
   overrun tails while returning only the verified prefix. Require a clean
   verified end: the final journal marker must verify at EOF, and every intent
   byte must belong to a valid chained frame. Reject any ignored suffix,
   including a partial or zero-extended tail, unless a separate
   version-specific verifier supplies independent evidence for its exact
   committed boundary. No such verifier is defined yet. Any CRC, length,
   framing or semantic ambiguity aborts without changing FORMAT, source files
   or the selected authority.
3. Build a fresh immutable v10 generation from the validated logical state.
   Sync all files and directories. Verify exact lengths, checksums, replay,
   manifest invariants and intent invariants. Recheck the source digest while
   the lease and lock are held.
4. Stage and durably publish the v10 `HEAD`. While `FORMAT` remains 9, v9
   remains the selected protocol and the new head is ignored.
5. Replace and sync the `FORMAT` marker last. This is the local protocol
   switch. If publication reports an error after rename, reread and validate
   the v10 marker and head, successfully sync the database directory, then
   reread and validate both before acknowledging the switch. After a crash,
   reread them: v9 means revalidate the current v9 source and rebuild if its
   digest changed. For v10, validate `HEAD` and roll forward. A legacy writer
   may have advanced v9 after an interruption before the switch, so a staged
   head is reusable only when its source digest still matches. Never change a
   v10 marker back automatically. If the marker or selected head cannot be
   resolved, fail closed and preserve all files.

### Cloud compatibility gate and upgrade

Released v0.5.3 can make shared mutations after acquiring a lease. Its
startup acquires a lease, then `materialize_cloud` and
`materialize_simulated_cloud` call `fence_cloud_ddl_scoped` and
`prepare_wal_catalog_within` before `bootstrap_provider_metadata_scoped`
reads and hydrates the selected FORMAT. The migration must reject v0.5.3 at
lease acquisition before either startup operation can mutate shared state.

Locally, an old binary can acquire the filesystem lease and run orphaned
transaction-spill cleanup before storage recovery validates FORMAT. It then
fails on FORMAT 10 before manifest/WAL replay or runtime startup. The
compatibility regression must assert that authoritative manifest, head, WAL
and SST bytes remain unchanged. The protocol permits that lease bookkeeping
and ephemeral cleanup.

The cloud path needs a persistent legacy-writer compatibility gate before
pointer cutover:

1. Read and strictly validate the exact selected v9 metadata generation under
   the acquired v4 lease. Verify object identities, lengths and checksums.
   Verify the strict v9 manifest and intent state. Keep the selected v9
   pointer unchanged.
2. Stage and verify a complete v10 candidate generation. Do not publish it as
   selected yet.
3. Conditionally replace the lease document with a new, strictly versioned
   lease protocol (proposed version 5), preserving holder, owner and epoch,
   and recording a pending upgrade with the migration ID, exact selected v9
   generation identity, exact staged v10 generation identity, source digest
   and target protocol. The pending state binds immutable generation
   identities. Pointer ownership and the provider's CAS token can change on
   takeover. The released v0.5.3 parser recognizes lease versions 3 and 4.
   Verify that it rejects version 5 as malformed before writing a lease
   document or activating lease validity. That rejection must prevent its
   later DDL-registry and WAL-catalog mutations.
4. Compare the current pointer's selected generation to the exact v9
   predecessor. CAS to the verified v10 candidate using the current
   pointer's conditional token under the still-valid v5 lease and owner. The
   candidate pointer selection is the cloud metadata switch. Retain the v5
   compatibility gate permanently so older binaries fail during acquisition
   before their startup mutations.
5. Verify the selected v10 candidate and conditionally transition the lease
   from pending to active FORMAT 10 under the current holder, owner and
   epoch. Active state keeps the v5 gate permanently and permits normal v10
   pointer advancement. Hydrate the local cache from the exact selected v10
   generation and verify it before startup recovery proceeds. Cache files
   are derived state. A restart after partial hydration repeats this step.

Every lease rewrite preserves the compatibility version and migration state,
including renewal, normal release, cleanup of an ambiguous lease operation,
and expired-owner takeover. Release conditionally expires the lease document
in place. Takeover increments the epoch, acquires the lease with CAS, and
claims the metadata pointer while preserving the selected generation. A
stale owner cannot change the gate or publish through the new owner's
pointer token. Initial gate installation and upgrade completion are the only
migration-state transitions. A missing initialized lease or pointer fails
closed.

After acquiring the lease and claiming the pointer, resolve the gate and
selected generation before DDL-registry fencing, WAL-catalog fencing, or any
normal metadata publication:

| Lease state | Selected generation | Startup action |
| --- | --- | --- |
| v4 | Valid FORMAT 9 | Open FORMAT 9 or start an explicit upgrade. |
| v5 pending | Exact v9 predecessor | Verify the persisted candidate and resume its CAS using the current owner's pointer token. |
| v5 pending | Exact v10 candidate | Verify the selected closure and finalize the lease state. |
| v5 active FORMAT 10 | Valid FORMAT 10 | Open the selected closure and hydrate the cache. |
| v4 | FORMAT 10 | Fail closed and preserve the metadata. |
| v5 pending or active | Any other selection | Fail closed and preserve the gate and metadata. |

The pending-state rows also govern readback after a lost pointer CAS
response. If readback is unavailable, preserve the generations and report
an unknown outcome. Resolve an uncertain lease-state write by reading the
lease again and checking the current owner, epoch and migration state before
proceeding. A new owner runs the same startup checks after takeover.

## Compatibility and backup rules

- Keep the FORMAT 9 decoder and strict interpretation unchanged. New v10
  paths must not weaken the legacy reader or treat bad complete-frame CRCs as
  interrupted writes.
- Preserve v9 files and the committed v9 cloud generation across the switch.
  After v10 writes begin, readers select v10 authority only.
- The v10 reader must reject unsupported future protocol versions and a
  missing, malformed, regressed or inconsistent head. Do not guess whether a
  boundary is stale or incomplete.
- Existing backups remain independent rollback points. A pre-upgrade backup
  restores a FORMAT 9 database. After v10 writes begin, rollback means
  restoring that backup and reconciling post-backup application writes.
- Backup capture must pin the selected head and exactly its referenced
  generation closure under the same barrier. Do not copy unselected orphans as
  authority. Restore verifies the head and closure before publishing the
  target. Extend `pin_durable_files` and CloudSim capture to include the v10
  head and selected objects. Keep `backup_version` unchanged only if the
  inventory schema and interpretation remain identical. Otherwise, version
  the backup artifact format separately from the database FORMAT.
- Cloud body capture, selected-cut digest/proof, resource admission, pointer
  generation validation, metadata-chain decoding, hydration, retention,
  orphan sweep and cache-loss recovery must all include the exact v10 closure.
  The v9 reader remains version-specific. Do not relax the current-format
  byte check in the ordinary cloud reader to make migration pass.

## Fault and recovery matrix

| Failure point | Selected authority | Required recovery |
| --- | --- | --- |
| v9 strict validation rejects ambiguous journal or intent bytes | v9 | Abort. Source bytes, FORMAT and cloud pointer stay unchanged. |
| Local generation write/sync fails before `HEAD` | v9 FORMAT and v9 files | Ignore unselected staging. Retry only after revalidating source. |
| Local `HEAD` durable, before FORMAT switch | v9 FORMAT | Old reader remains selected. Revalidate v9 and rebuild if its source digest changed. |
| Local FORMAT rename/sync outcome is uncertain | v9 or v10 | Read back marker and head, successfully resync the directory, then read back and validate both again. Retry v9 or validate/continue v10. Otherwise fail closed. |
| Cloud candidate upload fails before lease v5 gate | v9 pointer, v4 lease | Old release may continue. Candidate objects are unselected. Revalidate exact v9 source before retry. |
| Cloud lease v5 gate durable, before pointer CAS | v9 pointer, v5 gate | Old release cannot acquire. New binary resumes exact migration. Do not roll back the gate implicitly. |
| Cloud pointer CAS loses response | v9 or v10 pointer, v5 gate | Exact readback resolves candidate success or predecessor retry. Other/unknown state remains fenced. |
| Cloud pointer selected, lease finalization interrupted | v10 pointer, v5 pending or active | Read the current gate. Verify the exact candidate and finalize if pending, then hydrate. |
| Cloud cache hydration interrupted | v10 pointer, v5 active | Rehydrate exact selected closure on next new-binary startup. |
| Append segment durable, before head publication | Previous head | Segment is an orphan, not committed. Retry under writer authority. Do not replay it by listing. |
| Head publication outcome uncertain | Previous or candidate head | Exact head readback resolves. Otherwise unknown outcome and retain both. |
| Selected segment/head corruption | Corrupt selected head | Fail closed. No fallback to predecessor or FORMAT 9. |
| v10 write after upgrade followed by requested rollback | v10 head | Restore pre-upgrade backup and reconcile later application writes. Never rewrite FORMAT to 9. |

Every tail test must pair proven interrupted append cases with final and
mid-log durable corruption. For v9, drive the actual journal and intent
writers through cuts at header, body, checksum and zero-extension boundaries,
including a zero extension that reaches the advertised extent. Add a
v9-parser-success case with an ambiguous damaged final length and checksum.
Verify the migration validator refuses it without rewriting source bytes or
selecting a v10 generation. Preserve the strict verified prefix and require
independent boundary evidence before migrating any v9 suffix.

## Staged implementation and qualification

1. **FORMAT 9 source qualification.** Complete #1094 first: generate and
   preserve local and CloudSim fixtures with the published v0.5.3 crate,
   including multiple families, WAL/ledger, nonempty manifest journal, SST
   state and selected cloud metadata. Verify observable state, write/flush,
   and reopen before using them as upgrade sources.
2. **Protocol codec and local head.** Add v10 head/base/segment codecs and
   strict readers while preserving v9 fixture bytes and behavior. Implement
   local append, checkpoint, intent compaction and every-cut/corruption
   regressions before enabling migration.
3. **Explicit local upgrade.** Add the pre-open opt-in operation, strict v9
   source validation, immutable staging, marker-last switch, retry and
   uncertain-switch tests. Exercise v0.5.3 open attempts against the resulting
   local fixture. Assert v10 authority bytes, WAL and SST state are unchanged
   by a rejected old binary.
4. **Cloud gate and pointer migration.** Implement lease v5 upgrade/resume and
   cloud pointer CAS, early compatibility validation before DDL/WAL fencing,
   exact lost-CAS readback, cache hydration after interruption and cache-loss
   recovery. Inject renewal, release, and expired-owner takeover before the
   pointer switch and before lease finalization. Assert the persisted gate
   survives, the new owner resumes the exact candidate, and the stale owner
   cannot publish or clear the gate. Use the released v0.5.3 binary against
   both pending and active fixtures and assert acquisition rejects before
   remote DDL registry or WAL catalog changes. Exercise both simulated cloud
   and provider-backed topology.
5. **Backup, restore and qualification.** Capture/restore selected v10
   closures, preserve pre-upgrade backups, test rollback by restoring that
   backup after v10 writes, then run the crash campaign and provider
   qualification. Do not advertise v10 support or migration completion before
   local, CloudSim, cloud cache-loss, old-binary exclusion and backup/restore
   gates pass.

Implementation, crash-campaign and provider qualification are tracked in
#1093.
