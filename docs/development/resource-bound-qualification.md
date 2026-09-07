# Resource-bound cloud qualification

`scripts/qualify_resources.py` runs the operational recovery campaign on a
Linux host with cgroup v2, Docker, ext4 tools, a system trust store, and root
permission to mount its own temporary loop filesystem. Sqrzl runs separately.
The controller constructs the fixture outside the engine container. Database
files reside on the finite ext4 image; logs, configuration, acknowledgment
ledgers and evidence reside outside it. The image is fully allocated on disk.

The engine child has a read-only root filesystem, no extra capabilities, a
finite process count and a finite memory cgroup with swap disabled. The runner
reads back `memory.max` and `memory.swap.max` before allowing engine startup.
It samples the cgroup's memory peak and filesystem occupancy externally. The
engine still emits its own diagnostic counters; their overhead is included in
the process limit. The child does not run the filesystem sampler in this mode.
The host trust store is mounted read-only for the native provider HTTP client.

## Profiles and evidence

Dispatch `cloud.yml` with `resource_profile=reduced` (the default) or `full`.
The profiles are qualification fixtures, not supported production maxima.
All limits remain independent CLI arguments on the runner. Hosted evidence is
stored and uploaded from a directory unique to the workflow run and attempt,
so restored build caches cannot mix older campaigns into the current artifact.
Each phase also identifies the qualified revision.

| Setting | Reduced | Full |
| --- | ---: | ---: |
| Disk image | 64 MiB | 2 GiB |
| Engine pools | 64 MiB | 256 MiB |
| Process cgroup | 384 MiB | 1 GiB |
| Cloud WAL fixture | 128 MiB | 5 GiB |
| Source value size | 8 KiB | 32 KiB |

Engine disk admission is derived after formatting from `statvfs` usable bytes,
less a filesystem working allowance of the greater of 4 MiB or 5 percent.
`resource-contract.json` records the image size, actual usable capacity,
allowance, resulting admission limit, engine pools, cgroup limit, image identity
and revision. Each phase records cgroup limit readback, observed memory peak,
peak excess over the engine pool setting, exit outcome, OOM state, external
termination and observed filesystem occupancy. Peak excess is not an estimate
of every allocator overhead byte: pools may be partly unused and the cgroup
also includes kernel-charged cache. Both configured limits and observations are
reported so an engine pool setting cannot be mistaken for an RSS ceiling.

The filesystem and cgroup enforce their limits continuously. Observed peaks
are sampled evidence and can understate the final peak if a cgroup disappears
between samples. Docker's OOM outcome is checked independently. No successful
profile may silently accept an OOM kill, missing enforcement readback or failed
verification. The runner preserves evidence and removes only its own labeled
containers and temporary mounted image on completion or failure.

## Workload and recovery contract

The original complete source-value and keyset checks remain. Three additional
families receive eight overlapping generations, repeated overwrites, point
and range tombstones, and expiring values. A deterministic TTL clock advances
before final verification and starts advanced in fresh verifier processes;
lease clocks remain real. Scans run during flush and compaction and reject torn
values or mixed generations within one family snapshot. Atomic generation
values scale with the profile's disk window rather than exceeding the smallest
profile's indivisible-transaction admission limit.

Four further families exercise simultaneous writers with incompressible values,
eight overwrite rounds, point deletes and 64 older keys covered by dense range
tombstones. The value size derives from the memtable target and engine budget
(512 KiB reduced, 2 MiB full). The first 32 SST uploads are delayed by 100 ms.
Concurrent scans validate complete values during maintenance. The existing
cloud scheduler serializes flush and compaction turns: qualification requires
both to complete and rejects observed overlap. `flush-stress.json` records
acknowledged transactions, scans, delayed uploads, completed maintenance counts
and overlap samples. Fresh
processes check every live value, every deleted key and the complete keyset after
capacity restoration and repeated local-state loss.

The runner interrupts recovery at a durable checkpoint, removes local state,
then separately sends SIGKILL to another child at a checkpoint. It verifies
all fixture records after fresh-process recovery. After acknowledged workloads,
it opens another child, fills the filesystem until the kernel returns ENOSPC,
and maintains that pressure while the child attempts cloud-strict writes.
Every acknowledged and attempted operation is recorded outside the constrained
filesystem. Exhaustion must produce bounded backpressure or an explicit error.
After restoring capacity, fresh processes verify every acknowledged write and
allow only the one explicitly recorded uncertain operation, if it reached
durability before its error. Another complete local-state loss follows. Final
verification waits for a live idle runtime snapshot before checking storage
reservations and reader pins. Active compaction or flush charges are not leaks;
once all observable maintenance work is idle, any remaining reservation or pin
fails qualification. This bounded wait shares the phase deadline, and the child
must still shut down successfully after the snapshot.

## Allocation audit

The process limit owns all child allocations, including provider, codec,
allocator and thread overhead outside engine pools. Existing engine budgets
continue to constrain their respective buffers; their settings are unchanged.

| Allocation owner | Accounting and release boundary |
| --- | --- |
| Application SST readers | `ReadResources` shares a metadata budget across readers and evicts idle entries; active reader ownership retains reservations. |
| Recovery proof readers | Reader metadata, compressed/decoded blocks, decoder keys, comparison values and verification windows share one proof budget. Verified-identity map entries now reserve a table-growth allowance too. Exhaustion replays WAL; checkpoints drop readers and the map. |
| Tombstone metadata | Budgeted SST readers/writers retain metadata charges; memtable tombstones contribute to encoded admission bounds. The difficult workload verifies retention and reclamation semantics through compaction and recovery. |
| WAL catalog and cleanup metadata | Runtime configures one maintenance budget shared with compaction and retained WAL proofs. Control reads admit bodies before pinned ranges; decoded catalogs, serialization, provider copies and metadata snapshots retain charges through their owners. Transient contention keeps accepted WAL and its waiter in the existing retry queue. Completed retirement releases its manifest snapshot. |
| WAL replay | Frame, pending-transaction and checkpoint bounds derive from configured recovery limits. Remote inventory is streamed and remains independent of local capacity. |
| Flush | One serialized worker shares an internal allowance across streaming construction, final metadata validation, upload and bounded identity-pinned readback. Disk admission precedes scratch creation; unconfirmed scratch cleanup retains its token and blocks further builds. Timed-out uploads retain heap charges until underlying ownership ends. |
| Compaction | Shared execution budgets cover readers, writers, cursors and admitted file publication. Remote partition targets leave space for upload copies and live merge state; actual admission remains checked. Both cache modes prepare identity guards in the worker before single-owner installation. Timed-out providers retain their memory charges; failed publication retains authoritative inputs. |
| Transaction spill | Transaction thresholds control spill, database-local files participate in disk admission, and owner cleanup releases temporary files. Cloud-acknowledged records remain recoverable after spill cleanup or process loss. |

The new regression fills the recovery proof metadata budget with overlapping
SST identities: the previous implementation retained the map without a charge;
the corrected implementation declines proof and releases every coverage charge
at the checkpoint boundary. Native crash tests and the enforced campaigns
exercise success, cancellation and failure without changing persistent formats,
public cache settings, or single-owner publication.

The internal flush allowance is four times the greater of the configured
memtable target and one eighth of the resolved engine memory budget, plus 1 MiB,
with saturating arithmetic and checked admission. One eighth matches the
existing replay memory ceiling; a small flush target therefore does not become
an accidental maximum record size. Engine startup passes this resolved allowance
to both replay and the runtime worker before post-start configuration updates. It is a scratch/copy
allowance derived from admitted input, not a change to the engine pool setting
or a promise that process memory equals that setting. Construction charges one
key's version references, writer metadata, codec buffers and boundary keys;
epoch pins are dropped before scratch I/O. Upload charges the source allocation
and conservative provider copy workspace before reading the file. Native upload
retries share an immutable transport body, and completion adapters separately
charge their thread stacks. Identity-pinned readback uses 64 KiB windows; no
second verification SST is created outside the database filesystem. Both cache
modes keep conditional creation and the existing lease, intent and manifest
publication barriers. Admission failure retains recoverable data.

Flush and compaction share immutable-file publication. A publication attempt uses
one storage timeout across its sequential HEAD, conditional PUT and 64 KiB
identity-pinned readback operations. The returned guard binds the verified remote
identity; compaction revalidates it before installing replacements. Persistent
cache mode retains local output, while ephemeral mode removes it only after
successful publication. Format validation streams through the SST layer and CRC
calculation uses fixed stack space; neither path needs an external temporary SST.

Remote compaction rollover derives from the existing pool after subtracting the
fixed publication workspace, leaving half the remaining space for live merge
state and indivisible keys. The target is soft: oversized keys are never split,
and checked admission can still reject a partition while keeping input authority.

### Catalog and cleanup admission

Catalog decoding reserves a conservative envelope before materializing its tree.
Serialization first counts the unchanged JSON representation without allocating a
buffer, then reserves the exact encoded length. Conditional publication retains
provider workspace through late completion and verifies its readback with pinned
ranges. Filesystem CAS accepts the same version identity as its range reader,
under the existing process and mutation locks.

Cleanup admits manifest decoding and compares local metadata against bounded
remote ranges while holding the publication lock. After exact comparison, only
provider identity guards and the admitted decoded manifest remain. Retries reuse
that manifest and its reservation after exact metadata revalidation. Changed
metadata releases stale proof ownership before replacement admission. Local
simulation likewise reuses unchanged SST coverage. WAL parsing sizes its transient
workspace from the remaining shared allowance, so retained metadata larger than
half the pool can still make progress. The final WAL retirement releases the
cached manifest; memory pressure may discard idle proof work and recompute it
conservatively.

These changes add no catalog-entry limit or persistent format. A catalog that
cannot fit even without competing maintenance returns a resource error; it is
not silently truncated. Temporary competition with compaction retains cloud
waiters and sealed WAL for bounded retry. This admission work does not establish
an arbitrary-inventory streaming catalog format or OS process-limit qualification.
