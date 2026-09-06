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
All limits remain independent CLI arguments on the runner.

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
verification also requires zero outstanding storage reservations and reader
pins after quiescence.

## Allocation audit

The process limit owns all child allocations, including provider, codec,
allocator and thread overhead outside engine pools. Existing engine budgets
continue to constrain their respective buffers; their settings are unchanged.

| Allocation owner | Accounting and release boundary |
| --- | --- |
| Application SST readers | `ReadResources` shares a metadata budget across readers and evicts idle entries; active reader ownership retains reservations. |
| Recovery proof readers | Reader metadata, compressed/decoded blocks, decoder keys, comparison values and verification windows share one proof budget. Verified-identity map entries now reserve a table-growth allowance too. Exhaustion replays WAL; checkpoints drop readers and the map. |
| Tombstone metadata | Budgeted SST readers/writers retain metadata charges; memtable tombstones contribute to encoded admission bounds. The difficult workload verifies retention and reclamation semantics through compaction and recovery. |
| WAL replay | Frame, pending-transaction and checkpoint bounds derive from configured recovery limits. Remote inventory is streamed and remains independent of local capacity. |
| Flush | One serialized worker owns temporary construction/publication buffers; disk staging is admitted before output. Heap copies and codec workspace are included in the separately enforced process limit rather than represented as application cache capacity. |
| Compaction | Shared execution budgets cover reader/writer/cursor working sets; storage tokens cover staging. Completion, cancellation and failed publication retain authoritative inputs and release temporary reservations. |
| Transaction spill | Transaction thresholds control spill, database-local files participate in disk admission, and owner cleanup releases temporary files. Cloud-acknowledged records remain recoverable after spill cleanup or process loss. |

The new regression fills the recovery proof metadata budget with overlapping
SST identities: the previous implementation retained the map without a charge;
the corrected implementation declines proof and releases every coverage charge
at the checkpoint boundary. Native crash tests and the enforced campaigns
exercise success, cancellation and failure without changing persistent formats,
public cache settings, or single-owner publication.
