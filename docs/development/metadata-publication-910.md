# Ordered metadata publication without a provider-duration gate

Issue #910 needs a publication protocol change. Releasing the existing gate
alone permits an older full control-file image to overwrite a newer flush,
and permits checkpoint truncation to race a control-file capture. The current
protocol below is now wired into background runtime publishers, with active
regressions for independent flush publication, immutable capture during a
checkpoint, pointer contention and retained GC obligations. This is a local
implementation candidate; behavioral execution, crash campaigns and provider
latency qualification remain pending. Inline startup/compatibility publishers
retain their serialized path.

The event loop applies worker-returned version edits in arrival order. Each
edit validates live input identity, source/L0 order, complete key bounds and
target conflicts against the current manifest. Workers never install their own
full manifest snapshot. The existing intent, output proof, manifest publication
and input-retirement sequence remains attached to each operation.

A provider task receives an immutable control-file cut captured by the loop.
Capture takes the manifest writer lock across snapshot/journal/intent/FORMAT
selection, excluding a concurrent checkpoint installation and journal-tail
truncation. Each admitted cut has an epoch and monotonically increasing cut
ordinal, plus its applied edit/intent boundary. The runtime retains its owned
capture and protection obligation across asynchronous completion.

The committed pointer and selected manifest lineage identify that cut. Two
tasks may upload independent immutable candidate objects and attempt pointer
CAS without holding the cache/publication mutex over provider I/O. CAS still
names the exact predecessor object identity and active writer claim. The cache
accepts only monotonic epoch/cut/generation observations; a late receipt cannot
replace a later cache entry.

| Observed selected cut | Permitted completion |
| --- | --- |
| Same exact cut and identity | Acknowledge the owned operation |
| Later cut with validated coverage of this edit and its intent obligations | Acknowledge coalesced coverage |
| Earlier cut in the same writer epoch | Rebase the newer captured cut on that exact predecessor and retry within its budget |
| Changed writer epoch or claim | Fence the operation |
| Timeout or uncertain identity | Retain the obligation and resolve by receipt/readback; never infer success |

An older cut cannot rebase over a newer selected cut. Coalesced coverage must
preserve admitted intents/frontiers or prove their terminal completion; a
scalar high-water mark alone cannot authorize dropping an obligation. A
same-epoch CAS conflict prompts coverage/rebase checks and is distinguishable
from loss of the writer lease.

The event-loop gate protects local edit/capture turns, rather than provider
round trips. Flush build/upload work keeps its owned identity and completed
build while publication is pending. An unrelated compaction cannot reset it
to `Queued`. A matching flush receipt installs only its validated edit against
the current view and releases the corresponding immutable generation.

GC must include admitted cuts in its protection protocol. A conservative first
implementation may defer reclamation while cuts are admitted. Known-retired
SST deletion, general orphan discovery and metadata-body retention each need
that edge or an equivalent explicit lineage proof. A fresh pointer and an
old image being uploaded are insufficient grounds to delete captured sources.

The deterministic regression holds compaction provider work for two seconds
while a later flush commits within 100 ms through the same real publication
path. It then releases the earlier effect and verifies no rollback, exact
intent settlement, point/scan agreement and restart. Further regressions crash
with both edits admitted, deliver stale/duplicate completions, delay a successful
CAS receipt, lose the epoch, and capture during checkpoint installation.
Performance and crash/provider qualification follow the implementation pass.
