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

Capture admission uses the existing shared maintenance `ResourceBudget`.
The runtime stats the four files under the writer lock, then reserves their
encoded/copy/decode allowance before allocation. A separate 64-unit slot budget
bounds owned cuts. Both admissions preserve originating-pool contention across
error delivery; an already released competing charge still permits retry.
Accepted publication/build ownership and its original deadline survive that
retry. Oversized working sets retain their objects and intents and fail closed.

The cut carries SHA-256 of exact file presence, lengths and bytes, plus bounded
direct witnesses for earlier still-owned cuts in the same epoch/session.
Normalized publication identities permit forward phase progress. Selecting an
`OutputDurable` cut cannot discharge its operation's intent: a later clear needs
an exact selected `ManifestPublished` receipt at a non-regressing journal cut.
Those receipt records retain their source accounting while still needed by an
owned cut and are pruned as ownership ends.

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

Validated immutable PUTs leave a reduced staged descriptor with their owned
capture. A fresh pointer matching that exact generation/object closure proves
selection without allocating a second full body image. This lets lost-CAS
readback/coalescence use the capture's already admitted workspace in a small
shared pool. A cold external predecessor instead requires proportional shared
read/decode admission. Transient contention retries within the original budget;
an unavailable combined working set retains the obligation and reports expiry
or resource exhaustion. It does not grow the pool or infer a successful effect.
Immutable PUT and full-GET request callbacks retain their reservations until
the actual physical request ends, including after caller timeout or later-cut
coalescence.

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
intent settlement, point/scan agreement and restart. The background fixture
also hydrates the selected later cut into an empty cache before releasing the
earlier effect: exact unselected compaction output names must remain absent
from both hydrated and reopened authority. Variants delay/duplicate or lose an
actual successful pointer-CAS callback while the later flush selects.
This is cold/cache-loss recovery evidence, not a process-kill campaign.
Other active cases cover lost epochs, checkpoint installation, exact staged
readback with no second allocation, slot/byte contention and physical callback
charge release. These new behaviors remain unexecuted in the code-first pass.
Performance and crash/provider qualification follow the implementation pass.
