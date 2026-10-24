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
Cloud flush publication first selects its exact `ManifestPublished` intent,
then captures and selects its clear before terminal settlement. The two mirror
effects have distinct physical phase identities and share the original absolute
deadline. A delayed first-phase completion cannot settle the clear. This also
lets an earlier capture retain a failed flush's `OutputDurable` intent while
the same immutable retries, without weakening the selected terminal proof.

A compact selected witness survives the end of its source task while that
terminal intent can appear in a later owned cut. It records normalized intent
digests and the exact selected epoch, session, cut digest and journal horizon.
The cache installs it only after the matching body has selected; a locally
durable terminal phase alone grants no receipt. Partial clears retain witnesses
still needed by earlier owned sources.

Compact witnesses use the existing maintenance pool. Their charge is split
from the admitted source workspace rather than retaining a full old body. For
the compact layout, UUID sessions are inline and sorted intent digests use one
exact boxed slice. The charge includes that slice, the witness struct, both Arc
headers and the reservation struct plus its owned parent-reservation boxes.
It also includes three exact receipt-entry payloads per intent for the sorted
boxed receipt registry, including when final-source Drop cannot obtain the
registry mutex. Batch merge and binary-search lookup avoid per-entry copying.
The allowance covers old entries, a merged candidate and final-slice
replacement; removed receipt charges stay alive until old storage is freed.
No bound depends on the standard library's private tree layout.
For `E` encoded control bytes, `16E + 4096` is the minimum admission for raw
copies before typed preflight. It includes at most `4E` of a shared selected
witness and `4E` of a newly prepared witness; that arithmetic alone is not a
typed-decoder bound. Borrowed visitors count the accepted enum slots, nested
owned fields and replay-vector peaks before allocating those structures.
Admission grows the independent source charge in the same maintenance pool
when its concrete phase bound is larger. A refusal retains the original
control files and publication obligations.

Manifest replay, intent replay, normalization and physical publication have
separate peaks. Normalization owns the retained intent state and encodes one
publication at a time after the decoded append vector has dropped. Legacy
scalar intents contribute no publication guards or compact witnesses. Reduced
authority counts families/frontiers rather than discarded SST inventories.
Cold selected-body reconstruction also admits its typed decode before loading
the manifest; the raw GET keeps its original physical source reservation.
Oversized witness arrays and control-change maps are rejected before their
announced collection storage is allocated.

Larger shared caches receive no credit. Credit is exclusive and remains with
the last actual callback reservation owner. These bounds model requested
control-body allocations for the locked Serde implementation and supported
Rust collection behavior; native transport stacks and other runtime resources
retain separate charges. They are not a whole-process RSS cap.
Both immutable PUT and its exact fallback GET carry the source reservation.
Logical timeout or later-cut coverage cannot release the source charge while
the real readback body/callback remains physically outstanding.

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
charge release. Focused behavioral tests have now run; full provider and
performance qualification retain their separate evidence requirements.
