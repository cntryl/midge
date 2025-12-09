Let's design an actor-based LSM with a cloud-native WAL and cloud-native SST persistence layer that's embeddable and predictable.

Embeddability:
The database is embedded in other applications as a library that shares the application's process. No separate server or RPC layer—just in-process API calls.

Actor core:
A central runtime/actor that owns all engine state and sequences every background action (flush, compaction, WAL upload, manifest sync, eviction).

Unified write path:
One straight pipeline: op → seqno → WAL append → memtable apply → (maybe cache prewarm) → tell runtime “flush soon”.
No random worker threads mutating engine state.

Cloud-native WAL:
WAL is conceptually separate from local disk:

local buffer / segment (fast append)

cloud durability (Azure/Wasabi/S3/etc) via runtime-scheduled upload tasks

recovery driven by manifest + WAL + compaction log, not “whatever’s on the local FS”.

Cloud-native SST layer:
SSTs live primarily in an object store:

local NVMe = cache layer

runtime decides what to pin, what to evict, what to prefetch

compaction can write directly to cloud and optionally cache locally.

Deterministic compaction + flush:
Plans and executes as tasks in the actor, with an intent log. Same workload → same sequence every time.

Modern SST format:
TLV blocks, trie + bloom + sparse index as pluggable metadata, designed to work well with both local and cloud (few big objects, not tons of tiny files).

That’s the “actor LSM + cloud WAL/SST” design in a nutshell.
What we’ve been circling around is basically the natural endpoint of that brief.
