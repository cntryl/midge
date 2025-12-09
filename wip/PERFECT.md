Here is the **clean, copy-paste-ready, final optimal file structure** for Midge’s actor-driven, deterministic, cloud-native LSM.
This is the structure you should paste directly into your repo planning docs (e.g., `docs/STRUCTURE.md`).

No commentary.
No legacy.
No fluff.
Just the finalized tree.

---

# 📁 **Midge Optimal `src/` Structure (Copy/Paste)**

```
src/
├── lib.rs

├── engine/
│   ├── mod.rs
│   ├── engine.rs
│   ├── open.rs
│   ├── context.rs
│   └── api/
│       ├── mod.rs
│       ├── kv.rs
│       ├── cf.rs
│       ├── write_batch.rs
│       ├── snapshot.rs
│       ├── iterator.rs
│       ├── transaction.rs
│       ├── options.rs
│       ├── errors.rs
│       └── types.rs

├── runtime/
│   ├── mod.rs
│   ├── event_loop.rs
│   ├── state.rs
│   ├── task.rs
│   ├── scheduler.rs
│   ├── dispatch.rs
│   └── actors/
│       ├── flush.rs
│       ├── compaction.rs
│       ├── wal.rs
│       ├── cloud.rs
│       ├── gc.rs
│       └── manifest.rs

├── metadata/
│   ├── mod.rs
│   ├── manifest.rs
│   ├── version_set.rs
│   ├── version_manager.rs
│   ├── sst_catalog.rs
│   └── invariants.rs

├── wal/
│   ├── mod.rs
│   ├── traits.rs
│   ├── segment.rs
│   ├── writer.rs
│   ├── reader.rs
│   ├── index.rs
│   └── backends/
│       ├── local.rs
│       ├── hybrid.rs
│       ├── batched_sync.rs
│       └── cloud.rs

├── sst/
│   ├── mod.rs
│   ├── mutable/
│       ├── mod.rs
│       ├── segment.rs
│       └── builder.rs
│   ├── immutable/
│       ├── mod.rs
│       ├── reader.rs
│       ├── writer.rs
│       ├── block.rs
│       ├── table.rs
│       ├── index.rs
│       └── format.rs
│   ├── cache/
│       ├── mod.rs
│       ├── shard.rs
│       ├── admission.rs
│       ├── key.rs
│       ├── value.rs
│       ├── metrics.rs
│       └── policy/
│           ├── wtiny_lfu.rs
│           ├── lru.rs
│           └── clock_pro.rs
│   ├── bloom/
│       ├── mod.rs
│       ├── reader.rs
│       ├── writer.rs
│       └── factory.rs
│   ├── trie/
│       ├── mod.rs
│       ├── reader.rs
│       ├── writer.rs
│       └── factory.rs
│   └── sparse_index/
│       ├── mod.rs
│       ├── reader.rs
│       ├── writer.rs
│       └── shared.rs

├── storage/
│   ├── mod.rs
│   ├── filesystem.rs
│   ├── cloud.rs
│   ├── hybrid.rs
│   └── paths.rs

├── compaction/
│   ├── mod.rs
│   ├── planner.rs
│   ├── strategy.rs
│   ├── executor.rs
│   └── merge.rs

├── common/
│   ├── mod.rs
│   ├── codec.rs
│   ├── error.rs
│   ├── internal_key.rs
│   ├── range_tombstone.rs
│   ├── rate_limiter.rs
│   ├── timestamp.rs
│   ├── tlv.rs
│   ├── test_hooks.rs
│   └── worker.rs

├── iterators/
│   ├── mod.rs
│   ├── merge_iterator.rs
│   └── skiplist.rs

├── metrics/
│   ├── mod.rs
│   ├── block_meta.rs
│   ├── bloom.rs
│   ├── bloom_cache.rs
│   ├── cache.rs
│   ├── encoding.rs
│   ├── fast_negative_filter.rs
│   ├── file_manager.rs
│   ├── format.rs
│   ├── manifest_cache.rs
│   ├── metadata_cache.rs
│   ├── meta_index.rs
│   ├── range_tombstone.rs
│   ├── reader_common.rs
│   ├── sequential_access_optimizer.rs
│   ├── sparse_index.rs
│   ├── sparse_index_cache.rs
│   ├── table_cache.rs
│   ├── tombstone_index.rs
│   ├── traits.rs
│   └── trie_index.rs

└── testkit/
    ├── mod.rs
    ├── deterministic_runtime.rs
    ├── workloads.rs
    ├── state_snapshot.rs
    └── cloud_mock.rs
```

---

# ✔️ **Notes**

* Structure is sorted by subsystem, not by historical artifacts.
* Actor runtime (`runtime/`) becomes the owner of all state.
* Metadata has a single location.
* SST subsystem cleanly separates mutable/immutable/cache.
* WAL subsystem isolates backends under `backends/`.
* `engine/api/` is broken into small, manageable files.
* `testkit/` becomes a first-class testing harness.

