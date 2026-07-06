// == ARCHITECTURE MASTER RULES FOR SST AUTO-TUNED INDEX ==================================
// These rules define the *correct* architecture for Midge auto-tuned SST indexing.
// All completions touching index profiling, selection, or tuning MUST follow these
// rules exactly.
//
// =====================================================================================
// MIDGE AUTO-TUNED INDEX RULES — DO NOT VIOLATE
// =====================================================================================
//
// 1. Midge automatically selects between SparseIndex and TrieIndex based on key patterns.
// 2. KeyStructureProfiler analyzes keys during SST write:
//      - Zero-copy: keys passed as &[u8]
//      - Incremental: O(1) per key observation
//      - Streaming: no full SST in memory
//      - Deterministic: same keys = same profile
// 3. KeyStructureProfile metrics:
//      - avg_shared_prefix: trie usefulness indicator
//      - max_shared_prefix: hierarchical key detection
//      - entropy: Shannon entropy of key deltas (high = random)
//      - prefix_divergence: branching factor (low = structured)
//      - common_prefix_len: all-key shared prefix
//      - key_length_variance: variable length = structured
//      - prefix_heat: N-byte prefix frequency map
// 4. IndexTuner decision engine:
//      - Use Trie if: avg_shared_prefix >= 6 AND entropy < 3.5
//      - Use Trie if: common_prefix_len >= 2 OR max_shared_prefix >= 8
//      - Use Trie if: prefix_divergence < 256
//      - Use Sparse if: entropy > 4.0 (random keys like UUIDs)
//      - Use Sparse if: prefix_divergence >= 1024 (too many branches)
//      - Use Sparse if: key_count < 128 (overhead not worth it)
// 5. IndexKind enum serialized in SST Footer (1 byte):
//      - Sparse(0): O(log N) lookup, minimal overhead
//      - Trie(1): O(prefix_length) lookup, ultra-fast prefix scans
// 6. Integration:
//      - FsSstWriter calls profiler.add_key() during block writes
//      - After all blocks, call tuner.decide(profiler.finish())
//      - Build appropriate index (SparseIndexWriter or TrieWriter)
//      - Store IndexKind in Footer
//      - FsSstReader dispatches to correct reader based on IndexKind
// 7. Strategic value:
//      - Zero-cost for random workloads (auto-selects sparse)
//      - 10-100× faster range scans for structured workloads (auto-selects trie)
//      - No user configuration required
//      - Differentiator: RocksDB/Pebble/TiKV don't have this
//
// =====================================================================================
// AUTO-TUNING GOALS
// =====================================================================================
//
// The auto-tuned index provides:
//   - Optimal index structure per SST based on key patterns
//   - Zero overhead for random keys (UUIDs, hashes)
//   - Massive gains for structured keys (paths, hierarchical IDs)
//   - Transparent to users (no configuration needed)
//   - Enables workload-specific optimizations:
//       * Document stores (user/{id}/doc/{id})
//       * Search engines (term/{word}/posting/{doc})
//       * Time-series (metric/{name}/{timestamp})
//       * Vector DB namespace routing
//
// Decision flow:
//   1. Profile keys during SST write (KeyStructureProfiler)
//   2. Compute structural metrics (avg_shared_prefix, entropy, etc.)
//   3. Apply heuristics (IndexTuner)
//   4. Build appropriate index (SparseIndexWriter or TrieWriter)
//   5. Persist IndexKind in Footer
//   6. Reader dispatches based on IndexKind
//
// =====================================================================================

pub mod profiler;
pub mod tuner;
