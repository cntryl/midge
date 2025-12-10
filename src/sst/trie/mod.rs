// == COPILOT MASTER RULES FOR SST TRIE INDEX =========================================
// These rules define the *correct* architecture for Midge Trie Index. All completions
// touching trie building, serialization, or lookup MUST follow these rules exactly.
//
// =====================================================================================
// MIDGE SST TRIE RULES — DO NOT VIOLATE
// =====================================================================================
//
// 1. The trie is a prefix-compressed radix tree mapping keys → block_ids.
// 2. Key insertion uses prefix-diff logic with LCP calculation.
// 3. TrieNode {
//      prefix_len: u16,
//      key_delta: Vec<u8>,
//      block_id: Option<u32>,
//      children: Vec<TrieEdge>
//    }
// 4. All children sorted by first byte of key_delta.
// 5. Serialization uses varint fields, compact layout.
// 6. Writer:
//      - Build trie during block boundary creation.
//      - Add key only on block boundaries (block's first key).
// 7. Reader:
//      - Trie lookup precedes sparse index lookup.
//      - Trie supports exact lookup, prefix lookup, next-key seek.
// 8. SST footer must include trie_block_handle.
// 9. Trie must remain read-only / immutable.
// 10. Do not generate recursive structures; use node index arrays.
//
// =====================================================================================
// TRIE GOALS
// =====================================================================================
//
// The trie provides:
//   - O(prefix_length) lookup vs O(log N)
//   - Smaller index than sparse-index + restarts
//   - Predictable block boundaries
//   - Prefix queries (prefix → prefix_next)
//   - Hierarchical key support (a/b/c, JSON paths)
//   - Document-path routing
//
// Placement in SST:
//   DataBlocks[]
//   BloomFilter
//   SparseIndex
//   TrieIndex    <--- NEW
//   Footer
//
// =====================================================================================

pub mod builder;
pub mod encoding;
pub mod node;
pub mod reader;
pub mod writer;

pub use builder::TrieBuilder;
pub use node::{TrieEdge, TrieNode};
pub use reader::TrieReader;
pub use writer::TrieWriter;

/// Calculate longest common prefix length
pub fn lcp(a: &[u8], b: &[u8]) -> usize {
    a.iter()
        .zip(b.iter())
        .take_while(|(x, y)| x == y)
        .count()
}

#[cfg(test)]
mod lcp_tests {
    use super::*;

    #[test]
    fn should_compute_lcp_correctly() {
        assert_eq!(lcp(b"abc", b"abd"), 2);
        assert_eq!(lcp(b"test", b"test"), 4);
        assert_eq!(lcp(b"hello", b"world"), 0);
        assert_eq!(lcp(b"prefix_a", b"prefix_b"), 7);
        assert_eq!(lcp(b"", b"anything"), 0);
    }
}
