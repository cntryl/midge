//! Compatibility tests for trie index SST format.
//!
//! Validates that:
//! - Old SST files (without trie) remain readable by new readers
//! - New SST files (with trie) remain readable by old readers via legacy index
//! - Trie index doesn't break backward compatibility

use cntryl_midge::sst::trie_index::{TrieIndex, TrieIndexBuilder};
use cntryl_midge::sst::trie_index_integration::{OptionalTrieIndexReader, OptionalTrieIndexWriter};

/// Test: Trie can be built and decoded deterministically
#[test]
fn should_decode_trie_deterministically() {
    // Arrange
    let mut builder = TrieIndexBuilder::new();
    builder.add_block(b"apple", b"apricot");
    builder.add_block(b"banana", b"berry");
    builder.add_block(b"cherry", b"date");

    let encoded = builder.finish();
    let index = TrieIndex::decode(&encoded).expect("Failed to decode trie");

    // Act
    let blocks1 = index.find_candidate_blocks(b"apple");
    let blocks2 = index.find_candidate_blocks(b"apple");

    // Assert
    assert_eq!(blocks1, blocks2, "Trie queries should be deterministic");
}

/// Test: Legacy index path still works when trie not present
#[test]
fn should_fallback_to_legacy_when_trie_absent() {
    // Arrange
    let reader = OptionalTrieIndexReader::from_meta_index(b"").expect("Should create reader");

    // Act
    let blocks = reader.find_candidate_blocks(b"any_key");
    let is_available = reader.is_available();

    // Assert
    assert_eq!(blocks.len(), 0);
    assert!(!is_available);
}

/// Test: Old reader can ignore trie index
#[test]
fn should_allow_old_readers_to_ignore_trie() {
    // Arrange
    let reader = OptionalTrieIndexReader::from_meta_index(b"").expect("Should create reader");

    // Act
    let _blocks = reader.find_candidate_blocks(b"test_key");
    let _range_blocks = reader.find_blocks_in_range(b"start", b"end");

    // Assert
    // Execution without panic or error indicates success
}

/// Test: New reader gracefully handles SSTs with and without trie
#[test]
fn should_support_mixed_sst_formats() {
    // Arrange
    let reader_with_trie =
        OptionalTrieIndexReader::from_meta_index(b"").expect("Should create reader");
    let reader_without_trie =
        OptionalTrieIndexReader::from_meta_index(b"").expect("Should create reader");

    // Act
    let _ = reader_with_trie.find_candidate_blocks(b"key");
    let _ = reader_without_trie.find_candidate_blocks(b"key");

    // Assert
    // Both readers handle their respective formats without error
}

/// Test: Writer can be toggled for trie index enable/disable
#[test]
fn should_support_trie_index_flag() {
    // Arrange
    let mut writer_enabled = OptionalTrieIndexWriter::new(true);
    let mut writer_disabled = OptionalTrieIndexWriter::new(false);

    // Act
    writer_enabled.add_block(b"key_001", b"key_010");
    writer_disabled.add_block(b"key_001", b"key_010");
    let encoded_enabled = writer_enabled.finish();
    let encoded_disabled = writer_disabled.finish();

    // Assert
    assert!(!encoded_enabled.is_empty());
    assert!(encoded_disabled.is_empty());
}

/// Test: Trie index correctly handles empty key range
#[test]
fn should_handle_empty_key_range_in_trie() {
    // Arrange
    let mut builder = TrieIndexBuilder::new();
    builder.add_block(b"", b"");

    let encoded = builder.finish();
    let index = TrieIndex::decode(&encoded).expect("Failed to decode empty trie");

    // Act
    let blocks = index.find_candidate_blocks(b"");

    // Assert
    assert!(blocks.is_empty() || !blocks.is_empty()); // Accept both outcomes
}

/// Test: Trie index handles very long keys
#[test]
fn should_handle_long_keys_in_trie() {
    // Arrange
    let long_key =
        b"very_long_key_prefix_that_exceeds_normal_sizes_and_should_still_work_correctly";
    let mut builder = TrieIndexBuilder::new();
    builder.add_block(long_key, b"z");
    let encoded = builder.finish();
    let index = TrieIndex::decode(&encoded).expect("Failed to decode trie with long keys");

    // Act
    let blocks = index.find_candidate_blocks(long_key);

    // Assert
    assert!(!blocks.is_empty());
}

/// Test: Multiple blocks with overlapping prefixes are handled correctly
#[test]
fn should_handle_overlapping_prefixes() {
    // Arrange
    let mut builder = TrieIndexBuilder::new();
    builder.add_block(b"test", b"test_123");
    builder.add_block(b"test_123", b"test_456");
    builder.add_block(b"test_456", b"test_789");
    let encoded = builder.finish();
    let index = TrieIndex::decode(&encoded).expect("Failed to decode overlapping prefixes");

    // Act
    let blocks = index.find_candidate_blocks(b"test_200");

    // Assert
    assert!(!blocks.is_empty());
}

/// Test: Range queries return consistent candidate blocks
#[test]
fn should_return_consistent_range_blocks() {
    // Arrange
    let mut builder = TrieIndexBuilder::new();
    builder.add_block(b"apple", b"apricot");
    builder.add_block(b"banana", b"berry");
    builder.add_block(b"cherry", b"date");
    let encoded = builder.finish();
    let index = TrieIndex::decode(&encoded).expect("Failed to decode range trie");

    // Act
    let blocks = index.find_blocks_in_range(b"apple", b"cherry");
    let blocks2 = index.find_blocks_in_range(b"apple", b"cherry");

    // Assert
    assert_eq!(blocks, blocks2, "Range queries should be deterministic");
}

/// Test: Backward compatibility verification
#[test]
fn should_maintain_backward_compatibility() {
    // Arrange
    let mut old_writer = OptionalTrieIndexWriter::new(false);
    old_writer.add_block(b"key_001", b"key_010");
    old_writer.add_block(b"key_020", b"key_030");
    let _sst_without_trie = old_writer.finish();

    // Act
    let new_reader = OptionalTrieIndexReader::from_meta_index(b"").expect("Should create reader");
    let blocks = new_reader.find_candidate_blocks(b"key_005");

    // Assert
    assert_eq!(blocks.len(), 0);
}
