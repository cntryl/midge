//! Streaming Compaction Integration Tests
//!
//! Tests for the streaming compaction pipeline:
//! - Collect versions from SST files
//! - Merge multiple sorted streams (MergeIterator)
//! - Deduplicate on-the-fly (StreamDeduplicate)
//! - Write deduplicated results to output SST
//!
//! The streaming design ensures O(1) deduplication memory regardless of input size.

mod common;

use cntryl_midge::compaction::executor::{
    collect_versions, deduplicate_versions, filter_tombstones, write_versions_to_sst,
    CompactionVersion, StreamDeduplicate,
};
use cntryl_midge::compaction::merge::{MergeIterator, MergeEntry};
use cntryl_midge::sst::SstFactory;
use cntryl_midge::sst::fs::FsSstFactory;
use cntryl_midge::common::MidgeResult;
use bytes::Bytes;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/// Create a test SST factory with a temporary directory
fn test_sst_factory() -> MidgeResult<(FsSstFactory, TempDir)> {
    let temp_dir = TempDir::new()?;
    let factory = FsSstFactory::new(temp_dir.path(), 4096);
    Ok((factory, temp_dir))
}

/// Write test data to an SST file
fn write_test_sst(
    factory: &dyn SstFactory,
    filename: &str,
    entries: &[(Vec<u8>, Vec<u8>)],
) -> MidgeResult<()> {
    let mut writer = factory.create()?;
    for (key, value) in entries {
        writer.add(key, value)?;
    }
    let path = Path::new(filename);
    writer.finish_to_path(path)?;
    Ok(())
}

fn mk_version<K: AsRef<[u8]>, V: AsRef<[u8]>>(
    key: K,
    seq: u64,
    is_tombstone: bool,
    value: Option<V>,
    expiration: Option<u64>,
) -> CompactionVersion {
    CompactionVersion {
        key: key.as_ref().to_vec(),
        seq,
        is_tombstone,
        value: value.map(|v| v.as_ref().to_vec()),
        expiration,
    }
}

// ============================================================================
// STREAMING PIPELINE TESTS
// ============================================================================

#[test]
fn should_stream_merge_multiple_sorted_iterators_when_using_merge_iterator() {
    // Arrange: Two sorted input streams
    let stream1 = vec![
        MergeEntry {
            key: Bytes::from(b"a".to_vec()),
            value: Bytes::from(b"a1".to_vec()),
            seq: 1,
        },
        MergeEntry {
            key: Bytes::from(b"c".to_vec()),
            value: Bytes::from(b"c1".to_vec()),
            seq: 1,
        },
    ];

    let stream2 = vec![
        MergeEntry {
            key: Bytes::from(b"b".to_vec()),
            value: Bytes::from(b"b1".to_vec()),
            seq: 1,
        },
        MergeEntry {
            key: Bytes::from(b"d".to_vec()),
            value: Bytes::from(b"d1".to_vec()),
            seq: 1,
        },
    ];

    // Act: Merge the streams
    let merge_iter = MergeIterator::from_iterators(vec![
        stream1.into_iter(),
        stream2.into_iter(),
    ]);

    let merged: Vec<_> = merge_iter.collect();

    // Assert: Output should be sorted by key
    assert_eq!(merged.len(), 4);
    assert_eq!(merged[0].key, Bytes::from(b"a".to_vec()));
    assert_eq!(merged[1].key, Bytes::from(b"b".to_vec()));
    assert_eq!(merged[2].key, Bytes::from(b"c".to_vec()));
    assert_eq!(merged[3].key, Bytes::from(b"d".to_vec()));
}

#[test]
fn should_deduplicate_stream_keeping_highest_seq_per_key_when_streaming_dedup() {
    // Arrange: Stream with duplicates of same key
    let input = vec![
        MergeEntry {
            key: Bytes::from(b"x".to_vec()),
            value: Bytes::from(b"x10".to_vec()),
            seq: 10, // Higher seq
        },
        MergeEntry {
            key: Bytes::from(b"x".to_vec()),
            value: Bytes::from(b"x5".to_vec()),
            seq: 5, // Lower seq (skipped)
        },
        MergeEntry {
            key: Bytes::from(b"y".to_vec()),
            value: Bytes::from(b"y3".to_vec()),
            seq: 3,
        },
    ];

    // Act: Convert to CompactionVersion and deduplicate
    let merge_iter = MergeIterator::from_iterators(vec![input.into_iter()]);
    let version_iter = merge_iter.map(|entry| CompactionVersion {
        key: entry.key.to_vec(),
        seq: entry.seq,
        is_tombstone: false,
        value: Some(entry.value.to_vec()),
        expiration: None,
    });
    let dedup_iter = StreamDeduplicate::new(version_iter);
    let deduped: Vec<_> = dedup_iter.collect();

    // Assert
    assert_eq!(deduped.len(), 2); // Two unique keys: x, y
    assert_eq!(deduped[0].key, b"x".to_vec());
    assert_eq!(deduped[0].seq, 10); // Highest seq for x
    assert_eq!(deduped[1].key, b"y".to_vec());
}

#[test]
fn should_filter_expired_entries_when_deduplicating_with_ttl() {
    // Arrange: Create current timestamp and expired/valid entries
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let versions = vec![
        mk_version("key1", 1, false, Some("expired"), Some(now - 100)), // Expired
        mk_version("key1", 2, false, Some("valid"), None),              // No TTL
        mk_version("key2", 3, false, Some("val"), Some(now + 1000)),   // Future expiry
    ];

    // Act
    let deduped = deduplicate_versions(&versions);

    // Assert: Only 2 entries (expired one skipped, both keys kept with valid entries)
    assert_eq!(deduped.len(), 2);
    let key1_entries: Vec<_> = deduped.iter().filter(|v| v.key == b"key1").collect();
    assert_eq!(key1_entries.len(), 1);
    assert_eq!(key1_entries[0].value.as_deref(), Some(b"valid".as_ref()));
}

#[test]
fn should_remove_tombstones_when_filtering_after_dedup() {
    // Arrange: Mix of tombstones and live entries
    let versions = vec![
        mk_version("key1", 5, false, Some("live"), None),    // Live
        mk_version("key2", 6, true, None::<&[u8]>, None),   // Tombstone (delete marker)
        mk_version("key3", 7, false, Some("live2"), None),   // Live
    ];

    // Act
    let filtered = filter_tombstones(&versions);

    // Assert: Tombstones removed
    assert_eq!(filtered.len(), 2);
    assert_eq!(filtered[0].key, b"key1".to_vec());
    assert_eq!(filtered[1].key, b"key3".to_vec());
}

#[test]
fn should_handle_empty_input_streams_when_merging() {
    // Arrange: Empty input streams
    let empty1: Vec<MergeEntry> = Vec::new();
    let empty2: Vec<MergeEntry> = Vec::new();

    // Act: Merge empty streams
    let merge_iter = MergeIterator::from_iterators(vec![
        empty1.into_iter(),
        empty2.into_iter(),
    ]);
    let merged: Vec<_> = merge_iter.collect();

    // Assert
    assert!(merged.is_empty());
}

#[test]
fn should_handle_single_entry_in_merge() {
    // Arrange: Single entry across streams
    let stream1 = vec![MergeEntry {
        key: Bytes::from(b"k".to_vec()),
        value: Bytes::from(b"v".to_vec()),
        seq: 1,
    }];

    let empty: Vec<MergeEntry> = Vec::new();

    // Act
    let merge_iter = MergeIterator::from_iterators(vec![
        stream1.into_iter(),
        empty.into_iter(),
    ]);
    let merged: Vec<_> = merge_iter.collect();

    // Assert
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].key, Bytes::from(b"k".to_vec()));
}

// ============================================================================
// BATCH DEDUPLICATION TESTS (for backward compatibility)
// ============================================================================

#[test]
fn should_keep_only_highest_seq_when_batch_deduplicating_multiple_keys() {
    // Arrange
    let versions = vec![
        mk_version("a", 3, false, Some("a3"), None),
        mk_version("a", 1, false, Some("a1"), None),
        mk_version("b", 2, false, Some("b2"), None),
        mk_version("b", 4, false, Some("b4"), None),
    ];

    // Act
    let deduped = deduplicate_versions(&versions);

    // Assert
    assert_eq!(deduped.len(), 2);
    // BTreeMap sorts keys
    assert_eq!(deduped[0].key, b"a".to_vec());
    assert_eq!(deduped[0].seq, 3); // Highest seq for a
    assert_eq!(deduped[1].key, b"b".to_vec());
    assert_eq!(deduped[1].seq, 4); // Highest seq for b
}

#[test]
fn should_handle_mixed_tombstones_and_values_when_deduplicating() {
    // Arrange
    let versions = vec![
        mk_version("k1", 5, false, Some("v5"), None),      // Live
        mk_version("k1", 3, true, None::<&[u8]>, None),   // Older tombstone (ignored)
        mk_version("k2", 2, true, None::<&[u8]>, None),   // Tombstone
        mk_version("k2", 1, false, Some("v1"), None),      // Older live (ignored)
    ];

    // Act
    let deduped = deduplicate_versions(&versions);

    // Assert: Should have both k1 and k2 with their highest seqs
    assert_eq!(deduped.len(), 2);
    assert_eq!(deduped[0].key, b"k1".to_vec());
    assert_eq!(deduped[0].seq, 5);
    assert_eq!(deduped[0].is_tombstone, false);

    assert_eq!(deduped[1].key, b"k2".to_vec());
    assert_eq!(deduped[1].seq, 2);
    assert_eq!(deduped[1].is_tombstone, true); // Tombstone with highest seq
}

// ============================================================================
// END-TO-END PIPELINE TESTS
// ============================================================================

#[test]
fn should_merge_and_deduplicate_when_running_full_streaming_pipeline() {
    // Arrange: Three streams with overlapping keys
    let stream1 = vec![
        MergeEntry {
            key: Bytes::from(b"a".to_vec()),
            value: Bytes::from(b"a_stream1_seq3".to_vec()),
            seq: 3,
        },
        MergeEntry {
            key: Bytes::from(b"a".to_vec()),
            value: Bytes::from(b"a_stream1_seq1".to_vec()),
            seq: 1,
        },
        MergeEntry {
            key: Bytes::from(b"c".to_vec()),
            value: Bytes::from(b"c_stream1_seq2".to_vec()),
            seq: 2,
        },
    ];

    let stream2 = vec![
        MergeEntry {
            key: Bytes::from(b"a".to_vec()),
            value: Bytes::from(b"a_stream2_seq2".to_vec()),
            seq: 2,
        },
        MergeEntry {
            key: Bytes::from(b"b".to_vec()),
            value: Bytes::from(b"b_stream2_seq5".to_vec()),
            seq: 5,
        },
    ];

    let stream3 = vec![
        MergeEntry {
            key: Bytes::from(b"d".to_vec()),
            value: Bytes::from(b"d_stream3_seq4".to_vec()),
            seq: 4,
        },
    ];

    // Act: Full pipeline
    // 1. Merge
    let merge_iter = MergeIterator::from_iterators(vec![
        stream1.into_iter(),
        stream2.into_iter(),
        stream3.into_iter(),
    ]);

    // 2. Convert to CompactionVersion
    let version_iter = merge_iter.map(|entry| CompactionVersion {
        key: entry.key.to_vec(),
        seq: entry.seq,
        is_tombstone: false,
        value: Some(entry.value.to_vec()),
        expiration: None,
    });

    // 3. Stream deduplicate
    let dedup_iter = StreamDeduplicate::new(version_iter);

    // 4. Collect
    let result: Vec<_> = dedup_iter.collect();

    // Assert
    // Should have 4 unique keys
    assert_eq!(result.len(), 4);

    // Verify each key has the highest seq
    assert_eq!(result[0].key, b"a".to_vec());
    assert_eq!(result[0].seq, 3); // a: seq 3 from stream1

    assert_eq!(result[1].key, b"b".to_vec());
    assert_eq!(result[1].seq, 5); // b: seq 5 from stream2

    assert_eq!(result[2].key, b"c".to_vec());
    assert_eq!(result[2].seq, 2); // c: seq 2 from stream1

    assert_eq!(result[3].key, b"d".to_vec());
    assert_eq!(result[3].seq, 4); // d: seq 4 from stream3
}

#[test]
fn should_preserve_deterministic_output_when_merging_and_deduplicating() {
    // Arrange: Create the same input multiple times to verify determinism
    let create_streams = || {
        let s1 = vec![
            MergeEntry {
                key: Bytes::from(b"z".to_vec()),
                value: Bytes::from(b"z1".to_vec()),
                seq: 1,
            },
            MergeEntry {
                key: Bytes::from(b"a".to_vec()),
                value: Bytes::from(b"a2".to_vec()),
                seq: 2,
            },
        ];

        let s2 = vec![
            MergeEntry {
                key: Bytes::from(b"m".to_vec()),
                value: Bytes::from(b"m3".to_vec()),
                seq: 3,
            },
        ];

        vec![s1.into_iter(), s2.into_iter()]
    };

    // Act: Run pipeline twice
    let run_pipeline = |iterators| {
        let merge_iter = MergeIterator::from_iterators(iterators);
        let version_iter = merge_iter.map(|entry| CompactionVersion {
            key: entry.key.to_vec(),
            seq: entry.seq,
            is_tombstone: false,
            value: Some(entry.value.to_vec()),
            expiration: None,
        });
        let dedup_iter = StreamDeduplicate::new(version_iter);
        dedup_iter.collect::<Vec<_>>()
    };

    let result1 = run_pipeline(create_streams());
    let result2 = run_pipeline(create_streams());

    // Assert: Results should be identical
    assert_eq!(result1.len(), result2.len(), "Result lengths differ");
    assert_eq!(result1.len(), 3, "Expected 3 unique keys"); // 3 unique keys
    
    // Keys should be in ascending order: a, m, z
    assert_eq!(result1[0].key, b"a".to_vec(), "result1[0].key");
    assert_eq!(result2[0].key, b"a".to_vec(), "result2[0].key");
    
    assert_eq!(result1[1].key, b"m".to_vec(), "result1[1].key");
    assert_eq!(result2[1].key, b"m".to_vec(), "result2[1].key");
    
    assert_eq!(result1[2].key, b"z".to_vec(), "result1[2].key");
    assert_eq!(result2[2].key, b"z".to_vec(), "result2[2].key");
}
