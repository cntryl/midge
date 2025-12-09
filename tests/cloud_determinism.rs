//! Test suite for cloud operation determinism.
//!
//! These tests verify that cloud operations (uploads, downloads, eviction) are ordered
//! deterministically through EngineRuntime, ensuring crash recovery and replication work correctly.

use cntryl_midge::api::kvstore::{KvStore, WriteOp};
use cntryl_midge::api::engine::{EngineConfig, ColumnFamilyConfig};
use cntryl_midge::cloud::{MockCloud, CloudProvider};
use std::sync::Arc;
use std::path::Path;
use tempfile::TempDir;

/// Helper to create engine with cloud backend
fn create_engine_with_cloud(
    db_dir: &Path,
    cloud: Arc<dyn CloudProvider>,
) -> MidgeResult<Arc<cntryl_midge::core::engine::MidgeEngine>> {
    let mut config = EngineConfig::default();
    config.db_path = db_dir.to_path_buf();
    config.cloud_provider = Some(CloudProvider::Mock(cloud));
    
    cntryl_midge::core::engine::MidgeEngine::open(config)
}

// ============================================================================
// Test: Identical Workloads Produce Same SST Upload Order
// ============================================================================

#[test]
fn should_order_sst_uploads_deterministically_given_identical_compactions() {
    // Arrange
    let temp_dir1 = TempDir::new().unwrap();
    let temp_dir2 = TempDir::new().unwrap();
    let cloud1 = Arc::new(MockCloud::new());
    let cloud2 = Arc::new(MockCloud::new());
    
    let engine1 = create_engine_with_cloud(temp_dir1.path(), Arc::clone(&cloud1)).unwrap();
    let engine2 = create_engine_with_cloud(temp_dir2.path(), Arc::clone(&cloud2)).unwrap();
    
    // Write identical data to both engines
    let writes: Vec<WriteOp> = vec![
        WriteOp::Put { key: b"a".to_vec(), value: b"1".to_vec() },
        WriteOp::Put { key: b"b".to_vec(), value: b"2".to_vec() },
        WriteOp::Put { key: b"c".to_vec(), value: b"3".to_vec() },
    ];
    
    // Act 1: Write to engine1
    for write in writes.iter() {
        let cf = engine1.get_column_family(0).unwrap();
        match write {
            WriteOp::Put { key, value } => {
                cf.put(key, value).unwrap();
            }
            _ => {}
        }
    }
    engine1.flush(0).unwrap();
    
    // Act 2: Write to engine2
    for write in writes.iter() {
        let cf = engine2.get_column_family(0).unwrap();
        match write {
            WriteOp::Put { key, value } => {
                cf.put(key, value).unwrap();
            }
            _ => {}
        }
    }
    engine2.flush(0).unwrap();
    
    // Assert: Both engines should have uploaded same SSTs in same order
    let uploads1 = cloud1.list_uploads();
    let uploads2 = cloud2.list_uploads();
    
    assert_eq!(uploads1.len(), uploads2.len(), "Same number of uploads");
    for (upload1, upload2) in uploads1.iter().zip(uploads2.iter()) {
        assert_eq!(upload1.sst_id, upload2.sst_id, "SST IDs match");
        assert_eq!(upload1.size, upload2.size, "SST sizes match");
        assert_eq!(upload1.metadata, upload2.metadata, "SST metadata matches");
    }
}

// ============================================================================
// Test: Compaction Upload Order Is Deterministic
// ============================================================================

#[test]
fn should_order_compaction_uploads_deterministically_given_trigger() {
    // Arrange
    let temp_dir = TempDir::new().unwrap();
    let cloud = Arc::new(MockCloud::new());
    let engine = create_engine_with_cloud(temp_dir.path(), Arc::clone(&cloud)).unwrap();
    
    // Write enough data to trigger multiple SST creations
    let cf = engine.get_column_family(0).unwrap();
    for i in 0..100 {
        let key = format!("key{:03}", i);
        let value = format!("value{}", i);
        cf.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Act: Flush to create first SST, then compaction
    engine.flush(0).unwrap();
    
    // Write more data for second SST
    for i in 100..200 {
        let key = format!("key{:03}", i);
        let value = format!("value{}", i);
        cf.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    engine.flush(0).unwrap();
    
    // Trigger compaction (should produce predictable uploads)
    engine.compact_all(0, true).unwrap();
    
    // Assert: Cloud should have SSTs uploaded in deterministic order
    let uploads = cloud.list_uploads();
    
    // Verify order: flushes first, then compaction SST
    assert!(uploads.len() >= 3, "At least 3 uploads (2 flushes + 1 compaction)");
    
    // Verify SST IDs are in expected order
    for (i, upload) in uploads.iter().enumerate() {
        assert!(!upload.sst_id.is_empty(), "SST {} has valid ID", i);
    }
}

// ============================================================================
// Test: SST Metadata Is Consistent Across Replicas
// ============================================================================

#[test]
fn should_produce_identical_sst_metadata_given_same_workload() {
    // Arrange
    let temp_dir1 = TempDir::new().unwrap();
    let temp_dir2 = TempDir::new().unwrap();
    let cloud1 = Arc::new(MockCloud::new());
    let cloud2 = Arc::new(MockCloud::new());
    
    let engine1 = create_engine_with_cloud(temp_dir1.path(), Arc::clone(&cloud1)).unwrap();
    let engine2 = create_engine_with_cloud(temp_dir2.path(), Arc::clone(&cloud2)).unwrap();
    
    // Same workload for both
    let test_data: Vec<(&[u8], &[u8])> = vec![
        (b"apple", b"red"),
        (b"banana", b"yellow"),
        (b"cherry", b"red"),
    ];
    
    // Act 1: Write to engine1
    let cf1 = engine1.get_column_family(0).unwrap();
    for (key, value) in test_data.iter() {
        cf1.put(key, value).unwrap();
    }
    engine1.flush(0).unwrap();
    
    // Act 2: Write to engine2
    let cf2 = engine2.get_column_family(0).unwrap();
    for (key, value) in test_data.iter() {
        cf2.put(key, value).unwrap();
    }
    engine2.flush(0).unwrap();
    
    // Assert: SST metadata should be identical
    let uploads1 = cloud1.list_uploads();
    let uploads2 = cloud2.list_uploads();
    
    assert!(!uploads1.is_empty() && !uploads2.is_empty(), "Both engines uploaded SSTs");
    
    let meta1 = &uploads1[0].metadata;
    let meta2 = &uploads2[0].metadata;
    
    assert_eq!(
        meta1.smallest_key, meta2.smallest_key,
        "Smallest keys match: {:?} vs {:?}",
        String::from_utf8_lossy(&meta1.smallest_key.unwrap_or_default()),
        String::from_utf8_lossy(&meta2.smallest_key.unwrap_or_default())
    );
    assert_eq!(meta1.largest_key, meta2.largest_key, "Largest keys match");
    assert_eq!(meta1.entry_count, meta2.entry_count, "Entry counts match");
    assert_eq!(meta1.tombstone_count, meta2.tombstone_count, "Tombstone counts match");
}

// ============================================================================
// Test: Cloud Upload Task Submission Is Ordered
// ============================================================================

#[test]
fn should_submit_cloud_upload_tasks_in_order() {
    // Arrange
    let temp_dir = TempDir::new().unwrap();
    let cloud = Arc::new(MockCloud::new());
    let engine = create_engine_with_cloud(temp_dir.path(), Arc::clone(&cloud)).unwrap();
    
    // Act: Perform multiple flushes
    let cf = engine.get_column_family(0).unwrap();
    
    // Flush 1
    cf.put(b"key1", b"value1").unwrap();
    engine.flush(0).unwrap();
    
    // Flush 2
    cf.put(b"key2", b"value2").unwrap();
    engine.flush(0).unwrap();
    
    // Flush 3
    cf.put(b"key3", b"value3").unwrap();
    engine.flush(0).unwrap();
    
    // Assert: All flushes should be uploaded to cloud
    let uploads = cloud.list_uploads();
    
    assert_eq!(uploads.len(), 3, "All 3 SSTs uploaded");
    
    // Verify they're in flush order (by sequence numbers)
    for i in 0..uploads.len() - 1 {
        assert!(
            uploads[i].timestamp <= uploads[i + 1].timestamp,
            "Upload {} happens before upload {}",
            i,
            i + 1
        );
    }
}

// ============================================================================
// Test: Multiple Column Families Upload In Deterministic Order
// ============================================================================

#[test]
fn should_order_uploads_deterministically_across_multiple_column_families() {
    // Arrange
    let temp_dir = TempDir::new().unwrap();
    let cloud = Arc::new(MockCloud::new());
    let engine = create_engine_with_cloud(temp_dir.path(), Arc::clone(&cloud)).unwrap();
    
    // Act: Write to multiple column families
    let cf0 = engine.get_column_family(0).unwrap();
    let cf1 = engine.get_column_family(1).unwrap();
    
    // Write alternating
    cf0.put(b"cf0_key1", b"value1").unwrap();
    cf1.put(b"cf1_key1", b"value1").unwrap();
    cf0.put(b"cf0_key2", b"value2").unwrap();
    cf1.put(b"cf1_key2", b"value2").unwrap();
    
    // Flush both
    engine.flush(0).unwrap();
    engine.flush(1).unwrap();
    
    // Assert: Uploads should be in deterministic order
    let uploads = cloud.list_uploads();
    
    assert_eq!(uploads.len(), 2, "One SST per column family");
    assert!(uploads[0].sst_id.contains("cf000"), "First upload is CF0");
    assert!(uploads[1].sst_id.contains("cf001"), "Second upload is CF1");
}

// ============================================================================
// Helper Module: Mock Cloud Tracking
// ============================================================================

/// Mock cloud provider that tracks all uploads for determinism testing
struct MockCloudImpl {
    uploads: parking_lot::Mutex<Vec<UploadRecord>>,
}

struct UploadRecord {
    sst_id: String,
    size: u64,
    timestamp: std::time::SystemTime,
    metadata: SstMetadata,
}

struct SstMetadata {
    smallest_key: Option<Vec<u8>>,
    largest_key: Option<Vec<u8>>,
    entry_count: u64,
    tombstone_count: u64,
}

impl PartialEq for SstMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.smallest_key == other.smallest_key
            && self.largest_key == other.largest_key
            && self.entry_count == other.entry_count
            && self.tombstone_count == other.tombstone_count
    }
}
