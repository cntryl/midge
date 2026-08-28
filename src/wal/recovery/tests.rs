use super::*;
use crate::io::{FsPath, RealFs};
use crate::wal::fs::FsWalWriterIo;
use crate::wal::types::WalOpKind;
use crate::wal::WalWriter;
use bytes::Bytes;
use std::sync::Arc;
use tempfile::TempDir;

fn append_raw_bytes(path: &std::path::Path, bytes: &[u8]) {
    use std::io::Write;

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}

fn encode_frame(record: &WalRecord) -> Vec<u8> {
    let payload = crate::wal::encoding::encode(record).unwrap();
    let mut frame = Vec::new();
    crate::wal::frame::append_frame(&mut frame, &payload).unwrap();
    frame
}

#[test]
fn should_fail_replay_closed_given_duplicate_key_sequence() {
    // Arrange
    let mut memtables = HashMap::new();
    let first = put_record(b"duplicate", 7, 1);
    let mut conflicting = put_record(b"duplicate", 7, 1);
    conflicting.value = Some(Bytes::from_static(b"conflicting"));
    apply_record(&first, &mut memtables).expect("apply first record");

    // Act
    let result = apply_record(&conflicting, &mut memtables);

    // Assert: replay returns corruption, so startup never publishes this
    // temporary recovery-memtable map into RuntimeState.
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
    assert_eq!(
        memtables[&0].get(b"duplicate").expect("read staged value"),
        Some(b"value".to_vec())
    );
}

#[test]
fn should_reject_delete_range_record_missing_range_end() {
    // Arrange: RANGE_END is required for DeleteRange (lsm-spec format/wal.md
    // §5.1) — a record missing it must be rejected, not silently skipped.
    let mut memtables = HashMap::new();
    let record = WalRecord::new(WalOpKind::DeleteRange, Bytes::from_static(b"a"), None, 1, 1);
    assert_eq!(record.range_end, None);

    // Act
    let result = apply_record(&record, &mut memtables);

    // Assert
    assert!(
        matches!(result, Err(MidgeError::Corruption(_))),
        "{result:?}"
    );
}

fn put_record(key: &'static [u8], sequence: u64, writer_epoch: u64) -> WalRecord {
    WalRecord::new(
        WalOpKind::Put,
        Bytes::from_static(key),
        Some(Bytes::from_static(b"value")),
        sequence,
        writer_epoch,
    )
}

#[test]
fn should_reject_active_wal_given_writer_epoch_regression() {
    // Arrange
    let bytes = [
        encode_frame(&put_record(b"epoch-7", 1, 7)),
        encode_frame(&put_record(b"epoch-8", 2, 8)),
        encode_frame(&put_record(b"stale-epoch-7", 3, 7)),
    ]
    .concat();

    // Act
    let result = inspect_active_wal_bytes(&bytes);

    // Assert
    let failure = result.expect_err("regressing epoch must be corrupt");
    assert!(matches!(failure.failure.error(), MidgeError::Corruption(_)));
    assert_eq!(failure.verified_prefix.writer_epoch, 8);
    assert_eq!(failure.verified_prefix.record_count, 2);
}

fn wal_with_corrupted_length_before_valid_suffix() -> Vec<u8> {
    let prefix = encode_frame(&put_record(b"prefix", 1, 1));
    let hidden = encode_frame(&put_record(b"hidden", 2, 1));
    let suffix = encode_frame(&put_record(b"suffix", 3, 1));
    let hidden_offset = prefix.len();
    let mut wal_bytes = [prefix, hidden, suffix].concat();
    let corrupt_len = u32::try_from(wal_bytes.len()).unwrap();
    wal_bytes[hidden_offset..hidden_offset + 4].copy_from_slice(&corrupt_len.to_le_bytes());
    wal_bytes
}

#[test]
fn should_initialize_stats_with_zeros_when_created() {
    let stats = RecoveryStats::new();
    assert_eq!(stats.record_count, 0);
    assert_eq!(stats.bytes, 0);
}

#[test]
fn should_open_each_wal_file_once_per_recovery_pass() {
    // Arrange
    let directory = TempDir::new().expect("create WAL recovery directory");
    let storage = RealFs::new(directory.path()).expect("create recovery filesystem");
    let wal_dir = FsPath::new("wal");
    storage
        .create_dir_all(&wal_dir)
        .expect("create WAL directory");
    let wal_path = directory
        .path()
        .join("wal")
        .join(crate::wal::segment_file_name(1));
    let bytes = (1..=128)
        .map(|sequence| encode_frame(&put_record(b"key", sequence, 1)))
        .collect::<Vec<_>>()
        .concat();
    std::fs::write(wal_path, bytes).expect("write WAL fixture");
    let mut memtables = HashMap::new();
    reset_wal_replay_file_open_count();

    // Act
    replay_wal_with_policy(&storage, &wal_dir, &mut memtables, ReplayPolicy::Strict)
        .expect("replay WAL fixture");

    // Assert
    assert_eq!(
        wal_replay_file_open_count(),
        2,
        "epoch discovery and record replay should each open the WAL once"
    );
    assert_eq!(
        wal_replay_file_read_count(),
        2,
        "epoch discovery and record replay should each snapshot the WAL once"
    );
}

#[test]
fn should_not_tolerate_generic_corruption_based_on_incomplete_tail_error_text() {
    // Arrange
    let replay_file = ReplayFile {
        path: FsPath::new(crate::wal::ACTIVE_FILE_NAME),
        kind: ReplayFileKind::FinalActive,
    };
    let failure = ReplayFailure::Error(MidgeError::Corruption(
        "Incomplete WAL record text alone is not a typed tail".to_string(),
    ));

    // Act
    let action = replay_error_action(&replay_file, ReplayPolicy::Strict, &failure);

    // Assert
    assert_eq!(action, ReplayErrorAction::Fail);
}

#[test]
fn should_return_empty_stats_when_wal_directory_missing() {
    // Arrange
    let mut memtables = HashMap::new();
    let dir = TempDir::new().unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let non_existent = FsPath::new("midge_nonexistent_wal_dir_12345");

    // Act
    let stats = replay_wal(&storage, &non_existent, &mut memtables).unwrap();

    // Assert
    assert_eq!(stats.record_count, 0);
    assert_eq!(stats.max_sequence, None);
}

#[test]
fn should_recover_put_record_key_value_when_replaying_wal() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");

    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"test_key"),
            Some(Bytes::from_static(b"test_value")),
            1,
            1,
        );
        writer.append_record(&record).unwrap();
        writer.sync().unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let _stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

    // Assert
    let recovered_memtable = &memtables[&0];
    let value = recovered_memtable.get(b"test_key").unwrap();
    assert_eq!(value, Some(b"test_value".to_vec()));
}

#[test]
fn should_increment_record_count_when_replaying_put() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");

    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"test_key"),
            Some(Bytes::from_static(b"test_value")),
            1,
            1,
        );
        writer.append_record(&record).unwrap();
        writer.sync().unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

    // Assert
    assert!(stats.record_count > 0);
}

#[test]
fn should_track_max_sequence_from_put_record() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");

    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"test_key"),
            Some(Bytes::from_static(b"test_value")),
            1,
            1,
        );
        writer.append_record(&record).unwrap();
        writer.sync().unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

    // Assert
    assert_eq!(stats.max_sequence, Some(1));
}

#[test]
fn should_recover_delete_operation_when_replaying_wal() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");

    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();
        let put_record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"test_key"),
            Some(Bytes::from_static(b"test_value")),
            1,
            1,
        );
        writer.append_record(&put_record).unwrap();

        let delete_record = WalRecord::new(
            WalOpKind::Delete,
            Bytes::from_static(b"test_key"),
            None,
            2,
            1,
        );
        writer.append_record(&delete_record).unwrap();
        writer.sync().unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let _stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

    // Assert
    let recovered_memtable = &memtables[&0];
    let value = recovered_memtable.get(b"test_key").unwrap();
    assert_eq!(value, None);
}

#[test]
fn should_count_put_records() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");

    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();
        let put_record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"test_key"),
            Some(Bytes::from_static(b"test_value")),
            1,
            1,
        );
        writer.append_record(&put_record).unwrap();

        let delete_record = WalRecord::new(
            WalOpKind::Delete,
            Bytes::from_static(b"test_key"),
            None,
            2,
            1,
        );
        writer.append_record(&delete_record).unwrap();
        writer.sync().unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

    // Assert
    assert_eq!(stats.record_count, 2);
}

#[test]
fn should_separate_records_by_column_family_when_recovering() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");

    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();

        let record_cf0 = WalRecord::new_cf(
            0,
            WalOpKind::Put,
            Bytes::from_static(b"key0"),
            Some(Bytes::from_static(b"value0")),
            1,
            1,
        );
        writer.append_record(&record_cf0).unwrap();

        let record_cf1 = WalRecord::new_cf(
            1,
            WalOpKind::Put,
            Bytes::from_static(b"key1"),
            Some(Bytes::from_static(b"value1")),
            2,
            1,
        );
        writer.append_record(&record_cf1).unwrap();
        writer.sync().unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let _stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

    // Assert
    assert_eq!(memtables.len(), 2);
}

#[test]
fn should_recover_both_column_families_with_correct_data() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");

    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();

        let record_cf0 = WalRecord::new_cf(
            0,
            WalOpKind::Put,
            Bytes::from_static(b"key0"),
            Some(Bytes::from_static(b"value0")),
            1,
            1,
        );
        writer.append_record(&record_cf0).unwrap();

        let record_cf1 = WalRecord::new_cf(
            1,
            WalOpKind::Put,
            Bytes::from_static(b"key1"),
            Some(Bytes::from_static(b"value1")),
            2,
            1,
        );
        writer.append_record(&record_cf1).unwrap();
        writer.sync().unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let _stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

    // Assert
    assert!(memtables[&0].get(b"key0").unwrap().is_some());
    assert!(memtables[&1].get(b"key1").unwrap().is_some());
}

#[test]
fn should_count_records_across_multiple_column_families() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");

    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();

        let record_cf0 = WalRecord::new_cf(
            0,
            WalOpKind::Put,
            Bytes::from_static(b"key0"),
            Some(Bytes::from_static(b"value0")),
            1,
            1,
        );
        writer.append_record(&record_cf0).unwrap();

        let record_cf1 = WalRecord::new_cf(
            1,
            WalOpKind::Put,
            Bytes::from_static(b"key1"),
            Some(Bytes::from_static(b"value1")),
            2,
            1,
        );
        writer.append_record(&record_cf1).unwrap();
        writer.sync().unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

    // Assert: record_count aggregates across every column family, and each
    // record still lands in its own CF's memtable rather than being merged
    // or double-counted into a shared one.
    assert_eq!(stats.record_count, 2);
    assert_eq!(
        memtables[&0].get(b"key0").unwrap(),
        Some(b"value0".to_vec())
    );
    assert_eq!(
        memtables[&1].get(b"key1").unwrap(),
        Some(b"value1".to_vec())
    );
    assert!(memtables[&0].get(b"key1").unwrap().is_none());
}

#[test]
fn should_restore_sequence_frontier_given_recovered_wal_with_sequence_gaps() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");

    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();

        let record1 = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key1"),
            Some(Bytes::from_static(b"value1")),
            5,
            1,
        );
        writer.append_record(&record1).unwrap();

        let record2 = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key2"),
            Some(Bytes::from_static(b"value2")),
            10,
            1,
        );
        writer.append_record(&record2).unwrap();

        let record3 = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key3"),
            Some(Bytes::from_static(b"value3")),
            7,
            1,
        );
        writer.append_record(&record3).unwrap();
        writer.sync().unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

    // Assert: sequence-gap ordering must not affect either the frontier or
    // the record count (previously covered separately by a near-identical
    // fixture in `should_count_multiple_records_correctly`).
    assert_eq!(stats.max_sequence, Some(10));
    assert_eq!(stats.record_count, 3);
}

#[test]
fn should_return_empty_stats_when_no_records() {
    // Arrange: an existing but empty WAL file (as opposed to a missing
    // directory, covered separately above).
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");

    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let _writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

    // Assert
    assert_eq!(stats.record_count, 0);
    assert_eq!(stats.max_sequence, None);
}

// =========== TTL/Expiration Tests ===========

#[test]
fn should_mask_preserved_expired_record_at_read_time_when_recovering() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");

    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();
        let mut expired_record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"expired_key"),
            Some(Bytes::from_static(b"value")),
            1,
            1,
        );
        // Set expiration to the past (1 millisecond after epoch)
        expired_record.expiration = Some(1);
        writer.append_record(&expired_record).unwrap();

        let mut future_record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"future_key"),
            Some(Bytes::from_static(b"value")),
            2,
            1,
        );
        // Set expiration to far future
        future_record.expiration = Some(u64::MAX);
        writer.append_record(&future_record).unwrap();

        writer.sync().unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

    // Assert
    let recovered_memtable = &memtables[&0];
    assert!(matches!(
        recovered_memtable
            .get_key_state_at_with_time(b"expired_key", u64::MAX, 0)
            .unwrap(),
        crate::sst::types::KeyState::Value(_, 1, Some(1), _)
    ));
    assert_eq!(
        recovered_memtable
            .get_key_state_at_with_time(b"expired_key", u64::MAX, 1)
            .unwrap(),
        crate::sst::types::KeyState::Tombstone(1)
    );
    // Future record should be present
    assert!(recovered_memtable.get(b"future_key").unwrap().is_some());
    // Both raw records were processed.
    assert_eq!(stats.record_count, 2);
}

#[test]
fn should_track_bytes_accounting_correctly() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");

    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key123"),         // 6 bytes
            Some(Bytes::from_static(b"value456")), // 8 bytes
            1,
            1,
        );
        writer.append_record(&record).unwrap();
        writer.sync().unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

    // Assert
    // Should account for key (6) + value (8) = 14 bytes minimum
    assert!(stats.bytes >= 14);
    assert_eq!(stats.record_count, 1);
}

#[test]
fn should_handle_delete_range_operations() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");

    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();

        // Add a put record first
        let put_record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            1,
            1,
        );
        writer.append_record(&put_record).unwrap();

        // DeleteRange is currently a no-op, but should not cause errors
        let mut delete_range_record = WalRecord::new(
            WalOpKind::DeleteRange,
            Bytes::from_static(b"start"),
            None,
            2,
            1,
        );
        delete_range_record.range_end = Some(Bytes::from_static(b"end"));
        writer.append_record(&delete_range_record).unwrap();

        writer.sync().unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

    // Assert
    assert_eq!(stats.record_count, 2);
}

#[test]
fn should_handle_transaction_markers() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");

    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();

        let begin_record = WalRecord::new(
            WalOpKind::TxnBegin,
            Bytes::from_static(b"txn_key"),
            None,
            1,
            1,
        );
        writer.append_record(&begin_record).unwrap();

        let put_record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            2,
            1,
        );
        writer.append_record(&put_record).unwrap();

        let commit_record = WalRecord::new(
            WalOpKind::TxnCommit,
            Bytes::from_static(b"txn_key"),
            None,
            3,
            1,
        );
        writer.append_record(&commit_record).unwrap();

        writer.sync().unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

    // Assert
    assert_eq!(stats.record_count, 3);
    // The Put should have been applied, TxnBegin and TxnCommit are markers
    let recovered_memtable = &memtables[&0];
    assert_eq!(
        recovered_memtable.get(b"key").unwrap(),
        Some(b"value".to_vec())
    );
}

#[test]
fn should_apply_txn_batch_atomically_during_recovery() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");

    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();

        let mut put_record = WalRecord::new_cf(
            0,
            WalOpKind::Put,
            Bytes::from_static(b"batch-key"),
            Some(Bytes::from_static(b"batch-value")),
            2,
            7,
        );
        put_record.txn_id = Some(11);

        let payload =
            crate::wal::encoding::encode_txn_batch_payload(11, 1, 3, 7, &[put_record]).unwrap();
        let mut batch_record = WalRecord::new_cf(
            0,
            WalOpKind::TxnBatch,
            Bytes::from_static(b"txn"),
            Some(payload),
            3,
            7,
        );
        batch_record.txn_id = Some(11);

        writer.append_record(&batch_record).unwrap();
        writer.sync().unwrap();
    }

    let mut memtables = HashMap::new();
    let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

    // Act
    // Assert
    assert_eq!(stats.record_count, 1);
    let recovered_memtable = &memtables[&0];
    assert_eq!(
        recovered_memtable.get(b"batch-key").unwrap(),
        Some(b"batch-value".to_vec())
    );
    assert_eq!(stats.max_sequence, Some(3));
}

#[test]
fn should_replay_only_uncovered_operations_from_retained_txn_batch() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");
    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();
        let mut covered = put_record(b"covered", 2, 7);
        covered.txn_id = Some(11);
        let mut uncovered = put_record(b"uncovered", 3, 7);
        uncovered.txn_id = Some(11);
        let payload =
            crate::wal::encoding::encode_txn_batch_payload(11, 1, 4, 7, &[covered, uncovered])
                .unwrap();
        let mut batch = WalRecord::new_cf(
            0,
            WalOpKind::TxnBatch,
            Bytes::from_static(b"txn"),
            Some(payload),
            4,
            7,
        );
        batch.txn_id = Some(11);
        writer.append_record(&batch).unwrap();
        writer.sync().unwrap();
    }
    let should_apply = |record: &WalRecord| record.key.as_ref() != b"covered";

    // Act
    let mut memtables = HashMap::new();
    let stats = replay_wal_with_manifest_filter(
        &storage,
        &wal_dir,
        &mut memtables,
        ReplayPolicy::Strict,
        &should_apply,
    )
    .unwrap();

    // Assert
    assert_eq!(stats.record_count, 1);
    let recovered = &memtables[&0];
    assert_eq!(recovered.get(b"covered").unwrap(), None);
    assert_eq!(
        recovered.get(b"uncovered").unwrap(),
        Some(b"value".to_vec())
    );
}

#[test]
fn should_recover_only_committed_transaction_given_split_wal_records_when_commit_marker_is_missing()
{
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");

    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();

        let mut begin_record =
            WalRecord::new(WalOpKind::TxnBegin, Bytes::from_static(b"txn"), None, 1, 1);
        begin_record.txn_id = Some(42);
        writer.append_record(&begin_record).unwrap();

        let mut put_record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            2,
            1,
        );
        put_record.txn_id = Some(42);
        writer.append_record(&put_record).unwrap();

        writer.sync().unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

    // Assert
    assert_eq!(stats.record_count, 2);
    assert!(
        !memtables.contains_key(&0),
        "incomplete transactions must not materialize a recovered memtable entry"
    );
}

#[test]
fn should_isolate_reused_transaction_id_by_writer_epoch() {
    // Arrange: epoch 11 crashes after writing an uncommitted operation.
    // Epoch 12 then reuses transaction id 1 and commits different data.
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");
    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();
        let mut old_begin =
            WalRecord::new(WalOpKind::TxnBegin, Bytes::from_static(b"txn"), None, 1, 11);
        old_begin.txn_id = Some(1);
        writer.append_record(&old_begin).unwrap();
        let mut orphaned = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"orphaned"),
            Some(Bytes::from_static(b"must-stay-hidden")),
            2,
            11,
        );
        orphaned.txn_id = Some(1);
        writer.append_record(&orphaned).unwrap();

        let mut new_begin =
            WalRecord::new(WalOpKind::TxnBegin, Bytes::from_static(b"txn"), None, 3, 12);
        new_begin.txn_id = Some(1);
        writer.append_record(&new_begin).unwrap();
        let mut committed = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"committed"),
            Some(Bytes::from_static(b"visible")),
            4,
            12,
        );
        committed.txn_id = Some(1);
        writer.append_record(&committed).unwrap();
        let mut new_commit = WalRecord::new(
            WalOpKind::TxnCommit,
            Bytes::from_static(b"txn"),
            None,
            5,
            12,
        );
        new_commit.txn_id = Some(1);
        writer.append_record(&new_commit).unwrap();
        writer.sync().unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

    // Assert
    let recovered = &memtables[&0];
    assert_eq!(recovered.get(b"orphaned").unwrap(), None);
    assert_eq!(
        recovered.get(b"committed").unwrap(),
        Some(b"visible".to_vec())
    );
}

#[test]
fn should_replay_committed_split_transaction_from_spool() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");
    let txn_id = 73;
    let operation_count = 128_u64;
    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();
        let mut begin = WalRecord::new(WalOpKind::TxnBegin, Bytes::from_static(b"txn"), None, 1, 1);
        begin.txn_id = Some(txn_id);
        writer.append_record(&begin).unwrap();
        for index in 0..operation_count {
            let mut put = WalRecord::new(
                WalOpKind::Put,
                Bytes::from(format!("spooled-{index:03}")),
                Some(Bytes::from(vec![b'x'; 4 * 1024])),
                index + 2,
                1,
            );
            put.txn_id = Some(txn_id);
            writer.append_record(&put).unwrap();
        }
        let mut commit = WalRecord::new(
            WalOpKind::TxnCommit,
            Bytes::from_static(b"txn"),
            None,
            operation_count + 2,
            1,
        );
        commit.txn_id = Some(txn_id);
        writer.append_record(&commit).unwrap();
        writer.sync().unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

    // Assert
    assert_eq!(stats.record_count, operation_count + 2);
    let recovered = &memtables[&0];
    assert_eq!(
        recovered.get(b"spooled-000").unwrap(),
        Some(vec![b'x'; 4 * 1024])
    );
    assert_eq!(
        recovered.get(b"spooled-127").unwrap(),
        Some(vec![b'x'; 4 * 1024])
    );
}

#[test]
fn should_salvage_valid_prefix_when_txn_batch_frame_is_truncated() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");
    let wal_path = wal_subdir.join("wal.log");

    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();
        let prefix = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"prefix"),
            Some(Bytes::from_static(b"value")),
            1,
            1,
        );
        writer.append_record(&prefix).unwrap();

        let mut batched_put = WalRecord::new_cf(
            0,
            WalOpKind::Put,
            Bytes::from_static(b"truncated"),
            Some(Bytes::from_static(b"should-not-appear")),
            3,
            1,
        );
        batched_put.txn_id = Some(90);
        let payload =
            crate::wal::encoding::encode_txn_batch_payload(90, 2, 4, 1, &[batched_put]).unwrap();
        let mut batch_record = WalRecord::new_cf(
            0,
            WalOpKind::TxnBatch,
            Bytes::from_static(b"txn"),
            Some(payload),
            4,
            1,
        );
        batch_record.txn_id = Some(90);
        writer.append_record(&batch_record).unwrap();
        writer.sync().unwrap();
    }

    let bytes = std::fs::read(&wal_path).unwrap();
    std::fs::write(&wal_path, &bytes[..bytes.len() - 3]).unwrap();

    let mut memtables = HashMap::new();
    let stats = replay_wal_with_policy(
        &storage,
        &wal_dir,
        &mut memtables,
        ReplayPolicy::SalvageValidPrefix,
    )
    .unwrap();

    // Act
    // Assert
    assert!(stats.had_corruption || stats.record_count >= 1);
    let recovered_memtable = &memtables[&0];
    assert_eq!(
        recovered_memtable.get(b"prefix").unwrap(),
        Some(b"value".to_vec())
    );
    assert_eq!(recovered_memtable.get(b"truncated").unwrap(), None);
}

#[test]
fn should_reject_corrupt_final_active_wal_frame_given_complete_frame_header_when_recovering() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");
    let wal_path = wal_subdir.join("wal.log");

    let record = WalRecord::new(
        WalOpKind::Put,
        Bytes::from_static(b"crc_key"),
        Some(Bytes::from_static(b"crc_value")),
        1,
        9,
    );
    let payload = crate::wal::encoding::encode(&record).unwrap();
    let mut frame = Vec::new();
    crate::wal::frame::append_frame(&mut frame, &payload).unwrap();
    frame[4] ^= 0x5a;
    append_raw_bytes(&wal_path, &frame);

    let mut memtables = HashMap::new();

    // Act
    let err = replay_wal_with_policy(&storage, &wal_dir, &mut memtables, ReplayPolicy::Strict)
        .unwrap_err();

    // Assert
    match err {
        MidgeError::Corruption(msg) => assert!(msg.contains("CRC mismatch")),
        other => panic!("expected corruption error, got {other:?}"),
    }
}

#[test]
fn should_return_degraded_verified_prefix_when_salvaging_truncated_sealed_wal() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");
    let wal_path = wal_subdir.join("wal.log");

    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"good"),
            Some(Bytes::from_static(b"value")),
            1,
            2,
        );
        writer.append_record(&record).unwrap();
        writer.sync().unwrap();
    }

    let bad_record = WalRecord::new(
        WalOpKind::Put,
        Bytes::from_static(b"bad"),
        Some(Bytes::from_static(b"value")),
        2,
        2,
    );
    let payload = crate::wal::encoding::encode(&bad_record).unwrap();
    let mut frame = Vec::new();
    crate::wal::frame::append_frame(&mut frame, &payload).unwrap();
    frame[5] ^= 0x11;
    append_raw_bytes(&wal_path, &frame);

    let mut memtables = HashMap::new();

    // Act
    let stats = replay_wal_with_policy(
        &storage,
        &wal_dir,
        &mut memtables,
        ReplayPolicy::SalvageValidPrefix,
    )
    .unwrap();

    // Assert
    assert!(stats.had_corruption);
    assert_eq!(stats.record_count, 1);
    assert_eq!(memtables[&0].get(b"good").unwrap(), Some(b"value".to_vec()));
    assert_eq!(memtables[&0].get(b"bad").unwrap(), None);
}

#[test]
fn should_salvage_valid_prefix_on_truncated_tail_frame() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");
    let wal_path = wal_subdir.join("wal.log");

    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"good"),
            Some(Bytes::from_static(b"value")),
            1,
            3,
        );
        writer.append_record(&record).unwrap();
        writer.sync().unwrap();
    }

    let tail_record = WalRecord::new(
        WalOpKind::Put,
        Bytes::from_static(b"tail"),
        Some(Bytes::from_static(b"value")),
        2,
        3,
    );
    let payload = crate::wal::encoding::encode(&tail_record).unwrap();
    let mut frame = Vec::new();
    crate::wal::frame::append_frame(&mut frame, &payload).unwrap();
    frame.truncate(frame.len() - 3);
    append_raw_bytes(&wal_path, &frame);

    let mut memtables = HashMap::new();

    // Act
    let stats = replay_wal_with_policy(
        &storage,
        &wal_dir,
        &mut memtables,
        ReplayPolicy::SalvageValidPrefix,
    )
    .unwrap();

    // Assert
    assert!(!stats.had_corruption);
    assert_eq!(stats.record_count, 1);
    assert_eq!(memtables[&0].get(b"good").unwrap(), Some(b"value".to_vec()));
    assert_eq!(memtables[&0].get(b"tail").unwrap(), None);
}

#[test]
fn should_fail_strict_recovery_given_corruption_in_a_sealed_wal_segment() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");
    let first_segment = wal_subdir.join(crate::wal::segment_file_name(1));

    append_raw_bytes(&first_segment, &encode_frame(&put_record(b"prefix", 1, 1)));
    let mut torn_frame = encode_frame(&put_record(b"torn", 2, 1));
    torn_frame.truncate(torn_frame.len() - 3);
    append_raw_bytes(&first_segment, &torn_frame);
    append_raw_bytes(
        &wal_subdir.join(crate::wal::segment_file_name(2)),
        &encode_frame(&put_record(b"later-sealed", 3, 2)),
    );
    append_raw_bytes(
        &wal_subdir.join(crate::wal::ACTIVE_FILE_NAME),
        &encode_frame(&put_record(b"active", 4, 2)),
    );
    let mut memtables = HashMap::new();

    // Act
    let error = replay_wal_with_policy(&storage, &wal_dir, &mut memtables, ReplayPolicy::Strict)
        .unwrap_err();

    // Assert
    match error {
        MidgeError::Corruption(message) => {
            assert!(message.contains("Incomplete WAL record"));
            assert!(message.contains(&crate::wal::segment_file_name(1)));
        }
        other => panic!("expected corruption error, got {other:?}"),
    }
}

#[test]
fn should_salvage_verified_prefix_given_corruption_in_a_sealed_wal_segment_when_salvage_policy_is_enabled(
) {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");
    let first_segment = wal_subdir.join(crate::wal::segment_file_name(1));

    append_raw_bytes(&first_segment, &encode_frame(&put_record(b"prefix", 1, 1)));
    let mut torn_frame = encode_frame(&put_record(b"torn", 2, 1));
    torn_frame.truncate(torn_frame.len() - 3);
    append_raw_bytes(&first_segment, &torn_frame);
    append_raw_bytes(
        &wal_subdir.join(crate::wal::segment_file_name(2)),
        &encode_frame(&put_record(b"later-sealed", 3, 2)),
    );
    append_raw_bytes(
        &wal_subdir.join(crate::wal::ACTIVE_FILE_NAME),
        &encode_frame(&put_record(b"active", 4, 2)),
    );
    let mut memtables = HashMap::new();

    // Act
    let stats = replay_wal_with_policy(
        &storage,
        &wal_dir,
        &mut memtables,
        ReplayPolicy::SalvageValidPrefix,
    )
    .unwrap();

    // Assert
    assert!(stats.had_corruption);
    assert_eq!(stats.record_count, 1);
    assert_eq!(stats.max_epoch_seen, 1);
    assert_eq!(
        memtables[&0].get(b"prefix").unwrap(),
        Some(b"value".to_vec())
    );
    assert_eq!(memtables[&0].get(b"torn").unwrap(), None);
    assert_eq!(memtables[&0].get(b"later-sealed").unwrap(), None);
    assert_eq!(memtables[&0].get(b"active").unwrap(), None);
}

#[test]
fn should_ignore_only_incomplete_final_active_wal_tail_given_truncated_frame_when_recovering() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");

    append_raw_bytes(
        &wal_subdir.join(crate::wal::segment_file_name(1)),
        &encode_frame(&put_record(b"sealed", 1, 1)),
    );
    let active_path = wal_subdir.join(crate::wal::ACTIVE_FILE_NAME);
    append_raw_bytes(&active_path, &encode_frame(&put_record(b"active", 2, 1)));
    let mut torn_frame = encode_frame(&put_record(b"torn", 3, 1));
    torn_frame.truncate(torn_frame.len() - 3);
    append_raw_bytes(&active_path, &torn_frame);
    let mut memtables = HashMap::new();

    // Act
    let stats =
        replay_wal_with_policy(&storage, &wal_dir, &mut memtables, ReplayPolicy::Strict).unwrap();

    // Assert
    assert!(!stats.had_corruption);
    assert_eq!(stats.record_count, 2);
    assert_eq!(
        memtables[&0].get(b"sealed").unwrap(),
        Some(b"value".to_vec())
    );
    assert_eq!(
        memtables[&0].get(b"active").unwrap(),
        Some(b"value".to_vec())
    );
    assert_eq!(memtables[&0].get(b"torn").unwrap(), None);
}

#[test]
fn should_fail_strict_recovery_given_corrupted_length_before_valid_active_wal_suffix() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");
    let wal_path = wal_subdir.join(crate::wal::ACTIVE_FILE_NAME);

    append_raw_bytes(&wal_path, &wal_with_corrupted_length_before_valid_suffix());
    let mut memtables = HashMap::new();

    // Act
    let error = replay_wal_with_policy(&storage, &wal_dir, &mut memtables, ReplayPolicy::Strict)
        .expect_err("strict recovery must reject a length field that hides a valid WAL suffix");

    // Assert
    match error {
        MidgeError::Corruption(message) => {
            assert!(message.contains("hides a verified later frame"));
        }
        other => panic!("expected corruption error, got {other:?}"),
    }
}

#[test]
fn should_mark_corruption_when_salvaging_corrupted_length_before_valid_active_suffix() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");
    let wal_path = wal_subdir.join(crate::wal::ACTIVE_FILE_NAME);
    append_raw_bytes(&wal_path, &wal_with_corrupted_length_before_valid_suffix());
    let mut memtables = HashMap::new();

    // Act
    let stats = replay_wal_with_policy(
        &storage,
        &wal_dir,
        &mut memtables,
        ReplayPolicy::SalvageValidPrefix,
    )
    .expect("salvage recovery must preserve only the verified prefix");

    // Assert
    assert!(stats.had_corruption);
    assert_eq!(stats.record_count, 1);
    assert_eq!(
        memtables[&0].get(b"prefix").unwrap(),
        Some(b"value".to_vec())
    );
    assert_eq!(memtables[&0].get(b"suffix").unwrap(), None);
}

#[test]
fn should_tolerate_zero_filled_final_active_wal_tail_given_strict_recovery() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");
    let wal_path = wal_subdir.join(crate::wal::ACTIVE_FILE_NAME);
    append_raw_bytes(&wal_path, &encode_frame(&put_record(b"prefix", 1, 1)));
    append_raw_bytes(&wal_path, &[0_u8; crate::wal::frame::WAL_FRAME_HEADER_LEN]);
    let mut memtables = HashMap::new();

    // Act
    let stats = replay_wal_with_policy(&storage, &wal_dir, &mut memtables, ReplayPolicy::Strict)
        .expect("strict recovery must accept an unwritten zero-filled active WAL tail");

    // Assert
    assert!(!stats.had_corruption);
    assert_eq!(stats.record_count, 1);
    assert_eq!(
        memtables[&0].get(b"prefix").unwrap(),
        Some(b"value".to_vec())
    );
}

#[test]
fn should_skip_stale_writer_epoch_records_given_interleaved_epochs_when_replaying_multiple_segments(
) {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");

    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();

        let fresh = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"fresh"),
            Some(Bytes::from_static(b"v2")),
            1,
            2,
        );
        writer.append_record(&fresh).unwrap();

        let stale = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"stale"),
            Some(Bytes::from_static(b"v1")),
            2,
            1,
        );
        writer.append_record(&stale).unwrap();
        writer.sync().unwrap();
    }

    let mut memtables = HashMap::new();

    // Act
    let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

    // Assert
    assert_eq!(stats.max_epoch_seen, 2);
    assert_eq!(stats.stale_records_skipped, 1);
    assert_eq!(memtables[&0].get(b"fresh").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(memtables[&0].get(b"stale").unwrap(), None);
}

#[test]
fn should_skip_stale_writer_epoch_records_seen_before_fresh_epoch() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");

    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();

        let stale = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"stale-first"),
            Some(Bytes::from_static(b"v1")),
            3,
            1,
        );
        writer.append_record(&stale).unwrap();

        let fresh = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"fresh-second"),
            Some(Bytes::from_static(b"v2")),
            2,
            2,
        );
        writer.append_record(&fresh).unwrap();
        writer.sync().unwrap();
    }

    let mut memtables = HashMap::new();

    // Act
    let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

    // Assert
    assert_eq!(stats.max_epoch_seen, 2);
    assert_eq!(stats.stale_records_skipped, 1);
    assert_eq!(
        memtables[&0].get(b"fresh-second").unwrap(),
        Some(b"v2".to_vec())
    );
    assert_eq!(memtables[&0].get(b"stale-first").unwrap(), None);
}

#[test]
fn should_skip_lower_epoch_record_when_seen_after_fresh_epoch_with_lower_sequence() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");

    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new("wal.log", fs as Arc<dyn crate::io::Fs>).unwrap();

        let valid_old_epoch = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"valid-old-epoch"),
            Some(Bytes::from_static(b"v1")),
            1,
            1,
        );
        writer.append_record(&valid_old_epoch).unwrap();

        let fresh_epoch = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"fresh-epoch"),
            Some(Bytes::from_static(b"v2")),
            10,
            2,
        );
        writer.append_record(&fresh_epoch).unwrap();

        let stale_lower_sequence = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"zombie-low-seq"),
            Some(Bytes::from_static(b"stale")),
            2,
            1,
        );
        writer.append_record(&stale_lower_sequence).unwrap();
        writer.sync().unwrap();
    }

    let mut memtables = HashMap::new();

    // Act
    let stats = replay_wal(&storage, &wal_dir, &mut memtables).unwrap();

    // Assert
    assert_eq!(stats.max_epoch_seen, 2);
    assert_eq!(stats.record_count, 2);
    assert_eq!(stats.stale_records_skipped, 1);
    assert_eq!(
        memtables[&0].get(b"valid-old-epoch").unwrap(),
        Some(b"v1".to_vec())
    );
    assert_eq!(
        memtables[&0].get(b"fresh-epoch").unwrap(),
        Some(b"v2".to_vec())
    );
    assert_eq!(memtables[&0].get(b"zombie-low-seq").unwrap(), None);
}

#[test]
fn should_preserve_raw_value_given_forward_clock_skew_during_wal_replay_when_recovering() {
    // Arrange
    let mut record = WalRecord::new(
        WalOpKind::Put,
        Bytes::from_static(b"ttl-key"),
        Some(Bytes::from_static(b"ttl-value")),
        7,
        1,
    );
    record.expiration = Some(20_000);
    let mut memtables = HashMap::new();

    // Act
    super::apply_record(&record, &mut memtables).expect("apply recovered record");
    let visible_before_expiration = memtables[&0]
        .get_key_state_at_with_time(b"ttl-key", u64::MAX, 19_999)
        .expect("read recovered value");
    let masked_at_expiration = memtables[&0]
        .get_key_state_at_with_time(b"ttl-key", u64::MAX, 20_000)
        .expect("read expired value");

    // Assert
    assert!(matches!(
        visible_before_expiration,
        crate::sst::types::KeyState::Value(value, 7, Some(20_000), _)
            if value.as_ref() == b"ttl-value"
    ));
    assert_eq!(
        masked_at_expiration,
        crate::sst::types::KeyState::Tombstone(7)
    );
}
