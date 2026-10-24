use super::*;
use crate::config::DEFAULT_STORAGE_IO_TIMEOUT;
use crate::io::{Fs, FsPath, OpenMode, OpenOptions, RealFs};
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
    let first = put_record(b"duplicate", 7);
    let mut conflicting = put_record(b"duplicate", 7);
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
    let record = WalRecord::new(WalOpKind::DeleteRange, Bytes::from_static(b"a"), None, 1);
    assert_eq!(record.range_end, None);

    // Act
    let result = apply_record(&record, &mut memtables);

    // Assert
    assert!(
        matches!(result, Err(MidgeError::Corruption(_))),
        "{result:?}"
    );
}

fn put_record(key: &'static [u8], sequence: u64) -> WalRecord {
    WalRecord::new(
        WalOpKind::Put,
        Bytes::from_static(key),
        Some(Bytes::from_static(b"value")),
        sequence,
    )
}

fn wal_with_corrupted_length_before_valid_suffix() -> Vec<u8> {
    let prefix = encode_frame(&put_record(b"prefix", 1));
    let hidden = encode_frame(&put_record(b"hidden", 2));
    let suffix = encode_frame(&put_record(b"suffix", 3));
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
    // Arrange: a WAL segment on a crash device, whose open faults count the
    // opens of that segment.
    let device = crate::simulation::crash_fs::CrashDevice::new();
    let storage = device.fs();
    let wal_dir = FsPath::new("wal");
    storage
        .create_dir_all(&wal_dir)
        .expect("create WAL directory");
    let segment = FsPath::new(format!("wal/{}", crate::wal::segment_file_name(1)));
    let bytes = (1..=128)
        .map(|sequence| encode_frame(&put_record(b"key", sequence)))
        .collect::<Vec<_>>()
        .concat();
    let mut file = storage
        .open(
            &segment,
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadWrite,
                create: true,
                create_new: true,
                truncate: false,
            },
        )
        .expect("create WAL fixture");
    file.append(bytes::Bytes::from(bytes))
        .expect("write WAL fixture");
    drop(file);
    let segments = crate::wal::recovery::fixture::wrap_chain(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[1], None),
        0,
    );
    let open_fault = |nth| crate::simulation::crash_fs::Fault {
        op: crate::simulation::crash_fs::FaultOp::Open,
        path: segment.0.clone(),
        nth,
        effect: crate::simulation::crash_fs::FaultEffect::Before,
        error: crate::simulation::crash_fs::FaultError::Io,
    };

    let replay = |memtables: &mut HashMap<_, _>| {
        replay_wal_with_frontiers_within(
            &storage,
            &segments,
            memtables,
            ReplayPolicy::Strict,
            &crate::metadata::frontiers::FlushFrontiers::default(),
            None,
        )
    };

    // Act: the first open fails; then a second open would have failed.
    let first = device.inject(open_fault(1));
    let failed = replay(&mut HashMap::new());
    device.disarm_faults();
    let second = device.inject(open_fault(2));
    let replayed = replay(&mut HashMap::new());

    // Assert: one pass reads the WAL, opening it once (#843).
    assert!(device.fired(first) && failed.is_err());
    assert!(!device.fired(second), "replay opened the WAL a second time");
    assert_eq!(replayed.expect("replay").record_count, 128);
}

#[test]
fn should_not_tolerate_generic_corruption_based_on_incomplete_tail_error_text() {
    // Arrange
    let replay_file = ReplayFile {
        id: 1,
        path: FsPath::new(crate::wal::segment_file_name(1)),
        kind: ReplayFileKind::FinalActive,
        expect: super::Expectation::Member {
            member: crate::wal::ledger::Member {
                open: crate::wal::format::OpenFacts {
                    id: 1,
                    epoch: 0,
                    first_seq: 1,
                    prev: crate::wal::format::ChainLink::GENESIS,
                },
                sealed: None,
                durable_bytes: None,
            },
            holder: crate::wal::format::segment::FIXTURE_HOLDER,
        },
    };
    let failure = ReplayFailure::Error(MidgeError::Corruption(
        "Incomplete WAL record text alone is not a typed tail".to_string(),
    ));

    // Act
    let action = replay_error_action(&replay_file, ReplayPolicy::Strict, &failure, 0);

    // Assert
    assert_eq!(action, ReplayErrorAction::Fail);
}

#[test]
fn should_return_empty_stats_when_no_segment_is_replayed() {
    // Arrange
    let mut memtables = HashMap::new();
    let dir = TempDir::new().unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let non_existent = FsPath::new("midge_nonexistent_wal_dir_12345");

    // Act
    let stats = replay_fixture(
        &storage,
        &crate::wal::recovery::fixture::segments(&non_existent, &[], None),
        &mut memtables,
    )
    .unwrap();

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
        let writer = FsWalWriterIo::new(
            &crate::wal::segment_file_name(1),
            fs as Arc<dyn crate::io::Fs>,
        )
        .unwrap();
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"test_key"),
            Some(Bytes::from_static(b"test_value")),
            1,
        );
        writer.append_record_accounted(&record).unwrap();
        writer
            .sync_with_timeout(DEFAULT_STORAGE_IO_TIMEOUT)
            .unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let _stats = replay_fixture(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
        &mut memtables,
    )
    .unwrap();

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
        let writer = FsWalWriterIo::new(
            &crate::wal::segment_file_name(1),
            fs as Arc<dyn crate::io::Fs>,
        )
        .unwrap();
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"test_key"),
            Some(Bytes::from_static(b"test_value")),
            1,
        );
        writer.append_record_accounted(&record).unwrap();
        writer
            .sync_with_timeout(DEFAULT_STORAGE_IO_TIMEOUT)
            .unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let stats = replay_fixture(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
        &mut memtables,
    )
    .unwrap();

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
        let writer = FsWalWriterIo::new(
            &crate::wal::segment_file_name(1),
            fs as Arc<dyn crate::io::Fs>,
        )
        .unwrap();
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"test_key"),
            Some(Bytes::from_static(b"test_value")),
            1,
        );
        writer.append_record_accounted(&record).unwrap();
        writer
            .sync_with_timeout(DEFAULT_STORAGE_IO_TIMEOUT)
            .unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let stats = replay_fixture(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
        &mut memtables,
    )
    .unwrap();

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
        let writer = FsWalWriterIo::new(
            &crate::wal::segment_file_name(1),
            fs as Arc<dyn crate::io::Fs>,
        )
        .unwrap();
        let put_record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"test_key"),
            Some(Bytes::from_static(b"test_value")),
            1,
        );
        writer.append_record_accounted(&put_record).unwrap();

        let delete_record =
            WalRecord::new(WalOpKind::Delete, Bytes::from_static(b"test_key"), None, 2);
        writer.append_record_accounted(&delete_record).unwrap();
        writer
            .sync_with_timeout(DEFAULT_STORAGE_IO_TIMEOUT)
            .unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let _stats = replay_fixture(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
        &mut memtables,
    )
    .unwrap();

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
        let writer = FsWalWriterIo::new(
            &crate::wal::segment_file_name(1),
            fs as Arc<dyn crate::io::Fs>,
        )
        .unwrap();
        let put_record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"test_key"),
            Some(Bytes::from_static(b"test_value")),
            1,
        );
        writer.append_record_accounted(&put_record).unwrap();

        let delete_record =
            WalRecord::new(WalOpKind::Delete, Bytes::from_static(b"test_key"), None, 2);
        writer.append_record_accounted(&delete_record).unwrap();
        writer
            .sync_with_timeout(DEFAULT_STORAGE_IO_TIMEOUT)
            .unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let stats = replay_fixture(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
        &mut memtables,
    )
    .unwrap();

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
        let writer = FsWalWriterIo::new(
            &crate::wal::segment_file_name(1),
            fs as Arc<dyn crate::io::Fs>,
        )
        .unwrap();

        let record_cf0 = WalRecord::new_cf(
            0,
            WalOpKind::Put,
            Bytes::from_static(b"key0"),
            Some(Bytes::from_static(b"value0")),
            1,
        );
        writer.append_record_accounted(&record_cf0).unwrap();

        let record_cf1 = WalRecord::new_cf(
            1,
            WalOpKind::Put,
            Bytes::from_static(b"key1"),
            Some(Bytes::from_static(b"value1")),
            2,
        );
        writer.append_record_accounted(&record_cf1).unwrap();
        writer
            .sync_with_timeout(DEFAULT_STORAGE_IO_TIMEOUT)
            .unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let _stats = replay_fixture(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
        &mut memtables,
    )
    .unwrap();

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
        let writer = FsWalWriterIo::new(
            &crate::wal::segment_file_name(1),
            fs as Arc<dyn crate::io::Fs>,
        )
        .unwrap();

        let record_cf0 = WalRecord::new_cf(
            0,
            WalOpKind::Put,
            Bytes::from_static(b"key0"),
            Some(Bytes::from_static(b"value0")),
            1,
        );
        writer.append_record_accounted(&record_cf0).unwrap();

        let record_cf1 = WalRecord::new_cf(
            1,
            WalOpKind::Put,
            Bytes::from_static(b"key1"),
            Some(Bytes::from_static(b"value1")),
            2,
        );
        writer.append_record_accounted(&record_cf1).unwrap();
        writer
            .sync_with_timeout(DEFAULT_STORAGE_IO_TIMEOUT)
            .unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let _stats = replay_fixture(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
        &mut memtables,
    )
    .unwrap();

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
        let writer = FsWalWriterIo::new(
            &crate::wal::segment_file_name(1),
            fs as Arc<dyn crate::io::Fs>,
        )
        .unwrap();

        let record_cf0 = WalRecord::new_cf(
            0,
            WalOpKind::Put,
            Bytes::from_static(b"key0"),
            Some(Bytes::from_static(b"value0")),
            1,
        );
        writer.append_record_accounted(&record_cf0).unwrap();

        let record_cf1 = WalRecord::new_cf(
            1,
            WalOpKind::Put,
            Bytes::from_static(b"key1"),
            Some(Bytes::from_static(b"value1")),
            2,
        );
        writer.append_record_accounted(&record_cf1).unwrap();
        writer
            .sync_with_timeout(DEFAULT_STORAGE_IO_TIMEOUT)
            .unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let stats = replay_fixture(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
        &mut memtables,
    )
    .unwrap();

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
        let writer = FsWalWriterIo::new(
            &crate::wal::segment_file_name(1),
            fs as Arc<dyn crate::io::Fs>,
        )
        .unwrap();

        let record1 = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key1"),
            Some(Bytes::from_static(b"value1")),
            5,
        );
        writer.append_record_accounted(&record1).unwrap();

        // Sequences strictly increase within a segment (#843); gaps are
        // allowed.
        let record2 = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key2"),
            Some(Bytes::from_static(b"value2")),
            7,
        );
        writer.append_record_accounted(&record2).unwrap();

        let record3 = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key3"),
            Some(Bytes::from_static(b"value3")),
            10,
        );
        writer.append_record_accounted(&record3).unwrap();
        writer
            .sync_with_timeout(DEFAULT_STORAGE_IO_TIMEOUT)
            .unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let stats = replay_fixture(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
        &mut memtables,
    )
    .unwrap();

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
        let _writer = FsWalWriterIo::new(
            &crate::wal::segment_file_name(1),
            fs as Arc<dyn crate::io::Fs>,
        )
        .unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let stats = replay_fixture(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
        &mut memtables,
    )
    .unwrap();

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
        let writer = FsWalWriterIo::new(
            &crate::wal::segment_file_name(1),
            fs as Arc<dyn crate::io::Fs>,
        )
        .unwrap();
        let mut expired_record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"expired_key"),
            Some(Bytes::from_static(b"value")),
            1,
        );
        // Set expiration to the past (1 millisecond after epoch)
        expired_record.expiration = Some(1);
        writer.append_record_accounted(&expired_record).unwrap();

        let mut future_record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"future_key"),
            Some(Bytes::from_static(b"value")),
            2,
        );
        // Set expiration to far future
        future_record.expiration = Some(u64::MAX);
        writer.append_record_accounted(&future_record).unwrap();

        writer
            .sync_with_timeout(DEFAULT_STORAGE_IO_TIMEOUT)
            .unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let stats = replay_fixture(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
        &mut memtables,
    )
    .unwrap();

    // Assert
    let recovered_memtable = &memtables[&0];
    assert!(matches!(
        recovered_memtable
            .get_key_state_at_with_time(b"expired_key", u64::MAX, 0)
            .unwrap(),
        crate::types::KeyState::Value(_, 1, Some(1), _)
    ));
    assert_eq!(
        recovered_memtable
            .get_key_state_at_with_time(b"expired_key", u64::MAX, 1)
            .unwrap(),
        crate::types::KeyState::Tombstone(1)
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
        let writer = FsWalWriterIo::new(
            &crate::wal::segment_file_name(1),
            fs as Arc<dyn crate::io::Fs>,
        )
        .unwrap();
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key123"),         // 6 bytes
            Some(Bytes::from_static(b"value456")), // 8 bytes
            1,
        );
        writer.append_record_accounted(&record).unwrap();
        writer
            .sync_with_timeout(DEFAULT_STORAGE_IO_TIMEOUT)
            .unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let stats = replay_fixture(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
        &mut memtables,
    )
    .unwrap();

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
        let writer = FsWalWriterIo::new(
            &crate::wal::segment_file_name(1),
            fs as Arc<dyn crate::io::Fs>,
        )
        .unwrap();

        // Add a put record first
        let put_record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            1,
        );
        writer.append_record_accounted(&put_record).unwrap();

        // DeleteRange is currently a no-op, but should not cause errors
        let mut delete_range_record = WalRecord::new(
            WalOpKind::DeleteRange,
            Bytes::from_static(b"start"),
            None,
            2,
        );
        delete_range_record.range_end = Some(Bytes::from_static(b"end"));
        writer
            .append_record_accounted(&delete_range_record)
            .unwrap();

        writer
            .sync_with_timeout(DEFAULT_STORAGE_IO_TIMEOUT)
            .unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let stats = replay_fixture(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
        &mut memtables,
    )
    .unwrap();

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
        let writer = FsWalWriterIo::new(
            &crate::wal::segment_file_name(1),
            fs as Arc<dyn crate::io::Fs>,
        )
        .unwrap();

        let begin_record =
            WalRecord::new(WalOpKind::TxnBegin, Bytes::from_static(b"txn_key"), None, 1);
        writer.append_record_accounted(&begin_record).unwrap();

        let put_record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            2,
        );
        writer.append_record_accounted(&put_record).unwrap();

        let commit_record = WalRecord::new(
            WalOpKind::TxnCommit,
            Bytes::from_static(b"txn_key"),
            None,
            3,
        );
        writer.append_record_accounted(&commit_record).unwrap();

        writer
            .sync_with_timeout(DEFAULT_STORAGE_IO_TIMEOUT)
            .unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let stats = replay_fixture(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
        &mut memtables,
    )
    .unwrap();

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
        let writer = FsWalWriterIo::new(
            &crate::wal::segment_file_name(1),
            fs as Arc<dyn crate::io::Fs>,
        )
        .unwrap();

        let mut put_record = WalRecord::new_cf(
            0,
            WalOpKind::Put,
            Bytes::from_static(b"batch-key"),
            Some(Bytes::from_static(b"batch-value")),
            2,
        );
        put_record.txn_id = Some(11);

        let payload =
            crate::wal::encoding::encode_txn_batch_payload(11, 1, 3, 0, &[put_record]).unwrap();
        let mut batch_record = WalRecord::new_cf(
            0,
            WalOpKind::TxnBatch,
            Bytes::from_static(b"txn"),
            Some(payload),
            3,
        );
        batch_record.txn_id = Some(11);

        writer.append_record_accounted(&batch_record).unwrap();
        writer
            .sync_with_timeout(DEFAULT_STORAGE_IO_TIMEOUT)
            .unwrap();
    }

    let mut memtables = HashMap::new();
    let stats = replay_fixture(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
        &mut memtables,
    )
    .unwrap();

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
fn should_replay_complete_retained_txn_batch_when_commit_exceeds_frontier() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");
    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new(
            &crate::wal::segment_file_name(1),
            fs as Arc<dyn crate::io::Fs>,
        )
        .unwrap();
        let mut covered = put_record(b"covered", 2);
        covered.txn_id = Some(11);
        let mut uncovered = put_record(b"uncovered", 3);
        uncovered.txn_id = Some(11);
        let payload =
            crate::wal::encoding::encode_txn_batch_payload(11, 1, 4, 0, &[covered, uncovered])
                .unwrap();
        let mut batch = WalRecord::new_cf(
            0,
            WalOpKind::TxnBatch,
            Bytes::from_static(b"txn"),
            Some(payload),
            4,
        );
        batch.txn_id = Some(11);
        writer.append_record_accounted(&batch).unwrap();
        writer
            .sync_with_timeout(DEFAULT_STORAGE_IO_TIMEOUT)
            .unwrap();
    }
    let frontiers =
        serde_json::from_value(serde_json::json!([{"cf_id": 0, "through_sequence": 2}])).unwrap();

    // Act
    let mut memtables = HashMap::new();
    let segments = crate::wal::recovery::fixture::wrap_chain(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
        0,
    );
    let stats = replay_wal_with_frontiers_within(
        &storage,
        &segments,
        &mut memtables,
        ReplayPolicy::Strict,
        &frontiers,
        None,
    )
    .unwrap();

    // Assert
    assert_eq!(stats.record_count, 1);
    let recovered = &memtables[&0];
    assert_eq!(recovered.get(b"covered").unwrap(), Some(b"value".to_vec()));
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
        let writer = FsWalWriterIo::new(
            &crate::wal::segment_file_name(1),
            fs as Arc<dyn crate::io::Fs>,
        )
        .unwrap();

        let mut begin_record =
            WalRecord::new(WalOpKind::TxnBegin, Bytes::from_static(b"txn"), None, 1);
        begin_record.txn_id = Some(42);
        writer.append_record_accounted(&begin_record).unwrap();

        let mut put_record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            2,
        );
        put_record.txn_id = Some(42);
        writer.append_record_accounted(&put_record).unwrap();

        writer
            .sync_with_timeout(DEFAULT_STORAGE_IO_TIMEOUT)
            .unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let stats = replay_fixture(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
        &mut memtables,
    )
    .unwrap();

    // Assert
    assert_eq!(stats.record_count, 2);
    assert!(
        !memtables.contains_key(&0),
        "incomplete transactions must not materialize a recovered memtable entry"
    );
}

#[test]
fn should_isolate_reused_transaction_id_by_writer_epoch() {
    // Arrange: epoch 11 crashes after writing an uncommitted operation in
    // segment 1. Epoch 12 then reuses transaction id 1 in segment 2 and
    // commits different data. Transaction ids restart in every process; the
    // segment headers' epochs keep the two apart.
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let marker = |op, seq| {
        let mut record = WalRecord::new(op, Bytes::from_static(b"txn"), None, seq);
        record.txn_id = Some(1);
        record
    };
    let member = |key: &'static [u8], value: &'static [u8], seq| {
        let mut record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(key),
            Some(Bytes::from_static(value)),
            seq,
        );
        record.txn_id = Some(1);
        record
    };
    let (first, link) = crate::wal::format::segment::SegmentBuilder::new(
        1,
        11,
        crate::wal::format::ChainLink::GENESIS,
    )
    .record(&marker(WalOpKind::TxnBegin, 1))
    .record(&member(b"orphaned", b"must-stay-hidden", 2))
    .seal();
    let second = crate::wal::format::segment::SegmentBuilder::new(2, 12, link)
        .record(&marker(WalOpKind::TxnBegin, 3))
        .record(&member(b"committed", b"visible", 4))
        .record(&marker(WalOpKind::TxnCommit, 5))
        .open();
    std::fs::write(wal_subdir.join(crate::wal::segment_file_name(1)), first).unwrap();
    std::fs::write(wal_subdir.join(crate::wal::segment_file_name(2)), second).unwrap();

    // Act
    let mut memtables = HashMap::new();
    replay_fixture(
        &storage,
        &crate::wal::recovery::fixture::segments(&FsPath::new("wal"), &[1], Some(2)),
        &mut memtables,
    )
    .unwrap();

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
        let writer = FsWalWriterIo::new(
            &crate::wal::segment_file_name(1),
            fs as Arc<dyn crate::io::Fs>,
        )
        .unwrap();
        let mut begin = WalRecord::new(WalOpKind::TxnBegin, Bytes::from_static(b"txn"), None, 1);
        begin.txn_id = Some(txn_id);
        writer.append_record_accounted(&begin).unwrap();
        for index in 0..operation_count {
            let mut put = WalRecord::new(
                WalOpKind::Put,
                Bytes::from(format!("spooled-{index:03}")),
                Some(Bytes::from(vec![b'x'; 4 * 1024])),
                index + 2,
            );
            put.txn_id = Some(txn_id);
            writer.append_record_accounted(&put).unwrap();
        }
        let mut commit = WalRecord::new(
            WalOpKind::TxnCommit,
            Bytes::from_static(b"txn"),
            None,
            operation_count + 2,
        );
        commit.txn_id = Some(txn_id);
        writer.append_record_accounted(&commit).unwrap();
        writer
            .sync_with_timeout(DEFAULT_STORAGE_IO_TIMEOUT)
            .unwrap();
    }

    // Act
    let mut memtables = HashMap::new();
    let stats = replay_fixture(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
        &mut memtables,
    )
    .unwrap();

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
    let wal_path = wal_subdir.join(crate::wal::segment_file_name(1));

    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new(
            &crate::wal::segment_file_name(1),
            fs as Arc<dyn crate::io::Fs>,
        )
        .unwrap();
        let prefix = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"prefix"),
            Some(Bytes::from_static(b"value")),
            1,
        );
        writer.append_record_accounted(&prefix).unwrap();

        let mut batched_put = WalRecord::new_cf(
            0,
            WalOpKind::Put,
            Bytes::from_static(b"truncated"),
            Some(Bytes::from_static(b"should-not-appear")),
            3,
        );
        batched_put.txn_id = Some(90);
        let payload =
            crate::wal::encoding::encode_txn_batch_payload(90, 2, 4, 0, &[batched_put]).unwrap();
        let mut batch_record = WalRecord::new_cf(
            0,
            WalOpKind::TxnBatch,
            Bytes::from_static(b"txn"),
            Some(payload),
            4,
        );
        batch_record.txn_id = Some(90);
        writer.append_record_accounted(&batch_record).unwrap();
        writer
            .sync_with_timeout(DEFAULT_STORAGE_IO_TIMEOUT)
            .unwrap();
    }

    let bytes = std::fs::read(&wal_path).unwrap();
    std::fs::write(&wal_path, &bytes[..bytes.len() - 3]).unwrap();

    let mut memtables = HashMap::new();
    let stats = replay_fixture_with_policy(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
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
    let wal_path = wal_subdir.join(crate::wal::segment_file_name(1));

    let record = WalRecord::new(
        WalOpKind::Put,
        Bytes::from_static(b"crc_key"),
        Some(Bytes::from_static(b"crc_value")),
        1,
    );
    let payload = crate::wal::encoding::encode(&record).unwrap();
    let mut frame = Vec::new();
    crate::wal::frame::append_frame(&mut frame, &payload).unwrap();
    frame[4] ^= 0x5a;
    append_raw_bytes(&wal_path, &frame);

    let mut memtables = HashMap::new();

    // Act
    let err = replay_fixture_with_policy(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
        &mut memtables,
        ReplayPolicy::Strict,
    )
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
    let wal_path = wal_subdir.join(crate::wal::segment_file_name(1));

    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new(
            &crate::wal::segment_file_name(1),
            fs as Arc<dyn crate::io::Fs>,
        )
        .unwrap();
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"good"),
            Some(Bytes::from_static(b"value")),
            1,
        );
        writer.append_record_accounted(&record).unwrap();
        writer
            .sync_with_timeout(DEFAULT_STORAGE_IO_TIMEOUT)
            .unwrap();
    }

    let bad_record = WalRecord::new(
        WalOpKind::Put,
        Bytes::from_static(b"bad"),
        Some(Bytes::from_static(b"value")),
        2,
    );
    let payload = crate::wal::encoding::encode(&bad_record).unwrap();
    let mut frame = Vec::new();
    crate::wal::frame::append_frame(&mut frame, &payload).unwrap();
    frame[5] ^= 0x11;
    append_raw_bytes(&wal_path, &frame);

    let mut memtables = HashMap::new();

    // Act
    let stats = replay_fixture_with_policy(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
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
    let wal_path = wal_subdir.join(crate::wal::segment_file_name(1));

    {
        let fs = Arc::new(RealFs::new(&wal_subdir).unwrap());
        let writer = FsWalWriterIo::new(
            &crate::wal::segment_file_name(1),
            fs as Arc<dyn crate::io::Fs>,
        )
        .unwrap();
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"good"),
            Some(Bytes::from_static(b"value")),
            1,
        );
        writer.append_record_accounted(&record).unwrap();
        writer
            .sync_with_timeout(DEFAULT_STORAGE_IO_TIMEOUT)
            .unwrap();
    }

    let tail_record = WalRecord::new(
        WalOpKind::Put,
        Bytes::from_static(b"tail"),
        Some(Bytes::from_static(b"value")),
        2,
    );
    let payload = crate::wal::encoding::encode(&tail_record).unwrap();
    let mut frame = Vec::new();
    crate::wal::frame::append_frame(&mut frame, &payload).unwrap();
    frame.truncate(frame.len() - 3);
    append_raw_bytes(&wal_path, &frame);

    let mut memtables = HashMap::new();

    // Act
    let stats = replay_fixture_with_policy(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
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

    // Sealed segment 1 lost its footer to a torn final frame.
    let mut torn_frame = encode_frame(&put_record(b"torn", 2));
    torn_frame.truncate(torn_frame.len() - 3);
    let torn_segment = crate::wal::format::segment::SegmentBuilder::new(
        1,
        0,
        crate::wal::format::ChainLink::GENESIS,
    )
    .record(&put_record(b"prefix", 1))
    .bytes(&torn_frame)
    .open();
    append_raw_bytes(&first_segment, &torn_segment);
    append_raw_bytes(
        &wal_subdir.join(crate::wal::segment_file_name(2)),
        &encode_frame(&put_record(b"later-sealed", 3)),
    );
    append_raw_bytes(
        &wal_subdir.join(crate::wal::segment_file_name(3)),
        &encode_frame(&put_record(b"active", 4)),
    );
    let mut memtables = HashMap::new();

    // Act
    let error = replay_fixture_with_policy(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[1, 2], Some(3)),
        &mut memtables,
        ReplayPolicy::Strict,
    )
    .unwrap_err();

    // Assert
    match error {
        MidgeError::Corruption(message) => {
            assert!(message.contains("Incomplete WAL record"), "{message}");
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

    // Sealed segment 1 lost its footer to a torn final frame.
    let mut torn_frame = encode_frame(&put_record(b"torn", 2));
    torn_frame.truncate(torn_frame.len() - 3);
    let torn_segment = crate::wal::format::segment::SegmentBuilder::new(
        1,
        0,
        crate::wal::format::ChainLink::GENESIS,
    )
    .record(&put_record(b"prefix", 1))
    .bytes(&torn_frame)
    .open();
    append_raw_bytes(&first_segment, &torn_segment);
    append_raw_bytes(
        &wal_subdir.join(crate::wal::segment_file_name(2)),
        &encode_frame(&put_record(b"later-sealed", 3)),
    );
    append_raw_bytes(
        &wal_subdir.join(crate::wal::segment_file_name(3)),
        &encode_frame(&put_record(b"active", 4)),
    );
    let mut memtables = HashMap::new();

    // Act
    let stats = replay_fixture_with_policy(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[1, 2], Some(3)),
        &mut memtables,
        ReplayPolicy::SalvageValidPrefix,
    )
    .unwrap();

    // Assert
    assert!(stats.had_corruption);
    assert_eq!(stats.record_count, 1);
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
        &encode_frame(&put_record(b"sealed", 1)),
    );
    let active_path = wal_subdir.join(crate::wal::segment_file_name(2));
    append_raw_bytes(&active_path, &encode_frame(&put_record(b"active", 2)));
    let mut torn_frame = encode_frame(&put_record(b"torn", 3));
    torn_frame.truncate(torn_frame.len() - 3);
    append_raw_bytes(&active_path, &torn_frame);
    let mut memtables = HashMap::new();

    // Act
    let stats = replay_fixture_with_policy(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[1], Some(2)),
        &mut memtables,
        ReplayPolicy::Strict,
    )
    .unwrap();

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
fn should_report_verified_prefix_without_modifying_file_when_tolerating_active_wal_tail() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");
    let active_path = wal_subdir.join(crate::wal::segment_file_name(1));
    let first = encode_frame(&put_record(b"first", 1));
    let second = encode_frame(&put_record(b"second", 2));
    append_raw_bytes(&active_path, &first);
    append_raw_bytes(&active_path, &second);
    let mut torn_frame = encode_frame(&put_record(b"torn", 3));
    torn_frame.truncate(torn_frame.len() - 3);
    append_raw_bytes(&active_path, &torn_frame);
    let segments = crate::wal::recovery::fixture::wrap_chain(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
        0,
    );
    let original_len = std::fs::metadata(&active_path).unwrap().len();
    let mut memtables = HashMap::new();

    // Act
    let stats =
        replay_fixture_with_policy(&storage, &segments, &mut memtables, ReplayPolicy::Strict)
            .unwrap();

    // Assert
    assert_eq!(
        stats.tolerated_active_tail,
        Some(ToleratedActiveTail {
            path: FsPath::new(format!("wal/{}", crate::wal::segment_file_name(1))),
            valid_bytes: (crate::wal::format::HEADER_LEN + first.len() + second.len()) as u64,
        })
    );
    assert_eq!(
        std::fs::metadata(&active_path).unwrap().len(),
        original_len,
        "replay must stay read-only; truncation belongs to the owner reopening for append"
    );
}

#[test]
fn should_not_report_tolerated_tail_when_active_wal_is_complete() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");
    append_raw_bytes(
        &wal_subdir.join(crate::wal::segment_file_name(1)),
        &encode_frame(&put_record(b"complete", 1)),
    );
    let mut memtables = HashMap::new();

    // Act
    let stats = replay_fixture_with_policy(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
        &mut memtables,
        ReplayPolicy::Strict,
    )
    .unwrap();

    // Assert
    assert_eq!(stats.tolerated_active_tail, None);
}

#[test]
fn should_fail_strict_recovery_given_corrupted_length_before_valid_active_wal_suffix() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");
    let wal_path = wal_subdir.join(crate::wal::segment_file_name(1));

    append_raw_bytes(&wal_path, &wal_with_corrupted_length_before_valid_suffix());
    let mut memtables = HashMap::new();

    // Act
    let error = replay_fixture_with_policy(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
        &mut memtables,
        ReplayPolicy::Strict,
    )
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
    let wal_path = wal_subdir.join(crate::wal::segment_file_name(1));
    append_raw_bytes(&wal_path, &wal_with_corrupted_length_before_valid_suffix());
    let mut memtables = HashMap::new();

    // Act
    let stats = replay_fixture_with_policy(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
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
    let wal_path = wal_subdir.join(crate::wal::segment_file_name(1));
    append_raw_bytes(&wal_path, &encode_frame(&put_record(b"prefix", 1)));
    append_raw_bytes(&wal_path, &[0_u8; crate::wal::frame::WAL_FRAME_HEADER_LEN]);
    let mut memtables = HashMap::new();

    // Act
    let stats = replay_fixture_with_policy(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[], Some(1)),
        &mut memtables,
        ReplayPolicy::Strict,
    )
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
fn should_replay_distinct_records_that_share_a_key_across_wal_files() {
    // Arrange: same key and value in two files at different sequences is two
    // real writes, not a cross-file duplicate.
    let dir = TempDir::new().unwrap();
    let wal_subdir = dir.path().join("wal");
    std::fs::create_dir(&wal_subdir).unwrap();
    let storage = RealFs::new(dir.path()).unwrap();
    let wal_dir = FsPath::new("wal");
    append_raw_bytes(
        &wal_subdir.join(crate::wal::segment_file_name(1)),
        &encode_frame(&put_record(b"shared-key", 1)),
    );
    append_raw_bytes(
        &wal_subdir.join(crate::wal::segment_file_name(2)),
        &encode_frame(&put_record(b"shared-key", 2)),
    );
    let mut memtables = HashMap::new();

    // Act
    let stats = replay_fixture(
        &storage,
        &crate::wal::recovery::fixture::segments(&wal_dir, &[1], Some(2)),
        &mut memtables,
    )
    .unwrap();

    // Assert
    assert_eq!(stats.record_count, 2, "both writes must be replayed");
    assert_eq!(stats.max_sequence, Some(2));
}

#[test]
fn should_preserve_raw_value_given_forward_clock_skew_during_wal_replay_when_recovering() {
    // Arrange
    let mut record = WalRecord::new(
        WalOpKind::Put,
        Bytes::from_static(b"ttl-key"),
        Some(Bytes::from_static(b"ttl-value")),
        7,
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
        crate::types::KeyState::Value(value, 7, Some(20_000), _)
            if value.as_ref() == b"ttl-value"
    ));
    assert_eq!(masked_at_expiration, crate::types::KeyState::Tombstone(7));
}

/// What one walker concluded about a local WAL file: whether it accepts the
/// file and, if so, its verified sequence and byte frontiers.
#[derive(Debug, PartialEq, Eq)]
enum WalkerVerdict {
    Accepted { max_sequence: u64, valid_bytes: u64 },
    Rejected,
}

/// Runs local replay and the cloud active-WAL inspector over the same
/// open-segment bytes. Both walk local files, so they must agree (#413, #487).
fn classify_with_both_walkers(bytes: &[u8]) -> (WalkerVerdict, WalkerVerdict) {
    let dir = TempDir::new().expect("create temp dir");
    std::fs::create_dir_all(dir.path().join("wal")).expect("create WAL dir");
    std::fs::write(
        dir.path()
            .join("wal")
            .join(crate::wal::segment_file_name(1)),
        bytes,
    )
    .expect("write WAL fixture");
    let storage = RealFs::new(dir.path()).expect("open fs");

    // Damaged bytes stay as they are: no wrapping, only the builder's epoch.
    let segments =
        crate::wal::recovery::fixture::segments_in_epoch(&FsPath::new("wal"), &[], Some(1), 1);
    let replay = match validate_wal_with_policy(&storage, &segments, ReplayPolicy::Strict, None) {
        Ok(stats) => WalkerVerdict::Accepted {
            max_sequence: stats.max_sequence.unwrap_or(0),
            valid_bytes: stats
                .tolerated_active_tail
                .as_ref()
                .map_or(bytes.len() as u64, |tail| tail.valid_bytes),
        },
        Err(_) => WalkerVerdict::Rejected,
    };

    let path = FsPath::new(format!("wal/{}", crate::wal::segment_file_name(1)));
    let file = storage
        .open(
            &path,
            OpenOptions {
                mode: OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )
        .expect("open WAL fixture");
    let limits = super::streaming::StreamingReplayLimits {
        max_frame_bytes: 128 * 1_024,
        max_pending_txn_bytes: 256 * 1_024,
        max_memtable_encoded_bytes: 256 * 1_024,
        target_memtable_encoded_bytes: 256 * 1_024,
    };
    let inspector = match super::streaming::inspect_wal_file(file.as_ref(), &path, limits) {
        Ok(prefix) => WalkerVerdict::Accepted {
            max_sequence: prefix.max_sequence(),
            valid_bytes: prefix.valid_bytes as u64,
        },
        // Cloud recovery truncates a torn tail to the verified prefix, as
        // local replay tolerates it.
        Err(failure) if failure.is_incomplete_tail() => WalkerVerdict::Accepted {
            max_sequence: failure.verified_prefix().max_sequence(),
            valid_bytes: failure.verified_prefix().valid_bytes as u64,
        },
        Err(_) => WalkerVerdict::Rejected,
    };
    (replay, inspector)
}

/// An open segment holding `count` puts at sequences `1..=count`.
fn open_segment_with(count: u64) -> Vec<u8> {
    let mut builder = crate::wal::format::segment::SegmentBuilder::new(
        1,
        1,
        crate::wal::format::ChainLink::GENESIS,
    );
    for sequence in 1..=count {
        builder = builder.record(&put_record(b"key", sequence));
    }
    builder.open()
}

proptest::proptest! {
    #[test]
    fn should_classify_identically_across_walkers_when_given_fuzzed_wal_bytes(
        count in 1_u64..8,
        damage in proptest::option::of((proptest::prelude::any::<proptest::sample::Index>(), 1_u8..=255)),
        cut in proptest::option::of(proptest::prelude::any::<proptest::sample::Index>()),
    ) {
        // Arrange
        let mut bytes = open_segment_with(count);
        if let Some((index, mask)) = damage {
            let at = index.index(bytes.len());
            bytes[at] ^= mask;
        }
        if let Some(index) = cut {
            bytes.truncate(index.index(bytes.len()));
        }

        // Act
        let (replay, inspector) = classify_with_both_walkers(&bytes);

        // Assert
        proptest::prop_assert_eq!(inspector, replay, "bytes: {:?}", bytes);
    }
}

#[test]
fn should_not_skip_older_epoch_segments_when_a_regressed_segment_id_holds_newer_epoch() {
    // Arrange: the LR-F3 layout. Segments 5 and 6 hold epoch-1 acknowledged
    // writes. After the next-segment id regressed to 1 at an epoch-2 open,
    // rotation sealed the active log (epoch-1 tail, then epoch-2 records)
    // as segment 1, which now replays first. Each segment's header names
    // its writer's epoch and links the segment before it in replay order.
    let dir = TempDir::new().unwrap();
    let wal_dir = dir.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let seg = |id: u64| wal_dir.join(crate::wal::segment_file_name(id));
    let builder = crate::wal::format::segment::SegmentBuilder::new;
    let (one, link) = builder(1, 2, crate::wal::format::ChainLink::GENESIS)
        .record(&put_record(b"e1-tail", 3))
        .record(&put_record(b"e2-new", 4))
        .seal();
    let (five, link) = builder(5, 1, link)
        .first_seq(1)
        .record(&put_record(b"e1-acked-a", 1))
        .seal();
    let (six, link) = builder(6, 1, link)
        .first_seq(2)
        .record(&put_record(b"e1-acked-b", 2))
        .seal();
    let seven = builder(7, 2, link).open();
    for (id, bytes) in [(1, one), (5, five), (6, six), (7, seven)] {
        std::fs::write(seg(id), bytes).unwrap();
    }
    let storage = RealFs::new(dir.path()).unwrap();
    let mut memtables = HashMap::new();

    // Act
    let result = replay_fixture(
        &storage,
        &crate::wal::recovery::fixture::segments(&FsPath::new("wal"), &[1, 5, 6], Some(7)),
        &mut memtables,
    );

    // Assert: either replay refuses the inverted layout, or it keeps the
    // acknowledged epoch-1 writes. It must not drop them silently.
    match result {
        Err(_) => {}
        Ok(stats) => {
            let table = &memtables[&0];
            assert_eq!(
                (
                    table.get(b"e1-acked-a").unwrap().is_some(),
                    table.get(b"e1-acked-b").unwrap().is_some(),
                    stats.had_corruption,
                ),
                (true, true, false),
                "acknowledged epoch-1 writes were skipped as stale"
            );
        }
    }
}

/// Replay `segments` from `storage` through local recovery's production
/// path, applying every record.
fn replay_fixture_with_policy(
    storage: &dyn Fs,
    segments: &ReplaySegments,
    memtables: &mut HashMap<ColumnFamilyId, Arc<SkipListMemtable>>,
    policy: ReplayPolicy,
) -> MidgeResult<RecoveryStats> {
    // Bare bodies a test wrote become a chain of v2 segments first.
    let segments = crate::wal::recovery::fixture::wrap_chain(storage, segments, 0);
    let frontiers = serde_json::from_value(serde_json::json!([
        {"cf_id": 0, "through_sequence": 0},
        {"cf_id": 1, "through_sequence": 0}
    ]))
    .unwrap();
    replay_wal_with_frontiers_within(storage, &segments, memtables, policy, &frontiers, None)
}

fn replay_fixture(
    storage: &dyn Fs,
    segments: &ReplaySegments,
    memtables: &mut HashMap<ColumnFamilyId, Arc<SkipListMemtable>>,
) -> MidgeResult<RecoveryStats> {
    replay_fixture_with_policy(storage, segments, memtables, ReplayPolicy::Strict)
}

mod ledger_chain;
