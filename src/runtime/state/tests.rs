use super::*;
use std::path::PathBuf;

struct RecoveryRangeBudgetBackend {
    inner: Arc<crate::storage::cloud::MockCloudBackend>,
    ranges: std::sync::atomic::AtomicUsize,
    max_range_bytes: std::sync::atomic::AtomicU64,
}

impl crate::storage::cloud::CloudBackend for RecoveryRangeBudgetBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_get_with_metadata, submit_put, submit_get, submit_delete, submit_list, submit_head);

    fn submit_get_range(
        &self,
        key: &str,
        start: u64,
        end: Option<u64>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        self.inner.submit_get_range(key, start, end, callback);
    }

    fn submit_get_range_with_identity(
        &self,
        key: &str,
        start: u64,
        end: u64,
        expected: crate::storage::StorageObjectMetadata,
        timeout: std::time::Duration,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        self.ranges
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.max_range_bytes
            .fetch_max(end - start, std::sync::atomic::Ordering::Relaxed);
        self.inner
            .submit_get_range_with_identity(key, start, end, expected, timeout, callback);
    }
}

fn isolated_test_db_path() -> PathBuf {
    tempfile::tempdir().expect("temp dir").keep()
}

#[test]
#[cfg(feature = "failpoints")]
fn should_count_retained_recovery_files_when_startup_cleanup_fails() -> MidgeResult<()> {
    // Arrange
    let _test_guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir()?;
    let mut state = RuntimeState::new(dir.path().to_path_buf(), false);
    let retained = dir.path().join("cloud_recovery/wal/retained.wal");
    std::fs::create_dir_all(retained.parent().expect("WAL staging directory"))?;
    std::fs::write(&retained, [0x5A; 257])?;
    fail::cfg(
        "midge::recovery::inject_root_staging_delete_failure",
        "return",
    )
    .expect("configure cleanup failure");

    // Act
    state.cleanup_storage_residue();
    let bytes = state.retained_startup_scratch_bytes()?;
    fail::remove("midge::recovery::inject_root_staging_delete_failure");
    scenario.teardown();

    // Assert
    assert!(retained.exists());
    assert!(state.persistence_anomaly_detected());
    assert_eq!(bytes, 257);
    Ok(())
}

#[test]
fn should_count_startup_scratch_residue_without_counting_sst_or_wal_staging_twice(
) -> MidgeResult<()> {
    // Arrange
    let dir = tempfile::tempdir()?;
    let state = RuntimeState::new(dir.path().to_path_buf(), false);
    for (path, bytes) in [
        ("txn/one.run", 17),
        ("txn/one.ranges", 23),
        ("cloud_recovery/wal/recovered.wal", 19),
        ("manifest.tmp", 11),
        ("sst/.flush-staging/output.sst", 200),
        ("wal/current.wal", 100),
        ("cloud_store/sst/remote.sst", 400),
    ] {
        let path = dir.path().join(path);
        std::fs::create_dir_all(path.parent().expect("file directory"))?;
        std::fs::write(path, vec![0x5A; bytes])?;
    }

    // Act
    let bytes = state.retained_startup_scratch_bytes()?;

    // Assert
    assert_eq!(bytes, 70);
    Ok(())
}

#[test]
fn should_recover_compaction_generation_from_reserved_sst_namespace_without_published_outputs() {
    // Arrange
    let mut manifest = crate::metadata::Manifest::default();
    manifest.next_sst_seqs.insert(0, 73);
    manifest.next_sst_seqs.insert(1, 91);

    // Act
    let generation = RuntimeState::manifest_compaction_output_generation_floor(&manifest);

    // Assert
    assert!(manifest.files.is_empty());
    assert_eq!(
        generation, 91,
        "recovery must not reuse names uploaded before manifest publication"
    );
}

fn write_valid_sst_for_recovery_test(
    state: &RuntimeState,
    name: &str,
    level: u32,
    key: &[u8],
    sequence: u64,
) -> crate::runtime::FileMeta {
    use crate::sst::SstFactory;

    let factory = crate::sst::FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096);
    let mut writer = factory.create().expect("create recovery test SST");
    writer
        .add_with_meta(key, Some(b"value"), sequence, 0, None)
        .expect("write recovery test entry");
    let bytes = writer.finish_bytes().expect("finish recovery test SST");
    std::fs::create_dir_all(&state.sst_dir).expect("create recovery test SST directory");
    std::fs::write(state.sst_dir.join(name), &bytes).expect("persist recovery test SST");

    crate::runtime::FileMeta {
        name: name.to_string(),
        level,
        size_bytes: u64::try_from(bytes.len()).expect("SST length fits u64"),
        content_crc32c: Some(crc32c::crc32c(&bytes)),
        cf_id: 0,
        smallest_key: Some(key.to_vec()),
        largest_key: Some(key.to_vec()),
        smallest_seq: Some(sequence),
        largest_seq: Some(sequence),
        key_bounds_complete: true,
    }
}

fn manifest_meta_for_recovery_test(file: &crate::runtime::FileMeta) -> crate::metadata::FileMeta {
    crate::metadata::FileMeta {
        name: file.name.clone(),
        level: file.level,
        size_bytes: file.size_bytes,
        content_crc32c: file.content_crc32c,
        cf_id: file.cf_id,
        smallest_key: file.smallest_key.clone(),
        largest_key: file.largest_key.clone(),
        smallest_seq: file.smallest_seq,
        largest_seq: file.largest_seq,
        ..Default::default()
    }
}

#[test]
fn should_replay_published_compaction_from_remote_ssts_without_local_staging() {
    // Arrange
    let local_dir = tempfile::tempdir().expect("local recovery directory");
    let remote_dir = tempfile::tempdir().expect("remote recovery directory");
    let mut state = RuntimeState::new(local_dir.path().to_path_buf(), false);
    let remote_state = RuntimeState::new(remote_dir.path().to_path_buf(), false);
    let input_name = crate::sst::file_name(0, 0, 1);
    let input = write_valid_sst_for_recovery_test(&remote_state, &input_name, 0, b"a", 1);
    let output_name = crate::sst::file_name(0, 1, 2);
    let output = write_valid_sst_for_recovery_test(&remote_state, &output_name, 1, b"a", 1);
    state
        .manifest
        .files
        .push(manifest_meta_for_recovery_test(&input));
    state
        .intent_log
        .push(crate::runtime::IntentLogEntry::CompactionPublish {
            phase: PublicationPhase::ManifestPublished,
            cf_id: 0,
            removed: vec![input_name.clone()],
            added: vec![output],
        });
    let remote = Arc::new(
        crate::storage::filesystem::FileSystem::new(remote_dir.path())
            .expect("remote object storage"),
    );
    state.recovery_sst_fs = Some(Arc::new(crate::storage::remote_sst::RemoteSstFs::new(
        Arc::clone(&state.fs),
        remote,
        std::time::Duration::from_secs(5),
    )));

    // Act
    state
        .replay_intent_log()
        .expect("replay remote publication");

    // Assert
    assert!(!state.manifest_has_file(&input_name));
    assert!(state.manifest_has_file(&output_name));
    assert!(state.intent_log.is_empty());
    assert!(!state.sst_dir.join(&output_name).exists());
    assert!(remote_state.sst_dir.join(&input_name).exists());
    assert!(remote_state.sst_dir.join(&output_name).exists());
}

#[test]
fn should_reject_corrupt_remote_publication_with_bounded_checksum_ranges() {
    // Arrange
    let local_dir = tempfile::tempdir().expect("local recovery directory");
    let mut state = RuntimeState::new(local_dir.path().to_path_buf(), false);
    let backend = Arc::new(RecoveryRangeBudgetBackend {
        inner: Arc::new(crate::storage::cloud::MockCloudBackend::new()),
        ranges: std::sync::atomic::AtomicUsize::new(0),
        max_range_bytes: std::sync::atomic::AtomicU64::new(0),
    });
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
        backend.clone(),
        "midge".into(),
    ));
    let name = crate::sst::file_name(0, 0, 7);
    let corrupt_bytes = vec![0x5A; 3 * 1024 * 1024 + 19];
    let file_meta = crate::runtime::FileMeta {
        name: name.clone(),
        level: 0,
        size_bytes: corrupt_bytes.len() as u64,
        content_crc32c: Some(crc32c::crc32c(&corrupt_bytes) ^ 1),
        cf_id: 0,
        smallest_key: None,
        largest_key: None,
        smallest_seq: Some(7),
        largest_seq: Some(7),
        key_bounds_complete: false,
    };
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_put(
        &crate::sst::object_key(&name),
        corrupt_bytes,
        Vec::new(),
        tx,
    );
    assert!(matches!(
        rx.recv().expect("upload"),
        crate::storage::cloud::CloudEvent::Put {
            result: crate::storage::cloud::CloudOutcome::Ok(()),
            ..
        }
    ));
    state
        .intent_log
        .push(crate::runtime::IntentLogEntry::SstAdded { file_meta });
    state.recovery_sst_fs = Some(Arc::new(crate::storage::remote_sst::RemoteSstFs::new(
        Arc::clone(&state.fs),
        cloud,
        std::time::Duration::from_secs(5),
    )));
    backend.inner.clear_history();

    // Act
    let error = state
        .replay_intent_log()
        .expect_err("reject publication checksum mismatch");

    // Assert
    assert!(error.to_string().contains("checksum"), "{error}");
    assert!(
        backend.inner.get_downloads().is_empty(),
        "no whole-object GET is allowed"
    );
    assert_eq!(backend.ranges.load(std::sync::atomic::Ordering::Relaxed), 4);
    assert!(
        backend
            .max_range_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
            <= 1024 * 1024
    );
    assert!(!state.manifest_has_file(&name));
    assert_eq!(state.intent_log.len(), 1);
    assert!(!state.sst_dir.join(name).exists());
}

#[test]
fn should_preserve_column_family_identity_given_sst_add_is_appended_to_manifest() {
    // Arrange
    let state = RuntimeState::new(isolated_test_db_path(), false);
    let file = crate::runtime::FileMeta {
        name: "000042_00_00000000000000000001.sst".to_string(),
        level: 0,
        size_bytes: 437,
        content_crc32c: Some(0xA5A5_A5A5),
        cf_id: 42,
        smallest_key: Some(b"alpha".to_vec()),
        largest_key: Some(b"omega".to_vec()),
        smallest_seq: Some(7),
        largest_seq: Some(9),
        key_bounds_complete: true,
    };

    // Act
    state
        .append_manifest_add_sst(&file)
        .expect("append SST manifest edit");
    let edits =
        crate::metadata::journal::replay_journal(&state.db_path).expect("replay SST manifest edit");

    // Assert
    let [crate::metadata::ManifestEdit::AddSst(persisted)] = edits.as_slice() else {
        panic!("expected one persisted SST add edit: {edits:?}");
    };
    assert_eq!(persisted.cf_id, 42);
    assert_eq!(persisted.name, file.name);
    assert_eq!(persisted.smallest_key.as_deref(), Some(b"alpha".as_slice()));
    assert_eq!(persisted.largest_key.as_deref(), Some(b"omega".as_slice()));
}

fn grow_active_memtable(
    state: &mut RuntimeState,
    cf_id: crate::types::ColumnFamilyId,
    target_bytes: usize,
) {
    let mut seq = 1_u64;
    while state
        .get_cf(cf_id)
        .expect("column family")
        .memtable
        .size_bytes()
        < target_bytes
    {
        let cf = state.get_cf(cf_id).expect("column family");
        cf.memtable
            .put_with_seq(
                format!("key_{seq:06}").into_bytes(),
                vec![0xA5; 128],
                seq,
                None,
            )
            .expect("seed memtable");
        seq += 1;
    }

    state.total_memtable_bytes = state
        .get_cf(cf_id)
        .expect("column family")
        .memtable
        .size_bytes();
}

// =========== ColumnFamilyState Tests ===========

#[test]
fn should_create_column_family_state_with_empty_memtables() {
    // Arrange
    let id = 42;
    let name = "test_cf".to_string();

    // Act
    let cf = ColumnFamilyState::new(id, name);

    // Assert
    assert_eq!(cf.memtable.size_bytes(), 0);
    assert!(cf.immutable_memtables.is_empty());
    assert_eq!(cf.active_memtable_started_in_segment, 1);
}

#[test]
fn should_select_due_pending_immutable_before_active_memtable() {
    // Arrange
    let mut state = RuntimeState::new(isolated_test_db_path(), false);
    state.memtable_flush_threshold = 1;
    let immutable = Arc::new(SkipListMemtable::new());
    immutable
        .put_with_seq(b"older".to_vec(), b"value".to_vec(), 1, None)
        .expect("seed immutable");
    let tracked = state
        .track_new_immutable_flush(0, Arc::clone(&immutable), 1)
        .expect("track immutable");
    state
        .mark_immutable_flush_failed(tracked.flush_id)
        .expect("mark failed");
    state.make_immutable_flush_retry_due(0);
    state
        .get_cf(0)
        .expect("default cf")
        .memtable
        .put_with_seq(b"newer".to_vec(), b"value".to_vec(), 2, None)
        .expect("seed active memtable");

    // Act
    let candidate = state
        .next_flush_candidate(false)
        .expect("pending immutable candidate");

    // Assert
    assert_eq!(candidate.cf_id, 0);
    assert_eq!(candidate.reason, FlushReason::PendingImmutable);
}

#[test]
fn should_cap_immutable_flush_retry_backoff_at_one_second() {
    // Arrange
    let mut state = RuntimeState::new(isolated_test_db_path(), false);
    let immutable = Arc::new(SkipListMemtable::new());
    immutable
        .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)
        .expect("seed immutable");
    let tracked = state
        .track_new_immutable_flush(0, Arc::clone(&immutable), 1)
        .expect("track immutable");

    // Act
    let mut final_delay = Duration::ZERO;
    for _ in 0..32 {
        final_delay = state
            .mark_immutable_flush_failed(tracked.flush_id)
            .expect("mark failed");
        state.make_immutable_flush_retry_due(0);
        state.begin_next_immutable_flush().expect("begin retry");
    }

    // Assert
    assert_eq!(final_delay, MAX_FLUSH_RETRY_BACKOFF);
    assert!(
        state
            .flush_retry_deadline_timeout()
            .is_none_or(|delay| delay <= MAX_FLUSH_RETRY_BACKOFF),
        "maintenance retry deadline must remain bounded"
    );
}

#[test]
fn should_select_size_threshold_flush_candidate_before_cloud_gap_candidate() {
    // Arrange
    let mut state = RuntimeState::new(isolated_test_db_path(), false);
    state.memtable_flush_threshold = 4 * 1024;
    state.memtable_size_limit = 1024 * 1024;
    state.wal.current_segment_id = state.cloud_eventual_flush_segment_gap + 50;

    grow_active_memtable(&mut state, 0, 5 * 1024);

    let other_cf_id = state
        .create_cf("other".to_string())
        .expect("create other cf");
    {
        let cf_state = state.get_cf(other_cf_id).expect("other cf");
        cf_state
            .memtable
            .put_with_seq(b"other".to_vec(), b"value".to_vec(), 1, None)
            .expect("seed other cf");
    }
    state
        .get_cf_mut(other_cf_id)
        .expect("other cf")
        .active_memtable_started_in_segment = 1;

    let candidate = state
        .next_flush_candidate(true)
        .expect("flush candidate should exist");
    // Act
    // Assert
    assert_eq!(candidate.cf_id, 0);
    assert_eq!(candidate.reason, FlushReason::SizeThreshold);
}

#[test]
fn should_select_cloud_gap_flush_candidate_when_cloud_mode_and_gap_exceeded() {
    // Arrange
    let mut state = RuntimeState::new(isolated_test_db_path(), false);
    state.memtable_flush_threshold = 1024 * 1024;
    state.memtable_size_limit = 1024 * 1024;
    state.wal.current_segment_id = state.cloud_eventual_flush_segment_gap + 1;

    {
        let cf_state = state.get_cf(0).expect("default cf");
        cf_state
            .memtable
            .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)
            .expect("seed default cf");
    }
    state
        .get_cf_mut(0)
        .expect("default cf")
        .active_memtable_started_in_segment = 1;

    let candidate = state
        .next_flush_candidate(true)
        .expect("cloud gap flush candidate should exist");
    // Act
    // Assert
    assert_eq!(candidate.cf_id, 0);
    assert_eq!(candidate.reason, FlushReason::CloudSegmentGap);
}

#[test]
fn should_not_select_cloud_gap_flush_candidate_when_gap_mode_disabled() {
    // Arrange
    let mut state = RuntimeState::new(isolated_test_db_path(), false);
    state.memtable_flush_threshold = 1024 * 1024;
    state.memtable_size_limit = 1024 * 1024;
    state.wal.current_segment_id = state.cloud_eventual_flush_segment_gap + 10;

    {
        let cf_state = state.get_cf(0).expect("default cf");
        cf_state
            .memtable
            .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)
            .expect("seed default cf");
    }
    state
        .get_cf_mut(0)
        .expect("default cf")
        .active_memtable_started_in_segment = 1;

    // Act
    // Assert
    assert!(state.next_flush_candidate(false).is_none());
}

#[test]
fn should_report_max_memtable_wal_segment_gap_for_non_empty_memtables() {
    // Arrange
    let mut state = RuntimeState::new(isolated_test_db_path(), false);
    state.wal.current_segment_id = state.cloud_eventual_flush_segment_gap + 20;

    {
        let cf_state = state.get_cf(0).expect("default cf");
        cf_state
            .memtable
            .put_with_seq(b"default".to_vec(), b"value".to_vec(), 1, None)
            .expect("seed default cf");
    }
    state
        .get_cf_mut(0)
        .expect("default cf")
        .active_memtable_started_in_segment = 10;

    let cf_id = state.create_cf("secondary".to_string()).expect("create cf");
    {
        let cf_state = state.get_cf(cf_id).expect("secondary cf");
        cf_state
            .memtable
            .put_with_seq(b"secondary".to_vec(), b"value".to_vec(), 2, None)
            .expect("seed secondary cf");
    }
    state
        .get_cf_mut(cf_id)
        .expect("secondary cf")
        .active_memtable_started_in_segment = 1;

    // Act
    // Assert
    assert_eq!(
        state.max_memtable_wal_segment_gap(),
        state.wal.current_segment_id.saturating_sub(1)
    );
}

#[test]
fn should_flush_but_not_hard_stall_when_active_memtable_exceeds_flush_threshold() {
    // Arrange
    let mut state = RuntimeState::new(isolated_test_db_path(), false);
    state.memtable_size_limit = 1024 * 1024;
    state.memtable_flush_threshold = 4 * 1024;
    state.total_memtable_bytes = 0;

    grow_active_memtable(&mut state, 0, 5 * 1024);

    // Act
    // Assert
    assert_eq!(state.needs_flush(), Some(0));
    assert!(!state.should_hard_stall_writes(0));
    assert!(!state.should_stall_writes(0));
    assert!(!state.has_any_hard_write_stall());
}

#[test]
fn should_hard_stall_when_immutable_memtable_queue_is_full() {
    // Arrange
    let mut state = RuntimeState::new(isolated_test_db_path(), false);
    state.max_immutable_memtables = 1;
    state
        .get_cf_mut(0)
        .expect("default cf")
        .immutable_memtables
        .push(Arc::new(SkipListMemtable::new()));

    // Act
    // Assert
    assert!(state.is_immutable_memtable_queue_full(0));
    assert!(state.should_hard_stall_writes(0));
    assert!(state.has_any_hard_write_stall());
}

#[test]
fn should_derive_hard_l0_ceiling_from_capacity() {
    // Arrange
    let mut state = RuntimeState::new(isolated_test_db_path(), false);
    state.l0_compaction_trigger = 3;
    state.max_immutable_memtables = 2;
    state
        .manifest
        .files
        .extend((0..4).map(|index| crate::metadata::FileMeta {
            name: format!("l0-{index}.sst"),
            level: 0,
            cf_id: 0,
            ..Default::default()
        }));
    let immutable = Arc::new(SkipListMemtable::new());
    state
        .get_cf_mut(0)
        .expect("default cf")
        .immutable_memtables
        .push(immutable);

    // Act
    let ceiling = state.l0_hard_ceiling();
    let usage = state.l0_slot_usage(0);

    // Assert
    assert_eq!(ceiling, 6);
    assert_eq!(usage, 5);
    assert!(!state.has_critical_l0_debt(0));
}

#[test]
fn should_stall_next_write_after_active_generation_reserves_last_l0_slot() {
    // Arrange
    let mut state = RuntimeState::new(isolated_test_db_path(), false);
    state.l0_compaction_trigger = 1;
    state.max_immutable_memtables = 1;
    state.memtable_flush_threshold = 1;
    state
        .manifest
        .files
        .extend((0..2).map(|index| crate::metadata::FileMeta {
            name: format!("l0-{index}.sst"),
            level: 0,
            cf_id: 0,
            ..Default::default()
        }));
    assert_eq!(state.l0_hard_ceiling(), 3);
    assert!(!state.l0_write_slot_unavailable(0));
    state
        .get_cf(0)
        .expect("default cf")
        .memtable
        .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)
        .expect("reserve active L0 generation");

    // Act
    let usage = state.l0_slot_usage(0);

    // Assert
    assert_eq!(usage, state.l0_hard_ceiling());
    assert!(state.l0_write_slot_unavailable(0));
    assert!(state.should_hard_stall_writes(0));
    assert!(state.has_any_hard_write_stall());
}

#[test]
fn should_hard_stall_when_total_memtable_memory_exceeds_limit() {
    // Arrange
    let mut state = RuntimeState::new(isolated_test_db_path(), false);
    state.memtable_flush_threshold = 1024;
    state.total_memtable_bytes = 2 * 1024;

    // Act
    // Assert
    assert!(state.is_total_memtable_hard_limit_exceeded());
    assert!(state.should_hard_stall_writes(0));
    assert!(state.has_any_hard_write_stall());
}

#[test]
fn should_recompute_total_memtable_bytes_from_all_memtables() {
    // Arrange
    let mut state = RuntimeState::new(isolated_test_db_path(), false);
    let immutable = Arc::new(SkipListMemtable::new());
    immutable
        .put_with_seq(b"immutable".to_vec(), b"value".to_vec(), 1, None)
        .expect("seed immutable memtable");
    {
        let cf = state.get_cf_mut(0).expect("default cf");
        cf.memtable
            .put_with_seq(b"active".to_vec(), b"value".to_vec(), 2, None)
            .expect("seed active memtable");
        cf.immutable_memtables.push(Arc::clone(&immutable));
    }
    state.total_memtable_bytes = usize::MAX;

    // Act
    state.recompute_total_memtable_bytes();

    // Assert
    let cf = state.get_cf(0).expect("default cf");
    assert_eq!(
        state.total_memtable_bytes,
        cf.memtable.size_bytes() + immutable.size_bytes()
    );
}

#[test]
fn should_account_for_recovered_wal_memtable_bytes_when_reopening() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create recovery directory");
    let state = RuntimeState::new(temp_dir.path().to_path_buf(), false);
    crate::metadata::ManifestPersistence::save(temp_dir.path(), &state.manifest)
        .expect("persist manifest");
    drop(state);
    let record = crate::wal::WalRecord::new_cf(
        0,
        crate::wal::WalOpKind::Put,
        bytes::Bytes::from_static(b"recovered-key"),
        Some(bytes::Bytes::from(vec![0xA5; 4096])),
        1,
        1,
    );
    let payload = crate::wal::encoding::encode(&record).expect("encode WAL record");
    let mut frame = Vec::new();
    crate::wal::frame::append_frame(&mut frame, &payload).expect("frame WAL record");
    std::fs::write(temp_dir.path().join("wal").join("wal.log"), frame).expect("write retained WAL");

    // Act
    let reopened = RuntimeState::try_new(
        temp_dir.path().to_path_buf(),
        false,
        crate::config::RecoveryPolicy::Strict,
    )
    .expect("recover state");

    // Assert
    let recovered_bytes = reopened
        .get_cf(0)
        .expect("default column family")
        .memtable
        .size_bytes();
    assert!(recovered_bytes > 0);
    assert_eq!(reopened.total_memtable_bytes, recovered_bytes);
}

#[test]
fn should_hard_stall_when_external_backpressure_sets_write_stalled() {
    // Arrange
    let mut state = RuntimeState::new(isolated_test_db_path(), false);
    state.set_write_stalled(true);

    // Act
    // Assert
    assert!(state.should_hard_stall_writes(0));
    assert!(state.has_any_hard_write_stall());
    assert!(state.runtime_metrics_snapshot().write_stalled);
}

// =========== WalState Tests ===========

// Scope: `WalState`/`CompactionState`/`CloudState` are plain field bags with
// no invariant-enforcing methods of their own — the invariants that matter
// (segment rotation, compacting-SST bookkeeping, upload tracking) are
// exercised through the real actor/state methods elsewhere in this module
// and in src/runtime/actors/*. These tests are kept only to pin the
// documented default values, since a silent default change would be a
// behavioral regression on a fresh RuntimeState.
#[test]
fn should_initialize_wal_state_with_defaults() {
    // Arrange
    // (no setup)

    // Act
    let wal = WalState::default();

    // Assert
    assert_eq!(wal.current_segment_id, 1);
    assert_eq!(wal.last_synced_seq, 0);
    assert_eq!(wal.pending_writes, 0);
    assert_eq!(wal.local_durable_seq, 0);
    assert_eq!(wal.cloud_durable_seq, 0);
}

// =========== CompactionState Tests ===========

#[test]
fn should_initialize_compaction_state() {
    // Arrange
    // (no setup)

    // Act
    let compaction = CompactionState::default();

    // Assert
    assert!(compaction.compacting_ssts.is_empty());
    assert_eq!(compaction.pending_tasks, 0);
}

// =========== CloudState Tests ===========

#[test]
fn should_initialize_cloud_state() {
    // Arrange
    // (no setup)

    // Act
    let cloud = CloudState::default();

    // Assert
    assert!(cloud.pending_uploads.is_empty());
    assert_eq!(cloud.last_cloud_checkpoint_seq, 0);
}

// =========== RuntimeState Tests ===========

#[test]
fn should_create_runtime_state_in_memory_mode() {
    // Arrange
    // (no setup)

    // Act
    let state = RuntimeState::new("/tmp/test_midge".into(), true);

    // Assert
    assert!(state.is_memory_mode());
    assert_eq!(state.sequence, 0);
    assert_eq!(state.next_txn_id, 0);
    assert!(state.column_families.contains_key(&0)); // Default CF
    assert_eq!(state.column_families.len(), 1);
}

#[test]
fn should_retain_timed_out_snapshots_when_enforcement_runs() {
    // Arrange
    let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
    state.snapshots.max_snapshot_lifetime = std::time::Duration::from_millis(0);
    assert!(state.register_snapshot(1, 10, vec!["001.sst".to_string()]));
    assert_eq!(state.snapshot_pins.active_count(), 1);
    std::thread::sleep(std::time::Duration::from_millis(1));

    // Act
    let timed_out = state.warn_timed_out_snapshots();

    // Assert
    assert_eq!(timed_out, 1);
    assert_eq!(state.snapshot_pins.active_count(), 1);
    assert_eq!(state.oldest_active_snapshot_sequence(), Some(10));
    assert!(state.get_pinned_sst_names().contains("001.sst"));
}

#[test]
fn should_preserve_non_expired_snapshots_when_enforcement_runs() {
    // Arrange
    let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
    state.snapshots.max_snapshot_lifetime = std::time::Duration::from_hours(1);
    assert!(state.register_snapshot(2, 11, vec!["002.sst".to_string()]));

    // Act
    let timed_out = state.warn_timed_out_snapshots();

    // Assert
    assert_eq!(timed_out, 0);
    assert_eq!(state.snapshot_pins.active_count(), 1);
}

#[test]
fn should_prune_recent_delete_ranges_when_no_active_snapshots() {
    // Arrange
    let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
    state.record_delete_range(0, b"a", b"z", 10);

    // Act
    state.prune_recent_delete_ranges_by_snapshot_horizon();

    // Assert
    assert!(state.recent_delete_ranges.is_empty());
}

#[test]
fn should_retain_only_delete_ranges_newer_than_oldest_snapshot_when_pruning() {
    // Arrange
    let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
    assert!(state.register_snapshot(100, 20, vec![]));
    assert!(state.register_snapshot(101, 40, vec![]));

    state.recent_delete_ranges.push(RecentDeleteRange {
        cf_id: 0,
        start_key: b"a".to_vec(),
        end_key: b"c".to_vec(),
        sequence: 10,
    });
    state.recent_delete_ranges.push(RecentDeleteRange {
        cf_id: 0,
        start_key: b"c".to_vec(),
        end_key: b"e".to_vec(),
        sequence: 21,
    });
    state.recent_delete_ranges.push(RecentDeleteRange {
        cf_id: 0,
        start_key: b"e".to_vec(),
        end_key: b"g".to_vec(),
        sequence: 35,
    });

    // Act
    state.prune_recent_delete_ranges_by_snapshot_horizon();

    // Assert
    assert_eq!(state.recent_delete_ranges.len(), 2);
    assert!(state
        .recent_delete_ranges
        .iter()
        .all(|entry| entry.sequence > 20));
}

#[test]
fn should_increment_sequence_numbers_monotonically() {
    // Arrange
    let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
    let initial = state.sequence;

    // Act
    let seq1 = state.next_sequence();
    let seq2 = state.next_sequence();
    let seq3 = state.next_sequence();

    // Assert
    assert_eq!(seq1, initial + 1);
    assert_eq!(seq2, initial + 2);
    assert_eq!(seq3, initial + 3);
    assert!(seq1 < seq2 && seq2 < seq3);
}

#[test]
fn should_increment_transaction_ids_monotonically() {
    // Arrange
    let mut state = RuntimeState::new("/tmp/test_midge".into(), true);

    // Act
    let txn1 = state.next_txn_id();
    let txn2 = state.next_txn_id();
    let txn3 = state.next_txn_id();

    // Assert
    assert_eq!(txn1, 1);
    assert_eq!(txn2, 2);
    assert_eq!(txn3, 3);
    assert!(txn1 < txn2 && txn2 < txn3);
}

#[test]
fn should_create_new_column_family() {
    // Arrange
    let mut state = RuntimeState::new("/tmp/test_midge".into(), true);

    // Act
    let cf_id = state
        .create_cf("test_cf".to_string())
        .expect("create_cf should succeed");

    // Assert
    assert_eq!(cf_id, 1); // After default (0)
    assert!(state.column_families.contains_key(&cf_id));
    let cf = state.get_cf(cf_id).expect("Created CF should exist");
    assert_eq!(cf.memtable.size_bytes(), 0);
    assert!(cf.immutable_memtables.is_empty());
}

#[test]
fn should_get_column_family_by_id() {
    // Arrange
    let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
    state
        .create_cf("my_cf".to_string())
        .expect("create_cf should succeed");

    // Act
    let cf = state.get_cf(1);

    // Assert
    assert!(cf.is_some());
    assert_eq!(cf.unwrap().memtable.size_bytes(), 0);
}

#[test]
fn should_return_none_for_nonexistent_column_family() {
    // Arrange
    let state = RuntimeState::new("/tmp/test_midge".into(), true);

    // Act
    // (none)

    // Assert
    assert!(state.get_cf(999).is_none());
}

#[test]
fn should_get_mutable_column_family() {
    // Arrange
    let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
    state
        .create_cf("mutable_cf".to_string())
        .expect("create_cf should succeed");

    // Act
    {
        let cf_mut = state.get_cf_mut(1).expect("get_cf_mut should succeed");
        cf_mut
            .immutable_memtables
            .push(Arc::new(SkipListMemtable::new()));
    }

    // Assert
    let cf = state.get_cf(1).expect("CF should exist");
    assert_eq!(cf.immutable_memtables.len(), 1);
}

#[test]
fn should_load_intent_log_on_startup() {
    // Arrange: write an intent file to the test dir and then create RuntimeState
    let test_dir = std::env::temp_dir().join("midge_state_intent_test");
    let _ = std::fs::remove_dir_all(&test_dir);
    std::fs::create_dir_all(&test_dir).expect("create test dir");
    crate::metadata::ensure_or_create_format_marker(&test_dir).expect("create format marker");

    let intents = vec![crate::runtime::IntentLogEntry::WalSynced {
        segment_id: 2,
        seqno: 99,
    }];
    crate::runtime::IntentPersistence::save(&test_dir, &intents).expect("save intents");

    // Act: create runtime state for that path (not memory mode)
    let state = RuntimeState::new(test_dir.clone(), false);

    // Assert: intent log was loaded
    assert!(
        !state.intent_log.is_empty(),
        "intent log should be loaded from disk"
    );
    assert!(
        matches!(
            state.intent_log[0],
            crate::runtime::IntentLogEntry::WalSynced {
                segment_id: 2,
                seqno: 99
            }
        ),
        "expected WalSynced entry with segment_id=2, seqno=99, got: {:?}",
        state.intent_log[0]
    );
}

#[test]
fn should_replay_intent_log_idempotently_given_duplicate_publish_intents_when_reopening() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create duplicate-intent directory");
    let state = RuntimeState::new(temp_dir.path().to_path_buf(), false);
    let sst_name = crate::sst::file_name(0, 0, 17);
    let file_meta = write_valid_sst_for_recovery_test(&state, &sst_name, 0, b"value", 17);
    let duplicate = crate::runtime::IntentLogEntry::SstAdded {
        file_meta: file_meta.clone(),
    };
    crate::runtime::IntentPersistence::save(temp_dir.path(), &[duplicate.clone(), duplicate])
        .expect("persist duplicate publication intents");
    drop(state);

    // Act
    let mut reopened = RuntimeState::try_new(
        temp_dir.path().to_path_buf(),
        false,
        crate::config::RecoveryPolicy::Strict,
    )
    .expect("reopen duplicate intent log");
    assert_eq!(reopened.intent_log.len(), 2);
    reopened
        .replay_intent_log()
        .expect("replay duplicate intents");
    drop(reopened);
    let mut second_reopen = RuntimeState::try_new(
        temp_dir.path().to_path_buf(),
        false,
        crate::config::RecoveryPolicy::Strict,
    )
    .expect("reopen after idempotent replay");
    second_reopen
        .replay_intent_log()
        .expect("empty replay remains idempotent");

    // Assert
    assert_eq!(
        second_reopen
            .manifest
            .files
            .iter()
            .filter(|file| file.name == sst_name)
            .count(),
        1,
        "duplicate publication intents must produce one manifest owner"
    );
    assert!(second_reopen.intent_log.is_empty());
    assert!(second_reopen.sst_dir.join(sst_name).exists());
}

#[test]
fn should_sweep_non_authoritative_flush_staging_during_startup_cleanup() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create state directory");
    let mut state = RuntimeState::new(temp_dir.path().to_path_buf(), false);
    let staging_dir = state.sst_dir.join(".flush-staging");
    std::fs::create_dir_all(&staging_dir).expect("create flush staging directory");
    std::fs::write(staging_dir.join("1-1.sst"), b"unpublished")
        .expect("write abandoned staging file");

    // Act
    state.cleanup_storage_residue();

    // Assert
    assert!(
        !staging_dir.exists(),
        "startup cleanup must remove the entire non-authoritative staging tree"
    );
    assert!(!state.persistence_anomaly_detected());
}

#[test]
fn should_not_apply_wal_record_given_unknown_column_family_when_recovering() {
    // Arrange
    let mut column_families = HashMap::new();
    column_families.insert(0, ColumnFamilyState::new(0, "default".to_string()));
    let mut recovered = HashMap::new();
    recovered.insert(99, Arc::new(SkipListMemtable::new()));

    // Act
    RuntimeState::apply_recovered_memtables(&mut column_families, recovered);

    // Assert
    assert!(!column_families.contains_key(&99));
    assert!(column_families.contains_key(&0));
}

#[test]
fn should_not_mutate_compaction_intent_when_persistence_rejects_output() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create compaction intent directory");
    let mut state = RuntimeState::new(temp_dir.path().to_path_buf(), false);
    let invalid_output = crate::runtime::FileMeta {
        name: "../escape.sst".to_string(),
        level: 1,
        size_bytes: 1,
        content_crc32c: None,
        cf_id: 0,
        smallest_key: None,
        largest_key: None,
        smallest_seq: None,
        largest_seq: None,
        key_bounds_complete: false,
    };

    // Act
    let result = state.record_compaction_publication_intent(0, Vec::new(), vec![invalid_output]);

    // Assert
    assert!(result.is_err(), "unsafe output name must be rejected");
    assert!(
        state.intent_log.is_empty(),
        "failed intent persistence must not mutate in-memory state"
    );
}

#[test]
fn should_roll_back_output_durable_compaction_when_manifest_is_still_prepublication() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create recovery directory");
    let mut state = RuntimeState::new(temp_dir.path().to_path_buf(), false);
    let input_name = crate::sst::file_name(0, 0, 1);
    let output_name = crate::sst::file_name(0, 1, 2);
    let input = write_valid_sst_for_recovery_test(&state, &input_name, 0, b"input", 1);
    let output = write_valid_sst_for_recovery_test(&state, &output_name, 1, b"input", 2);
    state
        .manifest
        .files
        .push(manifest_meta_for_recovery_test(&input));
    state
        .append_intent(crate::runtime::IntentLogEntry::CompactionPublish {
            phase: PublicationPhase::OutputDurable,
            cf_id: 0,
            removed: vec![input_name.clone()],
            added: vec![output],
        })
        .expect("persist output-durable intent");

    // Act
    state.replay_intent_log().expect("replay intent");

    // Assert
    assert!(state.manifest_has_file(&input_name));
    assert!(!state.manifest_has_file(&output_name));
    assert!(state.sst_dir.join(&input_name).exists());
    assert!(!state.sst_dir.join(&output_name).exists());
}

#[derive(Debug, Clone, Copy)]
enum CompactionPublicationCrashPoint {
    OutputDurability,
    IntentDurability,
    PartialMirror,
    CompleteMirror,
    ManifestSwitch,
    SnapshotPublication,
    InputGc,
    IntentClear,
}

impl CompactionPublicationCrashPoint {
    const ALL: [Self; 8] = [
        Self::OutputDurability,
        Self::IntentDurability,
        Self::PartialMirror,
        Self::CompleteMirror,
        Self::ManifestSwitch,
        Self::SnapshotPublication,
        Self::InputGc,
        Self::IntentClear,
    ];

    fn manifest_published(self) -> bool {
        matches!(
            self,
            Self::ManifestSwitch | Self::SnapshotPublication | Self::InputGc | Self::IntentClear
        )
    }

    fn intent_phase(self) -> Option<PublicationPhase> {
        match self {
            Self::OutputDurability | Self::IntentClear => None,
            Self::IntentDurability | Self::PartialMirror | Self::CompleteMirror => {
                Some(PublicationPhase::OutputDurable)
            }
            Self::ManifestSwitch => Some(PublicationPhase::OutputDurable),
            Self::SnapshotPublication | Self::InputGc => Some(PublicationPhase::ManifestPublished),
        }
    }

    fn mirrored_output_count(self) -> usize {
        match self {
            Self::OutputDurability | Self::IntentDurability => 0,
            Self::PartialMirror => 1,
            Self::CompleteMirror
            | Self::ManifestSwitch
            | Self::SnapshotPublication
            | Self::InputGc
            | Self::IntentClear => 2,
        }
    }

    fn inputs_garbage_collected(self) -> bool {
        matches!(self, Self::InputGc | Self::IntentClear)
    }
}

fn persist_proof_mirror(
    state: &RuntimeState,
    mirror_dir: &std::path::Path,
    outputs: &[crate::runtime::FileMeta],
    output_count: usize,
) -> HashSet<String> {
    std::fs::create_dir_all(mirror_dir).expect("create proof mirror directory");
    for output in outputs.iter().take(output_count) {
        std::fs::copy(
            state.sst_dir.join(&output.name),
            mirror_dir.join(&output.name),
        )
        .expect("persist proof mirrored output");
    }
    std::fs::read_dir(mirror_dir)
        .expect("read proof mirror directory")
        .map(|entry| {
            entry
                .expect("read proof mirror entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect()
}

#[test]
fn should_replay_every_compaction_publication_crash_point_to_complete_authority() {
    for crash_point in CompactionPublicationCrashPoint::ALL {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("create publication crash directory");
        let mut state = RuntimeState::new(temp_dir.path().to_path_buf(), false);
        let input_names = [
            crate::sst::file_name(0, 0, 1),
            crate::sst::file_name(0, 0, 2),
        ];
        let output_names = [
            crate::sst::compaction_file_name(0, 1, 3, 0),
            crate::sst::compaction_file_name(0, 1, 3, 1),
        ];
        let inputs = [
            write_valid_sst_for_recovery_test(&state, &input_names[0], 0, b"a", 1),
            write_valid_sst_for_recovery_test(&state, &input_names[1], 0, b"z", 2),
        ];
        let outputs = [
            write_valid_sst_for_recovery_test(&state, &output_names[0], 1, b"a", 3),
            write_valid_sst_for_recovery_test(&state, &output_names[1], 1, b"z", 3),
        ];
        let old_authority = inputs
            .iter()
            .map(|file| file.name.clone())
            .collect::<HashSet<_>>();
        let new_authority = outputs
            .iter()
            .map(|file| file.name.clone())
            .collect::<HashSet<_>>();
        state.manifest.files = if crash_point.manifest_published() {
            outputs
                .iter()
                .map(manifest_meta_for_recovery_test)
                .collect()
        } else {
            inputs.iter().map(manifest_meta_for_recovery_test).collect()
        };
        crate::metadata::ManifestPersistence::save(temp_dir.path(), &state.manifest)
            .expect("persist crash-point manifest");
        if let Some(phase) = crash_point.intent_phase() {
            state
                .append_intent(crate::runtime::IntentLogEntry::CompactionPublish {
                    phase,
                    cf_id: 0,
                    removed: input_names.to_vec(),
                    added: outputs.to_vec(),
                })
                .expect("persist crash-point intent");
        }
        if crash_point.inputs_garbage_collected() {
            for input_name in &input_names {
                std::fs::remove_file(state.sst_dir.join(input_name))
                    .expect("remove garbage-collected proof input");
            }
        }
        let mirror_dir = temp_dir.path().join("proof-cloud-mirror");
        let mirrored_output_count = crash_point.mirrored_output_count();
        let mirrored_names =
            persist_proof_mirror(&state, &mirror_dir, &outputs, mirrored_output_count);
        assert_eq!(mirrored_names.len(), mirrored_output_count);
        assert!(mirrored_names.is_subset(&new_authority));
        drop(state);

        // Act
        let mut reopened = RuntimeState::try_new(
            temp_dir.path().to_path_buf(),
            false,
            crate::config::RecoveryPolicy::Strict,
        )
        .expect("reopen crash-point state");
        reopened
            .replay_intent_log()
            .expect("replay crash-point intent");
        let recovered_authority = reopened
            .manifest
            .files
            .iter()
            .map(|file| file.name.clone())
            .collect::<HashSet<_>>();

        // Assert
        let expected = if crash_point.manifest_published() {
            &new_authority
        } else {
            &old_authority
        };
        assert_eq!(
            &recovered_authority, expected,
            "partial authority recovered after {crash_point:?}"
        );
        assert!(recovered_authority == old_authority || recovered_authority == new_authority);
        assert_eq!(
            std::fs::read_dir(&mirror_dir)
                .expect("re-read proof mirror directory")
                .count(),
            mirrored_output_count,
            "object mirroring alone changed authority at {crash_point:?}"
        );
    }
}

#[test]
fn should_retain_inputs_for_output_durable_remove_only_compaction_after_crash() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create recovery directory");
    let mut state = RuntimeState::new(temp_dir.path().to_path_buf(), false);
    let input_name = crate::sst::file_name(0, 0, 1);
    let input = write_valid_sst_for_recovery_test(&state, &input_name, 0, b"deleted", 1);
    state
        .manifest
        .files
        .push(manifest_meta_for_recovery_test(&input));
    crate::metadata::ManifestPersistence::save(temp_dir.path(), &state.manifest)
        .expect("persist prepublication manifest");
    state
        .append_intent(crate::runtime::IntentLogEntry::CompactionPublish {
            phase: PublicationPhase::OutputDurable,
            cf_id: 0,
            removed: vec![input_name.clone()],
            added: Vec::new(),
        })
        .expect("persist remove-only output intent");
    drop(state);

    // Act
    let mut reopened = RuntimeState::try_new(
        temp_dir.path().to_path_buf(),
        false,
        crate::config::RecoveryPolicy::Strict,
    )
    .expect("reopen state");
    reopened.replay_intent_log().expect("replay intent");

    // Assert
    assert!(reopened.manifest_has_file(&input_name));
    assert!(reopened.sst_dir.join(input_name).exists());
}

#[test]
fn should_publish_remove_only_compaction_after_manifest_phase_crash() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create recovery directory");
    let mut state = RuntimeState::new(temp_dir.path().to_path_buf(), false);
    let input_name = crate::sst::file_name(0, 0, 1);
    let input = write_valid_sst_for_recovery_test(&state, &input_name, 0, b"deleted", 1);
    state
        .manifest
        .files
        .push(manifest_meta_for_recovery_test(&input));
    crate::metadata::ManifestPersistence::save(temp_dir.path(), &state.manifest)
        .expect("persist prepublication manifest");
    state
        .append_intent(crate::runtime::IntentLogEntry::CompactionPublish {
            phase: PublicationPhase::ManifestPublished,
            cf_id: 0,
            removed: vec![input_name.clone()],
            added: Vec::new(),
        })
        .expect("persist remove-only publication intent");
    drop(state);

    // Act
    let mut reopened = RuntimeState::try_new(
        temp_dir.path().to_path_buf(),
        false,
        crate::config::RecoveryPolicy::Strict,
    )
    .expect("reopen state");
    reopened.replay_intent_log().expect("replay intent");

    // Assert
    assert!(!reopened.manifest_has_file(&input_name));
    assert!(!reopened.sst_dir.join(input_name).exists());
}

#[test]
fn should_supersede_stale_output_durable_intent_when_retrying_same_compaction_inputs() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create recovery directory");
    let mut state = RuntimeState::new(temp_dir.path().to_path_buf(), false);
    let input_name = crate::sst::file_name(0, 0, 1);
    let first_output = crate::runtime::FileMeta {
        name: crate::sst::file_name(0, 1, 2),
        level: 1,
        size_bytes: 1,
        content_crc32c: None,
        cf_id: 0,
        smallest_key: Some(b"a".to_vec()),
        largest_key: Some(b"a".to_vec()),
        smallest_seq: Some(2),
        largest_seq: Some(2),
        key_bounds_complete: true,
    };
    let retry_output = crate::runtime::FileMeta {
        name: crate::sst::file_name(0, 1, 3),
        level: 1,
        size_bytes: 1,
        content_crc32c: None,
        cf_id: 0,
        smallest_key: Some(b"a".to_vec()),
        largest_key: Some(b"a".to_vec()),
        smallest_seq: Some(3),
        largest_seq: Some(3),
        key_bounds_complete: true,
    };
    state
        .record_compaction_publication_intent(0, vec![input_name.clone()], vec![first_output])
        .expect("persist first compaction intent");

    // Act
    state
        .record_compaction_publication_intent(0, vec![input_name], vec![retry_output.clone()])
        .expect("persist retry compaction intent");

    // Assert
    let compaction_intents: Vec<_> = state
        .intent_log
        .iter()
        .filter_map(|entry| match entry {
            crate::runtime::IntentLogEntry::CompactionPublish { added, .. } => Some(added),
            _ => None,
        })
        .collect();
    assert_eq!(compaction_intents.len(), 1);
    assert_eq!(compaction_intents[0][0].name, retry_output.name);
}

#[test]
fn should_keep_distinct_remove_only_compaction_intents_for_distinct_inputs() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create intent directory");
    let mut state = RuntimeState::new(temp_dir.path().to_path_buf(), false);
    let first_input = crate::sst::file_name(0, 6, 1);
    let second_input = crate::sst::file_name(0, 6, 2);
    state
        .record_compaction_publication_intent(0, vec![first_input.clone()], Vec::new())
        .expect("persist first remove-only intent");

    // Act
    state
        .record_compaction_publication_intent(0, vec![second_input.clone()], Vec::new())
        .expect("persist independent remove-only intent");

    // Assert
    let removed_sets: Vec<_> = state
        .intent_log
        .iter()
        .filter_map(|entry| match entry {
            crate::runtime::IntentLogEntry::CompactionPublish { removed, .. } => {
                Some(removed.as_slice())
            }
            _ => None,
        })
        .collect();
    assert_eq!(removed_sets.len(), 2);
    assert!(removed_sets.contains(&[first_input].as_slice()));
    assert!(removed_sets.contains(&[second_input].as_slice()));
}

#[test]
fn should_recover_published_retry_when_legacy_stale_intent_precedes_it() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create recovery directory");
    let mut state = RuntimeState::new(temp_dir.path().to_path_buf(), false);
    let input_name = crate::sst::file_name(0, 0, 1);
    let stale_output_name = crate::sst::file_name(0, 1, 2);
    let retry_output_name = crate::sst::file_name(0, 1, 3);
    let stale_output = write_valid_sst_for_recovery_test(&state, &stale_output_name, 1, b"key", 2);
    let retry_output = write_valid_sst_for_recovery_test(&state, &retry_output_name, 1, b"key", 3);
    state
        .manifest
        .files
        .push(manifest_meta_for_recovery_test(&retry_output));
    state
        .append_intent(crate::runtime::IntentLogEntry::CompactionPublish {
            phase: PublicationPhase::OutputDurable,
            cf_id: 0,
            removed: vec![input_name.clone()],
            added: vec![stale_output],
        })
        .expect("persist stale intent");
    state
        .append_intent(crate::runtime::IntentLogEntry::CompactionPublish {
            phase: PublicationPhase::OutputDurable,
            cf_id: 0,
            removed: vec![input_name],
            added: vec![retry_output],
        })
        .expect("persist published retry intent");

    // Act
    state
        .replay_intent_log()
        .expect("published retry should disambiguate stale output");

    // Assert
    assert!(!state.sst_dir.join(stale_output_name).exists());
    assert!(state.sst_dir.join(retry_output_name).exists());
    assert!(state.intent_log.is_empty());
}

#[test]
fn should_recover_manifest_published_retry_when_it_precedes_stale_output_intent() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create recovery directory");
    let mut state = RuntimeState::new(temp_dir.path().to_path_buf(), false);
    let input_name = crate::sst::file_name(0, 0, 1);
    let stale_output_name = crate::sst::file_name(0, 1, 2);
    let retry_output_name = crate::sst::file_name(0, 1, 3);
    let input = write_valid_sst_for_recovery_test(&state, &input_name, 0, b"key", 1);
    let stale_output = write_valid_sst_for_recovery_test(&state, &stale_output_name, 1, b"key", 2);
    let retry_output = write_valid_sst_for_recovery_test(&state, &retry_output_name, 1, b"key", 3);
    state
        .manifest
        .files
        .push(manifest_meta_for_recovery_test(&input));
    state
        .append_intent(crate::runtime::IntentLogEntry::CompactionPublish {
            phase: PublicationPhase::ManifestPublished,
            cf_id: 0,
            removed: vec![input_name.clone()],
            added: vec![retry_output],
        })
        .expect("persist published retry intent");
    state
        .append_intent(crate::runtime::IntentLogEntry::CompactionPublish {
            phase: PublicationPhase::OutputDurable,
            cf_id: 0,
            removed: vec![input_name.clone()],
            added: vec![stale_output],
        })
        .expect("persist stale intent after retry");

    // Act
    state.replay_intent_log().expect("replay canonical retry");

    // Assert
    assert!(!state.manifest_has_file(&input_name));
    assert!(state.manifest_has_file(&retry_output_name));
    assert!(!state.manifest_has_file(&stale_output_name));
    assert!(!state.sst_dir.join(input_name).exists());
    assert!(state.sst_dir.join(retry_output_name).exists());
    assert!(!state.sst_dir.join(stale_output_name).exists());
}

#[test]
fn should_fail_closed_when_multiple_retry_outputs_are_manifest_visible() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create recovery directory");
    let mut state = RuntimeState::new(temp_dir.path().to_path_buf(), false);
    let input_name = crate::sst::file_name(0, 0, 1);
    let first_output_name = crate::sst::file_name(0, 1, 2);
    let second_output_name = crate::sst::file_name(0, 1, 3);
    let first_output = write_valid_sst_for_recovery_test(&state, &first_output_name, 1, b"key", 2);
    let second_output =
        write_valid_sst_for_recovery_test(&state, &second_output_name, 1, b"key", 3);
    state
        .manifest
        .files
        .extend([&first_output, &second_output].map(manifest_meta_for_recovery_test));
    for output in [first_output, second_output] {
        state
            .append_intent(crate::runtime::IntentLogEntry::CompactionPublish {
                phase: PublicationPhase::OutputDurable,
                cf_id: 0,
                removed: vec![input_name.clone()],
                added: vec![output],
            })
            .expect("persist legacy retry intent");
    }

    // Act
    let error = state
        .replay_intent_log()
        .expect_err("two published retry outputs are ambiguous");

    // Assert
    assert!(matches!(
        error,
        crate::common::MidgeError::RecoveryFailed(_)
    ));
    assert!(state.manifest_has_file(&first_output_name));
    assert!(state.manifest_has_file(&second_output_name));
    assert!(state.sst_dir.join(first_output_name).exists());
    assert!(state.sst_dir.join(second_output_name).exists());
    assert_eq!(state.intent_log.len(), 2);
}

#[test]
fn should_reject_compaction_intent_for_dropped_column_family_after_crash() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create recovery directory");
    let mut state = RuntimeState::new(temp_dir.path().to_path_buf(), false);
    let cf_id = state.manifest.create_column_family("dropped".to_string());
    state
        .column_families
        .insert(cf_id, ColumnFamilyState::new(cf_id, "dropped".to_string()));
    let input_name = crate::sst::file_name(cf_id, 0, 1);
    let output_name = crate::sst::file_name(cf_id, 1, 2);
    let mut input = write_valid_sst_for_recovery_test(&state, &input_name, 0, b"key", 1);
    input.cf_id = cf_id;
    let mut output = write_valid_sst_for_recovery_test(&state, &output_name, 1, b"key", 2);
    output.cf_id = cf_id;
    state
        .manifest
        .files
        .push(manifest_meta_for_recovery_test(&input));
    state
        .append_intent(crate::runtime::IntentLogEntry::CompactionPublish {
            phase: PublicationPhase::ManifestPublished,
            cf_id,
            removed: vec![input_name.clone()],
            added: vec![output],
        })
        .expect("persist late compaction intent");
    assert!(state.manifest.delete_column_family_with_reclamation(
        cf_id,
        2,
        vec![input_name.clone()]
    ));
    crate::metadata::ManifestPersistence::save(temp_dir.path(), &state.manifest)
        .expect("persist dropped column family");
    drop(state);

    // Act
    let mut reopened = RuntimeState::try_new(
        temp_dir.path().to_path_buf(),
        false,
        crate::config::RecoveryPolicy::Strict,
    )
    .expect("reopen state");
    reopened.replay_intent_log().expect("reject late intent");

    // Assert
    assert!(reopened.manifest_has_file(&input_name));
    assert!(!reopened.manifest_has_file(&output_name));
    assert!(reopened.sst_dir.join(input_name).exists());
    assert!(!reopened.sst_dir.join(output_name).exists());
    assert!(reopened.persistence_anomaly_detected());
}

#[test]
fn should_fail_closed_when_output_durable_compaction_manifest_is_partial() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create recovery directory");
    let mut state = RuntimeState::new(temp_dir.path().to_path_buf(), false);
    let first_input_name = crate::sst::file_name(0, 0, 1);
    let missing_input_name = crate::sst::file_name(0, 0, 2);
    let output_name = crate::sst::file_name(0, 1, 3);
    let first_input = write_valid_sst_for_recovery_test(&state, &first_input_name, 0, b"a", 1);
    let output = write_valid_sst_for_recovery_test(&state, &output_name, 1, b"a", 3);
    state
        .manifest
        .files
        .push(manifest_meta_for_recovery_test(&first_input));
    state
        .append_intent(crate::runtime::IntentLogEntry::CompactionPublish {
            phase: PublicationPhase::OutputDurable,
            cf_id: 0,
            removed: vec![first_input_name.clone(), missing_input_name],
            added: vec![output],
        })
        .expect("persist output-durable intent");

    // Act
    let error = state
        .replay_intent_log()
        .expect_err("partial compaction authority must fail strict recovery");

    // Assert
    assert!(matches!(
        error,
        crate::common::MidgeError::RecoveryFailed(_)
    ));
    assert!(state.manifest_has_file(&first_input_name));
    assert!(state.sst_dir.join(&first_input_name).exists());
    assert!(state.sst_dir.join(&output_name).exists());
}

#[test]
fn should_delete_untracked_compaction_output_during_startup_residue_cleanup() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create residue directory");
    let mut state = RuntimeState::new(temp_dir.path().to_path_buf(), false);
    let orphan_name = crate::sst::file_name(0, 1, 9);
    let _orphan = write_valid_sst_for_recovery_test(&state, &orphan_name, 1, b"orphan", 9);
    assert!(state.sst_dir.join(&orphan_name).exists());

    // Act
    state.cleanup_storage_residue();

    // Assert
    assert!(!state.sst_dir.join(orphan_name).exists());
}

#[test]
fn should_maintain_memtable_size_limit() {
    // Arrange
    let state = RuntimeState::new("/tmp/test_midge".into(), true);

    // Act
    // (none)

    // Assert
    assert!(state.memtable_size_limit > 0);
    assert_eq!(state.memtable_size_limit, 64 * 1024 * 1024); // 64MB
}

#[test]
fn should_respect_read_only_flag() {
    // Arrange
    let mut state = RuntimeState::new("/tmp/test_midge".into(), true);

    // Assert - Initially not read-only
    assert!(!state.is_read_only());

    // Act - Set read-only
    state.set_read_only(true);

    // Assert
    assert!(state.is_read_only());
}

#[test]
fn should_handle_multiple_column_families() {
    // Arrange
    let mut state = RuntimeState::new("/tmp/test_midge".into(), true);

    // Act
    let cf1 = state
        .create_cf("cf1".to_string())
        .expect("create_cf should succeed");
    let cf2 = state
        .create_cf("cf2".to_string())
        .expect("create_cf should succeed");
    let cf3 = state
        .create_cf("cf3".to_string())
        .expect("create_cf should succeed");

    // Assert
    assert_eq!(state.column_families.len(), 4); // default + 3 created
    assert!(state.get_cf(cf1).is_some());
    assert!(state.get_cf(cf2).is_some());
    assert!(state.get_cf(cf3).is_some());
}

#[test]
fn should_protect_every_memtable_generation_when_computing_cloud_wal_floor() {
    // Arrange
    let mut state = RuntimeState::new(isolated_test_db_path(), false);
    state.wal.current_segment_id = 20;
    let secondary = state.create_cf("floor-secondary".into()).unwrap();
    for (cf_id, segment) in [(0, 5), (secondary, 7)] {
        let cf = state.get_cf_mut(cf_id).unwrap();
        cf.active_memtable_started_in_segment = segment;
        cf.memtable
            .put_with_seq(b"key".to_vec(), b"value".to_vec(), segment, None)
            .unwrap();
    }
    let frozen = state.get_cf(0).unwrap().memtable.clone();
    let generation = state
        .track_new_immutable_flush(0, frozen.clone(), 11)
        .unwrap();
    let cf = state.get_cf_mut(0).unwrap();
    cf.memtable = Arc::new(SkipListMemtable::new());
    cf.active_memtable_started_in_segment = 11;
    cf.memtable
        .put_with_seq(b"new".to_vec(), b"value".to_vec(), 12, None)
        .unwrap();

    // Act
    let initial = state.cloud_wal_recovery_floor_segment();
    let (_, queued) = state
        .immutable_flush_by_id_mut(generation.flush_id)
        .unwrap();
    queued.phase = ImmutableFlushPhase::RetryPending;
    queued.retry_at = Instant::now() + Duration::from_mins(1);
    let retrying = state.cloud_wal_recovery_floor_segment();
    state.complete_immutable_flush(0, &frozen).unwrap();
    let after_publication = state.cloud_wal_recovery_floor_segment();

    // Assert
    assert_eq!(initial, Some(5));
    assert_eq!(retrying, Some(5));
    assert_eq!(after_publication, Some(7));
}

#[test]
fn should_retain_all_wal_when_immutable_provenance_is_untracked() {
    // Arrange
    let mut state = RuntimeState::new(isolated_test_db_path(), false);
    state.wal.current_segment_id = 20;
    state
        .get_cf_mut(0)
        .unwrap()
        .immutable_memtables
        .push(Arc::new(SkipListMemtable::new()));

    // Act
    let floor = state.cloud_wal_recovery_floor_segment();

    // Assert
    assert_eq!(floor, None);
}

#[test]
fn should_preserve_active_wal_floor_when_freeze_identity_is_exhausted() {
    // Arrange
    let mut state = RuntimeState::new(isolated_test_db_path(), false);
    state.wal.current_segment_id = 20;
    state.next_flush_id = u64::MAX;
    let cf = state.get_cf_mut(0).unwrap();
    cf.active_memtable_started_in_segment = 3;
    cf.memtable
        .put_with_seq(b"key".to_vec(), b"value".to_vec(), 5, None)
        .unwrap();
    let table = cf.memtable.clone();

    // Act
    let generation = state.track_new_immutable_flush(0, table, 5);

    // Assert
    assert!(generation.is_none());
    assert_eq!(state.cloud_wal_recovery_floor_segment(), Some(3));
    assert!(state.get_cf(0).unwrap().immutable_memtables.is_empty());
}

#[test]
fn should_retain_all_wal_when_tracked_generation_provenance_is_unknown() {
    // Arrange
    let mut state = RuntimeState::new(isolated_test_db_path(), false);
    let table = state.get_cf(0).unwrap().memtable.clone();
    let flush = state.track_new_immutable_flush(0, table, 1).unwrap();
    state
        .immutable_flush_by_id_mut(flush.flush_id)
        .unwrap()
        .1
        .first_wal_segment = None;

    // Act
    let floor = state.cloud_wal_recovery_floor_segment();

    // Assert
    assert_eq!(floor, None);
}
