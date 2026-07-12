use super::*;
use std::path::PathBuf;

fn isolated_test_db_path() -> PathBuf {
    tempfile::tempdir().expect("temp dir").keep()
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
fn should_track_immutable_memtables_in_cf_state() {
    // Arrange
    let mut cf = ColumnFamilyState::new(1, "cf".to_string());
    let imm_memtable = Arc::new(SkipListMemtable::new());

    // Act
    cf.immutable_memtables.push(imm_memtable.clone());
    cf.immutable_memtables.push(imm_memtable.clone());

    // Assert
    assert_eq!(cf.immutable_memtables.len(), 2);
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
        .track_new_immutable_flush(
            0,
            Arc::clone(&immutable),
            crate::sst::file_name(0, 0, 1),
            1,
            1,
        )
        .expect("track immutable");
    state
        .mark_immutable_flush_failed(0, &tracked.memtable)
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
        .track_new_immutable_flush(
            0,
            Arc::clone(&immutable),
            crate::sst::file_name(0, 0, 1),
            1,
            1,
        )
        .expect("track immutable");

    // Act
    let mut final_delay = Duration::ZERO;
    for _ in 0..32 {
        final_delay = state
            .mark_immutable_flush_failed(0, &tracked.memtable)
            .expect("mark failed");
        state.make_immutable_flush_retry_due(0);
        state
            .begin_pending_immutable_flush(0, true)
            .expect("begin retry");
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

#[test]
fn should_maintain_wal_durability_frontiers() {
    // Arrange
    let wal = WalState {
        last_synced_seq: 10,
        local_durable_seq: 10,
        pending_writes: 5,
        cloud_durable_seq: 8,
        ..Default::default()
    };

    // Act
    // (none)

    // Assert - Verify monotonicity constraints
    assert!(wal.cloud_durable_seq <= wal.local_durable_seq);
    assert!(wal.local_durable_seq >= wal.last_synced_seq);
    assert!(wal.pending_writes < usize::MAX);
}

#[test]
fn should_track_segment_rotation() {
    // Arrange
    let mut wal = WalState::default();
    let initial_segment = wal.current_segment_id;

    // Act
    wal.current_segment_id += 1;
    wal.current_segment_id += 1;

    // Assert
    assert_eq!(wal.current_segment_id, initial_segment + 2);
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

#[test]
fn should_track_compacting_ssts() {
    // Arrange
    let mut compaction = CompactionState::default();

    // Act
    compaction
        .compacting_ssts
        .push(crate::sst::file_name(0, 0, 1));
    compaction
        .compacting_ssts
        .push(crate::sst::file_name(0, 0, 2));
    compaction.pending_tasks = 2;

    // Assert
    assert_eq!(compaction.compacting_ssts.len(), 2);
    assert_eq!(compaction.pending_tasks, 2);
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

#[test]
fn should_track_pending_uploads() {
    // Arrange
    let mut cloud = CloudState::default();

    // Act
    cloud.pending_uploads.push(crate::sst::file_name(0, 0, 1));
    cloud.last_cloud_checkpoint_seq = 100;

    // Assert
    assert_eq!(cloud.pending_uploads.len(), 1);
    assert_eq!(cloud.last_cloud_checkpoint_seq, 100);
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
fn should_initialize_default_column_family() {
    // Arrange
    // (no setup)

    // Act
    let state = RuntimeState::new("/tmp/test_midge".into(), true);

    // Assert
    let cf0 = state.get_cf(0).expect("Default CF should exist");
    assert_eq!(cf0.memtable.size_bytes(), 0);
    assert!(cf0.immutable_memtables.is_empty());
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
fn should_track_wal_state_separately() {
    // Arrange
    let mut state = RuntimeState::new("/tmp/test_midge".into(), true);

    // Act
    state.wal.current_segment_id = 5;
    state.wal.pending_writes = 10;

    // Assert
    assert_eq!(state.wal.current_segment_id, 5);
    assert_eq!(state.wal.pending_writes, 10);
}

#[test]
fn should_track_compaction_state_separately() {
    // Arrange
    let mut state = RuntimeState::new("/tmp/test_midge".into(), true);

    // Act
    state
        .compaction
        .compacting_ssts
        .push(crate::sst::file_name(0, 0, 1));
    state.compaction.pending_tasks = 3;

    // Assert
    assert_eq!(state.compaction.compacting_ssts.len(), 1);
    assert_eq!(state.compaction.pending_tasks, 3);
}

#[test]
fn should_track_cloud_state_separately() {
    // Arrange
    let mut state = RuntimeState::new("/tmp/test_midge".into(), true);

    // Act
    state
        .cloud
        .pending_uploads
        .push(crate::sst::file_name(0, 0, 50));
    state.cloud.last_cloud_checkpoint_seq = 50;

    // Assert
    assert_eq!(state.cloud.pending_uploads.len(), 1);
    assert_eq!(state.cloud.last_cloud_checkpoint_seq, 50);
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
fn should_track_all_state_components_independently() {
    // Arrange
    let mut state = RuntimeState::new("/tmp/test_midge".into(), true);

    // Act
    let seq1 = state.next_sequence();
    let txn1 = state.next_txn_id();
    state.wal.pending_writes = 5;
    state.compaction.pending_tasks = 2;
    state.cloud.last_cloud_checkpoint_seq = 100;

    // Assert
    assert_eq!(seq1, 1);
    assert_eq!(txn1, 1);
    assert_eq!(state.wal.pending_writes, 5);
    assert_eq!(state.compaction.pending_tasks, 2);
    assert_eq!(state.cloud.last_cloud_checkpoint_seq, 100);
}
