use super::*;
use crate::runtime::hybrid_persistence::HybridPersistence;
use crate::runtime::{state::RuntimeState, ResponseRouter};
use crate::sst::Memtable;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(0);

#[test]
fn should_dispatch_compact_all_before_background_cloud_progress() {
    // Arrange
    let request = RuntimeMsg::CompactAll { request_id: 91 };

    // Act
    let progress_first = request.is_mutation();

    // Assert
    assert!(!progress_first);
}

#[test]
fn should_wait_for_active_compaction_before_declaring_debt_clear() -> crate::common::MidgeResult<()>
{
    // Arrange
    let mut event_loop = create_test_event_loop()?;
    event_loop
        .compaction_actor
        .prepare_for_completion_test(&mut event_loop.state, &["active-input.sst".to_string()])?;
    let request_id = 93;
    let response = event_loop.router.register(request_id, "CompactAll");

    // Act
    super::compaction::CompactionCoordinator::compact_all(&mut event_loop, request_id);

    // Assert
    assert!(response.try_recv().is_err());
    assert_eq!(
        event_loop
            .state
            .pending_compaction_waits
            .lock()
            .get(&request_id)
            .map(String::as_str),
        Some("CompactAll")
    );
    Ok(())
}

#[test]
fn should_drain_flush_completion_after_non_mutating_request() -> crate::common::MidgeResult<()> {
    // Arrange
    let mut event_loop = create_test_local_event_loop()?;
    event_loop.state.sequence = 1;
    event_loop
        .state
        .get_cf(0)
        .expect("default column family")
        .memtable
        .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)?;
    event_loop.freeze_active_memtable(0)?;
    event_loop.schedule_next_flush_worker();
    let completion_deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while event_loop.flush_worker_result_rx.is_empty()
        && std::time::Instant::now() < completion_deadline
    {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert!(!event_loop.flush_worker_result_rx.is_empty());
    let request_id = 92;
    let response = event_loop.router.register(request_id, "Noop");
    let (_msg_tx, msg_rx) = crossbeam::channel::unbounded();

    // Act
    event_loop.process_one(RuntimeMsg::Noop { request_id }, &msg_rx);

    // Assert
    assert!(matches!(
        response.recv_timeout(std::time::Duration::from_secs(1)),
        Ok(RuntimeResponse::Ok { request_id: 92 })
    ));
    assert_eq!(event_loop.state.flush_metrics.build_count, 1);
    Ok(())
}

#[test]
fn should_retry_transient_flush_failure_during_shutdown_checkpoint(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut event_loop = create_test_local_event_loop()?;
    event_loop.state.sequence = 1;
    event_loop
        .state
        .get_cf(0)
        .expect("default column family")
        .memtable
        .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)?;
    let flush_id = event_loop
        .freeze_active_memtable(0)?
        .expect("freeze non-empty memtable");
    event_loop
        .state
        .mark_immutable_flush_failed(flush_id)
        .expect("retain failed immutable for retry");
    let deadline = crate::common::OperationDeadline::from_budget(std::time::Duration::from_secs(2));

    // Act
    event_loop.drain_shutdown_flush_pipeline_within(&deadline)?;

    // Assert
    assert!(event_loop
        .state
        .get_cf(0)
        .expect("default column family")
        .immutable_flushes
        .is_empty());
    Ok(())
}

fn unique_test_db_path(prefix: &str) -> std::path::PathBuf {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let counter = TEST_DB_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "{prefix}_{}_{}_{}",
        std::process::id(),
        unique,
        counter
    ))
}

// Helper to create a minimal runtime state for testing
pub(in crate::runtime::event_loop) fn create_test_state() -> RuntimeState {
    RuntimeState::new("/tmp/test_event_loop".into(), true) // Memory mode
}

// Helper to create a new event loop
pub(in crate::runtime::event_loop) fn create_test_event_loop(
) -> crate::common::MidgeResult<EventLoop> {
    let state = create_test_state();
    let router = Arc::new(ResponseRouter::new());
    EventLoop::new(
        state,
        false,
        router,
        crate::runtime::RuntimeConfig::default(),
        None,
    )
}

pub(in crate::runtime::event_loop) fn create_test_cloud_event_loop(
    storage_policy: crate::storage::hybrid::policy::StorageBudgetPolicy,
) -> crate::common::MidgeResult<EventLoop> {
    let db_path = unique_test_db_path("midge_event_loop_cloud");
    std::fs::create_dir_all(&db_path).expect("create temp cloud event loop dir");

    let state = RuntimeState::new(db_path.clone(), false);
    let router = Arc::new(ResponseRouter::new());
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(db_path.join("hybrid_local"))
            .expect("create local backend"),
    );
    let cloud = Arc::new(
        crate::storage::filesystem::FileSystem::new(db_path.join("cloud_store"))
            .expect("create cloud backend"),
    );
    let hybrid_storage = Arc::new(crate::storage::HybridStorage::with_policy(
        local,
        cloud,
        storage_policy,
    ));
    hybrid_storage.fence_cloud_wal_catalog(1)?;
    let config = crate::runtime::RuntimeConfig {
        wal_durability_policy: crate::wal::DurabilityPolicy::CloudAsync,
        hybrid_storage: Some(Arc::clone(&hybrid_storage)),
        writer_epoch: 1,
        ..crate::runtime::RuntimeConfig::default()
    };
    EventLoop::new(state, false, router, config, None)
}

pub(in crate::runtime::event_loop) fn create_test_local_event_loop(
) -> crate::common::MidgeResult<EventLoop> {
    let db_path = unique_test_db_path("midge_event_loop_local");
    std::fs::create_dir_all(&db_path).expect("create temp local event loop dir");

    let state = RuntimeState::new(db_path, false);
    let router = Arc::new(ResponseRouter::new());
    EventLoop::new(
        state,
        false,
        router,
        crate::runtime::RuntimeConfig::default(),
        None,
    )
}

#[test]
fn should_fail_every_held_request_when_shutdown_drain_restores_deferred_work() {
    // Arrange: this is the exact state produced when a flush completion pops
    // one deferred DDL request into `pending_msg` while shutdown is draining.
    let mut event_loop = create_test_event_loop().expect("create event loop");
    let request_ids = [8101_u64, 8102, 8103, 8104, 8105, 8106, 8107, 8108];
    let receivers = request_ids
        .into_iter()
        .map(|request_id| {
            (
                request_id,
                event_loop.router.register(request_id, "TestRequest"),
            )
        })
        .collect::<Vec<_>>();
    event_loop.pending_msg = Some(RuntimeMsg::ManifestCreateColumnFamily {
        request_id: 8101,
        name: "restored-during-drain".to_string(),
    });
    event_loop
        .publication_gate
        .deferred_messages
        .push_back(RuntimeMsg::ManifestDropColumnFamily {
            request_id: 8102,
            cf_id: 1,
            discard_unflushed: false,
        });
    event_loop
        .verification_barrier
        .deferred_messages
        .push_back(RuntimeMsg::ManifestPersist { request_id: 8103 });
    event_loop.flush_barrier_waiters.insert(
        0,
        vec![super::flush::FlushBarrierWaiter {
            request_id: 8104,
            frontier: 1,
        }],
    );
    event_loop
        .state
        .pending_compaction_waits
        .lock()
        .insert(8105, "CompactAll".to_string());
    event_loop.write_stall_waiters.insert(8106, 0);
    event_loop
        .write_stall_waiter_queues
        .entry(0)
        .or_default()
        .push_back(8106);
    event_loop.durability.queue_waiter_for_key(
        0,
        crate::runtime::durability::DurabilityWaiter::CloudDurability { request_id: 8107 },
    );
    event_loop.durability.queue_waiter(
        crate::runtime::durability::DurabilityWaiter::CloudDurability { request_id: 8108 },
    );
    event_loop.shutting_down = true;

    // Act
    event_loop.restore_verification_deferred_message();
    event_loop.restore_publication_deferred_message();
    event_loop.fail_shutdown_held_work();

    // Assert
    for (request_id, receiver) in receivers {
        let response = receiver
            .recv_timeout(std::time::Duration::from_millis(100))
            .expect("shutdown rejection response");
        assert!(
            matches!(
                response,
                RuntimeResponse::Error {
                    request_id: response_id,
                    error: crate::common::MidgeError::Busy(_),
                } if response_id == request_id
            ),
            "request {request_id} received the wrong shutdown response"
        );
    }
    assert!(event_loop.pending_msg.is_none());
    assert!(event_loop.publication_gate.deferred_messages.is_empty());
    assert!(event_loop.verification_barrier.deferred_messages.is_empty());
    assert!(event_loop.flush_barrier_waiters.is_empty());
    assert!(event_loop.state.pending_compaction_waits.lock().is_empty());
    assert!(event_loop.write_stall_waiters.is_empty());
    assert!(event_loop.write_stall_waiter_queues.is_empty());
    assert!(!event_loop.durability.has_pending_waiters());
}

#[test]
fn should_retain_writer_epochs_in_recovered_cloud_wal_runtime_config() {
    // Arrange
    let config = crate::runtime::RuntimeConfig {
        recovered_cloud_wal_segments: [(1, 60)].into_iter().collect(),
        recovered_cloud_wal_segment_epochs: [(1, 7)].into_iter().collect(),
        recovered_local_wal_segments: [(2, 100)].into_iter().collect(),
        recovered_local_wal_segment_epochs: [(2, 8)].into_iter().collect(),
        ..crate::runtime::RuntimeConfig::default()
    };

    // Act
    let recovered = RecoveredCloudWalConfig::from(&config);

    // Assert
    assert_eq!(
        recovered.remote_segments.get(&1),
        Some(&crate::runtime::RecoveredCloudWalSegment {
            max_sequence: 60,
            writer_epoch: 7,
        })
    );
    assert_eq!(
        recovered.local_segments.get(&2),
        Some(&crate::runtime::RecoveredCloudWalSegment {
            max_sequence: 100,
            writer_epoch: 8,
        })
    );
}

#[test]
fn should_return_invalid_argument_given_column_family_create_during_ingest() {
    // Arrange
    let mut event_loop = create_test_event_loop().expect("create event loop");
    event_loop
        .state
        .ingest_active
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let (_msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let request_id = 901;
    let response_rx = event_loop.router.register(request_id, "TestRequest");

    // Act
    super::manifest::ManifestCoordinator::create_column_family(
        &mut event_loop,
        &msg_rx,
        request_id,
        "during-ingest",
    );
    let response = response_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("receive create response");

    // Assert
    assert!(matches!(
        response,
        RuntimeResponse::Error {
            error: crate::common::MidgeError::InvalidArgument(message),
            ..
        } if message.contains("ingest mode")
    ));
}

#[test]
fn should_return_invalid_argument_given_column_family_drop_during_ingest() {
    // Arrange
    let mut event_loop = create_test_event_loop().expect("create event loop");
    event_loop
        .state
        .ingest_active
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let (_msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let request_id = 902;
    let response_rx = event_loop.router.register(request_id, "TestRequest");

    // Act
    super::manifest::ManifestCoordinator::drop_column_family(
        &mut event_loop,
        &msg_rx,
        request_id,
        42,
        false,
    );
    let response = response_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("receive drop response");

    // Assert
    assert!(matches!(
        response,
        RuntimeResponse::Error {
            error: crate::common::MidgeError::InvalidArgument(message),
            ..
        } if message.contains("ingest mode")
    ));
}

#[test]
fn should_hold_cloud_frontier_at_local_recovery_gap_until_resumed_upload_is_acknowledged(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let db_path = unique_test_db_path("midge_recovered_cloud_wal_gap");
    let wal_dir = db_path.join("wal");
    let initialized = RuntimeState::new(db_path.clone(), false);
    drop(initialized);
    let local_segment = wal_dir.join(crate::wal::segment_file_name(2));
    let record = crate::wal::WalRecord::new(
        crate::wal::WalOpKind::Put,
        bytes::Bytes::from_static(b"recovered-gap"),
        Some(bytes::Bytes::from_static(b"value")),
        2,
        0,
    );
    let payload = crate::wal::encoding::encode(&record).expect("encode recovered WAL record");
    let mut segment_bytes = Vec::new();
    crate::wal::frame::append_frame(&mut segment_bytes, &payload)
        .expect("frame recovered WAL record");
    std::fs::write(&local_segment, &segment_bytes).expect("write recovered local WAL segment");

    let mut state = RuntimeState::new(db_path.clone(), false);
    state.sequence = 3;
    state.wal.local_durable_seq = 3;
    state.reset_cloud_durable_sequence_for_recovery();
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(db_path.join("hybrid_local"))
            .expect("create local backend"),
    );
    let cloud = Arc::new(
        crate::storage::filesystem::FileSystem::new(db_path.join("cloud_store"))
            .expect("create cloud backend"),
    );
    let hybrid_storage = Arc::new(crate::storage::HybridStorage::with_policy(
        local,
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    ));
    hybrid_storage.fence_cloud_wal_catalog(1)?;
    let config = crate::runtime::RuntimeConfig {
        wal_durability_policy: crate::wal::DurabilityPolicy::CloudAsync,
        hybrid_storage: Some(Arc::clone(&hybrid_storage)),
        recovered_cloud_wal_segments: [(1, 1), (3, 3)].into_iter().collect(),
        recovered_local_wal_segments: [(2, 2)].into_iter().collect(),
        writer_epoch: 1,
        ..crate::runtime::RuntimeConfig::default()
    };
    let router = Arc::new(ResponseRouter::new());

    // Act
    let mut event_loop = EventLoop::new(state, false, router, config, None)?;

    // Assert
    assert_eq!(event_loop.state.wal.cloud_durable_seq, 1);
    assert_eq!(event_loop.cloud_wal.upload_backlog.get(&2), Some(&2));
    event_loop.drain_cloud_wal_upload_backlog();
    assert_eq!(hybrid_storage.pending_upload_count(), 1);
    assert_eq!(
        event_loop.durability.inflight_segment_for_sequence(2),
        Some(2)
    );

    let remote_segment = db_path
        .join("cloud_store")
        .join(crate::wal::cloud_segment_object_key(2, 0));
    std::fs::create_dir_all(remote_segment.parent().expect("remote WAL parent"))
        .expect("create remote WAL directory");
    std::fs::write(remote_segment, segment_bytes).expect("publish resumed WAL segment");
    event_loop.handle_storage_event(crate::storage::StorageEvent::CloudAck {
        segment_id: 2,
        max_sequence: 2,
    });
    assert_eq!(event_loop.state.wal.cloud_durable_seq, 3);
    Ok(())
}

#[test]
fn should_leave_later_write_queued_when_destructively_dropping_column_family() {
    // Arrange
    let mut event_loop = create_test_local_event_loop().expect("create event loop");
    let cf_id = event_loop
        .manifest_actor
        .create_column_family(&mut event_loop.state, "ordered-drop")
        .expect("create column family");
    let drop_request_id = 903;
    let write_request_id = 904;
    let drop_response = event_loop.router.register(drop_request_id, "TestRequest");
    let write_response = event_loop.router.register(write_request_id, "TestRequest");
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    msg_tx
        .send(RuntimeMsg::ApplyTransaction {
            request_id: write_request_id,
            ops: vec![crate::runtime::TransactionOp::Put {
                cf_id,
                key: bytes::Bytes::from_static(b"later-key"),
                value: bytes::Bytes::from_static(b"later-value"),
                ttl_seconds: None,
                insert_only: false,
            }],
            assertions: Vec::new(),
            durability_policy: Some(crate::wal::DurabilityPolicy::Batched),
            start_sequence: None,
            conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
            response_tx: None,
        })
        .expect("queue later write");

    // Act: the DDL WAL barrier must cover only already-processed writes. It
    // must not pull a message ordered after the destructive drop across that
    // boundary and then discard it.
    super::manifest::ManifestCoordinator::drop_column_family(
        &mut event_loop,
        &msg_rx,
        drop_request_id,
        cf_id,
        true,
    );

    // Assert
    assert!(matches!(
        drop_response
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("drop response"),
        RuntimeResponse::Ok { .. }
    ));
    assert!(write_response.try_recv().is_err());
    let later_write = msg_rx.try_recv().expect("later write must remain queued");
    event_loop.handle_runtime_msg(later_write, &msg_rx);
    assert!(matches!(
        write_response
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("later write response"),
        RuntimeResponse::Error {
            error: crate::common::MidgeError::InvalidArgument(message),
            ..
        } if message.contains("does not exist")
    ));
}

#[test]
fn should_cancel_column_family_waiters_after_drop_commits() {
    // Arrange
    let mut event_loop = create_test_local_event_loop().expect("create event loop");
    let cf_id = event_loop
        .manifest_actor
        .create_column_family(&mut event_loop.state, "drop-waiters")
        .expect("create column family");
    let flush_request_id = 905;
    let stall_request_id = 906;
    let drop_request_id = 907;
    let flush_response = event_loop.router.register(flush_request_id, "TestRequest");
    let stall_response = event_loop.router.register(stall_request_id, "TestRequest");
    let drop_response = event_loop.router.register(drop_request_id, "TestRequest");
    event_loop.flush_barrier_waiters.insert(
        cf_id,
        vec![super::flush::FlushBarrierWaiter {
            request_id: flush_request_id,
            frontier: 1,
        }],
    );
    event_loop
        .write_stall_waiters
        .insert(stall_request_id, cf_id);
    event_loop
        .write_stall_waiter_queues
        .entry(cf_id)
        .or_default()
        .push_back(stall_request_id);
    let (_msg_tx, msg_rx) = crossbeam::channel::unbounded();

    // Act
    super::manifest::ManifestCoordinator::drop_column_family(
        &mut event_loop,
        &msg_rx,
        drop_request_id,
        cf_id,
        false,
    );

    // Assert
    assert!(matches!(
        drop_response
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("drop response"),
        RuntimeResponse::Ok { .. }
    ));
    for response in [flush_response, stall_response] {
        assert!(matches!(
            response
                .recv_timeout(std::time::Duration::from_secs(1))
                .expect("canceled waiter response"),
            RuntimeResponse::Error {
                error: crate::common::MidgeError::InvalidArgument(message),
                ..
            } if message.contains("was dropped")
        ));
    }
    assert!(!event_loop.flush_barrier_waiters.contains_key(&cf_id));
    assert!(!event_loop
        .write_stall_waiters
        .contains_key(&stall_request_id));
    assert!(!event_loop.write_stall_waiter_queues.contains_key(&cf_id));
}

#[test]
fn should_apply_runtime_config_block_cache_policy_to_read_resources_when_initializing_event_loop(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let db_path = unique_test_db_path("midge_event_loop_config");
    std::fs::create_dir_all(&db_path).expect("create temp runtime config dir");
    let state = RuntimeState::new(db_path, false);
    let router = Arc::new(ResponseRouter::new());
    let config = crate::runtime::RuntimeConfig {
        block_cache_policy: crate::sst::cache::CachePolicyType::ClockPro,
        block_cache_size: 512 * 1024,
        l0_compaction_trigger: 7,
        ..crate::runtime::RuntimeConfig::default()
    };

    // Act
    let event_loop = EventLoop::new(state, false, router, config, None)?;

    // Assert
    assert_eq!(event_loop.compaction_actor.l0_file_count_threshold(), 7);
    let read_resources = event_loop
        .read_resources
        .as_ref()
        .expect("local runtime should construct read resources");
    assert_eq!(
        read_resources.block_cache().policy_type(),
        crate::sst::cache::CachePolicyType::ClockPro
    );
    Ok(())
}

fn valid_sst_bytes_for_event_loop_test(key: &[u8], value: &[u8], seq: u64) -> Vec<u8> {
    use crate::sst::SstFactory;

    let factory = crate::sst::FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096);
    let mut writer = factory.create().expect("create test SST writer");
    writer
        .add_with_meta(key, Some(value), seq, 0, None)
        .expect("add test SST entry");
    writer.finish_bytes().expect("finish test SST bytes")
}

fn test_manifest_l0_file_meta(name: &str, largest_seq: u64) -> crate::metadata::FileMeta {
    crate::metadata::FileMeta {
        name: name.to_string(),
        level: 0,
        size_bytes: 128,
        cf_id: 0,
        smallest_key: Some(format!("key-{largest_seq:04}").into_bytes()),
        largest_key: Some(format!("key-{largest_seq:04}").into_bytes()),
        smallest_seq: Some(largest_seq),
        largest_seq: Some(largest_seq),
        key_bounds_complete: true,
        ..Default::default()
    }
}

fn write_runtime_l0_sst_for_test(
    event_loop: &EventLoop,
    name: &str,
    largest_seq: u64,
) -> crate::runtime::FileMeta {
    let key = format!("key-{largest_seq:04}");
    let bytes = valid_sst_bytes_for_event_loop_test(key.as_bytes(), b"value", largest_seq);
    std::fs::create_dir_all(&event_loop.state.sst_dir).expect("create test sst dir");
    std::fs::write(event_loop.state.sst_dir.join(name), &bytes).expect("write test SST");
    crate::runtime::FileMeta {
        name: name.to_string(),
        level: 0,
        size_bytes: bytes.len() as u64,
        content_crc32c: Some(crc32c::crc32c(&bytes)),
        cf_id: 0,
        smallest_key: Some(key.clone().into_bytes()),
        largest_key: Some(key.into_bytes()),
        smallest_seq: Some(largest_seq),
        largest_seq: Some(largest_seq),
        key_bounds_complete: true,
    }
}

// =========== EventLoop Creation Tests ===========

#[test]
fn should_create_event_loop_given_supported_storage_modes() {
    // Arrange
    let memory_state = RuntimeState::new("/tmp/test_memory".into(), true);
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let fs_state = RuntimeState::new(temp_dir.path().to_path_buf(), false);

    // Act
    let memory_result = EventLoop::new(
        memory_state,
        false,
        Arc::new(ResponseRouter::new()),
        crate::runtime::RuntimeConfig::default(),
        None,
    );
    let fs_result = EventLoop::new(
        fs_state,
        false,
        Arc::new(ResponseRouter::new()),
        crate::runtime::RuntimeConfig::default(),
        None,
    );

    // Assert - both storage modes must construct successfully
    assert!(memory_result.is_ok());
    assert!(fs_result.is_ok());
}

#[test]
fn should_not_treat_blocked_auto_flush_candidate_as_standalone_actionable_work() {
    // Arrange
    let mut event_loop = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::new(1_000_000),
    )
    .expect("create cloud event loop");
    let hybrid = event_loop.hybrid_storage.as_ref().expect("hybrid storage");
    let reservation = hybrid
        .reserve_for_flush_with_token(960_000)
        .expect("test flush reservation");
    hybrid.flush_completed_with_token(reservation, 960_000);

    event_loop.state.memtable_flush_threshold = 1024;
    event_loop.state.memtable_size_limit = 1024 * 1024;
    event_loop.state.sequence = 1;
    {
        let cf = event_loop.state.get_cf(0).expect("default cf");
        cf.memtable
            .put_with_seq(b"key".to_vec(), vec![0xA5; 2048], 1, None)
            .expect("seed memtable");
    }
    event_loop.state.total_memtable_bytes = event_loop
        .state
        .get_cf(0)
        .expect("default cf")
        .memtable
        .size_bytes();

    // Act
    // Assert
    assert_eq!(event_loop.drain_auto_flush_memtables(), 1);
    assert_eq!(
        event_loop
            .state
            .get_cf(0)
            .expect("default cf")
            .immutable_flushes
            .len(),
        1,
        "the frozen memtable must remain queued for a bounded retry"
    );

    assert!(
        !event_loop.has_actionable_work(),
        "a retained flush with a future retry deadline must not spin the event loop"
    );
}

#[test]
fn should_schedule_compaction_after_l0_flush_when_threshold_reached(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut event_loop = create_test_local_event_loop().expect("create local event loop");
    let (worker_tx, _worker_rx) = crossbeam::channel::unbounded();
    event_loop.worker_msg_tx = Some(worker_tx);
    event_loop.state.set_compaction_enabled(true);
    event_loop
        .state
        .manifest
        .files
        .extend((1..=3).map(|seq| test_manifest_l0_file_meta(&format!("pre-{seq}.sst"), seq)));

    let flushed = write_runtime_l0_sst_for_test(&event_loop, "threshold-crossing.sst", 4);
    event_loop.publish_flushed_sst(0, "threshold-crossing.sst", 4, Some(flushed), None)?;

    // Act
    // Assert
    assert_eq!(
        event_loop
            .state
            .active_compactions
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "publishing the threshold-crossing L0 SST should schedule compaction"
    );
    assert_eq!(
        event_loop.state.compaction.compacting_ssts.len(),
        4,
        "scheduled L0 compaction should lock all threshold input files"
    );
    assert!(
        event_loop
            .state
            .compaction
            .compacting_ssts
            .iter()
            .any(|name| name == "threshold-crossing.sst"),
        "newly flushed SST should be part of the scheduled L0 compaction"
    );

    Ok(())
}

#[test]
fn should_schedule_compaction_from_existing_l0_files_during_startup_maintenance() {
    // Arrange
    let mut event_loop = create_test_local_event_loop().expect("create local event loop");
    let (worker_tx, _worker_rx) = crossbeam::channel::unbounded();
    event_loop.worker_msg_tx = Some(worker_tx);
    event_loop.state.set_compaction_enabled(true);
    for sequence in 1..=4 {
        let name = format!("recovered-{sequence}.sst");
        let _ = write_runtime_l0_sst_for_test(&event_loop, &name, sequence);
        event_loop
            .state
            .manifest
            .files
            .push(test_manifest_l0_file_meta(&name, sequence));
    }

    // Act
    event_loop.schedule_background_compaction_on_startup();

    // Assert
    assert_eq!(
        event_loop
            .state
            .active_compactions
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "an eligible recovered L0 layout should compact without another flush"
    );
    assert_eq!(event_loop.state.compaction.compacting_ssts.len(), 4);
}

#[test]
fn should_schedule_recovery_compaction_above_hard_l0_ceiling_when_background_disabled() {
    // Arrange
    let mut event_loop = create_test_local_event_loop().expect("create local event loop");
    let (worker_tx, _worker_rx) = crossbeam::channel::unbounded();
    event_loop.worker_msg_tx = Some(worker_tx);
    event_loop.state.set_compaction_enabled(false);
    event_loop.state.l0_compaction_trigger = 2;
    event_loop.state.max_immutable_memtables = 0;
    event_loop.compaction_actor.set_l0_file_count_threshold(2);
    assert_eq!(event_loop.state.l0_hard_ceiling(), 3);
    for sequence in 1..=4 {
        let name = format!("over-ceiling-{sequence}.sst");
        let _ = write_runtime_l0_sst_for_test(&event_loop, &name, sequence);
        event_loop
            .state
            .manifest
            .files
            .push(test_manifest_l0_file_meta(&name, sequence));
    }

    // Act
    event_loop.schedule_background_compaction_on_startup();

    // Assert
    assert_eq!(
        event_loop
            .state
            .active_compactions
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "startup must pay down an over-ceiling recovered layout even when ordinary background work is disabled"
    );
    assert!(!event_loop.state.compaction.compacting_ssts.is_empty());
}

#[test]
fn should_not_schedule_auto_compaction_when_compaction_disabled() -> crate::common::MidgeResult<()>
{
    // Arrange
    let mut event_loop = create_test_local_event_loop().expect("create local event loop");
    let (worker_tx, _worker_rx) = crossbeam::channel::unbounded();
    event_loop.worker_msg_tx = Some(worker_tx);
    event_loop.state.set_compaction_enabled(false);
    event_loop
        .state
        .manifest
        .files
        .extend((1..=3).map(|seq| test_manifest_l0_file_meta(&format!("pre-{seq}.sst"), seq)));

    let flushed = write_runtime_l0_sst_for_test(&event_loop, "disabled-threshold-crossing.sst", 4);
    event_loop.publish_flushed_sst(0, "disabled-threshold-crossing.sst", 4, Some(flushed), None)?;

    // Act
    // Assert
    assert_eq!(
        event_loop
            .state
            .active_compactions
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "post-flush compaction scheduling should respect enable_compaction=false"
    );
    assert!(
        event_loop.state.compaction.compacting_ssts.is_empty(),
        "disabled compaction should not lock SSTs after flush publication"
    );

    Ok(())
}

#[test]
fn should_skip_post_flush_compaction_check_during_ingest_when_compaction_disabled() {
    // Arrange
    #[derive(Clone)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

    struct CapturedLogWriter(Arc<Mutex<Vec<u8>>>);

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
        type Writer = CapturedLogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CapturedLogWriter(Arc::clone(&self.0))
        }
    }

    impl std::io::Write for CapturedLogWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let mut event_loop = create_test_local_event_loop().expect("create local event loop");
    event_loop.state.set_compaction_enabled(false);
    event_loop
        .state
        .ingest_active
        .store(true, std::sync::atomic::Ordering::SeqCst);
    event_loop.state.manifest.files.extend(
        (1..=4).map(|seq| test_manifest_l0_file_meta(&format!("ingest-disabled-{seq}.sst"), seq)),
    );

    let captured_logs = CapturedLogs(Arc::new(Mutex::new(Vec::new())));
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_writer(captured_logs.clone())
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        event_loop.schedule_compaction_after_flush_publication("ingest-disabled-4.sst");
    });

    let logs = String::from_utf8(
        captured_logs
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
    )
    .expect("captured logs should be utf8");
    // Act
    // Assert
    assert!(
        !logs.contains("no_compaction_during_ingest"),
        "disabled post-flush scheduling should not enter the ingest invariant path: {logs}"
    );
    assert!(
        !logs.contains("BUG: compaction scheduling attempted while ingest mode is active"),
        "disabled post-flush scheduling should stay quiet during ingest teardown: {logs}"
    );
    assert_eq!(
        event_loop
            .state
            .active_compactions
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "disabled post-flush scheduling should not start compaction during ingest"
    );
}

#[test]
fn should_apply_l0_compaction_trigger_from_runtime_config() {
    // Arrange
    let mut event_loop = create_test_local_event_loop().expect("create local event loop");
    let request_id = 99;
    let response_rx = event_loop.router.register(request_id, "TestRequest");
    let (_tx, msg_rx) = crossbeam::channel::unbounded();

    event_loop.handle_runtime_msg(
        RuntimeMsg::SetRuntimeConfig {
            request_id,
            memtable_size_limit: None,
            memtable_flush_threshold: None,
            enable_compaction: None,
            l0_compaction_trigger: Some(2),
            wal_durability_policy: None,
            wal_batch_config: None,
        },
        &msg_rx,
    );

    // Act
    // Assert
    assert!(matches!(
        response_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("runtime config response"),
        RuntimeResponse::Ok { .. }
    ));
    assert_eq!(
        event_loop.compaction_actor.l0_file_count_threshold(),
        2,
        "runtime config should update the compaction actor L0 file-count trigger"
    );
    assert_eq!(
        event_loop.state.l0_compaction_trigger, 2,
        "runtime config should update the admission ceiling from the same trigger"
    );
}

#[test]
fn should_reject_runtime_config_atomically_given_invalid_memtable_candidate() {
    // Arrange
    let mut event_loop = create_test_local_event_loop().expect("create local event loop");
    let request_id = 100;
    let response_rx = event_loop.router.register(request_id, "TestRequest");
    let (_tx, msg_rx) = crossbeam::channel::unbounded();
    let original_size = event_loop.state.memtable_size_limit;
    let original_threshold = event_loop.state.memtable_flush_threshold;
    let original_compaction = event_loop.state.compaction_enabled();
    let original_trigger = event_loop.compaction_actor.l0_file_count_threshold();
    let original_wal_policy = event_loop.wal_actor.durability_policy();
    let original_batch_config = event_loop.wal_actor.batch_config();

    // Act
    event_loop.handle_runtime_msg(
        RuntimeMsg::SetRuntimeConfig {
            request_id,
            memtable_size_limit: Some(1024),
            memtable_flush_threshold: Some(0),
            enable_compaction: Some(!original_compaction),
            l0_compaction_trigger: Some(original_trigger.saturating_add(1)),
            wal_durability_policy: Some(crate::wal::DurabilityPolicy::Strict),
            wal_batch_config: Some(crate::wal::policy::BatchConfig {
                max_delay_ms: 1,
                max_bytes: 1,
            }),
        },
        &msg_rx,
    );

    // Assert
    assert!(matches!(
        response_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("runtime config response"),
        RuntimeResponse::Error {
            error: crate::common::MidgeError::InvalidArgument(_),
            ..
        }
    ));
    assert_eq!(event_loop.state.memtable_size_limit, original_size);
    assert_eq!(
        event_loop.state.memtable_flush_threshold,
        original_threshold
    );
    assert_eq!(event_loop.state.compaction_enabled(), original_compaction);
    assert_eq!(
        event_loop.compaction_actor.l0_file_count_threshold(),
        original_trigger
    );
    assert_eq!(
        event_loop.wal_actor.durability_policy(),
        original_wal_policy
    );
    assert_eq!(
        event_loop.wal_actor.batch_config().max_delay_ms,
        original_batch_config.max_delay_ms
    );
    assert_eq!(
        event_loop.wal_actor.batch_config().max_bytes,
        original_batch_config.max_bytes
    );
}

#[test]
fn should_mark_persistence_anomaly_when_compaction_metadata_range_is_missing() {
    // Arrange
    let mut event_loop = create_test_local_event_loop().expect("create local event loop");
    event_loop.state.set_compaction_enabled(true);
    event_loop
        .state
        .manifest
        .files
        .push(crate::metadata::FileMeta {
            name: crate::sst::file_name(0, 0, 1),
            level: 0,
            cf_id: 0,
            smallest_key: None,
            largest_key: None,
            ..Default::default()
        });

    // Act
    let error = event_loop
        .schedule_one_background_compaction_if_needed("metadata regression")
        .expect_err("unknown overlap range must stop compaction planning");

    // Assert
    assert!(matches!(error, crate::common::MidgeError::Corruption(_)));
    assert!(event_loop.state.persistence_anomaly_detected());
}

#[test]
fn should_defer_layout_completion_until_verification_barrier_releases() {
    // Arrange
    let mut event_loop = create_test_event_loop().expect("create memory event loop");
    let barrier_request_id = 100;
    let barrier_rx = event_loop
        .router
        .register(barrier_request_id, "TestRequest");
    event_loop.begin_storage_verification(barrier_request_id);
    assert!(matches!(
        barrier_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("barrier response"),
        RuntimeResponse::StorageVerificationBarrier { token: 100, .. }
    ));

    let mutation_request_id = 101;
    let mutation_rx = event_loop
        .router
        .register(mutation_request_id, "TestRequest");
    let completion = RuntimeMsg::CompactionComplete {
        request_id: 102,
        input_ssts: vec!["input.sst".to_string()],
        output_ssts: vec!["output.sst".to_string()],
        cf_id: 0,
        target_level: 1,
        succeeded: true,
    };

    // Act
    let blocked_mutation =
        event_loop.gate_message_for_storage_verification(RuntimeMsg::ManifestPersist {
            request_id: mutation_request_id,
        });
    let deferred_completion = event_loop.gate_message_for_storage_verification(completion);
    let deferred_gc = event_loop.gate_message_for_storage_verification(RuntimeMsg::RetryGc);
    let duplicate_gc = event_loop.gate_message_for_storage_verification(RuntimeMsg::RetryGc);

    let release_request_id = 103;
    let release_rx = event_loop
        .router
        .register(release_request_id, "TestRequest");
    event_loop.end_storage_verification(release_request_id, barrier_request_id);

    // Assert
    assert!(blocked_mutation.is_none());
    assert!(matches!(
        mutation_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("blocked mutation response"),
        RuntimeResponse::Error {
            error: crate::common::MidgeError::Busy(_),
            ..
        }
    ));
    assert!(deferred_completion.is_none());
    assert!(deferred_gc.is_none());
    assert!(duplicate_gc.is_none());
    assert!(matches!(
        release_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("barrier release response"),
        RuntimeResponse::Ok { .. }
    ));
    assert!(matches!(
        event_loop.pending_msg,
        Some(RuntimeMsg::CompactionComplete {
            request_id: 102,
            ..
        })
    ));
    assert!(matches!(
        event_loop.verification_barrier.deferred_messages.front(),
        Some(RuntimeMsg::RetryGc)
    ));
    assert_eq!(
        event_loop.verification_barrier.deferred_messages.len(),
        1,
        "duplicate maintenance retries must be coalesced while verification is active"
    );
}

#[test]
fn should_reject_storage_verification_while_flush_publication_is_active() {
    // Arrange
    let mut event_loop = create_test_local_event_loop().expect("create local event loop");
    event_loop.publication_gate.active = true;
    let request_id = 104;
    let response_rx = event_loop.router.register(request_id, "TestRequest");

    // Act
    event_loop.begin_storage_verification(request_id);

    // Assert
    assert!(matches!(
        response_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("verification response"),
        RuntimeResponse::Error {
            error: crate::common::MidgeError::Busy(_),
            ..
        }
    ));
    assert!(event_loop.verification_barrier.token.is_none());
}

#[test]
fn should_block_for_messages_when_manifest_retry_is_due_during_verification() {
    // Arrange: maintenance has reached its retry time while online storage
    // verification deliberately freezes publication progress.
    let mut event_loop = create_test_local_event_loop().expect("create local event loop");
    event_loop
        .gc_actor
        .queue_manifest_reclamation(["retained-during-verification.sst".to_string()]);
    event_loop.gc_actor.defer_manifest_reclamation_retry();
    std::thread::sleep(Duration::from_millis(15));
    assert!(event_loop.gc_actor.manifest_reclamation_retry_due());
    assert!(event_loop.verification_barrier.activate(105));

    // Act
    let idle_timeout = event_loop.idle_progress_timeout();

    // Assert: the run loop uses blocking receive for `None`. Returning a due
    // zero-duration maintenance timeout here would spin until verification
    // released the barrier.
    assert_eq!(idle_timeout, None);
}

#[test]
fn should_not_spin_auto_flush_drain_in_memory_mode() {
    // Arrange
    let mut event_loop = create_test_event_loop().expect("create memory event loop");
    event_loop.state.memtable_flush_threshold = 1024;
    event_loop.state.memtable_size_limit = 1024 * 1024;

    {
        let cf = event_loop.state.get_cf(0).expect("default cf");
        cf.memtable
            .put_with_seq(b"memory-key".to_vec(), vec![0xA5; 2048], 1, None)
            .expect("seed memory memtable");
    }
    let expected_size = event_loop
        .state
        .get_cf(0)
        .expect("default cf")
        .memtable
        .size_bytes();
    event_loop.state.total_memtable_bytes = expected_size;

    let flushed = event_loop.drain_auto_flush_memtables();

    // Act
    // Assert
    assert_eq!(flushed, 0, "memory mode should not loop on no-op flushes");
    assert_eq!(event_loop.state.manifest.files.len(), 0);
    assert_eq!(
        event_loop
            .state
            .get_cf(0)
            .expect("default cf")
            .memtable
            .size_bytes(),
        expected_size,
        "memory mode should retain the active memtable contents"
    );
}

#[test]
fn should_drain_all_current_flush_candidates_in_single_auto_flush_pass() {
    // Arrange
    let mut event_loop = create_test_local_event_loop().expect("create local event loop");
    event_loop.state.memtable_flush_threshold = 1024;
    event_loop.state.memtable_size_limit = 1024 * 1024;

    let second_cf_id = event_loop
        .state
        .create_cf("second".to_string())
        .expect("create second cf");

    {
        let cf = event_loop.state.get_cf(0).expect("default cf");
        cf.memtable
            .put_with_seq(b"default-key".to_vec(), vec![0xA5; 2048], 1, None)
            .expect("seed default cf");
    }
    {
        let cf = event_loop.state.get_cf(second_cf_id).expect("second cf");
        cf.memtable
            .put_with_seq(b"second-key".to_vec(), vec![0x5A; 2048], 2, None)
            .expect("seed second cf");
    }
    event_loop.state.total_memtable_bytes = event_loop
        .state
        .column_families
        .values()
        .map(|cf| cf.memtable.size_bytes())
        .sum();

    let flushed = event_loop.drain_auto_flush_memtables();

    // Act
    // Assert
    assert_eq!(
        flushed, 2,
        "one successful auto-flush trigger should drain every currently eligible CF"
    );
    assert_eq!(
        event_loop.state.manifest.files.len(),
        2,
        "both flushable CFs should publish SSTs without waiting for another event"
    );
    assert!(event_loop
        .state
        .manifest
        .files
        .iter()
        .any(|file| file.cf_id == 0));
    assert!(event_loop
        .state
        .manifest
        .files
        .iter()
        .any(|file| file.cf_id == second_cf_id));
}

#[test]
fn should_publish_all_flushable_cfs_given_local_write_burst_without_further_writes() {
    // Arrange
    let mut event_loop = create_test_local_event_loop().expect("create local event loop");
    event_loop.state.memtable_flush_threshold = 2048;
    event_loop.state.memtable_size_limit = 1024 * 1024;
    let second_cf_id = event_loop
        .state
        .create_cf("burst-second".to_string())
        .expect("create second cf");

    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let payload = vec![0xA5; 1536];
    let burst = vec![
        RuntimeMsg::WalAppend {
            request_id: 1,
            cf_id: 0,
            key: b"default-1".to_vec(),
            value: Some(payload.clone()),
            ttl_seconds: None,
            insert_only: false,
        },
        RuntimeMsg::WalAppend {
            request_id: 2,
            cf_id: second_cf_id,
            key: b"second-1".to_vec(),
            value: Some(payload.clone()),
            ttl_seconds: None,
            insert_only: false,
        },
        RuntimeMsg::WalAppend {
            request_id: 3,
            cf_id: 0,
            key: b"default-2".to_vec(),
            value: Some(payload.clone()),
            ttl_seconds: None,
            insert_only: false,
        },
        RuntimeMsg::WalAppend {
            request_id: 4,
            cf_id: second_cf_id,
            key: b"second-2".to_vec(),
            value: Some(payload),
            ttl_seconds: None,
            insert_only: false,
        },
    ];

    let mut burst_iter = burst.into_iter();
    let first = burst_iter.next().expect("first burst write");
    for msg in burst_iter {
        msg_tx.send(msg).expect("enqueue burst write");
    }

    let outcome = event_loop.process_wake_msg(first, &msg_rx, 16);
    drop(msg_tx);

    // Act
    // Assert
    assert_eq!(outcome, HandleOutcome::Continue);
    assert_eq!(
            event_loop.state.manifest.files.len(),
            2,
            "local write burst should publish SSTs for every CF that became flushable without requiring another write"
        );
    assert!(event_loop
        .state
        .manifest
        .files
        .iter()
        .any(|file| file.cf_id == 0));
    assert!(event_loop
        .state
        .manifest
        .files
        .iter()
        .any(|file| file.cf_id == second_cf_id));
}

#[test]
fn should_assign_compaction_output_sequence_when_plan_has_zero() {
    // Arrange
    let mut event_loop = create_test_event_loop().expect("create event loop");
    event_loop.state.sequence = 41;
    let plan = crate::compaction::CompactionPlan::new(3, 0, 1);

    let assigned = event_loop
        .assign_compaction_output_sequence(plan)
        .expect("assign output sequence");

    // Act
    // Assert
    assert_eq!(assigned.output_seq, 42);
    assert_eq!(
        event_loop.state.sequence, 42,
        "assigning a compaction output sequence must consume one global sequence"
    );
}

#[test]
fn should_preserve_existing_compaction_output_sequence() {
    // Arrange
    let mut event_loop = create_test_event_loop().expect("create event loop");
    event_loop.state.sequence = 41;
    let plan = crate::compaction::CompactionPlan::new(3, 0, 1).with_output_seq(99);

    let assigned = event_loop
        .assign_compaction_output_sequence(plan)
        .expect("preserve output sequence");

    // Act
    // Assert
    assert_eq!(assigned.output_seq, 99);
    assert_eq!(
        event_loop.state.sequence, 41,
        "preassigned compaction output sequences must not consume another sequence"
    );
}

#[test]
fn should_advance_compaction_output_sequence_past_recovered_manifest_name() {
    // Arrange
    let mut event_loop = create_test_event_loop().expect("create event loop");
    event_loop.state.sequence = 408;
    event_loop
        .state
        .manifest
        .files
        .push(crate::metadata::FileMeta {
            name: crate::sst::compaction_file_name(0, 1, 409, 0),
            level: 1,
            cf_id: 0,
            ..Default::default()
        });
    event_loop.state.restore_sequence_floor_from_manifest();
    assert_eq!(event_loop.state.manifest.last_persisted_sequence, 0);
    let plan = crate::compaction::CompactionPlan::new(0, 0, 1);

    // Act
    let assigned = event_loop
        .assign_compaction_output_sequence(plan)
        .expect("assign collision-free output sequence");

    // Assert
    assert_eq!(assigned.output_seq, 410);
    assert_eq!(event_loop.state.sequence, 410);
}

mod compaction_scheduling {
    use super::*;

    fn manifest_file_from_runtime(file: crate::runtime::FileMeta) -> crate::metadata::FileMeta {
        crate::metadata::FileMeta {
            name: file.name,
            level: file.level,
            size_bytes: file.size_bytes,
            content_crc32c: file.content_crc32c,
            cf_id: file.cf_id,
            smallest_key: file.smallest_key,
            largest_key: file.largest_key,
            smallest_seq: file.smallest_seq,
            largest_seq: file.largest_seq,
            ..Default::default()
        }
    }

    #[test]
    fn should_assign_launch_metadata_at_launch_boundary() {
        // Arrange
        let mut event_loop = create_test_event_loop().expect("create event loop");
        event_loop.state.sequence = 41;
        // Act
        // Assert
        assert!(event_loop.state.register_snapshot(100, 37, Vec::new()));
        let plan = crate::compaction::CompactionPlan::new(3, 0, 1).with_snapshot_horizon(Some(99));

        let prepared = event_loop
            .prepare_compaction_plan_for_launch(plan)
            .expect("prepare compaction plan");

        assert_eq!(prepared.output_seq, 42);
        assert_eq!(prepared.snapshot_horizon, Some(37));
        assert_eq!(
            event_loop.state.sequence, 42,
            "compaction launch must consume one global sequence for raw plans"
        );
    }

    #[test]
    fn should_preserve_preassigned_identity_at_launch_boundary() {
        // Arrange
        let mut event_loop = create_test_event_loop().expect("create event loop");
        event_loop.state.sequence = 41;
        let plan = crate::compaction::CompactionPlan::new(3, 0, 1).with_output_seq(99);

        let prepared = event_loop
            .prepare_compaction_plan_for_launch(plan)
            .expect("prepare compaction plan");

        // Act
        // Assert
        assert_eq!(prepared.output_seq, 99);
        assert_eq!(
            event_loop.state.sequence, 41,
            "compaction launch must not consume a sequence for preassigned plans"
        );
    }

    #[test]
    fn should_assign_output_sequence_when_compaction_runs_after_end_ingest() {
        // Arrange
        let mut event_loop = create_test_local_event_loop().expect("create local event loop");
        let (worker_tx, worker_rx) = crossbeam::channel::unbounded();
        event_loop.worker_msg_tx = Some(worker_tx);
        event_loop.state.set_compaction_enabled(true);
        event_loop.state.sequence = 100;
        event_loop
            .state
            .ingest_active
            .store(true, std::sync::atomic::Ordering::SeqCst);

        for seq in 1..=4 {
            let name = crate::sst::file_name(0, 0, seq);
            let file = write_runtime_l0_sst_for_test(&event_loop, &name, seq);
            event_loop
                .state
                .manifest
                .files
                .push(manifest_file_from_runtime(file));
        }

        event_loop.handle_end_ingest(77);

        let msg = worker_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("EndIngest-triggered compaction should complete");
        match msg {
            RuntimeMsg::CompactionComplete { output_ssts, .. } => {
                // Act
                // Assert
                assert!(
                    !output_ssts.is_empty(),
                    "EndIngest-triggered compaction should produce an output SST"
                );
                assert!(
                    output_ssts
                        .iter()
                        .all(|name| !name.ends_with("00000000000000000000.sst")),
                    "EndIngest-triggered compaction must not use sequence zero: {output_ssts:?}"
                );
            }
            other => panic!("unexpected worker message: {other:?}"),
        }
    }

    #[test]
    fn should_launch_compactions_only_through_event_loop_helper() {
        // Arrange
        let event_loop_dir =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/runtime/event_loop");
        let pattern = format!(".run_{}(", "compaction");
        let mut call_sites = Vec::new();

        for entry in std::fs::read_dir(&event_loop_dir).expect("read event_loop dir") {
            let entry = entry.expect("read event_loop entry");
            let path = entry.path();
            if path.extension().and_then(std::ffi::OsStr::to_str) != Some("rs") {
                continue;
            }

            let source = std::fs::read_to_string(&path).expect("read event_loop source file");
            let relative = path
                .strip_prefix(env!("CARGO_MANIFEST_DIR"))
                .expect("strip source prefix")
                .display()
                .to_string()
                .replace('\\', "/");
            for (line_idx, line) in source.lines().enumerate() {
                if line.contains(&pattern) {
                    call_sites.push(format!("{relative}:{}:{}", line_idx + 1, line.trim()));
                }
            }
        }

        // Act
        // Assert
        assert_eq!(
            call_sites.len(),
            1,
            "event-loop compaction actor launches must stay centralized: {call_sites:?}"
        );
        assert!(
            call_sites[0].starts_with("src/runtime/event_loop/mod.rs:"),
            "central compaction actor launch must live in event_loop/mod.rs: {call_sites:?}"
        );
    }
}

// =========== Trace Flag Tests ===========

#[test]
fn should_respect_trace_enabled_flag() {
    // Arrange
    let state1 = create_test_state();
    let state2 = create_test_state();
    let router1 = Arc::new(ResponseRouter::new());
    let router2 = Arc::new(ResponseRouter::new());

    // Act
    let event_loop1 = EventLoop::new(
        state1,
        false,
        router1,
        crate::runtime::RuntimeConfig::default(),
        None,
    )
    .expect("Should create");
    let event_loop2 = EventLoop::new(
        state2,
        true,
        router2,
        crate::runtime::RuntimeConfig::default(),
        None,
    )
    .expect("Should create");

    // Assert
    assert!(!event_loop1.trace_enabled);
    assert!(event_loop2.trace_enabled);
}

// =========== Actor Initialization Tests ===========

#[test]
fn should_initialize_actors_with_expected_starting_state() {
    // Arrange

    // Act
    let event_loop = create_test_event_loop().expect("Should create event loop");

    // Assert - each actor's real, publicly observable starting state (not a
    // re-read of a field the test just set itself). ManifestActor has no such
    // accessor and is only implicitly covered here via successful construction.
    assert!(
        !event_loop.flush_actor.is_inflight(),
        "FlushActor must start with no in-flight flush"
    );
    assert_eq!(
        event_loop.wal_actor.pending_sync_count(),
        0,
        "WalActor must start with no pending syncs"
    );
    assert_eq!(
        event_loop.wal_actor.sync_calls(),
        0,
        "WalActor must start with no recorded sync calls"
    );
    assert_eq!(
        event_loop.compaction_actor.l0_file_count_threshold(),
        4,
        "CompactionActor must start with the default L0 file-count threshold"
    );
    assert_eq!(
        event_loop.cloud_actor.uploads_in_progress(),
        0,
        "CloudActor must start with no uploads in progress"
    );
    assert!(
        event_loop.gc_actor.last_gc_run().is_none(),
        "GcActor must start with no recorded GC run"
    );
    assert!(
        event_loop.hybrid_storage.is_none(),
        "hybrid storage is optional and unset by default"
    );
}

#[test]
fn should_maintain_router_reference() {
    // Arrange
    let state = create_test_state();
    let router = Arc::new(ResponseRouter::new());

    // Act
    let event_loop = EventLoop::new(
        state,
        false,
        Arc::clone(&router),
        crate::runtime::RuntimeConfig::default(),
        None,
    )
    .expect("Should create");

    // Assert - the event loop must retain the exact same router instance the
    // caller passed in, not a copy, so responses actually reach callers
    assert!(Arc::ptr_eq(&event_loop.router, &router));
}

#[test]
fn should_sleep_until_cloud_seal_deadline_without_spinning_given_subthreshold_wal(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut event_loop = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let max_flush_delay = Duration::from_millis(250);
    let policy = crate::runtime::CloudRuntimePolicy {
        wal_seal: crate::runtime::CloudWalSealPolicy {
            min_segment_bytes: usize::MAX,
            max_pending_writes: usize::MAX,
            max_flush_delay,
        },
        ..crate::runtime::CloudRuntimePolicy::default()
    };
    event_loop.durability = crate::runtime::durability::DurabilityCoordinator::new(0, true, policy);
    let (_, deferred) = event_loop.wal_actor.append(
        &mut event_loop.state,
        crate::runtime::actors::wal::AppendParams {
            request_id: 901,
            cf_id: 0,
            key: bytes::Bytes::from_static(b"deadline-key"),
            value: Some(bytes::Bytes::from_static(b"deadline-value")),
            insert_only: false,
            ttl_seconds: None,
        },
    )?;

    // Act
    let actionable = event_loop.has_actionable_work();
    let idle_timeout = event_loop.idle_progress_timeout();

    // Assert
    assert!(deferred);
    assert!(!actionable, "sub-threshold CloudAsync WAL must not spin");
    assert!(
        idle_timeout.is_some_and(|timeout| timeout <= max_flush_delay),
        "idle wait must be bounded by the CloudAsync seal deadline"
    );
    Ok(())
}

fn cloud_recovery_wal_bytes(sequence: u64) -> Vec<u8> {
    let record = crate::wal::WalRecord::new(
        crate::wal::WalOpKind::Put,
        bytes::Bytes::from(format!("key-{sequence}")),
        Some(bytes::Bytes::from_static(b"value")),
        sequence,
        0,
    );
    let payload = crate::wal::encoding::encode(&record).expect("encode recovery WAL record");
    let mut framed = Vec::new();
    crate::wal::frame::append_frame(&mut framed, &payload).expect("frame recovery WAL record");
    framed
}

#[test]
fn should_incrementally_drain_recovered_wal_given_bounded_upload_queue_at_open(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let db_path = unique_test_db_path("midge_recovered_wal_backlog");
    let mut state = RuntimeState::new(db_path.clone(), false);
    let wal_dir = db_path.join("wal");
    std::fs::create_dir_all(&wal_dir).expect("create recovered WAL directory");
    let first_bytes = cloud_recovery_wal_bytes(1);
    let second_bytes = cloud_recovery_wal_bytes(2);
    let active_bytes = cloud_recovery_wal_bytes(3);
    std::fs::write(wal_dir.join(crate::wal::segment_file_name(1)), &first_bytes)
        .expect("write first recovered WAL");
    std::fs::write(
        wal_dir.join(crate::wal::segment_file_name(2)),
        &second_bytes,
    )
    .expect("write second recovered WAL");
    std::fs::write(wal_dir.join(crate::wal::ACTIVE_FILE_NAME), &active_bytes)
        .expect("write recovered active WAL");

    state.sequence = 3;
    state.wal.current_segment_id = 3;
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(db_path.join("hybrid-local"))
            .expect("create local storage"),
    );
    let cloud = Arc::new(
        crate::storage::filesystem::FileSystem::new(db_path.join("cloud-store"))
            .expect("create cloud storage"),
    );
    let max_segment_bytes = first_bytes
        .len()
        .max(second_bytes.len())
        .max(active_bytes.len()) as u64;
    let storage = Arc::new(crate::storage::HybridStorage::with_test_upload_limits(
        local,
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        1,
        max_segment_bytes,
    ));
    storage.fence_cloud_wal_catalog(1)?;
    let config = crate::runtime::RuntimeConfig {
        wal_durability_policy: crate::wal::DurabilityPolicy::CloudAsync,
        hybrid_storage: Some(Arc::clone(&storage)),
        recovered_local_wal_segments: std::collections::BTreeMap::from([(1, 1), (2, 2)]),
        recovered_cloud_active_wal: Some(crate::runtime::RecoveredCloudActiveWal {
            max_sequence: 3,
            writer_epoch: 0,
            record_count: 1,
            valid_bytes: active_bytes.len(),
        }),
        writer_epoch: 1,
        ..crate::runtime::RuntimeConfig::default()
    };

    // Act
    let router = Arc::new(ResponseRouter::new());
    let mut event_loop = EventLoop::new(state, false, router, config, None)?;
    let backlog_after_open = event_loop.cloud_wal.upload_backlog.len();
    let queued_after_open = storage.pending_upload_count();
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline
        && (!event_loop.cloud_wal.upload_backlog.is_empty() || storage.pending_upload_count() > 0)
    {
        event_loop.tick_hybrid_storage();
        event_loop.drain_cloud_wal_upload_backlog();
        std::thread::sleep(Duration::from_millis(5));
    }

    // Assert
    assert_eq!(
        queued_after_open, 1,
        "startup should fill only one queue slot"
    );
    assert_eq!(
        backlog_after_open, 2,
        "remaining sealed and active WAL obligations must stay recoverable"
    );
    assert!(event_loop.cloud_wal.upload_backlog.is_empty());
    assert_eq!(storage.pending_upload_count(), 0);
    assert_eq!(event_loop.state.wal.cloud_durable_seq, 3);
    Ok(())
}

#[test]
fn should_remove_validated_local_copy_when_remote_recovery_segment_is_initially_durable(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let db_path = unique_test_db_path("midge_remote_wal_local_cleanup");
    let mut state = RuntimeState::new(db_path.clone(), false);
    let wal_dir = db_path.join("wal");
    std::fs::create_dir_all(&wal_dir).expect("create WAL directory");
    let local_path = wal_dir.join(crate::wal::segment_file_name(1));
    std::fs::write(&local_path, cloud_recovery_wal_bytes(1)).expect("write validated local copy");
    state.sequence = 1;
    state.wal.current_segment_id = 2;
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(db_path.join("hybrid-local"))
            .expect("create local storage"),
    );
    let cloud = Arc::new(
        crate::storage::filesystem::FileSystem::new(db_path.join("cloud-store"))
            .expect("create cloud storage"),
    );
    let config = crate::runtime::RuntimeConfig {
        wal_durability_policy: crate::wal::DurabilityPolicy::CloudAsync,
        hybrid_storage: Some(Arc::new(crate::storage::HybridStorage::with_policy(
            local,
            cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        ))),
        recovered_cloud_wal_segments: std::collections::BTreeMap::from([(1, 1)]),
        ..crate::runtime::RuntimeConfig::default()
    };

    // Act
    let event_loop = EventLoop::new(state, false, Arc::new(ResponseRouter::new()), config, None)?;

    // Assert
    assert!(!local_path.exists());
    assert_eq!(event_loop.state.wal.cloud_durable_seq, 1);
    Ok(())
}

#[test]
fn should_defer_column_family_drop_until_compaction_publication_finishes() {
    // Arrange
    let mut event_loop = create_test_local_event_loop().expect("create local event loop");
    let cf_id = event_loop
        .manifest_actor
        .create_column_family(&mut event_loop.state, "compacting")
        .expect("create compacting column family");
    let input_name = crate::sst::file_name(cf_id, 0, 1);
    let output_name = crate::sst::file_name(cf_id, 1, 2);
    let input_bytes = valid_sst_bytes_for_event_loop_test(b"key", b"old", 1);
    let output_bytes = valid_sst_bytes_for_event_loop_test(b"key", b"new", 2);
    std::fs::write(event_loop.state.sst_dir.join(&input_name), &input_bytes)
        .expect("write compaction input");
    event_loop
        .state
        .manifest
        .files
        .push(crate::metadata::FileMeta {
            name: input_name.clone(),
            level: 0,
            size_bytes: u64::try_from(input_bytes.len()).expect("input length"),
            content_crc32c: Some(crc32c::crc32c(&input_bytes)),
            cf_id,
            smallest_key: Some(b"key".to_vec()),
            largest_key: Some(b"key".to_vec()),
            smallest_seq: Some(1),
            largest_seq: Some(1),
            ..Default::default()
        });
    event_loop
        .compaction_actor
        .prepare_for_completion_test(&mut event_loop.state, std::slice::from_ref(&input_name))
        .expect("prepare active compaction fixture");
    let request_id = 8_155;
    let response_rx = event_loop.router.register(request_id, "TestRequest");
    let (_msg_tx, msg_rx) = crossbeam::channel::unbounded();

    // Act: the DDL arrives while the worker still owns its input set.
    let outcome = event_loop.handle_runtime_msg(
        RuntimeMsg::ManifestDropColumnFamily {
            request_id,
            cf_id,
            discard_unflushed: true,
        },
        &msg_rx,
    );

    // Assert: publication must win the authority race before drop snapshots files.
    assert_eq!(outcome, HandleOutcome::Continue);
    assert!(response_rx.try_recv().is_err());
    assert_eq!(event_loop.publication_gate.deferred_messages.len(), 1);
    assert!(event_loop.state.get_cf(cf_id).is_some());

    // Arrange: simulate the serialized compaction authority switch.
    std::fs::write(event_loop.state.sst_dir.join(&output_name), &output_bytes)
        .expect("write compaction output");
    event_loop
        .state
        .manifest
        .files
        .retain(|file| file.name != input_name);
    event_loop
        .state
        .manifest
        .files
        .push(crate::metadata::FileMeta {
            name: output_name.clone(),
            level: 1,
            size_bytes: u64::try_from(output_bytes.len()).expect("output length"),
            content_crc32c: Some(crc32c::crc32c(&output_bytes)),
            cf_id,
            smallest_key: Some(b"key".to_vec()),
            largest_key: Some(b"key".to_vec()),
            smallest_seq: Some(2),
            largest_seq: Some(2),
            ..Default::default()
        });
    event_loop.compaction_actor.handle_complete(
        &mut event_loop.state,
        std::slice::from_ref(&input_name),
        std::slice::from_ref(&output_name),
    );

    // Act: once publication is complete, process the deferred drop.
    event_loop.restore_publication_deferred_message();
    let deferred = event_loop
        .pending_msg
        .take()
        .expect("restored deferred drop");
    event_loop.handle_runtime_msg(deferred, &msg_rx);

    // Assert
    assert!(matches!(
        response_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("drop response"),
        RuntimeResponse::Ok { request_id: 8_155 }
    ));
    assert!(event_loop.state.get_cf(cf_id).is_none());
    assert!(event_loop
        .state
        .manifest
        .files
        .iter()
        .all(|file| file.cf_id != cf_id));
    assert!(!event_loop.state.sst_dir.join(output_name).exists());
}

#[test]
fn should_restore_deferred_column_family_drop_before_emergent_compaction_followup() {
    // Arrange
    let mut event_loop = create_test_local_event_loop().expect("create local event loop");
    let cf_id = event_loop
        .manifest_actor
        .create_column_family(&mut event_loop.state, "drop-before-followup")
        .expect("create column family");
    let current_input = crate::sst::file_name(cf_id, 0, 1);
    let current_output = crate::sst::file_name(cf_id, 1, 10);
    for (name, key, sequence) in
        std::iter::once((current_input.clone(), b'a', 1_u64)).chain((2_u8..=5).map(|index| {
            (
                crate::sst::file_name(cf_id, 0, u64::from(index)),
                b'a'.saturating_add(index),
                u64::from(index),
            )
        }))
    {
        let bytes = valid_sst_bytes_for_event_loop_test(&[key], b"input", sequence);
        std::fs::write(event_loop.state.sst_dir.join(&name), &bytes).expect("write input SST");
        event_loop
            .state
            .manifest
            .files
            .push(crate::metadata::FileMeta {
                name,
                level: 0,
                size_bytes: u64::try_from(bytes.len()).expect("input length"),
                content_crc32c: Some(crc32c::crc32c(&bytes)),
                cf_id,
                smallest_key: Some(vec![key]),
                largest_key: Some(vec![key]),
                smallest_seq: Some(sequence),
                largest_seq: Some(sequence),
                ..Default::default()
            });
    }
    let output_bytes = valid_sst_bytes_for_event_loop_test(b"a", b"output", 10);
    std::fs::write(
        event_loop.state.sst_dir.join(&current_output),
        &output_bytes,
    )
    .expect("write completed output");
    event_loop
        .compaction_actor
        .prepare_for_completion_test(&mut event_loop.state, std::slice::from_ref(&current_input))
        .expect("prepare active compaction fixture");
    let drop_request_id = 8_157;
    let drop_response = event_loop.router.register(drop_request_id, "TestRequest");
    let (_msg_tx, msg_rx) = crossbeam::channel::unbounded();
    event_loop.handle_runtime_msg(
        RuntimeMsg::ManifestDropColumnFamily {
            request_id: drop_request_id,
            cf_id,
            discard_unflushed: false,
        },
        &msg_rx,
    );
    assert_eq!(event_loop.publication_gate.deferred_messages.len(), 1);

    // Act
    event_loop.handle_runtime_msg(
        RuntimeMsg::CompactionComplete {
            request_id: 8_158,
            input_ssts: vec![current_input],
            output_ssts: vec![current_output],
            cf_id,
            target_level: 1,
            succeeded: true,
        },
        &msg_rx,
    );

    // Assert: four L0 files still qualify, but the waiting drop owns the next
    // serialized publication turn instead of another compaction worker.
    assert_eq!(
        event_loop
            .state
            .active_compactions
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(event_loop.publication_gate.deferred_messages.len(), 1);
    event_loop.restore_publication_deferred_message();
    let deferred = event_loop
        .pending_msg
        .take()
        .expect("drop should be restored before compaction followup");
    event_loop.handle_runtime_msg(deferred, &msg_rx);
    assert!(matches!(
        drop_response
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("drop response"),
        RuntimeResponse::Ok { .. }
    ));
    assert!(event_loop.state.get_cf(cf_id).is_none());
}

#[test]
fn should_reject_late_compaction_output_after_column_family_is_dropped() {
    // Arrange
    let mut event_loop = create_test_local_event_loop().expect("create local event loop");
    let cf_id = event_loop
        .manifest_actor
        .create_column_family(&mut event_loop.state, "already-dropped")
        .expect("create column family");
    event_loop
        .manifest_actor
        .drop_column_family(&mut event_loop.state, cf_id)
        .expect("drop column family");

    let input_name = crate::sst::file_name(cf_id, 0, 1);
    let output_name = crate::sst::file_name(cf_id, 1, 2);
    let output_bytes = valid_sst_bytes_for_event_loop_test(b"key", b"value", 2);
    std::fs::write(event_loop.state.sst_dir.join(&output_name), output_bytes)
        .expect("write late compaction output");
    event_loop
        .compaction_actor
        .prepare_for_completion_test(&mut event_loop.state, std::slice::from_ref(&input_name))
        .expect("prepare active compaction fixture");
    let request_id = 8_156;
    let response_rx = event_loop.router.register(request_id, "TestRequest");
    let (_msg_tx, msg_rx) = crossbeam::channel::unbounded();

    // Act
    event_loop.handle_runtime_msg(
        RuntimeMsg::CompactionComplete {
            request_id,
            input_ssts: vec![input_name],
            output_ssts: vec![output_name.clone()],
            cf_id,
            target_level: 1,
            succeeded: true,
        },
        &msg_rx,
    );

    // Assert
    assert!(matches!(
        response_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("late completion response"),
        RuntimeResponse::Error { .. }
    ));
    assert!(!event_loop.state.sst_dir.join(&output_name).exists());
    assert!(event_loop
        .state
        .manifest
        .files
        .iter()
        .all(|file| file.name != output_name));
}

#[test]
fn should_reject_out_of_order_compaction_output_set_before_publication(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut event_loop = create_test_local_event_loop()?;
    let input_name = crate::sst::file_name(0, 0, 1);
    let input_bytes = valid_sst_bytes_for_event_loop_test(b"input", b"value", 1);
    std::fs::write(event_loop.state.sst_dir.join(&input_name), &input_bytes)?;
    event_loop
        .state
        .manifest
        .files
        .push(crate::metadata::FileMeta {
            name: input_name.clone(),
            level: 0,
            size_bytes: u64::try_from(input_bytes.len()).expect("input length fits u64"),
            content_crc32c: Some(crc32c::crc32c(&input_bytes)),
            cf_id: 0,
            smallest_key: Some(b"input".to_vec()),
            largest_key: Some(b"input".to_vec()),
            smallest_seq: Some(1),
            largest_seq: Some(1),
            ..Default::default()
        });
    let first_name = crate::sst::compaction_file_name(0, 1, 2, 0);
    let second_name = crate::sst::compaction_file_name(0, 1, 2, 1);
    let first_bytes = valid_sst_bytes_for_event_loop_test(b"a", b"first", 2);
    let second_bytes = valid_sst_bytes_for_event_loop_test(b"z", b"second", 2);
    std::fs::write(event_loop.state.sst_dir.join(&first_name), first_bytes)?;
    std::fs::write(event_loop.state.sst_dir.join(&second_name), second_bytes)?;
    event_loop
        .compaction_actor
        .prepare_for_completion_test(&mut event_loop.state, std::slice::from_ref(&input_name))?;
    let request_id = 8_159;
    let response_rx = event_loop.router.register(request_id, "TestRequest");
    let (_msg_tx, msg_rx) = crossbeam::channel::unbounded();

    // Act
    event_loop.handle_runtime_msg(
        RuntimeMsg::CompactionComplete {
            request_id,
            input_ssts: vec![input_name.clone()],
            output_ssts: vec![second_name.clone(), first_name.clone()],
            cf_id: 0,
            target_level: 1,
            succeeded: true,
        },
        &msg_rx,
    );

    // Assert
    match response_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("invalid completion response")
    {
        RuntimeResponse::Error { error, .. } => assert!(
            error.to_string().contains("sorted and uniquely named"),
            "unexpected validation error: {error}"
        ),
        other => panic!("expected rejected output set, got {other:?}"),
    }
    assert!(event_loop.state.manifest_has_file(&input_name));
    assert!(!event_loop.state.manifest_has_file(&first_name));
    assert!(!event_loop.state.manifest_has_file(&second_name));
    assert!(event_loop.state.intent_log.is_empty());
    for _ in 0..100 {
        if !event_loop.state.sst_dir.join(&first_name).exists()
            && !event_loop.state.sst_dir.join(&second_name).exists()
        {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert!(!event_loop.state.sst_dir.join(first_name).exists());
    assert!(!event_loop.state.sst_dir.join(second_name).exists());
    Ok(())
}

#[test]
fn should_reject_compaction_when_target_span_changes_before_publication(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut event_loop = create_test_local_event_loop()?;
    let source_name = crate::sst::file_name(0, 0, 1);
    let selected_target = crate::sst::file_name(0, 1, 2);
    let concurrent_target = crate::sst::file_name(0, 1, 3);
    for (name, level, key, smallest, largest, sequence) in [
        (&source_name, 0, b'm', b'a', b'z', 1_u64),
        (&selected_target, 1, b'b', b'a', b'm', 2_u64),
        (&concurrent_target, 1, b'y', b'n', b'z', 3_u64),
    ] {
        let bytes = valid_sst_bytes_for_event_loop_test(&[key], b"value", sequence);
        std::fs::write(event_loop.state.sst_dir.join(name), &bytes)?;
        event_loop
            .state
            .manifest
            .files
            .push(crate::metadata::FileMeta {
                name: name.clone(),
                level,
                size_bytes: u64::try_from(bytes.len()).expect("input length fits u64"),
                content_crc32c: Some(crc32c::crc32c(&bytes)),
                cf_id: 0,
                smallest_key: Some(vec![smallest]),
                largest_key: Some(vec![largest]),
                smallest_seq: Some(sequence),
                largest_seq: Some(sequence),
                ..Default::default()
            });
    }
    let output_name = crate::sst::compaction_file_name(0, 1, 4, 0);
    let output_bytes = valid_sst_bytes_for_event_loop_test(b"m", b"replacement", 4);
    std::fs::write(event_loop.state.sst_dir.join(&output_name), output_bytes)?;
    let captured_inputs = vec![source_name.clone(), selected_target.clone()];
    event_loop
        .compaction_actor
        .prepare_for_completion_test(&mut event_loop.state, &captured_inputs)?;
    let request_id = 8_160;
    let response_rx = event_loop.router.register(request_id, "TestRequest");
    let compact_all_request_id = 8_163;
    let compact_all_rx = event_loop
        .router
        .register(compact_all_request_id, "CompactAll");
    event_loop
        .state
        .pending_compaction_waits
        .lock()
        .insert(compact_all_request_id, "CompactAll".to_string());
    let (_msg_tx, msg_rx) = crossbeam::channel::unbounded();

    // Act
    event_loop.handle_runtime_msg(
        RuntimeMsg::CompactionComplete {
            request_id,
            input_ssts: captured_inputs,
            output_ssts: vec![output_name.clone()],
            cf_id: 0,
            target_level: 1,
            succeeded: true,
        },
        &msg_rx,
    );

    // Assert
    match response_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("changed-span completion response")
    {
        RuntimeResponse::Error { error, .. } => assert!(
            error.to_string().contains("target-level span changed"),
            "unexpected validation error: {error}"
        ),
        other => panic!("expected rejected output set, got {other:?}"),
    }
    assert!(matches!(
        compact_all_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("compact_all publication failure response"),
        RuntimeResponse::Error {
            error: crate::common::MidgeError::Fenced(message),
            ..
        } if message.contains("target-level span changed")
    ));
    assert!(event_loop.state.manifest_has_file(&source_name));
    assert!(event_loop.state.manifest_has_file(&selected_target));
    assert!(event_loop.state.manifest_has_file(&concurrent_target));
    assert!(!event_loop.state.manifest_has_file(&output_name));
    assert!(event_loop.state.intent_log.is_empty());
    Ok(())
}

#[test]
fn should_return_exact_compaction_failure_to_compact_all_waiter() -> crate::common::MidgeResult<()>
{
    // Arrange
    let mut event_loop = create_test_local_event_loop()?;
    let input_name = crate::sst::file_name(0, 0, 1);
    event_loop
        .state
        .manifest
        .files
        .push(crate::metadata::FileMeta {
            name: input_name.clone(),
            level: 0,
            cf_id: 0,
            smallest_key: Some(b"a".to_vec()),
            largest_key: Some(b"z".to_vec()),
            ..Default::default()
        });
    event_loop
        .compaction_actor
        .prepare_for_completion_test(&mut event_loop.state, std::slice::from_ref(&input_name))?;
    let compact_all_request_id = 8_161;
    let response_rx = event_loop
        .router
        .register(compact_all_request_id, "CompactAll");
    event_loop
        .state
        .pending_compaction_waits
        .lock()
        .insert(compact_all_request_id, "CompactAll".to_string());
    event_loop.compaction_actor.set_worker_error_for_test(
        crate::common::MidgeError::ResourceLimit("compaction pool exhausted".to_string()),
    );
    let (_msg_tx, msg_rx) = crossbeam::channel::unbounded();

    // Act
    event_loop.handle_runtime_msg(
        RuntimeMsg::CompactionComplete {
            request_id: 8_162,
            input_ssts: vec![input_name.clone()],
            output_ssts: Vec::new(),
            cf_id: 0,
            target_level: 1,
            succeeded: false,
        },
        &msg_rx,
    );

    // Assert
    assert!(matches!(
        response_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("compact_all failure response"),
        RuntimeResponse::Error {
            error: crate::common::MidgeError::ResourceLimit(message),
            ..
        } if message == "compaction pool exhausted"
    ));
    assert!(event_loop.state.manifest_has_file(&input_name));
    assert_eq!(
        event_loop
            .state
            .active_compactions
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    Ok(())
}

#[test]
fn should_reject_cloud_partition_read_before_exceeding_compaction_pool(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("oversized-partition.sst");
    std::fs::write(&path, vec![0u8; 128])?;
    let budget = crate::common::resource_budget::ResourceBudget::new(64);

    // Act
    let result = EventLoop::read_file_with_budget(&path, &budget);

    // Assert
    assert!(matches!(
        result,
        Err(crate::common::MidgeError::ResourceLimit(_))
    ));
    assert!(path.is_file());
    Ok(())
}

#[test]
fn should_reject_sst_checksum_buffer_before_exceeding_compaction_pool(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("checksum-input.sst");
    std::fs::write(&path, [1u8; 128])?;
    let budget = crate::common::resource_budget::ResourceBudget::new(64);

    // Act
    let result = EventLoop::checksummed_file_crc(&path, &budget);

    // Assert
    assert!(matches!(
        result,
        Err(crate::common::MidgeError::ResourceLimit(_))
    ));
    assert!(path.is_file());
    Ok(())
}

#[test]
fn should_count_late_response_given_inline_route_when_caller_already_gave_up() {
    // Arrange: an inline caller registers a response channel and then stops
    // waiting, mirroring a transaction commit that hit its response timeout.
    let event_loop = create_test_event_loop().expect("create event loop");
    let request_id = 4242;
    let (response_tx, response_rx) = crossbeam::channel::bounded(1);
    event_loop.register_inline_response(request_id, response_tx);
    drop(response_rx);

    // Act: the loop finishes the work anyway.
    event_loop.respond(request_id, RuntimeResponse::Ok { request_id });

    // Assert
    assert_eq!(
        event_loop.router.late_responses_total(),
        1,
        "a discarded inline response must still be visible to operators"
    );
}

#[test]
fn should_not_count_late_response_given_inline_route_when_caller_is_still_waiting() {
    // Arrange
    let event_loop = create_test_event_loop().expect("create event loop");
    let request_id = 4243;
    let (response_tx, response_rx) = crossbeam::channel::bounded(1);
    event_loop.register_inline_response(request_id, response_tx);

    // Act
    event_loop.respond(request_id, RuntimeResponse::Ok { request_id });

    // Assert
    assert!(matches!(
        response_rx.try_recv(),
        Ok(RuntimeResponse::Ok { .. })
    ));
    assert_eq!(event_loop.router.late_responses_total(), 0);
}
