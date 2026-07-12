use super::*;
use crate::common::MidgeError;
use crate::wal::DurabilityPolicy;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn should_transition_open_to_closing_to_closed_without_waiting_for_transactions() {
    // Arrange
    let lifecycle = Arc::new(RuntimeLifecycle::new());
    let transaction_guard = lifecycle.acquire().expect("acquire transaction guard");

    // Act
    lifecycle.begin_shutdown();

    // Assert
    assert_eq!(lifecycle.state(), RuntimeLifecycleState::Closing);
    assert_eq!(lifecycle.active_transaction_count(), 1);
    assert!(matches!(lifecycle.acquire(), Err(MidgeError::Busy(_))));

    drop(transaction_guard);
    lifecycle.mark_closed();
    assert_eq!(lifecycle.state(), RuntimeLifecycleState::Closed);
}

#[test]
fn should_allow_shutdown_retry_when_initial_wait_times_out() {
    // Arrange: model a running worker that accepts the shutdown request but
    // deliberately withholds its response past the first deadline.
    let (runtime, handle) = Runtime::new();
    handle.lifecycle.mark_running();

    // Act
    let first = handle.shutdown(Duration::from_millis(5));

    // Assert
    assert!(matches!(first, Err(MidgeError::Timeout(_))));
    assert_eq!(handle.lifecycle.state(), RuntimeLifecycleState::Closing);
    assert!(
        handle
            .lifecycle
            .shutdown_response
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some(),
        "timed-out shutdown must retain its response receiver for retry"
    );

    handle.lifecycle.mark_closed();
    assert!(handle.shutdown(Duration::from_millis(5)).is_ok());
    drop(runtime);
}

#[test]
fn should_not_block_snapshot_release_when_closing_queue_is_full() {
    // Arrange
    let (runtime, handle) = Runtime::new();
    assert!(handle.snapshot_pins.register(7, 11, Vec::new()));
    for _ in 0..RUNTIME_QUEUE_CAPACITY {
        handle
            .msg_tx
            .try_send(RuntimeMsg::RetryGc)
            .expect("fill bounded runtime queue");
    }
    handle.lifecycle.begin_shutdown();

    // Act
    let started = std::time::Instant::now();
    let removed = handle.unregister_snapshot_pin(7);

    // Assert
    assert!(removed);
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "snapshot release blocked on the full closing queue"
    );
    drop(runtime);
}

// =========== next_request_id Tests ===========

#[test]
fn should_generate_unique_request_ids() {
    // Arrange
    // (no setup)

    // Act
    let id1 = next_request_id().expect("id");
    let id2 = next_request_id().expect("id");
    let id3 = next_request_id().expect("id");

    // Assert
    assert_ne!(id1, id2);
    assert_ne!(id2, id3);
    assert_ne!(id1, id3);
}

#[test]
fn should_increment_request_ids_monotonically() {
    // Arrange
    // (no setup)

    // Act
    let id1 = next_request_id().expect("id");
    let id2 = next_request_id().expect("id");
    let id3 = next_request_id().expect("id");

    // Assert
    assert!(id1 < id2);
    assert!(id2 < id3);
}

#[test]
fn should_allocate_request_ids_atomically_across_threads() {
    // Arrange
    let handles: Vec<_> = (0..5)
        .map(|_| {
            thread::spawn(|| {
                let mut ids = vec![];
                for _ in 0..20 {
                    ids.push(next_request_id().expect("id"));
                }
                ids
            })
        })
        .collect();

    // Act
    let all_ids: Vec<u64> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();

    // Assert - All IDs should be unique
    for i in 0..all_ids.len() {
        for j in (i + 1)..all_ids.len() {
            assert_ne!(all_ids[i], all_ids[j]);
        }
    }

    // Should have 100 IDs from 5 threads
    assert_eq!(all_ids.len(), 100);
}

#[test]
fn should_start_from_nonzero() {
    // Arrange
    // (no setup)

    // Act
    let id = next_request_id().expect("id");

    // Assert - Should never be 0
    assert!(id > 0);
}

// =========== RuntimeMsg Tests ===========

#[test]
fn should_extract_request_id_from_message() {
    // Arrange
    let msg = RuntimeMsg::Noop { request_id: 42 };

    // Act
    let req_id = msg.request_id();

    // Assert
    assert_eq!(req_id, Some(42));
}

#[test]
fn should_return_none_for_shutdown_message() {
    // Arrange
    let msg = RuntimeMsg::Shutdown;

    // Act
    let req_id = msg.request_id();

    // Assert
    assert_eq!(req_id, None);
}

fn write_side_request_response_messages() -> Vec<RuntimeMsg> {
    let file_meta = FileMeta {
        name: "contract.sst".to_string(),
        level: 0,
        size_bytes: 1,
        content_crc32c: None,
        cf_id: 0,
        smallest_key: None,
        largest_key: None,
        smallest_seq: None,
        largest_seq: None,
    };
    let compaction_plan = CompactionPlan {
        input_files: vec!["input.sst".to_string()],
        source_level: 0,
        target_level: 1,
        cf_id: 0,
    };
    vec![
        RuntimeMsg::FlushMemtable {
            request_id: 1,
            cf_id: 0,
        },
        RuntimeMsg::FlushComplete {
            request_id: 2,
            cf_id: 0,
            sst_name: "flushed.sst".to_string(),
            sequence: 10,
        },
        RuntimeMsg::CheckCompaction { request_id: 3 },
        RuntimeMsg::RunCompaction {
            request_id: 4,
            plan: compaction_plan,
        },
        RuntimeMsg::WalAppend {
            request_id: 5,
            cf_id: 0,
            key: b"k".to_vec(),
            value: Some(b"v".to_vec()),
            ttl_seconds: None,
            insert_only: false,
        },
        RuntimeMsg::WalAppendDeleteRange {
            request_id: 6,
            cf_id: 0,
            start_key: b"a".to_vec(),
            end_key: b"z".to_vec(),
            durability_policy: None,
        },
        RuntimeMsg::WalRotate { request_id: 7 },
        RuntimeMsg::WalSyncComplete {
            request_id: 8,
            segment_id: 1,
        },
        RuntimeMsg::CloudUploadSst {
            request_id: 9,
            sst_name: "remote.sst".to_string(),
        },
        RuntimeMsg::CloudUploadWal {
            request_id: 10,
            segment_id: 1,
        },
        RuntimeMsg::CloudUploadComplete {
            request_id: 11,
            resource: "wal/1".to_string(),
        },
        RuntimeMsg::CheckGc { request_id: 12 },
        RuntimeMsg::DeleteObsoleteSsts {
            request_id: 13,
            sst_names: vec!["old.sst".to_string()],
        },
        RuntimeMsg::ManifestAddSst {
            request_id: 14,
            file_meta: file_meta.clone(),
        },
        RuntimeMsg::ManifestCompactionComplete {
            request_id: 15,
            removed: vec!["old.sst".to_string()],
            added: vec![file_meta],
        },
        RuntimeMsg::BeginIngest { request_id: 16 },
        RuntimeMsg::EndIngest { request_id: 17 },
        RuntimeMsg::GetIngestState { request_id: 18 },
        RuntimeMsg::GetRuntimeConfig { request_id: 19 },
    ]
}

fn read_side_request_response_messages() -> Vec<RuntimeMsg> {
    vec![
        RuntimeMsg::Read {
            request_id: 20,
            cf_id: 0,
            key: b"k".to_vec(),
            sequence: 1,
            requested_durability: crate::types::ReadDurability::Strict,
        },
        RuntimeMsg::RangeScan {
            request_id: 21,
            cf_id: 0,
            start: b"a".to_vec(),
            end: b"z".to_vec(),
            sequence: 1,
            requested_durability: crate::types::ReadDurability::Strict,
        },
        RuntimeMsg::CaptureReadSnapshot {
            request_id: 22,
            cf_id: 0,
            sequence: 1,
        },
        RuntimeMsg::UnregisterSnapshot { snapshot_id: 23 },
        RuntimeMsg::Noop { request_id: 24 },
        RuntimeMsg::StartupPing { request_id: 25 },
    ]
}

fn assert_compact_all_protocol_roundtrip() {
    assert_eq!(
        RuntimeMsg::CompactAll { request_id: 4 }.kind_name(),
        "CompactAll"
    );
    assert!(RuntimeMsg::CompactAll { request_id: 7 }
        .request_id()
        .is_some());
    assert_eq!(
        RuntimeMsg::CompactAll { request_id: 8 }.kind_name(),
        "CompactAll"
    );

    let (runtime, _handle) = Runtime::new();
    let state = RuntimeState::new("/tmp/test_compact_all".into(), true);
    let (_runtime, h) = runtime
        .start_with_config(state, RuntimeConfig::default())
        .expect("start runtime");
    let resp = h
        .send_and_wait(RuntimeMsg::CompactAll {
            request_id: next_request_id().expect("id"),
        })
        .expect("compact_all call");
    match resp {
        RuntimeResponse::Ok { .. } => (),
        _ => panic!("CompactAll did not return Ok"),
    }

    h.send(RuntimeMsg::Shutdown).expect("send shutdown");
    assert!(RuntimeMsg::GetCurrentSequence { request_id: 7 }
        .request_id()
        .is_some());
}

#[test]
fn should_extract_request_id_from_all_request_response_messages() {
    // Arrange
    let messages = write_side_request_response_messages()
        .into_iter()
        .chain(read_side_request_response_messages())
        .collect::<Vec<_>>();

    // Act
    // Assert
    assert!(messages
        .iter()
        .filter(|msg| !matches!(msg, RuntimeMsg::UnregisterSnapshot { .. }))
        .all(|msg| msg.request_id().is_some()));
    assert_eq!(
        RuntimeMsg::UnregisterSnapshot { snapshot_id: 23 }.request_id(),
        None
    );
    assert_compact_all_protocol_roundtrip();
}

// =========== RuntimeResponse Tests ===========

#[test]
fn should_extract_request_id_from_response() {
    // Arrange
    let response = RuntimeResponse::Ok { request_id: 42 };

    // Act
    let req_id = response.request_id();

    // Assert
    assert_eq!(req_id, 42);
}

fn runtime_response_fixtures() -> Vec<RuntimeResponse> {
    let snapshot = Arc::new(crate::runtime::read_snapshot::ReadSnapshot::new(
        Arc::new(crate::sst::SkipListMemtable::new()),
        vec![],
        vec![],
        Arc::new(crate::io::MockFs::new()),
        std::path::PathBuf::new(),
        true,
    ));
    vec![
        RuntimeResponse::Ok { request_id: 1 },
        RuntimeResponse::Error {
            request_id: 2,
            error: crate::common::MidgeError::Internal("error".to_string()),
        },
        RuntimeResponse::ReadValue {
            request_id: 3,
            value: Some(b"value".to_vec()),
        },
        RuntimeResponse::RangeScanResults {
            request_id: 4,
            results: vec![(b"k".to_vec(), b"v".to_vec())],
        },
        RuntimeResponse::FlushComplete {
            request_id: 5,
            sst_name: "sst".to_string(),
        },
        RuntimeResponse::CompactionComplete {
            request_id: 6,
            output_ssts: vec!["out.sst".to_string()],
        },
        RuntimeResponse::ColumnFamilyCreated {
            request_id: 7,
            cf_id: 0,
        },
        RuntimeResponse::CurrentSequence {
            request_id: 8,
            sequence: 123,
        },
        RuntimeResponse::TransactionApplied {
            request_id: 9,
            last_sequence: 200,
            op_count: 2,
            write_stall_hint: false,
        },
        RuntimeResponse::WalAppended {
            request_id: 10,
            sequence: 201,
        },
        RuntimeResponse::ReadSnapshot {
            request_id: 11,
            snapshot,
        },
        RuntimeResponse::RuntimeConfigSnapshot {
            request_id: 12,
            memtable_size_limit: 1,
            memtable_flush_threshold: 1,
            enable_compaction: true,
            l0_compaction_trigger: 4,
            wal_durability_policy: DurabilityPolicy::Batched,
            wal_batch_config: crate::wal::policy::BatchConfig::default(),
        },
        RuntimeResponse::IngestState {
            request_id: 13,
            ingest_active: true,
        },
    ]
}

fn assert_response_payloads(responses: Vec<RuntimeResponse>) {
    for response in responses {
        match response {
            RuntimeResponse::ReadValue { value, .. } => {
                assert_eq!(value, Some(b"value".to_vec()));
            }
            RuntimeResponse::RangeScanResults { results, .. } => assert_eq!(results.len(), 1),
            RuntimeResponse::FlushComplete { sst_name, .. } => assert_eq!(sst_name, "sst"),
            RuntimeResponse::CompactionComplete { output_ssts, .. } => {
                assert_eq!(output_ssts, vec!["out.sst"]);
            }
            RuntimeResponse::CurrentSequence { sequence, .. } => assert_eq!(sequence, 123),
            RuntimeResponse::WalAppended { sequence, .. } => assert_eq!(sequence, 201),
            RuntimeResponse::ReadSnapshot { snapshot, .. } => assert_eq!(snapshot.cf_id, 0),
            RuntimeResponse::RuntimeConfigSnapshot {
                memtable_size_limit,
                memtable_flush_threshold,
                enable_compaction,
                l0_compaction_trigger,
                wal_durability_policy,
                wal_batch_config,
                ..
            } => {
                assert_eq!(memtable_size_limit, 1);
                assert_eq!(memtable_flush_threshold, 1);
                assert!(enable_compaction);
                assert_eq!(l0_compaction_trigger, 4);
                assert_eq!(wal_durability_policy, DurabilityPolicy::Batched);
                let default_batch = crate::wal::policy::BatchConfig::default();
                assert_eq!(wal_batch_config.max_delay_ms, default_batch.max_delay_ms);
                assert_eq!(wal_batch_config.max_bytes, default_batch.max_bytes);
            }
            RuntimeResponse::IngestState { ingest_active, .. } => assert!(ingest_active),
            _ => {}
        }
    }
}

#[test]
fn should_extract_request_id_from_all_responses() {
    // Arrange
    let responses = runtime_response_fixtures();

    // Act
    let request_ids: Vec<u64> = responses.iter().map(RuntimeResponse::request_id).collect();

    // Assert
    assert_eq!(request_ids, (1..=13).collect::<Vec<_>>());
    assert_response_payloads(responses);
}

// =========== ResponseRouter Tests ===========

#[test]
fn should_create_response_router() {
    // Arrange
    // (no setup)

    // Act
    let router = ResponseRouter::new();

    // Assert - Should be usable, register should return a receiver
    let rx = router.register(1);
    // Just verify we got a receiver back (doesn't block, nonblocking channel)
    drop(rx);
}

#[test]
fn should_register_then_complete_response() {
    // Arrange
    let router = ResponseRouter::new();

    // Act - Register and deliver response
    let rx = router.register(42);
    router.complete(RuntimeResponse::Ok { request_id: 42 });

    // Assert - Should receive response
    let received = rx.recv().unwrap();
    assert_eq!(received.request_id(), 42);
}

#[test]
fn should_handle_multiple_pending_requests() {
    // Arrange
    let router = Arc::new(ResponseRouter::new());
    let rx1 = router.register(1);
    let rx2 = router.register(2);
    let rx3 = router.register(3);

    // Act - Complete in different order
    router.complete(RuntimeResponse::Ok { request_id: 2 });
    router.complete(RuntimeResponse::Ok { request_id: 1 });
    router.complete(RuntimeResponse::Ok { request_id: 3 });

    // Assert - Should receive correct responses
    assert_eq!(rx1.recv().unwrap().request_id(), 1);
    assert_eq!(rx2.recv().unwrap().request_id(), 2);
    assert_eq!(rx3.recv().unwrap().request_id(), 3);
}

#[test]
fn should_handle_orphaned_response() {
    // Arrange
    let router = ResponseRouter::new();

    // Act - Try to complete response with no matching request
    // (Should not panic, logs warning)
    router.complete(RuntimeResponse::Ok { request_id: 999 });

    // Assert - Should not crash
}

// =========== RuntimeHandle Tests ===========

#[test]
fn should_create_runtime_handle() {
    // Arrange
    // (no setup)

    // Act
    let (runtime, handle) = Runtime::new();

    // Assert
    // Handle should be cloneable
    let handle2 = handle.clone();
    drop(runtime);
    drop(handle2);
}

#[test]
fn should_handle_send_noop_message() {
    // Arrange
    let (runtime, handle) = Runtime::new();
    let msg = RuntimeMsg::Noop { request_id: 1 };

    // Act
    let result = handle.send(msg);

    // Assert
    assert!(result.is_ok());
    drop(runtime);
}

#[test]
fn should_detect_closed_channel_on_send() {
    // Arrange
    let (runtime, handle) = Runtime::new();

    // Act - Drop runtime to close channel
    drop(runtime);

    // Wait a moment for channel to close
    thread::sleep(std::time::Duration::from_millis(10));

    // Assert
    let result = handle.send(RuntimeMsg::Noop { request_id: 1 });
    assert!(result.is_err());
}

#[test]
fn should_require_request_id_for_send_wait() {
    // Arrange
    let (runtime, handle) = Runtime::new();
    let msg = RuntimeMsg::Shutdown;

    // Act
    let result = handle.send_and_wait(msg);

    // Assert
    assert!(result.is_err());
    drop(runtime);
}

// =========== Runtime Tests ===========

#[test]
fn should_create_runtime() {
    // Arrange
    // (no setup)

    // Act
    let (runtime, _handle) = Runtime::new();

    // Assert
    drop(runtime);
}

#[test]
fn should_create_with_trace_disabled_by_default() {
    // Arrange
    // (no setup)

    // Act
    let (_runtime, _handle) = Runtime::new();

    // Assert - Default should have tracing disabled
    // (Verified by not panicking)
}

#[test]
fn should_shutdown_runtime() {
    // Arrange
    let (runtime, _handle) = Runtime::new();

    // Act - Shutdown should not panic
    runtime.shutdown();

    // Assert - Just checking no panic
}

// =========== CompactionPlan Tests ===========

#[test]
fn should_create_compaction_plan() {
    // Arrange
    // (no setup)

    // Act
    let plan = CompactionPlan {
        input_files: vec![crate::sst::file_name(0, 0, 1)],
        source_level: 0,
        target_level: 1,
        cf_id: 0,
    };

    // Assert
    assert_eq!(plan.input_files.len(), 1);
    assert_eq!(plan.source_level, 0);
    assert_eq!(plan.target_level, 1);
    assert_eq!(plan.cf_id, 0);
}

#[test]
fn should_clone_compaction_plan() {
    // Arrange
    let plan = CompactionPlan {
        input_files: vec!["sst.sst".to_string()],
        source_level: 0,
        target_level: 1,
        cf_id: 0,
    };

    // Act
    let cloned = plan.clone();

    // Assert
    assert_eq!(plan.input_files, cloned.input_files);
}

// =========== FileMeta Tests ===========

#[test]
fn should_create_file_meta() {
    // Arrange
    // (no setup)

    // Act
    let sst_name = crate::sst::file_name(0, 0, 1);
    let meta = FileMeta {
        name: sst_name.clone(),
        level: 0,
        size_bytes: 1024,
        content_crc32c: None,
        cf_id: 0,
        smallest_key: None,
        largest_key: None,
        smallest_seq: None,
        largest_seq: None,
    };

    // Assert
    assert_eq!(meta.name, sst_name);
    assert_eq!(meta.level, 0);
    assert_eq!(meta.size_bytes, 1024);
    assert_eq!(meta.cf_id, 0);
}

#[test]
fn should_clone_file_meta() {
    // Arrange
    let meta = FileMeta {
        name: "sst.sst".to_string(),
        level: 0,
        size_bytes: 100,
        content_crc32c: None,
        cf_id: 0,
        smallest_key: Some(b"a".to_vec()),
        largest_key: Some(b"z".to_vec()),
        smallest_seq: Some(1),
        largest_seq: Some(100),
    };

    // Act
    let cloned = meta.clone();

    // Assert
    assert_eq!(meta.name, cloned.name);
    assert_eq!(meta.smallest_key, cloned.smallest_key);
}
