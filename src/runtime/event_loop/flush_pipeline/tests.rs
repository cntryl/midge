use super::*;
use std::sync::atomic::AtomicBool;

fn event_loop_with_hybrid_storage(
    directory: &tempfile::TempDir,
) -> cntryl_midge_core::common::MidgeResult<(EventLoop, Arc<cntryl_midge_storage::HybridStorage>)> {
    let state = crate::runtime::state::RuntimeState::new(directory.path().to_path_buf(), false);
    let cloud = Arc::new(cntryl_midge_storage::cloud::CloudStorage::new(
        Arc::new(cntryl_midge_storage::cloud::MockCloudBackend::new()),
        String::new(),
    ));
    let hybrid = Arc::new(cntryl_midge_storage::HybridStorage::with_policy(
        cloud,
        cntryl_midge_storage::hybrid::policy::StorageBudgetPolicy::default(),
    ));
    let config = crate::runtime::RuntimeConfig {
        hybrid_storage: Some(Arc::clone(&hybrid)),
        ..crate::runtime::RuntimeConfig::default()
    };
    let event_loop = EventLoop::new(
        state,
        false,
        Arc::new(crate::runtime::ResponseRouter::new()),
        config,
        crate::runtime::event_loop::FlushWorkerMode::Inline,
    )?;
    Ok((event_loop, hybrid))
}

#[test]
fn should_preserve_resource_limit_kind_when_flush_waiter_is_failed(
) -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let (mut event_loop, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
    let request_id = 416;
    let response = event_loop
        .routing
        .router
        .register(request_id, "FlushMemtable");
    event_loop.publication.flush_barrier_waiters.insert(
        0,
        vec![super::super::flush::FlushBarrierWaiter {
            request_id,
            frontier: 7,
        }],
    );

    // Act
    event_loop.fail_flush_waiters(
        0,
        7,
        &cntryl_midge_core::common::MidgeError::ResourceLimit("flush budget exhausted".to_string()),
    );

    // Assert: backpressure must reach the flush caller as backpressure, not
    // as an Internal defect.
    match response.recv_timeout(std::time::Duration::from_secs(1)) {
        Ok(crate::runtime::RuntimeResponse::Error { error, .. }) => assert!(
            matches!(
                &error,
                cntryl_midge_core::common::MidgeError::ResourceLimit(message)
                    if message == "flush budget exhausted"
            ),
            "flush waiter lost the error kind: {error:?}"
        ),
        other => panic!("unexpected flush waiter response: {other:?}"),
    }
    Ok(())
}

#[test]
fn should_not_report_busy_when_deferred_drop_fails_from_flush_pipeline(
) -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange: a safe drop_column_family deferred behind an active
    // publication, then failed by a transient flush-pipeline Busy.
    let directory = tempfile::tempdir()?;
    let (mut event_loop, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
    let request_id = 4161;
    let response = event_loop
        .routing
        .router
        .register(request_id, "ManifestDropColumnFamily");
    event_loop.publication.publication_gate.defer(
        crate::runtime::RuntimeMsg::ManifestDropColumnFamily {
            request_id,
            cf_id: 0,
            discard_unflushed: false,
        },
    );

    // Act
    event_loop.fail_deferred_column_family_drops(
        0,
        &cntryl_midge_core::common::MidgeError::Busy(
            "flush 7 writer validation could not complete: lease io error".to_string(),
        ),
    );

    // Assert: Busy on drop_column_family licenses the caller to call
    // drop_column_family_discarding_unflushed, so a transient pipeline
    // failure must never wear it — and must not lose its message either.
    match response.recv_timeout(std::time::Duration::from_secs(1)) {
        Ok(crate::runtime::RuntimeResponse::Error { error, .. }) => {
            assert!(
                !matches!(error, cntryl_midge_core::common::MidgeError::Busy(_)),
                "flush pipeline failure reached drop_column_family as Busy: {error:?}"
            );
            assert!(
                matches!(
                    &error,
                    cntryl_midge_core::common::MidgeError::Aborted(message)
                        if message.contains(
                            "flush 7 writer validation could not complete: lease io error"
                        )
                ),
                "deferred drop failure lost the original message: {error:?}"
            );
        }
        other => panic!("unexpected deferred drop response: {other:?}"),
    }
    assert!(event_loop
        .publication
        .publication_gate
        .deferred_messages_is_empty());
    Ok(())
}

#[test]
fn should_yield_flush_publication_turn_to_restored_deferred_request(
) -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let (mut event_loop, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
    event_loop.state.sequence = 1;
    event_loop
        .state
        .get_cf(0)
        .expect("default column family")
        .memtable
        .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)?;
    event_loop
        .freeze_active_memtable(0)?
        .expect("freeze non-empty memtable");
    event_loop.routing.pending_msg =
        Some(crate::runtime::RuntimeMsg::CompactAll { request_id: 71 });

    let request_id = 71;
    let response = event_loop.routing.router.register(request_id, "CompactAll");
    let (_msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let restored = event_loop
        .routing
        .pending_msg
        .take()
        .expect("restored request");

    // Act
    event_loop.process_restored_one(restored, &msg_rx);

    // Assert
    assert!(
        !event_loop.actors.flush_actor.is_inflight(),
        "a restored control request must run before the next flush takes the publication gate"
    );
    assert!(matches!(
        response.recv_timeout(std::time::Duration::from_secs(1)),
        Ok(crate::runtime::RuntimeResponse::Ok { request_id: 71 })
    ));
    Ok(())
}

#[test]
fn should_separate_writes_at_freeze_linearization_boundary() -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let (mut event_loop, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
    event_loop
        .state
        .get_cf(0)
        .expect("default column family")
        .memtable
        .put_bytes_with_seq(
            bytes::Bytes::from_static(b"before"),
            bytes::Bytes::from_static(b"old"),
            1,
            None,
        )?;

    // Act
    event_loop
        .freeze_active_memtable(0)?
        .expect("freeze non-empty memtable");
    event_loop
        .state
        .get_cf(0)
        .expect("default column family")
        .memtable
        .put_bytes_with_seq(
            bytes::Bytes::from_static(b"after"),
            bytes::Bytes::from_static(b"new"),
            2,
            None,
        )?;

    // Assert
    let cf = event_loop.state.get_cf(0).expect("default column family");
    let frozen = cf.immutable_memtables.last().expect("frozen memtable");
    assert_eq!(
        frozen.get_bytes(b"before")?,
        Some(bytes::Bytes::from_static(b"old"))
    );
    assert_eq!(frozen.get_bytes(b"after")?, None);
    assert_eq!(cf.memtable.get_bytes(b"before")?, None);
    assert_eq!(
        cf.memtable.get_bytes(b"after")?,
        Some(bytes::Bytes::from_static(b"new"))
    );
    Ok(())
}

#[test]
fn should_resume_built_flush_when_compaction_publication_releases_the_gate(
) -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let (mut event_loop, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
    event_loop.state.sequence = 1;
    event_loop
        .state
        .get_cf(0)
        .expect("family")
        .memtable
        .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)?;
    let flush_id = event_loop.freeze_active_memtable(0)?.expect("frozen");
    event_loop
        .state
        .immutable_flush_by_id_mut(flush_id)
        .expect("immutable")
        .1
        .phase = ImmutableFlushPhase::Building;
    let identity = FlushIdentity {
        flush_id,
        writer_epoch: event_loop.authority.fencing.writer_epoch,
        cf_id: 0,
        sequence: 1,
    };
    event_loop.ensure_flush_sst_identity(identity)?;
    event_loop
        .publication
        .publication_gate
        .try_acquire(crate::runtime::event_loop::coordination::ManifestPublicationOwner::WalPrune);
    event_loop.prepare_flush_publication(
        identity,
        directory.path().join("built.sst"),
        None,
        crate::runtime::FileMeta {
            name: String::new(),
            level: 0,
            size_bytes: 1,
            content_crc32c: Some(0),
            cf_id: 0,
            smallest_key: Some(b"key".to_vec()),
            largest_key: Some(b"key".to_vec()),
            smallest_seq: Some(1),
            largest_seq: Some(1),
            key_bounds_complete: true,
            earliest_expiration_millis: None,
        },
    );

    // Act: the compaction publication finishes and releases the gate.
    event_loop
        .publication
        .publication_gate
        .release(&crate::runtime::event_loop::coordination::ManifestPublicationOwner::WalPrune);
    let start_blocked = event_loop.flush_start_blocked(false);
    let resumed = event_loop.state.begin_next_immutable_flush();

    // Assert
    assert!(
        !start_blocked,
        "a parked flush must not hold the flush worker"
    );
    let resumed = resumed.expect("a flush held behind the gate must be resumable");
    assert_eq!(resumed.flush_id, flush_id);
    assert!(resumed.built.is_some(), "the resumed flush keeps its build");
    Ok(())
}

#[test]
fn should_release_storage_reservation_when_orphan_build_completion_is_discarded(
) -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let (mut event_loop, hybrid) = event_loop_with_hybrid_storage(&directory)?;
    let reservation = hybrid
        .reserve_for_flush_with_token(256)
        .expect("reserve flush storage");
    let identity = FlushIdentity {
        flush_id: 99,
        writer_epoch: 0,
        cf_id: 0,
        sequence: 1,
    };
    let completion = crate::runtime::actors::flush::FlushBuildCompletion {
        identity,
        memtable: Arc::new(cntryl_midge_table::memtable::SkipListMemtable::new()),
        staging_path: directory.path().join("orphan.sst"),
        reservation: Some(reservation),
        build_ns: 1,
        result: Err(cntryl_midge_core::common::MidgeError::Fenced("orphan".to_string())),
    };

    // Act
    let should_continue = event_loop.handle_flush_build_completion(completion);

    // Assert
    assert!(!should_continue);
    assert_eq!(hybrid.budget_snapshot().total_committed_bytes, 0);
    Ok(())
}

#[test]
fn should_remove_failed_flush_output_from_injected_filesystem() -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let (mut event_loop, hybrid) = event_loop_with_hybrid_storage(&directory)?;
    let injected_fs = Arc::new(cntryl_midge_core::io::MockFs::new());
    let mut staged = cntryl_midge_core::io::Fs::open(
        injected_fs.as_ref(),
        &cntryl_midge_core::io::FsPath::new("orphan.sst"),
        cntryl_midge_core::io::OpenOptions {
            mode: cntryl_midge_core::io::OpenMode::ReadWrite,
            create: true,
            create_new: false,
            truncate: true,
        },
    )
    .map_err(FsError::into_midge)?;
    staged
        .write_at(0, bytes::Bytes::from_static(b"orphan"))
        .map_err(FsError::into_midge)?;
    event_loop.state.fs = injected_fs.clone();
    let reservation = hybrid
        .reserve_for_flush_with_token(256)
        .expect("reserve flush storage");
    let completion = FlushBuildCompletion {
        identity: FlushIdentity {
            flush_id: 99,
            writer_epoch: 0,
            cf_id: 0,
            sequence: 1,
        },
        memtable: Arc::new(cntryl_midge_table::memtable::SkipListMemtable::new()),
        staging_path: std::path::PathBuf::from("orphan.sst"),
        reservation: Some(reservation),
        build_ns: 1,
        result: Err(cntryl_midge_core::common::MidgeError::Fenced("orphan".to_string())),
    };

    // Act
    event_loop.handle_flush_build_completion(completion);

    // Assert
    assert_eq!(injected_fs.get_file("orphan.sst"), None);
    assert_eq!(hybrid.budget_snapshot().total_committed_bytes, 0);
    Ok(())
}

struct FlakyLeaderStore {
    epoch: u64,
    fail_next_read: std::sync::atomic::AtomicBool,
    error: fn() -> cntryl_midge_durability::lease::LeaseError,
}

impl cntryl_midge_durability::lease::LeaderStore for FlakyLeaderStore {
    fn acquire_leadership(
        &self,
        _holder_id: &str,
    ) -> Result<cntryl_midge_durability::lease::LeaderRecord, cntryl_midge_durability::lease::LeaseError> {
        Err(cntryl_midge_durability::lease::LeaseError::Internal("not used".into()))
    }

    fn read_current(&self) -> Result<Option<cntryl_midge_durability::lease::LeaderRecord>, cntryl_midge_durability::lease::LeaseError> {
        if self
            .fail_next_read
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err((self.error)());
        }
        Ok(Some(cntryl_midge_durability::lease::LeaderRecord {
            epoch: self.epoch,
            holder_id: "writer".to_string(),
            acquired_at: "test".to_string(),
        }))
    }
}

struct ValidityExpiringLeaderStore {
    epoch: u64,
    validity: Arc<cntryl_midge_durability::lease::LeaseValidity>,
    expire_on_validation: bool,
}
impl cntryl_midge_durability::lease::LeaderStore for ValidityExpiringLeaderStore {
    fn acquire_leadership(
        &self,
        _holder_id: &str,
    ) -> Result<cntryl_midge_durability::lease::LeaderRecord, cntryl_midge_durability::lease::LeaseError> {
        unreachable!("validation fixture")
    }
    fn read_current(&self) -> Result<Option<cntryl_midge_durability::lease::LeaderRecord>, cntryl_midge_durability::lease::LeaseError> {
        if self.expire_on_validation {
            self.validity.expire_for_test();
        }
        Ok(Some(cntryl_midge_durability::lease::LeaderRecord {
            epoch: self.epoch,
            holder_id: "writer".into(),
            acquired_at: "test".into(),
        }))
    }
}

#[test]
fn should_reject_flush_metadata_when_provider_validation_outlives_validity(
) -> cntryl_midge_core::common::MidgeResult<()> {
    assert_rejected_flush_metadata(true)
}

#[cfg(feature = "failpoints")]
#[test]
fn should_reject_flush_metadata_when_intent_persistence_outlives_validity(
) -> cntryl_midge_core::common::MidgeResult<()> {
    assert_rejected_flush_metadata(false)
}

fn assert_rejected_flush_metadata(expire_on_validation: bool) -> cntryl_midge_core::common::MidgeResult<()> {
    #[cfg(feature = "failpoints")]
    let _guard = cntryl_midge_core::failpoints::test_failpoint_guard();
    #[cfg(feature = "failpoints")]
    let _scenario = fail::FailScenario::setup();
    // Arrange
    let directory = tempfile::tempdir()?;
    let (mut event_loop, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
    event_loop.state.sequence = 1;
    event_loop.state.get_cf(0).unwrap().memtable.put_with_seq(
        b"key".to_vec(),
        b"value".to_vec(),
        1,
        None,
    )?;
    let flush_id = event_loop.freeze_active_memtable(0)?.unwrap();
    let epoch = event_loop.authority.fencing.writer_epoch;
    let validity = Arc::new(cntryl_midge_durability::lease::LeaseValidity::new());
    validity
        .activate(
            epoch,
            std::time::Instant::now() + std::time::Duration::from_mins(1),
        )
        .unwrap();
    event_loop.authority.fencing.lease_validity = Some(Arc::clone(&validity));
    event_loop.authority.fencing.leader_store = Some(Arc::new(ValidityExpiringLeaderStore {
        epoch,
        validity: Arc::clone(&validity),
        expire_on_validation,
    }));
    event_loop.authority.fencing.leader_holder_id = Some("writer".into());
    let delta = FlushPublicationDelta {
        identity: FlushIdentity {
            flush_id,
            writer_epoch: epoch,
            cf_id: 0,
            sequence: 1,
        },
        file_meta: crate::runtime::FileMeta {
            name: cntryl_midge_core::cloud_layout::file_name(0, 0, 1),
            level: 0,
            size_bytes: 128,
            content_crc32c: Some(1),
            cf_id: 0,
            smallest_key: Some(b"key".to_vec()),
            largest_key: Some(b"key".to_vec()),
            smallest_seq: Some(1),
            largest_seq: Some(1),
            key_bounds_complete: true,
            earliest_expiration_millis: None,
        },
        next_sst_seq: 2,
    };
    let metadata = || {
        [
            cntryl_midge_durability::metadata::files::MANIFEST_SNAPSHOT,
            cntryl_midge_durability::metadata::files::JOURNAL,
            cntryl_midge_durability::metadata::files::INTENT_LOG,
        ]
        .map(|name| std::fs::read(directory.path().join(name)).ok())
    };
    let before = metadata();
    #[cfg(feature = "failpoints")]
    if !expire_on_validation {
        fail::cfg_callback(
            "midge::flush::after_publication_intent_before_manifest",
            move || validity.expire_for_test(),
        )
        .unwrap();
    }
    // Act
    let result = event_loop.commit_flush_metadata(&delta);
    // Assert
    assert!(matches!(result, Err(cntryl_midge_core::common::MidgeError::Fenced(_))));
    if expire_on_validation {
        assert_eq!(metadata(), before);
    } else {
        assert_eq!(&metadata()[..2], &before[..2]);
        assert!(
            metadata()[2].is_some(),
            "accepted intent remains authoritative"
        );
    }
    Ok(())
}

/// Answers the first `valid_reads` leader reads, then reports a successor.
struct ExpiringLeaderStore {
    epoch: u64,
    valid_reads: std::sync::atomic::AtomicUsize,
}

impl cntryl_midge_durability::lease::LeaderStore for ExpiringLeaderStore {
    fn acquire_leadership(
        &self,
        _holder_id: &str,
    ) -> Result<cntryl_midge_durability::lease::LeaderRecord, cntryl_midge_durability::lease::LeaseError> {
        Err(cntryl_midge_durability::lease::LeaseError::Internal("not used".into()))
    }

    fn read_current(&self) -> Result<Option<cntryl_midge_durability::lease::LeaderRecord>, cntryl_midge_durability::lease::LeaseError> {
        let remaining = self.valid_reads.load(std::sync::atomic::Ordering::SeqCst);
        let holder = if remaining == 0 {
            "successor"
        } else {
            self.valid_reads
                .store(remaining - 1, std::sync::atomic::Ordering::SeqCst);
            "writer"
        };
        Ok(Some(cntryl_midge_durability::lease::LeaderRecord {
            epoch: self.epoch,
            holder_id: holder.to_string(),
            acquired_at: "test".to_string(),
        }))
    }
}

#[test]
fn should_retire_no_local_wal_when_lease_is_lost_during_prune() -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange: three flushed sealed segments, and a lease that stays
    // valid for the pass preamble only.
    let directory = tempfile::tempdir()?;
    let mut state = crate::runtime::state::RuntimeState::new(directory.path().to_path_buf(), false);
    let wal_dir = state.host_wal_dir().clone();
    for segment_id in 1..=3 {
        std::fs::write(
            wal_dir.join(cntryl_midge_durability::wal::segment_file_name(segment_id)),
            b"x",
        )?;
    }
    crate::runtime::event_loop::tests::record_wal_history(
        &mut state,
        directory.path(),
        &[1, 2, 3],
        4,
    );
    let router = Arc::new(crate::runtime::ResponseRouter::new());
    let mut event_loop = EventLoop::new(
        state,
        false,
        router,
        crate::runtime::RuntimeConfig::default(),
        crate::runtime::event_loop::FlushWorkerMode::Inline,
    )?;
    event_loop.authority.fencing.leader_store = Some(Arc::new(ExpiringLeaderStore {
        epoch: event_loop.authority.fencing.writer_epoch,
        valid_reads: std::sync::atomic::AtomicUsize::new(1),
    }));
    event_loop.authority.fencing.leader_holder_id = Some("writer".to_string());

    // Act
    event_loop.prune_local_wal_segments_covered_by_manifest();

    // Assert: the lease is checked again right before the ledger retires
    // anything, so after the loss nothing is retired or deleted.
    let exists = |segment_id| {
        wal_dir
            .join(cntryl_midge_durability::wal::segment_file_name(segment_id))
            .exists()
    };
    assert_eq!((exists(1), exists(2), exists(3)), (true, true, true));
    assert_eq!(
        Some(event_loop.state.wal.history.ledger().log_start()),
        Some(1)
    );
    Ok(())
}

#[test]
fn should_retry_flush_publication_when_completion_validation_fails_transiently(
) -> cntryl_midge_core::common::MidgeResult<()> {
    let unanswered: [fn() -> cntryl_midge_durability::lease::LeaseError; 2] = [
        || cntryl_midge_durability::lease::LeaseError::IoError("leader read failed".into()),
        || cntryl_midge_durability::lease::LeaseError::Timeout("leader read".into()),
    ];
    for error in unanswered {
        // Arrange
        let directory = tempfile::tempdir()?;
        let (mut event_loop, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
        event_loop.state.sequence = 1;
        event_loop
            .state
            .get_cf(0)
            .expect("family")
            .memtable
            .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)?;
        let flush_id = event_loop.freeze_active_memtable(0)?.expect("frozen");
        event_loop.authority.fencing.leader_store = Some(Arc::new(FlakyLeaderStore {
            epoch: event_loop.authority.fencing.writer_epoch,
            fail_next_read: std::sync::atomic::AtomicBool::new(true),
            error,
        }));
        event_loop.authority.fencing.leader_holder_id = Some("writer".to_string());
        let identity = FlushIdentity {
            flush_id,
            writer_epoch: event_loop.authority.fencing.writer_epoch,
            cf_id: 0,
            sequence: 1,
        };
        let (_, flush) = event_loop
            .state
            .immutable_flush_by_id_mut(flush_id)
            .expect("immutable");
        flush.phase = ImmutableFlushPhase::Publishing;
        event_loop.publication.publication_gate.try_acquire(
            crate::runtime::event_loop::coordination::ManifestPublicationOwner::Flush { flush_id },
        );

        // Act: the leader-store read behind validation fails once. The flush
        // may already be durably published, so it must be retried, not
        // stranded in Publishing forever.
        event_loop.handle_flush_publish_completion(FlushPublishCompletion {
            identity,
            reservation: None,
            publish_ns: 1,
            result: Err(cntryl_midge_core::common::MidgeError::Internal("unused".into())),
        });

        // Assert
        let (_, flush) = event_loop
            .state
            .immutable_flush_by_id(flush_id)
            .expect("immutable still owned");
        assert!(
            matches!(flush.phase, ImmutableFlushPhase::RetryPending),
            "phase after transient validation failure: {:?}",
            flush.phase
        );
        assert!(!event_loop.publication.publication_gate.is_active());
    }
    Ok(())
}

#[test]
fn should_not_reload_metadata_after_flush_worker_failure() -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let (mut event_loop, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
    event_loop.state.sequence = 1;
    event_loop
        .state
        .get_cf(0)
        .expect("family")
        .memtable
        .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)?;
    let flush_id = event_loop.freeze_active_memtable(0)?.expect("frozen");
    std::fs::write(
        event_loop
            .state
            .db_path
            .join(cntryl_midge_durability::metadata::files::MANIFEST_SNAPSHOT),
        b"not a manifest",
    )?;
    event_loop.publication.publication_gate.try_acquire(
        crate::runtime::event_loop::coordination::ManifestPublicationOwner::Flush { flush_id },
    );

    // Act
    event_loop.fail_flush_pipeline(
        flush_id,
        None,
        &cntryl_midge_core::common::MidgeError::Internal("worker failed".into()),
    );

    // Assert
    assert!(!event_loop.publication.publication_gate.is_active());
    assert!(!event_loop.state.persistence_anomaly_detected());
    assert!(event_loop.state.ensure_metadata_current().is_ok());
    Ok(())
}

#[test]
fn should_fence_metadata_when_flush_commit_result_is_uncertain() -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let (mut event_loop, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
    event_loop.state.sequence = 1;
    event_loop
        .state
        .get_cf(0)
        .expect("family")
        .memtable
        .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)?;
    let flush_id = event_loop.freeze_active_memtable(0)?.expect("frozen");
    let (_, flush) = event_loop
        .state
        .immutable_flush_by_id(flush_id)
        .expect("owned");
    let identity = FlushIdentity {
        flush_id,
        writer_epoch: flush.writer_epoch,
        cf_id: 0,
        sequence: flush.sequence,
    };
    std::fs::write(
        event_loop
            .state
            .db_path
            .join(cntryl_midge_durability::metadata::files::MANIFEST_SNAPSHOT),
        b"not a manifest",
    )?;
    event_loop.publication.publication_gate.try_acquire(
        crate::runtime::event_loop::coordination::ManifestPublicationOwner::Flush { flush_id },
    );

    // Act
    event_loop.handle_flush_publish_completion(FlushPublishCompletion {
        identity,
        reservation: None,
        publish_ns: 0,
        result: Ok(FlushPublicationDelta {
            identity,
            file_meta: crate::runtime::FileMeta {
                name: cntryl_midge_core::cloud_layout::file_name(0, 0, 1),
                level: 0,
                size_bytes: 10,
                content_crc32c: Some(1),
                cf_id: 0,
                smallest_key: Some(b"key".to_vec()),
                largest_key: Some(b"key".to_vec()),
                smallest_seq: Some(1),
                largest_seq: Some(1),
                key_bounds_complete: true,
                earliest_expiration_millis: None,
            },
            next_sst_seq: 2,
        }),
    });

    // Assert
    assert!(!event_loop.publication.publication_gate.is_active());
    assert!(event_loop.state.persistence_anomaly_detected());
    assert!(matches!(
        event_loop.state.ensure_metadata_current(),
        Err(cntryl_midge_core::common::MidgeError::Fenced(_))
    ));
    Ok(())
}

#[test]
fn should_keep_gate_owned_by_wal_prune_when_flush_failure_did_not_acquire_it(
) -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let (mut event_loop, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
    event_loop.state.sequence = 1;
    event_loop
        .state
        .get_cf(0)
        .expect("family")
        .memtable
        .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)?;
    let flush_id = event_loop.freeze_active_memtable(0)?.expect("frozen");
    std::fs::write(
        event_loop
            .state
            .db_path
            .join(cntryl_midge_durability::metadata::files::MANIFEST_SNAPSHOT),
        b"not a manifest",
    )?;
    let prune_owner = crate::runtime::event_loop::coordination::ManifestPublicationOwner::WalPrune;
    assert!(event_loop
        .publication
        .publication_gate
        .try_acquire(prune_owner.clone()));

    // Act: this flush never acquired the gate.
    event_loop.fail_flush_pipeline(
        flush_id,
        None,
        &cntryl_midge_core::common::MidgeError::Internal("flush admission failed".into()),
    );

    // Assert: a non-owner cannot open the gate or trigger a metadata reload.
    assert!(event_loop.publication.publication_gate.is_active());
    assert!(
        !event_loop.state.persistence_anomaly_detected(),
        "a pre-publication flush failure must not reload persisted metadata"
    );
    assert!(event_loop
        .publication
        .publication_gate
        .release(&prune_owner));
    Ok(())
}

#[test]
fn should_release_fenced_failed_publish_admission_only_when_all_owned_outputs_are_absent(
) -> cntryl_midge_core::common::MidgeResult<()> {
    for residue in ["absent", "staging", "temporary", "final", "unknown owner"] {
        // Arrange
        let directory = tempfile::tempdir()?;
        let (mut event_loop, hybrid) = event_loop_with_hybrid_storage(&directory)?;
        hybrid.enable_ephemeral_sst_cache(256);
        event_loop.state.sequence = 1;
        event_loop
            .state
            .get_cf(0)
            .expect("family")
            .memtable
            .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)?;
        let flush_id = event_loop.freeze_active_memtable(0)?.expect("frozen");
        let identity = FlushIdentity {
            flush_id,
            writer_epoch: event_loop.authority.fencing.writer_epoch,
            cf_id: 0,
            sequence: 1,
        };
        let token = hybrid
            .reserve_for_flush_with_token(128)
            .expect("flush admission");
        let staging_path = directory.path().join("failed.sst");
        let name = cntryl_midge_core::cloud_layout::file_name(0, 0, 1);
        let path = match residue {
            "staging" => Some(staging_path.clone()),
            "temporary" => Some(directory.path().join("failed.sst.tmp")),
            "final" => Some(event_loop.state.host_sst_dir().join(&name)),
            _ => None,
        };
        if let Some(path) = path {
            std::fs::create_dir_all(path.parent().unwrap())?;
            std::fs::write(path, b"retained output")?;
        }
        let (_, flush) = event_loop
            .state
            .immutable_flush_by_id_mut(flush_id)
            .expect("immutable");
        flush.sst_name = Some(name.clone());
        flush.built = Some(FlushBuildOutput {
            identity,
            staging_path,
            reservation: Some(token),
            file_meta: crate::runtime::FileMeta {
                name,
                level: 0,
                size_bytes: 128,
                content_crc32c: Some(1),
                cf_id: 0,
                smallest_key: Some(b"key".to_vec()),
                largest_key: Some(b"key".to_vec()),
                smallest_seq: Some(1),
                largest_seq: Some(1),
                key_bounds_complete: true,
                earliest_expiration_millis: None,
            },
        });
        if residue == "unknown owner" {
            event_loop
                .state
                .get_cf_mut(0)
                .expect("family")
                .immutable_flushes
                .clear();
        }
        event_loop.authority.fencing.writer_epoch += 1;
        // Act
        event_loop.handle_flush_publish_completion(FlushPublishCompletion {
            identity,
            reservation: Some(token),
            publish_ns: 1,
            result: Err(cntryl_midge_core::common::MidgeError::Fenced(
                "publisher lost its lease".into(),
            )),
        });
        // Assert
        assert_eq!(
            hybrid.budget_snapshot().total_committed_bytes,
            if residue == "absent" { 0 } else { 128 },
            "residue: {residue}"
        );
    }
    Ok(())
}

#[test]
fn should_keep_flush_admission_for_retry_when_publication_fails_after_output_is_built(
) -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let (mut event_loop, hybrid) = event_loop_with_hybrid_storage(&directory)?;
    hybrid.enable_ephemeral_sst_cache(256);
    event_loop.state.sequence = 1;
    event_loop
        .state
        .get_cf(0)
        .expect("default CF")
        .memtable
        .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)?;
    let flush_id = event_loop
        .freeze_active_memtable(0)?
        .expect("freeze memtable");
    let reservation = hybrid
        .reserve_for_flush_with_token(256)
        .expect("flush capacity");
    let identity = FlushIdentity {
        flush_id,
        writer_epoch: event_loop.authority.fencing.writer_epoch,
        cf_id: 0,
        sequence: 1,
    };
    let staging_path = directory.path().join("built.sst");
    std::fs::write(&staging_path, [1_u8; 128])?;
    let (_, flush) = event_loop
        .state
        .immutable_flush_by_id_mut(flush_id)
        .expect("immutable");
    flush.built = Some(FlushBuildOutput {
        identity,
        staging_path: staging_path.clone(),
        reservation: Some(reservation),
        file_meta: crate::runtime::FileMeta {
            name: cntryl_midge_core::cloud_layout::file_name(0, 0, 1),
            level: 0,
            size_bytes: 128,
            content_crc32c: Some(1),
            cf_id: 0,
            smallest_key: Some(b"key".to_vec()),
            largest_key: Some(b"key".to_vec()),
            smallest_seq: Some(1),
            largest_seq: Some(1),
            key_bounds_complete: true,
            earliest_expiration_millis: None,
        },
    });

    // Act
    event_loop.handle_flush_publish_completion(FlushPublishCompletion {
        identity,
        reservation: Some(reservation),
        publish_ns: 1,
        result: Err(cntryl_midge_core::common::MidgeError::Timeout(
            "cloud unavailable".into(),
        )),
    });

    // Assert
    assert!(staging_path.exists());
    let (_, flush) = event_loop
        .state
        .immutable_flush_by_id(flush_id)
        .expect("retained immutable");
    assert_eq!(
        flush.built.as_ref().expect("retry output").reservation,
        Some(reservation)
    );
    assert_eq!(hybrid.budget_snapshot().total_committed_bytes, 256);
    assert!(hybrid.reserve_for_flush_with_token(1).is_err());
    Ok(())
}

#[test]
fn should_retain_flush_admission_when_failed_build_residue_cannot_be_removed(
) -> cntryl_midge_core::common::MidgeResult<()> {
    for is_temp in [false, true] {
        // Arrange
        let directory = tempfile::tempdir()?;
        let (mut event_loop, hybrid) = event_loop_with_hybrid_storage(&directory)?;
        hybrid.enable_ephemeral_sst_cache(256);
        let reservation = hybrid
            .reserve_for_flush_with_token(256)
            .expect("flush capacity");
        let staging_path = directory.path().join("failed.sst");
        let residue_path = if is_temp {
            directory.path().join("failed.sst.tmp")
        } else {
            staging_path.clone()
        };
        // A nonempty directory reliably makes unlink fail without permissions or timing.
        std::fs::create_dir(&residue_path)?;
        std::fs::write(residue_path.join("retained-bytes"), [1_u8; 128])?;
        let completion = FlushBuildCompletion {
            identity: FlushIdentity {
                flush_id: 99,
                writer_epoch: 0,
                cf_id: 0,
                sequence: 1,
            },
            memtable: Arc::new(cntryl_midge_table::memtable::SkipListMemtable::new()),
            staging_path,
            reservation: Some(reservation),
            build_ns: 1,
            result: Err(cntryl_midge_core::common::MidgeError::NoSpace("build failed".into())),
        };

        // Act
        event_loop.handle_flush_build_completion(completion);

        // Assert
        assert!(residue_path.join("retained-bytes").exists());
        assert_eq!(hybrid.budget_snapshot().total_committed_bytes, 256);
        assert!(hybrid.reserve_for_flush_with_token(1).is_err());
    }
    Ok(())
}

#[test]
fn should_settle_storage_reservation_when_published_output_lost_immutable_owner(
) -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let (mut event_loop, hybrid) = event_loop_with_hybrid_storage(&directory)?;
    let reservation = hybrid
        .reserve_for_flush_with_token(256)
        .expect("reserve flush storage");
    let identity = FlushIdentity {
        flush_id: 99,
        writer_epoch: 0,
        cf_id: 0,
        sequence: 1,
    };
    let delta = FlushPublicationDelta {
        identity,
        file_meta: crate::runtime::FileMeta {
            name: cntryl_midge_core::cloud_layout::file_name(0, 0, 1),
            level: 0,
            size_bytes: 128,
            content_crc32c: Some(1),
            cf_id: 0,
            smallest_key: Some(b"key".to_vec()),
            largest_key: Some(b"key".to_vec()),
            smallest_seq: Some(1),
            largest_seq: Some(1),
            key_bounds_complete: true,
            earliest_expiration_millis: None,
        },
        next_sst_seq: 2,
    };

    // Act
    event_loop.install_flush_publication(&delta, Some(reservation), false);

    // Assert
    assert_eq!(hybrid.budget_snapshot().total_committed_bytes, 128);
    Ok(())
}

#[test]
fn should_seal_active_wal_when_a_memtable_is_still_unflushed() -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange: prune is the only local seal trigger, so a column family
    // that never goes quiet must not keep the open segment growing
    // forever.
    let directory = tempfile::tempdir()?;
    let state = crate::runtime::state::RuntimeState::new(directory.path().to_path_buf(), false);
    let router = Arc::new(crate::runtime::ResponseRouter::new());
    let mut event_loop = EventLoop::new(
        state,
        false,
        router,
        crate::runtime::RuntimeConfig::default(),
        crate::runtime::event_loop::FlushWorkerMode::Inline,
    )?;
    event_loop.actors.wal_actor.append_single_op(
        &mut event_loop.state,
        crate::runtime::actors::wal::AppendParams {
            request_id: 1,
            cf_id: 0,
            key: bytes::Bytes::from_static(b"unflushed"),
            value: Some(bytes::Bytes::from_static(b"value")),
            insert_only: false,
            ttl_seconds: None,
        },
    )?;
    let current = event_loop.state.wal.open_segment();
    let active = event_loop
        .state
        .host_wal_dir()
        .join(cntryl_midge_durability::wal::segment_file_name(current));
    let active_len_before = std::fs::metadata(&active)?.len();
    assert!(
        event_loop
            .state
            .get_cf(0)
            .expect("default family")
            .memtable
            .size_bytes()
            > 0,
        "the appended record must be unflushed"
    );

    // Act
    event_loop.prune_local_wal_segments_covered_by_manifest();

    // Assert
    assert_eq!(
        event_loop.state.wal.open_segment(),
        current + 1,
        "the open segment was {active_len_before} bytes on disk before prune"
    );
    Ok(())
}

/// A local event loop whose WAL has already appended far more than the
/// eventual-flush byte bound, with every memtable empty (#552).
fn local_event_loop_after_long_wal_history(
    directory: &std::path::Path,
) -> cntryl_midge_core::common::MidgeResult<EventLoop> {
    let state = crate::runtime::state::RuntimeState::new(directory.to_path_buf(), false);
    let router = Arc::new(crate::runtime::ResponseRouter::new());
    let mut event_loop = EventLoop::new(
        state,
        false,
        router,
        crate::runtime::RuntimeConfig::default(),
        crate::runtime::event_loop::FlushWorkerMode::Inline,
    )?;
    let history = u64::try_from(event_loop.state.limits.memtable_flush_threshold)
        .unwrap_or(u64::MAX)
        .saturating_mul(100);
    event_loop.state.wal.appended_bytes = history;
    Ok(event_loop)
}

#[test]
fn should_not_flush_memtable_started_by_a_put_when_wal_history_exceeds_the_byte_bound(
) -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange: the byte gap counts from when this memtable started, not
    // from the start of the WAL.
    let directory = tempfile::tempdir()?;
    let mut event_loop = local_event_loop_after_long_wal_history(directory.path())?;
    event_loop.actors.wal_actor.append_single_op(
        &mut event_loop.state,
        crate::runtime::actors::wal::AppendParams {
            request_id: 1,
            cf_id: 0,
            key: bytes::Bytes::from_static(b"first"),
            value: Some(bytes::Bytes::from_static(b"value")),
            insert_only: false,
            ttl_seconds: None,
        },
    )?;

    // Act
    let frozen = event_loop.drain_auto_flush_memtables();

    // Assert
    assert_eq!(frozen, 0, "a fresh memtable must not be flushed at once");
    Ok(())
}

#[test]
fn should_not_flush_memtable_started_by_a_range_delete_when_wal_history_exceeds_the_byte_bound(
) -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let mut event_loop = local_event_loop_after_long_wal_history(directory.path())?;
    event_loop.actors.wal_actor.append_single_range_delete(
        &mut event_loop.state,
        1,
        0,
        bytes::Bytes::from_static(b"a"),
        bytes::Bytes::from_static(b"z"),
        None,
    )?;

    // Act
    let frozen = event_loop.drain_auto_flush_memtables();

    // Assert
    assert_eq!(frozen, 0, "a fresh memtable must not be flushed at once");
    Ok(())
}

#[test]
fn should_count_put_bytes_toward_the_local_wal_gap_when_appending() -> cntryl_midge_core::common::MidgeResult<()>
{
    // Arrange
    let directory = tempfile::tempdir()?;
    let mut event_loop = local_event_loop_after_long_wal_history(directory.path())?;
    let before = event_loop.state.wal.appended_bytes;

    // Act
    event_loop.actors.wal_actor.append_single_op(
        &mut event_loop.state,
        crate::runtime::actors::wal::AppendParams {
            request_id: 1,
            cf_id: 0,
            key: bytes::Bytes::from_static(b"counted"),
            value: Some(bytes::Bytes::from_static(b"value")),
            insert_only: false,
            ttl_seconds: None,
        },
    )?;

    // Assert
    assert!(event_loop.state.wal.appended_bytes > before);
    Ok(())
}

#[test]
fn should_not_rotate_local_wal_when_active_segment_is_empty() -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange: nothing has been written since the last rotation.
    let directory = tempfile::tempdir()?;
    let state = crate::runtime::state::RuntimeState::new(directory.path().to_path_buf(), false);
    let router = Arc::new(crate::runtime::ResponseRouter::new());
    let mut event_loop = EventLoop::new(
        state,
        false,
        router,
        crate::runtime::RuntimeConfig::default(),
        crate::runtime::event_loop::FlushWorkerMode::Inline,
    )?;
    let current = event_loop.state.wal.open_segment();

    // Act
    event_loop.prune_local_wal_segments_covered_by_manifest();

    // Assert
    assert_eq!(event_loop.state.wal.open_segment(), current);
    Ok(())
}

fn local_event_loop_with_sealed_segments(
    directory: &std::path::Path,
) -> cntryl_midge_core::common::MidgeResult<EventLoop> {
    let mut state = crate::runtime::state::RuntimeState::new(directory.to_path_buf(), false);
    for sequence in 1..=3 {
        let record = cntryl_midge_durability::wal::WalRecord::new(
            cntryl_midge_durability::wal::WalOpKind::Put,
            bytes::Bytes::from(format!("key-{sequence}")),
            Some(bytes::Bytes::from_static(b"value")),
            sequence,
        );
        let mut frame = Vec::new();
        cntryl_midge_durability::wal::frame::append_frame(&mut frame, &cntryl_midge_durability::wal::encoding::encode(&record)?)?;
        std::fs::write(
            state
                .host_wal_dir()
                .join(cntryl_midge_durability::wal::segment_file_name(sequence)),
            frame,
        )?;
    }
    crate::runtime::event_loop::tests::record_wal_history(&mut state, directory, &[1, 2, 3], 4);
    EventLoop::new(
        state,
        false,
        Arc::new(crate::runtime::ResponseRouter::new()),
        crate::runtime::RuntimeConfig::default(),
        crate::runtime::event_loop::FlushWorkerMode::Inline,
    )
}

#[test]
fn should_retain_local_wal_when_sst_metadata_has_no_flush_frontier(
) -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange: file membership alone never grants retirement authority.
    let directory = tempfile::tempdir()?;
    let mut event_loop = local_event_loop_with_sealed_segments(directory.path())?;
    event_loop
        .state
        .manifest
        .add_file(cntryl_midge_durability::metadata::FileMeta {
            name: cntryl_midge_core::cloud_layout::file_name(0, 0, 1),
            cf_id: 0,
            smallest_seq: Some(1),
            largest_seq: Some(3),
            ..Default::default()
        });

    // Act
    event_loop.prune_local_wal_segments_covered_by_manifest();

    // Assert
    assert_eq!(event_loop.state.wal.history.ledger().log_start(), 1);
    for id in 1..=3 {
        assert!(event_loop
            .state
            .host_wal_dir()
            .join(cntryl_midge_durability::wal::segment_file_name(id))
            .exists());
    }
    Ok(())
}

#[test]
fn should_retire_local_wal_from_durable_frontiers_without_opening_sst_payloads(
) -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange: a durable flush publication grants prefix authority. The SST
    // path is deliberately absent; retention must use the persisted summary.
    let directory = tempfile::tempdir()?;
    let mut event_loop = local_event_loop_with_sealed_segments(directory.path())?;
    let file: crate::runtime::FileMeta = (&cntryl_midge_durability::metadata::FileMeta {
        name: cntryl_midge_core::cloud_layout::file_name(0, 0, 1),
        cf_id: 0,
        smallest_seq: Some(1),
        largest_seq: Some(3),
        ..Default::default()
    })
        .into();
    event_loop.state.commit_flush_publication_for(
        crate::runtime::state::FlushManifestPublication {
            origin: cntryl_midge_durability::metadata::accounting::Origin::OrdinaryLocalFlush,
            cf_id: 0,
            sequence: 3,
            file_meta: &file,
            next_sst_seq: 2,
            require_snapshot: false,
        },
    )?;

    // Act
    event_loop.prune_local_wal_segments_covered_by_manifest();

    // Assert
    assert_eq!(event_loop.state.wal.history.ledger().log_start(), 4);
    for id in 1..=3 {
        assert!(!event_loop
            .state
            .host_wal_dir()
            .join(cntryl_midge_durability::wal::segment_file_name(id))
            .exists());
    }
    assert!(!event_loop.state.host_sst_dir().join(&file.name).exists());
    Ok(())
}

#[test]
fn should_not_checkpoint_manifest_on_event_loop_for_each_flush_name_reservation(
) -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let (mut event_loop, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
    let snapshot = directory
        .path()
        .join(cntryl_midge_durability::metadata::files::MANIFEST_SNAPSHOT);
    let before = std::fs::read(&snapshot).ok();

    // Act
    let mut names = Vec::new();
    for _ in 0..10 {
        names.push(event_loop.reserve_flush_sst_seq(0)?);
    }

    // Assert: distinct names, no snapshot rewrite, one durable bump.
    let mut unique = names.clone();
    unique.dedup();
    assert_eq!(unique, names);
    assert_eq!(std::fs::read(&snapshot).ok(), before);
    let journaled = cntryl_midge_durability::metadata::ManifestPersistence::load(directory.path())
        .map_err(cntryl_midge_core::common::MidgeError::Internal)?;
    let durable = journaled.next_sst_seqs.get(&0).copied().unwrap_or(1);
    assert!(
        names.iter().all(|seq| *seq < durable),
        "{names:?} vs {durable}"
    );
    let bumps = cntryl_midge_durability::metadata::journal::replay_journal(directory.path())?
        .into_iter()
        .filter(|edit| matches!(edit, cntryl_midge_durability::metadata::ManifestEdit::BumpNextSstSeq { .. }))
        .count();
    assert_eq!(bumps, 1);
    Ok(())
}

#[test]
fn should_mirror_flush_sst_name_reservation_again_when_mirror_failed(
) -> cntryl_midge_core::common::MidgeResult<()> {
    use cntryl_midge_durability::lease::PrimaryLease as _;

    // Arrange: the journal append succeeds but the cloud mirror fails,
    // so the cloud metadata never learns the reserved block.
    let directory = tempfile::tempdir()?;
    let (mut event_loop, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
    cntryl_midge_durability::metadata::ManifestPersistence::save(
        &event_loop.state.db_path,
        &event_loop.state.manifest,
    )
    .map_err(cntryl_midge_core::common::MidgeError::Internal)?;
    let cloud = Arc::new(cntryl_midge_storage::cloud::CloudStorage::new(
        Arc::new(cntryl_midge_storage::cloud::MockCloudBackend::new()),
        String::new(),
    ));
    let lease = Arc::new(cntryl_midge_durability::lease::CloudStorageLease::new_provider_backed(
        cntryl_midge_durability::lease::CloudLeaseConfig {
            bucket: "test".to_string(),
            prefix: String::new(),
        },
        directory.path().to_path_buf(),
        Arc::clone(&cloud),
    ));
    let _lease_guard = Arc::clone(&lease)
        .try_acquire()
        .expect("acquire cloud metadata lease");
    event_loop.authority.fencing.writer_epoch = lease.epoch();
    event_loop.authority.fencing.leader_holder_id = Some(lease.holder_id());
    event_loop.authority.fencing.leader_store = lease.get_leader_store();
    event_loop.actors.cloud_coordinator.cloud_metadata_storage = Some(cloud);
    let lease_healthy = Arc::new(std::sync::atomic::AtomicBool::new(false));
    event_loop.authority.fencing.lease_healthy = Some(Arc::clone(&lease_healthy));
    let failed = event_loop.reserve_flush_sst_seq(0);

    // Act: a retry inside the same block must still reach the mirror.
    let retried = event_loop.reserve_flush_sst_seq(0);
    lease_healthy.store(true, std::sync::atomic::Ordering::SeqCst);
    let recovered = event_loop.reserve_flush_sst_seq(0);

    // Assert
    assert!(failed.is_err());
    assert!(
        retried.is_err(),
        "a name was handed out without a mirrored reservation: {retried:?}"
    );
    assert!(recovered.is_ok(), "{recovered:?}");
    Ok(())
}

#[test]
fn should_not_mirror_sst_name_reservation_while_metadata_reload_is_required(
) -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange: a failed reload leaves memory behind disk (#500), and the
    // on-disk intent log is unreadable.
    let directory = tempfile::tempdir()?;
    let (mut event_loop, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
    let cloud = Arc::new(cntryl_midge_storage::cloud::CloudStorage::new(
        Arc::new(cntryl_midge_storage::cloud::MockCloudBackend::new()),
        String::new(),
    ));
    event_loop.actors.cloud_coordinator.cloud_metadata_storage = Some(Arc::clone(&cloud));
    std::fs::write(
        event_loop
            .state
            .db_path
            .join(cntryl_midge_durability::metadata::files::INTENT_LOG),
        b"not json",
    )?;
    assert!(event_loop.state.reload_persisted_metadata().is_err());

    // Act
    let result = event_loop.reserve_sst_name_durably(0, 1);

    // Assert: fenced, and nothing reached the authoritative mirror.
    assert!(
        matches!(result, Err(cntryl_midge_core::common::MidgeError::Fenced(_))),
        "{result:?}"
    );
    let key =
        cntryl_midge_core::cloud_layout::CloudObjectLayout::metadata_key(cntryl_midge_durability::metadata::files::INTENT_LOG);
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_get(&key, tx);
    let uploaded = matches!(
        rx.recv_timeout(std::time::Duration::from_secs(1)),
        Ok(cntryl_midge_storage::cloud::CloudEvent::Get {
            result: cntryl_midge_storage::cloud::CloudOutcome::Ok(_),
            ..
        })
    );
    assert!(!uploaded, "stale intent log was mirrored");
    Ok(())
}

#[test]
fn should_not_reuse_reserved_sst_names_when_replacement_restores_from_cloud_metadata(
) -> cntryl_midge_core::common::MidgeResult<()> {
    use cntryl_midge_durability::lease::PrimaryLease as _;

    // Arrange: hand out flush names and a compaction generation, then lose
    // the machine. The replacement sees only the cloud metadata mirror.
    let directory = tempfile::tempdir()?;
    let (mut first, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
    cntryl_midge_durability::metadata::ManifestPersistence::save(&first.state.db_path, &first.state.manifest)
        .map_err(cntryl_midge_core::common::MidgeError::Internal)?;
    let cloud = Arc::new(cntryl_midge_storage::cloud::CloudStorage::new(
        Arc::new(cntryl_midge_storage::cloud::MockCloudBackend::new()),
        String::new(),
    ));
    let lease = Arc::new(cntryl_midge_durability::lease::CloudStorageLease::new_provider_backed(
        cntryl_midge_durability::lease::CloudLeaseConfig {
            bucket: "test".to_string(),
            prefix: String::new(),
        },
        directory.path().to_path_buf(),
        Arc::clone(&cloud),
    ));
    let _lease_guard = Arc::clone(&lease)
        .try_acquire()
        .expect("acquire cloud metadata lease");
    first.authority.fencing.writer_epoch = lease.epoch();
    first.authority.fencing.leader_holder_id = Some(lease.holder_id());
    first.authority.fencing.leader_store = lease.get_leader_store();
    first.actors.cloud_coordinator.cloud_metadata_storage = Some(Arc::clone(&cloud));
    let mut used = Vec::new();
    for _ in 0..20 {
        used.push(first.reserve_flush_sst_seq(0)?);
    }
    let plan =
        first.assign_compaction_output_sequence(crate::compaction::CompactionPlan::new(0, 0, 1))?;
    let orphan_key = cntryl_midge_core::cloud_layout::object_key(&cntryl_midge_core::cloud_layout::compaction_file_name(
        0,
        1,
        plan.output_seq,
        0,
    ));
    let orphan = b"abandoned immutable compaction output".to_vec();
    let deadline = cntryl_midge_core::common::OperationDeadline::from_budget(std::time::Duration::from_secs(5));
    cntryl_midge_storage::cloud::BlockingCloud::new(&cloud, &deadline).put_with_precondition(
        &orphan_key,
        orphan.clone(),
        &cntryl_midge_storage::StoragePrecondition::IfAbsent,
    )?;
    drop(first);
    let replacement = tempfile::tempdir()?;
    let leader_store = lease.get_leader_store().expect("cloud leader store");
    crate::runtime::cloud_startup::CloudStartupRecovery::hydrate_cloud_metadata(
        &cloud,
        leader_store.as_ref(),
        &cntryl_midge_core::io::real::test_root(replacement.path()),
        replacement.path(),
        cntryl_midge_durability::RecoveryPolicy::Strict,
    )?;

    // Act
    let (mut second, _hybrid) = event_loop_with_hybrid_storage(&replacement)?;
    let first_name = second.reserve_flush_sst_seq(0)?;
    let next_generation = second
        .assign_compaction_output_sequence(crate::compaction::CompactionPlan::new(0, 0, 1))?
        .output_seq;

    // Assert
    assert!(
        used.iter().all(|seq| *seq < first_name),
        "{used:?} then {first_name}"
    );
    assert!(next_generation > plan.output_seq);
    let deadline = cntryl_midge_core::common::OperationDeadline::from_budget(std::time::Duration::from_secs(5));
    assert_eq!(
        cntryl_midge_storage::cloud::BlockingCloud::new(&cloud, &deadline).get_optional(&orphan_key)?,
        Some(orphan)
    );
    Ok(())
}

#[test]
fn should_not_reuse_reserved_flush_sst_name_when_reopened_after_crash(
) -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange: hand out names, then lose the in-memory cursor.
    let directory = tempfile::tempdir()?;
    let (mut first, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
    let mut used = Vec::new();
    for _ in 0..3 {
        used.push(first.reserve_flush_sst_seq(0)?);
    }
    drop(first);

    // Act
    let (mut reopened, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
    let after_restart = reopened.reserve_flush_sst_seq(0)?;

    // Assert
    assert!(
        used.iter().all(|seq| *seq < after_restart),
        "{used:?} then {after_restart}"
    );
    Ok(())
}

#[test]
fn should_validate_transactions_through_event_loop_read_resources() -> cntryl_midge_core::common::MidgeResult<()>
{
    // Arrange
    let directory = tempfile::tempdir()?;
    let state = crate::runtime::state::RuntimeState::new(directory.path().to_path_buf(), false);

    // Act
    let event_loop = EventLoop::new(
        state,
        false,
        Arc::new(crate::runtime::ResponseRouter::new()),
        crate::runtime::RuntimeConfig::default(),
        crate::runtime::event_loop::FlushWorkerMode::Inline,
    )?;

    // Assert: validation shares the loop's reader and block caches (#492).
    assert!(event_loop.authority.read_resources.is_some());
    assert!(event_loop.actors.wal_actor.has_read_resources());
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_retain_all_wal_state_when_pre_prune_sync_fails() -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange
    let _failpoint_guard = cntryl_midge_core::failpoints::test_failpoint_guard();
    let directory = tempfile::tempdir()?;
    let state = crate::runtime::state::RuntimeState::new(directory.path().to_path_buf(), false);
    let router = Arc::new(crate::runtime::ResponseRouter::new());
    let config = crate::runtime::RuntimeConfig {
        wal_durability_policy: cntryl_midge_durability::wal::DurabilityPolicy::Batched,
        ..crate::runtime::RuntimeConfig::default()
    };
    let mut event_loop = EventLoop::new(
        state,
        false,
        router,
        config,
        crate::runtime::event_loop::FlushWorkerMode::Inline,
    )?;
    event_loop.actors.wal_actor.append_single_op(
        &mut event_loop.state,
        crate::runtime::actors::wal::AppendParams {
            request_id: 1,
            cf_id: 0,
            key: bytes::Bytes::from_static(b"published"),
            value: Some(bytes::Bytes::from_static(b"value")),
            insert_only: false,
            ttl_seconds: None,
        },
    )?;
    event_loop.sync_local_wal_before_prune_rotation()?;
    event_loop.rotate_local_wal_transition()?;
    event_loop.actors.wal_actor.append_single_op(
        &mut event_loop.state,
        crate::runtime::actors::wal::AppendParams {
            request_id: 2,
            cf_id: 0,
            key: bytes::Bytes::from_static(b"buffered"),
            value: Some(bytes::Bytes::from_static(b"value")),
            insert_only: false,
            ttl_seconds: None,
        },
    )?;
    let segment_before = event_loop.state.wal.open_segment();
    let durable_before = event_loop.state.wal.frontiers.local_durable();
    let sealed_path = event_loop
        .state
        .host_wal_dir()
        .join(cntryl_midge_durability::wal::segment_file_name(segment_before - 1));
    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::wal::inject_no_space_on_sync", "return").expect("configure WAL sync failure");

    // Act
    event_loop.prune_local_wal_segments_covered_by_manifest();
    fail::remove("midge::wal::inject_no_space_on_sync");
    scenario.teardown();

    // Assert
    assert_eq!(event_loop.state.wal.open_segment(), segment_before);
    assert_eq!(
        event_loop.state.wal.frontiers.local_durable(),
        durable_before
    );
    assert!(sealed_path.exists(), "failed sync must retain sealed WAL");
    assert!(
        event_loop
            .state
            .host_wal_dir()
            .join(cntryl_midge_durability::wal::segment_file_name(segment_before))
            .exists(),
        "failed sync must retain the open segment"
    );
    Ok(())
}

#[test]
fn should_reject_publication_completion_after_lease_loss() -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let state = crate::runtime::state::RuntimeState::new_in_epoch(directory.path(), false, 7);
    let router = Arc::new(crate::runtime::ResponseRouter::new());
    let healthy = Arc::new(AtomicBool::new(true));
    let cloud = Arc::new(cntryl_midge_storage::cloud::CloudStorage::new(
        Arc::new(cntryl_midge_storage::cloud::MockCloudBackend::new()),
        String::new(),
    ));
    let hybrid = Arc::new(cntryl_midge_storage::HybridStorage::with_policy(
        cloud,
        cntryl_midge_storage::hybrid::policy::StorageBudgetPolicy::default(),
    ));
    let config = crate::runtime::RuntimeConfig {
        writer_epoch: 7,
        lease_healthy: Some(Arc::clone(&healthy)),
        hybrid_storage: Some(Arc::clone(&hybrid)),
        ..crate::runtime::RuntimeConfig::default()
    };
    let mut event_loop = EventLoop::new(
        state,
        false,
        router,
        config,
        crate::runtime::event_loop::FlushWorkerMode::Inline,
    )?;
    event_loop.state.sequence = 1;
    event_loop
        .state
        .get_cf(0)
        .expect("default column family")
        .memtable
        .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)?;
    let flush_id = event_loop
        .freeze_active_memtable(0)?
        .expect("non-empty memtable must freeze");
    let identity = FlushIdentity {
        flush_id,
        writer_epoch: 7,
        cf_id: 0,
        sequence: 1,
    };
    let reservation = hybrid
        .reserve_for_flush_with_token(256)
        .expect("reserve flush storage");
    healthy.store(false, std::sync::atomic::Ordering::Release);
    let result =
        FlushWorkerResult::Publish(crate::runtime::actors::flush::FlushPublishCompletion {
            identity,
            reservation: Some(reservation),
            publish_ns: 10,
            result: Ok(FlushPublicationDelta {
                identity,
                file_meta: crate::runtime::FileMeta {
                    name: cntryl_midge_core::cloud_layout::file_name(0, 0, 1),
                    level: 0,
                    size_bytes: 128,
                    content_crc32c: Some(1),
                    cf_id: 0,
                    smallest_key: Some(b"key".to_vec()),
                    largest_key: Some(b"key".to_vec()),
                    smallest_seq: Some(1),
                    largest_seq: Some(1),
                    key_bounds_complete: true,
                    earliest_expiration_millis: None,
                },
                next_sst_seq: 2,
            }),
        });

    // Act
    event_loop.handle_flush_worker_result(result);

    // Assert
    assert!(event_loop.state.manifest.files.is_empty());
    assert_eq!(event_loop.state.wal.open_segment(), 1);
    assert_eq!(
        hybrid.budget_snapshot().total_committed_bytes,
        128,
        "a published stale completion must settle its reservation to actual SST bytes"
    );
    assert_eq!(
        event_loop
            .state
            .get_cf(0)
            .expect("default column family")
            .immutable_flushes
            .len(),
        1,
        "the stale runtime must retain the immutable without reclaiming it"
    );
    Ok(())
}

fn event_loop_with_frozen_publishing_flush(
    directory: &tempfile::TempDir,
) -> cntryl_midge_core::common::MidgeResult<(EventLoop, FlushIdentity)> {
    let (mut event_loop, _hybrid) = event_loop_with_hybrid_storage(directory)?;
    event_loop.state.sequence = 1;
    event_loop
        .state
        .get_cf(0)
        .expect("family")
        .memtable
        .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)?;
    let flush_id = event_loop.freeze_active_memtable(0)?.expect("frozen");
    let identity = FlushIdentity {
        flush_id,
        writer_epoch: event_loop.authority.fencing.writer_epoch,
        cf_id: 0,
        sequence: 1,
    };
    let (_, flush) = event_loop
        .state
        .immutable_flush_by_id_mut(flush_id)
        .expect("immutable");
    flush.phase = ImmutableFlushPhase::Publishing;
    Ok((event_loop, identity))
}

fn deliver_failed_publish(event_loop: &mut EventLoop, identity: FlushIdentity) {
    event_loop.handle_flush_publish_completion(FlushPublishCompletion {
        identity,
        reservation: None,
        publish_ns: 1,
        result: Err(cntryl_midge_core::common::MidgeError::Internal("unused".into())),
    });
}

#[test]
fn should_retry_flush_publication_when_ddl_authority_ambiguity_clears_after_completion(
) -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let (mut event_loop, identity) = event_loop_with_frozen_publishing_flush(&directory)?;
    event_loop.authority.fencing.ddl_authority_ambiguous = true;

    // Act: the publish worker returns while the writer is fenced.
    deliver_failed_publish(&mut event_loop, identity);
    event_loop.authority.fencing.ddl_authority_ambiguous = false;
    let (_, flush) = event_loop
        .state
        .immutable_flush_by_id_mut(identity.flush_id)
        .expect("immutable still owned");
    let phase = flush.phase;
    flush.retry.mark_due();
    let selected = event_loop.state.begin_next_immutable_flush();

    // Assert: the flush is retryable and the scheduler picks it up again.
    assert_eq!(phase, ImmutableFlushPhase::RetryPending);
    assert_eq!(
        selected.map(|flush| flush.flush_id),
        Some(identity.flush_id),
        "a flush that failed validation while fenced must stay schedulable"
    );
    Ok(())
}

fn recv_flush_result(event_loop: &EventLoop) -> FlushWorkerResult {
    event_loop
        .pipeline
        .flush_worker_result_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("flush worker result")
}

type PendingPublish = (
    EventLoop,
    Arc<cntryl_midge_storage::HybridStorage>,
    u64,
    FlushWorkerResult,
    crossbeam::channel::Receiver<crate::runtime::RuntimeResponse>,
);

/// Freezes one flush, runs the real inline worker through its build and hands
/// back the publish result before the loop has handled it.
fn event_loop_with_publish_result_pending(
    directory: &tempfile::TempDir,
) -> cntryl_midge_core::common::MidgeResult<PendingPublish> {
    let (mut event_loop, hybrid) = event_loop_with_hybrid_storage(directory)?;
    hybrid.enable_ephemeral_sst_cache(1 << 20);
    event_loop.state.sequence = 1;
    event_loop
        .state
        .get_cf(0)
        .expect("family")
        .memtable
        .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)?;
    let flush_id = event_loop.freeze_active_memtable(0)?.expect("frozen");
    let response = event_loop.routing.router.register(77, "FlushMemtable");
    event_loop.publication.flush_barrier_waiters.insert(
        0,
        vec![super::super::flush::FlushBarrierWaiter {
            request_id: 77,
            frontier: 1,
        }],
    );
    event_loop.schedule_next_flush_worker();
    let build = recv_flush_result(&event_loop);
    assert!(matches!(build, FlushWorkerResult::Build(_)));
    event_loop.handle_flush_worker_result(build);
    let publish = recv_flush_result(&event_loop);
    assert!(matches!(publish, FlushWorkerResult::Publish(_)));
    Ok((event_loop, hybrid, flush_id, publish, response))
}

fn flush_frontier(event_loop: &EventLoop) -> Option<u64> {
    match event_loop.state.manifest.flush_frontiers().get(0).ok()? {
        cntryl_midge_durability::metadata::frontiers::FamilyFrontier::Live { through_sequence } => {
            Some(through_sequence)
        }
        cntryl_midge_durability::metadata::frontiers::FamilyFrontier::Dropped => None,
    }
}

#[test]
fn should_republish_built_flush_exactly_once_when_fence_clears_after_publish_completion(
) -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange: baseline with no fence.
    let baseline_dir = tempfile::tempdir()?;
    let (mut baseline, baseline_hybrid, _, publish, _) =
        event_loop_with_publish_result_pending(&baseline_dir)?;
    baseline.handle_flush_worker_result(publish);
    baseline.drain_inline_flush_worker();
    let baseline_budget = baseline_hybrid.budget_snapshot();
    assert_eq!(baseline.state.manifest.files.len(), 1);

    // Arrange: same flush, fenced before its Ok publish result is handled.
    let directory = tempfile::tempdir()?;
    let (mut event_loop, hybrid, flush_id, publish, response) =
        event_loop_with_publish_result_pending(&directory)?;
    let (name, seq) = {
        let (_, flush) = event_loop
            .state
            .immutable_flush_by_id(flush_id)
            .expect("flush");
        (flush.sst_name.clone(), flush.sst_seq)
    };
    event_loop.authority.fencing.ddl_authority_ambiguous = true;

    // Act
    event_loop.handle_flush_worker_result(publish);

    // Assert: retryable, output kept, nothing published, waiter fenced.
    let (_, flush) = event_loop
        .state
        .immutable_flush_by_id(flush_id)
        .expect("immutable still owned");
    assert_eq!(flush.phase, ImmutableFlushPhase::RetryPending);
    assert_eq!(flush.failures, 1);
    assert!(flush.built.is_some());
    assert_eq!((flush.sst_name.clone(), flush.sst_seq), (name.clone(), seq));
    assert!(event_loop.state.manifest.files.is_empty());
    assert!(matches!(
        response.recv_timeout(std::time::Duration::from_secs(1)),
        Ok(crate::runtime::RuntimeResponse::Error {
            error: cntryl_midge_core::common::MidgeError::Fenced(_),
            ..
        })
    ));

    // Act: the fence clears and the retry comes due.
    event_loop.authority.fencing.ddl_authority_ambiguous = false;
    event_loop.state.make_immutable_flush_retry_due(0);
    event_loop.schedule_next_flush_worker();
    event_loop.drain_inline_flush_worker();

    // Assert: published exactly once, frontier advanced, queue drained, and
    // staging accounting matches an unfenced flush.
    let files = &event_loop.state.manifest.files;
    assert_eq!(files.len(), 1);
    assert_eq!(Some(files[0].name.clone()), name);
    assert_eq!(flush_frontier(&event_loop), Some(1));
    assert!(event_loop
        .state
        .get_cf(0)
        .expect("family")
        .immutable_flushes
        .is_empty());
    assert_eq!(
        format!("{:?}", hybrid.budget_snapshot()),
        format!("{baseline_budget:?}")
    );
    Ok(())
}

#[test]
fn should_back_off_each_retry_when_fence_never_clears() -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let (mut event_loop, hybrid, flush_id, publish, _response) =
        event_loop_with_publish_result_pending(&directory)?;
    event_loop.authority.fencing.ddl_authority_ambiguous = true;
    event_loop.handle_flush_worker_result(publish);
    let committed = hybrid.budget_snapshot().total_committed_bytes;
    let mut last_backoff = std::time::Duration::ZERO;

    for expected_failures in [2_u32, 3] {
        // Act: the retry comes due and fails validation again.
        event_loop.state.make_immutable_flush_retry_due(0);
        event_loop.schedule_next_flush_worker();

        // Assert
        let (_, flush) = event_loop
            .state
            .immutable_flush_by_id(flush_id)
            .expect("immutable retained");
        assert_eq!(flush.phase, ImmutableFlushPhase::RetryPending);
        assert_eq!(flush.failures, expected_failures);
        let backoff = flush.retry.remaining().expect("backoff scheduled");
        assert!(backoff > last_backoff, "{backoff:?} <= {last_backoff:?}");
        last_backoff = backoff;
        assert!(event_loop.state.manifest.files.is_empty());
        assert!(!event_loop.publication.publication_gate.is_active());
        assert!(!event_loop.actors.flush_actor.is_inflight());
        assert_eq!(hybrid.budget_snapshot().total_committed_bytes, committed);
    }
    Ok(())
}
