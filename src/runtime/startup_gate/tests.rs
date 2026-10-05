use super::*;
use crate::runtime::{Runtime, RuntimeHandle, RuntimeState};
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::time::Instant;

#[derive(Debug, Default)]
struct Events {
    prepared: AtomicUsize,
    admitted: AtomicUsize,
    aborted: AtomicUsize,
}

impl StartupObserver for Events {
    fn observe(&self, event: StartupEvent) {
        match event {
            StartupEvent::RuntimePrepared => &self.prepared,
            StartupEvent::RuntimeAdmitted => &self.admitted,
            StartupEvent::RuntimeAborted => &self.aborted,
            StartupEvent::LeaseAcquired { .. } | StartupEvent::CleanupFinished { .. } => return,
        }
        .fetch_add(1, Ordering::AcqRel);
    }
}

struct Fixture {
    runtime: Runtime,
    handle: RuntimeHandle,
    gate: StartupAdmission,
    events: Arc<Events>,
}

impl Fixture {
    fn prepare(scope: DeadlineScope, mut config: RuntimeConfig) -> Self {
        let events = Arc::new(Events::default());
        config.startup_observer = Some(events.clone());
        let state = RuntimeState::try_new(
            std::path::PathBuf::from("startup-gate-memory"),
            true,
            crate::config::RecoveryPolicy::Strict,
        )
        .unwrap();
        let (runtime, _) = Runtime::new();
        let (runtime, handle, gate) = runtime.prepare_with_config(state, config, scope).unwrap();
        Self {
            runtime,
            handle,
            gate,
            events,
        }
    }
}

fn unbounded_scope() -> DeadlineScope {
    DeadlineScope::new(crate::common::OperationDeadline::unbounded())
}

#[test]
fn should_abort_actual_prepared_runtime_when_owner_drops_without_acceptance() {
    // Arrange
    let fixture = Fixture::prepare(unbounded_scope(), RuntimeConfig::default());
    assert_eq!(fixture.events.prepared.load(Ordering::Acquire), 1);
    assert!(!fixture.handle.lifecycle.running.load(Ordering::Acquire));

    // Act: Runtime owns a gate clone, so its Drop wakes a dormant worker.
    drop(fixture.runtime);

    // Assert
    assert_eq!(fixture.events.admitted.load(Ordering::Acquire), 0);
    assert_eq!(fixture.events.aborted.load(Ordering::Acquire), 1);
    assert_eq!(
        fixture.handle.lifecycle.state(),
        super::super::RuntimeLifecycleState::Closed
    );
}

#[test]
fn should_admit_actual_prepared_runtime_when_scope_and_authority_are_healthy() {
    crate::failpoints::with_read_gate(|| {
        // Arrange
        let scope = DeadlineScope::new(crate::common::OperationDeadline::from_budget(
            Duration::from_secs(5),
        ));
        let fixture = Fixture::prepare(scope.clone(), RuntimeConfig::default());

        // Act
        let accepted = fixture.gate.accept();
        let running = fixture.handle.lifecycle.running.load(Ordering::Acquire);
        fixture.gate.cancel();
        let completed_scope = !scope.deadline().is_bounded();
        drop(fixture.runtime);

        // Assert: a losing cancellation cannot expire the accepted lifetime view.
        accepted.unwrap();
        assert!(running);
        assert!(completed_scope);
        assert_eq!(fixture.events.admitted.load(Ordering::Acquire), 1);
        assert_eq!(fixture.events.aborted.load(Ordering::Acquire), 0);
    });
}

#[test]
fn should_reject_actual_prepared_runtime_when_outer_owner_cancels_shared_scope() {
    // Arrange
    let scope = unbounded_scope();
    let fixture = Fixture::prepare(scope.clone(), RuntimeConfig::default());

    // Act
    scope.cancel();
    let accepted = fixture.gate.accept();
    drop(fixture.runtime);

    // Assert
    assert!(matches!(accepted, Err(MidgeError::Timeout(_))));
    assert_eq!(fixture.events.admitted.load(Ordering::Acquire), 0);
    assert_eq!(fixture.events.aborted.load(Ordering::Acquire), 1);
}

#[test]
fn should_reject_actual_prepared_runtime_when_original_open_deadline_expires() {
    crate::failpoints::with_read_gate(|| {
        // Arrange
        let deadline = Instant::now() + Duration::from_millis(120);
        let scope = DeadlineScope::new(crate::common::OperationDeadline::from_budget(
            Duration::from_millis(120),
        ));
        let fixture = Fixture::prepare(scope, RuntimeConfig::default());

        // Act
        std::thread::sleep(
            deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(1),
        );
        let accepted = fixture.gate.accept();
        drop(fixture.runtime);

        // Assert
        assert!(matches!(accepted, Err(MidgeError::Timeout(_))));
        assert_eq!(fixture.events.admitted.load(Ordering::Acquire), 0);
        assert_eq!(fixture.events.aborted.load(Ordering::Acquire), 1);
    });
}

#[test]
fn should_fence_actual_prepared_runtime_when_heartbeat_health_is_lost_before_acceptance() {
    // Arrange
    let health = Arc::new(AtomicBool::new(true));
    let fixture = Fixture::prepare(
        unbounded_scope(),
        RuntimeConfig {
            lease_healthy: Some(Arc::clone(&health)),
            ..RuntimeConfig::default()
        },
    );

    // Act
    health.store(false, Ordering::Release);
    let accepted = fixture.gate.accept();
    drop(fixture.runtime);

    // Assert
    assert!(matches!(accepted, Err(MidgeError::Fenced(_))));
    assert_eq!(fixture.events.admitted.load(Ordering::Acquire), 0);
    assert_eq!(fixture.events.aborted.load(Ordering::Acquire), 1);
}

#[test]
fn should_fence_actual_prepared_runtime_when_monotonic_validity_expires_without_watchdog() {
    crate::failpoints::with_read_gate(|| {
        // Arrange
        let validity = Arc::new(crate::lease::LeaseValidity::new());
        let until = Instant::now() + Duration::from_millis(120);
        validity.activate(7, until).unwrap();
        let fixture = Fixture::prepare(
            unbounded_scope(),
            RuntimeConfig {
                writer_epoch: 7,
                lease_validity: Some(validity),
                lease_healthy: Some(Arc::new(AtomicBool::new(true))),
                ..RuntimeConfig::default()
            },
        );

        // Act: no watchdog or heartbeat is installed; acceptance must check validity.
        std::thread::sleep(until.saturating_duration_since(Instant::now()));
        let accepted = fixture.gate.accept();
        drop(fixture.runtime);

        // Assert
        assert!(matches!(accepted, Err(MidgeError::Fenced(_))));
        assert_eq!(fixture.events.admitted.load(Ordering::Acquire), 0);
        assert_eq!(fixture.events.aborted.load(Ordering::Acquire), 1);
    });
}

#[test]
fn should_choose_one_actual_runtime_admission_when_acceptance_races_cancellation() {
    // Arrange
    let fixture = Fixture::prepare(unbounded_scope(), RuntimeConfig::default());
    let reached = Arc::new(std::sync::Barrier::new(2));

    // Act
    let accepted = std::thread::scope(|threads| {
        let gate = fixture.gate.clone();
        let ready = Arc::clone(&reached);
        let accepting = threads.spawn(move || {
            ready.wait();
            gate.accept()
        });
        reached.wait();
        fixture.gate.cancel();
        accepting.join().unwrap()
    });
    drop(fixture.runtime);

    // Assert: actual OS-worker completion follows exactly the winning decision.
    match accepted {
        Ok(()) => {
            assert_eq!(fixture.events.admitted.load(Ordering::Acquire), 1);
            assert_eq!(fixture.events.aborted.load(Ordering::Acquire), 0);
        }
        Err(MidgeError::Timeout(_)) => {
            assert_eq!(fixture.events.admitted.load(Ordering::Acquire), 0);
            assert_eq!(fixture.events.aborted.load(Ordering::Acquire), 1);
        }
        Err(error) => panic!("unexpected admission race outcome: {error}"),
    }
}

struct ActiveWalFixture {
    directory: tempfile::TempDir,
    state: RuntimeState,
    config: RuntimeConfig,
    bytes: Vec<u8>,
    events: Arc<Events>,
}

impl ActiveWalFixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("database");
        let mut state = RuntimeState::new(database.clone(), false);
        std::fs::create_dir_all(database.join("wal")).unwrap();
        let record = crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            bytes::Bytes::from_static(b"recovered-key"),
            Some(bytes::Bytes::from_static(b"acknowledged-value")),
            1,
            0,
        );
        let payload = crate::wal::encoding::encode(&record).unwrap();
        let mut bytes = Vec::new();
        crate::wal::frame::append_frame(&mut bytes, &payload).unwrap();
        std::fs::write(
            database.join("wal").join(crate::wal::ACTIVE_FILE_NAME),
            &bytes,
        )
        .unwrap();
        state.sequence = 1;
        state.wal.current_segment_id = 1;
        let local = Arc::new(
            crate::storage::filesystem::FileSystem::new(directory.path().join("hybrid-local"))
                .unwrap(),
        );
        let cloud = Arc::new(
            crate::storage::filesystem::FileSystem::new(directory.path().join("cloud-store"))
                .unwrap(),
        );
        let storage = Arc::new(crate::storage::HybridStorage::with_test_upload_limits(
            local,
            cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
            1,
            bytes.len() as u64,
        ));
        crate::runtime::hybrid_persistence::CloudPersistence::new(Arc::clone(&storage))
            .fence_cloud_wal_catalog(1)
            .unwrap();
        let events = Arc::new(Events::default());
        let config = RuntimeConfig {
            wal_durability_policy: crate::wal::DurabilityPolicy::CloudAsync,
            hybrid_storage: Some(storage),
            recovered_cloud_active_wal: Some(crate::runtime::RecoveredCloudActiveWal {
                max_sequence: 1,
                writer_epoch: 0,
                record_count: 1,
                valid_bytes: bytes.len(),
            }),
            writer_epoch: 1,
            startup_observer: Some(events.clone()),
            ..RuntimeConfig::default()
        };
        Self {
            directory,
            state,
            config,
            bytes,
            events,
        }
    }
}

#[test]
fn should_seal_actual_recovered_active_wal_when_short_open_budget_precedes_larger_io_cap() {
    crate::failpoints::with_read_gate(|| {
        // Arrange: genuine recovered active bytes require fsync and rotation at
        // startup. Fast local I/O fits500ms, while the normal I/O cap remains30s.
        let fixture = ActiveWalFixture::new();
        let budget = Duration::from_millis(500);
        assert!(fixture.config.storage_io_timeout > budget);
        let scope = DeadlineScope::new(crate::common::OperationDeadline::from_budget(budget));
        let (runtime, _) = Runtime::new();

        // Act: always release/join an actual prepared worker before asserting.
        let result = runtime.prepare_with_config(fixture.state, fixture.config, scope);
        let sealed_bytes = std::fs::read(
            fixture
                .directory
                .path()
                .join("database/wal")
                .join(crate::wal::segment_file_name(1)),
        )
        .ok();
        let prepared = result.map(|(runtime, _, gate)| {
            drop(runtime);
            drop(gate);
        });

        // Assert: a larger I/O cap must not reject individually fast mandatory work.
        prepared.unwrap();
        assert_eq!(sealed_bytes, Some(fixture.bytes));
        assert_eq!(fixture.events.prepared.load(Ordering::Acquire), 1);
        assert_eq!(fixture.events.admitted.load(Ordering::Acquire), 0);
        assert_eq!(fixture.events.aborted.load(Ordering::Acquire), 1);
    });
}

struct HeldStartupAuthority(Arc<AtomicUsize>);

impl crate::lease::LeaderStore for HeldStartupAuthority {
    fn acquire_leadership(
        &self,
        _: &str,
    ) -> Result<crate::lease::LeaderRecord, crate::lease::LeaseError> {
        Err(crate::lease::LeaseError::Internal(
            "unused fixture acquisition".into(),
        ))
    }

    fn read_current(&self) -> Result<Option<crate::lease::LeaderRecord>, crate::lease::LeaseError> {
        Ok(Some(crate::lease::LeaderRecord {
            epoch: 1,
            holder_id: "startup-owner".into(),
            acquired_at: "2026-10-04T00:00:00Z".into(),
        }))
    }

    fn validate_epoch_with_timeout(
        &self,
        _: &str,
        _: u64,
        timeout: Duration,
    ) -> Result<(), crate::lease::LeaseError> {
        self.0.fetch_add(1, Ordering::AcqRel);
        std::thread::sleep(timeout);
        Err(crate::lease::LeaseError::Timeout(
            "held startup authority read".into(),
        ))
    }
}

#[test]
fn should_retain_actual_recovered_active_wal_when_startup_authority_budget_expires() {
    crate::failpoints::with_read_gate(|| {
        // Arrange: hold the actual startup sealing authority call. The timeout
        // occurs before fsync/rotation; the same acknowledged bytes remain recoverable.
        let mut fixture = ActiveWalFixture::new();
        let validations = Arc::new(AtomicUsize::new(0));
        fixture.config.leader_store =
            Some(Arc::new(HeldStartupAuthority(Arc::clone(&validations))));
        fixture.config.leader_holder_id = Some("startup-owner".into());
        let scope = DeadlineScope::new(crate::common::OperationDeadline::from_budget(
            Duration::from_millis(80),
        ));
        let (runtime, _) = Runtime::new();

        // Act: failed preparation drops/joins its real runtime worker before return.
        let result = runtime.prepare_with_config(fixture.state, fixture.config, scope);
        let timeout = match result {
            Ok((runtime, _, gate)) => {
                drop(runtime);
                drop(gate);
                false
            }
            Err(MidgeError::Timeout(_)) => true,
            Err(error) => panic!("startup authority timeout lost its type: {error}"),
        };

        // Assert: no admission, no identity rotation and no loss of acknowledged bytes.
        assert!(timeout);
        assert_eq!(validations.load(Ordering::Acquire), 1);
        assert_eq!(
            std::fs::read(
                fixture
                    .directory
                    .path()
                    .join("database/wal")
                    .join(crate::wal::ACTIVE_FILE_NAME)
            )
            .unwrap(),
            fixture.bytes
        );
        assert!(!fixture
            .directory
            .path()
            .join("database/wal")
            .join(crate::wal::segment_file_name(1))
            .exists());
        assert_eq!(fixture.events.prepared.load(Ordering::Acquire), 0);
        assert_eq!(fixture.events.admitted.load(Ordering::Acquire), 0);
        assert_eq!(fixture.events.aborted.load(Ordering::Acquire), 1);
    });
}
