//! Production filesystem authority lifetimes, including queue and crash edges.

use super::*;
use crate::common::{DeadlineScope, OperationDeadline};
use crate::io::MockFs;
use crate::lease::scoped_leader_store;
use std::sync::mpsc;

const BUDGET: Duration = Duration::from_secs(2);

fn owner_store() -> (Arc<FsLeaderStore>, u64) {
    let store = Arc::new(FsLeaderStore::new(
        Arc::new(MockFs::new()),
        super::super::LeaseClock::default(),
    ));
    let epoch = store.acquire_leadership("writer").unwrap().epoch;
    (store, epoch)
}

#[test]
fn should_finish_descendant_jobs_when_the_creating_root_has_left() {
    // Arrange: a strict operation joins an unrelated active physical cohort.
    let (store, epoch) = owner_store();
    let creator = store.begin_group("writer", epoch, BUDGET).unwrap();
    let participant = store.begin_group("writer", epoch, BUDGET).unwrap();
    drop(creator);

    // Act: reserve a later worker phase after creator closure, then run it on
    // a fresh thread. Thread identity cannot grant this continuation.
    let ticket = participant.reserve_ticket(BUDGET).unwrap();
    let worker_store = Arc::clone(&store);
    let worker = std::thread::spawn(move || {
        let active = ticket.activate().unwrap();
        let mut ran = false;
        worker_store
            .run_active("writer", epoch, &active, BUDGET, &mut || ran = true)
            .unwrap();
        ran
    });
    let independent_store = Arc::clone(&store);
    let independent = std::thread::spawn(move || {
        matches!(
            independent_store.begin_group("writer", epoch, Duration::from_millis(10)),
            Err(LeaseError::Timeout(_))
        )
    })
    .join()
    .unwrap();

    // Assert
    assert!(worker.join().unwrap());
    assert!(independent);
    drop(participant);
    store.begin_group("writer", epoch, BUDGET).unwrap();
}

#[test]
fn should_keep_transition_fenced_when_an_activated_job_outlives_its_root() {
    // Arrange: root cancellation/drop cannot release a queued or running job.
    let (store, epoch) = owner_store();
    let root = store.begin_group("writer", epoch, BUDGET).unwrap();
    let active = root.reserve_ticket(BUDGET).unwrap().activate().unwrap();
    drop(root);

    // Act
    let release_store = Arc::clone(&store);
    let release = std::thread::spawn(move || {
        release_store
            .scoped_for_timeout(Duration::from_millis(10))
            .unwrap()
            .release_if_owner("writer", epoch)
    })
    .join()
    .unwrap();
    let mut ran = false;
    store
        .run_active("writer", epoch, &active, BUDGET, &mut || ran = true)
        .unwrap();
    drop(active);
    let released = store.release_if_owner("writer", epoch).unwrap();

    // Assert
    assert!(matches!(release, Err(LeaseError::Timeout(_))));
    assert!(ran);
    assert!(released);
}

#[test]
fn should_release_a_queued_job_fence_when_the_job_is_discarded() {
    // Arrange
    let (store, epoch) = owner_store();
    let root = store.begin_group("writer", epoch, BUDGET).unwrap();
    let ticket = root.reserve_ticket(BUDGET).unwrap();
    drop(root);

    // Act
    let blocked = store
        .scoped_for_timeout(Duration::from_millis(10))
        .unwrap()
        .release_if_owner("writer", epoch);
    drop(ticket);
    let released = store.release_if_owner("writer", epoch).unwrap();

    // Assert
    assert!(matches!(blocked, Err(LeaseError::Timeout(_))));
    assert!(released);
}

#[test]
fn should_reject_a_job_before_effect_when_its_authority_does_not_match() {
    // Arrange
    let (store, epoch) = owner_store();
    let root = store.begin_group("writer", epoch, BUDGET).unwrap();
    let active = root.reserve_ticket(BUDGET).unwrap().activate().unwrap();
    let different_store = FsLeaderStore::new(
        Arc::clone(&store.cleanup_fs),
        super::super::LeaseClock::default(),
    );
    let mut calls = 0;

    // Act
    let wrong_store =
        different_store.run_active("writer", epoch, &active, BUDGET, &mut || calls += 1);
    let wrong_holder = store.run_active("other", epoch, &active, BUDGET, &mut || calls += 1);
    let wrong_epoch = store.run_active("writer", epoch + 1, &active, BUDGET, &mut || calls += 1);

    // Assert
    assert!(matches!(wrong_store, Err(LeaseError::RenewalFailed(_))));
    assert!(matches!(wrong_holder, Err(LeaseError::RenewalFailed(_))));
    assert!(matches!(wrong_epoch, Err(LeaseError::RenewalFailed(_))));
    assert_eq!(calls, 0);
}

#[test]
fn should_release_the_native_fence_when_a_writer_callback_panics() {
    // Arrange
    let (store, epoch) = owner_store();

    // Act
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store
            .with_writer_epoch("writer", epoch, BUDGET, &mut || {
                panic!("physical effect panic")
            })
            .unwrap();
    }));
    let successor = store.acquire_leadership("successor").unwrap();

    // Assert
    assert!(panic.is_err());
    assert!(successor.epoch > epoch);
}

#[test]
fn should_keep_each_physical_budget_fresh_when_a_group_spans_several_stages() {
    // Arrange: only the proof acquisition is bounded by this short timeout.
    let (store, epoch) = owner_store();
    let root = store
        .begin_group("writer", epoch, Duration::from_millis(20))
        .unwrap();
    let active = root.reserve_ticket(BUDGET).unwrap().activate().unwrap();
    let mut stages = 0;
    store
        .run_active("writer", epoch, &active, BUDGET, &mut || stages += 1)
        .unwrap();

    // Act: an admitted logical operation can span more than one I/O budget.
    std::thread::sleep(Duration::from_millis(30));
    store
        .run_active("writer", epoch, &active, BUDGET, &mut || stages += 1)
        .unwrap();

    // Assert
    assert_eq!(stages, 2);
}

#[test]
fn should_share_one_fence_when_scoped_and_original_authorities_overlap() {
    // Arrange
    let (store, epoch) = owner_store();
    let scope = DeadlineScope::new(OperationDeadline::unbounded());
    let scoped = scoped_leader_store(store.clone() as Arc<dyn LeaderStore>, scope, BUDGET);
    let root = store.begin_group("writer", epoch, BUDGET).unwrap();

    // Act: the wrapper delegates to the same coordinator rather than blocking
    // on its own native lock or minting an independent fencing proof.
    let wrapped = scoped
        .writer_effect_groups()
        .unwrap()
        .begin_group("writer", epoch, BUDGET)
        .unwrap();
    let active = wrapped.reserve_ticket(BUDGET).unwrap().activate().unwrap();
    let mut ran = false;
    store
        .run_active("writer", epoch, &active, BUDGET, &mut || ran = true)
        .unwrap();
    drop(root);

    // Assert
    assert!(ran);
}

#[test]
fn should_retain_the_incoming_scope_when_a_queued_job_is_cancelled() {
    // Arrange
    let (store, epoch) = owner_store();
    let scope = DeadlineScope::new(OperationDeadline::unbounded());
    let scoped = scoped_leader_store(store.clone() as Arc<dyn LeaderStore>, scope.clone(), BUDGET);
    let root = scoped
        .writer_effect_groups()
        .unwrap()
        .begin_group("writer", epoch, BUDGET)
        .unwrap();
    let ticket = root.reserve_ticket(BUDGET).unwrap();
    let sibling = store.begin_group("writer", epoch, BUDGET).unwrap();

    // Act: cancellation rejects this queued job but cannot revoke its sibling.
    scope.cancel();
    let activation = ticket.activate();
    let active = sibling.reserve_ticket(BUDGET).unwrap().activate().unwrap();
    let mut ran = false;
    store
        .run_active("writer", epoch, &active, BUDGET, &mut || ran = true)
        .unwrap();

    // Assert
    assert!(matches!(activation, Err(LeaseError::Timeout(_))));
    assert!(ran);
}

#[test]
fn should_reject_cached_authority_when_a_process_crash_revokes_the_native_guard() {
    // Arrange
    let device = crate::simulation::crash_fs::CrashDevice::new();
    let store = FsLeaderStore::new(Arc::new(device.fs()), super::super::LeaseClock::default());
    let epoch = store.acquire_leadership("writer").unwrap().epoch;
    let root = store.begin_group("writer", epoch, BUDGET).unwrap();
    let active = root.reserve_ticket(BUDGET).unwrap().activate().unwrap();

    // Act
    device.process_crash();
    let successor_store =
        FsLeaderStore::new(Arc::new(device.fs()), super::super::LeaseClock::default());
    let successor = successor_store.acquire_leadership("successor").unwrap();
    let next_root = successor_store
        .begin_group("successor", successor.epoch, BUDGET)
        .unwrap();
    let mut stale_ran = false;
    let stale = store.run_active("writer", epoch, &active, BUDGET, &mut || stale_ran = true);
    drop(active);
    drop(root);
    let next_active = next_root
        .reserve_ticket(BUDGET)
        .unwrap()
        .activate()
        .unwrap();
    let mut successor_ran = false;
    successor_store
        .run_active(
            "successor",
            successor.epoch,
            &next_active,
            BUDGET,
            &mut || successor_ran = true,
        )
        .unwrap();

    // Assert: stale Rust handles cannot authorize effects or unlock a new peer.
    assert!(matches!(stale, Err(LeaseError::RenewalFailed(_))));
    assert!(!stale_ran);
    assert!(successor_ran);
}

#[test]
fn should_reject_a_nested_transition_when_its_callback_holds_the_fence() {
    // Arrange
    let (store, epoch) = owner_store();
    let mut release = None;

    // Act
    store
        .with_writer_epoch("writer", epoch, BUDGET, &mut || {
            release = Some(store.release_if_owner("writer", epoch));
        })
        .unwrap();

    // Assert
    assert!(matches!(release, Some(Err(LeaseError::RenewalFailed(_)))));
    assert!(store.release_if_owner("writer", epoch).unwrap());
}

#[test]
fn should_allow_takeover_when_the_last_logical_root_has_finished() {
    // Arrange: a living old process retains only its store, never idle proof.
    let (store, epoch) = owner_store();
    let root = store.begin_group("writer", epoch, BUDGET).unwrap();
    drop(root);

    // Act
    let successor = store.acquire_leadership("successor").unwrap();
    let old_root = store.begin_group("writer", epoch, BUDGET);

    // Assert
    assert!(successor.epoch > epoch);
    assert!(matches!(old_root, Err(LeaseError::RenewalFailed(_))));
}

#[test]
fn should_keep_a_transition_waiting_until_a_delayed_worker_discards_its_ticket() {
    // Arrange
    let (store, epoch) = owner_store();
    let root = store.begin_group("writer", epoch, BUDGET).unwrap();
    let ticket = root.reserve_ticket(BUDGET).unwrap();
    drop(root);
    let (finished, finishing) = mpsc::sync_channel(1);
    let transitioning = Arc::clone(&store);
    let transition = std::thread::spawn(move || {
        finished
            .send(transitioning.release_if_owner("writer", epoch))
            .unwrap();
    });
    store.writer_effects.wait_for_transition_waiters(1);

    // Act
    let early = finishing.recv_timeout(Duration::from_millis(20));
    drop(ticket);
    transition.join().unwrap();
    let released = finishing.recv().unwrap().unwrap();

    // Assert
    assert!(early.is_err());
    assert!(released);
}

struct PanicAdmissionFs {
    inner: MockFs,
    panic_once: std::sync::atomic::AtomicBool,
    panic_on_release: Arc<std::sync::atomic::AtomicBool>,
    validation_gate: Option<Arc<ValidationGate>>,
}

struct ValidationGate {
    block_next: std::sync::atomic::AtomicBool,
    entered: mpsc::SyncSender<()>,
    resume: parking_lot::Mutex<mpsc::Receiver<()>>,
}

struct PanicReleaseLock {
    native: Option<Box<dyn FsLock>>,
    panic_once: Arc<std::sync::atomic::AtomicBool>,
    validation_gate: Option<Arc<ValidationGate>>,
}

impl FsLock for PanicReleaseLock {
    fn validate_held(&self) -> crate::io::FsResult<()> {
        if let Some(gate) = &self.validation_gate {
            if gate
                .block_next
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                gate.entered.send(()).unwrap();
                gate.resume.lock().recv().unwrap();
            }
        }
        self.native.as_ref().unwrap().validate_held()
    }
}

impl Drop for PanicReleaseLock {
    fn drop(&mut self) {
        drop(self.native.take());
        if self
            .panic_once
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            panic!("backend panic after native unlock");
        }
    }
}

impl crate::io::ReadFs for PanicAdmissionFs {
    fn open_read(&self, path: &FsPath) -> crate::io::FsResult<Box<dyn crate::io::ReadFile + '_>> {
        self.inner.open_read(path)
    }
    fn exists(&self, path: &FsPath) -> crate::io::FsResult<bool> {
        self.inner.exists(path)
    }
    fn metadata(&self, path: &FsPath) -> crate::io::FsResult<crate::io::traits::Metadata> {
        self.inner.metadata(path)
    }
    fn list_dir(&self, path: &FsPath) -> crate::io::FsResult<Vec<crate::io::traits::DirEntry>> {
        self.inner.list_dir(path)
    }
}

impl Fs for PanicAdmissionFs {
    fn try_lock_exclusive(&self, path: &FsPath) -> crate::io::FsResult<Box<dyn FsLock>> {
        let lock = self.inner.try_lock_exclusive(path)?;
        if self
            .panic_once
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            panic!("backend panic after native acquisition");
        }
        Ok(Box::new(PanicReleaseLock {
            native: Some(lock),
            panic_once: Arc::clone(&self.panic_on_release),
            validation_gate: self.validation_gate.clone(),
        }))
    }
    fn open(
        &self,
        path: &FsPath,
        options: OpenOptions,
    ) -> crate::io::FsResult<Box<dyn crate::io::File + '_>> {
        self.inner.open(path, options)
    }
    fn create_dir_all(&self, path: &FsPath) -> crate::io::FsResult<()> {
        self.inner.create_dir_all(path)
    }
    fn remove_file(&self, path: &FsPath) -> crate::io::FsResult<()> {
        self.inner.remove_file(path)
    }
    fn remove_dir_all(&self, path: &FsPath) -> crate::io::FsResult<()> {
        self.inner.remove_dir_all(path)
    }
    fn rename_atomic(&self, source: &FsPath, target: &FsPath) -> crate::io::FsResult<()> {
        self.inner.rename_atomic(source, target)
    }
    fn sync_dir(&self, path: &FsPath, durability: Durability) -> crate::io::FsResult<()> {
        self.inner.sync_dir(path, durability)
    }
}

#[test]
fn should_recover_admission_when_the_backend_panics_after_native_lock_acquisition() {
    // Arrange
    let fs = Arc::new(PanicAdmissionFs {
        inner: MockFs::new(),
        panic_once: std::sync::atomic::AtomicBool::new(false),
        panic_on_release: Arc::default(),
        validation_gate: None,
    });
    let store = FsLeaderStore::new(fs.clone(), super::super::LeaseClock::default());
    let epoch = store.acquire_leadership("writer").unwrap().epoch;
    fs.panic_once
        .store(true, std::sync::atomic::Ordering::Release);

    // Act
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store.begin_group("writer", epoch, BUDGET).unwrap();
    }));
    let root = store.begin_group("writer", epoch, BUDGET).unwrap();
    let active = root.reserve_ticket(BUDGET).unwrap().activate().unwrap();
    let mut ran = false;
    store
        .run_active("writer", epoch, &active, BUDGET, &mut || ran = true)
        .unwrap();

    // Assert
    assert!(panic.is_err());
    assert!(ran);
}

#[test]
fn should_reject_a_transition_when_the_same_thread_retains_a_logical_root() {
    // Arrange: no physical callback is active between the strict operation's stages.
    let (store, epoch) = owner_store();
    let root = store.begin_group("writer", epoch, BUDGET).unwrap();

    // Act
    let released = store.release_if_owner("writer", epoch);
    drop(root);
    let next_release = store.release_if_owner("writer", epoch).unwrap();

    // Assert
    assert!(matches!(released, Err(LeaseError::RenewalFailed(_))));
    assert!(next_release);
}

#[test]
fn should_reject_a_transition_when_the_same_thread_retains_an_active_job() {
    // Arrange: the worker retains authority between append and rollback/sync.
    let (store, epoch) = owner_store();
    let root = store.begin_group("writer", epoch, BUDGET).unwrap();
    let active = root.reserve_ticket(BUDGET).unwrap().activate().unwrap();
    drop(root);

    // Act
    let released = store.release_if_owner("writer", epoch);
    drop(active);
    let next_release = store.release_if_owner("writer", epoch).unwrap();

    // Assert
    assert!(matches!(released, Err(LeaseError::RenewalFailed(_))));
    assert!(next_release);
}

#[test]
fn should_reject_a_physical_stage_when_the_original_caller_deadline_expires() {
    // Arrange
    let (store, epoch) = owner_store();
    let scope = DeadlineScope::new(OperationDeadline::from_budget(Duration::from_millis(20)));
    let scoped = scoped_leader_store(store.clone() as Arc<dyn LeaderStore>, scope, BUDGET);
    let root = scoped
        .writer_effect_groups()
        .unwrap()
        .begin_group("writer", epoch, BUDGET)
        .unwrap();
    let active = root.reserve_ticket(BUDGET).unwrap().activate().unwrap();

    // Act: fresh physical budgets must still respect a genuine caller lifetime.
    std::thread::sleep(Duration::from_millis(30));
    let mut ran = false;
    let stage = store.run_active("writer", epoch, &active, BUDGET, &mut || ran = true);

    // Assert
    assert!(matches!(stage, Err(LeaseError::Timeout(_))));
    assert!(!ran);
}

#[test]
fn should_recover_release_when_the_backend_panics_after_native_unlock() {
    // Arrange
    let fs = Arc::new(PanicAdmissionFs {
        inner: MockFs::new(),
        panic_once: std::sync::atomic::AtomicBool::new(false),
        panic_on_release: Arc::default(),
        validation_gate: None,
    });
    let store = FsLeaderStore::new(fs.clone(), super::super::LeaseClock::default());
    let epoch = store.acquire_leadership("writer").unwrap().epoch;
    let root = store.begin_group("writer", epoch, BUDGET).unwrap();
    fs.panic_on_release
        .store(true, std::sync::atomic::Ordering::Release);

    // Act: the cached proof is already gone before an external guard destructor
    // runs. Unwinding must allow a fresh native acquisition and proof afterward.
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(root)));
    let next = store.begin_group("writer", epoch, BUDGET).unwrap();
    let active = next.reserve_ticket(BUDGET).unwrap().activate().unwrap();
    let mut ran = false;
    store
        .run_active("writer", epoch, &active, BUDGET, &mut || ran = true)
        .unwrap();

    // Assert
    assert!(panic.is_err());
    assert!(ran);
}

#[test]
fn should_skip_physical_effect_when_cancellation_arrives_during_native_guard_validation() {
    // Arrange
    let (entered, entering) = mpsc::sync_channel(1);
    let (resume, resuming) = mpsc::sync_channel(1);
    let gate = Arc::new(ValidationGate {
        block_next: std::sync::atomic::AtomicBool::new(false),
        entered,
        resume: parking_lot::Mutex::new(resuming),
    });
    let fs = Arc::new(PanicAdmissionFs {
        inner: MockFs::new(),
        panic_once: std::sync::atomic::AtomicBool::new(false),
        panic_on_release: Arc::default(),
        validation_gate: Some(Arc::clone(&gate)),
    });
    let store = Arc::new(FsLeaderStore::new(fs, super::super::LeaseClock::default()));
    let epoch = store.acquire_leadership("writer").unwrap().epoch;
    let scope = DeadlineScope::new(OperationDeadline::unbounded());
    let scoped = scoped_leader_store(store.clone() as Arc<dyn LeaderStore>, scope.clone(), BUDGET);
    let root = scoped
        .writer_effect_groups()
        .unwrap()
        .begin_group("writer", epoch, BUDGET)
        .unwrap();
    let ticket = root.reserve_ticket(BUDGET).unwrap();
    let (activated, activating) = mpsc::sync_channel(1);
    let (start, starting) = mpsc::sync_channel(1);
    let worker_store = Arc::clone(&store);
    let worker = std::thread::spawn(move || {
        let active = ticket.activate().unwrap();
        activated.send(()).unwrap();
        starting.recv().unwrap();
        let mut ran = false;
        let result = worker_store.run_active("writer", epoch, &active, BUDGET, &mut || ran = true);
        (result, ran)
    });
    activating.recv().unwrap();
    gate.block_next
        .store(true, std::sync::atomic::Ordering::Release);

    // Act: cancellation happens inside the final proof check, before callback admission.
    start.send(()).unwrap();
    entering.recv().unwrap();
    scope.cancel();
    resume.send(()).unwrap();
    let (result, ran) = worker.join().unwrap();

    // Assert
    assert!(matches!(result, Err(LeaseError::Timeout(_))));
    assert!(!ran);
}
