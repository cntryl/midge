//! Exact readback waits for an existing physical completion adapter to finish.

use super::*;
use crate::common::resource_budget::ResourceBudget;
use crate::storage::cloud::BlockingCloud;

struct HeldAdapterTail {
    entered: mpsc::Sender<()>,
    released: mpsc::Receiver<()>,
}

impl Drop for HeldAdapterTail {
    fn drop(&mut self) {
        self.entered.send(()).unwrap();
        self.released
            .recv_timeout(Duration::from_secs(5))
            .expect("release the real completion adapter tail");
    }
}

pub(super) fn wait_for_charge(
    memory: &ResourceBudget,
    expected: usize,
    deadline: &OperationDeadline,
) {
    while memory.used() != expected && !deadline.is_expired() {
        std::thread::yield_now();
    }
    assert_eq!(memory.used(), expected);
}

#[test]
fn should_complete_exact_readback_when_a_prior_physical_adapter_releases_its_charge() {
    crate::failpoints::with_read_gate(|| {
        // Arrange: a genuine immutable object and source in the original pool.
        let backend = Arc::new(MockCloudBackend::new());
        let fixture = Fixture::new(CloudStorage::new(backend.clone(), String::new()));
        let memory = ResourceBudget::new(128 * 1024);
        fixture.lock.configure_capture_memory(memory.clone());
        let cut = fixture.cut();
        let source_charge = memory.used();
        let key = "manifest/adapter-readback";
        let body = b"actual immutable body";
        let (seeded, seed) = mpsc::channel();
        backend.submit_put(key, body.to_vec(), Vec::new(), seeded);
        assert!(matches!(
            seed.recv().unwrap(),
            crate::storage::cloud::CloudEvent::Put { result: Ok(()), .. }
        ));

        // A closed logical receiver makes the real generic adapter drop this
        // event after forwarding. Its destructor holds that actual stack tail.
        let (forwarded, abandoned) = mpsc::channel();
        drop(abandoned);
        let retained =
            crate::storage::retained_callback::retain(forwarded, cut.memory_reservation()).unwrap();
        let (entered, holding) = mpsc::channel();
        let (release, released) = mpsc::channel();
        retained
            .send(HeldAdapterTail { entered, released })
            .unwrap_or_else(|_| panic!("deliver the physical completion"));
        holding.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(memory.used(), source_charge + 64 * 1024);
        let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
        let cloud = Arc::clone(&fixture.cloud);
        let reservation = cut.memory_reservation();
        let (finished, completion) = mpsc::channel();

        // Act: the next read cannot admit another stack until that tail exits.
        let (premature, reads_before_release, remaining_at_release, result) =
            std::thread::scope(|scope| {
                let read = scope.spawn(move || {
                    let result = BlockingCloud::new(&cloud, &deadline)
                        .with_reservation(reservation)
                        .get_optional(key);
                    let _ = finished.send(());
                    result
                });
                let premature = completion.recv_timeout(Duration::from_millis(50));
                let reads = backend.get_downloads();
                assert_eq!(memory.used(), source_charge + 64 * 1024);
                let remaining = deadline.remaining();
                release.send(()).unwrap();
                (premature, reads, remaining, read.join().unwrap())
            });
        wait_for_charge(&memory, source_charge, &deadline);

        // Assert: only the admitted GET ran; no upload or capacity was added.
        assert_eq!(result.unwrap(), Some(body.to_vec()));
        assert!(matches!(premature, Err(mpsc::RecvTimeoutError::Timeout)));
        assert_eq!(reads_before_release, Vec::<String>::new());
        assert_eq!(backend.get_downloads(), [key]);
        let timeouts = backend.get_request_timeouts();
        assert_eq!(timeouts.len(), 1);
        assert!(!timeouts[0].is_zero() && timeouts[0] <= remaining_at_release);
        assert_eq!(backend.get_uploads().len(), 1);
        assert_eq!(fixture.lock.pending_captures(), 1);
        drop(cut);
        // An opaque GET cannot select this metadata capture. Its pending
        // registry still owns the source until this fixture shuts down.
        assert_eq!(memory.used(), source_charge);
        drop(fixture);
        wait_for_charge(&memory, 0, &deadline);
    });
}

#[test]
fn should_refuse_unsubmitted_read_when_its_adapter_exceeds_the_entire_pool() {
    crate::failpoints::with_read_gate(|| {
        // Arrange: one source fits, but the required physical adapter cannot.
        let backend = Arc::new(MockCloudBackend::new());
        let fixture = Fixture::new(CloudStorage::new(backend.clone(), String::new()));
        let memory = ResourceBudget::new(32 * 1024);
        fixture.lock.configure_capture_memory(memory.clone());
        let cut = fixture.cut();
        let source = cut.memory_reservation();
        let deadline = OperationDeadline::from_budget(Duration::from_secs(1));

        // Act
        let error = BlockingCloud::new(&fixture.cloud, &deadline)
            .with_reservation(Arc::clone(&source))
            .get_optional("manifest/not-submitted")
            .unwrap_err();

        // Assert: static oversize is terminal and no provider request ran.
        assert!(matches!(error, MidgeError::ResourceLimit(_)));
        assert!(!source.admission_retryable(&error));
        assert_eq!(backend.get_downloads(), Vec::<String>::new());
        assert_eq!(backend.get_request_timeouts(), Vec::<Duration>::new());
        assert_eq!(fixture.lock.pending_captures(), 1);
        assert_eq!(memory.used(), source.bytes());
    });
}

#[test]
fn should_end_unsubmitted_read_when_original_admission_budget_expires() {
    crate::failpoints::with_read_gate(|| {
        // Arrange: a live shared-pool owner keeps the next adapter inadmissible.
        let backend = Arc::new(MockCloudBackend::new());
        let fixture = Fixture::new(CloudStorage::new(backend.clone(), String::new()));
        let memory = ResourceBudget::new(128 * 1024);
        fixture.lock.configure_capture_memory(memory.clone());
        let cut = fixture.cut();
        let source = cut.memory_reservation();
        let held = source
            .reserve_related(64 * 1024, "another admitted maintenance owner")
            .unwrap();
        let deadline = OperationDeadline::from_budget(Duration::from_millis(50));

        // Act
        let error = BlockingCloud::new(&fixture.cloud, &deadline)
            .with_reservation(Arc::clone(&source))
            .get_optional("manifest/not-submitted")
            .unwrap_err();

        // Assert: admission cannot renew the original budget or release that owner.
        assert!(matches!(error, MidgeError::Timeout(_)));
        assert!(deadline.is_expired());
        assert_eq!(backend.get_downloads(), Vec::<String>::new());
        assert_eq!(backend.get_request_timeouts(), Vec::<Duration>::new());
        assert_eq!(fixture.lock.pending_captures(), 1);
        assert_eq!(memory.used(), source.bytes() + held.bytes());
    });
}

fn contextualize_local_admission(error: MidgeError) -> MidgeError {
    let MidgeError::ResourceLimit(message) = error else {
        panic!("resource admission must report its own typed error");
    };
    crate::storage::cloud::contextualize_operation_error(
        &crate::storage::cloud::CloudError::LocalReadAdmission(message),
        "unsubmitted read",
        &OperationDeadline::from_budget(Duration::from_secs(1)),
    )
}

#[test]
fn should_preserve_original_admission_scope_when_cloud_error_context_is_added() {
    // Arrange: two separate pool identities can produce identical capacity failures.
    let own = ResourceBudget::new(100).with_contention_errors();
    let foreign = ResourceBudget::new(100).with_contention_errors();
    let source = own.reserve(10, "owned read source").unwrap();
    let held = own.reserve(90, "earlier physical callback").unwrap();
    let foreign_held = foreign.reserve(100, "unrelated owner").unwrap();
    let own_error = own.reserve(1, "next read adapter").unwrap_err();
    let foreign_error = foreign.reserve(1, "foreign adapter").unwrap_err();
    drop((held, foreign_held));

    // Act: the owners can finish between refusal and classification.
    let scoped = contextualize_local_admission(own_error);
    let unrelated = contextualize_local_admission(foreign_error);
    let protocol = crate::storage::cloud::contextualize_operation_error(
        &crate::storage::cloud::CloudError::Protocol(scoped.to_string()),
        "provider protocol failure",
        &OperationDeadline::from_budget(Duration::from_secs(1)),
    );

    // Assert: only the typed refusal from the source's actual pool may retry.
    assert!(source.admission_retryable(&scoped));
    assert!(!source.admission_retryable(&unrelated));
    assert!(matches!(protocol, MidgeError::Internal(_)));
    assert!(!source.admission_retryable(&protocol));
    assert_eq!(own.used(), 10);
    assert_eq!(foreign.used(), 0);
}
