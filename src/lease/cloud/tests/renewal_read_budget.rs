use super::*;
use crate::lease::heartbeat::LeaseHeartbeat;
use crate::storage::cloud::{
    CloudBackend, CloudCallback, CloudEvent, CloudOutcome, MockCloudBackend,
};
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc;

#[derive(Debug)]
enum RenewalOutcome {
    Committed,
    Fenced,
}

struct HeldRenewalReadBackend {
    inner: MockCloudBackend,
    armed: AtomicBool,
    held_callback: Mutex<Option<CloudCallback>>,
    first_read_seen: Mutex<Option<mpsc::Sender<()>>>,
    renewal_outcome: Mutex<Option<mpsc::Sender<RenewalOutcome>>>,
    renewal_gets: AtomicUsize,
    renewal_puts: AtomicUsize,
}

impl CloudBackend for HeldRenewalReadBackend {
    fn submit_get_with_metadata(&self, key: &str, callback: CloudCallback) {
        if key.ends_with(LEASE_OBJECT_KEY) && self.armed.swap(false, Ordering::AcqRel) {
            self.renewal_gets.fetch_add(1, Ordering::AcqRel);
            *self.held_callback.lock().unwrap() = Some(callback);
            if let Some(seen) = self.first_read_seen.lock().unwrap().take() {
                let _ = seen.send(());
            }
            return;
        }
        if key.ends_with(LEASE_OBJECT_KEY) && self.held_callback.lock().unwrap().is_some() {
            self.renewal_gets.fetch_add(1, Ordering::AcqRel);
        }
        self.inner.submit_get_with_metadata(key, callback);
    }

    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        let renewal = key.ends_with(LEASE_OBJECT_KEY)
            && headers
                .iter()
                .any(|(name, _)| name.eq_ignore_ascii_case("if-match"))
            && self.held_callback.lock().unwrap().is_some();
        if !renewal {
            self.inner.submit_put(key, data, headers, callback);
            return;
        }
        let (sender, receiver) = mpsc::channel();
        self.inner.submit_put(key, data, headers, sender);
        let event = receiver
            .recv()
            .expect("mock conditional renewal completion");
        let committed = matches!(
            &event,
            CloudEvent::Put {
                result: CloudOutcome::Ok(()),
                ..
            }
        );
        let _ = callback.send(event);
        if committed {
            self.renewal_puts.fetch_add(1, Ordering::AcqRel);
            if let Some(outcome) = self.renewal_outcome.lock().unwrap().take() {
                let _ = outcome.send(RenewalOutcome::Committed);
            }
        }
    }

    crate::storage::cloud::forward_cloud_backend!(
        inner;
        submit_get, submit_get_range, submit_delete, submit_list, submit_head
    );
}

#[test]
fn should_retry_real_heartbeat_when_first_renewal_get_callback_times_out() {
    // Arrange: the real provider-backed lease and heartbeat retain their 30s
    // TTL and 1s retry pause. Only one read callback is held, without a PUT.
    let (first_read_sender, first_read_receiver) = mpsc::channel();
    let (outcome_sender, outcome_receiver) = mpsc::channel();
    let backend = Arc::new(HeldRenewalReadBackend {
        inner: MockCloudBackend::new(),
        armed: AtomicBool::new(false),
        held_callback: Mutex::new(None),
        first_read_seen: Mutex::new(Some(first_read_sender)),
        renewal_outcome: Mutex::new(Some(outcome_sender.clone())),
        renewal_gets: AtomicUsize::new(0),
        renewal_puts: AtomicUsize::new(0),
    });
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
        backend.clone(),
        "midge".to_string(),
    ));
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        cloud,
    ));
    let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
    let epoch = lease.epoch();
    let prior_deadline = Instant::now() + RENEWAL_WRITE_DEADLINE_MARGIN + Duration::from_secs(4);
    lease
        .validity
        .advance(epoch, prior_deadline)
        .expect("model remaining monotonic lease validity");
    backend.armed.store(true, Ordering::Release);
    let mut heartbeat = LeaseHeartbeat::new_with_healthy_and_validity(
        lease.clone(),
        Arc::new(AtomicBool::new(true)),
        Some(Arc::clone(&lease.validity)),
    );
    heartbeat.set_loss_hook(Arc::new(move || {
        let _ = outcome_sender.send(RenewalOutcome::Fenced);
    }));

    // Act: a positive callback handshake proves renewal reached the delayed
    // GET, and a second handshake distinguishes a completed CAS from fencing.
    heartbeat.start();
    let first_read = first_read_receiver.recv_timeout(Duration::from_secs(2));
    let outcome = outcome_receiver.recv_timeout(Duration::from_secs(7));
    heartbeat.stop();
    drop(backend.held_callback.lock().unwrap().take());

    // Assert: joining precedes assertions so a failing regression cannot leave
    // a renewal worker alive. A healthy snapshot alone is not completion proof.
    assert!(
        first_read.is_ok(),
        "renewal never reached held GET: {first_read:?}"
    );
    assert!(
        matches!(outcome, Ok(RenewalOutcome::Committed)),
        "one delayed GET must leave a fresh renewal admitted: {outcome:?}"
    );
    assert!(heartbeat.is_healthy());
    assert_eq!(lease.epoch(), epoch);
    assert_eq!(backend.renewal_gets.load(Ordering::Acquire), 2);
    assert_eq!(backend.renewal_puts.load(Ordering::Acquire), 1);
    assert!(matches!(
        lease.validity.snapshot(),
        super::super::super::traits::LeaseValidityState::Active { epoch: current, valid_until }
            if current == epoch && valid_until > prior_deadline
    ));
}

struct DelayedSubmissionReadBackend {
    inner: MockCloudBackend,
    delay: Duration,
    reads: AtomicUsize,
}

impl CloudBackend for DelayedSubmissionReadBackend {
    fn submit_get(&self, key: &str, callback: CloudCallback) {
        self.reads.fetch_add(1, Ordering::AcqRel);
        std::thread::sleep(self.delay);
        self.inner.submit_get(key, callback);
    }

    fn submit_get_with_metadata(&self, key: &str, callback: CloudCallback) {
        self.reads.fetch_add(1, Ordering::AcqRel);
        std::thread::sleep(self.delay);
        self.inner.submit_get_with_metadata(key, callback);
    }

    crate::storage::cloud::forward_cloud_backend!(
        inner;
        submit_put, submit_get_range, submit_delete, submit_list, submit_head
    );
}

type LeaseRead = fn(&crate::storage::cloud::CloudStorage, Duration) -> Result<(), LeaseError>;

fn timed_lease_reads() -> [(&'static str, LeaseRead); 4] {
    [
        ("lease GET", |cloud, budget| {
            provider_read_doc_with_timeout(cloud, budget).map(|_| ())
        }),
        ("lease metadata GET", |cloud, budget| {
            provider_read_doc_with_metadata(cloud, budget).map(|_| ())
        }),
        ("sentinel GET", |cloud, budget| {
            provider_read_authority_sentinel(cloud, budget).map(|_| ())
        }),
        ("sentinel metadata GET", |cloud, budget| {
            provider_read_authority_sentinel_with_metadata(cloud, budget).map(|_| ())
        }),
    ]
}

#[test]
fn should_refuse_lease_get_submission_when_read_budget_is_zero() {
    // Arrange: count actual submissions through the compatibility fallback.
    let backend = Arc::new(DelayedSubmissionReadBackend {
        inner: MockCloudBackend::new(),
        delay: Duration::ZERO,
        reads: AtomicUsize::new(0),
    });
    let cloud = crate::storage::cloud::CloudStorage::new(backend.clone(), "midge".into());

    // Act: invoke all four real lease and sentinel callers with zero budget.
    for (name, read) in timed_lease_reads() {
        let result = read(&cloud, Duration::ZERO);
        // Assert: each caller refuses dispatch and reports Timeout.
        assert!(
            matches!(result, Err(LeaseError::Timeout(_))),
            "{name}: {result:?}"
        );
    }
    assert_eq!(backend.reads.load(Ordering::Acquire), 0);
}

#[test]
fn should_reject_queued_lease_get_result_when_submission_spends_read_budget() {
    // Arrange: a submit-only backend overruns the caller budget and queues a
    // normal response before returning. A fresh recv timeout would accept it.
    let backend = Arc::new(DelayedSubmissionReadBackend {
        inner: MockCloudBackend::new(),
        delay: Duration::from_millis(30),
        reads: AtomicUsize::new(0),
    });
    let cloud = crate::storage::cloud::CloudStorage::new(backend.clone(), "midge".into());

    // Act: invoke all real lease and sentinel callers with a short budget.
    for (name, read) in timed_lease_reads() {
        let result = read(&cloud, Duration::from_millis(10));
        // Assert: each caller rejects its already-queued late result.
        assert!(
            matches!(result, Err(LeaseError::Timeout(_))),
            "{name}: {result:?}"
        );
    }
    assert_eq!(backend.reads.load(Ordering::Acquire), 4);
}
