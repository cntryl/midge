use super::*;
use crate::common::{DeadlineScope, OperationDeadline};
use crate::config::CloudProviderConfig;
use crate::storage::cloud::native_http::{
    observe_cancellation, NativeHttpServer, Request, Response,
};
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;

#[path = "startup_deadline/scoped_metadata.rs"]
mod scoped_metadata;

#[derive(Clone, Copy, Debug)]
enum Replacement {
    Epoch,
    OwnerToken,
}

#[derive(Clone, Copy, Debug)]
enum Hold {
    FirstLeaseGet,
    SentinelAfterLease,
    CommittedLeasePut,
    RejectedLeasePut,
    ReplacedLeasePut(Replacement),
}

impl Hold {
    fn withholds_put_response(self) -> bool {
        matches!(self, Self::CommittedLeasePut | Self::ReplacedLeasePut(_))
    }
}

#[derive(Default)]
struct EndpointObservations {
    lease_puts: AtomicUsize,
    lease_put_requests: AtomicUsize,
    acquisition_finished: AtomicBool,
    remote_lease: Mutex<Option<LeaseDocument>>,
    attempted_lease: Mutex<Option<LeaseDocument>>,
    conditional_release_puts: AtomicUsize,
}

struct LeaseEndpoint {
    server: NativeHttpServer,
    held: std::sync::mpsc::Receiver<bool>,
    observations: Arc<EndpointObservations>,
}

struct EndpointState {
    hold: Hold,
    held_tx: std::sync::mpsc::Sender<bool>,
    observations: Arc<EndpointObservations>,
    objects: HashMap<String, (Vec<u8>, usize)>,
    held_once: bool,
}

impl LeaseEndpoint {
    fn start(scenario: Hold) -> Self {
        let (held_tx, held) = std::sync::mpsc::channel();
        let observations = Arc::new(EndpointObservations::default());
        let mut state = EndpointState {
            hold: scenario,
            held_tx,
            observations: Arc::clone(&observations),
            objects: HashMap::new(),
            held_once: false,
        };
        let server = NativeHttpServer::start(move |stream, request| state.handle(stream, request));
        Self {
            server,
            held,
            observations,
        }
    }
}

impl EndpointState {
    fn handle(&mut self, stream: &mut std::net::TcpStream, request: Request) -> Option<Response> {
        let key = request.path.split('?').next().unwrap().to_string();
        let lease_key = key.ends_with(LEASE_OBJECT_KEY);
        let sentinel_key = key.ends_with(AUTHORITY_SENTINEL_KEY);
        let hold_read = request.method == "GET"
            && match self.hold {
                Hold::FirstLeaseGet => lease_key,
                Hold::SentinelAfterLease => {
                    sentinel_key && self.observations.lease_puts.load(Ordering::Acquire) > 0
                }
                Hold::CommittedLeasePut | Hold::ReplacedLeasePut(_) => {
                    lease_key
                        && self.observations.lease_puts.load(Ordering::Acquire) > 0
                        && !self
                            .observations
                            .acquisition_finished
                            .load(Ordering::Acquire)
                }
                Hold::RejectedLeasePut => false,
            };
        if hold_read && (!self.held_once || self.hold.withholds_put_response()) {
            let report_first_hold = !self.held_once;
            self.held_once = true;
            let cancelled = observe_cancellation(stream);
            if report_first_hold {
                let _ = self.held_tx.send(cancelled);
            }
            if cancelled {
                return None;
            }
        }
        match request.method.as_str() {
            "GET" => Some(self.objects.get(&key).map_or_else(
                || Response::new(404, b"<Error><Code>NoSuchKey</Code></Error>".to_vec()),
                |(body, version)| Response::new(200, body.clone()).with_etag(*version),
            )),
            "PUT" => self.put(stream, key, request),
            other => panic!("unexpected native lease method {other}"),
        }
    }

    fn put(
        &mut self,
        stream: &mut std::net::TcpStream,
        key: String,
        request: Request,
    ) -> Option<Response> {
        let lease_key = key.ends_with(LEASE_OBJECT_KEY);
        if lease_key {
            self.observations
                .lease_put_requests
                .fetch_add(1, Ordering::Release);
            if matches!(self.hold, Hold::RejectedLeasePut) {
                let _ = self.held_tx.send(false);
                return Some(Response::new(
                    412,
                    b"<Error><Code>PreconditionFailed</Code></Error>".to_vec(),
                ));
            }
        }
        if !condition_matches(&request, self.objects.get(&key)) {
            return Some(Response::new(
                412,
                b"<Error><Code>PreconditionFailed</Code></Error>".to_vec(),
            ));
        }
        let version = self.objects.get(&key).map_or(1, |(_, version)| version + 1);
        if lease_key {
            let document = parse_lease_document(
                std::str::from_utf8(&request.body).expect("native lease body UTF-8"),
            )
            .expect("actual native lease document");
            if document.is_expired().unwrap() {
                assert!(
                    request.headers.iter().any(|(name, _)| name == "if-match"),
                    "cleanup must use the actual object's conditional token"
                );
                self.observations
                    .conditional_release_puts
                    .fetch_add(1, Ordering::Release);
            } else if self.observations.lease_puts.load(Ordering::Acquire) == 0 {
                *self.observations.attempted_lease.lock().unwrap() = Some(document.clone());
            }
            *self.observations.remote_lease.lock().unwrap() = Some(document);
        }
        self.objects.insert(key.clone(), (request.body, version));
        if lease_key {
            self.observations.lease_puts.fetch_add(1, Ordering::Release);
            if self.hold.withholds_put_response() && !self.held_once {
                if let Hold::ReplacedLeasePut(replacement) = self.hold {
                    let attempted = self
                        .observations
                        .attempted_lease
                        .lock()
                        .unwrap()
                        .clone()
                        .unwrap();
                    let successor = replacement_document(&attempted, replacement);
                    self.objects.insert(
                        key,
                        (format_lease_document(&successor).into_bytes(), version + 1),
                    );
                    *self.observations.remote_lease.lock().unwrap() = Some(successor);
                }
                self.held_once = true;
                let cancelled = observe_cancellation(stream);
                let _ = self.held_tx.send(cancelled);
                if cancelled {
                    return None;
                }
            }
        }
        Some(Response::new(200, Vec::new()).with_etag(version))
    }
}

fn replacement_document(attempted: &LeaseDocument, replacement: Replacement) -> LeaseDocument {
    let mut successor = attempted.clone();
    match replacement {
        Replacement::Epoch => {
            successor.epoch = Some(attempted.epoch.unwrap().checked_add(1).unwrap());
        }
        Replacement::OwnerToken => {
            successor.owner_token = Some("actual-native-successor-owner".into());
        }
    }
    let mut generation = test_metadata_generation(99);
    for (index, object) in generation.objects.iter_mut().enumerate() {
        object.object_key = format!(
            "metadata/generations/00000000-0000-4000-8000-{index:012}/{}",
            object.file_name,
        );
    }
    successor.committed_metadata = Some(generation);
    successor
}

fn condition_matches(request: &Request, existing: Option<&(Vec<u8>, usize)>) -> bool {
    if request
        .headers
        .iter()
        .any(|(name, value)| name == "if-none-match" && value == "*")
    {
        return existing.is_none();
    }
    if let Some((_, condition)) = request.headers.iter().find(|(name, _)| name == "if-match") {
        return existing.is_some_and(|(_, version)| *condition == format!("\"{version}\""));
    }
    false
}

struct AcquisitionObservation {
    error: Option<LeaseError>,
    elapsed: Duration,
    native_cancelled: bool,
    lease_puts: usize,
    lease_put_requests: usize,
    pending_epoch: Option<u64>,
    acquired: bool,
    unresolved: bool,
    cleanup: CleanupObservation,
    attempted_lease: Option<LeaseDocument>,
}

struct CleanupObservation {
    succeeded: bool,
    before: Option<LeaseDocument>,
    after: Option<LeaseDocument>,
    pending_epoch: Option<u64>,
    conditional_release_puts: usize,
    lease_put_requests: usize,
}

impl CleanupObservation {
    fn was_live(&self) -> bool {
        self.before
            .as_ref()
            .is_some_and(|document| !document.is_expired().unwrap())
    }

    fn exact_owner_expired(&self) -> bool {
        self.before
            .as_ref()
            .zip(self.after.as_ref())
            .is_some_and(|(before, after)| {
                after.is_expired().unwrap()
                    && before.version == after.version
                    && before.epoch == after.epoch
                    && before.holder_id == after.holder_id
                    && before.owner_token == after.owner_token
                    && before.acquired_at == after.acquired_at
                    && before.committed_metadata == after.committed_metadata
            })
    }
}

fn acquire_observation(hold: Hold) -> AcquisitionObservation {
    let endpoint = LeaseEndpoint::start(hold);
    let provider = CloudProviderConfig::sqrzl_s3("test-bucket")
        .with_endpoint(&endpoint.server.endpoint)
        .unwrap();
    let cloud = crate::storage::providers::build_cloud_storage_with_timeout(
        &provider,
        "startup-lease",
        Duration::from_secs(3),
    )
    .expect("native S3 lease backend");
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        cloud,
    ));
    let scope = DeadlineScope::new(OperationDeadline::from_budget(Duration::from_millis(150)));
    let started = Instant::now();
    let result = Arc::clone(&lease).try_acquire_with_minimum_epoch_within(0, &scope);
    let elapsed = started.elapsed();
    let native_cancelled = endpoint
        .held
        .recv_timeout(Duration::from_secs(2))
        .expect("actual held native request must arrive");
    let lease_puts = endpoint.observations.lease_puts.load(Ordering::Acquire);
    let lease_put_requests = endpoint
        .observations
        .lease_put_requests
        .load(Ordering::Acquire);
    let pending_epoch = *lease.pending_release_epoch.lock().unwrap();
    let acquired = lease.acquired.load(Ordering::Acquire);
    let unresolved = scope.cancel();
    let error = result.err();
    let before_cleanup = endpoint.observations.remote_lease.lock().unwrap().clone();
    let attempted_lease = endpoint
        .observations
        .attempted_lease
        .lock()
        .unwrap()
        .clone();
    endpoint
        .observations
        .acquisition_finished
        .store(true, Ordering::Release);
    let cleanup_succeeded = lease.release().is_ok();
    let after_cleanup = endpoint.observations.remote_lease.lock().unwrap().clone();
    let pending_epoch_after_cleanup = *lease.pending_release_epoch.lock().unwrap();
    let conditional_release_puts = endpoint
        .observations
        .conditional_release_puts
        .load(Ordering::Acquire);
    let lease_put_requests_after_cleanup = endpoint
        .observations
        .lease_put_requests
        .load(Ordering::Acquire);
    // The held response has already been cancelled or released by the endpoint
    // before storage/runtime destruction. All cleanup settles before assertions.
    drop(lease);
    drop(endpoint);
    AcquisitionObservation {
        error,
        elapsed,
        native_cancelled,
        lease_puts,
        lease_put_requests,
        pending_epoch,
        acquired,
        unresolved,
        cleanup: CleanupObservation {
            succeeded: cleanup_succeeded,
            before: before_cleanup,
            after: after_cleanup,
            pending_epoch: pending_epoch_after_cleanup,
            conditional_release_puts,
            lease_put_requests: lease_put_requests_after_cleanup,
        },
        attempted_lease,
    }
}

#[test]
fn should_time_out_native_acquisition_when_first_read_exhausts_startup_budget() {
    // Arrange: the actual S3 first lease GET is held; provider default is 3s,
    // while the caller startup budget is 150ms and no CAS has been admitted.
    // Act
    let observed = acquire_observation(Hold::FirstLeaseGet);
    // Assert
    assert!(
        matches!(observed.error, Some(LeaseError::Timeout(_))),
        "{:?}",
        observed.error
    );
    assert!(
        observed.native_cancelled,
        "native read must consume the startup budget"
    );
    assert!(
        observed.elapsed < Duration::from_millis(500),
        "{:?}",
        observed.elapsed
    );
    assert_eq!(observed.lease_puts, 0);
    assert_eq!(observed.pending_epoch, None);
    assert!(!observed.acquired);
    assert!(!observed.unresolved);
    assert!(observed.cleanup.succeeded);
    assert!(!observed.cleanup.was_live());
    assert!(!observed.cleanup.exact_owner_expired());
    assert_eq!(observed.cleanup.conditional_release_puts, 0);
}

#[test]
fn should_retain_known_native_owner_when_sentinel_read_exhausts_startup_budget() {
    // Arrange: real S3 accepts one conditional lease PUT; the subsequent
    // sentinel metadata GET is held beyond the same 150ms startup budget.
    // Act
    let observed = acquire_observation(Hold::SentinelAfterLease);
    // Assert
    assert!(
        matches!(observed.error, Some(LeaseError::Indeterminate(_))),
        "{:?}",
        observed.error
    );
    assert!(observed.native_cancelled);
    assert!(
        observed.elapsed < Duration::from_millis(500),
        "{:?}",
        observed.elapsed
    );
    assert_eq!(observed.lease_puts, 1);
    assert_eq!(
        observed.pending_epoch,
        Some(1),
        "confirmed owner must remain available to conditional cleanup"
    );
    assert!(!observed.acquired);
    assert!(observed.unresolved);
    assert!(observed.cleanup.succeeded);
    assert!(observed.cleanup.was_live());
    assert!(
        observed.cleanup.exact_owner_expired(),
        "known owner cleanup must actually expire the exact remotely committed owner"
    );
    assert_eq!(observed.cleanup.conditional_release_puts, 1);
}

#[test]
fn should_preserve_native_cas_uncertainty_when_response_exceeds_startup_budget() {
    // Arrange: the real endpoint stores the conditional lease PUT before
    // withholding its response. A caller deadline cannot prove non-commit.
    // Act
    let observed = acquire_observation(Hold::CommittedLeasePut);
    // Assert
    assert!(
        matches!(observed.error, Some(LeaseError::Indeterminate(_))),
        "{:?}",
        observed.error
    );
    assert!(observed.native_cancelled);
    assert!(
        observed.elapsed < Duration::from_millis(500),
        "{:?}",
        observed.elapsed
    );
    assert_eq!(
        observed.lease_puts, 1,
        "ambiguous conditional PUT must never be replayed"
    );
    assert!(!observed.acquired);
    assert!(observed.unresolved);
    assert!(observed.cleanup.succeeded);
    assert!(observed.cleanup.was_live());
    assert!(
        observed.cleanup.exact_owner_expired(),
        "unknown CAS cleanup must actually expire the exact remotely committed owner"
    );
    assert_eq!(observed.cleanup.conditional_release_puts, 1);
}

#[test]
fn should_clear_candidate_without_remote_expiration_when_native_cas_is_definitely_rejected() {
    // Arrange: the real native CAS reaches the endpoint, which returns 412
    // before committing a lease document. Retention is a candidate, not authority.
    // Act
    let observed = acquire_observation(Hold::RejectedLeasePut);
    // Assert: one actual submission, definite classification and no cleanup PUT.
    assert!(
        matches!(observed.error, Some(LeaseError::AcquisitionFailed(_))),
        "{:?}",
        observed.error
    );
    assert_eq!(observed.lease_put_requests, 1);
    assert_eq!(observed.lease_puts, 0);
    assert_eq!(observed.pending_epoch, Some(1));
    assert!(!observed.acquired);
    assert!(
        !observed.unresolved,
        "definite rejection must resolve ambiguity"
    );
    assert!(observed.cleanup.succeeded);
    assert_eq!(observed.cleanup.before, None);
    assert_eq!(observed.cleanup.after, None);
    assert_eq!(observed.cleanup.pending_epoch, None);
    assert_eq!(observed.cleanup.conditional_release_puts, 0);
    assert_eq!(observed.cleanup.lease_put_requests, 1);
}

#[test]
fn should_preserve_exact_successor_when_uncertain_native_candidate_is_released() {
    // Arrange: commit one native CAS then replace the endpoint object before
    // withholding its response. Isolate epoch and token guards in two cases;
    // keep the same holder and preserve a distinct committed metadata pointer.
    let replacements = [Replacement::Epoch, Replacement::OwnerToken];
    // Act: every endpoint/runtime is cancelled, released and joined first.
    let observations = replacements.map(|replacement| {
        (
            replacement,
            acquire_observation(Hold::ReplacedLeasePut(replacement)),
        )
    });
    // Assert: an attempted candidate cannot authorize expiring either successor.
    for (replacement, observed) in observations {
        assert!(matches!(observed.error, Some(LeaseError::Indeterminate(_))));
        assert!(observed.native_cancelled);
        assert_eq!(observed.lease_put_requests, 1);
        assert_eq!(observed.lease_puts, 1);
        assert_eq!(observed.pending_epoch, Some(1));
        assert!(!observed.acquired);
        assert!(observed.unresolved);
        assert!(observed.cleanup.succeeded);
        assert!(observed.cleanup.was_live());
        let attempted = observed
            .attempted_lease
            .expect("actual native CAS document");
        let expected = replacement_document(&attempted, replacement);
        assert_eq!(
            observed.cleanup.before,
            Some(expected.clone()),
            "{replacement:?}"
        );
        assert_eq!(observed.cleanup.after, Some(expected), "{replacement:?}");
        assert_eq!(observed.cleanup.pending_epoch, None);
        assert_eq!(observed.cleanup.conditional_release_puts, 0);
        assert_eq!(
            observed.cleanup.lease_put_requests, 1,
            "candidate CAS must not replay"
        );
    }
}
