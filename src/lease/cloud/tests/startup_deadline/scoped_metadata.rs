use super::*;

const IO_CAP: Duration = Duration::from_millis(400);
const CAS_WORK: Duration = Duration::from_millis(250);
const LOGICAL_CAP: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug)]
enum AuthorityView {
    Original,
    Active,
    Complete,
}

#[derive(Debug)]
struct MetadataExchange {
    conditional: bool,
    elapsed: Duration,
    rejected: bool,
}

struct MetadataEndpoint {
    server: NativeHttpServer,
    exchanges: Arc<Mutex<Vec<MetadataExchange>>>,
    lease_reads: Arc<AtomicUsize>,
}

struct MetadataState {
    objects: HashMap<String, (Vec<u8>, usize)>,
    exchanges: Arc<Mutex<Vec<MetadataExchange>>>,
    lease_reads: Arc<AtomicUsize>,
    collided_once: bool,
}

impl MetadataEndpoint {
    fn start() -> Self {
        let exchanges = Arc::new(Mutex::new(Vec::new()));
        let lease_reads = Arc::new(AtomicUsize::new(0));
        let mut state = MetadataState {
            objects: HashMap::new(),
            exchanges: Arc::clone(&exchanges),
            lease_reads: Arc::clone(&lease_reads),
            collided_once: false,
        };
        let server = NativeHttpServer::start(move |_stream, request| Some(state.respond(&request)));
        Self {
            server,
            exchanges,
            lease_reads,
        }
    }
}

impl MetadataState {
    fn respond(&mut self, request: &Request) -> Response {
        let key = request.path.split('?').next().unwrap().to_string();
        match request.method.as_str() {
            "GET" => {
                if key.ends_with(LEASE_OBJECT_KEY) {
                    self.lease_reads.fetch_add(1, Ordering::Release);
                }
                self.objects.get(&key).map_or_else(
                    || Response::new(404, b"<Error><Code>NoSuchKey</Code></Error>".to_vec()),
                    |(body, version)| Response::new(200, body.clone()).with_etag(*version),
                )
            }
            "PUT" => self.put(key, request),
            other => panic!("unexpected native metadata method {other}"),
        }
    }

    fn put(&mut self, key: String, request: &Request) -> Response {
        if !condition_matches(request, self.objects.get(&key)) {
            return Response::new(
                412,
                b"<Error><Code>PreconditionFailed</Code></Error>".to_vec(),
            );
        }
        let document = key
            .ends_with(LEASE_OBJECT_KEY)
            .then(|| parse_lease_document(std::str::from_utf8(&request.body).unwrap()).unwrap());
        let is_publication = document.as_ref().is_some_and(|document| {
            document.committed_metadata.is_some() && !document.is_expired().unwrap()
        });
        let version = self.objects.get(&key).map_or(1, |(_, version)| version + 1);
        if is_publication {
            let started = Instant::now();
            std::thread::sleep(CAS_WORK);
            let rejected = !self.collided_once;
            self.exchanges.lock().unwrap().push(MetadataExchange {
                conditional: request.headers.iter().any(|(name, _)| name == "if-match"),
                elapsed: started.elapsed(),
                rejected,
            });
            if rejected {
                self.collided_once = true;
                // Advance only the current object version, retaining identical
                // authority and pointer bytes. The first pinned token lost.
                let current = self.objects.get_mut(&key).unwrap();
                current.1 = version;
                return Response::new(
                    412,
                    b"<Error><Code>PreconditionFailed</Code></Error>".to_vec(),
                );
            }
        }
        self.objects.insert(key, (request.body.clone(), version));
        Response::new(200, Vec::new()).with_etag(version)
    }
}

struct PublicationObservation {
    outcome: Result<(), LeaseError>,
    elapsed: Duration,
    expected: CloudMetadataGeneration,
    committed: Result<CloudMetadataHead, LeaseError>,
    exchanges: Vec<MetadataExchange>,
    publication_lease_reads: usize,
    cleanup: Result<(), LeaseError>,
}

fn publish_observation(view: AuthorityView) -> PublicationObservation {
    let endpoint = MetadataEndpoint::start();
    let provider = CloudProviderConfig::sqrzl_s3("test-bucket")
        .with_endpoint(&endpoint.server.endpoint)
        .unwrap();
    let cloud = crate::storage::providers::build_cloud_storage_with_timeout(
        &provider,
        "startup-metadata",
        IO_CAP,
    )
    .unwrap();
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        cloud,
    ));
    let guard = Arc::clone(&lease)
        .try_acquire()
        .expect("actual native acquired owner");
    let store = lease.get_leader_store().unwrap();
    let scope = DeadlineScope::new(OperationDeadline::from_budget(Duration::from_secs(1)));
    let authority = match view {
        AuthorityView::Original => Arc::clone(&store),
        AuthorityView::Active => {
            crate::lease::scoped_leader_store(Arc::clone(&store), scope, IO_CAP)
        }
        AuthorityView::Complete => {
            scope.complete().unwrap();
            crate::lease::scoped_leader_store(Arc::clone(&store), scope, IO_CAP)
        }
    };
    let expected = test_metadata_generation(40);
    let prior_reads = endpoint.lease_reads.load(Ordering::Acquire);
    let started = Instant::now();
    let outcome = authority.publish_committed_metadata(
        &lease.holder_id(),
        lease.epoch(),
        None,
        expected.clone(),
        LOGICAL_CAP,
    );
    let elapsed = started.elapsed();
    let publication_lease_reads = endpoint.lease_reads.load(Ordering::Acquire) - prior_reads;
    // Read the actual committed pointer through the original native authority
    // store. It may settle a late ambiguous CAS on the buggy wrapper baseline.
    let committed = store.read_committed_metadata(IO_CAP);
    let cleanup = lease.release();
    drop(authority);
    drop(store);
    drop(guard);
    drop(lease);
    let exchanges = std::mem::take(&mut *endpoint.exchanges.lock().unwrap());
    drop(endpoint);
    PublicationObservation {
        outcome,
        elapsed,
        expected,
        committed,
        exchanges,
        publication_lease_reads,
        cleanup,
    }
}

fn assert_successful_native_publication(observed: &PublicationObservation) {
    assert!(observed.outcome.is_ok(), "{:?}", observed.outcome);
    assert_eq!(
        observed.exchanges.len(),
        2,
        "one definite collision, then one real commit"
    );
    assert!(observed
        .exchanges
        .iter()
        .all(|exchange| exchange.conditional));
    assert!(observed.exchanges[0].rejected);
    assert!(!observed.exchanges[1].rejected);
    assert!(
        observed
            .exchanges
            .iter()
            .all(|exchange| exchange.elapsed < IO_CAP),
        "every native request's finite work fits ordinary I/O: {:?}",
        observed.exchanges
    );
    assert!(
        observed.elapsed > IO_CAP,
        "whole logical publication must exceed one I/O cap"
    );
    assert!(
        observed.elapsed < LOGICAL_CAP,
        "finite publication must fit caller budget"
    );
    assert!(
        observed.publication_lease_reads >= 2,
        "definite rejection needs a real fresh authority read"
    );
    assert!(
        matches!(&observed.committed, Ok(CloudMetadataHead::Committed(actual))
        if *actual == observed.expected),
        "{:?}",
        observed.committed
    );
    assert!(observed.cleanup.is_ok(), "{:?}", observed.cleanup);
}

#[test]
fn should_complete_native_metadata_retry_when_work_exceeds_one_io_cap() {
    // Arrange: the unwrapped provider caps each native request at 400ms.
    // Act: a real conditional conflict and commit each take 250ms.
    let observed = publish_observation(AuthorityView::Original);
    // Assert: exact committed-generation readback qualifies the baseline path.
    assert_successful_native_publication(&observed);
}

#[test]
fn should_preserve_native_metadata_retry_budget_when_startup_scope_is_complete() {
    // Arrange: the accepted private view shares an unbounded completed scope.
    // Act: the same native retry work exceeds 400ms but fits the 2s logical cap.
    let observed = publish_observation(AuthorityView::Complete);
    // Assert: the lifetime view must restore the ordinary aggregate budget.
    assert_successful_native_publication(&observed);
}

#[test]
fn should_admit_native_metadata_retry_when_each_io_fits_remaining_startup_scope() {
    // Arrange: active startup has one 1s deadline and a 400ms native I/O cap.
    // Act: execute 500ms actual publication work under a 2s requested budget.
    let observed = publish_observation(AuthorityView::Active);
    // Assert: the logical retry uses remaining scope; each native retains 400ms.
    assert_successful_native_publication(&observed);
}
