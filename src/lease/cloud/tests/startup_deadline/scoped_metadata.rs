use super::*;

const IO_CAP: Duration = Duration::from_secs(3);
const CAS_WORK: Duration = Duration::from_millis(1750);
const LOGICAL_CAP: Duration = Duration::from_secs(20);
const ACTIVE_CAP: Duration = Duration::from_secs(10);

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
    let scope = DeadlineScope::new(OperationDeadline::from_budget(ACTIVE_CAP));
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
    // Arrange: the unwrapped provider caps each native request at 3s.
    // Act: a real conditional conflict and commit each take 1750ms.
    let observed = publish_observation(AuthorityView::Original);
    // Assert: exact committed-generation readback qualifies the baseline path.
    assert_successful_native_publication(&observed);
}

#[test]
fn should_preserve_native_metadata_retry_budget_when_startup_scope_is_complete() {
    // Arrange: the accepted private view shares an unbounded completed scope.
    // Act: the same native retry work exceeds 3s but fits the 20s logical cap.
    let observed = publish_observation(AuthorityView::Complete);
    // Assert: the lifetime view must restore the ordinary aggregate budget.
    assert_successful_native_publication(&observed);
}

#[test]
fn should_admit_native_metadata_retry_when_each_io_fits_remaining_startup_scope() {
    // Arrange: active startup has one 10s deadline and a 3s native I/O cap.
    // Act: execute 3500ms actual publication work under a 20s requested budget.
    let observed = publish_observation(AuthorityView::Active);
    // Assert: the logical retry uses remaining scope; each native retains 3s.
    assert_successful_native_publication(&observed);
}

struct MirrorObservation {
    outcome: crate::common::MidgeResult<()>,
    committed: Result<CloudMetadataHead, LeaseError>,
    expected_objects: HashMap<String, Vec<u8>>,
    remote_objects: Vec<(String, crate::common::MidgeResult<Option<Vec<u8>>>)>,
    deadlines: Vec<OperationDeadline>,
    startup_deadline: OperationDeadline,
    exchanges: Vec<MetadataExchange>,
    cleanup: Result<(), LeaseError>,
}

fn mirror_observation(scope: &DeadlineScope) -> MirrorObservation {
    let startup_deadline = scope.deadline();
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(
        directory.path().join(crate::metadata::files::FORMAT),
        crate::metadata::format::current_format_marker_bytes(),
    )
    .unwrap();
    crate::metadata::ManifestPersistence::save(
        directory.path(),
        &crate::metadata::Manifest::default(),
    )
    .unwrap();
    let expected_objects = crate::metadata::files::CLOUD_MIRRORED
        .iter()
        .filter_map(|name| {
            std::fs::read(directory.path().join(name))
                .ok()
                .map(|bytes| ((*name).to_string(), bytes))
        })
        .collect();
    let endpoint = MetadataEndpoint::start();
    let provider = CloudProviderConfig::sqrzl_s3("test-bucket")
        .with_endpoint(&endpoint.server.endpoint)
        .unwrap();
    let cloud = crate::storage::providers::build_cloud_storage_with_timeout(
        &provider,
        "startup-mirror",
        IO_CAP,
    )
    .unwrap();
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));
    let guard = Arc::clone(&lease).try_acquire().unwrap();
    let store = lease.get_leader_store().unwrap();
    let mut deadlines = Vec::new();
    let outcome = crate::runtime::cloud_startup::CloudStartupRecovery::mirror_cloud_metadata_within(
        &cloud,
        directory.path(),
        crate::config::RecoveryPolicy::Strict,
        crate::runtime::hybrid_persistence::CloudMetadataMirrorAuthority {
            store: store.as_ref(),
            holder_id: &lease.holder_id(),
            writer_epoch: lease.epoch(),
        },
        &crate::runtime::MetadataPublicationLock::default(),
        |deadline| {
            deadlines.push(*deadline);
            store
                .validate_epoch_with_timeout(
                    &lease.holder_id(),
                    lease.epoch(),
                    deadline.remaining(),
                )
                .map_err(|error| error.into_validation_error("native mirror fixture authority"))
        },
        scope,
    );
    let committed = store.read_committed_metadata(IO_CAP);
    let mut remote_objects = Vec::new();
    if let Ok(CloudMetadataHead::Committed(generation)) = &committed {
        for object in &generation.objects {
            let deadline = OperationDeadline::from_budget(IO_CAP);
            remote_objects.push((
                object.file_name.clone(),
                crate::storage::cloud::BlockingCloud::new(&cloud, &deadline)
                    .get_optional(&object.object_key),
            ));
        }
    }
    let cleanup = lease.release();
    drop(store);
    drop(guard);
    drop(lease);
    drop(cloud);
    let exchanges = std::mem::take(&mut *endpoint.exchanges.lock().unwrap());
    drop(endpoint);
    MirrorObservation {
        outcome,
        committed,
        expected_objects,
        remote_objects,
        deadlines,
        startup_deadline,
        exchanges,
        cleanup,
    }
}

fn assert_successful_native_mirror(observed: &MirrorObservation) {
    assert!(observed.outcome.is_ok(), "{:?}", observed.outcome);
    assert!(observed.cleanup.is_ok(), "{:?}", observed.cleanup);
    assert!(observed.deadlines.len() >= 2);
    assert!(observed.deadlines.iter().all(OperationDeadline::is_bounded));
    if observed.startup_deadline.is_bounded() {
        assert!(observed
            .deadlines
            .iter()
            .all(|deadline| *deadline == observed.startup_deadline));
    }
    let Ok(CloudMetadataHead::Committed(generation)) = &observed.committed else {
        panic!(
            "actual native pointer was not committed: {:?}",
            observed.committed
        );
    };
    assert_eq!(generation.manifest_sequence, 0);
    assert_eq!(generation.objects.len(), observed.expected_objects.len());
    for object in &generation.objects {
        let expected = &observed.expected_objects[&object.file_name];
        assert_eq!(object.len, u64::try_from(expected.len()).unwrap());
        assert_eq!(object.crc32c, crc32c::crc32c(expected));
    }
    assert_eq!(
        observed.remote_objects.len(),
        observed.expected_objects.len()
    );
    for (name, remote) in &observed.remote_objects {
        assert_eq!(
            remote.as_ref().unwrap().as_ref().unwrap(),
            &observed.expected_objects[name],
            "exact native mirrored bytes for {name}"
        );
    }
    assert_eq!(observed.exchanges.len(), 2);
    assert!(observed
        .exchanges
        .iter()
        .all(|exchange| exchange.conditional));
    assert!(observed.exchanges[0].rejected);
    assert!(!observed.exchanges[1].rejected);
    assert!(observed
        .exchanges
        .iter()
        .all(|exchange| exchange.elapsed < IO_CAP));
}

#[test]
fn should_preserve_native_mirror_operation_budget_when_startup_scope_is_unbounded() {
    // Arrange: defaultNone recovery passes its genuine unbounded startup scope.
    let scope = DeadlineScope::new(OperationDeadline::unbounded());

    // Act: run the actual mirror entry with real FORMAT/manifest and native CAS.
    let observed = mirror_observation(&scope);

    // Assert: retained finite mirror work commits the exact durable generation.
    assert_successful_native_mirror(&observed);
}

#[test]
fn should_publish_native_mirror_when_bounded_startup_scope_has_remaining_budget() {
    // Arrange: a configured open retains its captured finite aggregate deadline.
    let scope = DeadlineScope::new(OperationDeadline::from_budget(ACTIVE_CAP));

    // Act: run the same actual native mirror within the configured scope.
    let observed = mirror_observation(&scope);

    // Assert: finite per-I/O work commits without weakening the startup budget.
    assert_successful_native_mirror(&observed);
}
