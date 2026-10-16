//! Request budgets over each provider's native upload and catalog
//! publication paths, against a small in-process object store.

use crate::common::OperationDeadline;
use crate::config::{S3CompatibleConfig, S3CredentialSource};
use crate::runtime::hybrid_persistence::CloudPersistence;
use crate::storage::cloud::native_http::{NativeHttpServer, Request, Response};
use crate::storage::cloud::CloudStorage;
use crate::storage::HybridStorage;
use crate::wal::format::{segment::SegmentBuilder, ChainLink};
use base64::Engine as _;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The wire protocol the in-process store speaks.
#[derive(Clone, Copy, Debug)]
enum Dialect {
    S3,
    Azure,
    GcsXml,
}

impl Dialect {
    const ALL: [Self; 3] = [Self::S3, Self::Azure, Self::GcsXml];

    fn backend(self, endpoint: String) -> Arc<dyn crate::storage::cloud::CloudBackend> {
        match self {
            Self::S3 => {
                let config = S3CompatibleConfig::new(
                    "bucket",
                    "us-east-1",
                    endpoint,
                    S3CredentialSource::access_key("access", "secret"),
                )
                .with_checksum_receipts(true);
                super::s3_resolver::try_resolve(&config.into())
                    .unwrap()
                    .unwrap()
            }
            Self::Azure => super::azure::contract_test_backend(endpoint),
            Self::GcsXml => super::gcs::contract_test_xml_backend(endpoint),
        }
    }

    fn path(self, key: &str) -> String {
        match self {
            Self::S3 | Self::GcsXml => format!("/bucket/db/{key}"),
            Self::Azure => format!("/account/container/db/{key}"),
        }
    }

    fn precondition_fails(self, request: &Request, version: Option<usize>) -> bool {
        let header = |name: &str| header(request, name);
        match self {
            Self::S3 | Self::Azure => {
                let etag = version.map(|version| format!("\"{version}\""));
                header("if-match").is_some_and(|expected| Some(expected) != etag.as_deref())
                    || (header("if-none-match") == Some("*") && version.is_some())
            }
            Self::GcsXml => header("x-goog-if-generation-match")
                .is_some_and(|expected| expected.parse::<usize>().unwrap() != version.unwrap_or(0)),
        }
    }

    fn precondition_failed(self) -> Response {
        let mut response = Response::new(
            412,
            b"<Error><Code>PreconditionFailed</Code></Error>".to_vec(),
        );
        if matches!(self, Self::Azure) {
            response
                .headers
                .push(("x-ms-error-code".into(), "ConditionNotMet".into()));
        }
        response
    }

    fn put_status(self) -> u16 {
        match self {
            Self::S3 | Self::GcsXml => 200,
            Self::Azure => 201,
        }
    }

    fn identify(self, response: Response, version: usize) -> Response {
        let mut response = response.with_etag(version);
        if matches!(self, Self::GcsXml) {
            response
                .headers
                .push(("x-goog-generation".into(), version.to_string()));
        }
        response
    }

    /// The integrity header an upload sent, checked against its body.
    fn sent_checksum(self, request: &Request) -> Option<String> {
        let encode = |bytes: &[u8]| base64::engine::general_purpose::STANDARD.encode(bytes);
        let (sent, expected) = match self {
            Self::S3 => (
                header(request, "x-amz-checksum-crc32c")?,
                encode(&crc32c::crc32c(&request.body).to_be_bytes()),
            ),
            Self::Azure => {
                use md5::Digest as _;
                (
                    header(request, "content-md5")?,
                    encode(&md5::Md5::digest(&request.body)),
                )
            }
            Self::GcsXml => (
                header(request, "x-goog-hash")?,
                format!(
                    "crc32c={}",
                    encode(&crc32c::crc32c(&request.body).to_be_bytes())
                ),
            ),
        };
        assert_eq!(sent, expected, "{self:?} upload integrity header");
        Some(sent.to_string())
    }

    /// The checksum header a confirmed upload answers with. GCS reports the
    /// hash of what it stored; S3 and Azure echo the hash they validated.
    fn receipt(self, sent: String, stored: &[u8], corrupt: bool) -> (String, String) {
        let corrupt_hash = || "AAAAAA==".to_string();
        match self {
            Self::S3 => (
                "x-amz-checksum-crc32c".into(),
                if corrupt { corrupt_hash() } else { sent },
            ),
            Self::Azure => (
                "Content-MD5".into(),
                if corrupt { corrupt_hash() } else { sent },
            ),
            Self::GcsXml => (
                "x-goog-hash".into(),
                format!(
                    "crc32c={}",
                    base64::engine::general_purpose::STANDARD
                        .encode(crc32c::crc32c(stored).to_be_bytes())
                ),
            ),
        }
    }
}

fn header<'a>(request: &'a Request, name: &str) -> Option<&'a str> {
    request
        .headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

#[derive(Default)]
enum CatalogFault {
    #[default]
    None,
    RejectMirror,
    LosePrimaryResponse,
}

struct State {
    dialect: Dialect,
    objects: HashMap<String, (Vec<u8>, usize)>,
    requests: Vec<(String, String)>,
    fault: CatalogFault,
    omit_receipts: bool,
    corrupt_upload: bool,
}

fn respond(state: &mut State, request: Request) -> Response {
    state
        .requests
        .push((request.method.clone(), request.path.clone()));
    let dialect = state.dialect;
    let existing = state.objects.get(&request.path);
    if dialect.precondition_fails(&request, existing.map(|(_, version)| *version)) {
        return dialect.precondition_failed();
    }
    if request.method == "PUT" {
        return put(state, request);
    }
    let Some((bytes, version)) = existing else {
        return Response::new(404, Vec::new());
    };
    if let Some(range) = header(&request, "range") {
        let (start, end) = range
            .strip_prefix("bytes=")
            .unwrap()
            .split_once('-')
            .unwrap();
        let start: usize = start.parse().unwrap();
        let end: usize = end.parse().unwrap();
        let mut response =
            dialect.identify(Response::new(206, bytes[start..=end].to_vec()), *version);
        response.headers.push((
            "Content-Range".into(),
            format!("bytes {start}-{end}/{}", bytes.len()),
        ));
        return response;
    }
    dialect.identify(Response::new(200, bytes.clone()), *version)
}

fn put(state: &mut State, request: Request) -> Response {
    let dialect = state.dialect;
    if matches!(state.fault, CatalogFault::RejectMirror)
        && request
            .path
            .ends_with(crate::wal::cloud_catalog::MIRROR_OBJECT_KEY)
    {
        state.fault = CatalogFault::None;
        return Response::new(403, Vec::new());
    }
    let sent = dialect.sent_checksum(&request);
    let version = state
        .objects
        .get(&request.path)
        .map_or(1, |(_, version)| version + 1);
    let corrupt = state.corrupt_upload
        && std::path::Path::new(&request.path)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("wal"));
    let body = if corrupt {
        b"corrupted".to_vec()
    } else {
        request.body
    };
    let mut response = dialect.identify(Response::new(dialect.put_status(), Vec::new()), version);
    if let Some(sent) = sent.filter(|_| !state.omit_receipts) {
        response.headers.push(dialect.receipt(sent, &body, corrupt));
    }
    state.objects.insert(request.path, (body, version));
    response
}

struct Fixture {
    dialect: Dialect,
    server: NativeHttpServer,
    state: Arc<Mutex<State>>,
    directory: tempfile::TempDir,
}

impl Fixture {
    fn new(dialect: Dialect) -> Self {
        let state = Arc::new(Mutex::new(State {
            dialect,
            objects: HashMap::new(),
            requests: Vec::new(),
            fault: CatalogFault::None,
            omit_receipts: false,
            corrupt_upload: false,
        }));
        let served = Arc::clone(&state);
        Self {
            dialect,
            server: NativeHttpServer::start(move |_, request| {
                let mut state = served.lock().unwrap();
                let lose_reply = matches!(state.fault, CatalogFault::LosePrimaryResponse)
                    && request.method == "PUT"
                    && request
                        .path
                        .ends_with(crate::wal::cloud_catalog::OBJECT_KEY);
                if lose_reply {
                    state.fault = CatalogFault::None;
                }
                let response = respond(&mut state, request);
                if lose_reply {
                    None
                } else {
                    Some(response)
                }
            }),
            state,
            directory: tempfile::tempdir().unwrap(),
        }
    }

    fn object(&self, key: &str) -> (Vec<u8>, usize) {
        self.state.lock().unwrap().objects[&self.dialect.path(key)].clone()
    }

    fn storage(&self) -> CloudPersistence {
        self.storage_with_timeout(Duration::from_secs(5))
    }

    fn storage_with_timeout(&self, timeout: Duration) -> CloudPersistence {
        CloudPersistence::new(Arc::new(
            HybridStorage::with_policy_event_sender_and_limits(
                Arc::new(CloudStorage::new(self.backend(), "db".into())),
                crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
                None,
                crate::storage::hybrid::backend::HybridQueueLimits {
                    callback_timeout: timeout,
                    ..crate::storage::hybrid::backend::HybridQueueLimits::default()
                },
            ),
        ))
    }

    fn backend(&self) -> Arc<dyn crate::storage::cloud::CloudBackend> {
        self.dialect.backend(self.server.endpoint.clone())
    }

    fn assert_recovered_rows(
        &self,
        catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
        expected: u64,
    ) {
        let local: Arc<dyn crate::io::Fs> = Arc::new(crate::io::MockFs::new());
        let remote: Arc<dyn crate::storage::StorageBackend> =
            Arc::new(CloudStorage::new(self.backend(), "db".into()));
        let recovered =
            crate::runtime::cloud_startup::streaming_wal_plan::StreamingCloudWalRecovery::build(
                &local,
                None,
                &remote,
                catalog,
                crate::config::RecoveryPolicy::Strict,
                Duration::from_secs(5),
                127,
                crate::wal::recovery::streaming::StreamingReplayLimits {
                    max_frame_bytes: 128 * 1024,
                    max_pending_txn_bytes: 256 * 1024,
                    max_memtable_encoded_bytes: 256 * 1024,
                    target_memtable_encoded_bytes: 256 * 1024,
                },
            )
            .unwrap();
        let mut memtables =
            HashMap::from([(0, Arc::new(crate::memtable::SkipListMemtable::new()))]);
        let stats = crate::wal::recovery::replay_wal_with_frontiers_within(
            recovered.fs.as_ref(),
            &recovered.segments,
            &mut memtables,
            crate::wal::recovery::ReplayPolicy::Strict,
            &crate::metadata::frontiers::FlushFrontiers::default(),
            None,
        )
        .unwrap();
        assert_eq!(stats.record_count, expected, "{:?}", self.dialect);
        assert_eq!(stats.max_sequence, Some(expected), "{:?}", self.dialect);
        assert_eq!(
            memtables[&0].get_at_seq(b"key", expected).unwrap(),
            Some(b"value".to_vec()),
            "{:?}",
            self.dialect
        );
    }

    fn segment(&self, id: u64, previous: ChainLink) -> (crate::io::FsLocation, ChainLink) {
        let record = crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            bytes::Bytes::from_static(b"key"),
            Some(bytes::Bytes::from_static(b"value")),
            id,
        );
        let (bytes, link) = SegmentBuilder::new(id, 7, previous).record(&record).seal();
        let path = self.directory.path().join(format!("{id}.wal"));
        std::fs::write(&path, bytes).unwrap();
        (crate::io::FsLocation::host_file(&path), link)
    }

    fn try_publish(
        &self,
        storage: &CloudPersistence,
        id: u64,
        previous: ChainLink,
    ) -> crate::common::MidgeResult<ChainLink> {
        let (source, link) = self.segment(id, previous);
        storage.enqueue_wal_segment(&source, id, id).unwrap();
        let uploaded = await_upload(storage, id);
        assert!(
            matches!(uploaded, crate::storage::StorageEvent::CloudAck { .. }),
            "{:?}: {uploaded:?}",
            self.dialect
        );
        storage.publish_remote_wal_segment(id, id, &source, 7, &OperationDeadline::unbounded())?;
        Ok(link)
    }

    fn publish(&self, storage: &CloudPersistence, id: u64, previous: ChainLink) -> ChainLink {
        self.try_publish(storage, id, previous)
            .unwrap_or_else(|error| panic!("{:?}: {error:?}", self.dialect))
    }
}

fn await_upload(storage: &CloudPersistence, id: u64) -> crate::storage::StorageEvent {
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        for event in storage.process_uploads() {
            if matches!(&event,
                crate::storage::StorageEvent::CloudAck { segment_id, .. }
                | crate::storage::StorageEvent::CloudFail { segment_id, .. } if *segment_id == id)
            {
                return event;
            }
        }
        assert!(Instant::now() < until, "upload did not complete");
        std::thread::yield_now();
    }
}

#[test]
fn should_publish_wal_with_three_requests_when_authority_is_warm_on_every_provider() {
    for dialect in Dialect::ALL {
        // Arrange
        let fixture = Fixture::new(dialect);
        let storage = fixture.storage();
        storage.fence_cloud_wal_catalog(7).unwrap();
        let previous = fixture.publish(&storage, 1, ChainLink::GENESIS);
        fixture.state.lock().unwrap().requests.clear();

        // Act
        fixture.publish(&storage, 2, previous);
        let requests = fixture.state.lock().unwrap().requests.clone();

        // Assert
        assert_eq!(
            requests.len(),
            3,
            "{dialect:?}: one WAL PUT and two catalog PUTs: {requests:?}"
        );
        assert!(requests.iter().all(|(method, _)| method == "PUT"));
        drop(storage);
        let reopened = fixture.storage();
        let catalog = reopened.fence_cloud_wal_catalog(8).unwrap();
        reopened.verify_remote_wal_segment(1, 1).unwrap();
        reopened.verify_remote_wal_segment(2, 2).unwrap();
        fixture.assert_recovered_rows(&catalog, 2);
    }
}

#[test]
fn should_reject_cached_publication_when_successor_fences_catalog_on_every_provider() {
    for dialect in Dialect::ALL {
        // Arrange
        let fixture = Fixture::new(dialect);
        let predecessor = fixture.storage();
        predecessor.fence_cloud_wal_catalog(7).unwrap();
        let previous = fixture.publish(&predecessor, 1, ChainLink::GENESIS);
        let successor = fixture.storage();
        successor.fence_cloud_wal_catalog(8).unwrap();
        let before = fixture.object(crate::wal::cloud_catalog::OBJECT_KEY);

        // Act
        let result = fixture.try_publish(&predecessor, 2, previous);

        // Assert
        assert!(
            matches!(result, Err(crate::common::MidgeError::Fenced(_))),
            "{dialect:?}: {result:?}"
        );
        assert_eq!(
            fixture.object(crate::wal::cloud_catalog::OBJECT_KEY),
            before
        );
        successor.verify_remote_wal_segment(1, 1).unwrap();
        assert!(successor.verify_remote_wal_segment(2, 2).is_err());
    }
}

#[test]
fn should_repair_mirror_on_reopen_when_receipted_primary_commits_alone_on_every_provider() {
    for dialect in Dialect::ALL {
        // Arrange
        let fixture = Fixture::new(dialect);
        let storage = fixture.storage();
        storage.fence_cloud_wal_catalog(7).unwrap();
        let previous = fixture.publish(&storage, 1, ChainLink::GENESIS);
        fixture.state.lock().unwrap().fault = CatalogFault::RejectMirror;

        // Act
        let result = fixture.try_publish(&storage, 2, previous);
        assert!(result.is_err(), "{dialect:?}");
        drop(storage);
        let reopened = fixture.storage();
        let catalog = reopened.fence_cloud_wal_catalog(8).unwrap();

        // Assert
        reopened.verify_remote_wal_segment(1, 1).unwrap();
        reopened.verify_remote_wal_segment(2, 2).unwrap();
        fixture.assert_recovered_rows(&catalog, 2);
        let primary = fixture.object(crate::wal::cloud_catalog::OBJECT_KEY).0;
        let mirror = fixture
            .object(crate::wal::cloud_catalog::MIRROR_OBJECT_KEY)
            .0;
        assert_eq!(primary, mirror, "{dialect:?}");
    }
}

#[test]
fn should_verify_remote_bytes_when_provider_omits_checksum_receipts_on_every_provider() {
    for dialect in Dialect::ALL {
        // Arrange
        let fixture = Fixture::new(dialect);
        fixture.state.lock().unwrap().omit_receipts = true;
        let storage = fixture.storage();
        storage.fence_cloud_wal_catalog(7).unwrap();
        fixture.state.lock().unwrap().requests.clear();

        // Act
        fixture.publish(&storage, 1, ChainLink::GENESIS);

        // Assert
        let requests = fixture.state.lock().unwrap().requests.clone();
        assert!(
            requests.iter().any(|(method, key)| method == "GET"
                && std::path::Path::new(key)
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("wal"))),
            "{dialect:?}: {requests:?}"
        );
        drop(storage);
        let reopened = fixture.storage();
        reopened.fence_cloud_wal_catalog(8).unwrap();
        reopened.verify_remote_wal_segment(1, 1).unwrap();
    }
}

#[test]
fn should_refuse_cloud_ack_when_upload_checksum_and_bytes_disagree_on_every_provider() {
    for dialect in Dialect::ALL {
        // Arrange
        let fixture = Fixture::new(dialect);
        let storage = fixture.storage();
        storage.fence_cloud_wal_catalog(7).unwrap();
        fixture.state.lock().unwrap().corrupt_upload = true;
        let (source, _) = fixture.segment(1, ChainLink::GENESIS);

        // Act
        storage.enqueue_wal_segment(&source, 1, 1).unwrap();
        let result = await_upload(&storage, 1);

        // Assert
        assert!(
            matches!(
                result,
                crate::storage::StorageEvent::CloudFail {
                    failure_kind: crate::storage::CloudUploadFailureKind::Corruption,
                    ..
                }
            ),
            "{dialect:?}: {result:?}"
        );
        assert!(storage.verify_remote_wal_segment(1, 1).is_err());
    }
}

#[test]
fn should_reload_authority_on_retry_when_primary_write_response_is_lost_on_every_provider() {
    for dialect in Dialect::ALL {
        // Arrange
        let fixture = Fixture::new(dialect);
        let storage = fixture.storage_with_timeout(Duration::from_secs(1));
        storage.fence_cloud_wal_catalog(7).unwrap();
        let previous = fixture.publish(&storage, 1, ChainLink::GENESIS);
        fixture.state.lock().unwrap().fault = CatalogFault::LosePrimaryResponse;

        // Act
        let uncertain = fixture.try_publish(&storage, 2, previous);
        let retried = fixture.try_publish(&storage, 2, previous);

        // Assert
        assert!(
            uncertain.is_err(),
            "{dialect:?}: lost receipt must not acknowledge a commit"
        );
        assert!(
            retried.is_ok(),
            "{dialect:?}: reload must reconcile the committed primary: {retried:?}"
        );
        drop(storage);
        let reopened = fixture.storage();
        let catalog = reopened.fence_cloud_wal_catalog(8).unwrap();
        fixture.assert_recovered_rows(&catalog, 2);
    }
}

#[test]
fn should_release_upload_workspace_when_control_receipt_is_retained_on_every_provider() {
    for dialect in Dialect::ALL {
        // Arrange
        let fixture = Fixture::new(dialect);
        let storage = fixture.storage();
        let budget = crate::common::resource_budget::ResourceBudget::new(1024);
        let bytes = b"admitted control bytes";

        // Act
        let object = storage
            .write_control_object(
                "control",
                None,
                bytes,
                &budget,
                &OperationDeadline::from_budget(Duration::from_secs(5)),
            )
            .unwrap();

        // Assert
        assert!(object.has_write_receipt(), "{dialect:?}");
        assert_eq!(budget.used(), bytes.len());
        assert_eq!(object.bytes(), bytes);
        drop(object);
        assert_eq!(budget.used(), 0);
    }
}

mod catalog_memory;
