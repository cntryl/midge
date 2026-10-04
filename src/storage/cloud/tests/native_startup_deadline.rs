use super::*;
use crate::common::{MidgeError, OperationDeadline};
use crate::config::CloudProviderConfig;
use crate::storage::cloud::native_http::{observe_cancellation, NativeHttpServer, Response};
use std::sync::{atomic::AtomicBool, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
enum Dialect {
    S3,
    Azure,
    GcsXml,
    GcsJson,
}

impl Dialect {
    fn provider(self) -> CloudProviderConfig {
        match self {
            Self::S3 => CloudProviderConfig::sqrzl_s3("budget-test"),
            Self::Azure => CloudProviderConfig::sqrzl_azure("budget-test"),
            Self::GcsXml => CloudProviderConfig::sqrzl_gcs("budget-test"),
            Self::GcsJson => CloudProviderConfig::sqrzl_gcs_json("budget-test"),
        }
    }

    fn page(self, number: usize, last: bool) -> Vec<u8> {
        let key = format!("native-deadline/object-{number}");
        let token = if last {
            String::new()
        } else {
            format!("page-{number}")
        };
        match self {
            Self::S3 => format!("<ListBucketResult><Contents><Key>{key}</Key></Contents><IsTruncated>{}</IsTruncated><NextContinuationToken>{token}</NextContinuationToken></ListBucketResult>", !last).into_bytes(),
            Self::Azure => format!("<EnumerationResults><Blobs><Blob><Name>{key}</Name></Blob></Blobs><NextMarker>{token}</NextMarker></EnumerationResults>").into_bytes(),
            Self::GcsXml => format!("<ListBucketResult><Contents><Key>{key}</Key></Contents><IsTruncated>{}</IsTruncated><NextMarker>{token}</NextMarker></ListBucketResult>", !last).into_bytes(),
            Self::GcsJson => {
                let mut body = serde_json::json!({ "items": [{ "name": key }] });
                if !last { body["nextPageToken"] = serde_json::json!(token); }
                serde_json::to_vec(&body).unwrap()
            }
        }
    }
}

const DIALECTS: [Dialect; 4] = [
    Dialect::S3,
    Dialect::Azure,
    Dialect::GcsXml,
    Dialect::GcsJson,
];

#[derive(Clone, Copy, Debug)]
enum HeldPath {
    Head,
    List,
    Put,
}

fn held_request_observation(dialect: Dialect, path: HeldPath) -> (bool, bool, bool) {
    let (observation_tx, observation_rx) = mpsc::channel();
    let server = NativeHttpServer::start(move |stream, request| {
        let arrived = match path {
            HeldPath::Head => match dialect {
                Dialect::GcsJson => {
                    request.method == "GET"
                        && request.path.contains("/storage/v1/b/budget-test/o/")
                        && request.path.ends_with("native-deadline%2Fobject")
                }
                Dialect::S3 | Dialect::Azure | Dialect::GcsXml => request.method == "HEAD",
            },
            HeldPath::List => request.method == "GET" && request.path.contains('?'),
            HeldPath::Put => request.method == "PUT" || request.method == "POST",
        };
        let cancelled = observe_cancellation(stream);
        let _ = observation_tx.send((arrived, cancelled));
        (!cancelled).then(|| {
            let body = if matches!(path, HeldPath::List) {
                dialect.page(1, true)
            } else {
                Vec::new()
            };
            Response::new(200, body).with_etag(1)
        })
    });
    let provider = dialect.provider().with_endpoint(&server.endpoint).unwrap();
    let cloud = crate::storage::providers::build_cloud_storage_with_timeout(
        &provider,
        "native-deadline",
        Duration::from_secs(3),
    )
    .unwrap();
    let deadline = OperationDeadline::from_budget(Duration::from_millis(150));
    let io = crate::storage::cloud::BlockingCloud::new(&cloud, &deadline);
    let timed_out = match path {
        HeldPath::Head => matches!(io.head_optional("object"), Err(MidgeError::Timeout(_))),
        HeldPath::List => matches!(io.list("object"), Err(MidgeError::Timeout(_))),
        HeldPath::Put => matches!(
            io.put_with_headers(
                "object",
                b"actual submitted body".to_vec(),
                vec![("If-None-Match".into(), "*".into())],
            ),
            Err(MidgeError::Timeout(_))
        ),
    };
    let (arrived, cancelled) = observation_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("real native request must arrive and finish the held observation");
    // The handler cancelled or released the response before runtime destruction.
    drop(server);
    drop(cloud);
    (timed_out, arrived, cancelled)
}

fn assert_held_deadline(path: HeldPath) {
    let observations = DIALECTS.map(|dialect| (dialect, held_request_observation(dialect, path)));
    for (dialect, (timed_out, arrived, cancelled)) in observations {
        assert!(arrived, "{dialect:?} {path:?}: no actual native request");
        assert!(timed_out, "{dialect:?} {path:?}: caller must time out");
        assert!(
            cancelled,
            "{dialect:?} {path:?}: native request outlived the caller deadline"
        );
    }
}

#[test]
fn should_cancel_native_head_when_blocking_caller_budget_expires() {
    // Arrange: four real protocols use a 3s default and a 150ms caller cap.
    // Act: the real endpoint withholds HEAD responses through the callback wait.
    // Assert: require positive request admission, typed Timeout and socket closure.
    assert_held_deadline(HeldPath::Head);
}

#[test]
fn should_cancel_native_list_when_blocking_caller_budget_expires() {
    // Arrange: four native LIST protocols have a longer default than the caller.
    // Act: hold the first real page response until cancellation is observed.
    // Assert: timeout must cancel the native request, not merely its caller wait.
    assert_held_deadline(HeldPath::List);
}

#[test]
fn should_cancel_native_put_when_blocking_caller_budget_expires() {
    // Arrange: native PUT/POST sends an actual body and conditional headers.
    // Act: withhold its response under a 150ms caller and 3s provider default.
    // Assert: caller Timeout and native cancellation do not prove non-commit.
    assert_held_deadline(HeldPath::Put);
}

const LIST_BUDGET: Duration = Duration::from_secs(2);

#[derive(Default)]
struct PaginationControl {
    started: Mutex<Option<Instant>>,
    requests: AtomicUsize,
    consumed: AtomicUsize,
    cancelled: AtomicBool,
}

impl PaginationControl {
    fn response(
        &self,
        stream: &std::net::TcpStream,
        request: &crate::storage::cloud::native_http::Request,
        dialect: Dialect,
    ) -> Option<Response> {
        assert_eq!(request.method, "GET", "actual native LIST page request");
        let page = self.requests.fetch_add(1, Ordering::AcqRel) + 1;
        assert!(page <= 3, "finite native pagination fixture");
        if page > 1 {
            assert!(
                request.path.contains(&format!("page-{}", page - 1)),
                "a continuation request must consume the preceding real page: {}",
                request.path
            );
            self.consumed.store(page - 1, Ordering::Release);
        }
        let started = *self.started.lock().unwrap();
        if let Some(started) = started {
            if page == 2 {
                // Spend earlier-page work inside the one captured wall budget.
                std::thread::sleep((LIST_BUDGET / 2).saturating_sub(started.elapsed()));
            } else if page == 3 {
                // A reset budget would accept this final page: the previous
                // successful page spent half the original allowance already.
                let release_at = started + LIST_BUDGET + LIST_BUDGET / 4;
                let cancelled = hold_pagination_response(stream, release_at);
                self.cancelled.store(cancelled, Ordering::Release);
                if cancelled {
                    return None;
                }
            }
        }
        Some(Response::new(200, dialect.page(page, page == 3)))
    }

    fn arm(&self, started: Instant) {
        self.requests.store(0, Ordering::Release);
        self.consumed.store(0, Ordering::Release);
        self.cancelled.store(false, Ordering::Release);
        *self.started.lock().unwrap() = Some(started);
    }
}

fn hold_pagination_response(stream: &std::net::TcpStream, release_at: Instant) -> bool {
    stream.set_nonblocking(true).unwrap();
    let cancelled = loop {
        match stream.peek(&mut [0_u8; 1]) {
            Ok(0) => break true,
            Ok(_) => panic!("unexpected client data while holding native LIST response"),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
                ) =>
            {
                break true;
            }
            Err(error) => panic!("observe held native LIST socket: {error}"),
        }
        let remaining = release_at.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break false;
        }
        std::thread::sleep(remaining.min(Duration::from_millis(5)));
    };
    stream.set_nonblocking(false).unwrap();
    cancelled
}

struct PaginationObservation {
    event: CloudEvent,
    admitted: usize,
    consumed: usize,
    cancelled: bool,
    elapsed: Duration,
}

fn pagination_observation(dialect: Dialect) -> PaginationObservation {
    let control = Arc::new(PaginationControl::default());
    let worker_control = Arc::clone(&control);
    let server = NativeHttpServer::start(move |stream, request| {
        worker_control.response(stream, &request, dialect)
    });
    let provider = dialect.provider().with_endpoint(&server.endpoint).unwrap();
    let cloud = crate::storage::providers::build_cloud_storage_with_timeout(
        &provider,
        "native-deadline",
        Duration::from_secs(3),
    )
    .unwrap();
    let mut headers = Vec::new();
    set_request_timeout_header(&mut headers, LIST_BUDGET);
    // Qualify all three real pages and warm the executor before timed phases.
    let (sender, receiver) = mpsc::channel();
    cloud
        .backend
        .submit_list_with_headers(&cloud.full_path(""), headers.clone(), sender);
    let positive = receiver.recv_timeout(Duration::from_secs(6)).unwrap();
    let expected: Vec<_> = (1..=3)
        .map(|page| format!("native-deadline/object-{page}"))
        .collect();
    assert!(
        matches!(&positive, CloudEvent::List { result: Ok(keys), .. } if keys == &expected),
        "{dialect:?}: healthy real pagination must succeed: {positive:?}"
    );
    assert_eq!(control.requests.load(Ordering::Acquire), 3);
    assert_eq!(control.consumed.load(Ordering::Acquire), 2);

    let (sender, receiver) = mpsc::channel();
    let started = Instant::now();
    control.arm(started);
    // One native LIST budget spans the successful prefix and the held final page.
    cloud
        .backend
        .submit_list_with_headers(&cloud.full_path(""), headers, sender);
    let event = receiver.recv_timeout(Duration::from_secs(6)).unwrap();
    let elapsed = started.elapsed();
    // Cancellation or the finite response release always precedes assertions.
    drop(server);
    drop(cloud);
    PaginationObservation {
        event,
        admitted: control.requests.load(Ordering::Acquire),
        consumed: control.consumed.load(Ordering::Acquire),
        cancelled: control.cancelled.load(Ordering::Acquire),
        elapsed,
    }
}

#[test]
fn should_stop_native_pagination_when_aggregate_list_budget_expires() {
    // Arrange: healthy real three-page pagination qualifies each dialect first.
    // Two pages then succeed, spending half one deadline before the held third.
    // Act
    let observations = DIALECTS.map(|dialect| (dialect, pagination_observation(dialect)));
    // Assert
    for (dialect, observation) in observations {
        assert_eq!(
            observation.admitted, 3,
            "{dialect:?}: final page must arrive"
        );
        assert_eq!(
            observation.consumed, 2,
            "{dialect:?}: continuations must acknowledge two successful pages"
        );
        assert!(
            matches!(
                &observation.event,
                CloudEvent::List {
                    result: Err(CloudError::Timeout(_)),
                    ..
                }
            ),
            "{dialect:?}: per-page timeout replenished the LIST budget: {:?}",
            observation.event
        );
        assert!(
            observation.cancelled,
            "{dialect:?}: final page must close before its finite release"
        );
        assert!(
            observation.elapsed >= LIST_BUDGET * 3 / 4
                && observation.elapsed < LIST_BUDGET + Duration::from_secs(1),
            "{dialect:?}: callback must use the original deadline: {:?}",
            observation.elapsed
        );
    }
}

#[derive(Clone, Copy, Debug)]
enum ScopeLifecycle {
    Active,
    Absent,
    Accepted,
}

fn oversized_native_response(
    stream: &mut std::net::TcpStream,
    request: &crate::storage::cloud::native_http::Request,
    dialect: Dialect,
    path: HeldPath,
    observed: &mpsc::Sender<(bool, bool)>,
) -> Option<Response> {
    use std::io::Read;

    let arrived = match path {
        HeldPath::Head => match dialect {
            Dialect::GcsJson => {
                request.method == "GET"
                    && request.path.contains("/storage/v1/b/budget-test/o/")
                    && request.path.ends_with("native-deadline%2Fobject")
            }
            Dialect::S3 | Dialect::Azure | Dialect::GcsXml => request.method == "HEAD",
        },
        HeldPath::Put => {
            (request.method == "PUT" || request.method == "POST")
                && request.body == b"actual oversized native request"
        }
        HeldPath::List => unreachable!("native LIST already bounds its whole page loop"),
    };
    stream
        .set_read_timeout(Some(Duration::from_millis(400)))
        .unwrap();
    let cancelled = match stream.read(&mut [0u8; 1]) {
        Ok(0) => true,
        Ok(_) => false,
        Err(error) => matches!(
            error.kind(),
            std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
        ),
    };
    let _ = observed.send((arrived, cancelled));
    if cancelled {
        return None;
    }
    let body = if matches!((dialect, path), (Dialect::GcsJson, HeldPath::Head)) {
        br#"{"size":"0","etag":"native-cap-etag","generation":"1"}"#.to_vec()
    } else {
        Vec::new()
    };
    let status = if matches!((dialect, path), (Dialect::Azure, HeldPath::Put)) {
        201
    } else {
        200
    };
    let mut response = Response::new(status, body).with_etag(1);
    if matches!((dialect, path), (Dialect::GcsXml, HeldPath::Head)) {
        response
            .headers
            .push(("x-goog-generation".into(), "1".into()));
    }
    Some(response)
}

fn oversized_native_request(
    dialect: Dialect,
    path: HeldPath,
    lifecycle: ScopeLifecycle,
) -> (CloudEvent, bool, bool, Duration) {
    use crate::common::DeadlineScope;

    let (observed_tx, observed_rx) = mpsc::channel();
    let server = NativeHttpServer::start(move |stream, request| {
        oversized_native_response(stream, &request, dialect, path, &observed_tx)
    });
    let provider = dialect.provider().with_endpoint(&server.endpoint).unwrap();
    let original = crate::storage::providers::build_cloud_storage_with_timeout(
        &provider,
        "native-deadline",
        Duration::from_millis(150),
    )
    .unwrap();
    let scope = DeadlineScope::new(OperationDeadline::from_budget(Duration::from_secs(3)));
    let cloud = match lifecycle {
        ScopeLifecycle::Absent => original,
        ScopeLifecycle::Active => Arc::new(original.with_startup_scope(scope.clone())),
        ScopeLifecycle::Accepted => {
            scope.complete().unwrap();
            Arc::new(original.with_startup_scope(scope.clone()))
        }
    };
    let (tx, rx) = mpsc::channel();
    let started = Instant::now();
    match path {
        HeldPath::Head => cloud.submit_head_within("object", Duration::from_secs(2), tx),
        HeldPath::Put => {
            let mut headers = vec![("If-None-Match".into(), "*".into())];
            set_request_timeout_header(&mut headers, Duration::from_secs(2));
            cloud.submit_put(
                "object",
                b"actual oversized native request".to_vec(),
                headers,
                tx,
            );
        }
        HeldPath::List => {
            unreachable!("test exercises explicit duration and mutation-header paths")
        }
    }
    let event = rx
        .recv_timeout(Duration::from_secs(3))
        .expect("finite native callback");
    let elapsed = started.elapsed();
    let (arrived, cancelled) = observed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    drop(server);
    drop(cloud);
    (event, arrived, cancelled, elapsed)
}

#[test]
fn should_cap_scoped_native_requests_when_explicit_timeout_exceeds_ordinary_io_budget() {
    // Arrange: real native defaults are 150ms, while explicit duration/header
    // requests are 2s and the shared active startup scope is 3s.
    let paths = [HeldPath::Head, HeldPath::Put];
    // Act: collect all protocol observations after every finite endpoint joins.
    let observations = DIALECTS.map(|dialect| {
        paths.map(|path| {
            (
                dialect,
                path,
                oversized_native_request(dialect, path, ScopeLifecycle::Active),
            )
        })
    });
    // Assert: active startup uses the minimum of all three caps in native I/O.
    for protocol in observations {
        for (dialect, path, (event, arrived, cancelled, elapsed)) in protocol {
            assert!(
                arrived,
                "{dialect:?} {path:?}: positive native request required"
            );
            assert!(
                matches!(
                    event,
                    CloudEvent::Head {
                        result: CloudOutcome::Err(CloudError::Timeout(_)),
                        ..
                    } | CloudEvent::Put {
                        result: CloudOutcome::Err(CloudError::Timeout(_)),
                        ..
                    }
                ),
                "{dialect:?} {path:?}: {event:?}"
            );
            assert!(
                cancelled,
                "{dialect:?} {path:?}: native request exceeded ordinary cap"
            );
            assert!(
                elapsed < Duration::from_millis(350),
                "{dialect:?} {path:?}: {elapsed:?}"
            );
        }
    }
}

#[test]
fn should_restore_native_override_when_startup_scope_is_absent_or_accepted() {
    // Arrange: ordinary/accepted views retain an explicit 2s request override
    // under the same configured 150ms default, with a real 400ms endpoint hold.
    let lifecycles = [ScopeLifecycle::Absent, ScopeLifecycle::Accepted];
    let paths = [HeldPath::Head, HeldPath::Put];
    // Act: collect every actual success and settle all native workers first.
    let observations = lifecycles.map(|lifecycle| {
        DIALECTS.map(|dialect| {
            paths.map(|path| {
                (
                    lifecycle,
                    dialect,
                    path,
                    oversized_native_request(dialect, path, lifecycle),
                )
            })
        })
    });
    // Assert: accepted startup budget does not freeze a new lifetime cap.
    for lifecycle in observations {
        for protocol in lifecycle {
            for (scope, dialect, path, (event, arrived, cancelled, elapsed)) in protocol {
                assert!(arrived, "{scope:?} {dialect:?} {path:?}");
                assert!(
                    !cancelled,
                    "{scope:?} {dialect:?} {path:?}: override was lost"
                );
                assert!(
                    matches!(
                        event,
                        CloudEvent::Head {
                            result: CloudOutcome::Ok(_),
                            ..
                        } | CloudEvent::Put {
                            result: CloudOutcome::Ok(()),
                            ..
                        }
                    ),
                    "{scope:?} {dialect:?} {path:?}: {event:?}"
                );
                assert!(
                    elapsed >= Duration::from_millis(350),
                    "{scope:?}: {elapsed:?}"
                );
            }
        }
    }
}
