//! Real provider retries after earlier work under one captured caller clock.
//!
//! Actor tests prove the compaction-to-storage handoff. These TCP controls prove
//! that the native read/retry boundary consumes that inherited clock as well.
//! The executor's outer timeout also bounds retries; these controls observe the
//! effective deadline, rather than independently isolating inner-loop clocks.

use crate::common::{MidgeError, OperationDeadline};
use crate::config::CloudProviderConfig;
use crate::storage::cloud::native_http::{NativeHttpServer, Response};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const CALLER_BUDGET: Duration = Duration::from_secs(2);
const EARLIER_WORK: Duration = Duration::from_millis(600);
const RETRY_STATUS_AT: Duration = Duration::from_millis(1200);
const PROVIDER_CAP: Duration = Duration::from_secs(5);
const FIRST_VALUE: &[u8] = b"genuine earlier provider bytes";
const RETRIED_VALUE: &[u8] = b"genuine retried provider bytes";

#[derive(Default)]
struct RetryEvidence {
    started: Option<Instant>,
    paths: Vec<String>,
    retry_cancelled: bool,
}

fn hold_valid_response(stream: &TcpStream, until: Instant) -> bool {
    stream.set_nonblocking(true).unwrap();
    let cancelled = loop {
        match stream.peek(&mut [0_u8; 1]) {
            Ok(0) => break true,
            Ok(_) => panic!("unexpected client body during a held native GET"),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
                ) =>
            {
                break true;
            }
            Err(error) => panic!("observe native retry cancellation: {error}"),
        }
        let remaining = until.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break false;
        }
        std::thread::sleep(remaining.min(Duration::from_millis(5)));
    };
    stream.set_nonblocking(false).unwrap();
    cancelled
}

struct RetryObservation {
    first: Result<Option<Vec<u8>>, MidgeError>,
    retried: Result<Option<Vec<u8>>, MidgeError>,
    paths: Vec<String>,
    cancelled: bool,
    status_delivered: bool,
    elapsed: Duration,
}

fn observe_retry(provider: CloudProviderConfig, hold_retry: bool) -> RetryObservation {
    let evidence = Arc::new(Mutex::new(RetryEvidence::default()));
    let server_evidence = Arc::clone(&evidence);
    let (delivery, delivered) = std::sync::mpsc::channel();
    let server = NativeHttpServer::start(move |stream, request| {
        assert_eq!(
            request.method, "GET",
            "native provider must submit real GETs"
        );
        let (number, started) = {
            let mut evidence = server_evidence.lock().unwrap();
            evidence.paths.push(request.path);
            (evidence.paths.len(), evidence.started.unwrap())
        };
        match number {
            1 => {
                // Spend real successful provider work before the retrying read.
                std::thread::sleep(EARLIER_WORK.saturating_sub(started.elapsed()));
                Some(Response::new(200, FIRST_VALUE).with_etag(1))
            }
            2 => {
                // Real response work also consumes the read's initial remainder.
                // A retry that renews that remainder would now outlive the hold.
                std::thread::sleep(RETRY_STATUS_AT.saturating_sub(started.elapsed()));
                Some(Response::new(503, b"retry this read").with_delivery_receipt(delivery.clone()))
            }
            3 => {
                let cancelled = hold_retry
                    && hold_valid_response(
                        stream,
                        started + CALLER_BUDGET + Duration::from_millis(400),
                    );
                server_evidence.lock().unwrap().retry_cancelled = cancelled;
                (!cancelled).then(|| Response::new(200, RETRIED_VALUE).with_etag(2))
            }
            _ => panic!("unexpected extra request in finite native retry fixture"),
        }
    });
    let provider = provider.with_endpoint(&server.endpoint).unwrap();
    let storage = crate::storage::providers::build_cloud_storage_with_timeout(
        &provider,
        "inherited-retry",
        PROVIDER_CAP,
    )
    .expect("build actual native provider before starting the caller clock");
    let started = Instant::now();
    let deadline = OperationDeadline::from_start(started, CALLER_BUDGET);
    evidence.lock().unwrap().started = Some(started);
    let io = crate::storage::cloud::BlockingCloud::new(&storage, &deadline);
    let first = io.get_optional("earlier-budget");
    let retried = io.get_optional("retry-budget");
    let elapsed = started.elapsed();
    // The finite hold has an unconditional target. Join before assertions so an
    // intended RED cannot leave a handler or its socket owned by the test.
    drop(server);
    drop(storage);
    let status_delivered = delivered
        .recv_timeout(Duration::from_secs(1))
        .expect("actual complete 503 write receipt");
    let evidence = evidence.lock().unwrap();
    RetryObservation {
        first,
        retried,
        paths: evidence.paths.clone(),
        cancelled: evidence.retry_cancelled,
        status_delivered,
        elapsed,
    }
}

fn providers() -> [(&'static str, CloudProviderConfig); 4] {
    [
        ("s3", CloudProviderConfig::sqrzl_s3("budget-test")),
        ("azure", CloudProviderConfig::sqrzl_azure("budget-test")),
        ("gcs-xml", CloudProviderConfig::sqrzl_gcs("budget-test")),
        (
            "gcs-json",
            CloudProviderConfig::sqrzl_gcs_json("budget-test"),
        ),
    ]
}

fn assert_actual_retry(name: &str, observation: &RetryObservation) {
    assert!(
        matches!(&observation.first, Ok(Some(bytes)) if bytes == FIRST_VALUE),
        "{name}: the earlier request must genuinely succeed"
    );
    assert_eq!(observation.paths.len(), 3, "{name}: real 503 then retry");
    assert!(
        observation.status_delivered,
        "{name}: send complete 503 response"
    );
    assert!(observation.paths[0].contains("earlier-budget"));
    assert!(observation.paths[1].contains("retry-budget"));
    assert_eq!(observation.paths[1], observation.paths[2]);
}

#[test]
fn should_cancel_native_retry_when_earlier_work_consumed_original_budget() {
    // Arrange: four real protocol backends have a 5s provider cap. A successful
    // earlier GET consumes 600ms of the one original 2s caller allowance.
    // Act: send the full 503 at 1.2s, then observe another GET with a held valid
    // response. Renewing its initial remainder at retry admission would outlive
    // the 2.4s hold; retaining the original 2s clock closes its socket in time.
    let observations = providers().map(|(name, provider)| (name, observe_retry(provider, true)));

    // Assert: all protocols reached the actual retry, then cancelled its socket
    // at the inherited boundary; no refreshed retry budget accepted late bytes.
    for (name, observation) in observations {
        assert_actual_retry(name, &observation);
        assert!(matches!(observation.retried, Err(MidgeError::Timeout(_))));
        assert!(observation.cancelled, "{name}: native retry must cancel");
        assert!(observation.elapsed >= CALLER_BUDGET);
        assert!(observation.elapsed < CALLER_BUDGET + Duration::from_secs(1));
    }
}

#[test]
fn should_return_exact_native_retry_bytes_when_original_budget_remains() {
    // Arrange: the same real earlier work and 503/retry path has a fast result.
    // Act: complete the actual retried GET inside the original allowance.
    let observations = providers().map(|(name, provider)| (name, observe_retry(provider, false)));

    // Assert: 503 alone is not a timeout; the original deadline permits exact
    // successful bytes while time remains across the actual two read operations.
    for (name, observation) in observations {
        assert_actual_retry(name, &observation);
        assert!(matches!(observation.retried, Ok(Some(bytes)) if bytes == RETRIED_VALUE));
        assert!(!observation.cancelled);
        assert!(observation.elapsed >= RETRY_STATUS_AT);
        assert!(observation.elapsed < CALLER_BUDGET);
    }
}
