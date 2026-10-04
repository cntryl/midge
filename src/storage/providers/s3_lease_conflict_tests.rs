//! Exercise lease conflict classification through the actual S3 HTTP adapter.

use crate::lease::{CloudLeaseConfig, CloudStorageLease, PrimaryLease};
use crate::storage::cloud::{CloudBackend, CloudEvent, CloudStorage, MockCloudBackend};
use std::fmt::Write as _;
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
enum PutFault {
    #[default]
    None,
    RejectAndFailRead,
    ReplaceOwner,
    ApplyThenServerError {
        fail_read: bool,
    },
}

#[derive(Default)]
struct FaultState {
    conflicts_remaining: usize,
    conflict_code: &'static str,
    put_fault: PutFault,
    fail_next_lease_read: bool,
    rejected_puts: usize,
    conditional_lease_puts: usize,
    accepted_lease_updates: usize,
    failed_reads: usize,
}

struct LeaseHttpServer {
    endpoint: String,
    state: Arc<Mutex<FaultState>>,
    stopped: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
    cache: tempfile::TempDir,
}

impl LeaseHttpServer {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind lease HTTP server");
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(FaultState::default()));
        let server_state = Arc::clone(&state);
        let stopped = Arc::new(AtomicBool::new(false));
        let server_stopped = Arc::clone(&stopped);
        let handle = std::thread::spawn(move || {
            let backend = MockCloudBackend::new();
            while !server_stopped.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let (method, key, headers, body) = read_request(&mut stream);
                        let response =
                            respond(&backend, &server_state, &method, &key, headers, body);
                        let _ = stream.write_all(&response);
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("accept lease HTTP request: {error}"),
                }
            }
        });
        Self {
            endpoint,
            state,
            stopped,
            handle: Some(handle),
            cache: tempfile::tempdir().unwrap(),
        }
    }

    fn acquire(&self) -> Arc<CloudStorageLease> {
        let cloud = Arc::new(CloudStorage::new(
            super::contract_test_backend(self.endpoint.clone()),
            "midge".into(),
        ));
        let lease = Arc::new(CloudStorageLease::new_provider_backed(
            CloudLeaseConfig {
                bucket: "bucket".into(),
                prefix: "midge".into(),
            },
            self.cache.path().to_path_buf(),
            cloud,
        ));
        let _guard = Arc::clone(&lease)
            .try_acquire()
            .expect("acquire HTTP lease");
        lease
    }
}

impl Drop for LeaseHttpServer {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let result = handle.join();
            if !std::thread::panicking() {
                result.expect("lease HTTP server panicked");
            }
        }
    }
}

type RequestParts = (String, String, Vec<(String, String)>, Vec<u8>);

fn read_request(stream: &mut TcpStream) -> RequestParts {
    // macOS inherits the listener's nonblocking mode on accepted sockets.
    // Reading the complete request needs a blocking socket with a deadline.
    stream
        .set_nonblocking(false)
        .expect("use bounded blocking request reads");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut bytes = Vec::new();
    let mut chunk = [0; 4096];
    let (header_end, body_len) = loop {
        if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            let header_end = end + 4;
            let head = String::from_utf8_lossy(&bytes[..header_end]);
            let body_len = head
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .map_or(0, |(_, value)| value.trim().parse::<usize>().unwrap());
            if bytes.len() >= header_end + body_len {
                break (header_end, body_len);
            }
        }
        let read = stream.read(&mut chunk).expect("read HTTP request");
        assert!(read > 0, "request ended before body");
        bytes.extend_from_slice(&chunk[..read]);
    };
    let head = String::from_utf8_lossy(&bytes[..header_end]);
    let mut lines = head.lines();
    let mut request = lines.next().unwrap().split_whitespace();
    let method = request.next().unwrap().to_string();
    let key = request.next().unwrap().to_string();
    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| {
            // The fixture emits real quoted HTTP entity tags, while the
            // deterministic object backend stores their unquoted token.
            (name.to_string(), value.trim().trim_matches('"').to_string())
        })
        .collect();
    (
        method,
        key,
        headers,
        bytes[header_end..header_end + body_len].to_vec(),
    )
}

fn http_response(status: u16, etag: Option<&str>, body: &[u8]) -> Vec<u8> {
    let mut response = format!(
        "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    if let Some(etag) = etag {
        write!(response, "ETag: \"{etag}\"\r\n").expect("format entity tag");
    }
    response.push_str("\r\n");
    let mut bytes = response.into_bytes();
    bytes.extend_from_slice(body);
    bytes
}

fn xml_error(status: u16, code: &str) -> Vec<u8> {
    http_response(
        status,
        None,
        format!("<Error><Code>{code}</Code></Error>").as_bytes(),
    )
}

fn receive_event(receiver: &std::sync::mpsc::Receiver<CloudEvent>) -> CloudEvent {
    receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("fixture callback")
}

fn respond(
    backend: &MockCloudBackend,
    state: &Mutex<FaultState>,
    method: &str,
    key: &str,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
) -> Vec<u8> {
    let lease_key = key.ends_with(crate::cloud_layout::CloudObjectLayout::LEASE_OBJECT_KEY);
    let conditional_lease_update = method == "PUT"
        && lease_key
        && headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("if-match"));
    let mut state = state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if method == "GET" && lease_key && state.fail_next_lease_read {
        state.fail_next_lease_read = false;
        state.failed_reads += 1;
        return xml_error(400, "InvalidRequest");
    }
    if conditional_lease_update {
        state.conditional_lease_puts += 1;
        if state.conflicts_remaining > 0 {
            state.conflicts_remaining -= 1;
            state.rejected_puts += 1;
            return xml_error(
                409,
                if state.conflict_code.is_empty() {
                    "ConditionalRequestConflict"
                } else {
                    state.conflict_code
                },
            );
        }
        if matches!(
            state.put_fault,
            PutFault::RejectAndFailRead | PutFault::ReplaceOwner
        ) {
            let replace_owner = matches!(state.put_fault, PutFault::ReplaceOwner);
            state.put_fault = PutFault::None;
            state.fail_next_lease_read = !replace_owner;
            state.rejected_puts += 1;
            replace_lease_for_rejection(backend, key, replace_owner);
            return xml_error(412, "PreconditionFailed");
        }
    }
    let (tx, rx) = std::sync::mpsc::channel();
    match method {
        "GET" => backend.submit_get_with_metadata(key, tx),
        "PUT" => backend.submit_put(key, body, headers, tx),
        other => panic!("unexpected fixture method {other}"),
    }
    match receive_event(&rx) {
        CloudEvent::GetWithMetadata {
            result: Ok((body, metadata)),
            ..
        } => http_response(200, Some(&metadata.etag), &body),
        CloudEvent::GetWithMetadata { result: Err(_), .. } => xml_error(404, "NoSuchKey"),
        CloudEvent::Put { result: Ok(()), .. } => {
            if conditional_lease_update {
                state.accepted_lease_updates += 1;
                if let PutFault::ApplyThenServerError { fail_read } = state.put_fault {
                    state.put_fault = PutFault::None;
                    state.fail_next_lease_read = fail_read;
                    return xml_error(503, "ServiceUnavailable");
                }
            }
            http_response(200, None, b"")
        }
        CloudEvent::Put { result: Err(_), .. } => xml_error(412, "PreconditionFailed"),
        event => panic!("unexpected fixture event {event:?}"),
    }
}

fn replace_lease_for_rejection(backend: &MockCloudBackend, key: &str, replace_owner: bool) {
    // A concurrent same-owner write changes the ETag without changing
    // ownership. The rejected renewal itself never applies.
    let (tx, rx) = std::sync::mpsc::channel();
    backend.submit_get(key, tx);
    let CloudEvent::Get {
        result: Ok(current),
        ..
    } = receive_event(&rx)
    else {
        panic!("lease exists before rejection");
    };
    let generation = uuid::Uuid::new_v4();
    let objects = [
        crate::metadata::files::FORMAT,
        crate::metadata::files::MANIFEST_SNAPSHOT,
    ]
    .map(|name| {
        serde_json::json!({
            "file_name": name,
            "object_key": format!("metadata/generations/{generation}/{name}"),
            "len": 0,
            "crc32c": 0,
        })
    });
    let pointer = serde_json::json!({"manifest_sequence": 1, "objects": objects});
    let current = String::from_utf8(current).unwrap();
    assert!(current.contains("metadata: null\n"));
    let current = if replace_owner {
        current
            .lines()
            .map(|line| {
                if line.starts_with("holder:") {
                    "holder: foreign-holder@host"
                } else if line.starts_with("owner:") {
                    "owner: foreign-token"
                } else {
                    line
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n"
    } else {
        current.replace("metadata: null\n", &format!("metadata: {pointer}\n"))
    };
    let (tx, rx) = std::sync::mpsc::channel();
    backend.submit_put(key, current.into_bytes(), Vec::new(), tx);
    assert!(matches!(
        receive_event(&rx),
        CloudEvent::Put { result: Ok(()), .. }
    ));
}

#[test]
fn should_retry_provider_renewal_after_exhausted_s3_conditional_conflicts() {
    for code in ["ConditionalRequestConflict", "OperationAborted"] {
        assert_renewal_survives_exhausted_conflicts(code);
    }
}

fn assert_renewal_survives_exhausted_conflicts(code: &'static str) {
    // Arrange: four definite rejections exhaust the executor's retry count;
    // the lease stays unchanged and the next fresh ownership check can succeed.
    let server = LeaseHttpServer::start();
    let lease = server.acquire();
    let epoch = lease.epoch();
    {
        let mut state = server
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.conflicts_remaining = 4;
        state.conflict_code = code;
    }

    // Act
    let renewed = lease.renew();

    // Assert
    assert!(
        renewed.is_ok(),
        "definite CAS rejection fenced valid ownership: {renewed:?}"
    );
    assert!(lease.lease_validity().remaining(epoch).is_ok());
    let state = server
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(state.rejected_puts, 4);
    assert_eq!(state.accepted_lease_updates, 1);
}

#[test]
fn should_keep_provider_renewal_valid_when_rejected_s3_cas_readback_fails() {
    // Arrange: a definite 412 follows a same-owner ETag change, then one
    // ordinary lease GET fails. Local lease validity still has ample time.
    let server = LeaseHttpServer::start();
    let lease = server.acquire();
    let epoch = lease.epoch();
    server
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .put_fault = PutFault::RejectAndFailRead;

    // Act
    let first_renewal = lease.renew();
    let validity = lease.lease_validity().remaining(epoch);

    // Assert: a failed read after a rejected PUT cannot make that PUT ambiguous.
    assert!(
        validity.is_ok(),
        "definite 412 invalidated ownership: {first_renewal:?}; {validity:?}"
    );
    if first_renewal.is_err() {
        lease
            .renew()
            .expect("retry renewal after transient read error");
    }
    let state = server
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(state.rejected_puts, 1);
    assert_eq!(state.failed_reads, 1);
    assert_eq!(state.accepted_lease_updates, 1);
}

#[test]
fn should_fence_provider_renewal_when_rejected_s3_cas_reveals_another_owner() {
    // Arrange: the rejected renewal does not apply; its fresh ownership read
    // reveals a foreign holder and token. It cannot retry that holder's CAS.
    let server = LeaseHttpServer::start();
    let lease = server.acquire();
    let epoch = lease.epoch();
    server
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .put_fault = PutFault::ReplaceOwner;

    // Act
    let renewed = lease.renew();

    // Assert
    assert!(
        matches!(renewed, Err(crate::lease::LeaseError::RenewalFailed(_))),
        "foreign ownership must be terminal: {renewed:?}"
    );
    assert!(lease.lease_validity().remaining(epoch).is_err());
    let state = server
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(state.conditional_lease_puts, 1);
    assert_eq!(state.rejected_puts, 1);
    assert_eq!(state.accepted_lease_updates, 0);
}

#[test]
fn should_confirm_applied_s3_renewal_without_replaying_ambiguous_server_error() {
    // Arrange: the provider applies one PUT, but returns a 503. An exact
    // readback can confirm the original write; a second PUT would be unsafe.
    let server = LeaseHttpServer::start();
    let lease = server.acquire();
    let epoch = lease.epoch();
    server
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .put_fault = PutFault::ApplyThenServerError { fail_read: false };

    // Act
    let renewed = lease.renew();

    // Assert
    assert!(renewed.is_ok(), "{renewed:?}");
    assert!(lease.lease_validity().remaining(epoch).is_ok());
    let state = server
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(state.conditional_lease_puts, 1);
    assert_eq!(state.accepted_lease_updates, 1);
}

#[test]
fn should_fence_provider_renewal_when_s3_write_and_readback_are_ambiguous() {
    // Arrange: the PUT applies with a lost success response and readback fails.
    let server = LeaseHttpServer::start();
    let lease = server.acquire();
    let epoch = lease.epoch();
    {
        let mut state = server
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.put_fault = PutFault::ApplyThenServerError { fail_read: true };
    }

    // Act
    let renewed = lease.renew();

    // Assert: definite-conflict handling must preserve fail-closed behavior
    // for a write whose commit outcome cannot be established.
    assert!(renewed.is_err());
    assert!(lease.lease_validity().remaining(epoch).is_err());
    let state = server
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(state.failed_reads, 1);
}
