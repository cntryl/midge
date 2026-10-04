use super::*;
use crate::config::CloudProviderConfig;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::AtomicBool;
use std::thread::JoinHandle;
use std::time::Duration;

struct DelayedGetServer {
    endpoint: String,
    request_arrived: mpsc::Receiver<String>,
    observation: mpsc::Receiver<bool>,
    release: mpsc::Sender<()>,
    stopping: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl DelayedGetServer {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind delayed GET server");
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let (request_sender, request_arrived) = mpsc::channel();
        let (observation_sender, observation) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let stopping = Arc::new(AtomicBool::new(false));
        let worker_stopping = Arc::clone(&stopping);
        let worker = std::thread::spawn(move || {
            let mut stream = loop {
                if worker_stopping.load(Ordering::Acquire) {
                    return;
                }
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("accept delayed GET: {error}"),
                }
            };
            stream.set_nonblocking(false).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut request = Vec::new();
            let mut buffer = [0u8; 1024];
            while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                match stream.read(&mut buffer) {
                    Ok(0) | Err(_) => return,
                    Ok(count) => request.extend_from_slice(&buffer[..count]),
                }
            }
            let _ = request_sender.send(String::from_utf8_lossy(&request).into_owned());
            // The response stays withheld. Only cancellation of the native
            // provider request can close this connection before the gate opens.
            let cancelled = match stream.read(&mut buffer) {
                Ok(0) => true,
                Err(error) => matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::UnexpectedEof
                ),
                Ok(_) => false,
            };
            let _ = observation_sender.send(cancelled);
            let _ = released.recv();
            if !cancelled {
                let _ = stream.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nETag: \"version\"\r\nConnection: close\r\n\r\n",
                );
            }
        });
        Self {
            endpoint,
            request_arrived,
            observation,
            release,
            stopping,
            worker: Some(worker),
        }
    }
}

impl Drop for DelayedGetServer {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        let _ = self.release.send(());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[derive(Clone, Copy)]
enum ReadPath {
    Metadata,
    Provider,
    Blocking,
}

fn get_deadline_observation(provider: CloudProviderConfig, path: ReadPath) -> (bool, bool, bool) {
    let server = DelayedGetServer::start();
    let provider = provider
        .with_endpoint(&server.endpoint)
        .expect("local endpoint");
    let storage = crate::storage::providers::build_cloud_storage_with_timeout(
        &provider,
        "deadline-proof",
        Duration::from_secs(10),
    )
    .expect("native cloud backend");
    let caller_timed_out = match path {
        ReadPath::Metadata => {
            let (sender, receiver) = mpsc::channel();
            StorageBackend::submit_metadata_read_request(
                storage.as_ref(),
                crate::storage::StorageRequest::new(
                    "lease.json",
                    crate::common::OperationDeadline::from_budget(Duration::from_millis(500)),
                    Duration::from_secs(10),
                ),
                sender,
            );
            matches!(
                receiver.recv_timeout(Duration::from_secs(1)),
                Ok(Err(error)) if error.kind() == crate::storage::StorageErrorKind::Timeout
            )
        }
        ReadPath::Provider => {
            let (sender, receiver) = mpsc::channel();
            storage.submit_get_within("lease.json", Duration::from_millis(500), sender);
            matches!(
                receiver.recv_timeout(Duration::from_secs(1)),
                Ok(CloudEvent::Get {
                    result: Err(CloudError::Timeout(_)),
                    ..
                })
            )
        }
        ReadPath::Blocking => {
            let deadline =
                crate::common::OperationDeadline::from_budget(Duration::from_millis(500));
            matches!(
                crate::storage::cloud::BlockingCloud::new(storage.as_ref(), &deadline)
                    .get_optional("lease.json"),
                Err(crate::common::MidgeError::Timeout(_))
            )
        }
    };
    let request_arrived = matches!(
        server.request_arrived.recv_timeout(Duration::from_secs(3)),
        Ok(request) if request.starts_with("GET ")
    );
    let native_cancelled = matches!(
        server.observation.recv_timeout(Duration::from_secs(3)),
        Ok(true)
    );
    // Release the server even in the red case before dropping its runtime.
    drop(server);
    drop(storage);
    (caller_timed_out, request_arrived, native_cancelled)
}

fn assert_native_deadlines(path: ReadPath) {
    let providers = [
        ("s3", CloudProviderConfig::sqrzl_s3("budget-test")),
        ("azure", CloudProviderConfig::sqrzl_azure("budget-test")),
        ("gcs-xml", CloudProviderConfig::sqrzl_gcs("budget-test")),
        (
            "gcs-json",
            CloudProviderConfig::sqrzl_gcs_json("budget-test"),
        ),
    ];
    let observations =
        providers.map(|(name, provider)| (name, get_deadline_observation(provider, path)));
    for (name, (caller_timed_out, request_arrived, native_cancelled)) in observations {
        assert!(request_arrived, "{name}: server never observed a GET");
        assert!(caller_timed_out, "{name}: read must time out");
        assert!(
            native_cancelled,
            "{name}: native GET outlived the caller deadline"
        );
    }
}

#[test]
fn should_cancel_native_metadata_get_when_operation_deadline_expires() {
    // Arrange: every native backend has a 10s default, while the typed read's
    // deadline is 500ms. Its server never answers until after observation.
    // Act: exercise the typed metadata-read path against each native backend.
    // Assert: require both caller Timeout and native socket closure.
    assert_native_deadlines(ReadPath::Metadata);
}

#[test]
fn should_cancel_native_get_when_caller_timeout_expires() {
    // Arrange: the ordinary GET uses the same native protocol backends and
    // delayed server as the metadata GET, under a short request budget.
    // Act: exercise ordinary GET through each native backend.
    // Assert: require both a typed provider Timeout and socket closure.
    assert_native_deadlines(ReadPath::Provider);
}

#[test]
fn should_cancel_native_blocking_get_when_operation_deadline_expires() {
    // Arrange: the synchronous adapter has a 500ms deadline, while each native
    // provider has a 10s default and its server withholds the GET response.
    // Act: exercise BlockingCloud GET through each native backend.
    // Assert: require the caller Timeout and cancellation of its GET.
    assert_native_deadlines(ReadPath::Blocking);
}
