use super::*;
use std::sync::Mutex;

#[derive(Clone, Default)]
struct Output(Arc<Mutex<Vec<u8>>>);

impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn should_record_cancelled_attempt_when_live_response_future_is_dropped() {
    // Arrange
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/object", listener.local_addr().unwrap());
    let (ready, receiving) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_complete_http_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\ndata\r\n")
            .unwrap();
        ready.send(()).unwrap();
        let _ = released.recv_timeout(Duration::from_secs(5));
    });
    let request = CloudRequest::new(Method::GET, endpoint).with_timeout(Duration::from_secs(5));
    let output = Output::default();
    let writer = output.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_env_filter("off,midge::cloud_io=debug")
        .with_writer(move || writer.clone())
        .finish();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    // Act: cancel only after the server confirms an actual request was received.
    tracing::subscriber::with_default(subscriber, || {
        runtime.block_on(async {
        let mut response = Box::pin(CloudExecutor::execute_request(Client::new(), request));
        tokio::select! {
            completed = &mut response => panic!("response must remain incomplete: {}", completed.is_ok()),
            ready = receiving => ready.expect("live response"),
        }
        drop(response);
    });
    });
    release.send(()).unwrap();
    server.join().unwrap();
    let log = String::from_utf8(
        output
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
    )
    .unwrap();

    // Assert
    assert_eq!(
        log.lines()
            .filter(|line| line.contains("midge::cloud_io"))
            .count(),
        1
    );
    assert!(
        log.contains("cancelled=true"),
        "incomplete attempts must remain visible: {log}"
    );
    assert!(
        log.contains("transport_error=false"),
        "cancellation is a distinct outcome"
    );
}

#[test]
fn should_observe_each_http_attempt_when_range_read_retries() {
    // Arrange
    let server =
        crate::storage::providers::test_support::spawn_scripted_http_response_server(vec![
            (503, "text/plain".into(), "retry".into()),
            (206, "text/plain".into(), "value".into()),
        ]);
    let request = CloudRequest::new(
        Method::GET,
        format!("{}/private-key?secret=hidden", server.endpoint),
    )
    .with_header("Range", "bytes=0-4")
    .with_timeout(Duration::from_secs(5));
    let output = Output::default();
    let writer = output.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_env_filter("off,midge::cloud_io=debug")
        .with_writer(move || writer.clone())
        .finish();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    // Act
    let response = tracing::subscriber::with_default(subscriber, || {
        runtime.block_on(CloudExecutor::execute_request(Client::new(), request))
    })
    .expect("successful retry");
    let attempts = server.finish();
    let log = String::from_utf8(
        output
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
    )
    .unwrap();
    let events: Vec<_> = log
        .lines()
        .filter(|line| line.contains("midge::cloud_io"))
        .collect();

    // Assert
    assert_eq!(response.body, b"value");
    assert_eq!(attempts, 2);
    assert_eq!(
        events.len(),
        attempts,
        "every HTTP attempt must be counted: {log}"
    );
    assert!(events[0].contains("status=503"));
    assert!(events[1].contains("status=206"));
    for event in events {
        assert!(event.contains("response_body_bytes=5"));
        assert!(event.contains("range=true"));
        assert!(event.contains("cancelled=false"));
    }
    assert!(!log.contains("private-key"));
    assert!(!log.contains("secret=hidden"));
}
