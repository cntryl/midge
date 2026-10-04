//! Small real TCP endpoint for native request-budget regressions.

use std::fmt::Write as FmtWrite;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

pub(crate) struct Request {
    pub(crate) method: String,
    pub(crate) path: String,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Vec<u8>,
}

pub(crate) struct Response {
    pub(crate) status: u16,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Vec<u8>,
}

impl Response {
    pub(crate) fn new(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: body.into(),
        }
    }

    pub(crate) fn with_etag(mut self, version: usize) -> Self {
        self.headers.push(("ETag".into(), format!("\"{version}\"")));
        self
    }
}

pub(crate) struct NativeHttpServer {
    pub(crate) endpoint: String,
    stopping: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl NativeHttpServer {
    pub(crate) fn start(
        mut handler: impl FnMut(&mut TcpStream, Request) -> Option<Response> + Send + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind native deadline server");
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let stopping = Arc::new(AtomicBool::new(false));
        let worker_stopping = Arc::clone(&stopping);
        let worker = std::thread::spawn(move || {
            while !worker_stopping.load(Ordering::Acquire) {
                let mut stream = match listener.accept() {
                    Ok((stream, _)) => stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    Err(error) => panic!("accept native deadline request: {error}"),
                };
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let Some(request) = read_request(&mut stream) else {
                    continue;
                };
                if let Some(response) = handler(&mut stream, request) {
                    write_response(&mut stream, response);
                }
            }
        });
        Self {
            endpoint,
            stopping,
            worker: Some(worker),
        }
    }
}

impl Drop for NativeHttpServer {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            worker.join().expect("native deadline server panicked");
        }
    }
}

fn read_request(stream: &mut TcpStream) -> Option<Request> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 2048];
    let header_end = loop {
        let count = stream.read(&mut buffer).ok()?;
        if count == 0 {
            return None;
        }
        bytes.extend_from_slice(&buffer[..count]);
        if let Some(index) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            break index + 4;
        }
        assert!(bytes.len() <= 64 * 1024, "bounded test request headers");
    };
    let header_text = std::str::from_utf8(&bytes[..header_end]).ok()?;
    let mut lines = header_text.lines();
    let mut first = lines.next()?.split_whitespace();
    let method = first.next()?.to_string();
    let path = first.next()?.to_string();
    let headers: Vec<_> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_string()))
        .collect();
    let length: usize = headers
        .iter()
        .find(|(name, _)| name == "content-length")
        .map_or(Some(0), |(_, value)| value.parse().ok())?;
    assert!(length <= 1024 * 1024, "bounded test request body");
    while bytes.len() - header_end < length {
        let count = stream.read(&mut buffer).ok()?;
        if count == 0 {
            return None;
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
    Some(Request {
        method,
        path,
        headers,
        body: bytes[header_end..header_end + length].to_vec(),
    })
}

fn write_response(stream: &mut TcpStream, response: Response) {
    let mut headers = format!(
        "HTTP/1.1 {} Test\r\nContent-Length: {}\r\nConnection: close\r\n",
        response.status,
        response.body.len()
    );
    for (name, value) in response.headers {
        write!(headers, "{name}: {value}\r\n").expect("format native HTTP response header");
    }
    headers.push_str("\r\n");
    let _ = stream.write_all(headers.as_bytes());
    let _ = stream.write_all(&response.body);
}

/// Observe socket cancellation without sending the held response first.
pub(crate) fn observe_cancellation(stream: &mut TcpStream) -> bool {
    stream
        .set_read_timeout(Some(Duration::from_millis(600)))
        .unwrap();
    let mut byte = [0_u8; 1];
    match stream.read(&mut byte) {
        Ok(0) => true,
        Err(error) => matches!(
            error.kind(),
            std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::UnexpectedEof
        ),
        Ok(_) => false,
    }
}
