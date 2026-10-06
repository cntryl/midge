//! Real native S3 requests forwarded to Sqrzl without fabricated outcomes.
//! Every client connection is closed after its response so every native request
//! reaches the request gate. Original signed headers and actual response bytes
//! are preserved. Do not log the buffered Authorization header.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const CATALOG: &str = "/wal/publication-catalog.v1.json";
const MIRROR: &str = "/wal/publication-catalog.v1.mirror.json";
const LEASE: &str = "/midge_primary_lease.json";
const SOCKET_WAIT: Duration = Duration::from_secs(4);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DelayMode {
    Fast,
    DiscoveryPair,
    ConfirmedRecovery,
    PostOpenWalOnce,
    CommittedLeaseResponse,
}

#[derive(Clone, Debug)]
pub struct LeasePutProof {
    pub epoch: u64,
    holder: String,
    owner: String,
    acquired: String,
    expires: String,
    metadata: String,
}

impl LeasePutProof {
    fn parse(request: &[u8]) -> Option<Self> {
        let header_end = request.windows(4).position(|bytes| bytes == b"\r\n\r\n")? + 4;
        let body = std::str::from_utf8(&request[header_end..]).ok()?;
        let field = |name: &str| {
            body.lines()
                .find_map(|line| line.strip_prefix(name))
                .map(str::to_string)
        };
        Some(Self {
            epoch: field("fencing_epoch: ")?.parse().ok()?,
            holder: field("holder: ")?,
            owner: field("owner: ")?,
            acquired: field("acquired: ")?,
            expires: field("expires: ")?,
            metadata: field("metadata: ")?,
        })
    }

    fn expired(&self) -> bool {
        chrono::DateTime::parse_from_rfc3339(&self.expires)
            .is_ok_and(|expires| chrono::Utc::now() > expires)
    }

    fn same_owner_and_metadata(&self, other: &Self) -> bool {
        self.epoch == other.epoch
            && self.holder == other.holder
            && self.owner == other.owner
            && self.acquired == other.acquired
            && self.metadata == other.metadata
    }
}

#[derive(Clone, Debug)]
pub struct RequestObservation {
    pub method: String,
    pub path: String,
    pub delayed: bool,
    pub started: Instant,
    pub completed: Instant,
    pub status: Option<u16>,
    pub client_cancelled: bool,
    pub response_written: bool,
    pub error: Option<String>,
    pub lease_put: Option<LeasePutProof>,
    pub condition: LeaseCondition,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseCondition {
    Create,
    Match,
    Unconditional,
}

pub struct Control {
    upstream: SocketAddr,
    mode: Mutex<DelayMode>,
    delayed_count: AtomicUsize,
    pub confirmed_lease_epoch: AtomicU64,
    observations: Mutex<Vec<RequestObservation>>,
    completed: Condvar,
    committed_lease: Mutex<Option<LeasePutProof>>,
    committed_response_hold: Mutex<bool>,
    committed_response_changed: Condvar,
}

impl Control {
    pub fn set_mode(&self, mode: DelayMode) {
        *self.mode.lock().unwrap() = mode;
        self.delayed_count.store(0, Ordering::Release);
        self.confirmed_lease_epoch.store(0, Ordering::Release);
        self.release_committed_response();
    }

    fn delay(&self, method: &str, path: &str) -> Duration {
        if *self.mode.lock().unwrap() == DelayMode::PostOpenWalOnce {
            return if method == "PUT"
                && std::path::Path::new(path).extension() == Some(std::ffi::OsStr::new("wal"))
                && self.delayed_count.fetch_add(1, Ordering::AcqRel) == 0
            {
                Duration::from_millis(800)
            } else {
                Duration::ZERO
            };
        }
        if method != "HEAD" || (!path.ends_with(CATALOG) && !path.ends_with(MIRROR)) {
            return Duration::ZERO;
        }
        match *self.mode.lock().unwrap() {
            DelayMode::Fast | DelayMode::CommittedLeaseResponse => Duration::ZERO,
            DelayMode::DiscoveryPair => {
                if self.delayed_count.fetch_add(1, Ordering::AcqRel) < 2 {
                    Duration::from_millis(350)
                } else {
                    Duration::ZERO
                }
            }
            DelayMode::ConfirmedRecovery => {
                if self.confirmed_lease_epoch.load(Ordering::Acquire) > 0
                    && self.delayed_count.fetch_add(1, Ordering::AcqRel) == 0
                {
                    Duration::from_millis(1100)
                } else {
                    Duration::ZERO
                }
            }
            DelayMode::PostOpenWalOnce => unreachable!("handled before catalog classification"),
        }
    }

    pub fn observations(&self) -> Vec<RequestObservation> {
        self.observations.lock().unwrap().clone()
    }

    pub fn wait_for_delayed_completion(&self, count: usize) -> bool {
        let (events, _) = self
            .completed
            .wait_timeout_while(self.observations.lock().unwrap(), SOCKET_WAIT, |events| {
                events.iter().filter(|event| event.delayed).count() < count
            })
            .unwrap();
        events.iter().filter(|event| event.delayed).count() >= count
    }

    pub fn wait_for_committed_response_completion(&self) -> bool {
        let is_committed_response = |event: &RequestObservation| {
            event.delayed
                && event.method == "PUT"
                && event.path.ends_with(LEASE)
                && event.condition == LeaseCondition::Create
                && event.status == Some(200)
        };
        let (events, _) = self
            .completed
            .wait_timeout_while(self.observations.lock().unwrap(), SOCKET_WAIT, |events| {
                !events.iter().any(is_committed_response)
            })
            .unwrap();
        events.iter().any(is_committed_response)
    }

    fn record(&self, observation: RequestObservation) {
        self.observations.lock().unwrap().push(observation);
        self.completed.notify_all();
    }

    pub fn committed_lease(&self) -> Option<LeasePutProof> {
        self.committed_lease.lock().unwrap().clone()
    }

    pub fn committed_response_is_held(&self) -> bool {
        *self.committed_response_hold.lock().unwrap() && self.committed_lease().is_some()
    }

    pub fn release_committed_response(&self) {
        *self.committed_response_hold.lock().unwrap() = false;
        self.committed_response_changed.notify_all();
    }

    fn hold_committed_response(&self) {
        let (mut held, _) = self
            .committed_response_changed
            .wait_timeout_while(
                self.committed_response_hold.lock().unwrap(),
                Duration::from_millis(1100),
                |held| *held,
            )
            .unwrap();
        *held = false;
    }

    fn should_hold_committed_response(&self, observation: &RequestObservation) -> bool {
        *self.mode.lock().unwrap() == DelayMode::CommittedLeaseResponse
            && observation.method == "PUT"
            && observation.path.ends_with(LEASE)
            && observation.condition == LeaseCondition::Create
            && self.delayed_count.fetch_add(1, Ordering::AcqRel) == 0
    }

    fn should_hold_readback(&self, observation: &RequestObservation) -> bool {
        *self.mode.lock().unwrap() == DelayMode::CommittedLeaseResponse
            && observation.method == "GET"
            && observation.path.ends_with(LEASE)
            && self.committed_response_is_held()
    }

    pub fn wait_for_conditional_cleanup(&self, committed: &LeasePutProof) -> bool {
        let is_cleanup = |observation: &RequestObservation| {
            observation.method == "PUT"
                && observation.path.ends_with(LEASE)
                && observation.condition == LeaseCondition::Match
                && observation.status == Some(200)
                && observation.response_written
                && observation.lease_put.as_ref().is_some_and(|released| {
                    released.expired() && released.same_owner_and_metadata(committed)
                })
        };
        let (events, _) = self
            .completed
            .wait_timeout_while(self.observations.lock().unwrap(), SOCKET_WAIT, |events| {
                !events.iter().any(is_cleanup)
            })
            .unwrap();
        events.iter().any(is_cleanup)
    }
}

pub struct NativeStartupProxy {
    pub endpoint: String,
    pub control: Arc<Control>,
    stopping: Arc<AtomicBool>,
    accept_worker: Option<JoinHandle<()>>,
    request_workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl NativeStartupProxy {
    pub fn start(mode: DelayMode, upstream: SocketAddr) -> io::Result<Self> {
        // Explicit prerequisite: the caller must also invoke require_sqrzl
        // and create a unique bucket with the repository's signed helper.
        TcpStream::connect_timeout(&upstream, Duration::from_millis(200))?;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        listener.set_nonblocking(true)?;
        let control = Arc::new(Control {
            upstream,
            mode: Mutex::new(mode),
            delayed_count: AtomicUsize::new(0),
            confirmed_lease_epoch: AtomicU64::new(0),
            observations: Mutex::new(Vec::new()),
            completed: Condvar::new(),
            committed_lease: Mutex::new(None),
            committed_response_hold: Mutex::new(mode == DelayMode::CommittedLeaseResponse),
            committed_response_changed: Condvar::new(),
        });
        let stopping = Arc::new(AtomicBool::new(false));
        let request_workers = Arc::new(Mutex::new(Vec::new()));
        let worker_control = Arc::clone(&control);
        let worker_stopping = Arc::clone(&stopping);
        let workers = Arc::clone(&request_workers);
        let accept_worker = std::thread::spawn(move || {
            while !worker_stopping.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let control = Arc::clone(&worker_control);
                        let worker = std::thread::spawn(move || handle_request(stream, &control));
                        workers.lock().unwrap().push(worker);
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("native startup proxy accept: {error}"),
                }
            }
        });
        Ok(Self {
            endpoint,
            control,
            stopping,
            accept_worker: Some(accept_worker),
            request_workers,
        })
    }
}

impl Drop for NativeStartupProxy {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        if let Some(worker) = self.accept_worker.take() {
            worker.join().expect("startup proxy accept worker");
        }
        for worker in std::mem::take(&mut *self.request_workers.lock().unwrap()) {
            worker.join().expect("startup proxy request worker");
        }
    }
}

pub(crate) fn read_request(stream: &mut TcpStream) -> io::Result<(String, String, Vec<u8>)> {
    stream.set_nonblocking(false)?; // Accepted sockets inherit flags on some systems.
    stream.set_read_timeout(Some(SOCKET_WAIT))?;
    stream.set_write_timeout(Some(SOCKET_WAIT))?;
    let mut request = Vec::new();
    let mut buffer = [0u8; 4096];
    let header_end = loop {
        if let Some(index) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
            break index + 4;
        }
        let count = stream.read(&mut buffer)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "request header closed",
            ));
        }
        request.extend_from_slice(&buffer[..count]);
        if request.len() > 64 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "header exceeds fixture limit",
            ));
        }
    };
    let header = std::str::from_utf8(&request[..header_end])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid request header"))?;
    let mut lines = header.split("\r\n");
    let mut first = lines.next().unwrap_or_default().split_whitespace();
    let method = first.next().unwrap_or_default().to_string();
    let path = first
        .next()
        .unwrap_or_default()
        .split('?')
        .next()
        .unwrap_or_default()
        .to_string();
    let mut length = 0;
    let mut forwarded = Vec::new();
    for line in header.split("\r\n").filter(|line| !line.is_empty()) {
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("connection") {
                continue;
            }
            if name.eq_ignore_ascii_case("transfer-encoding") {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "native fixture requires length-framed bodies",
                ));
            }
            if name.eq_ignore_ascii_case("content-length") {
                length = value.trim().parse::<usize>().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid content length")
                })?;
            }
        }
        forwarded.extend_from_slice(line.as_bytes());
        forwarded.extend_from_slice(b"\r\n");
    }
    forwarded.extend_from_slice(b"Connection: close\r\n\r\n");
    if length > 8 * 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "body exceeds fixture limit",
        ));
    }
    while request.len() - header_end < length {
        let count = stream.read(&mut buffer)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "request body closed",
            ));
        }
        request.extend_from_slice(&buffer[..count]);
    }
    forwarded.extend_from_slice(&request[header_end..header_end + length]);
    Ok((method, path, forwarded))
}

fn client_closed(stream: &TcpStream) -> io::Result<bool> {
    stream.set_nonblocking(true)?;
    let result = match stream.peek(&mut [0u8; 1]) {
        Ok(0) => Ok(true),
        Ok(_) => Ok(false),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(false),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted
            ) =>
        {
            Ok(true)
        }
        Err(error) => Err(error),
    };
    stream.set_nonblocking(false)?;
    result
}

pub(crate) fn forward(request: &[u8], address: SocketAddr) -> io::Result<Vec<u8>> {
    let mut upstream = TcpStream::connect_timeout(&address, SOCKET_WAIT)?;
    upstream.set_read_timeout(Some(SOCKET_WAIT))?;
    upstream.set_write_timeout(Some(SOCKET_WAIT))?;
    upstream.write_all(request)?;
    let mut response = Vec::new();
    upstream.read_to_end(&mut response)?;
    Ok(response)
}

fn handle_request(mut stream: TcpStream, control: &Control) {
    let started = Instant::now();
    let Ok((method, path, request)) = read_request(&mut stream) else {
        return;
    };
    let delay = control.delay(&method, &path);
    std::thread::sleep(delay);
    let lease_put = (method == "PUT" && path.ends_with(LEASE))
        .then(|| LeasePutProof::parse(&request))
        .flatten();
    let mut observation = RequestObservation {
        method,
        path,
        delayed: !delay.is_zero(),
        started,
        completed: Instant::now(),
        status: None,
        client_cancelled: false,
        response_written: false,
        error: None,
        lease_put,
        condition: if has_header(&request, "If-None-Match") {
            LeaseCondition::Create
        } else if has_header(&request, "If-Match") {
            LeaseCondition::Match
        } else {
            LeaseCondition::Unconditional
        },
    };
    if let Err(error) = serve_response(&mut stream, control, &request, &mut observation) {
        observation.error = Some(error.to_string());
    }
    observation.completed = Instant::now();
    control.record(observation);
}

fn has_header(request: &[u8], name: &str) -> bool {
    let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") else {
        return false;
    };
    std::str::from_utf8(&request[..end]).is_ok_and(|header| {
        header.lines().any(|line| {
            line.split_once(':')
                .is_some_and(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
        })
    })
}

fn serve_response(
    stream: &mut TcpStream,
    control: &Control,
    request: &[u8],
    observation: &mut RequestObservation,
) -> io::Result<()> {
    let hold_response = control.should_hold_committed_response(observation);
    if control.should_hold_readback(observation) {
        observation.delayed = true;
        control.hold_committed_response();
    }
    if !hold_response && client_closed(stream)? {
        observation.client_cancelled = true;
        return Ok(());
    }
    let response = forward(request, control.upstream)?;
    observation.status = std::str::from_utf8(
        response
            .split(|byte| *byte == b'\n')
            .next()
            .unwrap_or_default(),
    )
    .ok()
    .and_then(|line| line.split_whitespace().nth(1))
    .and_then(|status| status.parse().ok());
    if hold_response && observation.status == Some(200) {
        observation.delayed = true;
        (*control.committed_lease.lock().unwrap()).clone_from(&observation.lease_put);
        control.hold_committed_response();
    }
    if client_closed(stream)? {
        observation.client_cancelled = true;
    } else {
        observation.response_written = stream.write_all(&response).is_ok();
        observation.client_cancelled = !observation.response_written;
    }
    Ok(())
}
