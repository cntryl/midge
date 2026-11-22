//! Gate real native requests without manufacturing provider successes.
use super::super::startup_deadline::proxy::{forward, read_request};
use serde::Serialize;
use std::io::{self, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, Serialize)]
pub(super) enum Boundary {
    BeforePut,
    DisconnectBeforePut,
    AfterPut,
    LostResponse,
    LostResponseAndReadback,
}

#[derive(Clone, Copy)]
enum Target {
    Wal,
    IntentClearManifest,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct Observation {
    pub method: String,
    pub path: String,
    pub forwarded: bool,
    pub status: Option<u16>,
    pub delivered: bool,
    pub error: Option<String>,
}

#[derive(Default)]
enum Claim {
    #[default]
    Disabled,
    Armed,
    Claimed,
}

#[derive(Default)]
struct GateState {
    claim: Claim,
    reached: Option<Observation>,
    released: bool,
    finished: bool,
    expired: bool,
}

pub(super) struct Control {
    upstream: SocketAddr,
    boundary: Boundary,
    gate: Mutex<GateState>,
    changed: Condvar,
    deny_lease_writes: AtomicBool,
    deny_manifest_readbacks: AtomicBool,
    target: Target,
    observations: Mutex<Vec<Observation>>,
}

impl Control {
    pub fn arm(&self) {
        self.gate.lock().unwrap().claim = Claim::Armed;
    }

    pub fn stop_renewals(&self) {
        self.deny_lease_writes.store(true, Ordering::Release);
    }

    pub fn reached(&self) -> Observation {
        let (state, _) = self
            .changed
            .wait_timeout_while(self.gate.lock().unwrap(), WAIT, |state| {
                state.reached.is_none()
            })
            .unwrap();
        let reached = state.reached.clone();
        drop(state);
        reached.expect("selected provider request boundary must be reached")
    }

    fn hold(&self, observation: &Observation) -> io::Result<()> {
        let mut state = self.gate.lock().unwrap();
        state.reached = Some(observation.clone());
        self.changed.notify_all();
        let (mut state, _) = self
            .changed
            .wait_timeout_while(state, WAIT, |state| !state.released)
            .unwrap();
        if !state.released {
            state.expired = true;
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "fixture gate was not released",
            ));
        }
        Ok(())
    }

    pub fn release(&self) {
        self.gate.lock().unwrap().released = true;
        self.changed.notify_all();
    }

    pub fn wait_finished(&self) {
        let (state, _) = self
            .changed
            .wait_timeout_while(self.gate.lock().unwrap(), WAIT, |state| !state.finished)
            .unwrap();
        let completed = state.finished && !state.expired;
        drop(state);
        assert!(
            completed,
            "gated request must finish without fixture timeout"
        );
    }

    pub fn observations(&self) -> Vec<Observation> {
        self.observations.lock().unwrap().clone()
    }

    pub fn has_reached_boundary(&self) -> bool {
        self.gate.lock().unwrap().reached.is_some()
    }

    fn claim(&self, method: &str, path: &str) -> bool {
        let mut state = self.gate.lock().unwrap();
        let selected = match self.target {
            Target::Wal => method == "PUT" && is_wal_path(path),
            Target::IntentClearManifest => method == "PUT" && path.contains("/manifest/"),
        };
        if matches!(state.claim, Claim::Armed) && selected {
            state.claim = Claim::Claimed;
            true
        } else {
            false
        }
    }

    fn serve(
        &self,
        stream: &mut TcpStream,
        request: &[u8],
        observation: &mut Observation,
        gated: bool,
    ) -> io::Result<()> {
        if self.deny_manifest_readbacks.load(Ordering::Acquire)
            && observation.method == "GET"
            && observation.path.contains("/manifest/")
        {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "immutable manifest readback deliberately disconnected",
            ));
        }
        if self.deny_lease_writes.load(Ordering::Acquire)
            && observation.method == "PUT"
            && observation.path.ends_with("/midge_primary_lease.json")
        {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "predecessor lease request disconnected before forwarding",
            ));
        }
        if gated
            && matches!(
                self.boundary,
                Boundary::BeforePut | Boundary::DisconnectBeforePut
            )
        {
            self.hold(observation)?;
        }
        if gated && matches!(self.boundary, Boundary::DisconnectBeforePut) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "immutable manifest PUT deliberately disconnected before forwarding",
            ));
        }
        // Submission remains observable even when the upstream result is
        // ambiguous. A transport error must not hide a forbidden mutation.
        observation.forwarded = true;
        let response = forward(request, self.upstream)?;
        observation.status = std::str::from_utf8(
            response
                .split(|byte| *byte == b'\n')
                .next()
                .unwrap_or_default(),
        )
        .ok()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse().ok());
        if gated && !matches!(self.boundary, Boundary::BeforePut) {
            self.hold(observation)?;
        }
        if gated
            && matches!(
                self.boundary,
                Boundary::LostResponse | Boundary::LostResponseAndReadback
            )
        {
            if matches!(self.boundary, Boundary::LostResponseAndReadback) {
                self.deny_manifest_readbacks.store(true, Ordering::Release);
            }
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "successful upstream response deliberately disconnected",
            ));
        }
        stream.write_all(&response)?;
        observation.delivered = true;
        Ok(())
    }

    fn handle(&self, mut stream: TcpStream) {
        let Ok((method, path, request)) = read_request(&mut stream) else {
            return;
        };
        let gated = self.claim(&method, &path);
        let mut observation = Observation {
            method,
            path,
            forwarded: false,
            status: None,
            delivered: false,
            error: None,
        };
        if let Err(error) = self.serve(&mut stream, &request, &mut observation, gated) {
            observation.error = Some(error.to_string());
        }
        self.observations.lock().unwrap().push(observation);
        if gated {
            self.gate.lock().unwrap().finished = true;
            self.changed.notify_all();
        }
    }
}

pub(super) struct Proxy {
    pub endpoint: String,
    pub control: Arc<Control>,
    stopping: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
    requests: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl Proxy {
    pub fn start(upstream: SocketAddr, boundary: Boundary) -> Self {
        Self::start_for(upstream, boundary, Target::Wal)
    }

    pub fn start_intent_clear_manifest(upstream: SocketAddr, boundary: Boundary) -> Self {
        Self::start_for(upstream, boundary, Target::IntentClearManifest)
    }

    fn start_for(upstream: SocketAddr, boundary: Boundary, target: Target) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let control = Arc::new(Control {
            upstream,
            boundary,
            gate: Mutex::new(GateState::default()),
            changed: Condvar::new(),
            deny_lease_writes: AtomicBool::new(false),
            deny_manifest_readbacks: AtomicBool::new(false),
            target,
            observations: Mutex::new(Vec::new()),
        });
        let stopping = Arc::new(AtomicBool::new(false));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let worker_control = Arc::clone(&control);
        let worker_stopping = Arc::clone(&stopping);
        let worker_requests = Arc::clone(&requests);
        let worker = std::thread::spawn(move || {
            while !worker_stopping.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let control = Arc::clone(&worker_control);
                        worker_requests
                            .lock()
                            .unwrap()
                            .push(std::thread::spawn(move || control.handle(stream)));
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("authority proxy accept: {error}"),
                }
            }
        });
        Self {
            endpoint,
            control,
            stopping,
            worker: Some(worker),
            requests,
        }
    }

    pub fn allow_pointer_readbacks(&self) {
        self.control
            .deny_manifest_readbacks
            .store(false, Ordering::Release);
    }
}

pub(super) fn is_wal_path(path: &str) -> bool {
    std::path::Path::new(path)
        .extension()
        .is_some_and(|extension| extension == "wal")
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.control.release();
        self.stopping.store(true, Ordering::Release);
        self.worker.take().unwrap().join().unwrap();
        for worker in std::mem::take(&mut *self.requests.lock().unwrap()) {
            worker.join().unwrap();
        }
    }
}
