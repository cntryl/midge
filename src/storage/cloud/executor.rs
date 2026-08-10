use crate::common::{MidgeError, MidgeResult};
use crate::storage::cloud::{CloudCallback, CloudEvent};
use reqwest::{Client, Method};
use std::sync::Arc;
use std::time::Duration;

const MAX_TRANSIENT_RETRIES: u32 = 3;
const TRANSIENT_BACKOFF_BASE_MS: u64 = 50;
pub(crate) const MAX_CLOUD_RESPONSE_BYTES: usize = 1024 * 1024 * 1024;

/// Represents a generic HTTP request issued by cloud providers.
#[derive(Clone)]
pub struct CloudRequest {
    pub method: Method,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
    pub timeout: Option<Duration>,
}

impl CloudRequest {
    pub fn new(method: Method, url: String) -> Self {
        Self {
            method,
            url,
            headers: Vec::new(),
            body: None,
            timeout: None,
        }
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    pub fn with_body(mut self, body: Vec<u8>) -> Self {
        self.body = Some(body);
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    fn is_conditional_mutation(&self) -> bool {
        if !matches!(self.method, Method::PUT | Method::POST | Method::DELETE) {
            return false;
        }
        self.headers.iter().any(|(name, _)| {
            name.eq_ignore_ascii_case("if-match")
                || name.eq_ignore_ascii_case("if-none-match")
                || name.eq_ignore_ascii_case("x-goog-if-generation-match")
                || name.eq_ignore_ascii_case("x-goog-if-generation-not-match")
        }) || self.url.contains("ifGenerationMatch=")
            || self.url.contains("ifGenerationNotMatch=")
    }
}

/// Minimal response representation returned by the executor.
pub struct CloudResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Signing abstraction used by different providers.
pub trait CloudSigner: Send + Sync {
    fn sign(&self, request: &mut CloudRequest) -> MidgeResult<()>;
}

/// Cloud executor that runs async HTTP requests.
///
/// Embeds a single-threaded tokio runtime to execute HTTP operations
/// without blocking the Midge synchronous runtime.
/// CRITICAL: All cloud operations happen inside this embedded runtime.
pub struct CloudExecutor {
    client: Client,
    signer: Option<Arc<dyn CloudSigner>>,
    rt: Option<Arc<tokio::runtime::Runtime>>,
}

impl CloudExecutor {
    pub fn new(signer: Option<Arc<dyn CloudSigner>>) -> MidgeResult<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .map_err(|e| {
                MidgeError::Internal(format!("Failed to build cloud tokio runtime: {e}"))
            })?;

        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| {
                MidgeError::Internal(format!("Failed to build cloud HTTP client: {error}"))
            })?;

        Ok(Self {
            client,
            signer,
            rt: Some(Arc::new(rt)),
        })
    }

    fn sign_request(&self, request: &mut CloudRequest) -> MidgeResult<()> {
        if let Some(signer) = &self.signer {
            signer.sign(request)
        } else {
            Ok(())
        }
    }

    pub fn spawn_request<F>(
        &self,
        mut request: CloudRequest,
        context: String,
        callback: CloudCallback,
        mapper: F,
    ) where
        F: FnOnce(String, MidgeResult<CloudResponse>) -> CloudEvent + Send + 'static,
    {
        if let Err(err) = self.sign_request(&mut request) {
            let event = mapper(context, Err(err));
            let _ = callback.send(event);
            return;
        }

        let client = self.client.clone();

        let Some(rt) = self.rt.as_ref() else {
            let event = mapper(
                context,
                Err(MidgeError::Internal(
                    "cloud executor runtime is shut down".to_string(),
                )),
            );
            let _ = callback.send(event);
            return;
        };

        rt.spawn(async move {
            let result = Self::execute_request(client, request).await;

            let event = mapper(context, result);
            let _ = callback.send(event);
        });
    }

    pub fn spawn_request_loop<State, Make, Step, Finish>(
        &self,
        initial_state: State,
        context: String,
        callback: CloudCallback,
        make_request: Make,
        mut step: Step,
        finish: Finish,
    ) where
        State: Send + 'static,
        Make: Fn(&State) -> MidgeResult<CloudRequest> + Send + Sync + 'static,
        Step: FnMut(&mut State, CloudResponse) -> MidgeResult<bool> + Send + 'static,
        Finish: FnOnce(String, MidgeResult<State>) -> CloudEvent + Send + 'static,
    {
        let client = self.client.clone();
        let signer = self.signer.clone();

        let Some(rt) = self.rt.as_ref() else {
            let event = finish(
                context,
                Err(MidgeError::Internal(
                    "cloud executor runtime is shut down".to_string(),
                )),
            );
            let _ = callback.send(event);
            return;
        };

        rt.spawn(async move {
            let mut state = initial_state;
            let result = loop {
                let mut request = match make_request(&state) {
                    Ok(request) => request,
                    Err(error) => break Err(error),
                };

                if let Some(signer) = &signer {
                    if let Err(error) = signer.sign(&mut request) {
                        break Err(error);
                    }
                }

                let response = match Self::execute_request(client.clone(), request).await {
                    Ok(response) => response,
                    Err(error) => break Err(error),
                };

                match step(&mut state, response) {
                    Ok(true) => {}
                    Ok(false) => break Ok(state),
                    Err(error) => break Err(error),
                }
            };

            let event = finish(context, result);
            let _ = callback.send(event);
        });
    }

    async fn execute_request(client: Client, request: CloudRequest) -> MidgeResult<CloudResponse> {
        let mut attempt = 0u32;
        let conditional_mutation = request.is_conditional_mutation();
        loop {
            match Self::execute_request_once(client.clone(), request.clone()).await {
                Ok(response) if Self::is_transient_status(response.status) => {
                    if conditional_mutation || attempt >= MAX_TRANSIENT_RETRIES {
                        return Ok(response);
                    }
                    tokio::time::sleep(Self::retry_delay(attempt)).await;
                    attempt += 1;
                }
                Ok(response) => return Ok(response),
                Err(error)
                    if error.transient
                        && !conditional_mutation
                        && attempt < MAX_TRANSIENT_RETRIES =>
                {
                    tokio::time::sleep(Self::retry_delay(attempt)).await;
                    attempt += 1;
                }
                Err(error) => return Err(MidgeError::Internal(error.message)),
            }
        }
    }

    fn retry_delay(attempt: u32) -> Duration {
        Duration::from_millis(TRANSIENT_BACKOFF_BASE_MS.saturating_mul(1u64 << attempt.min(4)))
    }

    fn is_transient_status(status: u16) -> bool {
        matches!(status, 408 | 425 | 429 | 500 | 502 | 503 | 504)
    }

    async fn execute_request_once(
        client: Client,
        request: CloudRequest,
    ) -> Result<CloudResponse, RequestError> {
        let mut builder = client.request(request.method.clone(), &request.url);
        if let Some(timeout) = request.timeout {
            builder = builder.timeout(timeout);
        }
        for (k, v) in &request.headers {
            builder = builder.header(k, v);
        }
        if let Some(body) = request.body {
            builder = builder.body(body);
        }

        match builder.send().await {
            Ok(mut resp) => {
                let status = resp.status().as_u16();
                let headers = resp
                    .headers()
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                    .collect::<Vec<_>>();

                if resp.content_length().is_some_and(|length| {
                    length > u64::try_from(MAX_CLOUD_RESPONSE_BYTES).unwrap_or(u64::MAX)
                }) {
                    return Err(RequestError::permanent(format!(
                        "cloud response exceeds {MAX_CLOUD_RESPONSE_BYTES} byte limit"
                    )));
                }

                let mut body = Vec::new();
                while let Some(chunk) = resp
                    .chunk()
                    .await
                    .map_err(|err| RequestError::permanent(format!("cloud body error: {err}")))?
                {
                    append_bounded_response_chunk(&mut body, &chunk, MAX_CLOUD_RESPONSE_BYTES)
                        .map_err(RequestError::permanent)?;
                }

                Ok(CloudResponse {
                    status,
                    headers,
                    body,
                })
            }
            Err(err) => Err(RequestError::from_reqwest(&err)),
        }
    }
}

fn append_bounded_response_chunk(
    body: &mut Vec<u8>,
    chunk: &[u8],
    max_size: usize,
) -> Result<(), String> {
    let next_len = body
        .len()
        .checked_add(chunk.len())
        .ok_or_else(|| format!("cloud response exceeds {max_size} byte limit"))?;
    if next_len > max_size {
        return Err(format!("cloud response exceeds {max_size} byte limit"));
    }
    body.extend_from_slice(chunk);
    Ok(())
}

struct RequestError {
    transient: bool,
    message: String,
}

impl RequestError {
    fn permanent(message: String) -> Self {
        Self {
            transient: false,
            message,
        }
    }

    fn from_reqwest(error: &reqwest::Error) -> Self {
        let transient =
            error.is_timeout() || error.is_connect() || error.is_request() || error.is_body();
        Self {
            transient,
            message: format!("cloud request failed: {error}"),
        }
    }
}

impl Drop for CloudExecutor {
    fn drop(&mut self) {
        // Try to get exclusive ownership of the runtime for explicit shutdown
        // If we can't (multiple references exist), the runtime will cleanup naturally
        let Some(rt_arc) = self.rt.take() else {
            return;
        };

        if let Ok(rt) = Arc::try_unwrap(rt_arc) {
            // We have exclusive ownership - perform explicit shutdown with timeout
            // Increased from 5s to 10s to accommodate slow cloud operations
            let timeout = Duration::from_secs(10);
            rt.shutdown_timeout(timeout);
            tracing::debug!("CloudExecutor tokio runtime shutdown completed");
        } else {
            // Multiple references exist - runtime will cleanup when last ref drops
            tracing::debug!("CloudExecutor dropping with shared runtime reference");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{append_bounded_response_chunk, CloudExecutor, CloudRequest};
    use reqwest::{Client, Method};
    use std::io::{ErrorKind, Read, Write};
    use std::net::TcpListener;
    use std::time::{Duration, Instant};

    #[test]
    fn should_stop_at_redirect_when_cloud_request_is_mutating() {
        // Arrange
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind redirect server");
        listener
            .set_nonblocking(true)
            .expect("configure nonblocking redirect server");
        let address = listener.local_addr().expect("redirect server address");
        let endpoint = format!("http://{address}/object");
        let redirected = format!("http://{address}/redirected");
        let server = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(2);
            let mut served = 0;
            while Instant::now() < deadline {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let mut request = [0_u8; 2048];
                        let _ = stream.read(&mut request);
                        let (status, location) = if served == 0 {
                            ("303 See Other", format!("Location: {redirected}\r\n"))
                        } else {
                            ("200 OK", String::new())
                        };
                        write!(
                            stream,
                            "HTTP/1.1 {status}\r\n{location}Content-Length: 0\r\nConnection: close\r\n\r\n"
                        )
                        .expect("write redirect response");
                        served += 1;
                        if served > 1 {
                            break;
                        }
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("accept redirect request: {error}"),
                }
            }
            served
        });
        let request = CloudRequest::new(Method::PUT, endpoint).with_body(b"value".to_vec());
        let executor = CloudExecutor::new(None).expect("cloud executor");

        // Act
        let response = executor
            .rt
            .as_ref()
            .expect("cloud runtime")
            .block_on(CloudExecutor::execute_request(
                executor.client.clone(),
                request,
            ))
            .expect("redirect response");
        let request_count = server.join().expect("join redirect server");

        // Assert
        assert_eq!(response.status, 303);
        assert_eq!(
            request_count, 1,
            "cloud mutations must not follow redirects"
        );
    }

    #[test]
    fn should_reject_cloud_response_chunk_past_limit() {
        // Arrange
        let mut body = vec![0_u8; 4];

        // Act
        let result = append_bounded_response_chunk(&mut body, &[1, 2], 5);

        // Assert
        assert!(result.is_err());
        assert_eq!(body.len(), 4);
    }

    #[test]
    fn should_not_retry_conditional_mutation_given_ambiguous_transient_response() {
        // Arrange
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        listener
            .set_nonblocking(true)
            .expect("configure nonblocking test server");
        let endpoint = format!(
            "http://{}/object",
            listener.local_addr().expect("server address")
        );
        let server = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(2);
            let mut served = 0;
            let mut last_request: Option<Instant> = None;
            while Instant::now() < deadline
                && last_request.is_none_or(|last| last.elapsed() < Duration::from_millis(250))
            {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let mut request = [0_u8; 2048];
                        let _ = stream.read(&mut request);
                        let status = if served == 0 {
                            "503 Service Unavailable"
                        } else {
                            "412 Precondition Failed"
                        };
                        write!(
                            stream,
                            "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        )
                        .expect("write test response");
                        served += 1;
                        last_request = Some(Instant::now());
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("accept test request: {error}"),
                }
            }
            served
        });
        let request = CloudRequest::new(Method::PUT, endpoint)
            .with_header("If-Match", "\"old-etag\"")
            .with_body(b"new-value".to_vec());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");

        // Act
        let response = runtime
            .block_on(CloudExecutor::execute_request(Client::new(), request))
            .expect("transient HTTP response");
        let request_count = server.join().expect("join test server");

        // Assert
        assert_eq!(response.status, 503);
        assert_eq!(
            request_count, 1,
            "conditional mutation must not be replayed blindly"
        );
    }

    #[test]
    fn should_return_at_provider_deadline_even_when_remote_may_commit_later() {
        // Arrange
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let endpoint = format!(
            "http://{}/lease",
            listener.local_addr().expect("server address")
        );
        let (request_seen_tx, request_seen_rx) = std::sync::mpsc::channel();
        let (inspect_tx, inspect_rx) = std::sync::mpsc::channel();
        let committed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let server_committed = std::sync::Arc::clone(&committed);
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept request");
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .expect("set server read timeout");
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).expect("read request");
            request_seen_tx.send(()).expect("signal request receipt");
            inspect_rx.recv().expect("wait until request deadline");

            // A client-side deadline is not proof that the provider did not
            // commit. Model that honest worst case: the backing store applies
            // the mutation after the caller has already received a timeout.
            server_committed.store(true, std::sync::atomic::Ordering::Release);
        });
        let request = CloudRequest::new(Method::PUT, endpoint)
            .with_header("If-Match", "\"lease-etag\"")
            .with_header("Connection", "close")
            .with_body(b"renewed-authority".to_vec())
            .with_timeout(Duration::from_millis(50));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");

        // Act
        let result = runtime.block_on(CloudExecutor::execute_request(Client::new(), request));
        request_seen_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("server observed request");
        inspect_tx.send(()).expect("inspect cancelled connection");
        server.join().expect("join test server");

        // Assert
        assert!(result.is_err());
        assert!(committed.load(std::sync::atomic::Ordering::Acquire));
    }
}
