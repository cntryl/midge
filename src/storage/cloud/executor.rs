use crate::common::{MidgeError, MidgeResult};
use crate::storage::cloud::{CloudCallback, CloudEvent};
use reqwest::{Client, Method};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
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
    retry_conditional_conflicts: bool,
}

impl CloudRequest {
    pub fn new(method: Method, url: String) -> Self {
        Self {
            method,
            url,
            headers: Vec::new(),
            body: None,
            timeout: None,
            retry_conditional_conflicts: false,
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

    /// Permit bounded retries for provider-specific conditional-conflict
    /// responses. AWS S3 uses 409 `ConditionalRequestConflict` for a retryable
    /// conditional PUT race; other providers do not share that contract.
    pub fn with_conditional_conflict_retries(mut self) -> Self {
        self.retry_conditional_conflicts = true;
        self
    }

    fn is_mutation(&self) -> bool {
        matches!(self.method, Method::PUT | Method::POST | Method::DELETE)
    }

    fn is_conditionally_idempotent_mutation(&self) -> bool {
        if !self.is_mutation() {
            return false;
        }

        // Generation-not-match does not fence the identity being mutated and
        // therefore cannot make an ambiguous replay safe.
        if self
            .headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("x-goog-if-generation-not-match"))
            || self.url.contains("ifGenerationNotMatch=")
        {
            return false;
        }

        self.headers.iter().any(|(name, _)| {
            name.eq_ignore_ascii_case("if-match")
                || name.eq_ignore_ascii_case("if-none-match")
                || name.eq_ignore_ascii_case("x-goog-if-generation-match")
        }) || self.url.contains("ifGenerationMatch=")
    }
}

/// Minimal response representation returned by the executor.
pub struct CloudResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Verify every declared length against the body from the same GET response.
/// Chunked responses may omit Content-Length; conflicting or malformed values
/// must never be replaced with a synthesized length.
pub(crate) fn validate_get_response_length(response: &CloudResponse) -> super::CloudOutcome<u64> {
    let actual = u64::try_from(response.body.len()).unwrap_or(u64::MAX);
    for (_, value) in response
        .headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
    {
        let declared = value.trim().parse::<u64>().map_err(|error| {
            super::CloudError::Protocol(format!("GET response has invalid Content-Length: {error}"))
        })?;
        if declared != actual {
            return Err(super::CloudError::Protocol(format!(
                "GET response length mismatch: Content-Length={declared}, body={actual}"
            )));
        }
    }
    Ok(actual)
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
    default_timeout_ms: AtomicU64,
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
            default_timeout_ms: AtomicU64::new(Self::duration_millis(
                crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
            )),
        })
    }

    pub(crate) fn set_default_timeout(&self, timeout: Duration) {
        self.default_timeout_ms
            .store(Self::duration_millis(timeout), Ordering::Release);
    }

    fn duration_millis(timeout: Duration) -> u64 {
        let milliseconds = timeout.as_millis();
        let rounded_milliseconds = if timeout.subsec_nanos().is_multiple_of(1_000_000) {
            milliseconds
        } else {
            milliseconds.saturating_add(1)
        };
        u64::try_from(rounded_milliseconds)
            .unwrap_or(u64::MAX)
            .max(1)
    }

    fn default_timeout(&self) -> Duration {
        Duration::from_millis(self.default_timeout_ms.load(Ordering::Acquire))
    }

    fn apply_default_timeout(mut request: CloudRequest, default_timeout: Duration) -> CloudRequest {
        if request.timeout.is_none() {
            request.timeout = Some(default_timeout);
        }
        request
    }

    pub fn spawn_request<F>(
        &self,
        request: CloudRequest,
        context: String,
        callback: CloudCallback,
        mapper: F,
    ) where
        F: FnOnce(String, MidgeResult<CloudResponse>) -> CloudEvent + Send + 'static,
    {
        let client = self.client.clone();
        let signer = self.signer.clone();
        let request = Self::apply_default_timeout(request, self.default_timeout());
        let deadline = Self::deadline_from_timeout(request.timeout);

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
            let result =
                Self::execute_request_with_signer_at(client, signer, request, deadline).await;

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
        let default_timeout = self.default_timeout();
        let operation_deadline = Self::deadline_from_timeout(Some(default_timeout));

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
            let operation = async move {
                let mut state = initial_state;
                loop {
                    Self::ensure_deadline_active(Some(default_timeout), operation_deadline)?;
                    let request = match make_request(&state) {
                        Ok(request) => Self::apply_default_timeout(request, default_timeout),
                        Err(error) => break Err(error),
                    };
                    let request_deadline = Self::earliest_deadline(
                        operation_deadline,
                        Self::deadline_from_timeout(request.timeout),
                    );

                    let response = match Self::execute_request_with_signer_at(
                        client.clone(),
                        signer.clone(),
                        request,
                        request_deadline,
                    )
                    .await
                    {
                        Ok(response) => response,
                        Err(error) => break Err(error),
                    };

                    match step(&mut state, response) {
                        Ok(true) => {}
                        Ok(false) => break Ok(state),
                        Err(error) => break Err(error),
                    }
                }
            };
            let result = if operation_deadline
                .is_some_and(|deadline| deadline <= tokio::time::Instant::now())
            {
                Err(Self::timeout_error(default_timeout))
            } else if let Some(deadline) = operation_deadline {
                tokio::time::timeout_at(deadline, operation)
                    .await
                    .unwrap_or_else(|_| Err(Self::timeout_error(default_timeout)))
            } else {
                tokio::time::timeout(default_timeout, operation)
                    .await
                    .unwrap_or_else(|_| Err(Self::timeout_error(default_timeout)))
            };

            let event = finish(context, result);
            let _ = callback.send(event);
        });
    }

    async fn sign_request_async(
        signer: Option<Arc<dyn CloudSigner>>,
        mut request: CloudRequest,
    ) -> MidgeResult<CloudRequest> {
        let Some(signer) = signer else {
            return Ok(request);
        };
        tokio::task::spawn_blocking(move || {
            signer.sign(&mut request)?;
            Ok(request)
        })
        .await
        .map_err(|error| MidgeError::Internal(format!("cloud signer task failed: {error}")))?
    }

    #[cfg(test)]
    async fn execute_request(client: Client, request: CloudRequest) -> MidgeResult<CloudResponse> {
        Self::execute_request_with_signer(client, None, request).await
    }

    #[cfg(test)]
    async fn execute_request_with_signer(
        client: Client,
        signer: Option<Arc<dyn CloudSigner>>,
        request: CloudRequest,
    ) -> MidgeResult<CloudResponse> {
        let deadline = Self::deadline_from_timeout(request.timeout);
        Self::execute_request_with_signer_at(client, signer, request, deadline).await
    }

    async fn execute_request_with_signer_at(
        client: Client,
        signer: Option<Arc<dyn CloudSigner>>,
        request: CloudRequest,
        deadline: Option<tokio::time::Instant>,
    ) -> MidgeResult<CloudResponse> {
        let timeout = request.timeout;
        Self::ensure_deadline_active(timeout, deadline)?;
        let operation = async move {
            let request = Self::sign_request_async(signer, request).await?;
            Self::ensure_deadline_active(timeout, deadline)?;
            Self::execute_request_with_retries(client, request, deadline).await
        };

        match (timeout, deadline) {
            (Some(timeout), Some(deadline)) => tokio::time::timeout_at(deadline, operation)
                .await
                .map_err(|_| Self::timeout_error(timeout))?,
            (Some(timeout), None) => tokio::time::timeout(timeout, operation)
                .await
                .map_err(|_| Self::timeout_error(timeout))?,
            (None, _) => operation.await,
        }
    }

    fn deadline_from_timeout(timeout: Option<Duration>) -> Option<tokio::time::Instant> {
        timeout.and_then(|timeout| tokio::time::Instant::now().checked_add(timeout))
    }

    fn earliest_deadline(
        first: Option<tokio::time::Instant>,
        second: Option<tokio::time::Instant>,
    ) -> Option<tokio::time::Instant> {
        match (first, second) {
            (Some(first), Some(second)) => Some(first.min(second)),
            (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
            (None, None) => None,
        }
    }

    fn ensure_deadline_active(
        timeout: Option<Duration>,
        deadline: Option<tokio::time::Instant>,
    ) -> MidgeResult<()> {
        match (timeout, deadline) {
            (Some(timeout), Some(deadline)) if deadline <= tokio::time::Instant::now() => {
                Err(Self::timeout_error(timeout))
            }
            _ => Ok(()),
        }
    }

    fn timeout_error(timeout: Duration) -> MidgeError {
        MidgeError::Timeout(format!(
            "cloud request timed out after {} ms",
            timeout.as_millis()
        ))
    }

    async fn execute_request_with_retries(
        client: Client,
        request: CloudRequest,
        deadline: Option<tokio::time::Instant>,
    ) -> MidgeResult<CloudResponse> {
        let mut attempt = 0u32;
        loop {
            Self::ensure_deadline_active(request.timeout, deadline)?;
            let attempt_request = Self::constrain_request_to_deadline(request.clone(), deadline);
            match Self::execute_request_once(client.clone(), attempt_request).await {
                Ok(response) if Self::is_retryable_conditional_conflict(&request, &response) => {
                    if attempt >= MAX_TRANSIENT_RETRIES {
                        return Ok(response);
                    }
                    tokio::time::sleep(Self::retry_delay(attempt)).await;
                    attempt += 1;
                }
                Ok(response) if Self::is_transient_status(response.status) => {
                    // A mutation may have committed before its transient response
                    // was observed. Replaying it can turn that successful commit
                    // into a false precondition failure, so only reads receive
                    // generic transient retries. Provider-specific conflict
                    // responses are handled above when their contract explicitly
                    // guarantees that replay is safe.
                    if request.is_mutation() || attempt >= MAX_TRANSIENT_RETRIES {
                        return Ok(response);
                    }
                    tokio::time::sleep(Self::retry_delay(attempt)).await;
                    attempt += 1;
                }
                Ok(response) => return Ok(response),
                Err(error)
                    if error.transient
                        && !request.is_mutation()
                        && attempt < MAX_TRANSIENT_RETRIES =>
                {
                    tokio::time::sleep(Self::retry_delay(attempt)).await;
                    attempt += 1;
                }
                Err(error) if error.timed_out => {
                    return Err(MidgeError::Timeout(error.message));
                }
                Err(error) => return Err(MidgeError::Internal(error.message)),
            }
        }
    }

    fn constrain_request_to_deadline(
        mut request: CloudRequest,
        deadline: Option<tokio::time::Instant>,
    ) -> CloudRequest {
        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            request.timeout = Some(
                request
                    .timeout
                    .map_or(remaining, |timeout| timeout.min(remaining)),
            );
        }
        request
    }

    fn retry_delay(attempt: u32) -> Duration {
        Duration::from_millis(TRANSIENT_BACKOFF_BASE_MS.saturating_mul(1u64 << attempt.min(4)))
    }

    fn is_transient_status(status: u16) -> bool {
        matches!(status, 408 | 425 | 429 | 500 | 502 | 503 | 504)
    }

    fn is_retryable_conditional_conflict(request: &CloudRequest, response: &CloudResponse) -> bool {
        if response.status != 409
            || !request.retry_conditional_conflicts
            || !request.is_conditionally_idempotent_mutation()
        {
            return false;
        }
        let body = String::from_utf8_lossy(&response.body);
        body.contains("<Code>ConditionalRequestConflict</Code>")
            || body.contains("<Code>OperationAborted</Code>")
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
                    .map_err(|err| RequestError::from_reqwest(&err))?
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
    timed_out: bool,
    message: String,
}

impl RequestError {
    fn permanent(message: String) -> Self {
        Self {
            transient: false,
            timed_out: false,
            message,
        }
    }

    fn from_reqwest(error: &reqwest::Error) -> Self {
        let transient = error.is_timeout()
            || error.is_connect()
            || error.is_request()
            || error.is_body()
            || error.is_decode();
        Self {
            transient,
            timed_out: error.is_timeout(),
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
    use super::{append_bounded_response_chunk, CloudExecutor, CloudRequest, CloudSigner};
    use crate::common::{MidgeError, MidgeResult};
    use crate::storage::cloud::{CloudError, CloudEvent, CloudOutcome};
    use reqwest::{Client, Method};
    use std::io::{ErrorKind, Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Barrier,
    };
    use std::time::{Duration, Instant};

    struct SlowFailingSigner;
    struct SlowSuccessfulSigner;
    struct CountingSlowSuccessfulSigner {
        calls: Arc<AtomicUsize>,
    }

    impl CloudSigner for SlowFailingSigner {
        fn sign(&self, _request: &mut CloudRequest) -> MidgeResult<()> {
            std::thread::sleep(Duration::from_millis(150));
            Err(MidgeError::Internal("injected signer failure".to_string()))
        }
    }

    impl CloudSigner for SlowSuccessfulSigner {
        fn sign(&self, _request: &mut CloudRequest) -> MidgeResult<()> {
            std::thread::sleep(Duration::from_millis(150));
            Ok(())
        }
    }

    impl CloudSigner for CountingSlowSuccessfulSigner {
        fn sign(&self, _request: &mut CloudRequest) -> MidgeResult<()> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            std::thread::sleep(Duration::from_millis(150));
            Ok(())
        }
    }

    fn block_executor_runtime(executor: &CloudExecutor) -> Arc<Barrier> {
        let ready = Arc::new(Barrier::new(5));
        let release = Arc::new(Barrier::new(5));
        for _ in 0..4 {
            let task_ready = Arc::clone(&ready);
            let task_release = Arc::clone(&release);
            executor
                .rt
                .as_ref()
                .expect("cloud runtime")
                .spawn(async move {
                    task_ready.wait();
                    task_release.wait();
                });
        }
        ready.wait();
        release
    }

    fn read_complete_http_request(stream: &mut TcpStream) {
        stream
            .set_nonblocking(false)
            .expect("configure blocking HTTP request stream");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("configure HTTP request timeout");
        let mut bytes = Vec::new();
        let mut chunk = [0_u8; 4096];

        loop {
            if let Some(header_end) = bytes
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|position| position + 4)
            {
                let head = String::from_utf8_lossy(&bytes[..header_end]);
                let content_length = head
                    .split("\r\n")
                    .filter_map(|line| line.split_once(':'))
                    .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                    .map_or(0, |(_, value)| {
                        value
                            .trim()
                            .parse::<usize>()
                            .expect("valid request Content-Length")
                    });
                if bytes.len() >= header_end + content_length {
                    break;
                }
            }

            let read = stream.read(&mut chunk).expect("read complete HTTP request");
            if read == 0 {
                break;
            }
            bytes.extend_from_slice(&chunk[..read]);
        }
    }

    #[test]
    fn should_apply_configured_default_timeout_without_overriding_request_deadline() {
        // Arrange
        let executor = CloudExecutor::new(None).expect("create cloud executor");
        executor.set_default_timeout(Duration::from_millis(123));
        let defaulted = CloudRequest::new(Method::GET, "http://example.invalid".to_string());
        let explicit = CloudRequest::new(Method::GET, "http://example.invalid".to_string())
            .with_timeout(Duration::from_millis(456));

        // Act
        let defaulted = CloudExecutor::apply_default_timeout(defaulted, executor.default_timeout());
        let explicit = CloudExecutor::apply_default_timeout(explicit, executor.default_timeout());

        // Assert
        assert_eq!(defaulted.timeout, Some(Duration::from_millis(123)));
        assert_eq!(explicit.timeout, Some(Duration::from_millis(456)));
    }

    #[test]
    fn should_round_fractional_default_timeout_up_to_next_millisecond() {
        // Arrange
        let timeout = Duration::from_micros(1_999);

        // Act
        let timeout_millis = CloudExecutor::duration_millis(timeout);

        // Assert
        assert_eq!(timeout_millis, 2);
    }

    #[test]
    fn should_preserve_exact_default_timeout_boundary() {
        // Arrange
        let exact_millisecond = Duration::from_millis(1);

        // Act
        let exact_millis = CloudExecutor::duration_millis(exact_millisecond);

        // Assert
        assert_eq!(exact_millis, 1);
    }

    #[test]
    fn should_saturate_default_timeout_at_u64_max() {
        // Arrange
        let timeout = Duration::MAX;

        // Act
        let saturated_millis = CloudExecutor::duration_millis(timeout);

        // Assert
        assert_eq!(saturated_millis, u64::MAX);
    }

    #[test]
    fn should_return_immediately_when_cloud_signer_refresh_is_slow() {
        // Arrange
        let executor =
            CloudExecutor::new(Some(Arc::new(SlowFailingSigner))).expect("create cloud executor");
        let request = CloudRequest::new(Method::GET, "http://127.0.0.1:9/object".to_string());
        let (sender, receiver) = std::sync::mpsc::channel();
        let started = Instant::now();

        // Act
        executor.spawn_request(request, "object".to_string(), sender, |key, result| {
            CloudEvent::Get {
                key,
                result: match result {
                    Ok(response) => CloudOutcome::Ok(response.body),
                    Err(error) => CloudOutcome::Err(CloudError::from_transport_error(error)),
                },
            }
        });
        let returned_after = started.elapsed();
        let event = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("receive signer failure");

        // Assert
        assert!(returned_after < Duration::from_millis(50));
        assert!(matches!(
            event,
            CloudEvent::Get {
                result: CloudOutcome::Err(_),
                ..
            }
        ));
    }

    #[test]
    fn should_charge_slow_signer_refresh_to_request_deadline() {
        // Arrange
        let executor = CloudExecutor::new(Some(Arc::new(SlowSuccessfulSigner)))
            .expect("create cloud executor");
        let request = CloudRequest::new(Method::PUT, "http://127.0.0.1:9/object".to_string())
            .with_timeout(Duration::from_millis(30));
        let (sender, receiver) = std::sync::mpsc::channel();
        let started = Instant::now();

        // Act
        executor.spawn_request(request, "object".to_string(), sender, |key, result| {
            CloudEvent::Put {
                key,
                result: result.map(|_| ()).map_err(CloudError::from_transport_error),
            }
        });
        let event = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("receive signer timeout");

        // Assert
        assert!(started.elapsed() < Duration::from_millis(120));
        assert!(matches!(
            event,
            CloudEvent::Put {
                result: CloudOutcome::Err(CloudError::Timeout(message)),
                ..
            } if message.contains("timed out after 30 ms")
        ));
    }

    #[test]
    fn should_not_authorize_spawned_request_after_submission_deadline() {
        // Arrange
        let signer_calls = Arc::new(AtomicUsize::new(0));
        let executor = CloudExecutor::new(Some(Arc::new(CountingSlowSuccessfulSigner {
            calls: Arc::clone(&signer_calls),
        })))
        .expect("create cloud executor");
        let release = block_executor_runtime(&executor);
        let request = CloudRequest::new(Method::PUT, "http://127.0.0.1:9/object".to_string())
            .with_timeout(Duration::from_millis(30));
        let (sender, receiver) = std::sync::mpsc::channel();

        // Act
        executor.spawn_request(request, "object".to_string(), sender, |key, result| {
            CloudEvent::Put {
                key,
                result: result.map(|_| ()).map_err(CloudError::from_transport_error),
            }
        });
        std::thread::sleep(Duration::from_millis(60));
        release.wait();
        let event = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("receive expired request result");

        // Assert
        assert_eq!(signer_calls.load(Ordering::Acquire), 0);
        assert!(matches!(
            event,
            CloudEvent::Put {
                result: CloudOutcome::Err(CloudError::Timeout(message)),
                ..
            } if message.contains("timed out after 30 ms")
        ));
    }

    #[test]
    fn should_not_begin_spawned_request_loop_after_submission_deadline() {
        // Arrange
        let make_calls = Arc::new(AtomicUsize::new(0));
        let executor = CloudExecutor::new(None).expect("create cloud executor");
        executor.set_default_timeout(Duration::from_millis(30));
        let release = block_executor_runtime(&executor);
        let (sender, receiver) = std::sync::mpsc::channel();
        let counted_make_calls = Arc::clone(&make_calls);

        // Act
        executor.spawn_request_loop(
            Vec::<String>::new(),
            "objects/".to_string(),
            sender,
            move |_| {
                counted_make_calls.fetch_add(1, Ordering::AcqRel);
                Ok(CloudRequest::new(
                    Method::GET,
                    "http://127.0.0.1:9/objects".to_string(),
                ))
            },
            |_, _| Ok(false),
            |prefix, result| CloudEvent::List {
                prefix,
                result: result.map_err(CloudError::from_transport_error),
            },
        );
        std::thread::sleep(Duration::from_millis(60));
        release.wait();
        let event = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("receive expired list result");

        // Assert
        assert_eq!(make_calls.load(Ordering::Acquire), 0);
        assert!(matches!(
            event,
            CloudEvent::List {
                result: CloudOutcome::Err(CloudError::Timeout(message)),
                ..
            } if message.contains("timed out after 30 ms")
        ));
    }

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
        let request_finished = Arc::new(AtomicBool::new(false));
        let server_request_finished = Arc::clone(&request_finished);
        let server = std::thread::spawn(move || {
            let mut served = 0;
            let mut last_request: Option<Instant> = None;
            // A wall-clock startup timeout is unsafe here: the parallel test
            // runner may not schedule the client before that timeout expires.
            while !server_request_finished.load(Ordering::Acquire)
                || last_request.is_some_and(|last| last.elapsed() < Duration::from_millis(250))
            {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        read_complete_http_request(&mut stream);
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
                        last_request = Some(Instant::now());
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
        let response =
            executor
                .rt
                .as_ref()
                .expect("cloud runtime")
                .block_on(CloudExecutor::execute_request(
                    executor.client.clone(),
                    request,
                ));
        request_finished.store(true, Ordering::Release);
        let request_count = server.join().expect("join redirect server");
        let response = response.expect("redirect response");

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
        let request_finished = Arc::new(AtomicBool::new(false));
        let server_request_finished = Arc::clone(&request_finished);
        let server = std::thread::spawn(move || {
            let mut served = 0;
            let mut last_request: Option<Instant> = None;
            while !server_request_finished.load(Ordering::Acquire)
                || last_request.is_some_and(|last| last.elapsed() < Duration::from_millis(250))
            {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        read_complete_http_request(&mut stream);
                        write!(
                            stream,
                            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
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
        let response = runtime.block_on(CloudExecutor::execute_request(Client::new(), request));
        request_finished.store(true, Ordering::Release);
        let request_count = server.join().expect("join test server");
        let response = response.expect("transient HTTP response");

        // Assert
        assert_eq!(response.status, 503);
        assert_eq!(
            request_count, 1,
            "an ambiguous conditional mutation requires caller reconciliation before replay"
        );
    }

    #[test]
    fn should_retry_read_given_partial_response_body_disconnect() {
        // Arrange
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        listener
            .set_nonblocking(true)
            .expect("configure nonblocking test server");
        let endpoint = format!(
            "http://{}/object",
            listener.local_addr().expect("server address")
        );
        let request_finished = Arc::new(AtomicBool::new(false));
        let server_request_finished = Arc::clone(&request_finished);
        let server = std::thread::spawn(move || {
            let mut served = 0;
            while served < 2 && !server_request_finished.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        read_complete_http_request(&mut stream);
                        if served == 0 {
                            stream
                                .write_all(
                                    b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\npart",
                                )
                                .expect("write partial response");
                        } else {
                            stream
                                .write_all(
                                    b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\ncomplete",
                                )
                                .expect("write complete response");
                        }
                        served += 1;
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("accept test request: {error}"),
                }
            }
            served
        });
        let request = CloudRequest::new(Method::GET, endpoint);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");

        // Act
        let response = runtime.block_on(CloudExecutor::execute_request(Client::new(), request));
        request_finished.store(true, Ordering::Release);
        let request_count = server.join().expect("join test server");
        let response = response.expect("read should recover from partial body disconnect");

        // Assert
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"complete");
        assert_eq!(request_count, 2);
    }

    #[test]
    fn should_not_retry_unconditional_mutation_given_ambiguous_transient_response() {
        // Arrange
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        listener
            .set_nonblocking(true)
            .expect("configure nonblocking test server");
        let endpoint = format!(
            "http://{}/object",
            listener.local_addr().expect("server address")
        );
        let request_finished = Arc::new(AtomicBool::new(false));
        let server_request_finished = Arc::clone(&request_finished);
        let server = std::thread::spawn(move || {
            let mut served = 0;
            let mut last_request: Option<Instant> = None;
            while !server_request_finished.load(Ordering::Acquire)
                || last_request.is_some_and(|last| last.elapsed() < Duration::from_millis(250))
            {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        read_complete_http_request(&mut stream);
                        write!(
                            stream,
                            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
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
        let request = CloudRequest::new(Method::POST, endpoint).with_body(b"new-value".to_vec());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");

        // Act
        let response = runtime.block_on(CloudExecutor::execute_request(Client::new(), request));
        request_finished.store(true, Ordering::Release);
        let request_count = server.join().expect("join test server");
        let response = response.expect("transient HTTP response");

        // Assert
        assert_eq!(response.status, 503);
        assert_eq!(
            request_count, 1,
            "an unconditional object mutation must not overwrite a concurrent replacement on retry"
        );
    }

    #[test]
    fn should_not_retry_generation_not_match_mutation_given_transient_response() {
        // Arrange
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        listener
            .set_nonblocking(true)
            .expect("configure nonblocking test server");
        let endpoint = format!(
            "http://{}/object",
            listener.local_addr().expect("server address")
        );
        let request_finished = Arc::new(AtomicBool::new(false));
        let server_request_finished = Arc::clone(&request_finished);
        let server = std::thread::spawn(move || {
            let mut served = 0;
            let mut last_request: Option<Instant> = None;
            while !server_request_finished.load(Ordering::Acquire)
                || last_request.is_some_and(|last| last.elapsed() < Duration::from_millis(250))
            {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        read_complete_http_request(&mut stream);
                        write!(
                            stream,
                            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
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
        let request = CloudRequest::new(Method::DELETE, endpoint)
            .with_header("x-goog-if-generation-not-match", "7");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");

        // Act
        let response = runtime.block_on(CloudExecutor::execute_request(Client::new(), request));
        request_finished.store(true, Ordering::Release);
        let request_count = server.join().expect("join test server");
        let response = response.expect("transient HTTP response");

        // Assert
        assert_eq!(response.status, 503);
        assert_eq!(request_count, 1);
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
            read_complete_http_request(&mut stream);
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
