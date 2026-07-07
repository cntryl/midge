use crate::common::{MidgeError, MidgeResult};
use crate::storage::cloud::{CloudCallback, CloudEvent};
use reqwest::{Client, Method};
use std::sync::Arc;
use std::time::Duration;

const MAX_TRANSIENT_RETRIES: u32 = 3;
const TRANSIENT_BACKOFF_BASE_MS: u64 = 50;

/// Represents a generic HTTP request issued by cloud providers.
#[derive(Clone)]
pub struct CloudRequest {
    pub method: Method,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
}

impl CloudRequest {
    pub fn new(method: Method, url: String) -> Self {
        Self {
            method,
            url,
            headers: Vec::new(),
            body: None,
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

        Ok(Self {
            client: Client::new(),
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
        loop {
            match Self::execute_request_once(client.clone(), request.clone()).await {
                Ok(response) if Self::is_transient_status(response.status) => {
                    if attempt >= MAX_TRANSIENT_RETRIES {
                        return Ok(response);
                    }
                    tokio::time::sleep(Self::retry_delay(attempt)).await;
                    attempt += 1;
                }
                Ok(response) => return Ok(response),
                Err(error) if error.transient && attempt < MAX_TRANSIENT_RETRIES => {
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
        for (k, v) in &request.headers {
            builder = builder.header(k, v);
        }
        if let Some(body) = request.body {
            builder = builder.body(body);
        }

        match builder.send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let headers = resp
                    .headers()
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                    .collect::<Vec<_>>();

                match resp.bytes().await {
                    Ok(bytes) => Ok(CloudResponse {
                        status,
                        headers,
                        body: bytes.to_vec(),
                    }),
                    Err(err) => Err(RequestError::permanent(format!("cloud body error: {err}"))),
                }
            }
            Err(err) => Err(RequestError::from_reqwest(&err)),
        }
    }
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
