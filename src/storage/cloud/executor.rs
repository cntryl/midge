//
// == COPILOT RULES: CLOUD EXECUTOR ==
//
// You MUST implement CloudExecutor as a fully self-contained async engine.
//
// Requirements:
// 1. CloudExecutor MUST embed its own multi-threaded Tokio runtime:
//      let rt = tokio::runtime::Builder::new_multi_thread()
//          .worker_threads(4)  // Prevents single-thread starvation
//          .enable_all()
//          .build()
//          .unwrap();
//
// 2. spawn_request MUST execute inside that runtime using rt.spawn().
//    NEVER call tokio::spawn directly, because Midge runtime is synchronous.
//
// 3. Every cloud request MUST eventually produce a CloudEvent,
//    either CloudAck or CloudFail. Dropped futures are forbidden.
//
// 4. All HTTP calls MUST use reqwest::Client inside the executor runtime.
//    If signer is present, call signer.sign(request) BEFORE dispatch.
//
// 5. CloudResponse MUST include:
//      - status code
//      - headers
//      - full body bytes
//
// 6. Errors MUST be mapped to MidgeError::Internal with full context.
//
// 7. CloudExecutor is thread-safe and MUST NOT block the Midge runtime thread.
//
// 8. No request may outlive the executor. All pending tasks must complete.
//
// FOLLOW THESE RULES EXACTLY.

use crate::common::{MidgeError, MidgeResult};
use crate::storage::cloud::{CloudCallback, CloudEvent};
use reqwest::{Client, Method};
use std::sync::Arc;
use std::time::Duration;

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
    rt: Arc<tokio::runtime::Runtime>,
}

impl CloudExecutor {
    pub fn new(signer: Option<Arc<dyn CloudSigner>>) -> MidgeResult<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .map_err(|e| {
                MidgeError::Internal(format!("Failed to build cloud tokio runtime: {}", e))
            })?;

        Ok(Self {
            client: Client::new(),
            signer,
            rt: Arc::new(rt),
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
        let method = request.method.clone();
        let url = request.url.clone();
        let headers = request.headers.clone();
        let body = request.body.clone();

        let cb = callback.clone();

        self.rt.spawn(async move {
            let mut builder = client.request(method.clone(), &url);
            for (k, v) in headers.iter() {
                builder = builder.header(k, v);
            }
            if let Some(b) = body {
                builder = builder.body(b);
            }

            let result = match builder.send().await {
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
                        Err(err) => Err(MidgeError::Internal(format!("cloud body error: {err}"))),
                    }
                }
                Err(err) => Err(MidgeError::Internal(format!("cloud request failed: {err}"))),
            };

            let event = mapper(context.clone(), result);
            let _ = cb.send(event);
        });
    }
}

impl Drop for CloudExecutor {
    fn drop(&mut self) {
        // Try to get exclusive ownership of the runtime for explicit shutdown
        // If we can't (multiple references exist), the runtime will cleanup naturally
        let rt_arc = std::mem::replace(
            &mut self.rt,
            Arc::new(
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .build()
                    .expect("Failed to create placeholder runtime"),
            ),
        );

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
