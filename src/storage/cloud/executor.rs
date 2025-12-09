#![cfg(feature = "cloud-common")]

use crate::common::{MidgeError, MidgeResult};
use crate::storage::cloud::{CloudCallback, CloudEvent};
use reqwest::{Client, Method};
use std::sync::Arc;

/// Represents a generic HTTP request issued by cloud providers.
#[derive(Clone)]
pub struct CloudRequest {
    pub method: Method,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
}

#[derive(Clone)]
pub struct AwsCredentials {
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
}

impl AwsCredentials {
    pub fn new(access_key: String, secret_key: String, region: String) -> Self {
        Self {
            access_key,
            secret_key,
            region,
        }
    }
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
pub struct CloudExecutor {
    client: Client,
    signer: Option<Arc<dyn CloudSigner>>,
}

impl CloudExecutor {
    pub fn new(signer: Option<Arc<dyn CloudSigner>>) -> Self {
        Self {
            client: Client::new(),
            signer,
        }
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
        let headers = request.headers.clone();
        let body = request.body.clone();
        let method = request.method.clone();
        let url = request.url.clone();
        let callback = callback.clone();

        tokio::spawn(async move {
            let mut builder = client.request(method.clone(), &url);
            for (key, value) in headers.iter() {
                builder = builder.header(key, value);
            }
            if let Some(body) = body {
                builder = builder.body(body);
            }

            let response = builder.send().await;
            let result = match response {
                Ok(resp) => match resp.bytes().await {
                    Ok(bytes) => {
                        let headers = resp
                            .headers()
                            .iter()
                            .map(|(k, v)| {
                                (k.to_string(), v.to_str().unwrap_or_default().to_string())
                            })
                            .collect();
                        Ok(CloudResponse {
                            status: resp.status().as_u16(),
                            headers,
                            body: bytes.to_vec(),
                        })
                    }
                    Err(err) => Err(MidgeError::Internal(format!("cloud body error: {}", err))),
                },
                Err(err) => Err(MidgeError::Internal(format!(
                    "cloud request failed: {}",
                    err
                ))),
            };

            let event = mapper(context, result);
            let _ = callback.send(event);
        });
    }
}
