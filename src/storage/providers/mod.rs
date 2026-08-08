//! Cloud provider implementations
//!
//! Custom, lean implementations for each cloud vendor without heavy SDKs.
//! Each provider is callback-based, non-blocking, and asynchronous.
//!
//! ## Provider Architecture
//!
//! Implementations are organized by capability:
//!
//! ### S3-Compatible Layer
//!
//! **Base**: [s3.rs] - Generic S3-compatible REST implementation
//! - Object PUT/GET/DELETE/LIST/HEAD
//! - `SigV4` signing (optional, can be extended)
//! - Works with any S3-compatible service
//!
//! **AWS**: configured through `CloudProviderConfig::AwsS3`
//! - Uses AWS region, access key, secret key, or the AWS default chain
//! - Proper AWS `SigV4` request signing
//!
//! **Other S3-compatible providers**: configured through `CloudProviderConfig::S3Compatible`
//! - Covers `MinIO`, `Wasabi`, `OCI` S3 front doors, and generic S3-compatible services
//! - Caller supplies endpoint, region, path-style preference, and credentials
//!
//! ### Direct REST APIs
//!
//! **Google Cloud Storage**: [gcs.rs]
//! - Direct REST API (no SDK)
//! - `OAuth2` authentication (placeholder)
//! - Standalone implementation
//!
//! **Azure Blob Storage**: [azure.rs]
//! - Direct REST API (no SDK)
//! - SAS token or shared key auth (placeholder)
//! - Standalone implementation
//!
//! ## Async Model
//!
//! All providers are non-blocking callback-based:
//! - `submit_put()`, `submit_get()`, etc. return immediately
//! - Results sent via `CloudCallback` channels
//! - Actual HTTP execution happens in `CloudExecutor`'s embedded tokio runtime
//!
//! ## Example Usage

#[cfg(feature = "cloud-azure")]
pub mod azure;
#[cfg(feature = "cloud-azure")]
mod azure_resolver;
#[cfg(feature = "cloud-common")]
mod factory;
#[cfg(feature = "cloud-gcp")]
pub mod gcs;
#[cfg(feature = "cloud-gcp")]
mod gcs_resolver;
#[cfg(all(test, feature = "cloud-all", feature = "sqrzl-tests"))]
pub mod qualification;
#[cfg(any(feature = "cloud-aws", feature = "cloud-oci"))]
pub mod s3;
#[cfg(any(feature = "cloud-aws", feature = "cloud-oci"))]
mod s3_resolver;

#[cfg(all(test, feature = "cloud-common"))]
pub(crate) mod test_support {
    use crate::storage::cloud::{CloudBackend, CloudEvent, CloudOutcome};
    use std::io::{ErrorKind, Read, Write};
    use std::net::TcpListener;
    use std::thread::JoinHandle;
    use std::time::{Duration, Instant};

    pub(crate) struct ScriptedHttpServer {
        pub(crate) endpoint: String,
        handle: JoinHandle<usize>,
    }

    #[derive(Debug)]
    pub(crate) struct RecordedHttpRequest {
        pub(crate) method: String,
        pub(crate) target: String,
        headers: Vec<(String, String)>,
    }

    impl RecordedHttpRequest {
        pub(crate) fn header(&self, name: &str) -> Option<&str> {
            self.headers
                .iter()
                .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
                .map(|(_, value)| value.as_str())
        }
    }

    pub(crate) struct RecordingHttpServer {
        pub(crate) endpoint: String,
        handle: JoinHandle<RecordedHttpRequest>,
    }

    impl RecordingHttpServer {
        pub(crate) fn finish(self) -> RecordedHttpRequest {
            self.handle.join().expect("recording HTTP server panicked")
        }
    }

    impl ScriptedHttpServer {
        pub(crate) fn finish(self) -> usize {
            self.handle.join().expect("scripted HTTP server panicked")
        }
    }

    pub(crate) fn spawn_scripted_http_server(bodies: Vec<String>) -> ScriptedHttpServer {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind scripted HTTP server");
        listener
            .set_nonblocking(true)
            .expect("configure scripted HTTP server");
        let endpoint = format!("http://{}", listener.local_addr().expect("server address"));
        let handle = std::thread::spawn(move || {
            let overall_deadline = Instant::now() + Duration::from_secs(5);
            let mut idle_deadline = None;
            let mut served = 0;

            while served < bodies.len() && Instant::now() < overall_deadline {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream
                            .set_read_timeout(Some(Duration::from_secs(1)))
                            .expect("configure request timeout");
                        let mut request = [0_u8; 4096];
                        let _ = stream.read(&mut request);

                        let body = &bodies[served];
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        );
                        stream
                            .write_all(response.as_bytes())
                            .expect("write scripted response");
                        served += 1;
                        idle_deadline = Some(Instant::now() + Duration::from_millis(500));
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        if idle_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("scripted HTTP server accept failed: {error}"),
                }
            }

            served
        });

        ScriptedHttpServer { endpoint, handle }
    }

    pub(crate) fn spawn_recording_http_server(
        response_headers: Vec<(String, String)>,
        response_body: Vec<u8>,
    ) -> RecordingHttpServer {
        spawn_recording_http_server_with_status(200, response_headers, response_body)
    }

    pub(crate) fn spawn_recording_http_server_with_status(
        response_status: u16,
        response_headers: Vec<(String, String)>,
        response_body: Vec<u8>,
    ) -> RecordingHttpServer {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind recording HTTP server");
        let endpoint = format!("http://{}", listener.local_addr().expect("server address"));
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept recorded HTTP request");
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("configure recorded request timeout");

            let mut bytes = Vec::new();
            let mut chunk = [0_u8; 4096];
            while !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = stream.read(&mut chunk).expect("read recorded HTTP request");
                if read == 0 {
                    break;
                }
                bytes.extend_from_slice(&chunk[..read]);
            }

            let head = String::from_utf8_lossy(&bytes);
            let mut lines = head.split("\r\n");
            let request_line = lines.next().expect("recorded HTTP request line");
            let mut request_parts = request_line.split_whitespace();
            let method = request_parts
                .next()
                .expect("recorded HTTP request method")
                .to_string();
            let target = request_parts
                .next()
                .expect("recorded HTTP request target")
                .to_string();
            let headers = lines
                .take_while(|line| !line.is_empty())
                .filter_map(|line| line.split_once(':'))
                .map(|(name, value)| (name.to_string(), value.trim().to_string()))
                .collect::<Vec<_>>();

            let mut response = format!("HTTP/1.1 {response_status} Test\r\n");
            for (name, value) in &response_headers {
                use std::fmt::Write as _;
                write!(&mut response, "{name}: {value}\r\n")
                    .expect("write recorded response header");
            }
            if !response_headers
                .iter()
                .any(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            {
                use std::fmt::Write as _;
                write!(&mut response, "Content-Length: {}\r\n", response_body.len())
                    .expect("write recorded response content length");
            }
            response.push_str("Connection: close\r\n\r\n");
            stream
                .write_all(response.as_bytes())
                .expect("write recorded HTTP response headers");
            stream
                .write_all(&response_body)
                .expect("write recorded HTTP response body");

            RecordedHttpRequest {
                method,
                target,
                headers,
            }
        });

        RecordingHttpServer { endpoint, handle }
    }

    pub(crate) fn receive_list_result(backend: &dyn CloudBackend) -> CloudOutcome<Vec<String>> {
        let (sender, receiver) = std::sync::mpsc::channel();
        backend.submit_list("sst/", sender);
        match receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("receive LIST result")
        {
            CloudEvent::List { result, .. } => result,
            event => panic!("expected LIST event, got {event:?}"),
        }
    }
}

use std::sync::Arc;

#[cfg(not(feature = "cloud-common"))]
use crate::common::MidgeError;
use crate::common::MidgeResult;
#[cfg(feature = "cloud-common")]
use crate::storage::cloud::CloudBackend;
use crate::storage::cloud::CloudStorage;

#[cfg(feature = "cloud-azure")]
pub(crate) use crate::config::AzureCredentialSource;
pub(crate) use crate::config::CloudProviderConfig;
#[cfg(any(feature = "cloud-aws", feature = "cloud-oci"))]
pub(crate) use crate::config::S3CredentialSource;
#[cfg(feature = "cloud-gcp")]
pub(crate) use crate::config::{GcsApiStyle, GcsCredentialSource};

#[cfg(feature = "cloud-common")]
pub(super) fn is_aws_s3(provider: &CloudProviderConfig) -> bool {
    matches!(provider, CloudProviderConfig::AwsS3 { .. })
}

#[cfg(feature = "cloud-common")]
pub(super) fn is_s3_compatible(provider: &CloudProviderConfig) -> bool {
    matches!(provider, CloudProviderConfig::S3Compatible { .. })
}

#[cfg(feature = "cloud-common")]
pub(super) fn is_azure_blob(provider: &CloudProviderConfig) -> bool {
    matches!(provider, CloudProviderConfig::AzureBlob { .. })
}

#[cfg(feature = "cloud-common")]
pub(crate) fn build_cloud_backend(
    provider: &CloudProviderConfig,
) -> MidgeResult<Arc<dyn CloudBackend>> {
    factory::CloudProviderFactory::build_backend(provider)
}

#[cfg(feature = "cloud-common")]
pub(crate) fn build_cloud_storage(
    provider: &CloudProviderConfig,
    prefix: &str,
) -> MidgeResult<Arc<CloudStorage>> {
    let backend = build_cloud_backend(provider)?;
    Ok(Arc::new(CloudStorage::new(
        backend,
        prefix.trim_matches('/').to_string(),
    )))
}

#[cfg(not(feature = "cloud-common"))]
pub(crate) fn build_cloud_storage(
    _provider: &CloudProviderConfig,
    _prefix: &str,
) -> MidgeResult<Arc<CloudStorage>> {
    Err(MidgeError::InvalidArgument(
        "real cloud storage requires an enabled cloud provider feature".to_string(),
    ))
}
