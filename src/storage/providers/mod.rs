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
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use std::thread::JoinHandle;
    use std::time::Duration;

    pub(crate) struct ScriptedHttpServer {
        pub(crate) endpoint: String,
        handle: Option<JoinHandle<usize>>,
        finished: Arc<AtomicBool>,
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
        pub(crate) fn finish(mut self) -> usize {
            self.finished.store(true, Ordering::Release);
            self.handle
                .take()
                .expect("scripted HTTP server handle")
                .join()
                .expect("scripted HTTP server panicked")
        }
    }

    impl Drop for ScriptedHttpServer {
        fn drop(&mut self) {
            self.finished.store(true, Ordering::Release);
            if let Some(handle) = self.handle.take() {
                let result = handle.join();
                if !std::thread::panicking() {
                    result.expect("scripted HTTP server panicked");
                }
            }
        }
    }

    pub(crate) fn spawn_scripted_http_server(bodies: Vec<String>) -> ScriptedHttpServer {
        spawn_scripted_http_response_server(
            bodies
                .into_iter()
                .map(|body| (200, "application/octet-stream".to_string(), body))
                .collect(),
        )
    }

    pub(crate) fn spawn_scripted_http_response_server(
        responses: Vec<(u16, String, String)>,
    ) -> ScriptedHttpServer {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind scripted HTTP server");
        listener
            .set_nonblocking(true)
            .expect("configure scripted HTTP server");
        let endpoint = format!("http://{}", listener.local_addr().expect("server address"));
        let finished = Arc::new(AtomicBool::new(false));
        let server_finished = Arc::clone(&finished);
        let handle = std::thread::spawn(move || {
            let mut served = 0;

            // Keep the listener alive until the caller's provider operation
            // finishes; test-runner scheduling is not a server lifetime.
            while served < responses.len() && !server_finished.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream
                            .set_read_timeout(Some(Duration::from_secs(1)))
                            .expect("configure request timeout");
                        let mut request = [0_u8; 4096];
                        let _ = stream.read(&mut request);

                        let (status, content_type, body) = &responses[served];
                        let response = format!(
                            "HTTP/1.1 {status} Test\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        );
                        stream
                            .write_all(response.as_bytes())
                            .expect("write scripted response");
                        served += 1;
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("scripted HTTP server accept failed: {error}"),
                }
            }

            served
        });

        ScriptedHttpServer {
            endpoint,
            handle: Some(handle),
            finished,
        }
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
pub(crate) fn build_cloud_backend(
    provider: &CloudProviderConfig,
) -> MidgeResult<Arc<dyn CloudBackend>> {
    #[cfg(any(
        feature = "cloud-aws",
        feature = "cloud-azure",
        feature = "cloud-gcp",
        feature = "cloud-oci"
    ))]
    validate_cloud_provider_config(provider)?;

    factory::CloudProviderFactory::build_backend(provider)
}

#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-azure",
    feature = "cloud-gcp",
    feature = "cloud-oci"
))]
fn validate_cloud_provider_config(provider: &CloudProviderConfig) -> MidgeResult<()> {
    let target = provider.bucket_or_container();
    if target.trim().is_empty() {
        return Err(crate::common::MidgeError::InvalidArgument(
            "cloud bucket or container must not be empty".to_string(),
        ));
    }

    let endpoint = match provider {
        CloudProviderConfig::S3Compatible(config) => Some(config.endpoint.as_str()),
        CloudProviderConfig::AzureBlob(config) => config.endpoint.as_deref(),
        CloudProviderConfig::Gcs(config) => config.endpoint.as_deref(),
        CloudProviderConfig::AwsS3(_) | CloudProviderConfig::OciObjectStorage(_) => None,
    };

    if let Some(endpoint) = endpoint {
        let parsed = url::Url::parse(endpoint).map_err(|error| {
            crate::common::MidgeError::InvalidArgument(format!(
                "cloud endpoint must be an absolute HTTP(S) URL: {error}"
            ))
        })?;
        if !matches!(parsed.scheme(), "http" | "https")
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(crate::common::MidgeError::InvalidArgument(
                "cloud endpoint must be an absolute HTTP(S) origin without credentials, query, or fragment"
                    .to_string(),
            ));
        }
    }

    Ok(())
}

#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-azure",
    feature = "cloud-gcp",
    feature = "cloud-oci"
))]
fn validate_list_xml(body: &str, expected_root: &str) -> MidgeResult<()> {
    let mut remaining = body.trim();
    let mut stack = Vec::new();
    let mut saw_root = false;

    while !remaining.is_empty() {
        let Some(open) = remaining.find('<') else {
            if remaining.trim().is_empty() {
                break;
            }
            return Err(crate::common::MidgeError::Internal(
                "provider LIST XML contains text outside its root element".to_string(),
            ));
        };
        if !remaining[..open].trim().is_empty() && stack.is_empty() {
            return Err(crate::common::MidgeError::Internal(
                "provider LIST XML contains text outside its root element".to_string(),
            ));
        }
        let after_open = &remaining[open + 1..];
        let Some(close) = after_open.find('>') else {
            return Err(crate::common::MidgeError::Internal(
                "provider LIST XML contains an unterminated tag".to_string(),
            ));
        };
        let tag = after_open[..close].trim();
        remaining = &after_open[close + 1..];

        if tag.starts_with('?') || tag.starts_with('!') {
            continue;
        }
        if let Some(closing_name) = tag.strip_prefix('/') {
            let closing_name = closing_name.trim();
            let Some(opening_name) = stack.pop() else {
                return Err(crate::common::MidgeError::Internal(
                    "provider LIST XML has an unmatched closing tag".to_string(),
                ));
            };
            if opening_name != closing_name {
                return Err(crate::common::MidgeError::Internal(format!(
                    "provider LIST XML closes {closing_name} while {opening_name} is open"
                )));
            }
            continue;
        }

        let self_closing = tag.ends_with('/');
        let name = tag
            .trim_end_matches('/')
            .split_whitespace()
            .next()
            .unwrap_or_default();
        if name.is_empty() {
            return Err(crate::common::MidgeError::Internal(
                "provider LIST XML contains an empty tag".to_string(),
            ));
        }
        if stack.is_empty() {
            if saw_root || name != expected_root {
                return Err(crate::common::MidgeError::Internal(format!(
                    "provider LIST XML expected root {expected_root}, found {name}"
                )));
            }
            saw_root = true;
        }
        if !self_closing {
            stack.push(name.to_string());
        }
    }

    if !saw_root || !stack.is_empty() {
        return Err(crate::common::MidgeError::Internal(format!(
            "provider LIST XML is empty or incomplete for root {expected_root}"
        )));
    }
    Ok(())
}

#[cfg(all(test, feature = "cloud-all"))]
pub(crate) fn build_cloud_storage(
    provider: &CloudProviderConfig,
    prefix: &str,
) -> MidgeResult<Arc<CloudStorage>> {
    build_cloud_storage_with_timeout(provider, prefix, crate::config::DEFAULT_STORAGE_IO_TIMEOUT)
}

#[cfg(feature = "cloud-common")]
pub(crate) fn build_cloud_storage_with_timeout(
    provider: &CloudProviderConfig,
    prefix: &str,
    timeout: std::time::Duration,
) -> MidgeResult<Arc<CloudStorage>> {
    let namespace = validated_cloud_prefix(prefix)?;
    let backend = build_cloud_backend(provider)?;
    backend.set_request_timeout(timeout);
    Ok(Arc::new(CloudStorage::new_with_timeout(
        backend, namespace, timeout,
    )))
}

#[cfg(feature = "cloud-common")]
fn validated_cloud_prefix(prefix: &str) -> MidgeResult<String> {
    let prefix = prefix.trim_matches('/');
    for segment in prefix.split('/') {
        if matches!(segment, "." | "..") {
            return Err(MidgeError::InvalidArgument(
                "cloud storage prefix must not contain a dot segment".to_string(),
            ));
        }
    }
    Ok(prefix.to_string())
}

#[cfg(all(test, feature = "cloud-all"))]
mod validation_tests {
    use super::*;
    use crate::config::{AzureCredentialSource, GcsCredentialSource};
    use crate::storage::cloud::{CloudError, CloudOutcome};
    use crate::storage::providers::test_support::{
        receive_list_result, spawn_scripted_http_server,
    };

    #[test]
    fn should_reject_empty_bucket_or_container_given_cloud_provider_config_when_building() {
        // Arrange
        let providers = [
            CloudProviderConfig::aws_s3_static("", "us-east-1", "access", "secret"),
            CloudProviderConfig::s3_compatible_static(
                " ",
                "http://127.0.0.1:9000",
                "access",
                "secret",
            ),
            CloudProviderConfig::azure_blob_shared_key("account", "", "YQ=="),
            CloudProviderConfig::gcs_bearer_token("", "token"),
        ];

        // Act
        let errors = providers.map(|provider| {
            build_cloud_backend(&provider)
                .err()
                .expect("empty cloud target must fail before provider construction")
        });

        // Assert
        for error in errors {
            assert!(matches!(
                error,
                crate::common::MidgeError::InvalidArgument(message)
                    if message.contains("bucket or container")
            ));
        }
    }

    #[test]
    fn should_reject_malformed_endpoint_given_cloud_provider_config_when_building() {
        // Arrange
        let providers = [
            CloudProviderConfig::s3_compatible_static("bucket", "not a URL", "access", "secret"),
            CloudProviderConfig::azure_blob("account", "container")
                .with_azure_credentials(AzureCredentialSource::shared_key("YQ=="))
                .expect("Azure credential override should match")
                .with_endpoint("ftp://example.test")
                .expect("Azure supports endpoint overrides"),
            CloudProviderConfig::gcs("bucket")
                .with_gcs_credentials(GcsCredentialSource::bearer_token("token"))
                .expect("GCS credential override should match")
                .with_endpoint("http://user:secret@example.test?query=1")
                .expect("GCS supports endpoint overrides"),
        ];

        // Act
        let errors = providers.map(|provider| {
            build_cloud_backend(&provider)
                .err()
                .expect("malformed endpoint must fail before provider construction")
        });

        // Assert
        for error in errors {
            assert!(matches!(
                error,
                crate::common::MidgeError::InvalidArgument(message)
                    if message.contains("endpoint")
            ));
        }
    }

    #[test]
    fn should_reject_dot_segments_given_cloud_storage_prefix() {
        // Arrange
        let provider = CloudProviderConfig::s3_compatible_static(
            "bucket",
            "http://127.0.0.1:9000",
            "access",
            "secret",
        );
        let prefixes = ["tenant/../shared", "tenant/./shared"];

        // Act
        let errors = prefixes.map(|prefix| {
            build_cloud_storage(&provider, prefix)
                .err()
                .expect("dot segments must fail before any cloud request")
        });

        // Assert
        for error in errors {
            assert!(matches!(
                error,
                crate::common::MidgeError::InvalidArgument(message)
                    if message.contains("dot segment")
            ));
        }
    }

    #[test]
    fn should_preserve_literal_percent_sequences_given_cloud_storage_prefix() {
        // Arrange
        let prefix = "tenant/%2e%2e/%/shared";

        // Act
        let validated = validated_cloud_prefix(prefix).expect("literal percent prefix");

        // Assert
        assert_eq!(validated, prefix);
    }

    #[test]
    fn should_parse_empty_provider_response_as_error_given_malformed_http_body_when_listing() {
        // Arrange
        let server = spawn_scripted_http_server(vec![String::new()]);
        let provider = CloudProviderConfig::s3_compatible_static(
            "bucket",
            server.endpoint.clone(),
            "access",
            "secret",
        );
        let backend = build_cloud_backend(&provider).expect("build local S3 provider");

        // Act
        let result = receive_list_result(backend.as_ref());
        let requests = server.finish();

        // Assert
        assert_eq!(requests, 1);
        assert!(matches!(
            result,
            CloudOutcome::Err(CloudError::Protocol(message))
                if message.contains("empty or incomplete")
        ));
    }

    #[test]
    fn should_reject_invalid_xml_or_json_given_malformed_provider_response_when_parsing() {
        // Arrange
        let xml_server = spawn_scripted_http_server(vec![
            "<ListBucketResult><Contents><Key>sst/a.sst</Contents></ListBucketResult>".to_string(),
        ]);
        let xml_provider = CloudProviderConfig::s3_compatible_static(
            "bucket",
            xml_server.endpoint.clone(),
            "access",
            "secret",
        );
        let xml_backend = build_cloud_backend(&xml_provider).expect("build local S3 provider");

        let json_server = spawn_scripted_http_server(vec!["{\"items\":[}".to_string()]);
        let json_provider = CloudProviderConfig::gcs_bearer_token("bucket", "token")
            .with_endpoint(json_server.endpoint.clone())
            .expect("GCS supports endpoint overrides");
        let json_backend = build_cloud_backend(&json_provider).expect("build local GCS provider");

        // Act
        let xml_result = receive_list_result(xml_backend.as_ref());
        let json_result = receive_list_result(json_backend.as_ref());
        let xml_requests = xml_server.finish();
        let json_requests = json_server.finish();

        // Assert
        assert_eq!((xml_requests, json_requests), (1, 1));
        assert!(matches!(
            xml_result,
            CloudOutcome::Err(CloudError::Protocol(message))
                if message.contains("closes Contents while Key is open")
        ));
        assert!(matches!(
            json_result,
            CloudOutcome::Err(CloudError::Protocol(message))
                if message.contains("GCS list JSON parse")
        ));
    }
}

#[cfg(not(feature = "cloud-common"))]
pub(crate) fn build_cloud_storage_with_timeout(
    _provider: &CloudProviderConfig,
    _prefix: &str,
    _timeout: std::time::Duration,
) -> MidgeResult<Arc<CloudStorage>> {
    Err(MidgeError::InvalidArgument(
        "real cloud storage requires an enabled cloud provider feature".to_string(),
    ))
}
