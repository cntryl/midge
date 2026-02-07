//! Google Cloud Storage Provider
//!
//! Production implementation using direct JSON API (no SDK dependency):
//! - OAuth2 Bearer token authentication
//! - Service-account HMAC key authentication (for S3-interop or simple setups)
//! - Non-blocking callback-based API via `CloudExecutor`
//! - All operations routed through the same `CloudBackend` trait as S3/Azure

use crate::common::{MidgeError, MidgeResult};
use crate::storage::cloud::{
    CloudBackend, CloudCallback, CloudEvent, CloudExecutor, CloudOutcome, CloudRequest,
    CloudResponse, CloudSigner, ObjectMetadata,
};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use reqwest::Method;
use std::sync::Arc;

const ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'<')
    .add(b'>')
    .add(b'`')
    .add(b'#')
    .add(b'?')
    .add(b'{')
    .add(b'}');

// ---------------------------------------------------------------------------
// Credentials
// ---------------------------------------------------------------------------

/// GCS authentication credentials.
#[derive(Debug, Clone)]
pub enum GcsCredential {
    /// OAuth2 Bearer token (short-lived, from gcloud CLI or metadata server).
    BearerToken { token: String },
    /// Service-account HMAC key pair (for HMAC-based auth, simpler than OAuth2).
    HmacKey {
        access_id: String,
        secret: String,
    },
}

// ---------------------------------------------------------------------------
// Public provider
// ---------------------------------------------------------------------------

/// Google Cloud Storage provider.
///
/// Wraps a [`GcsBackend`] that sends real HTTP requests via
/// [`CloudExecutor`]. Follows the same architecture as [`S3Provider`] and
/// [`AzureProvider`].
pub struct GcsProvider {
    backend: Arc<dyn CloudBackend>,
    bucket: String,
    project_id: String,
    credential: GcsCredential,
}

impl GcsProvider {
    /// Create provider with an OAuth2 Bearer token.
    pub fn with_bearer_token(
        bucket: String,
        project_id: String,
        token: String,
    ) -> Self {
        let credential = GcsCredential::BearerToken {
            token: token.clone(),
        };
        let signer: Option<Arc<dyn CloudSigner>> =
            Some(Arc::new(BearerTokenSigner::new(token)));
        let executor = CloudExecutor::new(signer);
        let backend = Arc::new(GcsBackend::new(
            bucket.clone(),
            executor,
        ));
        Self {
            backend,
            bucket,
            project_id,
            credential,
        }
    }

    /// Create provider with a service-account HMAC key pair.
    pub fn with_hmac_key(
        bucket: String,
        project_id: String,
        access_id: String,
        secret: String,
    ) -> Self {
        let credential = GcsCredential::HmacKey {
            access_id: access_id.clone(),
            secret: secret.clone(),
        };
        // HMAC keys use S3-compatible signing; for now inject as bearer-style header.
        // A full implementation would use AWS SigV4-compatible signing against
        // storage.googleapis.com. We store the credential for future expansion.
        let signer: Option<Arc<dyn CloudSigner>> = Some(Arc::new(HmacKeySigner {
            access_id,
            _secret: secret,
        }));
        let executor = CloudExecutor::new(signer);
        let backend = Arc::new(GcsBackend::new(
            bucket.clone(),
            executor,
        ));
        Self {
            backend,
            bucket,
            project_id,
            credential,
        }
    }

    /// Legacy constructor — creates a provider with an empty bearer token.
    /// Callers should prefer `with_bearer_token` or `with_hmac_key`.
    pub fn new(bucket: String, project_id: String) -> Self {
        Self::with_bearer_token(bucket, project_id, String::new())
    }

    /// Get the underlying cloud backend for use with `CloudStorage`.
    pub fn backend(&self) -> Arc<dyn CloudBackend> {
        Arc::clone(&self.backend)
    }

    /// Expose bucket for tests.
    #[cfg(test)]
    pub(crate) fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Expose project_id for tests.
    #[cfg(test)]
    pub(crate) fn project_id(&self) -> &str {
        &self.project_id
    }

    /// Expose credential for tests.
    #[cfg(test)]
    pub(crate) fn credential(&self) -> &GcsCredential {
        &self.credential
    }
}

// ---------------------------------------------------------------------------
// Backend (private — implements CloudBackend)
// ---------------------------------------------------------------------------

struct GcsBackend {
    bucket: String,
    executor: CloudExecutor,
}

impl GcsBackend {
    fn new(bucket: String, executor: CloudExecutor) -> Self {
        Self { bucket, executor }
    }

    fn canonical_key(&self, key: &str) -> String {
        key.split('/')
            .map(|seg| utf8_percent_encode(seg, ENCODE_SET).to_string())
            .collect::<Vec<_>>()
            .join("/")
    }

    /// Upload URL: `https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o?uploadType=media&name={key}`
    fn upload_url(&self, key: &str) -> String {
        format!(
            "https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}",
            self.bucket,
            urlencoding::encode(key)
        )
    }

    /// Download URL (media): `https://storage.googleapis.com/storage/v1/b/{bucket}/o/{key}?alt=media`
    fn download_url(&self, key: &str) -> String {
        format!(
            "https://storage.googleapis.com/storage/v1/b/{}/o/{}?alt=media",
            self.bucket,
            self.canonical_key(key)
        )
    }

    /// Metadata URL: `https://storage.googleapis.com/storage/v1/b/{bucket}/o/{key}`
    fn metadata_url(&self, key: &str) -> String {
        format!(
            "https://storage.googleapis.com/storage/v1/b/{}/o/{}",
            self.bucket,
            self.canonical_key(key)
        )
    }

    /// List URL: `https://storage.googleapis.com/storage/v1/b/{bucket}/o?prefix={prefix}`
    fn list_url(&self, prefix: &str) -> String {
        format!(
            "https://storage.googleapis.com/storage/v1/b/{}/o?prefix={}",
            self.bucket,
            urlencoding::encode(prefix)
        )
    }
}

impl CloudBackend for GcsBackend {
    fn submit_put(&self, key: String, data: Vec<u8>, callback: CloudCallback) {
        let url = self.upload_url(&key);
        let request = CloudRequest::new(Method::POST, url)
            .with_body(data)
            .with_header("Content-Type", "application/octet-stream");
        let mapper = move |ctx: String, result: MidgeResult<CloudResponse>| match result {
            Ok(resp) if resp.status == 200 => CloudEvent::PutComplete {
                key: ctx,
                result: CloudOutcome::Ok(()),
            },
            Ok(resp) => CloudEvent::PutComplete {
                key: ctx,
                result: CloudOutcome::Err(format!("GCS PUT status {}", resp.status)),
            },
            Err(err) => CloudEvent::PutComplete {
                key: ctx,
                result: CloudOutcome::Err(format!("{:?}", err)),
            },
        };
        self.executor.spawn_request(request, key, callback, mapper);
    }

    fn submit_get(&self, key: String, callback: CloudCallback) {
        let url = self.download_url(&key);
        let request = CloudRequest::new(Method::GET, url);
        let mapper = move |ctx: String, result: MidgeResult<CloudResponse>| match result {
            Ok(resp) if resp.status == 200 => CloudEvent::GetComplete {
                key: ctx,
                result: CloudOutcome::Ok(resp.body),
            },
            Ok(resp) if resp.status == 404 => CloudEvent::GetComplete {
                key: ctx,
                result: CloudOutcome::Err("not found".into()),
            },
            Ok(resp) => CloudEvent::GetComplete {
                key: ctx,
                result: CloudOutcome::Err(format!("GCS GET status {}", resp.status)),
            },
            Err(err) => CloudEvent::GetComplete {
                key: ctx,
                result: CloudOutcome::Err(format!("{:?}", err)),
            },
        };
        self.executor.spawn_request(request, key, callback, mapper);
    }

    fn submit_get_range(
        &self,
        key: String,
        start: u64,
        end: Option<u64>,
        callback: CloudCallback,
    ) {
        let url = self.download_url(&key);
        let range = match end {
            Some(e) => format!("bytes={}-{}", start, e.saturating_sub(1)),
            None => format!("bytes={}-", start),
        };
        let request = CloudRequest::new(Method::GET, url).with_header("Range", range);
        let mapper = move |ctx: String, result: MidgeResult<CloudResponse>| match result {
            Ok(resp) if resp.status == 206 || resp.status == 200 => {
                CloudEvent::GetRangeComplete {
                    key: ctx,
                    start,
                    end,
                    result: CloudOutcome::Ok(resp.body),
                }
            }
            Ok(resp) => CloudEvent::GetRangeComplete {
                key: ctx,
                start,
                end,
                result: CloudOutcome::Err(format!("GCS GET_RANGE status {}", resp.status)),
            },
            Err(err) => CloudEvent::GetRangeComplete {
                key: ctx,
                start,
                end,
                result: CloudOutcome::Err(format!("{:?}", err)),
            },
        };
        self.executor.spawn_request(request, key, callback, mapper);
    }

    fn submit_delete(&self, key: String, callback: CloudCallback) {
        let url = self.metadata_url(&key);
        let request = CloudRequest::new(Method::DELETE, url);
        let mapper = move |ctx: String, result: MidgeResult<CloudResponse>| match result {
            Ok(resp) if resp.status == 204 || resp.status == 200 => CloudEvent::DeleteComplete {
                key: ctx,
                result: CloudOutcome::Ok(()),
            },
            Ok(resp) if resp.status == 404 => CloudEvent::DeleteComplete {
                key: ctx,
                result: CloudOutcome::Ok(()), // idempotent delete
            },
            Ok(resp) => CloudEvent::DeleteComplete {
                key: ctx,
                result: CloudOutcome::Err(format!("GCS DELETE status {}", resp.status)),
            },
            Err(err) => CloudEvent::DeleteComplete {
                key: ctx,
                result: CloudOutcome::Err(format!("{:?}", err)),
            },
        };
        self.executor.spawn_request(request, key, callback, mapper);
    }

    fn submit_list(&self, prefix: String, callback: CloudCallback) {
        let url = self.list_url(&prefix);
        let request = CloudRequest::new(Method::GET, url);
        let mapper = move |ctx: String, result: MidgeResult<CloudResponse>| match result {
            Ok(resp) if resp.status == 200 => {
                // GCS JSON API returns { "items": [ { "name": "..." }, ... ] }
                let body = String::from_utf8_lossy(&resp.body);
                let mut items = Vec::new();
                // Simple JSON parsing: extract "name" values from items array.
                // This avoids adding a JSON parsing dependency for a simple list.
                for segment in body.split("\"name\"") {
                    if let Some(colon_rest) = segment.strip_prefix(':') {
                        let trimmed = colon_rest.trim().trim_start_matches('"');
                        if let Some(end) = trimmed.find('"') {
                            items.push(trimmed[..end].to_string());
                        }
                    }
                }
                CloudEvent::ListComplete {
                    prefix: ctx,
                    result: CloudOutcome::Ok(items),
                }
            }
            Ok(resp) => CloudEvent::ListComplete {
                prefix: ctx,
                result: CloudOutcome::Err(format!("GCS LIST status {}", resp.status)),
            },
            Err(err) => CloudEvent::ListComplete {
                prefix: ctx,
                result: CloudOutcome::Err(format!("{:?}", err)),
            },
        };
        self.executor
            .spawn_request(request, prefix.clone(), callback, mapper);
    }

    fn submit_head(&self, key: String, callback: CloudCallback) {
        // GCS: GET metadata URL (without ?alt=media) returns JSON metadata.
        let url = self.metadata_url(&key);
        let request = CloudRequest::new(Method::GET, url);
        let mapper = move |ctx: String, result: MidgeResult<CloudResponse>| match result {
            Ok(resp) if resp.status == 200 => {
                let body = String::from_utf8_lossy(&resp.body);
                // Extract "size" from JSON metadata.
                let size = extract_json_string_value(&body, "size")
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);
                let etag = extract_json_string_value(&body, "etag").unwrap_or_default();
                let metadata = ObjectMetadata::new(size, etag, 0);
                CloudEvent::HeadComplete {
                    key: ctx,
                    result: CloudOutcome::Ok(metadata),
                }
            }
            Ok(resp) if resp.status == 404 => CloudEvent::HeadComplete {
                key: ctx,
                result: CloudOutcome::Err("not found".into()),
            },
            Ok(resp) => CloudEvent::HeadComplete {
                key: ctx,
                result: CloudOutcome::Err(format!("GCS HEAD status {}", resp.status)),
            },
            Err(err) => CloudEvent::HeadComplete {
                key: ctx,
                result: CloudOutcome::Err(format!("{:?}", err)),
            },
        };
        self.executor.spawn_request(request, key, callback, mapper);
    }
}

/// Extract a string value from a JSON body by key name.
/// Simple parser — avoids adding a JSON dependency for lightweight metadata extraction.
fn extract_json_string_value(body: &str, key: &str) -> Option<String> {
    let pattern = format!("\"{}\"", key);
    let start = body.find(&pattern)?;
    let after_key = &body[start + pattern.len()..];
    let after_colon = after_key.trim_start().strip_prefix(':')?;
    let trimmed = after_colon.trim_start().trim_start_matches('"');
    let end = trimmed.find('"')?;
    Some(trimmed[..end].to_string())
}

// ---------------------------------------------------------------------------
// Bearer Token Signer
// ---------------------------------------------------------------------------

struct BearerTokenSigner {
    token: String,
}

impl BearerTokenSigner {
    fn new(token: String) -> Self {
        Self { token }
    }
}

impl CloudSigner for BearerTokenSigner {
    fn sign(&self, request: &mut CloudRequest) -> MidgeResult<()> {
        if !self.token.is_empty() {
            request.headers.retain(|(n, _)| {
                !n.eq_ignore_ascii_case("Authorization")
            });
            request.headers.push((
                "Authorization".into(),
                format!("Bearer {}", self.token),
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// HMAC Key Signer (placeholder — sets access ID header for now)
// ---------------------------------------------------------------------------

struct HmacKeySigner {
    access_id: String,
    _secret: String,
}

impl CloudSigner for HmacKeySigner {
    fn sign(&self, request: &mut CloudRequest) -> MidgeResult<()> {
        // GCS HMAC keys are S3-compatible. A full implementation would use
        // SigV4-style signing against storage.googleapis.com. For now we
        // attach the access_id so the request is identifiable.
        request.headers.retain(|(n, _)| {
            !n.eq_ignore_ascii_case("x-goog-access-id")
        });
        request
            .headers
            .push(("x-goog-access-id".into(), self.access_id.clone()));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cloud::CloudBackend;

    // =========== GcsCredential Tests ===========

    #[test]
    fn should_create_bearer_token_credential() {
        // Arrange
        let token = "ya29.example".to_string();

        // Act
        let cred = GcsCredential::BearerToken { token };

        // Assert
        match cred {
            GcsCredential::BearerToken { token } => assert_eq!(token, "ya29.example"),
            _ => panic!("Expected BearerToken credential"),
        }
    }

    #[test]
    fn should_create_hmac_key_credential() {
        // Arrange
        let access_id = "GOOG123".to_string();
        let secret = "secret456".to_string();

        // Act
        let cred = GcsCredential::HmacKey { access_id, secret };

        // Assert
        match cred {
            GcsCredential::HmacKey { access_id, secret } => {
                assert_eq!(access_id, "GOOG123");
                assert_eq!(secret, "secret456");
            }
            _ => panic!("Expected HmacKey credential"),
        }
    }

    // =========== GcsProvider Construction Tests ===========

    #[test]
    fn should_create_provider_with_bearer_token() {
        // Arrange
        let bucket = "my-bucket";
        let project = "my-project";
        let token = "ya29.token";

        // Act
        let provider = GcsProvider::with_bearer_token(
            bucket.into(),
            project.into(),
            token.into(),
        );

        // Assert
        assert_eq!(provider.bucket(), "my-bucket");
        assert_eq!(provider.project_id(), "my-project");
        assert!(matches!(
            provider.credential(),
            GcsCredential::BearerToken { .. }
        ));
    }

    #[test]
    fn should_create_provider_with_hmac_key() {
        // Arrange
        let bucket = "my-bucket";
        let project = "my-project";
        let access_id = "GOOG123";
        let secret = "secret";

        // Act
        let provider = GcsProvider::with_hmac_key(
            bucket.into(),
            project.into(),
            access_id.into(),
            secret.into(),
        );

        // Assert
        assert_eq!(provider.bucket(), "my-bucket");
        assert!(matches!(
            provider.credential(),
            GcsCredential::HmacKey { .. }
        ));
    }

    #[test]
    fn should_default_to_bearer_token_with_new() {
        // Arrange
        let bucket = "bucket";
        let project = "project";

        // Act
        let provider = GcsProvider::new(bucket.into(), project.into());

        // Assert
        assert!(matches!(
            provider.credential(),
            GcsCredential::BearerToken { .. }
        ));
    }

    #[test]
    fn should_handle_empty_bucket_name() {
        // Arrange
        let bucket = "";
        let project = "project";

        // Act
        let provider = GcsProvider::new(bucket.into(), project.into());

        // Assert
        assert_eq!(provider.bucket(), "");
        assert_eq!(provider.project_id(), "project");
    }

    #[test]
    fn should_handle_empty_project_id() {
        // Arrange
        let bucket = "bucket";
        let project = "";

        // Act
        let provider = GcsProvider::new(bucket.into(), project.into());

        // Assert
        assert_eq!(provider.bucket(), "bucket");
        assert_eq!(provider.project_id(), "");
    }

    #[test]
    fn should_handle_special_characters_in_bucket() {
        // Arrange
        let bucket = "my-bucket-123";
        let project = "my_project-123";

        // Act
        let provider = GcsProvider::new(bucket.into(), project.into());

        // Assert
        assert_eq!(provider.bucket(), "my-bucket-123");
        assert_eq!(provider.project_id(), "my_project-123");
    }

    // =========== GcsBackend URL Tests ===========

    #[test]
    fn should_build_correct_upload_url() {
        // Arrange
        let backend = GcsBackend::new("my-bucket".into(), make_noop_executor());

        // Act
        let url = backend.upload_url("path/to/object");

        // Assert
        assert!(url.starts_with("https://storage.googleapis.com/upload/storage/v1/b/my-bucket/o"));
        assert!(url.contains("uploadType=media"));
        assert!(url.contains("name=path%2Fto%2Fobject"));
    }

    #[test]
    fn should_build_correct_download_url() {
        // Arrange
        let backend = GcsBackend::new("my-bucket".into(), make_noop_executor());

        // Act
        let url = backend.download_url("path/to/object");

        // Assert
        assert!(url.starts_with("https://storage.googleapis.com/storage/v1/b/my-bucket/o/"));
        assert!(url.contains("alt=media"));
    }

    #[test]
    fn should_build_correct_metadata_url() {
        // Arrange
        let backend = GcsBackend::new("my-bucket".into(), make_noop_executor());

        // Act
        let url = backend.metadata_url("myobject");

        // Assert
        assert_eq!(
            url,
            "https://storage.googleapis.com/storage/v1/b/my-bucket/o/myobject"
        );
    }

    #[test]
    fn should_build_correct_list_url() {
        // Arrange
        let backend = GcsBackend::new("my-bucket".into(), make_noop_executor());

        // Act
        let url = backend.list_url("wal/");

        // Assert
        assert!(url.contains("prefix=wal%2F"));
    }

    // =========== BearerTokenSigner Tests ===========

    #[test]
    fn should_add_bearer_authorization_header() {
        // Arrange
        let signer = BearerTokenSigner::new("ya29.example_token".into());
        let mut request = CloudRequest::new(
            Method::GET,
            "https://storage.googleapis.com/storage/v1/b/bucket/o/key?alt=media".into(),
        );

        // Act
        let result = signer.sign(&mut request);

        // Assert
        assert!(result.is_ok());
        let auth = request
            .headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case("Authorization"))
            .map(|(_, v)| v.as_str());
        assert_eq!(auth, Some("Bearer ya29.example_token"));
    }

    #[test]
    fn should_skip_auth_header_when_token_is_empty() {
        // Arrange
        let signer = BearerTokenSigner::new(String::new());
        let mut request = CloudRequest::new(
            Method::GET,
            "https://storage.googleapis.com/storage/v1/b/bucket/o/key".into(),
        );

        // Act
        let _ = signer.sign(&mut request);

        // Assert
        let has_auth = request
            .headers
            .iter()
            .any(|(n, _)| n.eq_ignore_ascii_case("Authorization"));
        assert!(!has_auth);
    }

    // =========== JSON Value Extraction Tests ===========

    #[test]
    fn should_extract_size_from_json_metadata() {
        // Arrange
        let json = r#"{"kind":"storage#object","name":"test","size":"12345","etag":"abc"}"#;

        // Act
        let size = extract_json_string_value(json, "size");

        // Assert
        assert_eq!(size, Some("12345".to_string()));
    }

    #[test]
    fn should_extract_etag_from_json_metadata() {
        // Arrange
        let json = r#"{"kind":"storage#object","name":"test","size":"100","etag":"CLT3abc="}"#;

        // Act
        let etag = extract_json_string_value(json, "etag");

        // Assert
        assert_eq!(etag, Some("CLT3abc=".to_string()));
    }

    #[test]
    fn should_return_none_when_key_missing_from_json() {
        // Arrange
        let json = r#"{"kind":"storage#object","name":"test"}"#;

        // Act
        let result = extract_json_string_value(json, "size");

        // Assert
        assert!(result.is_none());
    }

    // =========== Helper ===========

    fn make_noop_executor() -> CloudExecutor {
        CloudExecutor::new(None)
    }
}
