//! Azure Blob Storage Provider
//!
//! Production implementation using direct REST API (no SDK dependency):
//! - Shared Key authentication (HMAC-SHA256 over canonicalized headers)
//! - SAS token authentication (pre-signed query string)
//! - Non-blocking callback-based API via `CloudExecutor`
//! - All operations routed through the same `CloudBackend` trait as S3

use crate::common::{MidgeError, MidgeResult};
use crate::storage::cloud::{
    CloudBackend, CloudCallback, CloudEvent, CloudExecutor, CloudOutcome, CloudRequest,
    CloudResponse, CloudSigner, ObjectMetadata,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as Base64Engine};
use chrono::Utc;
use hmac::{Hmac, Mac};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use reqwest::Method;
use sha2::Sha256;
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

/// Azure authentication credentials.
#[derive(Debug, Clone)]
pub enum AzureCredential {
    /// Shared key (account name + account key) — HMAC-SHA256 signing.
    SharedKey { account_key: String },
    /// SAS token — pre-signed query string appended to every URL.
    SasToken { token: String },
}

// ---------------------------------------------------------------------------
// Public provider
// ---------------------------------------------------------------------------

/// Azure Blob Storage provider.
///
/// Wraps an [`AzureBackend`] that sends real HTTP requests via
/// [`CloudExecutor`]. Follows the same architecture as [`S3Provider`].
pub struct AzureProvider {
    backend: Arc<dyn CloudBackend>,
    account_name: String,
    container: String,
    credential: AzureCredential,
}

impl AzureProvider {
    /// Create provider with Shared Key authentication.
    pub fn with_shared_key(account_name: String, container: String, account_key: String) -> Self {
        let credential = AzureCredential::SharedKey {
            account_key: account_key.clone(),
        };
        let signer: Option<Arc<dyn CloudSigner>> = Some(Arc::new(SharedKeySigner::new(
            account_name.clone(),
            account_key,
        )));
        let executor = CloudExecutor::new(signer);
        let backend = Arc::new(AzureBackend::new(
            account_name.clone(),
            container.clone(),
            None, // no SAS — signer handles auth
            executor,
        ));
        Self {
            backend,
            account_name,
            container,
            credential,
        }
    }

    /// Create provider with SAS token authentication.
    pub fn with_sas_token(account_name: String, container: String, sas_token: String) -> Self {
        // Normalise: strip leading '?' if present.
        let token = sas_token
            .strip_prefix('?')
            .unwrap_or(&sas_token)
            .to_string();
        let credential = AzureCredential::SasToken {
            token: token.clone(),
        };
        let executor = CloudExecutor::new(None); // SAS goes on the URL, no signer
        let backend = Arc::new(AzureBackend::new(
            account_name.clone(),
            container.clone(),
            Some(token),
            executor,
        ));
        Self {
            backend,
            account_name,
            container,
            credential,
        }
    }

    /// Legacy constructor — defaults to shared key with an empty key.
    /// Callers should prefer `with_shared_key` or `with_sas_token`.
    pub fn new(account_name: String, container: String) -> Self {
        Self::with_shared_key(account_name, container, String::new())
    }

    /// Get the underlying cloud backend for use with `CloudStorage`.
    pub fn backend(&self) -> Arc<dyn CloudBackend> {
        Arc::clone(&self.backend)
    }

    /// Expose account name for tests.
    #[cfg(test)]
    pub(crate) fn account_name(&self) -> &str {
        &self.account_name
    }

    /// Expose container for tests.
    #[cfg(test)]
    pub(crate) fn container(&self) -> &str {
        &self.container
    }

    /// Expose credential for tests.
    #[cfg(test)]
    pub(crate) fn credential(&self) -> &AzureCredential {
        &self.credential
    }
}

// ---------------------------------------------------------------------------
// Backend (private — implements CloudBackend)
// ---------------------------------------------------------------------------

struct AzureBackend {
    account_name: String,
    container: String,
    /// If present, appended as `?{sas_token}` to every URL.
    sas_token: Option<String>,
    executor: CloudExecutor,
}

impl AzureBackend {
    fn new(
        account_name: String,
        container: String,
        sas_token: Option<String>,
        executor: CloudExecutor,
    ) -> Self {
        Self {
            account_name,
            container,
            sas_token,
            executor,
        }
    }

    /// Base URL: `https://{account}.blob.core.windows.net/{container}`
    fn base_url(&self) -> String {
        format!(
            "https://{}.blob.core.windows.net/{}",
            self.account_name, self.container
        )
    }

    fn canonical_key(&self, key: &str) -> String {
        key.split('/')
            .map(|seg| utf8_percent_encode(seg, ENCODE_SET).to_string())
            .collect::<Vec<_>>()
            .join("/")
    }

    /// Object URL, with optional SAS token.
    fn object_url(&self, key: &str) -> String {
        let base = format!("{}/{}", self.base_url(), self.canonical_key(key));
        match &self.sas_token {
            Some(tok) => format!("{}?{}", base, tok),
            None => base,
        }
    }

    /// List URL — uses Azure's `restype=container&comp=list&prefix=...`.
    fn list_url(&self, prefix: &str) -> String {
        let base = format!(
            "{}?restype=container&comp=list&prefix={}",
            self.base_url(),
            urlencoding::encode(prefix)
        );
        match &self.sas_token {
            Some(tok) => format!("{}&{}", base, tok),
            None => base,
        }
    }
}

impl CloudBackend for AzureBackend {
    fn submit_put(&self, key: String, data: Vec<u8>, callback: CloudCallback) {
        let url = self.object_url(&key);
        let len = data.len();
        let request = CloudRequest::new(Method::PUT, url)
            .with_body(data)
            .with_header("x-ms-blob-type", "BlockBlob")
            .with_header("Content-Length", len.to_string());
        let mapper = move |ctx: String, result: MidgeResult<CloudResponse>| match result {
            Ok(resp) if resp.status < 400 => CloudEvent::PutComplete {
                key: ctx,
                result: CloudOutcome::Ok(()),
            },
            Ok(resp) => CloudEvent::PutComplete {
                key: ctx,
                result: CloudOutcome::Err(format!("Azure PUT status {}", resp.status)),
            },
            Err(err) => CloudEvent::PutComplete {
                key: ctx,
                result: CloudOutcome::Err(format!("{:?}", err)),
            },
        };
        self.executor.spawn_request(request, key, callback, mapper);
    }

    fn submit_get(&self, key: String, callback: CloudCallback) {
        let url = self.object_url(&key);
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
                result: CloudOutcome::Err(format!("Azure GET status {}", resp.status)),
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
        let url = self.object_url(&key);
        let range = match end {
            Some(e) => format!("bytes={}-{}", start, e.saturating_sub(1)),
            None => format!("bytes={}-", start),
        };
        let request = CloudRequest::new(Method::GET, url).with_header("x-ms-range", range);
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
                result: CloudOutcome::Err(format!("Azure GET_RANGE status {}", resp.status)),
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
        let url = self.object_url(&key);
        let request = CloudRequest::new(Method::DELETE, url);
        let mapper = move |ctx: String, result: MidgeResult<CloudResponse>| match result {
            Ok(resp) if resp.status < 400 => CloudEvent::DeleteComplete {
                key: ctx,
                result: CloudOutcome::Ok(()),
            },
            Ok(resp) => CloudEvent::DeleteComplete {
                key: ctx,
                result: CloudOutcome::Err(format!("Azure DELETE status {}", resp.status)),
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
                let body = String::from_utf8_lossy(&resp.body);
                let mut items = Vec::new();
                // Azure LIST XML: <Name>key</Name>
                for line in body.lines() {
                    if let Some(start) = line.find("<Name>") {
                        if let Some(end) = line.find("</Name>") {
                            items.push(line[start + 6..end].to_string());
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
                result: CloudOutcome::Err(format!("Azure LIST status {}", resp.status)),
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
        let url = self.object_url(&key);
        let request = CloudRequest::new(Method::HEAD, url);
        let mapper = move |ctx: String, result: MidgeResult<CloudResponse>| match result {
            Ok(resp) if resp.status == 200 => {
                let size = resp
                    .headers
                    .iter()
                    .find(|(n, _)| n.eq_ignore_ascii_case("content-length"))
                    .and_then(|(_, v)| v.parse().ok())
                    .unwrap_or(0);
                let etag = resp
                    .headers
                    .iter()
                    .find(|(n, _)| n.eq_ignore_ascii_case("etag"))
                    .map(|(_, v)| v.trim_matches('"').to_string())
                    .unwrap_or_default();
                let metadata = ObjectMetadata::new(size, etag, 0);
                CloudEvent::HeadComplete {
                    key: ctx,
                    result: CloudOutcome::Ok(metadata),
                }
            }
            Ok(resp) => CloudEvent::HeadComplete {
                key: ctx,
                result: CloudOutcome::Err(format!("Azure HEAD status {}", resp.status)),
            },
            Err(err) => CloudEvent::HeadComplete {
                key: ctx,
                result: CloudOutcome::Err(format!("{:?}", err)),
            },
        };
        self.executor.spawn_request(request, key, callback, mapper);
    }
}

// ---------------------------------------------------------------------------
// Shared Key Signer — Azure Storage Services Signature (version 2)
//
// Reference: https://learn.microsoft.com/en-us/rest/api/storageservices/
//            authorize-with-shared-key
// ---------------------------------------------------------------------------

struct SharedKeySigner {
    account_name: String,
    /// Base64-decoded account key (raw bytes for HMAC-SHA256).
    decoded_key: Vec<u8>,
}

impl SharedKeySigner {
    fn new(account_name: String, account_key_base64: String) -> Self {
        let decoded_key = BASE64.decode(&account_key_base64).unwrap_or_default();
        Self {
            account_name,
            decoded_key,
        }
    }

    /// Build the string-to-sign per Azure Shared Key for Blob/Queue.
    ///
    /// See: <https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key>
    ///
    /// Layout:
    /// ```text
    /// VERB\n
    /// Content-Encoding\n
    /// Content-Language\n
    /// Content-Length\n
    /// Content-MD5\n
    /// Content-Type\n
    /// Date\n
    /// If-Modified-Since\n
    /// If-Match\n
    /// If-None-Match\n
    /// If-Unmodified-Since\n
    /// Range\n
    /// CanonicalizedHeaders\n
    /// CanonicalizedResource
    /// ```
    fn string_to_sign(
        &self,
        method: &str,
        headers: &[(String, String)],
        url: &url::Url,
        content_length: Option<usize>,
    ) -> String {
        let hdr = |name: &str| -> String {
            headers
                .iter()
                .find(|(n, _)| n.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.clone())
                .unwrap_or_default()
        };

        let content_len_str = match content_length {
            Some(0) | None => String::new(),
            Some(n) => n.to_string(),
        };

        // Canonicalized x-ms-* headers, sorted, colon-separated.
        let mut x_ms: Vec<(String, String)> = headers
            .iter()
            .filter(|(n, _)| n.starts_with("x-ms-"))
            .map(|(n, v)| (n.to_lowercase(), v.trim().to_string()))
            .collect();
        x_ms.sort_by(|a, b| a.0.cmp(&b.0));
        let canonical_headers: String = x_ms
            .iter()
            .map(|(k, v)| format!("{}:{}\n", k, v))
            .collect();

        // Canonicalized resource: /{account}/{path}?{sorted-query-params}
        let path = url.path();
        let mut canonical_resource = format!("/{}{}", self.account_name, path);
        let mut query_pairs: Vec<(String, String)> = url
            .query_pairs()
            .map(|(k, v)| (k.to_lowercase(), v.to_string()))
            .collect();
        query_pairs.sort();
        for (k, v) in &query_pairs {
            canonical_resource.push_str(&format!("\n{}:{}", k, v));
        }

        format!(
            "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}{}",
            method,
            hdr("Content-Encoding"),
            hdr("Content-Language"),
            content_len_str,
            hdr("Content-MD5"),
            hdr("Content-Type"),
            hdr("Date"),
            hdr("If-Modified-Since"),
            hdr("If-Match"),
            hdr("If-None-Match"),
            hdr("If-Unmodified-Since"),
            hdr("Range"),
            canonical_headers,
            canonical_resource,
        )
    }
}

impl CloudSigner for SharedKeySigner {
    fn sign(&self, request: &mut CloudRequest) -> MidgeResult<()> {
        let url = url::Url::parse(&request.url)
            .map_err(|e| MidgeError::InvalidArgument(format!("url parse: {}", e)))?;

        // Ensure mandatory x-ms-* headers.
        let now = Utc::now();
        let date = now.format("%a, %d %b %Y %H:%M:%S GMT").to_string();

        // Remove any stale date / version headers.
        request.headers.retain(|(n, _)| {
            !n.eq_ignore_ascii_case("x-ms-date") && !n.eq_ignore_ascii_case("x-ms-version")
        });
        request.headers.push(("x-ms-date".into(), date));
        request
            .headers
            .push(("x-ms-version".into(), "2024-11-04".into()));

        let content_length = request.body.as_ref().map(|b| b.len());

        let sts =
            self.string_to_sign(request.method.as_str(), &request.headers, &url, content_length);

        let mut mac = Hmac::<Sha256>::new_from_slice(&self.decoded_key)
            .map_err(|_| MidgeError::Internal("hmac init failed".into()))?;
        mac.update(sts.as_bytes());
        let signature = BASE64.encode(mac.finalize().into_bytes());

        let auth = format!("SharedKey {}:{}", self.account_name, signature);
        request.headers.push(("Authorization".into(), auth));

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

    // =========== AzureCredential Tests ===========

    #[test]
    fn should_create_shared_key_credential() {
        // Arrange
        let account_key = "mykey".to_string();

        // Act
        let cred = AzureCredential::SharedKey { account_key };

        // Assert
        match cred {
            AzureCredential::SharedKey { account_key } => assert_eq!(account_key, "mykey"),
            _ => panic!("Expected SharedKey credential"),
        }
    }

    #[test]
    fn should_create_sas_token_credential() {
        // Arrange
        let token = "token123".to_string();

        // Act
        let cred = AzureCredential::SasToken { token };

        // Assert
        match cred {
            AzureCredential::SasToken { token } => assert_eq!(token, "token123"),
            _ => panic!("Expected SasToken credential"),
        }
    }

    // =========== AzureProvider Construction Tests ===========

    #[test]
    fn should_create_provider_with_shared_key() {
        // Arrange
        let account = "myaccount";
        let container = "mycontainer";
        let key = "accountkey123";

        // Act
        let provider = AzureProvider::with_shared_key(
            account.into(),
            container.into(),
            key.into(),
        );

        // Assert
        assert_eq!(provider.account_name(), "myaccount");
        assert_eq!(provider.container(), "mycontainer");
        assert!(matches!(
            provider.credential(),
            AzureCredential::SharedKey { .. }
        ));
    }

    #[test]
    fn should_create_provider_with_sas_token() {
        // Arrange
        let account = "myaccount";
        let container = "mycontainer";
        let sas_token = "sv=2021-06-08&ss=b&srt=sco";

        // Act
        let provider = AzureProvider::with_sas_token(
            account.into(),
            container.into(),
            sas_token.into(),
        );

        // Assert
        assert_eq!(provider.account_name(), "myaccount");
        match provider.credential() {
            AzureCredential::SasToken { token } => assert!(token.contains("sv=")),
            _ => panic!("Expected SasToken credential"),
        }
    }

    #[test]
    fn should_normalize_sas_token_with_question_mark() {
        // Arrange
        let sas_token = "?sv=2021-06-08&ss=b";

        // Act
        let provider = AzureProvider::with_sas_token(
            "account".into(),
            "container".into(),
            sas_token.into(),
        );

        // Assert
        match provider.credential() {
            AzureCredential::SasToken { token } => {
                assert!(!token.starts_with('?'));
                assert!(token.contains("sv="));
            }
            _ => panic!("Expected SasToken credential"),
        }
    }

    #[test]
    fn should_normalize_sas_token_without_question_mark() {
        // Arrange
        let sas_token = "sv=2021-06-08&ss=b";

        // Act
        let provider = AzureProvider::with_sas_token(
            "account".into(),
            "container".into(),
            sas_token.into(),
        );

        // Assert
        match provider.credential() {
            AzureCredential::SasToken { token } => assert_eq!(token, "sv=2021-06-08&ss=b"),
            _ => panic!("Expected SasToken credential"),
        }
    }

    #[test]
    fn should_default_to_shared_key_with_new() {
        // Arrange
        let account = "account";
        let container = "container";

        // Act
        let provider = AzureProvider::new(account.into(), container.into());

        // Assert
        assert!(matches!(
            provider.credential(),
            AzureCredential::SharedKey { .. }
        ));
    }

    #[test]
    fn should_handle_empty_account_name() {
        // Arrange
        let account = "";
        let container = "container";

        // Act
        let provider = AzureProvider::new(account.into(), container.into());

        // Assert
        assert_eq!(provider.account_name(), "");
        assert_eq!(provider.container(), "container");
    }

    #[test]
    fn should_handle_empty_container_name() {
        // Arrange
        let account = "account";
        let container = "";

        // Act
        let provider = AzureProvider::new(account.into(), container.into());

        // Assert
        assert_eq!(provider.account_name(), "account");
        assert_eq!(provider.container(), "");
    }

    #[test]
    fn should_handle_special_characters_in_names() {
        // Arrange
        let account = "my-account-123";
        let container = "my-container-456";

        // Act
        let provider = AzureProvider::new(account.into(), container.into());

        // Assert
        assert_eq!(provider.account_name(), "my-account-123");
        assert_eq!(provider.container(), "my-container-456");
    }

    #[test]
    fn should_create_provider_with_different_shared_keys() {
        // Arrange
        let (a1, c1, k1) = ("a1", "c1", "k1");
        let (a2, c2, k2) = ("a2", "c2", "k2");

        // Act
        let p1 = AzureProvider::with_shared_key(a1.into(), c1.into(), k1.into());
        let p2 = AzureProvider::with_shared_key(a2.into(), c2.into(), k2.into());

        // Assert
        assert_ne!(p1.account_name(), p2.account_name());
    }

    // =========== AzureBackend URL Tests ===========

    #[test]
    fn should_build_correct_base_url() {
        // Arrange
        let backend = AzureBackend::new(
            "myaccount".into(),
            "mycontainer".into(),
            None,
            make_noop_executor(),
        );

        // Act
        let url = backend.base_url();

        // Assert
        assert_eq!(url, "https://myaccount.blob.core.windows.net/mycontainer");
    }

    #[test]
    fn should_build_correct_object_url_without_sas() {
        // Arrange
        let backend =
            AzureBackend::new("acct".into(), "ctr".into(), None, make_noop_executor());

        // Act
        let url = backend.object_url("path/to/blob");

        // Assert
        assert_eq!(url, "https://acct.blob.core.windows.net/ctr/path/to/blob");
    }

    #[test]
    fn should_append_sas_token_to_object_url() {
        // Arrange
        let backend = AzureBackend::new(
            "acct".into(),
            "ctr".into(),
            Some("sv=2021&sig=abc".into()),
            make_noop_executor(),
        );

        // Act
        let url = backend.object_url("myblob");

        // Assert
        assert!(url.contains("?sv=2021&sig=abc"));
    }

    #[test]
    fn should_build_correct_list_url() {
        // Arrange
        let backend =
            AzureBackend::new("acct".into(), "ctr".into(), None, make_noop_executor());

        // Act
        let url = backend.list_url("wal/");

        // Assert
        assert!(url.contains("restype=container"));
        assert!(url.contains("comp=list"));
        assert!(url.contains("prefix=wal%2F"));
    }

    // =========== SharedKeySigner Tests ===========

    #[test]
    fn should_add_authorization_header_when_signing() {
        // Arrange
        let signer = SharedKeySigner::new(
            "devstoreaccount1".into(),
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".into(),
        );
        let mut request = CloudRequest::new(
            Method::PUT,
            "https://devstoreaccount1.blob.core.windows.net/mycontainer/myblob".into(),
        );

        // Act
        let result = signer.sign(&mut request);

        // Assert
        assert!(result.is_ok());
        let has_auth = request
            .headers
            .iter()
            .any(|(n, _)| n.eq_ignore_ascii_case("Authorization"));
        assert!(has_auth, "should have Authorization header");
    }

    #[test]
    fn should_include_xms_date_header_when_signing() {
        // Arrange
        let signer = SharedKeySigner::new("acct".into(), "dGVzdA==".into());
        let mut request = CloudRequest::new(
            Method::GET,
            "https://acct.blob.core.windows.net/ctr/blob".into(),
        );

        // Act
        let _ = signer.sign(&mut request);

        // Assert
        let has_date = request.headers.iter().any(|(n, _)| n == "x-ms-date");
        assert!(has_date, "should have x-ms-date header");
    }

    #[test]
    fn should_include_xms_version_header_when_signing() {
        // Arrange
        let signer = SharedKeySigner::new("acct".into(), "dGVzdA==".into());
        let mut request = CloudRequest::new(
            Method::GET,
            "https://acct.blob.core.windows.net/ctr/blob".into(),
        );

        // Act
        let _ = signer.sign(&mut request);

        // Assert
        let version = request
            .headers
            .iter()
            .find(|(n, _)| n == "x-ms-version")
            .map(|(_, v)| v.as_str());
        assert_eq!(version, Some("2024-11-04"));
    }

    // =========== Helper ===========

    /// Create a no-op executor for URL-building tests (no signer).
    fn make_noop_executor() -> CloudExecutor {
        CloudExecutor::new(None)
    }
}
