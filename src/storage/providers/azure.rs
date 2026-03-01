//! Azure Blob Storage Provider
//!
//! Production implementation using direct REST API (no SDK dependency):
//! - Shared Key authentication (HMAC-SHA256 over canonicalized headers)
//! - SAS token authentication (pre-signed query string)
//! - Managed Identity (System-assigned and User-assigned via IMDS)
//! - Non-blocking callback-based API via `CloudExecutor`
//! - All operations routed through the same `CloudBackend` trait as S3

use crate::common::{MidgeError, MidgeResult};
use super::super::cloud::{
    CloudBackend, CloudCallback, CloudEvent, CloudExecutor, CloudOutcome, CloudRequest,
    CloudResponse, CloudSigner, ObjectMetadata,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as Base64Engine};
use chrono::Utc;
use hmac::{Hmac, Mac};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use reqwest::Method;
use sha2::Sha256;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

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
    /// Managed Identity — OAuth bearer tokens from Azure IMDS.
    /// Supports both system-assigned and user-assigned (via client_id).
    ManagedIdentity { client_id: Option<String> },
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
    pub fn with_shared_key(account_name: String, container: String, account_key: String) -> MidgeResult<Self> {
        let credential = AzureCredential::SharedKey {
            account_key: account_key.clone(),
        };
        let signer = SharedKeySigner::new(account_name.clone(), account_key)?;
        let executor = CloudExecutor::new(Some(Arc::new(signer)))?;
        let backend = Arc::new(AzureBackend::new(
            account_name.clone(),
            container.clone(),
            None, // no SAS — signer handles auth
            executor,
        ));
        Ok(Self {
            backend,
            account_name,
            container,
            credential,
        })
    }

    /// Create provider with SAS token authentication.
    pub fn with_sas_token(account_name: String, container: String, sas_token: String) -> MidgeResult<Self> {
        // Normalise: strip leading '?' if present.
        let token = sas_token
            .strip_prefix('?')
            .unwrap_or(&sas_token)
            .to_string();
        let credential = AzureCredential::SasToken {
            token: token.clone(),
        };
        let executor = CloudExecutor::new(None)?; // SAS goes on the URL, no signer
        let backend = Arc::new(AzureBackend::new(
            account_name.clone(),
            container.clone(),
            Some(token),
            executor,
        ));
        Ok(Self {
            backend,
            account_name,
            container,
            credential,
        })
    }

    /// Create provider with Managed Identity authentication.
    ///
    /// Uses Azure Instance Metadata Service (IMDS) to fetch OAuth tokens.
    /// Supports both system-assigned and user-assigned managed identities.
    ///
    /// # Arguments
    /// * `account_name` - Storage account name
    /// * `container` - Blob container name
    /// * `client_id` - Optional client ID for user-assigned identity.
    ///   If None, uses system-assigned identity.
    ///
    /// # Environment Variables
    /// - `AZURE_CLIENT_ID`: User-assigned managed identity client ID
    /// - `MSI_ENDPOINT`: Custom IMDS endpoint (for testing)
    /// - `IDENTITY_ENDPOINT`: Alternative endpoint (App Service/Container Apps)
    ///
    /// # Examples
    /// ```no_run
    /// # use cntryl_midge::storage::providers::AzureProvider;
    /// // System-assigned identity
    /// let provider = AzureProvider::with_managed_identity(
    ///     "mystorageaccount".into(),
    ///     "mycontainer".into(),
    ///     None
    /// )?;
    ///
    /// // User-assigned identity
    /// std::env::set_var("AZURE_CLIENT_ID", "00000000-0000-0000-0000-000000000000");
    /// let provider = AzureProvider::with_managed_identity(
    ///     "mystorageaccount".into(),
    ///     "mycontainer".into(),
    ///     Some("00000000-0000-0000-0000-000000000000".into())
    /// )?;
    /// # Ok::<(), cntryl_midge::common::MidgeError>(())
    /// ```
    pub fn with_managed_identity(
        account_name: String,
        container: String,
        client_id: Option<String>,
    ) -> MidgeResult<Self> {
        // Determine client_id: explicit arg > env var > None (system-assigned)
        let effective_client_id = client_id
            .or_else(|| std::env::var("AZURE_CLIENT_ID").ok())
            .filter(|id| !id.is_empty());

        let credential = AzureCredential::ManagedIdentity {
            client_id: effective_client_id.clone(),
        };

        let signer: Arc<dyn CloudSigner> = Arc::new(ManagedIdentitySigner::new(
            account_name.clone(),
            effective_client_id,
        )?);
        
        let executor = CloudExecutor::new(Some(signer))?;
        let backend = Arc::new(AzureBackend::new(
            account_name.clone(),
            container.clone(),
            None, // No SAS token for managed identity
            executor,
        ));

        Ok(Self {
            backend,
            account_name,
            container,
            credential,
        })
    }

    /// Legacy constructor — defaults to shared key with an empty key.
    /// Callers should prefer `with_shared_key` or `with_sas_token`.
    pub fn new(account_name: String, container: String) -> MidgeResult<Self> {
        Self::with_shared_key(account_name, container, String::new())
    }

    /// Create provider with automatic credential discovery from environment.
    ///
    /// Credentials are discovered in the following order:
    /// 1. **SharedKey**: `AZURE_STORAGE_KEY` or connection string
    /// 2. **SAS Token**: `AZURE_STORAGE_SAS_TOKEN`
    /// 3. **Managed Identity**: `AZURE_CLIENT_ID` (or system-assigned if none)
    ///
    /// # Environment Variables
    /// - `AZURE_STORAGE_CONNECTION_STRING`: Full connection string (highest priority)
    /// - `AZURE_STORAGE_ACCOUNT`: Storage account name (if not in args)
    /// - `AZURE_STORAGE_KEY`: Account key for SharedKey auth
    /// - `AZURE_STORAGE_SAS_TOKEN`: SAS token
    /// - `AZURE_CLIENT_ID`: User-assigned managed identity client ID
    ///
    /// # Examples
    /// ```no_run
    /// # use cntryl_midge::storage::providers::AzureProvider;
    /// // Using connection string
    /// std::env::set_var("AZURE_STORAGE_CONNECTION_STRING", 
    ///     "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=...");
    /// let provider = AzureProvider::from_env("myaccount".into(), "mycontainer".into())?;
    ///
    /// // Using managed identity
    /// std::env::set_var("AZURE_CLIENT_ID", "00000000-0000-0000-0000-000000000000");
    /// let provider = AzureProvider::from_env("myaccount".into(), "mycontainer".into())?;
    /// # Ok::<(), cntryl_midge::common::MidgeError>(())
    /// ```
    pub fn from_env(account_name: String, container: String) -> MidgeResult<Self> {
        // Try connection string first
        if let Ok(conn_str) = std::env::var("AZURE_STORAGE_CONNECTION_STRING") {
            return Self::from_connection_string(conn_str, container);
        }

        // Try explicit storage key
        if let Ok(key) = std::env::var("AZURE_STORAGE_KEY") {
            return Self::with_shared_key(account_name, container, key);
        }

        // Try SAS token
        if let Ok(sas) = std::env::var("AZURE_STORAGE_SAS_TOKEN") {
            return Self::with_sas_token(account_name, container, sas);
        }

        // Default to managed identity (checks AZURE_CLIENT_ID internally)
        Self::with_managed_identity(account_name, container, None)
    }

    /// Create provider from Azure Storage connection string.
    ///
    /// Parses connection strings in the format:
    /// `DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=...`
    fn from_connection_string(conn_str: String, container: String) -> MidgeResult<Self> {
        let mut account_name: Option<String> = None;
        let mut account_key: Option<String> = None;

        for part in conn_str.split(';') {
            let kv: Vec<&str> = part.splitn(2, '=').collect();
            if kv.len() == 2 {
                match kv[0] {
                    "AccountName" => account_name = Some(kv[1].to_string()),
                    "AccountKey" => account_key = Some(kv[1].to_string()),
                    _ => {}
                }
            }
        }

        let account = account_name
            .ok_or_else(|| MidgeError::InvalidArgument("Missing AccountName in connection string".into()))?;
        let key = account_key
            .ok_or_else(|| MidgeError::InvalidArgument("Missing AccountKey in connection string".into()))?;

        Self::with_shared_key(account, container, key)
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
    fn submit_put(&self, key: String, data: Vec<u8>, headers: Vec<(String, String)>, callback: CloudCallback) {
        let url = self.object_url(&key);
        let len = data.len();
        let mut request = CloudRequest::new(Method::PUT, url)
            .with_body(data)
            .with_header("x-ms-blob-type", "BlockBlob")
            .with_header("Content-Length", len.to_string());
        // Merge provided headers into the request (caller-controlled; e.g. If-None-Match)
        for (name, value) in headers.into_iter() {
            request = request.with_header(name, value);
        }
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
                // Azure LIST XML response: <Blob><Name>key</Name>...</Blob>
                // Use a more robust pattern that requires <Name> to be a complete XML tag
                let mut search_pos = 0;
                while let Some(start) = body[search_pos..].find("<Name>") {
                    let start = search_pos + start;
                    if let Some(end) = body[start..].find("</Name>") {
                        let end = start + end;
                        let name_content = &body[start + 6..end];
                        items.push(name_content.to_string());
                        search_pos = end + 7; // Move past </Name>
                    } else {
                        break; // No closing tag found
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
    fn new(account_name: String, account_key_base64: String) -> MidgeResult<Self> {
        let decoded_key = BASE64.decode(&account_key_base64)
            .map_err(|e| MidgeError::InvalidArgument(
                format!("Invalid base64 in account key: {}. \
                         Ensure AZURE_STORAGE_KEY is properly base64-encoded", e)))?;
        Ok(Self {
            account_name,
            decoded_key,
        })
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
// Managed Identity Signer (OAuth Bearer Token)
// ---------------------------------------------------------------------------

/// Cached OAuth token with expiry tracking.
#[derive(Debug, Clone)]
struct CachedToken {
    access_token: String,
    expires_at: u64, // Unix timestamp
}

impl CachedToken {
    fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        // Refresh 5 minutes before actual expiry
        now >= self.expires_at.saturating_sub(300)
    }
}

/// Managed Identity signer that fetches OAuth tokens from Azure IMDS.
///
/// Supports:
/// - System-assigned managed identity
/// - User-assigned managed identity (via client_id)
/// - Multiple IMDS endpoints (VM, App Service, Container Apps)
/// - Token caching and automatic refresh
struct ManagedIdentitySigner {
    account_name: String,
    client_id: Option<String>,
    /// Cached token with expiry tracking
    token_cache: Arc<Mutex<Option<CachedToken>>>,
    /// IMDS endpoint (defaults to standard Azure IMDS)
    imds_endpoint: String,
}

impl ManagedIdentitySigner {
    fn new(account_name: String, client_id: Option<String>) -> MidgeResult<Self> {
        // Determine IMDS endpoint from environment
        let imds_endpoint = std::env::var("IDENTITY_ENDPOINT")
            .or_else(|_| std::env::var("MSI_ENDPOINT"))
            .unwrap_or_else(|_| "http://169.254.169.254/metadata/identity/oauth2/token".into());

        Ok(Self {
            account_name,
            client_id,
            token_cache: Arc::new(Mutex::new(None)),
            imds_endpoint,
        })
    }

    /// Fetch a fresh OAuth token from Azure IMDS.
    fn fetch_token(&self) -> MidgeResult<CachedToken> {
        // Build IMDS request URL
        let mut url = format!(
            "{}?api-version=2019-08-01&resource=https://storage.azure.com/",
            self.imds_endpoint
        );

        // Add client_id for user-assigned identity
        if let Some(ref client_id) = self.client_id {
            url.push_str(&format!("&client_id={}", client_id));
        }

        // Make synchronous HTTP request to IMDS
        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| MidgeError::Internal(format!("IMDS client init: {}", e)))?;

        let response = client
            .get(&url)
            .header("Metadata", "true")
            .send()
            .map_err(|e| {
                MidgeError::Internal(format!(
                    "Failed to fetch managed identity token from IMDS: {}. \
                     Ensure managed identity is enabled on this Azure resource.",
                    e
                ))
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(MidgeError::Internal(format!(
                "IMDS token request failed with status {}: {}. \
                 Ensure managed identity is properly configured.",
                status, body
            )));
        }

        // Parse JSON response
        let body = response.text().map_err(|e| {
            MidgeError::Internal(format!("Failed to read IMDS response: {}", e))
        })?;

        let json: serde_json::Value = serde_json::from_str(&body).map_err(|e| {
            MidgeError::Internal(format!("Failed to parse IMDS JSON: {}", e))
        })?;

        let access_token = json["access_token"]
            .as_str()
            .ok_or_else(|| MidgeError::Internal("Missing access_token in IMDS response".into()))?
            .to_string();

        // Parse expires_on (Unix timestamp string)
        let expires_on = json["expires_on"]
            .as_str()
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| MidgeError::Internal("Missing or invalid expires_on in IMDS response".into()))?;

        Ok(CachedToken {
            access_token,
            expires_at: expires_on,
        })
    }

    /// Get a valid cached token, refreshing if necessary.
    fn get_token(&self) -> MidgeResult<String> {
        let mut cache = match self.token_cache.lock() {
            Ok(guard) => guard,
            Err(_poisoned) => {
                return Err(MidgeError::Internal(
                    "Token cache mutex poisoned; token fetch failed in another thread. Restart required.".into()
                ))
            }
        };

        // Check if cached token is valid
        if let Some(ref token) = *cache {
            if !token.is_expired() {
                return Ok(token.access_token.clone());
            }
        }

        // Fetch fresh token
        let fresh_token = self.fetch_token()?;
        let access_token = fresh_token.access_token.clone();
        *cache = Some(fresh_token);

        Ok(access_token)
    }
}

impl CloudSigner for ManagedIdentitySigner {
    fn sign(&self, request: &mut CloudRequest) -> MidgeResult<()> {
        // Get valid OAuth token
        let token = self.get_token()?;

        // Add Bearer token authorization header
        request.headers.push((
            "Authorization".into(),
            format!("Bearer {}", token),
        ));

        // Add required Azure headers
        let now = Utc::now();
        let date = now.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        request.headers.push(("x-ms-date".into(), date));
        request.headers.push(("x-ms-version".into(), "2024-11-04".into()));

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
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

    #[test]
    fn should_create_managed_identity_credential_system_assigned() {
        // Arrange

        // Act
        let cred = AzureCredential::ManagedIdentity { client_id: None };

        // Assert
        match cred {
            AzureCredential::ManagedIdentity { client_id } => assert!(client_id.is_none()),
            _ => panic!("Expected ManagedIdentity credential"),
        }
    }

    #[test]
    fn should_create_managed_identity_credential_user_assigned() {
        // Arrange
        let client_id = "00000000-0000-0000-0000-000000000000".to_string();

        // Act
        let cred = AzureCredential::ManagedIdentity {
            client_id: Some(client_id.clone()),
        };

        // Assert
        match cred {
            AzureCredential::ManagedIdentity {
                client_id: Some(id),
            } => assert_eq!(id, "00000000-0000-0000-0000-000000000000"),
            _ => panic!("Expected ManagedIdentity credential with client_id"),
        }
    }

    // =========== AzureProvider Construction Tests ===========

    #[test]
    fn should_create_provider_with_shared_key() {
        // Arrange
        let account = "myaccount";
        let container = "mycontainer";
        let key = "YWNjb3VudGtleTEyMw==";

        // Act
        let provider = AzureProvider::with_shared_key(
            account.into(),
            container.into(),
            key.into(),
        )
        .expect("should create provider with shared key");

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
        )
        .expect("should create provider with sas token");

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
        )
        .expect("should create provider with normalized sas token");

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
        )
        .expect("should create provider with sas token without question mark");

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
        let provider = provider.expect("should create provider with default shared key");

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
        let provider = provider.expect("should create provider with empty account");

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
        let provider = provider.expect("should create provider with empty container");

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
        let provider = provider.expect("should create provider with special characters");

        // Assert
        assert_eq!(provider.account_name(), "my-account-123");
        assert_eq!(provider.container(), "my-container-456");
    }

    #[test]
    fn should_create_provider_with_different_shared_keys() {
        // Arrange
        let (a1, c1, k1) = ("a1", "c1", "YTEta2V5");
        let (a2, c2, k2) = ("a2", "c2", "YTIta2V5");

        // Act
        let p1 = AzureProvider::with_shared_key(a1.into(), c1.into(), k1.into());
        let p2 = AzureProvider::with_shared_key(a2.into(), c2.into(), k2.into());
        let p1 = p1.expect("should create first provider");
        let p2 = p2.expect("should create second provider");

        // Assert
        assert_ne!(p1.account_name(), p2.account_name());
    }

    #[test]
    fn should_create_provider_with_managed_identity_system_assigned() {
        // Arrange
        let account = "myaccount";
        let container = "mycontainer";

        // Act
        let result = AzureProvider::with_managed_identity(
            account.into(),
            container.into(),
            None,
        );

        // Assert
        assert!(result.is_ok());
        let provider = result.unwrap();
        assert_eq!(provider.account_name(), "myaccount");
        assert_eq!(provider.container(), "mycontainer");
        assert!(matches!(
            provider.credential(),
            AzureCredential::ManagedIdentity { client_id: None }
        ));
    }

    #[test]
    fn should_create_provider_with_managed_identity_user_assigned() {
        // Arrange
        let account = "myaccount";
        let container = "mycontainer";
        let client_id = "00000000-0000-0000-0000-000000000000";

        // Act
        let result = AzureProvider::with_managed_identity(
            account.into(),
            container.into(),
            Some(client_id.into()),
        );

        // Assert
        assert!(result.is_ok());
        let provider = result.unwrap();
        match provider.credential() {
            AzureCredential::ManagedIdentity {
                client_id: Some(id),
            } => assert_eq!(id, "00000000-0000-0000-0000-000000000000"),
            _ => panic!("Expected ManagedIdentity with client_id"),
        }
    }

    #[test]
    fn should_use_env_var_for_client_id_when_none_provided() {
        // Arrange
        std::env::set_var("AZURE_CLIENT_ID", "env-client-id");
        let account = "myaccount";
        let container = "mycontainer";

        // Act
        let result = AzureProvider::with_managed_identity(
            account.into(),
            container.into(),
            None,
        );

        // Assert
        std::env::remove_var("AZURE_CLIENT_ID");
        assert!(result.is_ok());
        let provider = result.unwrap();
        match provider.credential() {
            AzureCredential::ManagedIdentity {
                client_id: Some(id),
            } => assert_eq!(id, "env-client-id"),
            _ => panic!("Expected ManagedIdentity with client_id from env"),
        }
    }

    #[test]
    fn should_prefer_explicit_client_id_over_env() {
        // Arrange
        std::env::set_var("AZURE_CLIENT_ID", "env-client-id");
        let account = "myaccount";
        let container = "mycontainer";
        let explicit_id = "explicit-client-id";

        // Act
        let result = AzureProvider::with_managed_identity(
            account.into(),
            container.into(),
            Some(explicit_id.into()),
        );

        // Assert
        std::env::remove_var("AZURE_CLIENT_ID");
        assert!(result.is_ok());
        let provider = result.unwrap();
        match provider.credential() {
            AzureCredential::ManagedIdentity {
                client_id: Some(id),
            } => assert_eq!(id, "explicit-client-id"),
            _ => panic!("Expected explicit client_id to take precedence"),
        }
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
        ).expect("Failed to create signer");
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
        let signer = SharedKeySigner::new("acct".into(), "dGVzdA==".into())
            .expect("Failed to create signer");
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
        let signer = SharedKeySigner::new("acct".into(), "dGVzdA==".into())
            .expect("Failed to create signer");
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
        CloudExecutor::new(None).expect("Failed to create noop executor in test")
    }
}
