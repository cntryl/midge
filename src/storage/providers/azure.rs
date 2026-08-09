//! Azure Blob Storage Provider
//!
//! Production implementation using direct REST API (no SDK dependency):
//! - Shared Key authentication (HMAC-SHA256 over canonicalized headers)
//! - SAS token authentication (pre-signed query string)
//! - Managed Identity (System-assigned and User-assigned via IMDS)
//! - Non-blocking callback-based API via `CloudExecutor`
//! - All operations routed through the same `CloudBackend` trait as S3

use super::super::cloud::{
    CloudBackend, CloudCallback, CloudError, CloudEvent, CloudExecutor, CloudListBudget,
    CloudOutcome, CloudRequest, CloudResponse, CloudSigner, ObjectMetadata,
};
use crate::common::{MidgeError, MidgeResult};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as Base64Engine};
use chrono::Utc;
use hmac::{Hmac, KeyInit, Mac};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use reqwest::Method;
use sha2::Sha256;
use std::fmt::Write as _;
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
#[cfg(test)]
pub enum AzureCredential {
    /// Shared key (account name + account key) — HMAC-SHA256 signing.
    SharedKey { account_key: String },
    /// SAS token — pre-signed query string appended to every URL.
    SasToken { token: String },
    /// Managed Identity — OAuth bearer tokens from Azure IMDS.
    /// Supports both system-assigned and user-assigned (via `client_id`).
    ManagedIdentity { client_id: Option<String> },
    /// Lightweight OAuth credential resolved without Azure SDKs or CLI shell-out.
    OAuth { source: String },
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
    #[cfg(test)]
    account_name: String,
    #[cfg(test)]
    container: String,
    #[cfg(test)]
    credential: AzureCredential,
}

#[derive(Debug, Clone)]
enum AzureEndpoint {
    /// Path-style emulator front door: `{endpoint}/{account}/{container}/{blob}`.
    PathStyleBase(String),
    /// Account-scoped Blob endpoint: `{endpoint}/{container}/{blob}`.
    AccountBase {
        endpoint: String,
        emulator_compat: bool,
    },
}

impl AzureEndpoint {
    fn path_style(endpoint: String) -> Self {
        Self::PathStyleBase(endpoint)
    }

    fn account_base(endpoint: String, account_name: &str) -> Self {
        let emulator_compat = account_endpoint_looks_emulated(&endpoint, account_name);
        Self::AccountBase {
            endpoint,
            emulator_compat,
        }
    }

    fn emulator_compat(&self) -> bool {
        match self {
            Self::PathStyleBase(_) => true,
            Self::AccountBase {
                emulator_compat, ..
            } => *emulator_compat,
        }
    }
}

impl AzureProvider {
    /// Create provider with Shared Key authentication.
    #[cfg(test)]
    pub fn with_shared_key(
        account_name: String,
        container: String,
        account_key: &str,
    ) -> MidgeResult<Self> {
        Self::with_shared_key_and_endpoint(account_name, container, account_key, None)
    }

    /// Create provider with Shared Key authentication and optional endpoint override.
    ///
    /// When `endpoint` is set, requests use path-style emulator URLs:
    /// `{endpoint}/{account}/{container}/{blob}`.
    pub fn with_shared_key_and_endpoint(
        account_name: String,
        container: String,
        account_key: &str,
        endpoint: Option<String>,
    ) -> MidgeResult<Self> {
        Self::with_shared_key_and_azure_endpoint(
            account_name,
            container,
            account_key,
            endpoint.map(AzureEndpoint::path_style),
        )
    }

    fn with_shared_key_and_azure_endpoint(
        account_name: String,
        container: String,
        account_key: &str,
        endpoint: Option<AzureEndpoint>,
    ) -> MidgeResult<Self> {
        #[cfg(test)]
        let credential = AzureCredential::SharedKey {
            account_key: account_key.to_string(),
        };
        #[cfg(test)]
        let backend_account_name = account_name.clone();
        #[cfg(not(test))]
        let backend_account_name = account_name;
        #[cfg(test)]
        let backend_container = container.clone();
        #[cfg(not(test))]
        let backend_container = container;
        let signer = SharedKeySigner::new_with_emulator_compat(
            backend_account_name.clone(),
            account_key,
            endpoint
                .as_ref()
                .is_some_and(AzureEndpoint::emulator_compat),
        );
        let executor = CloudExecutor::new(Some(Arc::new(signer)))?;
        let backend = Arc::new(AzureBackend::new(
            backend_account_name,
            backend_container,
            endpoint,
            None, // no SAS — signer handles auth
            executor,
        ));
        Ok(Self {
            backend,
            #[cfg(test)]
            account_name,
            #[cfg(test)]
            container,
            #[cfg(test)]
            credential,
        })
    }

    /// Create provider with SAS token authentication.
    #[cfg(test)]
    pub fn with_sas_token(
        account_name: String,
        container: String,
        sas_token: &str,
    ) -> MidgeResult<Self> {
        Self::with_sas_token_and_endpoint(account_name, container, sas_token, None)
    }

    /// Create provider with SAS token authentication and optional endpoint override.
    pub fn with_sas_token_and_endpoint(
        account_name: String,
        container: String,
        sas_token: &str,
        endpoint: Option<String>,
    ) -> MidgeResult<Self> {
        Self::with_sas_token_and_azure_endpoint(
            account_name,
            container,
            sas_token,
            endpoint.map(AzureEndpoint::path_style),
        )
    }

    fn with_sas_token_and_azure_endpoint(
        account_name: String,
        container: String,
        sas_token: &str,
        endpoint: Option<AzureEndpoint>,
    ) -> MidgeResult<Self> {
        // Normalise: strip leading '?' if present.
        let token = sas_token.strip_prefix('?').unwrap_or(sas_token).to_string();
        #[cfg(test)]
        let credential = AzureCredential::SasToken {
            token: token.clone(),
        };
        #[cfg(test)]
        let backend_account_name = account_name.clone();
        #[cfg(not(test))]
        let backend_account_name = account_name;
        #[cfg(test)]
        let backend_container = container.clone();
        #[cfg(not(test))]
        let backend_container = container;
        let executor = CloudExecutor::new(None)?; // SAS goes on the URL, no signer
        let backend = Arc::new(AzureBackend::new(
            backend_account_name,
            backend_container,
            endpoint,
            Some(token),
            executor,
        ));
        Ok(Self {
            backend,
            #[cfg(test)]
            account_name,
            #[cfg(test)]
            container,
            #[cfg(test)]
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
    pub fn with_managed_identity(
        account_name: String,
        container: String,
        client_id: Option<String>,
    ) -> MidgeResult<Self> {
        // Determine client_id: explicit arg > env var > None (system-assigned)
        let effective_client_id = client_id
            .or_else(|| std::env::var("AZURE_CLIENT_ID").ok())
            .filter(|id| !id.is_empty());

        #[cfg(test)]
        let credential = AzureCredential::ManagedIdentity {
            client_id: effective_client_id.clone(),
        };

        let signer: Arc<dyn CloudSigner> =
            Arc::new(ManagedIdentitySigner::new(effective_client_id));

        #[cfg(test)]
        let backend_account_name = account_name.clone();
        #[cfg(not(test))]
        let backend_account_name = account_name;
        #[cfg(test)]
        let backend_container = container.clone();
        #[cfg(not(test))]
        let backend_container = container;
        let executor = CloudExecutor::new(Some(signer))?;
        let backend = Arc::new(AzureBackend::new(
            backend_account_name,
            backend_container,
            None,
            None, // No SAS token for managed identity
            executor,
        ));

        Ok(Self {
            backend,
            #[cfg(test)]
            account_name,
            #[cfg(test)]
            container,
            #[cfg(test)]
            credential,
        })
    }

    /// Legacy constructor — defaults to shared key with an empty key.
    /// Callers should prefer `with_shared_key` or `with_sas_token`.
    #[cfg(test)]
    pub fn new(account_name: String, container: String) -> MidgeResult<Self> {
        Self::with_shared_key(account_name, container, "")
    }

    /// Create provider with automatic credential discovery from environment.
    ///
    /// Credentials are discovered in the following order:
    /// 1. **`SharedKey`**: `AZURE_STORAGE_KEY` or connection string
    /// 2. **SAS Token**: `AZURE_STORAGE_SAS_TOKEN`
    /// 3. **Managed Identity**: `AZURE_CLIENT_ID` (or system-assigned if none)
    ///
    /// # Environment Variables
    /// - `AZURE_STORAGE_CONNECTION_STRING`: Full connection string (highest priority)
    /// - `AZURE_STORAGE_ACCOUNT`: Storage account name (if not in args)
    /// - `AZURE_STORAGE_KEY`: Account key for `SharedKey` auth
    /// - `AZURE_STORAGE_SAS_TOKEN`: SAS token
    /// - `AZURE_CLIENT_ID`: User-assigned managed identity client ID
    #[cfg(test)]
    pub fn from_env(account_name: String, container: String) -> MidgeResult<Self> {
        Self::from_env_and_endpoint(account_name, container, None)
    }

    pub fn from_env_and_endpoint(
        account_name: String,
        container: String,
        endpoint: Option<String>,
    ) -> MidgeResult<Self> {
        // Try connection string first
        if let Ok(conn_str) = std::env::var("AZURE_STORAGE_CONNECTION_STRING") {
            return Self::from_connection_string_and_endpoint(&conn_str, container, endpoint);
        }

        // Try explicit storage key
        if let Ok(key) = std::env::var("AZURE_STORAGE_KEY") {
            return Self::with_shared_key_and_endpoint(account_name, container, &key, endpoint);
        }

        // Try SAS token
        if let Ok(sas) = std::env::var("AZURE_STORAGE_SAS_TOKEN") {
            return Self::with_sas_token_and_endpoint(account_name, container, &sas, endpoint);
        }

        if endpoint.is_some() {
            return Err(MidgeError::InvalidArgument(
                "Azure storage environment credentials for emulator endpoints require connection string, key, or SAS token"
                    .to_string(),
            ));
        }

        // Default to managed identity (checks AZURE_CLIENT_ID internally)
        Self::with_managed_identity(account_name, container, None)
    }

    /// Create provider from Azure Storage connection string.
    ///
    /// Parses connection strings in the format:
    /// `DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=...`
    pub fn from_connection_string_and_endpoint(
        conn_str: &str,
        container: String,
        endpoint: Option<String>,
    ) -> MidgeResult<Self> {
        let parts = AzureConnectionString::parse(conn_str);

        let account = parts
            .account_name
            .as_deref()
            .ok_or_else(|| {
                MidgeError::InvalidArgument("Missing AccountName in connection string".into())
            })?
            .to_string();
        let resolved_endpoint = endpoint
            .map(AzureEndpoint::path_style)
            .or_else(|| parts.blob_endpoint(account.as_str()));

        if let Some(key) = parts.account_key {
            return Self::with_shared_key_and_azure_endpoint(
                account,
                container,
                &key,
                resolved_endpoint,
            );
        }

        if let Some(sas) = parts.sas_token {
            return Self::with_sas_token_and_azure_endpoint(
                account,
                container,
                &sas,
                resolved_endpoint,
            );
        }

        Err(MidgeError::InvalidArgument(
            "Azure connection string must include AccountKey or SharedAccessSignature".into(),
        ))
    }

    pub fn from_lightweight_credential_source(
        account_name: String,
        container: String,
        source: super::AzureCredentialSource,
    ) -> MidgeResult<Self> {
        match source {
            super::AzureCredentialSource::EnvironmentClientSecret => Self::with_oauth_provider(
                account_name,
                container,
                AzureOAuthProvider::from_environment_client_secret()?,
                "environment-client-secret",
            ),
            super::AzureCredentialSource::WorkloadIdentity {
                tenant_id,
                client_id,
                token_file,
            } => Self::with_oauth_provider(
                account_name,
                container,
                AzureOAuthProvider::from_workload_identity(tenant_id, client_id, token_file)?,
                "workload-identity",
            ),
            super::AzureCredentialSource::LightweightDefaultChain => {
                if AzureOAuthProvider::has_environment_client_secret() {
                    return Self::with_oauth_provider(
                        account_name,
                        container,
                        AzureOAuthProvider::from_environment_client_secret()?,
                        "environment-client-secret",
                    );
                }
                if AzureOAuthProvider::has_workload_identity() {
                    return Self::with_oauth_provider(
                        account_name,
                        container,
                        AzureOAuthProvider::from_workload_identity(None, None, None)?,
                        "workload-identity",
                    );
                }
                Self::with_managed_identity(account_name, container, None)
            }
            super::AzureCredentialSource::ManagedIdentity { client_id } => {
                Self::with_managed_identity(account_name, container, client_id)
            }
            other => Err(MidgeError::InvalidArgument(format!(
                "unsupported Azure OAuth credential source in this path: {other:?}"
            ))),
        }
    }

    fn with_oauth_provider(
        account_name: String,
        container: String,
        provider: AzureOAuthProvider,
        source_name: &str,
    ) -> MidgeResult<Self> {
        #[cfg(not(test))]
        tracing::trace!(source = source_name, "Azure OAuth provider configured");
        let signer: Arc<dyn CloudSigner> = Arc::new(OAuthTokenSigner::new(provider));
        #[cfg(test)]
        let backend_account_name = account_name.clone();
        #[cfg(not(test))]
        let backend_account_name = account_name;
        #[cfg(test)]
        let backend_container = container.clone();
        #[cfg(not(test))]
        let backend_container = container;
        let executor = CloudExecutor::new(Some(signer))?;
        let backend = Arc::new(AzureBackend::new(
            backend_account_name,
            backend_container,
            None,
            None,
            executor,
        ));
        Ok(Self {
            backend,
            #[cfg(test)]
            account_name,
            #[cfg(test)]
            container,
            #[cfg(test)]
            credential: AzureCredential::OAuth {
                source: source_name.to_string(),
            },
        })
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

#[derive(Debug, Default)]
struct AzureConnectionString {
    account_name: Option<String>,
    account_key: Option<String>,
    sas_token: Option<String>,
    blob_endpoint: Option<String>,
    default_endpoints_protocol: Option<String>,
    endpoint_suffix: Option<String>,
    use_development_storage: bool,
}

impl AzureConnectionString {
    fn parse(connection_string: &str) -> Self {
        let mut parsed = Self::default();
        for part in connection_string.split(';') {
            let mut kv = part.splitn(2, '=');
            let key = kv.next().unwrap_or_default().trim().to_ascii_lowercase();
            let value = kv.next().unwrap_or_default().trim();
            if value.is_empty() {
                continue;
            }
            match key.as_str() {
                "accountname" => parsed.account_name = Some(value.to_string()),
                "accountkey" => parsed.account_key = Some(value.to_string()),
                "sharedaccesssignature" => parsed.sas_token = Some(value.to_string()),
                "blobendpoint" => {
                    parsed.blob_endpoint = Some(value.trim_end_matches('/').to_string());
                }
                "defaultendpointsprotocol" => {
                    parsed.default_endpoints_protocol = Some(value.to_string());
                }
                "endpointsuffix" => parsed.endpoint_suffix = Some(value.to_string()),
                "usedevelopmentstorage" if value.eq_ignore_ascii_case("true") => {
                    parsed.use_development_storage = true;
                }
                _ => {}
            }
        }

        if parsed.use_development_storage {
            parsed
                .account_name
                .get_or_insert_with(|| "devstoreaccount1".to_string());
            parsed
                .account_key
                .get_or_insert_with(default_azurite_account_key);
            parsed
                .blob_endpoint
                .get_or_insert_with(|| "http://127.0.0.1:10000/devstoreaccount1".to_string());
        }

        parsed
    }

    fn blob_endpoint(&self, account_name: &str) -> Option<AzureEndpoint> {
        if let Some(endpoint) = self.blob_endpoint.as_deref() {
            return Some(AzureEndpoint::account_base(
                endpoint.trim_end_matches('/').to_string(),
                account_name,
            ));
        }

        let suffix = self.endpoint_suffix.as_deref()?;
        let protocol = self
            .default_endpoints_protocol
            .as_deref()
            .unwrap_or("https");
        Some(AzureEndpoint::account_base(
            format!(
                "{}://{}.blob.{}",
                protocol,
                account_name,
                suffix.trim_start_matches('.')
            ),
            account_name,
        ))
    }
}

fn default_azurite_account_key() -> String {
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
        .to_string()
}

fn account_endpoint_looks_emulated(endpoint: &str, account_name: &str) -> bool {
    let Ok(url) = url::Url::parse(endpoint) else {
        return false;
    };
    let host_is_local = matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "::1"));
    let first_path_segment = url
        .path_segments()
        .and_then(|mut segments| segments.next())
        .unwrap_or_default();
    host_is_local || first_path_segment.eq_ignore_ascii_case(account_name)
}

impl Drop for AzureProvider {
    fn drop(&mut self) {
        tracing::trace!("AzureProvider dropping, cleanup will propagate to CloudExecutor");
    }
}

// ---------------------------------------------------------------------------
// Backend (private — implements CloudBackend)
// ---------------------------------------------------------------------------

struct AzureBackend {
    account_name: String,
    container: String,
    endpoint: Option<AzureEndpoint>,
    /// If present, appended as `?{sas_token}` to every URL.
    sas_token: Option<String>,
    executor: CloudExecutor,
}

impl AzureBackend {
    fn new(
        account_name: String,
        container: String,
        endpoint: Option<AzureEndpoint>,
        sas_token: Option<String>,
        executor: CloudExecutor,
    ) -> Self {
        Self {
            account_name,
            container,
            endpoint,
            sas_token,
            executor,
        }
    }

    /// Base URL: `https://{account}.blob.core.windows.net/{container}`
    fn base_url(&self) -> String {
        match &self.endpoint {
            Some(AzureEndpoint::PathStyleBase(endpoint)) => format!(
                "{}/{}/{}",
                endpoint.trim_end_matches('/'),
                self.account_name,
                self.container
            ),
            Some(AzureEndpoint::AccountBase { endpoint, .. }) => {
                format!("{}/{}", endpoint.trim_end_matches('/'), self.container)
            }
            None => format!(
                "https://{}.blob.core.windows.net/{}",
                self.account_name, self.container
            ),
        }
    }

    fn canonical_key(key: &str) -> String {
        key.split('/')
            .map(|seg| utf8_percent_encode(seg, ENCODE_SET).to_string())
            .collect::<Vec<_>>()
            .join("/")
    }

    /// Object URL, with optional SAS token.
    fn object_url(&self, key: &str) -> String {
        let base = format!("{}/{}", self.base_url(), Self::canonical_key(key));
        match &self.sas_token {
            Some(tok) => format!("{base}?{tok}"),
            None => base,
        }
    }

    /// List URL — uses Azure's `restype=container&comp=list&prefix=...`.
    #[cfg(test)]
    fn list_url(&self, prefix: &str, marker: Option<&str>) -> String {
        let mut base = format!(
            "{}?restype=container&comp=list&prefix={}",
            self.base_url(),
            urlencoding::encode(prefix)
        );
        if let Some(marker) = marker {
            base.push_str("&marker=");
            base.push_str(&urlencoding::encode(marker));
        }
        match &self.sas_token {
            Some(tok) => format!("{base}&{tok}"),
            None => base,
        }
    }
}

struct AzureListState {
    prefix: String,
    base_url: String,
    sas_token: Option<String>,
    marker: Option<String>,
    items: Vec<String>,
    budget: CloudListBudget,
}

impl AzureListState {
    fn url(&self) -> String {
        let mut url = format!(
            "{}?restype=container&comp=list&prefix={}",
            self.base_url,
            urlencoding::encode(&self.prefix)
        );
        if let Some(marker) = self.marker.as_deref() {
            url.push_str("&marker=");
            url.push_str(&urlencoding::encode(marker));
        }
        if let Some(token) = self.sas_token.as_deref() {
            url.push('&');
            url.push_str(token);
        }
        url
    }
}

impl CloudBackend for AzureBackend {
    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        let key = key.to_string();
        let url = self.object_url(&key);
        let len = data.len();
        let mut request = CloudRequest::new(Method::PUT, url)
            .with_body(data)
            .with_header("x-ms-blob-type", "BlockBlob")
            .with_header("Content-Length", len.to_string());
        // Merge provided headers into the request (caller-controlled; e.g. If-None-Match)
        for (name, value) in headers {
            if name.eq_ignore_ascii_case(crate::storage::cloud::REQUEST_TIMEOUT_HEADER) {
                match value.parse::<u64>() {
                    Ok(milliseconds) => {
                        request =
                            request.with_timeout(std::time::Duration::from_millis(milliseconds));
                    }
                    Err(error) => {
                        let _ = callback.send(CloudEvent::Put {
                            key,
                            result: CloudOutcome::Err(CloudError::Protocol(format!(
                                "invalid internal request timeout: {error}"
                            ))),
                        });
                        return;
                    }
                }
            } else {
                request = request.with_header(name, value);
            }
        }
        let mapper = move |ctx: String, result: MidgeResult<CloudResponse>| match result {
            Ok(resp) if resp.status < 400 => CloudEvent::Put {
                key: ctx,
                result: CloudOutcome::Ok(()),
            },
            Ok(resp) => CloudEvent::Put {
                key: ctx,
                result: CloudOutcome::Err(CloudError::from_http_status(resp.status, "Azure PUT")),
            },
            Err(err) => CloudEvent::Put {
                key: ctx,
                result: CloudOutcome::Err(CloudError::Transport(format!("{err:?}"))),
            },
        };
        self.executor.spawn_request(request, key, callback, mapper);
    }

    fn submit_get(&self, key: &str, callback: CloudCallback) {
        let key = key.to_string();
        let url = self.object_url(&key);
        let request = CloudRequest::new(Method::GET, url);
        let mapper = move |ctx: String, result: MidgeResult<CloudResponse>| match result {
            Ok(resp) if resp.status == 200 => CloudEvent::Get {
                key: ctx,
                result: CloudOutcome::Ok(resp.body),
            },
            Ok(resp) => CloudEvent::Get {
                key: ctx,
                result: CloudOutcome::Err(CloudError::from_http_status(resp.status, "Azure GET")),
            },
            Err(err) => CloudEvent::Get {
                key: ctx,
                result: CloudOutcome::Err(CloudError::Transport(format!("{err:?}"))),
            },
        };
        self.executor.spawn_request(request, key, callback, mapper);
    }

    fn submit_get_with_metadata(&self, key: &str, callback: CloudCallback) {
        let key = key.to_string();
        let request = CloudRequest::new(Method::GET, self.object_url(&key));
        let mapper = move |ctx: String, result: MidgeResult<CloudResponse>| match result {
            Ok(resp) if resp.status == 200 => {
                let etag = resp
                    .headers
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case("etag"))
                    .map(|(_, value)| value.trim().to_string())
                    .unwrap_or_default();
                let metadata = ObjectMetadata::new(resp.body.len() as u64, etag, 0);
                CloudEvent::GetWithMetadata {
                    key: ctx,
                    result: CloudOutcome::Ok((resp.body, metadata)),
                }
            }
            Ok(resp) => CloudEvent::GetWithMetadata {
                key: ctx,
                result: CloudOutcome::Err(CloudError::from_http_status(resp.status, "Azure GET")),
            },
            Err(err) => CloudEvent::GetWithMetadata {
                key: ctx,
                result: CloudOutcome::Err(CloudError::Transport(format!("{err:?}"))),
            },
        };
        self.executor.spawn_request(request, key, callback, mapper);
    }

    #[cfg(test)]
    fn submit_get_range(&self, key: &str, start: u64, end: Option<u64>, callback: CloudCallback) {
        let key = key.to_string();
        let url = self.object_url(&key);
        let range = match end {
            Some(e) => format!("bytes={}-{}", start, e.saturating_sub(1)),
            None => format!("bytes={start}-"),
        };
        let request = CloudRequest::new(Method::GET, url).with_header("x-ms-range", range);
        let mapper = move |ctx: String, result: MidgeResult<CloudResponse>| match result {
            Ok(resp) if resp.status == 206 || resp.status == 200 => CloudEvent::GetRange {
                key: ctx,
                start,
                end,
                result: CloudOutcome::Ok(resp.body),
            },
            Ok(resp) => CloudEvent::GetRange {
                key: ctx,
                start,
                end,
                result: CloudOutcome::Err(CloudError::from_http_status(
                    resp.status,
                    "Azure GET_RANGE",
                )),
            },
            Err(err) => CloudEvent::GetRange {
                key: ctx,
                start,
                end,
                result: CloudOutcome::Err(CloudError::Transport(format!("{err:?}"))),
            },
        };
        self.executor.spawn_request(request, key, callback, mapper);
    }

    fn submit_delete(&self, key: &str, headers: Vec<(String, String)>, callback: CloudCallback) {
        let key = key.to_string();
        let url = self.object_url(&key);
        let mut request = CloudRequest::new(Method::DELETE, url);
        for (name, value) in headers {
            request = request.with_header(name, value);
        }
        let mapper = move |ctx: String, result: MidgeResult<CloudResponse>| match result {
            Ok(resp) if resp.status < 400 || resp.status == 404 => CloudEvent::Delete {
                key: ctx,
                result: CloudOutcome::Ok(()),
            },
            Ok(resp) => CloudEvent::Delete {
                key: ctx,
                result: CloudOutcome::Err(CloudError::from_http_status(
                    resp.status,
                    "Azure DELETE",
                )),
            },
            Err(err) => CloudEvent::Delete {
                key: ctx,
                result: CloudOutcome::Err(CloudError::Transport(format!("{err:?}"))),
            },
        };
        self.executor.spawn_request(request, key, callback, mapper);
    }

    fn submit_list(&self, prefix: &str, callback: CloudCallback) {
        let prefix = prefix.to_string();
        let state = AzureListState {
            prefix: prefix.clone(),
            base_url: self.base_url(),
            sas_token: self.sas_token.clone(),
            marker: None,
            items: Vec::new(),
            budget: CloudListBudget::default(),
        };
        self.executor.spawn_request_loop(
            state,
            prefix,
            callback,
            |state| Ok(CloudRequest::new(Method::GET, state.url())),
            |state, resp| {
                if resp.status != 200 {
                    return Err(MidgeError::Internal(format!(
                        "Azure LIST status {}",
                        resp.status
                    )));
                }
                let body = String::from_utf8_lossy(&resp.body);
                super::validate_list_xml(&body, "EnumerationResults")?;
                let page_items = extract_xml_tag_values(&body, "Name");
                let marker = extract_xml_tag_values(&body, "NextMarker")
                    .into_iter()
                    .next()
                    .filter(|marker| !marker.is_empty());
                state.budget.record_page(&page_items, marker.as_deref())?;
                state.items.extend(page_items);
                state.marker = marker;
                Ok(state.marker.is_some())
            },
            |ctx, result| match result {
                Ok(state) => CloudEvent::List {
                    prefix: ctx,
                    result: CloudOutcome::Ok(state.items),
                },
                Err(err) => CloudEvent::List {
                    prefix: ctx,
                    result: CloudOutcome::Err(CloudError::Protocol(format!("{err:?}"))),
                },
            },
        );
    }

    fn submit_head(&self, key: &str, callback: CloudCallback) {
        let key = key.to_string();
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
                    .map(|(_, v)| v.trim().to_string())
                    .unwrap_or_default();
                let metadata = ObjectMetadata::new(size, etag, 0);
                CloudEvent::Head {
                    key: ctx,
                    result: CloudOutcome::Ok(metadata),
                }
            }
            Ok(resp) => CloudEvent::Head {
                key: ctx,
                result: CloudOutcome::Err(CloudError::from_http_status(resp.status, "Azure HEAD")),
            },
            Err(err) => CloudEvent::Head {
                key: ctx,
                result: CloudOutcome::Err(CloudError::Transport(format!("{err:?}"))),
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
    emulator_compat: bool,
}

impl SharedKeySigner {
    #[cfg(test)]
    fn new(account_name: String, account_key_base64: &str) -> Self {
        Self::new_with_emulator_compat(account_name, account_key_base64, false)
    }

    fn new_with_emulator_compat(
        account_name: String,
        account_key_base64: &str,
        emulator_compat: bool,
    ) -> Self {
        let decoded_key = BASE64
            .decode(account_key_base64)
            .unwrap_or_else(|_| account_key_base64.as_bytes().to_vec());
        Self {
            account_name,
            decoded_key,
            emulator_compat,
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
        let canonical_headers = x_ms.iter().fold(String::new(), |mut output, (k, v)| {
            let _ = writeln!(output, "{k}:{v}");
            output
        });

        // Canonicalized resource: /{account}/{path}?{sorted-query-params}
        let path = url.path();
        let mut canonical_resource = format!("/{}{}", self.account_name, path);
        let mut query_pairs: Vec<(String, String)> = url
            .query_pairs()
            .map(|(k, v)| (k.to_lowercase(), v.to_string()))
            .collect();
        query_pairs.sort();
        for (k, v) in &query_pairs {
            let _ = write!(canonical_resource, "\n{k}:{v}");
        }

        if self.emulator_compat {
            [
                method.to_string(),
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
            ]
            .join("\n")
        } else {
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
}

impl CloudSigner for SharedKeySigner {
    fn sign(&self, request: &mut CloudRequest) -> MidgeResult<()> {
        let url = url::Url::parse(&request.url)
            .map_err(|e| MidgeError::InvalidArgument(format!("url parse: {e}")))?;

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

        let content_length = request.body.as_ref().map(std::vec::Vec::len);

        let sts = self.string_to_sign(
            request.method.as_str(),
            &request.headers,
            &url,
            content_length,
        );

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

#[derive(Debug, Clone)]
enum AzureOAuthProvider {
    ClientSecret {
        tenant_id: String,
        client_id: String,
        client_secret: String,
    },
    WorkloadIdentity {
        tenant_id: String,
        client_id: String,
        token_file: std::path::PathBuf,
    },
}

impl AzureOAuthProvider {
    fn has_environment_client_secret() -> bool {
        std::env::var_os("AZURE_TENANT_ID").is_some()
            && std::env::var_os("AZURE_CLIENT_ID").is_some()
            && std::env::var_os("AZURE_CLIENT_SECRET").is_some()
    }

    fn has_workload_identity() -> bool {
        std::env::var_os("AZURE_TENANT_ID").is_some()
            && std::env::var_os("AZURE_CLIENT_ID").is_some()
            && std::env::var_os("AZURE_FEDERATED_TOKEN_FILE").is_some()
    }

    fn from_environment_client_secret() -> MidgeResult<Self> {
        Ok(Self::ClientSecret {
            tenant_id: required_env("AZURE_TENANT_ID")?,
            client_id: required_env("AZURE_CLIENT_ID")?,
            client_secret: required_env("AZURE_CLIENT_SECRET")?,
        })
    }

    fn from_workload_identity(
        tenant_id: Option<String>,
        client_id: Option<String>,
        token_file: Option<std::path::PathBuf>,
    ) -> MidgeResult<Self> {
        Ok(Self::WorkloadIdentity {
            tenant_id: tenant_id
                .or_else(|| std::env::var("AZURE_TENANT_ID").ok())
                .ok_or_else(|| MidgeError::InvalidArgument("missing AZURE_TENANT_ID".into()))?,
            client_id: client_id
                .or_else(|| std::env::var("AZURE_CLIENT_ID").ok())
                .ok_or_else(|| MidgeError::InvalidArgument("missing AZURE_CLIENT_ID".into()))?,
            token_file: token_file
                .or_else(|| {
                    std::env::var("AZURE_FEDERATED_TOKEN_FILE")
                        .ok()
                        .map(std::path::PathBuf::from)
                })
                .ok_or_else(|| {
                    MidgeError::InvalidArgument("missing AZURE_FEDERATED_TOKEN_FILE".into())
                })?,
        })
    }

    fn fetch_token(&self) -> MidgeResult<CachedToken> {
        match self {
            Self::ClientSecret {
                tenant_id,
                client_id,
                client_secret,
            } => post_azure_token_form(
                tenant_id,
                &[
                    ("grant_type", "client_credentials".to_string()),
                    ("client_id", client_id.clone()),
                    ("client_secret", client_secret.clone()),
                    ("scope", "https://storage.azure.com/.default".to_string()),
                ],
            ),
            Self::WorkloadIdentity {
                tenant_id,
                client_id,
                token_file,
            } => {
                let assertion = std::fs::read_to_string(token_file).map_err(|error| {
                    MidgeError::Internal(format!(
                        "failed to read Azure federated token file '{}': {}",
                        token_file.display(),
                        error
                    ))
                })?;
                post_azure_token_form(
                    tenant_id,
                    &[
                        ("grant_type", "client_credentials".to_string()),
                        ("client_id", client_id.clone()),
                        (
                            "client_assertion_type",
                            "urn:ietf:params:oauth:client-assertion-type:jwt-bearer".to_string(),
                        ),
                        ("client_assertion", assertion.trim().to_string()),
                        ("scope", "https://storage.azure.com/.default".to_string()),
                    ],
                )
            }
        }
    }
}

struct OAuthTokenSigner {
    provider: AzureOAuthProvider,
    token_cache: Arc<Mutex<Option<CachedToken>>>,
}

impl OAuthTokenSigner {
    fn new(provider: AzureOAuthProvider) -> Self {
        Self {
            provider,
            token_cache: Arc::new(Mutex::new(None)),
        }
    }

    fn get_token(&self) -> MidgeResult<String> {
        {
            let cache = self
                .token_cache
                .lock()
                .map_err(|_| MidgeError::Internal("Azure OAuth token cache poisoned".into()))?;
            if let Some(token) = cache.as_ref() {
                if !token.is_expired() {
                    return Ok(token.access_token.clone());
                }
            }
        }

        let fresh = self.provider.fetch_token()?;
        let token = fresh.access_token.clone();
        let mut cache = self
            .token_cache
            .lock()
            .map_err(|_| MidgeError::Internal("Azure OAuth token cache poisoned".into()))?;
        *cache = Some(fresh);
        Ok(token)
    }
}

impl CloudSigner for OAuthTokenSigner {
    fn sign(&self, request: &mut CloudRequest) -> MidgeResult<()> {
        let token = self.get_token()?;
        request
            .headers
            .push(("Authorization".into(), format!("Bearer {token}")));
        let date = Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        request.headers.push(("x-ms-date".into(), date));
        request
            .headers
            .push(("x-ms-version".into(), "2024-11-04".into()));
        Ok(())
    }
}

fn required_env(name: &str) -> MidgeResult<String> {
    std::env::var(name).map_err(|_| MidgeError::InvalidArgument(format!("missing {name}")))
}

fn post_azure_token_form(tenant_id: &str, values: &[(&str, String)]) -> MidgeResult<CachedToken> {
    let body = values
        .iter()
        .map(|(key, value)| format!("{}={}", key, urlencoding::encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    let url = format!(
        "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
        urlencoding::encode(tenant_id)
    );
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|error| MidgeError::Internal(format!("Azure OAuth client init: {error}")))?;
    let response = client
        .post(url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .map_err(|error| MidgeError::Internal(format!("Azure OAuth token request: {error}")))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().unwrap_or_default();
        return Err(MidgeError::Internal(format!(
            "Azure OAuth token request failed with status {status}: {body}"
        )));
    }
    let body = response
        .text()
        .map_err(|error| MidgeError::Internal(format!("Azure OAuth token body: {error}")))?;
    let json: serde_json::Value = serde_json::from_str(&body)
        .map_err(|error| MidgeError::Internal(format!("Azure OAuth token JSON: {error}")))?;
    let access_token = json
        .get("access_token")
        .and_then(|value| value.as_str())
        .ok_or_else(|| MidgeError::Internal("Azure OAuth response missing access_token".into()))?
        .to_string();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let expires_at = json
        .get("expires_on")
        .and_then(|value| value.as_str())
        .and_then(|value| value.parse::<u64>().ok())
        .or_else(|| {
            json.get("expires_in")
                .and_then(serde_json::Value::as_u64)
                .map(|ttl| now.saturating_add(ttl))
        })
        .unwrap_or_else(|| now.saturating_add(3600));
    Ok(CachedToken {
        access_token,
        expires_at,
    })
}

fn extract_xml_tag_values(body: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut values = Vec::new();
    let mut rest = body;
    while let Some(start) = rest.find(&open) {
        let after_open = &rest[start + open.len()..];
        let Some(end) = after_open.find(&close) else {
            break;
        };
        values.push(decode_xml_entities(&after_open[..end]));
        rest = &after_open[end + close.len()..];
    }
    values
}

fn decode_xml_entities(value: &str) -> String {
    value
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
}

/// Managed Identity signer that fetches OAuth tokens from Azure IMDS.
///
/// Supports:
/// - System-assigned managed identity
/// - User-assigned managed identity (via `client_id`)
/// - Multiple IMDS endpoints (VM, App Service, Container Apps)
/// - Token caching and automatic refresh
struct ManagedIdentitySigner {
    client_id: Option<String>,
    /// Cached token with expiry tracking
    token_cache: Arc<Mutex<Option<CachedToken>>>,
    /// IMDS endpoint (defaults to standard Azure IMDS)
    imds_endpoint: String,
}

impl ManagedIdentitySigner {
    fn new(client_id: Option<String>) -> Self {
        // Determine IMDS endpoint from environment
        let imds_endpoint = std::env::var("IDENTITY_ENDPOINT")
            .or_else(|_| std::env::var("MSI_ENDPOINT"))
            .unwrap_or_else(|_| "http://169.254.169.254/metadata/identity/oauth2/token".into());

        Self {
            client_id,
            token_cache: Arc::new(Mutex::new(None)),
            imds_endpoint,
        }
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
            let _ = write!(url, "&client_id={client_id}");
        }

        // Make synchronous HTTP request to IMDS
        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| MidgeError::Internal(format!("IMDS client init: {e}")))?;

        let response = client
            .get(&url)
            .header("Metadata", "true")
            .send()
            .map_err(|e| {
                MidgeError::Internal(format!(
                    "Failed to fetch managed identity token from IMDS: {e}. \
                     Ensure managed identity is enabled on this Azure resource."
                ))
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(MidgeError::Internal(format!(
                "IMDS token request failed with status {status}: {body}. \
                 Ensure managed identity is properly configured."
            )));
        }

        // Parse JSON response
        let body = response
            .text()
            .map_err(|e| MidgeError::Internal(format!("Failed to read IMDS response: {e}")))?;

        let json: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| MidgeError::Internal(format!("Failed to parse IMDS JSON: {e}")))?;

        let access_token = json["access_token"]
            .as_str()
            .ok_or_else(|| MidgeError::Internal("Missing access_token in IMDS response".into()))?
            .to_string();

        // Parse expires_on (Unix timestamp string)
        let expires_on = json["expires_on"]
            .as_str()
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| {
                MidgeError::Internal("Missing or invalid expires_on in IMDS response".into())
            })?;

        Ok(CachedToken {
            access_token,
            expires_at: expires_on,
        })
    }

    /// Get a valid cached token, refreshing if necessary.
    fn get_token(&self) -> MidgeResult<String> {
        // CRITICAL: Phase 3.1 - Azure token mutex contention fix
        // Original code held the mutex lock while calling fetch_token() (HTTP request),
        // blocking all other threads for up to 10 seconds during token refresh.
        // This pattern is fixed by:
        // 1. Check cache without holding lock across slow operation
        // 2. Release lock before HTTP call to IMDS
        // 3. Update cache atomically after fetch

        // Fast path: check if cached token is still valid (minimal lock hold)
        {
            let cache = match self.token_cache.lock() {
                Ok(guard) => guard,
                Err(_poisoned) => {
                    return Err(MidgeError::Internal(
                        "Token cache mutex poisoned; token fetch failed in another thread. Restart required.".into()
                    ))
                }
            };

            if let Some(ref token) = *cache {
                if !token.is_expired() {
                    return Ok(token.access_token.clone());
                }
            }
        } // Lock released here before fetch_token()

        // Fetch fresh token WITHOUT holding the lock
        // This allows other threads to check the cache while we wait for IMDS
        let fresh_token = self.fetch_token()?;
        let access_token = fresh_token.access_token.clone();

        // Update cache atomically after fetch completes
        {
            let mut cache = match self.token_cache.lock() {
                Ok(guard) => guard,
                Err(_poisoned) => {
                    return Err(MidgeError::Internal(
                        "Token cache mutex poisoned during cache update".into(),
                    ))
                }
            };
            *cache = Some(fresh_token);
        }

        Ok(access_token)
    }
}

impl CloudSigner for ManagedIdentitySigner {
    fn sign(&self, request: &mut CloudRequest) -> MidgeResult<()> {
        // Get valid OAuth token
        let token = self.get_token()?;

        // Add Bearer token authorization header
        request
            .headers
            .push(("Authorization".into(), format!("Bearer {token}")));

        // Add required Azure headers
        let now = Utc::now();
        let date = now.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        request.headers.push(("x-ms-date".into(), date));
        request
            .headers
            .push(("x-ms-version".into(), "2024-11-04".into()));

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::providers::test_support::{
        receive_list_result, spawn_recording_http_server, spawn_recording_http_server_with_status,
        spawn_scripted_http_server,
    };
    use std::sync::{Mutex, OnceLock};

    fn recording_backend(endpoint: String) -> AzureBackend {
        AzureBackend::new(
            "account".into(),
            "container".into(),
            Some(AzureEndpoint::PathStyleBase(endpoint)),
            None,
            CloudExecutor::new(None).expect("cloud executor"),
        )
    }

    fn receive_put_result(receiver: &std::sync::mpsc::Receiver<CloudEvent>) -> CloudOutcome<()> {
        match receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("receive Azure PUT result")
        {
            CloudEvent::Put { result, .. } => result,
            event => panic!("expected Azure PUT event, got {event:?}"),
        }
    }

    fn receive_delete_result(receiver: &std::sync::mpsc::Receiver<CloudEvent>) -> CloudOutcome<()> {
        match receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("receive Azure DELETE result")
        {
            CloudEvent::Delete { result, .. } => result,
            event => panic!("expected Azure DELETE event, got {event:?}"),
        }
    }

    #[test]
    fn should_treat_missing_azure_blob_as_success_when_deleting() {
        // Arrange
        let server = spawn_recording_http_server_with_status(404, Vec::new(), Vec::new());
        let backend = recording_backend(server.endpoint.clone());
        let (sender, receiver) = std::sync::mpsc::channel();

        // Act
        backend.submit_delete("missing/blob", Vec::new(), sender);
        let result = receive_delete_result(&receiver);
        let request = server.finish();

        // Assert
        assert!(matches!(result, CloudOutcome::Ok(())));
        assert_eq!(request.method, "DELETE");
    }

    #[test]
    fn should_echo_provider_quoted_etag_given_azure_conditional_mutations() {
        // Arrange
        let head_server = spawn_recording_http_server(
            vec![
                ("Content-Length".into(), "5".into()),
                ("ETag".into(), "\"azure-etag\"".into()),
            ],
            Vec::new(),
        );
        let head_backend = recording_backend(head_server.endpoint.clone());
        let (head_sender, head_receiver) = std::sync::mpsc::channel();
        head_backend.submit_head("lease/primary", head_sender);
        let metadata = match head_receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("receive Azure HEAD result")
        {
            CloudEvent::Head {
                result: CloudOutcome::Ok(metadata),
                ..
            } => metadata,
            event => panic!("expected successful Azure HEAD event, got {event:?}"),
        };
        let _ = head_server.finish();

        let create_server = spawn_recording_http_server(Vec::new(), Vec::new());
        let create_backend = recording_backend(create_server.endpoint.clone());
        let (create_sender, create_receiver) = std::sync::mpsc::channel();
        let update_server = spawn_recording_http_server(Vec::new(), Vec::new());
        let update_backend = recording_backend(update_server.endpoint.clone());
        let (update_sender, update_receiver) = std::sync::mpsc::channel();
        let delete_server = spawn_recording_http_server(Vec::new(), Vec::new());
        let delete_backend = recording_backend(delete_server.endpoint.clone());
        let (delete_sender, delete_receiver) = std::sync::mpsc::channel();

        // Act
        create_backend.submit_put(
            "lease/primary",
            b"lease".to_vec(),
            vec![("If-None-Match".into(), "*".into())],
            create_sender,
        );
        let create_result = receive_put_result(&create_receiver);
        update_backend.submit_put(
            "lease/primary",
            b"renewed".to_vec(),
            vec![("If-Match".into(), metadata.etag.clone())],
            update_sender,
        );
        let update_result = receive_put_result(&update_receiver);
        delete_backend.submit_delete(
            "lease/primary",
            vec![("If-Match".into(), metadata.etag.clone())],
            delete_sender,
        );
        let delete_result = receive_delete_result(&delete_receiver);
        let create_request = create_server.finish();
        let update_request = update_server.finish();
        let delete_request = delete_server.finish();

        // Assert
        assert_eq!(metadata.etag, "\"azure-etag\"");
        assert!(matches!(create_result, CloudOutcome::Ok(())));
        assert!(matches!(update_result, CloudOutcome::Ok(())));
        assert!(matches!(delete_result, CloudOutcome::Ok(())));
        assert_eq!(create_request.method, "PUT");
        assert!(
            create_request
                .target
                .ends_with("/account/container/lease/primary"),
            "unexpected Azure request target: {}",
            create_request.target
        );
        assert_eq!(create_request.header("if-none-match"), Some("*"));
        assert_eq!(update_request.header("if-match"), Some("\"azure-etag\""));
        assert_eq!(delete_request.header("if-match"), Some("\"azure-etag\""));
    }

    fn azure_client_id_env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn should_reject_repeated_azure_list_marker() {
        // Arrange
        let repeated_page = "<EnumerationResults><Blobs><Blob><Name>sst/a.sst</Name></Blob></Blobs><NextMarker>repeat</NextMarker></EnumerationResults>";
        let final_page =
            "<EnumerationResults><Blobs></Blobs><NextMarker></NextMarker></EnumerationResults>";
        let server = spawn_scripted_http_server(vec![
            repeated_page.to_string(),
            repeated_page.to_string(),
            final_page.to_string(),
        ]);
        let backend = AzureBackend::new(
            "account".into(),
            "container".into(),
            Some(AzureEndpoint::PathStyleBase(server.endpoint.clone())),
            None,
            CloudExecutor::new(None).unwrap(),
        );

        // Act
        let error = receive_list_result(&backend).expect_err("repeated marker should fail LIST");

        // Assert
        assert!(
            error.to_string().contains("repeated continuation token"),
            "{error}"
        );
        assert_eq!(server.finish(), 2);
    }

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

    #[test]
    fn should_create_oauth_credential_with_source() {
        // Arrange
        let source = "workload-identity".to_string();

        // Act
        let cred = AzureCredential::OAuth {
            source: source.clone(),
        };

        // Assert
        match cred {
            AzureCredential::OAuth { source } => assert_eq!(source, "workload-identity"),
            _ => panic!("Expected OAuth credential"),
        }
    }

    #[test]
    fn should_expose_from_env_constructor_for_credential_discovery() {
        // Arrange
        let constructor: fn(String, String) -> MidgeResult<AzureProvider> = AzureProvider::from_env;

        // Act
        let type_name = std::any::type_name_of_val(&constructor);

        // Assert
        assert!(type_name.contains("fn"));
    }

    // =========== AzureProvider Construction Tests ===========

    #[test]
    fn should_create_provider_with_shared_key() {
        // Arrange
        let account = "myaccount";
        let container = "mycontainer";
        let key = "YWNjb3VudGtleTEyMw==";

        // Act
        let provider = AzureProvider::with_shared_key(account.into(), container.into(), key)
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
        let provider = AzureProvider::with_sas_token(account.into(), container.into(), sas_token)
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
        let provider =
            AzureProvider::with_sas_token("account".into(), "container".into(), sas_token)
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
        let provider =
            AzureProvider::with_sas_token("account".into(), "container".into(), sas_token)
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
        let p1 = AzureProvider::with_shared_key(a1.into(), c1.into(), k1);
        let p2 = AzureProvider::with_shared_key(a2.into(), c2.into(), k2);
        let p1 = p1.expect("should create first provider");
        let p2 = p2.expect("should create second provider");

        // Assert
        assert_ne!(p1.account_name(), p2.account_name());
    }

    #[test]
    fn should_create_provider_with_managed_identity_system_assigned() {
        // Arrange
        let _env_guard = azure_client_id_env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let account = "myaccount";
        let container = "mycontainer";

        // Act
        let result = AzureProvider::with_managed_identity(account.into(), container.into(), None);

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
        let _env_guard = azure_client_id_env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
        let _env_guard = azure_client_id_env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::env::set_var("AZURE_CLIENT_ID", "env-client-id");
        let account = "myaccount";
        let container = "mycontainer";

        // Act
        let result = AzureProvider::with_managed_identity(account.into(), container.into(), None);

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
    fn should_preserve_explicit_credential_over_environment_credential_given_both_are_set_when_building(
    ) {
        // Arrange
        let _env_guard = azure_client_id_env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
            None,
            make_noop_executor(),
        );

        // Act
        let url = backend.base_url();

        // Assert
        assert_eq!(url, "https://myaccount.blob.core.windows.net/mycontainer");
    }

    #[test]
    fn should_build_account_endpoint_base_url() {
        // Arrange
        let backend = AzureBackend::new(
            "myaccount".into(),
            "mycontainer".into(),
            Some(AzureEndpoint::account_base(
                "https://myaccount.blob.core.usgovcloudapi.net".to_string(),
                "myaccount",
            )),
            None,
            make_noop_executor(),
        );

        // Act
        // Assert
        assert_eq!(
            backend.base_url(),
            "https://myaccount.blob.core.usgovcloudapi.net/mycontainer"
        );
    }

    #[test]
    fn should_build_path_style_endpoint_base_url() {
        // Arrange
        let backend = AzureBackend::new(
            "admin".into(),
            "container".into(),
            Some(AzureEndpoint::path_style(
                "http://127.0.0.1:9000".to_string(),
            )),
            None,
            make_noop_executor(),
        );

        // Act
        // Assert
        assert_eq!(backend.base_url(), "http://127.0.0.1:9000/admin/container");
    }

    #[test]
    fn should_parse_blob_endpoint_connection_string() {
        // Arrange
        let provider = AzureProvider::from_connection_string_and_endpoint(
            "AccountName=myaccount;AccountKey=dGVzdA==;BlobEndpoint=https://myaccount.blob.core.usgovcloudapi.net",
            "container".to_string(),
            None,
        )
        .expect("connection string");

        let backend = provider.backend();
        // Act
        // Assert
        assert_eq!(provider.account_name(), "myaccount");
        assert_eq!(provider.container(), "container");
        assert!(Arc::strong_count(&backend) >= 1);
    }

    #[test]
    fn should_parse_endpoint_suffix_connection_string() {
        // Arrange
        let parts = AzureConnectionString::parse(
            "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=dGVzdA==;EndpointSuffix=core.usgovcloudapi.net",
        );
        let endpoint = parts
            .blob_endpoint("myaccount")
            .expect("endpoint suffix should produce endpoint");

        match endpoint {
            AzureEndpoint::AccountBase { endpoint, .. } => {
                // Act
                // Assert
                assert_eq!(endpoint, "https://myaccount.blob.core.usgovcloudapi.net");
            }
            AzureEndpoint::PathStyleBase(_) => panic!("expected account endpoint"),
        }
    }

    #[test]
    fn should_parse_development_storage_connection_string() {
        // Arrange
        let parts = AzureConnectionString::parse("UseDevelopmentStorage=true");

        // Act
        // Assert
        assert_eq!(parts.account_name.as_deref(), Some("devstoreaccount1"));
        assert!(parts.account_key.is_some());
        assert!(matches!(
            parts.blob_endpoint("devstoreaccount1"),
            Some(AzureEndpoint::AccountBase {
                emulator_compat: true,
                ..
            })
        ));
    }

    #[test]
    fn should_build_correct_object_url_without_sas() {
        // Arrange
        let backend = AzureBackend::new(
            "acct".into(),
            "ctr".into(),
            None,
            None,
            make_noop_executor(),
        );

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
            None,
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
        let backend = AzureBackend::new(
            "acct".into(),
            "ctr".into(),
            None,
            None,
            make_noop_executor(),
        );

        // Act
        let url = backend.list_url("wal/", None);

        // Assert
        assert!(url.contains("restype=container"));
        assert!(url.contains("comp=list"));
        assert!(url.contains("prefix=wal%2F"));
    }

    #[test]
    fn should_build_list_url_with_marker() {
        // Arrange
        let backend = AzureBackend::new(
            "acct".into(),
            "ctr".into(),
            None,
            None,
            make_noop_executor(),
        );

        let url = backend.list_url("sst/", Some("next marker"));

        // Act
        // Assert
        assert!(url.contains("prefix=sst%2F"));
        assert!(url.contains("marker=next%20marker"));
    }

    #[test]
    fn should_extract_azure_list_names_from_compact_xml() {
        // Arrange
        let body = "<EnumerationResults><Blobs><Blob><Name>a</Name></Blob><Blob><Name>b&amp;c</Name></Blob></Blobs><NextMarker>next</NextMarker></EnumerationResults>";

        let names = extract_xml_tag_values(body, "Name");
        let marker = extract_xml_tag_values(body, "NextMarker");

        // Act
        // Assert
        assert_eq!(names, vec!["a".to_string(), "b&c".to_string()]);
        assert_eq!(marker, vec!["next".to_string()]);
    }

    // =========== SharedKeySigner Tests ===========

    #[test]
    fn should_add_authorization_header_when_signing() {
        // Arrange
        let signer = SharedKeySigner::new(
            "devstoreaccount1".into(),
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
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
        let signer = SharedKeySigner::new("acct".into(), "dGVzdA==");
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
        let signer = SharedKeySigner::new("acct".into(), "dGVzdA==");
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
