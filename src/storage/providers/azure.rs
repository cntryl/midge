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
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'\\')
    .add(b'`')
    .add(b'#')
    .add(b'?')
    .add(b'{')
    .add(b'}');

const AZURE_PUBLIC_AUTHORITY: &str = "https://login.microsoftonline.com";
const AZURE_GOVERNMENT_AUTHORITY: &str = "https://login.microsoftonline.us";
const AZURE_CHINA_AUTHORITY: &str = "https://login.chinacloudapi.cn";

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
    #[cfg(test)]
    oauth_authority: Option<&'static str>,
}

#[derive(Debug, Clone)]
enum AzureEndpoint {
    /// Path-style emulator front door: `{endpoint}/{account}/{container}/{blob}`.
    PathStyleBase(String),
    /// Account-scoped Blob endpoint: `{endpoint}/{container}/{blob}`.
    AccountBase(String),
}

impl AzureEndpoint {
    fn path_style(endpoint: String) -> Self {
        Self::PathStyleBase(endpoint)
    }

    fn account_base(endpoint: String) -> Self {
        Self::AccountBase(endpoint)
    }
}

fn identity_blob_endpoint(endpoint: Option<String>) -> MidgeResult<Option<AzureEndpoint>> {
    endpoint
        .map(|endpoint| {
            validated_https_origin(&endpoint, "Azure identity credential Blob endpoint")
                .map(AzureEndpoint::account_base)
        })
        .transpose()
}

fn azure_oauth_authority_for_blob_endpoint(endpoint: Option<&AzureEndpoint>) -> &'static str {
    // Azure Storage's generic OAuth audience is shared by every Azure cloud,
    // but client-secret and workload-identity tokens must be issued by the
    // Entra authority that owns the configured account endpoint.
    let Some(AzureEndpoint::AccountBase(endpoint)) = endpoint else {
        return AZURE_PUBLIC_AUTHORITY;
    };
    let Ok(parsed) = url::Url::parse(endpoint) else {
        return AZURE_PUBLIC_AUTHORITY;
    };
    let Some(host) = parsed.host_str() else {
        return AZURE_PUBLIC_AUTHORITY;
    };

    if host.ends_with(".blob.core.usgovcloudapi.net") {
        AZURE_GOVERNMENT_AUTHORITY
    } else if host.ends_with(".blob.core.chinacloudapi.cn") {
        AZURE_CHINA_AUTHORITY
    } else {
        AZURE_PUBLIC_AUTHORITY
    }
}

fn validated_https_origin(value: &str, label: &str) -> MidgeResult<String> {
    if value.is_empty() || value != value.trim() {
        return Err(MidgeError::InvalidArgument(format!(
            "{label} must be an absolute HTTPS origin"
        )));
    }
    let parsed = url::Url::parse(value).map_err(|error| {
        MidgeError::InvalidArgument(format!("{label} must be an absolute HTTPS origin: {error}"))
    })?;
    if parsed.scheme() != "https"
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.path() != "/"
    {
        return Err(MidgeError::InvalidArgument(format!(
            "{label} must be an absolute HTTPS origin without credentials, path, query, or fragment"
        )));
    }
    Ok(parsed.as_str().trim_end_matches('/').to_string())
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
        let signer = SharedKeySigner::new(backend_account_name.clone(), account_key);
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
            #[cfg(test)]
            oauth_authority: None,
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
            #[cfg(test)]
            oauth_authority: None,
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
    #[cfg(test)]
    pub fn with_managed_identity(
        account_name: String,
        container: String,
        client_id: Option<String>,
    ) -> MidgeResult<Self> {
        Self::with_managed_identity_and_endpoint(account_name, container, client_id, None)
    }

    pub(super) fn with_managed_identity_and_endpoint(
        account_name: String,
        container: String,
        client_id: Option<String>,
        endpoint: Option<String>,
    ) -> MidgeResult<Self> {
        let endpoint = identity_blob_endpoint(endpoint)?;
        Self::with_managed_identity_and_azure_endpoint(account_name, container, client_id, endpoint)
    }

    fn with_managed_identity_and_azure_endpoint(
        account_name: String,
        container: String,
        client_id: Option<String>,
        endpoint: Option<AzureEndpoint>,
    ) -> MidgeResult<Self> {
        // Determine client_id: explicit arg > env var > None (system-assigned)
        let effective_client_id = client_id
            .or_else(|| std::env::var("AZURE_CLIENT_ID").ok())
            .filter(|id| !id.trim().is_empty());

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
            endpoint,
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
            #[cfg(test)]
            oauth_authority: None,
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

        let account_name = if account_name.trim().is_empty() {
            std::env::var("AZURE_STORAGE_ACCOUNT")
                .ok()
                .filter(|account| !account.trim().is_empty())
                .ok_or_else(|| {
                    MidgeError::InvalidArgument(
                        "missing Azure storage account name; pass it explicitly or set AZURE_STORAGE_ACCOUNT"
                            .to_string(),
                    )
                })?
        } else {
            account_name
        };

        // Try explicit storage key
        if let Ok(key) = std::env::var("AZURE_STORAGE_KEY") {
            return Self::with_shared_key_and_endpoint(account_name, container, &key, endpoint);
        }

        // Try SAS token
        if let Ok(sas) = std::env::var("AZURE_STORAGE_SAS_TOKEN") {
            return Self::with_sas_token_and_endpoint(account_name, container, &sas, endpoint);
        }

        // Default to managed identity (checks AZURE_CLIENT_ID internally)
        Self::with_managed_identity_and_endpoint(account_name, container, None, endpoint)
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

        if let Some(key) = parts.account_key.as_deref() {
            let account = parts.account_name.as_deref().ok_or_else(|| {
                MidgeError::InvalidArgument("Missing AccountName in connection string".into())
            })?;
            let resolved_endpoint = endpoint
                .map(AzureEndpoint::path_style)
                .or_else(|| parts.blob_endpoint(account));
            return Self::with_shared_key_and_azure_endpoint(
                account.to_string(),
                container,
                key,
                resolved_endpoint,
            );
        }

        if let Some(sas) = parts.sas_token.as_deref() {
            let account = parts.account_name.clone().unwrap_or_default();
            let has_account_scoped_endpoint = endpoint.is_none() && parts.blob_endpoint.is_some();
            if account.is_empty() && !has_account_scoped_endpoint {
                return Err(MidgeError::InvalidArgument(
                    "Azure SAS connection string must include AccountName or BlobEndpoint".into(),
                ));
            }
            let resolved_endpoint = endpoint
                .map(AzureEndpoint::path_style)
                .or_else(|| parts.blob_endpoint(&account));
            return Self::with_sas_token_and_azure_endpoint(
                account,
                container,
                sas,
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
        endpoint: Option<String>,
        source: super::AzureCredentialSource,
    ) -> MidgeResult<Self> {
        let endpoint = identity_blob_endpoint(endpoint)?;
        match source {
            super::AzureCredentialSource::EnvironmentClientSecret => Self::with_oauth_provider(
                account_name,
                container,
                endpoint,
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
                endpoint,
                AzureOAuthProvider::from_workload_identity(tenant_id, client_id, token_file)?,
                "workload-identity",
            ),
            super::AzureCredentialSource::LightweightDefaultChain => {
                if AzureOAuthProvider::has_environment_client_secret() {
                    return Self::with_oauth_provider(
                        account_name,
                        container,
                        endpoint,
                        AzureOAuthProvider::from_environment_client_secret()?,
                        "environment-client-secret",
                    );
                }
                if AzureOAuthProvider::has_workload_identity() {
                    return Self::with_oauth_provider(
                        account_name,
                        container,
                        endpoint,
                        AzureOAuthProvider::from_workload_identity(None, None, None)?,
                        "workload-identity",
                    );
                }
                Self::with_managed_identity_and_azure_endpoint(
                    account_name,
                    container,
                    None,
                    endpoint,
                )
            }
            super::AzureCredentialSource::ManagedIdentity { client_id } => {
                Self::with_managed_identity_and_azure_endpoint(
                    account_name,
                    container,
                    client_id,
                    endpoint,
                )
            }
            other => Err(MidgeError::InvalidArgument(format!(
                "unsupported Azure OAuth credential source in this path: {other:?}"
            ))),
        }
    }

    fn with_oauth_provider(
        account_name: String,
        container: String,
        endpoint: Option<AzureEndpoint>,
        provider: AzureOAuthProvider,
        source_name: &str,
    ) -> MidgeResult<Self> {
        #[cfg(not(test))]
        tracing::trace!(source = source_name, "Azure OAuth provider configured");
        let oauth_authority = azure_oauth_authority_for_blob_endpoint(endpoint.as_ref());
        let signer: Arc<dyn CloudSigner> =
            Arc::new(OAuthTokenSigner::new(provider, oauth_authority));
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
            endpoint,
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
            #[cfg(test)]
            oauth_authority: Some(oauth_authority),
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

    #[cfg(test)]
    fn oauth_authority(&self) -> Option<&'static str> {
        self.oauth_authority
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
            ));
        }

        let suffix = self.endpoint_suffix.as_deref()?;
        let protocol = self
            .default_endpoints_protocol
            .as_deref()
            .unwrap_or("https");
        Some(AzureEndpoint::account_base(format!(
            "{}://{}.blob.{}",
            protocol,
            account_name,
            suffix.trim_start_matches('.')
        )))
    }
}

fn default_azurite_account_key() -> String {
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
        .to_string()
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
            Some(AzureEndpoint::AccountBase(endpoint)) => {
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
    error: Option<CloudError>,
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
    fn set_request_timeout(&self, timeout: std::time::Duration) {
        self.executor.set_default_timeout(timeout);
    }

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
        let conditional_mutation = headers.iter().any(|(name, _)| {
            name.eq_ignore_ascii_case("if-match") || name.eq_ignore_ascii_case("if-none-match")
        });
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
            Ok(resp) if resp.status == 201 => CloudEvent::Put {
                key: ctx,
                result: CloudOutcome::Ok(()),
            },
            Ok(resp) => CloudEvent::Put {
                key: ctx,
                result: CloudOutcome::Err(azure_response_error(
                    &resp,
                    "Azure PUT",
                    conditional_mutation,
                )),
            },
            Err(err) => CloudEvent::Put {
                key: ctx,
                result: CloudOutcome::Err(CloudError::from_transport_error(err)),
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
                result: CloudOutcome::Err(azure_response_error(&resp, "Azure GET", false)),
            },
            Err(err) => CloudEvent::Get {
                key: ctx,
                result: CloudOutcome::Err(CloudError::from_transport_error(err)),
            },
        };
        self.executor.spawn_request(request, key, callback, mapper);
    }

    fn submit_get_with_metadata(&self, key: &str, callback: CloudCallback) {
        let key = key.to_string();
        let request = CloudRequest::new(Method::GET, self.object_url(&key));
        let mapper = move |ctx: String, result: MidgeResult<CloudResponse>| match result {
            Ok(resp) if resp.status == 200 => {
                let metadata = object_metadata_from_azure_response(
                    &resp,
                    Some(u64::try_from(resp.body.len()).unwrap_or(u64::MAX)),
                );
                CloudEvent::GetWithMetadata {
                    key: ctx,
                    result: metadata.map(|metadata| (resp.body, metadata)),
                }
            }
            Ok(resp) => CloudEvent::GetWithMetadata {
                key: ctx,
                result: CloudOutcome::Err(azure_response_error(&resp, "Azure GET", false)),
            },
            Err(err) => CloudEvent::GetWithMetadata {
                key: ctx,
                result: CloudOutcome::Err(CloudError::from_transport_error(err)),
            },
        };
        self.executor.spawn_request(request, key, callback, mapper);
    }

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
                result: CloudOutcome::Err(azure_response_error(&resp, "Azure GET_RANGE", false)),
            },
            Err(err) => CloudEvent::GetRange {
                key: ctx,
                start,
                end,
                result: CloudOutcome::Err(CloudError::from_transport_error(err)),
            },
        };
        self.executor.spawn_request(request, key, callback, mapper);
    }

    fn submit_delete(&self, key: &str, headers: Vec<(String, String)>, callback: CloudCallback) {
        let key = key.to_string();
        let url = self.object_url(&key);
        let conditional_mutation = headers.iter().any(|(name, _)| {
            name.eq_ignore_ascii_case("if-match") || name.eq_ignore_ascii_case("if-none-match")
        });
        let mut request = CloudRequest::new(Method::DELETE, url);
        for (name, value) in headers {
            request = request.with_header(name, value);
        }
        let mapper = move |ctx: String, result: MidgeResult<CloudResponse>| match result {
            Ok(resp) if matches!(resp.status, 202 | 404) => CloudEvent::Delete {
                key: ctx,
                result: CloudOutcome::Ok(()),
            },
            Ok(resp) => CloudEvent::Delete {
                key: ctx,
                result: CloudOutcome::Err(azure_response_error(
                    &resp,
                    "Azure DELETE",
                    conditional_mutation,
                )),
            },
            Err(err) => CloudEvent::Delete {
                key: ctx,
                result: CloudOutcome::Err(CloudError::from_transport_error(err)),
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
            error: None,
        };
        self.executor.spawn_request_loop(
            state,
            prefix,
            callback,
            |state| Ok(CloudRequest::new(Method::GET, state.url())),
            |state, resp| {
                if resp.status != 200 {
                    state.error = Some(azure_response_error(&resp, "Azure LIST", false));
                    return Ok(false);
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
                Ok(state) if state.error.is_none() => CloudEvent::List {
                    prefix: ctx,
                    result: CloudOutcome::Ok(state.items),
                },
                Ok(mut state) => CloudEvent::List {
                    prefix: ctx,
                    result: CloudOutcome::Err(state.error.take().expect("checked list error")),
                },
                Err(err) => CloudEvent::List {
                    prefix: ctx,
                    result: CloudOutcome::Err(CloudError::from_protocol_or_timeout_error(err)),
                },
            },
        );
    }

    fn submit_head(&self, key: &str, callback: CloudCallback) {
        let key = key.to_string();
        let url = self.object_url(&key);
        let request = CloudRequest::new(Method::HEAD, url);
        let mapper = move |ctx: String, result: MidgeResult<CloudResponse>| match result {
            Ok(resp) if resp.status == 200 => CloudEvent::Head {
                key: ctx,
                result: object_metadata_from_azure_response(&resp, None),
            },
            Ok(resp) => CloudEvent::Head {
                key: ctx,
                result: CloudOutcome::Err(azure_response_error(&resp, "Azure HEAD", false)),
            },
            Err(err) => CloudEvent::Head {
                key: ctx,
                result: CloudOutcome::Err(CloudError::from_transport_error(err)),
            },
        };
        self.executor.spawn_request(request, key, callback, mapper);
    }
}

fn object_metadata_from_azure_response(
    response: &CloudResponse,
    known_size: Option<u64>,
) -> CloudOutcome<ObjectMetadata> {
    let size = match known_size {
        Some(_) => crate::storage::cloud::executor::validate_get_response_length(response)?,
        None => response
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .ok_or_else(|| {
                CloudError::Protocol(
                    "Azure metadata response is missing Content-Length".to_string(),
                )
            })?
            .1
            .parse::<u64>()
            .map_err(|error| {
                CloudError::Protocol(format!(
                    "Azure metadata response has invalid Content-Length: {error}"
                ))
            })?,
    };
    let etag = response
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("etag"))
        .map(|(_, value)| value.trim())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            CloudError::Protocol("Azure metadata response is missing ETag".to_string())
        })?;
    Ok(ObjectMetadata::new(size, etag.to_string()))
}

fn azure_response_error(
    response: &CloudResponse,
    operation: &str,
    conditional_mutation: bool,
) -> CloudError {
    let body = String::from_utf8_lossy(&response.body);
    let code = response
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("x-ms-error-code"))
        .map(|(_, value)| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| extract_xml_tag_values(&body, "Code").into_iter().next());
    let message = extract_xml_tag_values(&body, "Message").into_iter().next();
    let detail = match (code.as_deref(), message.as_deref()) {
        (Some(code), Some(message)) => format!("{operation}: {code}: {message}"),
        (Some(code), None) => format!("{operation}: {code}"),
        (None, _) => operation.to_string(),
    };
    let predicate_failed = code.as_deref().is_some_and(|code| {
        code.eq_ignore_ascii_case("ConditionNotMet")
            || code.eq_ignore_ascii_case("TargetConditionNotMet")
    });

    if conditional_mutation && response.status == 412 && predicate_failed {
        CloudError::PreconditionFailed(format!("status {}: {detail}", response.status))
    } else {
        CloudError::from_http_status(response.status, detail)
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
    fn new(account_name: String, account_key_base64: &str) -> Self {
        let decoded_key = BASE64
            .decode(account_key_base64)
            .unwrap_or_else(|_| account_key_base64.as_bytes().to_vec());
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
        ["AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET"]
            .into_iter()
            .all(environment_value_is_nonempty)
    }

    fn has_workload_identity() -> bool {
        [
            "AZURE_TENANT_ID",
            "AZURE_CLIENT_ID",
            "AZURE_FEDERATED_TOKEN_FILE",
        ]
        .into_iter()
        .all(environment_value_is_nonempty)
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
                .filter(|value| !value.trim().is_empty())
                .or_else(|| std::env::var("AZURE_TENANT_ID").ok())
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| MidgeError::InvalidArgument("missing AZURE_TENANT_ID".into()))?,
            client_id: client_id
                .filter(|value| !value.trim().is_empty())
                .or_else(|| std::env::var("AZURE_CLIENT_ID").ok())
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| MidgeError::InvalidArgument("missing AZURE_CLIENT_ID".into()))?,
            token_file: token_file
                .filter(|path| !path.as_os_str().is_empty())
                .or_else(|| {
                    std::env::var("AZURE_FEDERATED_TOKEN_FILE")
                        .ok()
                        .filter(|value| !value.trim().is_empty())
                        .map(std::path::PathBuf::from)
                })
                .ok_or_else(|| {
                    MidgeError::InvalidArgument("missing AZURE_FEDERATED_TOKEN_FILE".into())
                })?,
        })
    }

    fn fetch_token(&self, default_authority: &str) -> MidgeResult<CachedToken> {
        match self {
            Self::ClientSecret {
                tenant_id,
                client_id,
                client_secret,
            } => post_azure_token_form(
                tenant_id,
                default_authority,
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
                let assertion = assertion.trim();
                if assertion.is_empty() {
                    return Err(MidgeError::InvalidArgument(format!(
                        "Azure federated token file '{}' is empty",
                        token_file.display()
                    )));
                }
                post_azure_token_form(
                    tenant_id,
                    default_authority,
                    &[
                        ("grant_type", "client_credentials".to_string()),
                        ("client_id", client_id.clone()),
                        (
                            "client_assertion_type",
                            "urn:ietf:params:oauth:client-assertion-type:jwt-bearer".to_string(),
                        ),
                        ("client_assertion", assertion.to_string()),
                        ("scope", "https://storage.azure.com/.default".to_string()),
                    ],
                )
            }
        }
    }
}

struct OAuthTokenSigner {
    provider: AzureOAuthProvider,
    default_authority: &'static str,
    token_cache: Arc<Mutex<Option<CachedToken>>>,
}

impl OAuthTokenSigner {
    fn new(provider: AzureOAuthProvider, default_authority: &'static str) -> Self {
        Self {
            provider,
            default_authority,
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

        let fresh = self.provider.fetch_token(self.default_authority)?;
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
    let value =
        std::env::var(name).map_err(|_| MidgeError::InvalidArgument(format!("missing {name}")))?;
    if value.trim().is_empty() {
        return Err(MidgeError::InvalidArgument(format!(
            "missing or empty {name}"
        )));
    }
    Ok(value)
}

fn environment_value_is_nonempty(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| !value.trim().is_empty())
}

fn azure_oauth_token_url(tenant_id: &str, default_authority: &str) -> MidgeResult<String> {
    let authority = match std::env::var("AZURE_AUTHORITY_HOST") {
        Ok(authority) => validated_https_origin(&authority, "AZURE_AUTHORITY_HOST")?,
        Err(std::env::VarError::NotPresent) => default_authority.to_string(),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(MidgeError::InvalidArgument(
                "AZURE_AUTHORITY_HOST must contain valid Unicode".to_string(),
            ));
        }
    };
    Ok(format!(
        "{authority}/{}/oauth2/v2.0/token",
        urlencoding::encode(tenant_id)
    ))
}

fn post_azure_token_form(
    tenant_id: &str,
    default_authority: &str,
    values: &[(&str, String)],
) -> MidgeResult<CachedToken> {
    let body = values
        .iter()
        .map(|(key, value)| format!("{}={}", key, urlencoding::encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    let url = azure_oauth_token_url(tenant_id, default_authority)?;
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
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    parse_azure_oauth_token_response(&body, now)
}

fn parse_azure_oauth_token_response(body: &str, now: u64) -> MidgeResult<CachedToken> {
    let json: serde_json::Value = serde_json::from_str(body)
        .map_err(|error| MidgeError::Internal(format!("Azure OAuth token JSON: {error}")))?;
    let access_token = json
        .get("access_token")
        .and_then(|value| value.as_str())
        .filter(|token| !token.trim().is_empty())
        .ok_or_else(|| {
            MidgeError::Internal("Azure OAuth response missing or empty access_token".into())
        })?
        .to_string();
    let expires_at = if let Some(expires_on) = json.get("expires_on") {
        json_u64(expires_on).ok_or_else(|| {
            MidgeError::Internal("Azure OAuth response has invalid expires_on".into())
        })?
    } else {
        let expires_in = json.get("expires_in").and_then(json_u64).ok_or_else(|| {
            MidgeError::Internal(
                "Azure OAuth response is missing a valid expires_on or expires_in".into(),
            )
        })?;
        now.checked_add(expires_in).ok_or_else(|| {
            MidgeError::Internal("Azure OAuth response expires_in overflows timestamp".into())
        })?
    };
    if expires_at <= now {
        return Err(MidgeError::Internal(
            "Azure OAuth response expiry is not in the future".into(),
        ));
    }
    Ok(CachedToken {
        access_token,
        expires_at,
    })
}

fn json_u64(value: &serde_json::Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|text| text.parse::<u64>().ok()))
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
    /// Authentication contract used by the selected managed identity endpoint.
    endpoint_kind: ManagedIdentityEndpointKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManagedIdentityEndpointKind {
    /// App Service/Container Apps `IDENTITY_ENDPOINT` contract.
    Identity,
    /// Legacy App Service `MSI_ENDPOINT` contract.
    LegacyMsi,
    /// Azure VM Instance Metadata Service contract.
    VmImds,
}

impl ManagedIdentitySigner {
    fn new(client_id: Option<String>) -> Self {
        // IDENTITY_ENDPOINT uses the App Service/Container Apps contract, which
        // authenticates callers with the rotating IDENTITY_HEADER value. Legacy
        // App Service uses MSI_ENDPOINT with MSI_SECRET, while VM IMDS uses the
        // Metadata header.
        let (imds_endpoint, endpoint_kind) = match std::env::var("IDENTITY_ENDPOINT") {
            Ok(endpoint) if !endpoint.trim().is_empty() => {
                (endpoint, ManagedIdentityEndpointKind::Identity)
            }
            _ => match std::env::var("MSI_ENDPOINT") {
                Ok(endpoint) if !endpoint.trim().is_empty() => {
                    (endpoint, ManagedIdentityEndpointKind::LegacyMsi)
                }
                _ => (
                    "http://169.254.169.254/metadata/identity/oauth2/token".into(),
                    ManagedIdentityEndpointKind::VmImds,
                ),
            },
        };

        Self::with_endpoint(client_id, imds_endpoint, endpoint_kind)
    }

    fn with_endpoint(
        client_id: Option<String>,
        imds_endpoint: String,
        endpoint_kind: ManagedIdentityEndpointKind,
    ) -> Self {
        Self {
            client_id,
            token_cache: Arc::new(Mutex::new(None)),
            imds_endpoint,
            endpoint_kind,
        }
    }

    /// Fetch a fresh OAuth token from Azure IMDS.
    fn fetch_token(&self) -> MidgeResult<CachedToken> {
        // Build IMDS request URL
        let api_version = match self.endpoint_kind {
            ManagedIdentityEndpointKind::LegacyMsi => "2017-09-01",
            ManagedIdentityEndpointKind::Identity | ManagedIdentityEndpointKind::VmImds => {
                "2019-08-01"
            }
        };
        let mut url = format!(
            "{}?api-version={api_version}&resource=https://storage.azure.com/",
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

        let request = match self.endpoint_kind {
            ManagedIdentityEndpointKind::Identity => client
                .get(&url)
                .header("X-IDENTITY-HEADER", required_env("IDENTITY_HEADER")?),
            ManagedIdentityEndpointKind::LegacyMsi => client
                .get(&url)
                .header("Secret", required_env("MSI_SECRET")?),
            ManagedIdentityEndpointKind::VmImds => client.get(&url).header("Metadata", "true"),
        };
        let response = request.send().map_err(|e| {
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
            .filter(|token| !token.trim().is_empty())
            .ok_or_else(|| {
                MidgeError::Internal(
                    "Missing or empty access_token in managed identity response".into(),
                )
            })?
            .to_string();

        // Parse expires_on (Unix timestamp string)
        let expires_on = json.get("expires_on").and_then(json_u64).ok_or_else(|| {
            MidgeError::Internal(
                "Missing or invalid expires_on in managed identity response".into(),
            )
        })?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if expires_on <= now {
            return Err(MidgeError::Internal(
                "Managed identity response expires_on is not in the future".into(),
            ));
        }

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
    #[test]
    fn should_validate_get_length_when_building_object_identity() {
        // Arrange
        let identity_headers = &[("etag", "identity")];
        // Act
        // Assert
        crate::storage::providers::test_support::assert_get_metadata_length_contract(
            identity_headers,
            |response| object_metadata_from_azure_response(response, Some(3)),
        );
    }

    use super::*;
    use crate::storage::providers::test_support::{
        receive_list_result, spawn_recording_http_server, spawn_recording_http_server_with_status,
        spawn_scripted_http_response_server, spawn_scripted_http_server,
    };
    use std::ffi::OsString;
    use std::sync::{Mutex, OnceLock};

    struct TestEnvVar {
        name: &'static str,
        previous: Option<OsString>,
    }

    impl TestEnvVar {
        fn set(name: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(name);
            std::env::set_var(name, value);
            Self { name, previous }
        }

        fn remove(name: &'static str) -> Self {
            let previous = std::env::var_os(name);
            std::env::remove_var(name);
            Self { name, previous }
        }
    }

    impl Drop for TestEnvVar {
        fn drop(&mut self) {
            if let Some(previous) = self.previous.as_ref() {
                std::env::set_var(self.name, previous);
            } else {
                std::env::remove_var(self.name);
            }
        }
    }

    fn recording_backend(endpoint: String) -> AzureBackend {
        AzureBackend::new(
            "account".into(),
            "container".into(),
            Some(AzureEndpoint::PathStyleBase(endpoint)),
            None,
            CloudExecutor::new(None).expect("cloud executor"),
        )
    }

    #[test]
    fn should_reject_redirect_status_when_putting_azure_blob() {
        // Arrange
        let server = spawn_recording_http_server_with_status(302, Vec::new(), Vec::new());
        let backend = recording_backend(server.endpoint.clone());
        let (sender, receiver) = std::sync::mpsc::channel();

        // Act
        backend.submit_put("blob", b"value".to_vec(), Vec::new(), sender);
        let result = receive_put_result(&receiver);
        let _ = server.finish();

        // Assert
        assert!(matches!(result, CloudOutcome::Err(CloudError::Protocol(_))));
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
    fn should_only_classify_azure_target_condition_as_precondition_conflict() {
        // Arrange
        let response = |status, code: &str| CloudResponse {
            status,
            headers: vec![("x-ms-error-code".to_string(), code.to_string())],
            body: Vec::new(),
        };
        let predicate = response(412, "TargetConditionNotMet");
        let missing_lease = response(412, "LeaseIdMissing");
        let retained_snapshot = response(409, "SnapshotsPresent");

        // Act
        let predicate_error = azure_response_error(&predicate, "Azure PUT", true);
        let lease_error = azure_response_error(&missing_lease, "Azure PUT", true);
        let snapshot_error = azure_response_error(&retained_snapshot, "Azure DELETE", true);

        // Assert
        assert!(matches!(predicate_error, CloudError::PreconditionFailed(_)));
        assert!(matches!(lease_error, CloudError::InvalidRequest(_)));
        assert!(matches!(snapshot_error, CloudError::InvalidRequest(_)));
    }

    #[test]
    fn should_preserve_azure_authorization_error_when_listing() {
        // Arrange
        let server = spawn_scripted_http_response_server(vec![(
            403,
            "application/xml".to_string(),
            "<Error><Code>AuthorizationFailure</Code><Message>denied</Message></Error>".to_string(),
        )]);
        let backend = recording_backend(server.endpoint.clone());

        // Act
        let result = receive_list_result(&backend);
        let requests = server.finish();

        // Assert
        assert!(matches!(
            result,
            CloudOutcome::Err(CloudError::Unauthorized(message))
                if message.contains("AuthorizationFailure")
        ));
        assert_eq!(requests, 1);
    }

    #[test]
    fn should_fail_closed_when_azure_head_omits_identity_metadata() {
        // Arrange
        let server = spawn_recording_http_server(
            vec![("Content-Length".to_string(), "7".to_string())],
            Vec::new(),
        );
        let backend = recording_backend(server.endpoint.clone());
        let (sender, receiver) = std::sync::mpsc::channel();

        // Act
        backend.submit_head("lease/primary", sender);
        let result = match receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("receive Azure HEAD result")
        {
            CloudEvent::Head { result, .. } => result,
            event => panic!("expected Azure HEAD event, got {event:?}"),
        };
        let _ = server.finish();

        // Assert
        assert!(matches!(
            result,
            CloudOutcome::Err(CloudError::Protocol(message)) if message.contains("ETag")
        ));
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

        let create_server = spawn_recording_http_server_with_status(201, Vec::new(), Vec::new());
        let create_backend = recording_backend(create_server.endpoint.clone());
        let (create_sender, create_receiver) = std::sync::mpsc::channel();
        let update_server = spawn_recording_http_server_with_status(201, Vec::new(), Vec::new());
        let update_backend = recording_backend(update_server.endpoint.clone());
        let (update_sender, update_receiver) = std::sync::mpsc::channel();
        let delete_server = spawn_recording_http_server_with_status(202, Vec::new(), Vec::new());
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
    fn should_discover_shared_key_credential_via_from_env() {
        // Arrange: `from_env` is a thin convenience wrapper over `from_env_and_endpoint(..,
        // None)`; that endpoint-taking constructor's behavior is exercised precisely elsewhere,
        // so here we only need to confirm the wrapper actually reaches it (rather than, say,
        // silently defaulting the account or credential) with an explicit account argument.
        let _env_guard = azure_client_id_env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _connection_string = TestEnvVar::remove("AZURE_STORAGE_CONNECTION_STRING");
        let _key = TestEnvVar::set("AZURE_STORAGE_KEY", "dGVzdA==");
        let _sas = TestEnvVar::remove("AZURE_STORAGE_SAS_TOKEN");

        // Act
        let provider = AzureProvider::from_env("myaccount".to_string(), "container".to_string())
            .expect("environment-backed Azure provider");

        // Assert
        assert_eq!(provider.account_name(), "myaccount");
        assert_eq!(provider.container(), "container");
        assert!(matches!(
            provider.credential(),
            AzureCredential::SharedKey { .. }
        ));
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

        // Assert: each provider retains its own account, container, and key rather than any
        // one of them leaking into or overwriting the other.
        assert_eq!(p1.account_name(), "a1");
        assert_eq!(p2.account_name(), "a2");
        assert_ne!(p1.account_name(), p2.account_name());
        match (p1.credential(), p2.credential()) {
            (
                AzureCredential::SharedKey { account_key: k1 },
                AzureCredential::SharedKey { account_key: k2 },
            ) => {
                assert_eq!(k1, "YTEta2V5");
                assert_eq!(k2, "YTIta2V5");
                assert_ne!(k1, k2);
            }
            other => panic!("expected SharedKey credentials, got {other:?}"),
        }
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
    fn should_treat_identity_endpoint_as_account_scoped_blob_origin() {
        // Arrange
        let endpoint = "https://myaccount.blob.core.usgovcloudapi.net/";

        // Act
        let endpoint = identity_blob_endpoint(Some(endpoint.to_string()))
            .expect("valid sovereign Blob endpoint")
            .expect("configured endpoint");

        // Assert
        assert!(matches!(
            endpoint,
            AzureEndpoint::AccountBase(value)
                if value == "https://myaccount.blob.core.usgovcloudapi.net"
        ));
    }

    #[test]
    fn should_reject_insecure_or_non_origin_identity_blob_endpoint() {
        // Arrange
        let endpoints = [
            "not a URL",
            "http://myaccount.blob.core.windows.net",
            "https://user:secret@myaccount.blob.core.windows.net",
            "https://myaccount.blob.core.windows.net/base",
            "https://myaccount.blob.core.windows.net?sig=secret",
            "https://myaccount.blob.core.windows.net#fragment",
        ];

        // Act
        let results = endpoints.map(|endpoint| identity_blob_endpoint(Some(endpoint.to_string())));

        // Assert
        for result in results {
            assert!(
                matches!(result, Err(MidgeError::InvalidArgument(ref message)) if message.contains("identity")),
                "identity Blob endpoints must be secure account-scoped origins: {result:?}"
            );
        }
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
            AzureEndpoint::AccountBase(endpoint) => {
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
            Some(AzureEndpoint::AccountBase(_))
        ));
    }

    #[test]
    fn should_accept_accountless_sas_connection_string_when_blob_endpoint_is_explicit() {
        // Arrange
        let server = spawn_recording_http_server(Vec::new(), Vec::new());
        let connection_string = format!(
            "BlobEndpoint={};SharedAccessSignature=sv=2024-11-04&sig=test",
            server.endpoint
        );

        // Act
        let provider = AzureProvider::from_connection_string_and_endpoint(
            &connection_string,
            "container".to_string(),
            None,
        )
        .expect("accountless SAS connection string");
        let (sender, receiver) = std::sync::mpsc::channel();
        provider.backend().submit_get("blob", sender);
        let event = receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("receive Azure GET result");
        let request = server.finish();

        // Assert
        assert!(matches!(
            event,
            CloudEvent::Get {
                result: CloudOutcome::Ok(_),
                ..
            }
        ));
        assert_eq!(request.target, "/container/blob?sv=2024-11-04&sig=test");
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
    fn should_escape_literal_percent_when_building_object_url() {
        // Arrange
        let backend = AzureBackend::new(
            "acct".into(),
            "ctr".into(),
            None,
            None,
            make_noop_executor(),
        );

        // Act
        let object_url = backend.object_url("tenant%2Fblue/wal/segment");
        let list_url = backend.list_url("tenant%2Fblue/wal/", None);

        // Assert
        assert_eq!(
            object_url,
            "https://acct.blob.core.windows.net/ctr/tenant%252Fblue/wal/segment"
        );
        assert!(list_url.contains("prefix=tenant%252Fblue%2Fwal%2F"));
    }

    #[test]
    fn should_escape_backslashes_before_parsing_azure_object_url() {
        // Arrange
        let backend = AzureBackend::new(
            "acct".into(),
            "ctr".into(),
            None,
            None,
            make_noop_executor(),
        );

        // Act
        let object_url = backend.object_url("tenant\\..\\shared/wal/segment");
        let parsed = url::Url::parse(&object_url).expect("Azure object URL");

        // Assert
        assert_eq!(
            parsed.path(),
            "/ctr/tenant%5C..%5Cshared/wal/segment",
            "backslashes must remain object-name bytes rather than path separators"
        );
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
    fn should_join_emulator_canonical_headers_directly_to_resource() {
        // Arrange
        let signer = SharedKeySigner::new(
            "devstoreaccount1".into(),
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
        );
        let headers = vec![
            (
                "x-ms-date".to_string(),
                "Sun, 10 Aug 2026 12:00:00 GMT".to_string(),
            ),
            ("x-ms-version".to_string(), "2024-11-04".to_string()),
        ];
        let url = url::Url::parse("http://127.0.0.1:10000/devstoreaccount1/container/blob")
            .expect("emulator URL");

        // Act
        let string_to_sign = signer.string_to_sign("GET", &headers, &url, None);

        // Assert
        assert!(string_to_sign.contains(
            "x-ms-version:2024-11-04\n/devstoreaccount1/devstoreaccount1/container/blob"
        ));
        assert!(!string_to_sign.contains("x-ms-version:2024-11-04\n\n/"));
    }

    #[test]
    fn should_send_rotating_identity_header_when_using_identity_endpoint() {
        // Arrange
        let _env_guard = azure_client_id_env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let server = spawn_recording_http_server(
            Vec::new(),
            br#"{"access_token":"token","expires_on":"4102444800"}"#.to_vec(),
        );
        let _endpoint = TestEnvVar::set("IDENTITY_ENDPOINT", &server.endpoint);
        let _identity_header = TestEnvVar::set("IDENTITY_HEADER", "rotating-secret");
        let signer = ManagedIdentitySigner::new(None);

        // Act
        let token = signer.fetch_token().expect("managed identity token");
        let request = server.finish();

        // Assert
        assert_eq!(token.access_token, "token");
        assert_eq!(request.header("x-identity-header"), Some("rotating-secret"));
        assert_eq!(request.header("secret"), None);
        assert_eq!(request.header("metadata"), None);
    }

    #[test]
    fn should_send_legacy_secret_header_when_using_msi_endpoint() {
        // Arrange
        let _env_guard = azure_client_id_env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let server = spawn_recording_http_server(
            Vec::new(),
            br#"{"access_token":"token","expires_on":"4102444800"}"#.to_vec(),
        );
        let _identity_endpoint = TestEnvVar::remove("IDENTITY_ENDPOINT");
        let _msi_endpoint = TestEnvVar::set("MSI_ENDPOINT", &server.endpoint);
        let _msi_secret = TestEnvVar::set("MSI_SECRET", "legacy-secret");
        let signer = ManagedIdentitySigner::new(None);

        // Act
        let token = signer.fetch_token().expect("managed identity token");
        let request = server.finish();

        // Assert
        assert_eq!(token.access_token, "token");
        assert_eq!(request.header("secret"), Some("legacy-secret"));
        assert_eq!(request.header("metadata"), None);
        assert_eq!(request.header("x-identity-header"), None);
        assert!(request.target.contains("api-version=2017-09-01"));
    }

    #[test]
    fn should_send_metadata_header_when_using_vm_imds_endpoint() {
        // Arrange
        let server = spawn_recording_http_server(
            Vec::new(),
            br#"{"access_token":"token","expires_on":"4102444800"}"#.to_vec(),
        );
        let signer = ManagedIdentitySigner::with_endpoint(
            None,
            server.endpoint.clone(),
            ManagedIdentityEndpointKind::VmImds,
        );

        // Act
        let token = signer.fetch_token().expect("managed identity token");
        let request = server.finish();

        // Assert
        assert_eq!(token.access_token, "token");
        assert_eq!(request.header("metadata"), Some("true"));
        assert_eq!(request.header("secret"), None);
        assert_eq!(request.header("x-identity-header"), None);
        assert!(request.target.contains("api-version=2019-08-01"));
    }

    #[test]
    fn should_honor_azure_authority_host_when_building_oauth_token_url() {
        // Arrange
        let _env_guard = azure_client_id_env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _authority =
            TestEnvVar::set("AZURE_AUTHORITY_HOST", "https://login.microsoftonline.us/");

        // Act
        let url = azure_oauth_token_url("tenant id", AZURE_PUBLIC_AUTHORITY)
            .expect("sovereign OAuth token URL");

        // Assert
        assert_eq!(
            url,
            "https://login.microsoftonline.us/tenant%20id/oauth2/v2.0/token"
        );
    }

    #[test]
    fn should_default_to_public_azure_authority_when_override_is_absent() {
        // Arrange
        let _env_guard = azure_client_id_env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _authority = TestEnvVar::remove("AZURE_AUTHORITY_HOST");

        // Act
        let url = azure_oauth_token_url("tenant", AZURE_PUBLIC_AUTHORITY)
            .expect("default OAuth token URL");

        // Assert
        assert_eq!(
            url,
            "https://login.microsoftonline.com/tenant/oauth2/v2.0/token"
        );
    }

    #[test]
    fn should_derive_sovereign_authority_when_blob_endpoint_identifies_cloud() {
        // Arrange
        let _env_guard = azure_client_id_env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _authority = TestEnvVar::remove("AZURE_AUTHORITY_HOST");
        let government =
            AzureEndpoint::account_base("https://account.blob.core.usgovcloudapi.net".to_string());
        let china =
            AzureEndpoint::account_base("https://account.blob.core.chinacloudapi.cn".to_string());

        // Act
        let government_url = azure_oauth_token_url(
            "government-tenant",
            azure_oauth_authority_for_blob_endpoint(Some(&government)),
        )
        .expect("government OAuth token URL");
        let china_url = azure_oauth_token_url(
            "china-tenant",
            azure_oauth_authority_for_blob_endpoint(Some(&china)),
        )
        .expect("China OAuth token URL");

        // Assert
        assert_eq!(
            government_url,
            "https://login.microsoftonline.us/government-tenant/oauth2/v2.0/token"
        );
        assert_eq!(
            china_url,
            "https://login.chinacloudapi.cn/china-tenant/oauth2/v2.0/token"
        );
    }

    #[test]
    fn should_wire_sovereign_authority_through_oauth_provider_construction() {
        // Arrange
        let _env_guard = azure_client_id_env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _authority = TestEnvVar::remove("AZURE_AUTHORITY_HOST");
        let _tenant = TestEnvVar::set("AZURE_TENANT_ID", "tenant");
        let _client = TestEnvVar::set("AZURE_CLIENT_ID", "client");
        let _secret = TestEnvVar::set("AZURE_CLIENT_SECRET", "secret");

        // Act
        let government = AzureProvider::from_lightweight_credential_source(
            "account".to_string(),
            "container".to_string(),
            Some("https://account.blob.core.usgovcloudapi.net".to_string()),
            super::super::AzureCredentialSource::EnvironmentClientSecret,
        )
        .expect("government OAuth provider");
        let china = AzureProvider::from_lightweight_credential_source(
            "account".to_string(),
            "container".to_string(),
            Some("https://account.blob.core.chinacloudapi.cn".to_string()),
            super::super::AzureCredentialSource::EnvironmentClientSecret,
        )
        .expect("China OAuth provider");

        // Assert
        assert_eq!(
            government.oauth_authority(),
            Some(AZURE_GOVERNMENT_AUTHORITY)
        );
        assert_eq!(china.oauth_authority(), Some(AZURE_CHINA_AUTHORITY));
    }

    #[test]
    fn should_preserve_explicit_authority_override_given_sovereign_blob_endpoint() {
        // Arrange
        let _env_guard = azure_client_id_env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _authority = TestEnvVar::set("AZURE_AUTHORITY_HOST", "https://login.example.invalid");
        let government =
            AzureEndpoint::account_base("https://account.blob.core.usgovcloudapi.net".to_string());

        // Act
        let url = azure_oauth_token_url(
            "tenant",
            azure_oauth_authority_for_blob_endpoint(Some(&government)),
        )
        .expect("explicit OAuth token URL");

        // Assert
        assert_eq!(
            url,
            "https://login.example.invalid/tenant/oauth2/v2.0/token"
        );
    }

    #[test]
    fn should_ignore_blank_azure_environment_credentials_when_selecting_default_chain() {
        // Arrange
        let _env_guard = azure_client_id_env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _tenant = TestEnvVar::set("AZURE_TENANT_ID", "   ");
        let _client = TestEnvVar::set("AZURE_CLIENT_ID", "");
        let _secret = TestEnvVar::set("AZURE_CLIENT_SECRET", "\t");
        let _token_file = TestEnvVar::set("AZURE_FEDERATED_TOKEN_FILE", "  ");

        // Act
        let has_client_secret = AzureOAuthProvider::has_environment_client_secret();
        let has_workload_identity = AzureOAuthProvider::has_workload_identity();

        // Assert
        assert!(!has_client_secret);
        assert!(!has_workload_identity);
    }

    #[test]
    fn should_reject_empty_azure_workload_identity_assertion() {
        // Arrange
        let token_path = std::env::temp_dir().join(format!(
            "midge-empty-azure-federated-token-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::write(&token_path, "  \n").expect("write empty federated token fixture");
        let provider = AzureOAuthProvider::from_workload_identity(
            Some("tenant".to_string()),
            Some("client".to_string()),
            Some(token_path.clone()),
        )
        .expect("construct workload identity provider");

        // Act
        let error = provider
            .fetch_token(AZURE_PUBLIC_AUTHORITY)
            .expect_err("empty workload identity assertion must fail closed");
        let _ = std::fs::remove_file(token_path);

        // Assert
        assert!(matches!(
            error,
            MidgeError::InvalidArgument(message) if message.contains("is empty")
        ));
    }

    #[test]
    fn should_reject_insecure_or_malformed_azure_authority_host() {
        // Arrange
        let _env_guard = azure_client_id_env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let authorities = [
            "not a URL",
            "http://login.microsoftonline.us",
            "https://user:secret@login.microsoftonline.us",
            "https://login.microsoftonline.us/base",
            "https://login.microsoftonline.us?query=1",
            "https://login.microsoftonline.us#fragment",
        ];

        // Act
        for authority in authorities {
            let _authority = TestEnvVar::set("AZURE_AUTHORITY_HOST", authority);
            let result = azure_oauth_token_url("tenant", AZURE_PUBLIC_AUTHORITY);

            // Assert
            assert!(
                matches!(result, Err(MidgeError::InvalidArgument(ref message)) if message.contains("AZURE_AUTHORITY_HOST")),
                "invalid authority host must fail closed: {result:?}"
            );
        }
    }

    #[test]
    fn should_reject_empty_azure_oauth_access_token() {
        // Arrange
        let response = r#"{"access_token":"   ","expires_in":3600}"#;

        // Act
        let result = parse_azure_oauth_token_response(response, 1_000);

        // Assert
        assert!(
            matches!(result, Err(MidgeError::Internal(ref message)) if message.contains("access_token")),
            "empty access tokens must fail closed: {result:?}"
        );
    }

    #[test]
    fn should_reject_missing_invalid_or_non_future_azure_oauth_expiry() {
        // Arrange
        let responses = [
            r#"{"access_token":"token"}"#,
            r#"{"access_token":"token","expires_on":"not-a-timestamp"}"#,
            r#"{"access_token":"token","expires_on":"1000"}"#,
            r#"{"access_token":"token","expires_in":0}"#,
        ];

        // Act
        let results = responses.map(|response| parse_azure_oauth_token_response(response, 1_000));

        // Assert
        for result in results {
            assert!(
                matches!(result, Err(MidgeError::Internal(ref message)) if message.contains("expir")),
                "unusable OAuth expiry must fail closed: {result:?}"
            );
        }
    }

    #[test]
    fn should_reject_missing_or_invalid_managed_identity_expiry() {
        // Arrange
        let responses = [
            br#"{"access_token":"token"}"#.to_vec(),
            br#"{"access_token":"token","expires_on":"not-a-timestamp"}"#.to_vec(),
        ];

        // Act
        let results = responses.map(|body| {
            let server = spawn_recording_http_server(Vec::new(), body);
            let signer = ManagedIdentitySigner::with_endpoint(
                None,
                server.endpoint.clone(),
                ManagedIdentityEndpointKind::VmImds,
            );
            let result = signer.fetch_token();
            let _ = server.finish();
            result
        });

        // Assert
        for result in results {
            assert!(
                matches!(result, Err(MidgeError::Internal(ref message)) if message.contains("expires_on")),
                "unusable managed identity expiry must fail closed: {result:?}"
            );
        }
    }

    #[test]
    fn should_reject_empty_managed_identity_access_token() {
        // Arrange
        let _env_guard = azure_client_id_env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let server = spawn_recording_http_server(
            Vec::new(),
            br#"{"access_token":"   ","expires_on":"4102444800"}"#.to_vec(),
        );
        let _identity_endpoint = TestEnvVar::set("IDENTITY_ENDPOINT", &server.endpoint);
        let _identity_header = TestEnvVar::set("IDENTITY_HEADER", "rotating-secret");
        let signer = ManagedIdentitySigner::new(None);

        // Act
        let result = signer.fetch_token();
        let _ = server.finish();

        // Assert
        assert!(
            matches!(result, Err(MidgeError::Internal(ref message)) if message.contains("access_token")),
            "empty access tokens must fail closed: {result:?}"
        );
    }

    #[test]
    fn should_reject_non_future_managed_identity_expiry() {
        // Arrange
        let _env_guard = azure_client_id_env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let server = spawn_recording_http_server(
            Vec::new(),
            br#"{"access_token":"token","expires_on":"1"}"#.to_vec(),
        );
        let _identity_endpoint = TestEnvVar::set("IDENTITY_ENDPOINT", &server.endpoint);
        let _identity_header = TestEnvVar::set("IDENTITY_HEADER", "rotating-secret");
        let signer = ManagedIdentitySigner::new(None);

        // Act
        let result = signer.fetch_token();
        let _ = server.finish();

        // Assert
        assert!(
            matches!(result, Err(MidgeError::Internal(ref message)) if message.contains("expires_on")),
            "expired tokens must fail closed: {result:?}"
        );
    }

    #[test]
    fn should_resolve_storage_account_from_environment_when_argument_is_empty() {
        // Arrange
        let _env_guard = azure_client_id_env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _connection_string = TestEnvVar::remove("AZURE_STORAGE_CONNECTION_STRING");
        let _account = TestEnvVar::set("AZURE_STORAGE_ACCOUNT", "environment-account");
        let _key = TestEnvVar::set("AZURE_STORAGE_KEY", "dGVzdA==");
        let _sas = TestEnvVar::remove("AZURE_STORAGE_SAS_TOKEN");

        // Act
        let provider =
            AzureProvider::from_env_and_endpoint(String::new(), "container".to_string(), None)
                .expect("environment-backed Azure provider");

        // Assert
        assert_eq!(provider.account_name(), "environment-account");
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
