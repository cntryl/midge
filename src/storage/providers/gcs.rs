//! Google Cloud Storage Provider
//!
//! Production implementation using direct JSON API (no SDK dependency):
//! - OAuth2 Bearer token authentication
//! - Service-account HMAC key authentication (for S3-interop or simple setups)
//! - Non-blocking callback-based API via `CloudExecutor`
//! - All operations routed through the same `CloudBackend` trait as S3/Azure

use super::super::cloud::{
    CloudBackend, CloudCallback, CloudEvent, CloudExecutor, CloudOutcome, CloudRequest,
    CloudResponse, CloudSigner, ObjectMetadata,
};
use crate::common::{MidgeError, MidgeResult};
use base64::{
    engine::general_purpose::{STANDARD as BASE64, URL_SAFE_NO_PAD},
    Engine as _,
};
use chrono::Utc;
use hmac::{Hmac, KeyInit, Mac};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use reqwest::Method;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs1v15::SigningKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::signature::{SignatureEncoding, Signer as _};
use rsa::RsaPrivateKey;
use serde_json::json;
use sha1::Sha1;
use sha2::Sha256;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
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

/// GCS authentication credentials.
#[derive(Debug, Clone)]
pub enum GcsCredential {
    /// OAuth2 Bearer token (short-lived, from gcloud CLI or metadata server).
    BearerToken { token: String },
    /// Service-account HMAC key pair (for HMAC-based auth, simpler than OAuth2).
    HmacKey { access_id: String, secret: String },
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
    ) -> MidgeResult<Self> {
        Self::with_bearer_token_endpoint(bucket, project_id, token, None)
    }

    /// Create provider with an OAuth2 Bearer token and optional endpoint override.
    pub fn with_bearer_token_endpoint(
        bucket: String,
        project_id: String,
        token: String,
        endpoint: Option<String>,
    ) -> MidgeResult<Self> {
        let credential = GcsCredential::BearerToken {
            token: token.clone(),
        };
        let signer: Option<Arc<dyn CloudSigner>> =
            Some(Arc::new(BearerTokenSigner::new_static(token)));
        let executor = CloudExecutor::new(signer)?;
        let backend = Arc::new(GcsBackend::json(bucket.clone(), endpoint, executor));
        Ok(Self {
            backend,
            bucket,
            project_id,
            credential,
        })
    }

    pub(crate) fn with_bearer_credential_endpoint(
        bucket: String,
        project_id: String,
        source: &super::GcsCredentialSource,
        endpoint: Option<String>,
    ) -> MidgeResult<Self> {
        let provider = GcsTokenProvider::from_source(source)?;
        let credential = GcsCredential::BearerToken {
            token: "<dynamic>".to_string(),
        };
        let signer: Option<Arc<dyn CloudSigner>> =
            Some(Arc::new(BearerTokenSigner::new_provider(provider)));
        let executor = CloudExecutor::new(signer)?;
        let backend = Arc::new(GcsBackend::json(bucket.clone(), endpoint, executor));
        Ok(Self {
            backend,
            bucket,
            project_id,
            credential,
        })
    }

    /// Create provider with a service-account HMAC key pair.
    pub fn with_hmac_key(
        bucket: String,
        project_id: String,
        access_id: String,
        secret: String,
    ) -> MidgeResult<Self> {
        Self::with_hmac_key_xml_endpoint(bucket, project_id, access_id, secret, None)
    }

    /// Create provider with a GCS XML API HMAC key pair and optional endpoint override.
    pub fn with_hmac_key_xml_endpoint(
        bucket: String,
        project_id: String,
        access_id: String,
        secret: String,
        endpoint: Option<String>,
    ) -> MidgeResult<Self> {
        let credential = GcsCredential::HmacKey {
            access_id: access_id.clone(),
            secret: secret.clone(),
        };
        let signer: Option<Arc<dyn CloudSigner>> =
            Some(Arc::new(Goog1HmacSigner::new(access_id, secret)));
        let executor = CloudExecutor::new(signer)?;
        let backend = Arc::new(GcsBackend::xml(bucket.clone(), endpoint, executor));
        Ok(Self {
            backend,
            bucket,
            project_id,
            credential,
        })
    }

    /// Legacy constructor — creates a provider with an empty bearer token.
    /// Callers should prefer `with_bearer_token` or `with_hmac_key`.
    pub fn new(bucket: String, project_id: String) -> MidgeResult<Self> {
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

impl Drop for GcsProvider {
    fn drop(&mut self) {
        tracing::trace!("GcsProvider dropping, cleanup will propagate to CloudExecutor");
    }
}

pub(crate) fn resolve_bearer_token_from_source(
    source: &super::GcsCredentialSource,
) -> MidgeResult<String> {
    Ok(GcsTokenProvider::from_source(source)?
        .fetch_token()?
        .access_token)
}

fn default_gcloud_adc_file() -> Option<std::path::PathBuf> {
    std::env::var_os("HOME")
        .map(std::path::PathBuf::from)
        .map(|home| home.join(".config/gcloud/application_default_credentials.json"))
}

#[derive(Clone)]
struct CachedGcsToken {
    access_token: String,
    expires_at: Option<u64>,
}

impl CachedGcsToken {
    fn static_token(access_token: String) -> Self {
        Self {
            access_token,
            expires_at: None,
        }
    }

    fn expiring(access_token: String, expires_at: u64) -> Self {
        Self {
            access_token,
            expires_at: Some(expires_at),
        }
    }

    fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(expires_at) => current_unix_secs() >= expires_at.saturating_sub(300),
            None => false,
        }
    }
}

#[derive(Clone)]
enum GcsTokenProvider {
    StaticBearer(String),
    ApplicationDefault,
    ServiceAccountFile(PathBuf),
    AuthorizedUserFile(PathBuf),
    MetadataServer,
}

impl GcsTokenProvider {
    fn from_source(source: &super::GcsCredentialSource) -> MidgeResult<Self> {
        match source {
            super::GcsCredentialSource::BearerToken { token } => {
                Ok(Self::StaticBearer(token.clone()))
            }
            super::GcsCredentialSource::ApplicationDefault => Ok(Self::ApplicationDefault),
            super::GcsCredentialSource::ServiceAccountJsonFile { path } => {
                Ok(Self::ServiceAccountFile(path.clone()))
            }
            super::GcsCredentialSource::AuthorizedUserJsonFile { path } => {
                Ok(Self::AuthorizedUserFile(path.clone()))
            }
            super::GcsCredentialSource::MetadataServer => Ok(Self::MetadataServer),
            super::GcsCredentialSource::HmacKey { .. } => Err(MidgeError::InvalidArgument(
                "GCS HMAC credentials are not bearer-token credentials".to_string(),
            )),
        }
    }

    fn fetch_token(&self) -> MidgeResult<CachedGcsToken> {
        match self {
            Self::StaticBearer(token) => Ok(CachedGcsToken::static_token(token.clone())),
            Self::MetadataServer => fetch_metadata_token(),
            Self::ServiceAccountFile(path) | Self::AuthorizedUserFile(path) => {
                token_from_adc_file(path)
            }
            Self::ApplicationDefault => {
                if let Ok(path) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
                    return token_from_adc_file(std::path::Path::new(&path));
                }
                if let Some(path) = default_gcloud_adc_file() {
                    if path.exists() {
                        return token_from_adc_file(&path);
                    }
                }
                fetch_metadata_token()
            }
        }
    }
}

fn token_from_adc_file(path: &std::path::Path) -> MidgeResult<CachedGcsToken> {
    let content = std::fs::read_to_string(path).map_err(|error| {
        MidgeError::Internal(format!(
            "failed to read GCS ADC file '{}': {}",
            path.display(),
            error
        ))
    })?;
    let json: serde_json::Value = serde_json::from_str(&content).map_err(|error| {
        MidgeError::InvalidArgument(format!(
            "failed to parse GCS ADC file '{}': {}",
            path.display(),
            error
        ))
    })?;
    match json.get("type").and_then(|value| value.as_str()) {
        Some("authorized_user") => refresh_authorized_user_token(&json),
        Some("service_account") => fetch_service_account_token(&json),
        Some("external_account") => fetch_external_account_token(&json),
        other => Err(MidgeError::InvalidArgument(format!(
            "unsupported GCS ADC credential type {:?}",
            other
        ))),
    }
}

fn fetch_external_account_token(json: &serde_json::Value) -> MidgeResult<CachedGcsToken> {
    let audience = required_json_str(json, "audience")?;
    let subject_token_type = required_json_str(json, "subject_token_type")?;
    let token_url = required_json_str(json, "token_url")?;
    let subject_token = external_account_subject_token(json)?;
    let token = post_form_for_access_token(
        token_url,
        &[
            (
                "grant_type",
                "urn:ietf:params:oauth:grant-type:token-exchange".to_string(),
            ),
            ("audience", audience.to_string()),
            (
                "scope",
                "https://www.googleapis.com/auth/devstorage.full_control".to_string(),
            ),
            (
                "requested_token_type",
                "urn:ietf:params:oauth:token-type:access_token".to_string(),
            ),
            ("subject_token_type", subject_token_type.to_string()),
            ("subject_token", subject_token),
        ],
    )?;

    if let Some(url) = json
        .get("service_account_impersonation_url")
        .and_then(|value| value.as_str())
    {
        impersonate_gcs_service_account(url, &token.access_token)
    } else {
        Ok(token)
    }
}

fn external_account_subject_token(json: &serde_json::Value) -> MidgeResult<String> {
    let source = json.get("credential_source").ok_or_else(|| {
        MidgeError::InvalidArgument("GCS external_account ADC missing credential_source".into())
    })?;
    if source.get("executable").is_some()
        || source.get("command").is_some()
        || json.get("executable").is_some()
    {
        return Err(MidgeError::InvalidArgument(
            "GCS executable external_account ADC is not supported without process execution"
                .to_string(),
        ));
    }

    let file = source
        .get("file")
        .and_then(|value| value.as_str())
        .ok_or_else(|| {
            MidgeError::InvalidArgument(
                "GCS external_account ADC currently supports file credential_source only"
                    .to_string(),
            )
        })?;
    let content = std::fs::read_to_string(file).map_err(|error| {
        MidgeError::Internal(format!(
            "failed to read GCS external_account subject token file '{}': {}",
            file, error
        ))
    })?;

    match source
        .get("format")
        .and_then(|format| format.get("type"))
        .and_then(|value| value.as_str())
    {
        Some("json") => {
            let field = source
                .get("format")
                .and_then(|format| format.get("subject_token_field_name"))
                .and_then(|value| value.as_str())
                .ok_or_else(|| {
                    MidgeError::InvalidArgument(
                        "GCS external_account JSON credential_source missing subject_token_field_name"
                            .to_string(),
                    )
                })?;
            let token_json: serde_json::Value =
                serde_json::from_str(&content).map_err(|error| {
                    MidgeError::InvalidArgument(format!(
                        "failed to parse GCS external_account subject token JSON '{}': {}",
                        file, error
                    ))
                })?;
            token_json
                .get(field)
                .and_then(|value| value.as_str())
                .map(str::to_string)
                .ok_or_else(|| {
                    MidgeError::InvalidArgument(format!(
                        "GCS external_account subject token JSON missing string field {}",
                        field
                    ))
                })
        }
        Some("text") | None => {
            let token = content.trim();
            if token.is_empty() {
                Err(MidgeError::InvalidArgument(
                    "GCS external_account subject token file is empty".to_string(),
                ))
            } else {
                Ok(token.to_string())
            }
        }
        Some(format) => Err(MidgeError::InvalidArgument(format!(
            "unsupported GCS external_account subject token format {}",
            format
        ))),
    }
}

fn impersonate_gcs_service_account(
    url: &str,
    source_access_token: &str,
) -> MidgeResult<CachedGcsToken> {
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|error| {
            MidgeError::Internal(format!(
                "GCS service account impersonation client init: {}",
                error
            ))
        })?;
    let response = client
        .post(url)
        .bearer_auth(source_access_token)
        .json(&json!({
            "scope": ["https://www.googleapis.com/auth/devstorage.full_control"],
            "lifetime": "3600s",
        }))
        .send()
        .map_err(|error| {
            MidgeError::Internal(format!(
                "GCS service account impersonation request: {}",
                error
            ))
        })?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().unwrap_or_default();
        return Err(MidgeError::Internal(format!(
            "GCS service account impersonation failed with status {}: {}",
            status, body
        )));
    }
    let body = response.text().map_err(|error| {
        MidgeError::Internal(format!("GCS service account impersonation body: {}", error))
    })?;
    parse_impersonated_access_token_json(&body)
}

fn refresh_authorized_user_token(json: &serde_json::Value) -> MidgeResult<CachedGcsToken> {
    let client_id = required_json_str(json, "client_id")?;
    let client_secret = required_json_str(json, "client_secret")?;
    let refresh_token = required_json_str(json, "refresh_token")?;
    let token_uri = json
        .get("token_uri")
        .and_then(|value| value.as_str())
        .unwrap_or("https://oauth2.googleapis.com/token");
    post_form_for_access_token(
        token_uri,
        &[
            ("grant_type", "refresh_token".to_string()),
            ("client_id", client_id.to_string()),
            ("client_secret", client_secret.to_string()),
            ("refresh_token", refresh_token.to_string()),
        ],
    )
}

fn fetch_service_account_token(json: &serde_json::Value) -> MidgeResult<CachedGcsToken> {
    let client_email = required_json_str(json, "client_email")?;
    let private_key = required_json_str(json, "private_key")?;
    let token_uri = json
        .get("token_uri")
        .and_then(|value| value.as_str())
        .unwrap_or("https://oauth2.googleapis.com/token");
    let iat = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let exp = iat.saturating_add(3600);
    let header = json!({"alg": "RS256", "typ": "JWT"});
    let claims = json!({
        "iss": client_email,
        "scope": "https://www.googleapis.com/auth/devstorage.full_control",
        "aud": token_uri,
        "iat": iat,
        "exp": exp,
    });
    let signing_input = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).map_err(|error| {
            MidgeError::Internal(format!("failed to encode GCS JWT header: {}", error))
        })?),
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).map_err(|error| {
            MidgeError::Internal(format!("failed to encode GCS JWT claims: {}", error))
        })?)
    );
    let assertion = sign_service_account_jwt(private_key, &signing_input)?;
    post_form_for_access_token(
        token_uri,
        &[
            (
                "grant_type",
                "urn:ietf:params:oauth:grant-type:jwt-bearer".to_string(),
            ),
            ("assertion", assertion),
        ],
    )
}

fn sign_service_account_jwt(private_key: &str, signing_input: &str) -> MidgeResult<String> {
    let key = match RsaPrivateKey::from_pkcs8_pem(private_key) {
        Ok(key) => key,
        Err(pkcs8_error) => RsaPrivateKey::from_pkcs1_pem(private_key).map_err(|pkcs1_error| {
            MidgeError::InvalidArgument(format!(
                "invalid GCS service account private key: PKCS#8 parse failed: {}; PKCS#1 parse failed: {}",
                pkcs8_error, pkcs1_error
            ))
        })?,
    };
    let signing_key = SigningKey::<Sha256>::new(key);
    let signature = signing_key.sign(signing_input.as_bytes());
    Ok(format!(
        "{}.{}",
        signing_input,
        URL_SAFE_NO_PAD.encode(signature.to_bytes())
    ))
}

fn fetch_metadata_token() -> MidgeResult<CachedGcsToken> {
    let host = std::env::var("GCE_METADATA_HOST")
        .unwrap_or_else(|_| "metadata.google.internal".to_string());
    let url = format!(
        "http://{}/computeMetadata/v1/instance/service-accounts/default/token",
        host.trim_end_matches('/')
    );
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
        .map_err(|error| MidgeError::Internal(format!("GCS metadata client init: {}", error)))?;
    let response = client
        .get(url)
        .header("Metadata-Flavor", "Google")
        .send()
        .map_err(|error| MidgeError::Internal(format!("GCS metadata token request: {}", error)))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().unwrap_or_default();
        return Err(MidgeError::Internal(format!(
            "GCS metadata token request failed with status {}: {}",
            status, body
        )));
    }
    let body = response
        .text()
        .map_err(|error| MidgeError::Internal(format!("GCS metadata response body: {}", error)))?;
    parse_access_token_json(&body)
}

fn post_form_for_access_token(url: &str, values: &[(&str, String)]) -> MidgeResult<CachedGcsToken> {
    let body = values
        .iter()
        .map(|(key, value)| format!("{}={}", key, urlencoding::encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|error| MidgeError::Internal(format!("GCS token client init: {}", error)))?;
    let response = client
        .post(url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .map_err(|error| MidgeError::Internal(format!("GCS token request: {}", error)))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().unwrap_or_default();
        return Err(MidgeError::Internal(format!(
            "GCS token request failed with status {}: {}",
            status, body
        )));
    }
    let body = response
        .text()
        .map_err(|error| MidgeError::Internal(format!("GCS token response body: {}", error)))?;
    parse_access_token_json(&body)
}

fn parse_access_token_json(body: &str) -> MidgeResult<CachedGcsToken> {
    let json: serde_json::Value = serde_json::from_str(body)
        .map_err(|error| MidgeError::Internal(format!("GCS token JSON parse: {}", error)))?;
    let access_token = json
        .get("access_token")
        .and_then(|value| value.as_str())
        .map(str::to_string)
        .ok_or_else(|| MidgeError::Internal("GCS token response missing access_token".into()))?;
    let expires_at = json
        .get("expires_in")
        .and_then(|value| value.as_u64())
        .map(|ttl| current_unix_secs().saturating_add(ttl))
        .unwrap_or_else(|| current_unix_secs().saturating_add(3600));
    Ok(CachedGcsToken::expiring(access_token, expires_at))
}

fn parse_impersonated_access_token_json(body: &str) -> MidgeResult<CachedGcsToken> {
    let json: serde_json::Value = serde_json::from_str(body).map_err(|error| {
        MidgeError::Internal(format!(
            "GCS service account impersonation JSON parse: {}",
            error
        ))
    })?;
    let access_token = json
        .get("accessToken")
        .and_then(|value| value.as_str())
        .map(str::to_string)
        .ok_or_else(|| {
            MidgeError::Internal(
                "GCS service account impersonation response missing accessToken".into(),
            )
        })?;
    let expires_at = json
        .get("expireTime")
        .and_then(|value| value.as_str())
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .map(|datetime| datetime.timestamp().max(0) as u64)
        .unwrap_or_else(|| current_unix_secs().saturating_add(3600));
    Ok(CachedGcsToken::expiring(access_token, expires_at))
}

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn required_json_str<'a>(json: &'a serde_json::Value, field: &str) -> MidgeResult<&'a str> {
    json.get(field)
        .and_then(|value| value.as_str())
        .ok_or_else(|| {
            MidgeError::InvalidArgument(format!("GCS credential JSON missing {}", field))
        })
}

// ---------------------------------------------------------------------------
// Backend (private — implements CloudBackend)
// ---------------------------------------------------------------------------

struct GcsBackend {
    bucket: String,
    endpoint: String,
    mode: GcsBackendMode,
    executor: CloudExecutor,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum GcsBackendMode {
    Json,
    Xml,
}

impl GcsBackend {
    fn new(bucket: String, executor: CloudExecutor) -> Self {
        Self::json(bucket, None, executor)
    }

    fn json(bucket: String, endpoint: Option<String>, executor: CloudExecutor) -> Self {
        Self {
            bucket,
            endpoint: endpoint.unwrap_or_else(|| "https://storage.googleapis.com".to_string()),
            mode: GcsBackendMode::Json,
            executor,
        }
    }

    fn xml(bucket: String, endpoint: Option<String>, executor: CloudExecutor) -> Self {
        Self {
            bucket,
            endpoint: endpoint.unwrap_or_else(|| "https://storage.googleapis.com".to_string()),
            mode: GcsBackendMode::Xml,
            executor,
        }
    }

    fn canonical_key(&self, key: &str) -> String {
        key.split('/')
            .map(|seg| utf8_percent_encode(seg, ENCODE_SET).to_string())
            .collect::<Vec<_>>()
            .join("/")
    }

    /// Upload URL: `https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o?uploadType=media&name={key}`
    fn upload_url(&self, key: &str) -> String {
        match self.mode {
            GcsBackendMode::Json => format!(
                "{}/upload/storage/v1/b/{}/o?uploadType=media&name={}",
                self.endpoint.trim_end_matches('/'),
                self.bucket,
                urlencoding::encode(key)
            ),
            GcsBackendMode::Xml => format!(
                "{}/{}/{}",
                self.endpoint.trim_end_matches('/'),
                self.bucket,
                self.canonical_key(key)
            ),
        }
    }

    /// Download URL (media): `https://storage.googleapis.com/storage/v1/b/{bucket}/o/{key}?alt=media`
    fn download_url(&self, key: &str) -> String {
        match self.mode {
            GcsBackendMode::Json => format!(
                "{}/storage/v1/b/{}/o/{}?alt=media",
                self.endpoint.trim_end_matches('/'),
                self.bucket,
                self.canonical_key(key)
            ),
            GcsBackendMode::Xml => self.upload_url(key),
        }
    }

    /// Metadata URL: `https://storage.googleapis.com/storage/v1/b/{bucket}/o/{key}`
    fn metadata_url(&self, key: &str) -> String {
        match self.mode {
            GcsBackendMode::Json => format!(
                "{}/storage/v1/b/{}/o/{}",
                self.endpoint.trim_end_matches('/'),
                self.bucket,
                self.canonical_key(key)
            ),
            GcsBackendMode::Xml => self.upload_url(key),
        }
    }

    /// List URL: `https://storage.googleapis.com/storage/v1/b/{bucket}/o?prefix={prefix}`
    fn list_url(&self, prefix: &str, page_token: Option<&str>) -> String {
        match self.mode {
            GcsBackendMode::Json => {
                let mut url = format!(
                    "{}/storage/v1/b/{}/o?prefix={}",
                    self.endpoint.trim_end_matches('/'),
                    self.bucket,
                    urlencoding::encode(prefix)
                );
                if let Some(token) = page_token {
                    url.push_str("&pageToken=");
                    url.push_str(&urlencoding::encode(token));
                }
                url
            }
            GcsBackendMode::Xml => {
                let mut url = format!(
                    "{}/{}?prefix={}",
                    self.endpoint.trim_end_matches('/'),
                    self.bucket,
                    urlencoding::encode(prefix)
                );
                if let Some(token) = page_token {
                    url.push_str("&marker=");
                    url.push_str(&urlencoding::encode(token));
                }
                url
            }
        }
    }
}

struct GcsListState {
    prefix: String,
    endpoint: String,
    bucket: String,
    mode: GcsBackendMode,
    page_token: Option<String>,
    items: Vec<String>,
}

impl GcsListState {
    fn url(&self) -> String {
        match self.mode {
            GcsBackendMode::Json => {
                let mut url = format!(
                    "{}/storage/v1/b/{}/o?prefix={}",
                    self.endpoint.trim_end_matches('/'),
                    self.bucket,
                    urlencoding::encode(&self.prefix)
                );
                if let Some(token) = self.page_token.as_deref() {
                    url.push_str("&pageToken=");
                    url.push_str(&urlencoding::encode(token));
                }
                url
            }
            GcsBackendMode::Xml => {
                let mut url = format!(
                    "{}/{}?prefix={}",
                    self.endpoint.trim_end_matches('/'),
                    self.bucket,
                    urlencoding::encode(&self.prefix)
                );
                if let Some(token) = self.page_token.as_deref() {
                    url.push_str("&marker=");
                    url.push_str(&urlencoding::encode(token));
                }
                url
            }
        }
    }
}

impl CloudBackend for GcsBackend {
    fn submit_put(
        &self,
        key: String,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        let method = match self.mode {
            GcsBackendMode::Json => Method::POST,
            GcsBackendMode::Xml => Method::PUT,
        };
        let url = self.upload_url(&key);
        let mut request = CloudRequest::new(method, url)
            .with_body(data)
            .with_header("Content-Type", "application/octet-stream");
        // Attach any additional headers provided by caller (e.g. conditional headers)
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

    fn submit_get_range(&self, key: String, start: u64, end: Option<u64>, callback: CloudCallback) {
        let url = self.download_url(&key);
        let range = match end {
            Some(e) => format!("bytes={}-{}", start, e.saturating_sub(1)),
            None => format!("bytes={}-", start),
        };
        let request = CloudRequest::new(Method::GET, url).with_header("Range", range);
        let mapper = move |ctx: String, result: MidgeResult<CloudResponse>| match result {
            Ok(resp) if resp.status == 206 || resp.status == 200 => CloudEvent::GetRangeComplete {
                key: ctx,
                start,
                end,
                result: CloudOutcome::Ok(resp.body),
            },
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

    fn submit_delete(&self, key: String, headers: Vec<(String, String)>, callback: CloudCallback) {
        let mut url = self.metadata_url(&key);
        let mut request = CloudRequest::new(Method::DELETE, String::new());
        for (name, value) in headers.into_iter() {
            if self.mode == GcsBackendMode::Json
                && name.eq_ignore_ascii_case("x-goog-if-generation-match")
            {
                url = append_query_param(&url, "ifGenerationMatch", &value);
            } else {
                request = request.with_header(name, value);
            }
        }
        request.url = url;
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
        let state = GcsListState {
            prefix: prefix.clone(),
            endpoint: self.endpoint.clone(),
            bucket: self.bucket.clone(),
            mode: self.mode,
            page_token: None,
            items: Vec::new(),
        };
        self.executor.spawn_request_loop(
            state,
            prefix.clone(),
            callback,
            |state| Ok(CloudRequest::new(Method::GET, state.url())),
            |state, resp| {
                if resp.status != 200 {
                    return Err(MidgeError::Internal(format!(
                        "GCS LIST status {}",
                        resp.status
                    )));
                }
                let body = String::from_utf8_lossy(&resp.body);
                match state.mode {
                    GcsBackendMode::Json => {
                        let (items, next_page_token) = extract_gcs_json_list(&body)?;
                        state.items.extend(items);
                        state.page_token = next_page_token;
                    }
                    GcsBackendMode::Xml => {
                        state.items.extend(extract_xml_tag_values(&body, "Key"));
                        let truncated = extract_xml_tag_values(&body, "IsTruncated")
                            .first()
                            .map(|value| value.eq_ignore_ascii_case("true"))
                            .unwrap_or(false);
                        state.page_token = extract_xml_tag_values(&body, "NextMarker")
                            .into_iter()
                            .next()
                            .or_else(|| {
                                if truncated {
                                    state.items.last().cloned()
                                } else {
                                    None
                                }
                            })
                            .filter(|marker| !marker.is_empty());
                        if truncated && state.page_token.is_none() {
                            return Err(MidgeError::Internal(
                                "GCS XML list response was truncated without NextMarker"
                                    .to_string(),
                            ));
                        }
                    }
                }
                Ok(state.page_token.is_some())
            },
            |ctx, result| match result {
                Ok(state) => CloudEvent::ListComplete {
                    prefix: ctx,
                    result: CloudOutcome::Ok(state.items),
                },
                Err(err) => CloudEvent::ListComplete {
                    prefix: ctx,
                    result: CloudOutcome::Err(format!("{:?}", err)),
                },
            },
        );
    }

    fn submit_head(&self, key: String, callback: CloudCallback) {
        // GCS JSON: GET metadata URL. GCS XML: HEAD object URL.
        let url = self.metadata_url(&key);
        let method = match self.mode {
            GcsBackendMode::Json => Method::GET,
            GcsBackendMode::Xml => Method::HEAD,
        };
        let request = CloudRequest::new(method, url);
        let mapper = move |ctx: String, result: MidgeResult<CloudResponse>| match result {
            Ok(resp) if resp.status == 200 => {
                let body = String::from_utf8_lossy(&resp.body);
                let size = resp
                    .headers
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                    .and_then(|(_, value)| value.parse::<u64>().ok())
                    .or_else(|| {
                        extract_json_string_value(&body, "size").and_then(|s| s.parse().ok())
                    })
                    .unwrap_or(0);
                let etag = resp
                    .headers
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case("etag"))
                    .map(|(_, value)| value.trim_matches('"').to_string())
                    .or_else(|| extract_json_string_value(&body, "etag"))
                    .unwrap_or_default();
                let generation = resp
                    .headers
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case("x-goog-generation"))
                    .map(|(_, value)| value.to_string())
                    .or_else(|| extract_json_string_value(&body, "generation"));
                let metadata = match generation {
                    Some(generation) => ObjectMetadata::with_generation(size, etag, 0, generation),
                    None => ObjectMetadata::new(size, etag, 0),
                };
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

fn extract_gcs_json_list(body: &str) -> MidgeResult<(Vec<String>, Option<String>)> {
    let json: serde_json::Value = serde_json::from_str(body)
        .map_err(|error| MidgeError::Internal(format!("GCS list JSON parse: {}", error)))?;
    let items = json
        .get("items")
        .and_then(|value| value.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.get("name").and_then(|value| value.as_str()))
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let next_page_token = json
        .get("nextPageToken")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    Ok((items, next_page_token))
}

fn extract_xml_tag_values(body: &str, tag: &str) -> Vec<String> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
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

fn append_query_param(url: &str, key: &str, value: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!(
        "{}{}{}={}",
        url,
        separator,
        urlencoding::encode(key),
        urlencoding::encode(value)
    )
}

// ---------------------------------------------------------------------------
// Bearer Token Signer
// ---------------------------------------------------------------------------

struct BearerTokenSigner {
    provider: GcsTokenProvider,
    token_cache: Arc<Mutex<Option<CachedGcsToken>>>,
}

impl BearerTokenSigner {
    fn new_static(token: String) -> Self {
        Self::new_provider(GcsTokenProvider::StaticBearer(token))
    }

    fn new_provider(provider: GcsTokenProvider) -> Self {
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
                .map_err(|_| MidgeError::Internal("GCS token cache poisoned".into()))?;
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
            .map_err(|_| MidgeError::Internal("GCS token cache poisoned".into()))?;
        *cache = Some(fresh);
        Ok(token)
    }
}

impl CloudSigner for BearerTokenSigner {
    fn sign(&self, request: &mut CloudRequest) -> MidgeResult<()> {
        let token = self.get_token()?;
        if !token.is_empty() {
            request
                .headers
                .retain(|(n, _)| !n.eq_ignore_ascii_case("Authorization"));
            request
                .headers
                .push(("Authorization".into(), format!("Bearer {}", token)));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// GCS XML API GOOG1 HMAC signer
// ---------------------------------------------------------------------------

struct Goog1HmacSigner {
    access_id: String,
    secret: String,
}

impl Goog1HmacSigner {
    fn new(access_id: String, secret: String) -> Self {
        Self { access_id, secret }
    }
}

impl CloudSigner for Goog1HmacSigner {
    fn sign(&self, request: &mut CloudRequest) -> MidgeResult<()> {
        let url = url::Url::parse(&request.url).map_err(|err| {
            crate::common::MidgeError::InvalidArgument(format!("url parse: {}", err))
        })?;
        let date = Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        request.headers.retain(|(n, _)| {
            !n.eq_ignore_ascii_case("authorization") && !n.eq_ignore_ascii_case("date")
        });
        request.headers.push(("Date".into(), date.clone()));

        let content_md5 = request
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-md5"))
            .map(|(_, value)| value.as_str())
            .unwrap_or("");
        let content_type = request
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
            .map(|(_, value)| value.as_str())
            .unwrap_or("");
        let resource = if url.path().is_empty() {
            "/"
        } else {
            url.path()
        };
        let string_to_sign = format!(
            "{}\n{}\n{}\n{}\n{}",
            request.method.as_str(),
            content_md5,
            content_type,
            date,
            resource
        );
        let key = BASE64
            .decode(&self.secret)
            .unwrap_or_else(|_| self.secret.as_bytes().to_vec());
        let mut mac = Hmac::<Sha1>::new_from_slice(&key)
            .map_err(|_| crate::common::MidgeError::Internal("gcs hmac init".to_string()))?;
        mac.update(string_to_sign.as_bytes());
        let signature = BASE64.encode(mac.finalize().into_bytes());
        request.headers.push((
            "Authorization".into(),
            format!("GOOG1 {}:{}", self.access_id, signature),
        ));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

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
        let provider = GcsProvider::with_bearer_token(bucket.into(), project.into(), token.into())
            .expect("should create provider with bearer token");

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
        )
        .expect("should create provider with hmac key");

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
        let provider = provider.expect("should create provider with default credential");

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
        let provider = provider.expect("should create provider with empty bucket");

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
        let provider = provider.expect("should create provider with empty project id");

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
        let provider = provider.expect("should create provider with special characters");

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
        let url = backend.list_url("wal/", None);

        // Assert
        assert!(url.contains("prefix=wal%2F"));
    }

    // =========== BearerTokenSigner Tests ===========

    #[test]
    fn should_add_bearer_authorization_header() {
        // Arrange
        let signer = BearerTokenSigner::new_static("ya29.example_token".into());
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
        let signer = BearerTokenSigner::new_static(String::new());
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

    #[test]
    fn should_refresh_expired_authorized_user_token() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let token_url = format!("http://{}/token", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            for token in ["token-one", "token-two"] {
                let (mut stream, _) = listener.accept().unwrap();
                let mut buf = [0_u8; 2048];
                let _ = std::io::Read::read(&mut stream, &mut buf);
                let body = format!(r#"{{"access_token":"{}","expires_in":1}}"#, token);
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                std::io::Write::write_all(&mut stream, response.as_bytes()).unwrap();
            }
        });

        let path = std::env::temp_dir().join(format!(
            "midge_gcs_authorized_user_{}_{}.json",
            std::process::id(),
            current_unix_secs()
        ));
        std::fs::write(
            &path,
            format!(
                r#"{{
                    "type":"authorized_user",
                    "client_id":"client",
                    "client_secret":"secret",
                    "refresh_token":"refresh",
                    "token_uri":"{}"
                }}"#,
                token_url
            ),
        )
        .unwrap();

        let signer =
            BearerTokenSigner::new_provider(GcsTokenProvider::AuthorizedUserFile(path.clone()));
        let mut first = CloudRequest::new(
            Method::GET,
            "https://storage.googleapis.com/storage/v1/b/bucket/o/key".into(),
        );
        signer.sign(&mut first).unwrap();
        let mut second = CloudRequest::new(
            Method::GET,
            "https://storage.googleapis.com/storage/v1/b/bucket/o/key".into(),
        );
        signer.sign(&mut second).unwrap();

        let first_auth = first
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("authorization"))
            .map(|(_, value)| value.as_str());
        let second_auth = second
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("authorization"))
            .map(|(_, value)| value.as_str());

        assert_eq!(first_auth, Some("Bearer token-one"));
        assert_eq!(second_auth, Some("Bearer token-two"));
        server.join().unwrap();
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn should_read_external_account_file_subject_token() {
        let token_path = std::env::temp_dir().join(format!(
            "midge_gcs_subject_token_{}_{}.txt",
            std::process::id(),
            current_unix_secs()
        ));
        std::fs::write(&token_path, "subject-token\n").unwrap();
        let json = json!({
            "type": "external_account",
            "audience": "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
            "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
            "token_url": "https://sts.googleapis.com/v1/token",
            "credential_source": {
                "file": token_path,
                "format": {"type": "text"}
            }
        });

        let token = external_account_subject_token(&json).expect("subject token");

        assert_eq!(token, "subject-token");
        let file = json
            .get("credential_source")
            .and_then(|source| source.get("file"))
            .and_then(|value| value.as_str())
            .unwrap_or_default();
        let _ = std::fs::remove_file(file);
    }

    #[test]
    fn should_exchange_external_account_subject_token_with_sts() {
        let token_path = std::env::temp_dir().join(format!(
            "midge_gcs_sts_subject_token_{}_{}.txt",
            std::process::id(),
            current_unix_secs()
        ));
        std::fs::write(&token_path, "subject-token").unwrap();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let token_url = format!("http://{}/token", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0_u8; 4096];
            let n = std::io::Read::read(&mut stream, &mut buf).unwrap();
            let request = String::from_utf8_lossy(&buf[..n]);
            assert!(request.starts_with("POST /token HTTP/1.1"));
            assert!(request
                .contains("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange"));
            assert!(request.contains("subject_token=subject-token"));
            assert!(request
                .contains("subject_token_type=urn%3Aietf%3Aparams%3Aoauth%3Atoken-type%3Ajwt"));
            let body = r#"{"access_token":"sts-access-token","expires_in":60}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            std::io::Write::write_all(&mut stream, response.as_bytes()).unwrap();
        });

        let json = json!({
            "type": "external_account",
            "audience": "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
            "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
            "token_url": token_url,
            "credential_source": {
                "file": token_path,
                "format": {"type": "text"}
            }
        });

        let token = fetch_external_account_token(&json).expect("STS token exchange");

        assert_eq!(token.access_token, "sts-access-token");
        assert!(token.expires_at.unwrap_or_default() > current_unix_secs());
        let file = json
            .get("credential_source")
            .and_then(|source| source.get("file"))
            .and_then(|value| value.as_str())
            .unwrap_or_default();
        let _ = std::fs::remove_file(file);
        server.join().unwrap();
    }

    #[test]
    fn should_reject_executable_external_account_credentials() {
        let json = json!({
            "type": "external_account",
            "audience": "audience",
            "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
            "token_url": "https://sts.googleapis.com/v1/token",
            "credential_source": {
                "executable": {"command": "gcloud auth print-access-token"}
            }
        });

        let error = external_account_subject_token(&json).expect_err("should reject executable");

        assert!(error.to_string().contains("process execution"));
    }

    #[test]
    fn should_parse_impersonated_access_token() {
        let token = parse_impersonated_access_token_json(
            r#"{"accessToken":"impersonated","expireTime":"2099-01-01T00:00:00Z"}"#,
        )
        .expect("parse impersonation token");

        assert_eq!(token.access_token, "impersonated");
        assert!(token.expires_at.unwrap_or_default() > current_unix_secs());
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

    #[test]
    fn should_extract_gcs_json_list_items_and_next_page_token() {
        let body = r#"{
            "items": [
                {"name": "sst/a.sst"},
                {"name": "sst/b.sst"}
            ],
            "nextPageToken": "next-token"
        }"#;

        let (items, token) = extract_gcs_json_list(body).unwrap();

        assert_eq!(
            items,
            vec!["sst/a.sst".to_string(), "sst/b.sst".to_string()]
        );
        assert_eq!(token, Some("next-token".to_string()));
    }

    #[test]
    fn should_build_gcs_list_url_with_page_token() {
        let backend = GcsBackend::new("my-bucket".into(), make_noop_executor());

        let url = backend.list_url("sst/", Some("next token"));

        assert!(url.contains("prefix=sst%2F"));
        assert!(url.contains("pageToken=next%20token"));
    }

    // =========== Helper ===========

    fn make_noop_executor() -> CloudExecutor {
        CloudExecutor::new(None).expect("Failed to create noop executor in test")
    }
}
