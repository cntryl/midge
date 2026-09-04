//! Google Cloud Storage Provider
//!
//! Production implementation using direct JSON API (no SDK dependency):
//! - `OAuth2` Bearer token authentication
//! - Service-account HMAC key authentication (for S3-interop or simple setups)
//! - Non-blocking callback-based API via `CloudExecutor`
//! - All operations routed through the same `CloudBackend` trait as S3/Azure

use super::super::cloud::{
    CloudBackend, CloudCallback, CloudError, CloudEvent, CloudExecutor, CloudListBudget,
    CloudOutcome, CloudRequest, CloudResponse, CloudSigner, ObjectMetadata,
};
use crate::common::{MidgeError, MidgeResult};
use base64::{
    engine::general_purpose::{STANDARD as BASE64, URL_SAFE_NO_PAD},
    Engine as _,
};
use chrono::Utc;
use hmac::{Hmac, KeyInit, Mac};
use reqwest::Method;
use ring::{
    rand,
    signature::{RsaKeyPair, RSA_PKCS1_SHA256},
};
use serde_json::json;
use sha1::Sha1;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// Credentials
// ---------------------------------------------------------------------------

/// GCS authentication credentials.
#[derive(Debug, Clone)]
#[cfg(test)]
pub enum GcsCredential {
    /// `OAuth2` Bearer token (short-lived, from gcloud CLI or metadata server).
    BearerToken { token: String },
    /// Service-account HMAC key pair (for HMAC-based auth, simpler than `OAuth2`).
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
    #[cfg(test)]
    bucket: String,
    #[cfg(test)]
    project_id: String,
    #[cfg(test)]
    credential: GcsCredential,
}

impl GcsProvider {
    /// Create provider with an `OAuth2` Bearer token.
    #[cfg(test)]
    pub fn with_bearer_token(
        bucket: String,
        project_id: String,
        token: String,
    ) -> MidgeResult<Self> {
        Self::with_bearer_token_endpoint(bucket, project_id, token, None)
    }

    /// Create provider with an `OAuth2` Bearer token and optional endpoint override.
    pub fn with_bearer_token_endpoint(
        bucket: String,
        project_id: String,
        token: String,
        endpoint: Option<String>,
    ) -> MidgeResult<Self> {
        #[cfg(test)]
        let credential = GcsCredential::BearerToken {
            token: token.clone(),
        };
        #[cfg(test)]
        let backend_bucket = bucket.clone();
        #[cfg(not(test))]
        let backend_bucket = bucket;
        #[cfg(not(test))]
        drop(project_id);
        let signer: Option<Arc<dyn CloudSigner>> =
            Some(Arc::new(BearerTokenSigner::new_static(token)));
        let executor = CloudExecutor::new(signer)?;
        let backend = Arc::new(GcsBackend::json(backend_bucket, endpoint, executor));
        Ok(Self {
            backend,
            #[cfg(test)]
            bucket,
            #[cfg(test)]
            project_id,
            #[cfg(test)]
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
        #[cfg(test)]
        let credential = GcsCredential::BearerToken {
            token: "<dynamic>".to_string(),
        };
        #[cfg(test)]
        let backend_bucket = bucket.clone();
        #[cfg(not(test))]
        let backend_bucket = bucket;
        #[cfg(not(test))]
        drop(project_id);
        let signer: Option<Arc<dyn CloudSigner>> =
            Some(Arc::new(BearerTokenSigner::new_provider(provider)));
        let executor = CloudExecutor::new(signer)?;
        let backend = Arc::new(GcsBackend::json(backend_bucket, endpoint, executor));
        Ok(Self {
            backend,
            #[cfg(test)]
            bucket,
            #[cfg(test)]
            project_id,
            #[cfg(test)]
            credential,
        })
    }

    /// Create provider with a service-account HMAC key pair.
    #[cfg(test)]
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
        #[cfg(test)]
        let credential = GcsCredential::HmacKey {
            access_id: access_id.clone(),
            secret: secret.clone(),
        };
        #[cfg(test)]
        let backend_bucket = bucket.clone();
        #[cfg(not(test))]
        let backend_bucket = bucket;
        #[cfg(not(test))]
        drop(project_id);
        let signer: Option<Arc<dyn CloudSigner>> =
            Some(Arc::new(Goog1HmacSigner::new(access_id, secret)));
        let executor = CloudExecutor::new(signer)?;
        let backend = Arc::new(GcsBackend::xml(backend_bucket, endpoint, executor));
        Ok(Self {
            backend,
            #[cfg(test)]
            bucket,
            #[cfg(test)]
            project_id,
            #[cfg(test)]
            credential,
        })
    }

    /// Legacy constructor — creates a provider with an empty bearer token.
    /// Callers should prefer `with_bearer_token` or `with_hmac_key`.
    #[cfg(test)]
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

    /// Expose `project_id` for tests.
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

#[cfg(not(windows))]
fn default_gcloud_adc_file() -> Option<std::path::PathBuf> {
    std::env::var_os("HOME")
        .map(std::path::PathBuf::from)
        .map(|home| home.join(".config/gcloud/application_default_credentials.json"))
}

#[cfg(windows)]
fn default_gcloud_adc_file() -> Option<std::path::PathBuf> {
    std::env::var_os("APPDATA")
        .map(std::path::PathBuf::from)
        .map(|app_data| app_data.join("gcloud/application_default_credentials.json"))
}

#[derive(Clone, Debug)]
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
            "unsupported GCS ADC credential type {other:?}"
        ))),
    }
}

fn fetch_external_account_token(json: &serde_json::Value) -> MidgeResult<CachedGcsToken> {
    if json.get("client_id").is_some() || json.get("client_secret").is_some() {
        return Err(MidgeError::InvalidArgument(
            "GCS external_account client authentication is not supported".to_string(),
        ));
    }
    let audience = required_json_str(json, "audience")?;
    let subject_token_type = required_json_str(json, "subject_token_type")?;
    let token_url = required_json_str(json, "token_url")?;
    let subject_token = external_account_subject_token(json)?;
    let impersonation_url = json
        .get("service_account_impersonation_url")
        .and_then(|value| value.as_str());
    let scope = if impersonation_url.is_some() {
        // IAM Credentials requires the underlying federated token to carry
        // either the IAM or cloud-platform scope before it can mint the final
        // service-account token.
        "https://www.googleapis.com/auth/cloud-platform"
    } else {
        "https://www.googleapis.com/auth/devstorage.full_control"
    };
    let mut fields = vec![
        (
            "grant_type",
            "urn:ietf:params:oauth:grant-type:token-exchange".to_string(),
        ),
        ("audience", audience.to_string()),
        ("scope", scope.to_string()),
        (
            "requested_token_type",
            "urn:ietf:params:oauth:token-type:access_token".to_string(),
        ),
        ("subject_token_type", subject_token_type.to_string()),
        ("subject_token", subject_token),
    ];
    if let Some(user_project) = json
        .get("workforce_pool_user_project")
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
    {
        fields.push((
            "options",
            json!({ "userProject": user_project }).to_string(),
        ));
    }
    let token = post_form_for_access_token(token_url, &fields)?;

    if let Some(url) = impersonation_url {
        let lifetime_seconds = external_account_impersonation_lifetime(json)?;
        impersonate_gcs_service_account(url, &token.access_token, lifetime_seconds)
    } else {
        Ok(token)
    }
}

fn external_account_impersonation_lifetime(json: &serde_json::Value) -> MidgeResult<u64> {
    let configured = json
        .get("service_account_impersonation")
        .and_then(|value| value.get("token_lifetime_seconds"));
    match configured {
        None => Ok(3600),
        Some(value) => value
            .as_u64()
            .filter(|seconds| (600..=43_200).contains(seconds))
            .ok_or_else(|| {
            MidgeError::InvalidArgument(
                "GCS external_account service_account_impersonation.token_lifetime_seconds must be an integer from 600 through 43200"
                    .to_string(),
            )
        }),
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

    if source.get("environment_id").is_some() {
        return Err(MidgeError::InvalidArgument(
            "GCS AWS external_account credential_source is not yet supported; use a file- or URL-sourced workload identity credential"
                .to_string(),
        ));
    }

    let (content, source_description) =
        if let Some(file) = source.get("file").and_then(|value| value.as_str()) {
            let content = std::fs::read_to_string(file).map_err(|error| {
                MidgeError::Internal(format!(
                    "failed to read GCS external_account subject token file '{file}': {error}"
                ))
            })?;
            (content, format!("file '{file}'"))
        } else if let Some(url) = source.get("url").and_then(|value| value.as_str()) {
            (
                fetch_external_account_subject_token_url(source, url)?,
                format!("URL '{url}'"),
            )
        } else {
            return Err(MidgeError::InvalidArgument(
                "GCS external_account ADC requires a supported file or URL credential_source"
                    .to_string(),
            ));
        };

    parse_external_account_subject_token(source, &content, &source_description)
}

fn fetch_external_account_subject_token_url(
    source: &serde_json::Value,
    url: &str,
) -> MidgeResult<String> {
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|error| {
            MidgeError::Internal(format!(
                "GCS external_account subject token client init: {error}"
            ))
        })?;
    let mut request = client.get(url);
    if let Some(headers) = source.get("headers") {
        let headers = headers.as_object().ok_or_else(|| {
            MidgeError::InvalidArgument(
                "GCS external_account credential_source.headers must be an object".to_string(),
            )
        })?;
        for (name, value) in headers {
            let value = value.as_str().ok_or_else(|| {
                MidgeError::InvalidArgument(format!(
                    "GCS external_account credential_source header '{name}' must be a string"
                ))
            })?;
            request = request.header(name.as_str(), value);
        }
    }
    let response = request.send().map_err(|error| {
        MidgeError::Internal(format!(
            "GCS external_account subject token URL request failed: {error}"
        ))
    })?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().unwrap_or_default();
        return Err(MidgeError::Internal(format!(
            "GCS external_account subject token URL failed with status {status}: {body}"
        )));
    }
    response.text().map_err(|error| {
        MidgeError::Internal(format!(
            "GCS external_account subject token URL response body: {error}"
        ))
    })
}

fn parse_external_account_subject_token(
    source: &serde_json::Value,
    content: &str,
    source_description: &str,
) -> MidgeResult<String> {
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
                serde_json::from_str(content).map_err(|error| {
                    MidgeError::InvalidArgument(format!(
                        "failed to parse GCS external_account subject token JSON from {source_description}: {error}"
                    ))
                })?;
            let token = token_json
                .get(field)
                .and_then(|value| value.as_str())
                .ok_or_else(|| {
                    MidgeError::InvalidArgument(format!(
                        "GCS external_account subject token JSON missing string field {field}"
                    ))
                })?
                .trim();
            if token.is_empty() {
                return Err(MidgeError::InvalidArgument(format!(
                    "GCS external_account subject token JSON field {field} from {source_description} is empty"
                )));
            }
            Ok(token.to_string())
        }
        Some("text") | None => {
            let token = content.trim();
            if token.is_empty() {
                Err(MidgeError::InvalidArgument(format!(
                    "GCS external_account subject token from {source_description} is empty"
                )))
            } else {
                Ok(token.to_string())
            }
        }
        Some(format) => Err(MidgeError::InvalidArgument(format!(
            "unsupported GCS external_account subject token format {format}"
        ))),
    }
}

fn impersonate_gcs_service_account(
    url: &str,
    source_access_token: &str,
    lifetime_seconds: u64,
) -> MidgeResult<CachedGcsToken> {
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|error| {
            MidgeError::Internal(format!(
                "GCS service account impersonation client init: {error}"
            ))
        })?;
    let response = client
        .post(url)
        .bearer_auth(source_access_token)
        .json(&json!({
            "scope": ["https://www.googleapis.com/auth/devstorage.full_control"],
            "lifetime": format!("{lifetime_seconds}s"),
        }))
        .send()
        .map_err(|error| {
            MidgeError::Internal(format!(
                "GCS service account impersonation request: {error}"
            ))
        })?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().unwrap_or_default();
        return Err(MidgeError::Internal(format!(
            "GCS service account impersonation failed with status {status}: {body}"
        )));
    }
    let body = response.text().map_err(|error| {
        MidgeError::Internal(format!("GCS service account impersonation body: {error}"))
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
            MidgeError::Internal(format!("failed to encode GCS JWT header: {error}"))
        })?),
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).map_err(|error| {
            MidgeError::Internal(format!("failed to encode GCS JWT claims: {error}"))
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
    let (format, key_der) = parse_service_account_private_key(private_key)?;
    let key_pair = match format {
        ServiceAccountPrivateKeyFormat::Pkcs8 => RsaKeyPair::from_pkcs8(&key_der),
        ServiceAccountPrivateKeyFormat::Pkcs1 => RsaKeyPair::from_der(&key_der),
    }
    .map_err(|_error| {
        MidgeError::InvalidArgument("invalid GCS service account private key".to_string())
    })?;
    let mut signature = vec![0u8; key_pair.public().modulus_len()];
    key_pair
        .sign(
            &RSA_PKCS1_SHA256,
            &rand::SystemRandom::new(),
            signing_input.as_bytes(),
            &mut signature,
        )
        .map_err(|_error| {
            MidgeError::Internal("failed to sign GCS service account JWT".to_string())
        })?;
    Ok(format!(
        "{}.{}",
        signing_input,
        URL_SAFE_NO_PAD.encode(signature)
    ))
}

#[derive(Clone, Copy)]
enum ServiceAccountPrivateKeyFormat {
    Pkcs1,
    Pkcs8,
}

fn parse_service_account_private_key(
    private_key: &str,
) -> MidgeResult<(ServiceAccountPrivateKeyFormat, Vec<u8>)> {
    if private_key.contains("-----BEGIN PRIVATE KEY-----")
        || private_key.contains("-----BEGIN PRIVATE KEY-----\n")
    {
        return Ok((
            ServiceAccountPrivateKeyFormat::Pkcs8,
            decode_pem_block(
                private_key,
                "-----BEGIN PRIVATE KEY-----",
                "-----END PRIVATE KEY-----",
            )?,
        ));
    }

    if private_key.contains("-----BEGIN RSA PRIVATE KEY-----")
        || private_key.contains("-----BEGIN RSA PRIVATE KEY-----\n")
    {
        return Ok((
            ServiceAccountPrivateKeyFormat::Pkcs1,
            decode_pem_block(
                private_key,
                "-----BEGIN RSA PRIVATE KEY-----",
                "-----END RSA PRIVATE KEY-----",
            )?,
        ));
    }

    Err(MidgeError::InvalidArgument(
        "invalid GCS service account private key: expected PKCS#8 or PKCS#1 PEM".to_string(),
    ))
}

fn decode_pem_block(private_key: &str, begin: &str, end: &str) -> MidgeResult<Vec<u8>> {
    let start = private_key.find(begin).ok_or_else(|| {
        MidgeError::InvalidArgument(
            "invalid GCS service account private key: missing PEM header".to_string(),
        )
    })?;
    let finish = private_key.find(end).ok_or_else(|| {
        MidgeError::InvalidArgument(
            "invalid GCS service account private key: missing PEM footer".to_string(),
        )
    })?;
    if finish <= start {
        return Err(MidgeError::InvalidArgument(
            "invalid GCS service account private key: malformed PEM block".to_string(),
        ));
    }

    let payload = &private_key[start + begin.len()..finish];
    let encoded = payload
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<String>();

    BASE64.decode(encoded.as_bytes()).map_err(|error| {
        MidgeError::InvalidArgument(format!(
            "invalid GCS service account private key: base64 decode failure: {error}"
        ))
    })
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
        .map_err(|error| MidgeError::Internal(format!("GCS metadata client init: {error}")))?;
    let response = client
        .get(url)
        .header("Metadata-Flavor", "Google")
        .send()
        .map_err(|error| MidgeError::Internal(format!("GCS metadata token request: {error}")))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().unwrap_or_default();
        return Err(MidgeError::Internal(format!(
            "GCS metadata token request failed with status {status}: {body}"
        )));
    }
    let body = response
        .text()
        .map_err(|error| MidgeError::Internal(format!("GCS metadata response body: {error}")))?;
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
        .map_err(|error| MidgeError::Internal(format!("GCS token client init: {error}")))?;
    let response = client
        .post(url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .map_err(|error| MidgeError::Internal(format!("GCS token request: {error}")))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().unwrap_or_default();
        return Err(MidgeError::Internal(format!(
            "GCS token request failed with status {status}: {body}"
        )));
    }
    let body = response
        .text()
        .map_err(|error| MidgeError::Internal(format!("GCS token response body: {error}")))?;
    parse_access_token_json(&body)
}

fn parse_access_token_json(body: &str) -> MidgeResult<CachedGcsToken> {
    let json: serde_json::Value = serde_json::from_str(body)
        .map_err(|error| MidgeError::Internal(format!("GCS token JSON parse: {error}")))?;
    let access_token = json
        .get("access_token")
        .and_then(|value| value.as_str())
        .ok_or_else(|| MidgeError::Internal("GCS token response missing access_token".into()))?;
    if access_token.trim().is_empty() {
        return Err(MidgeError::Internal(
            "GCS token response contains empty access_token".into(),
        ));
    }
    let ttl = json
        .get("expires_in")
        .and_then(serde_json::Value::as_u64)
        .filter(|ttl| *ttl > 0)
        .ok_or_else(|| {
            MidgeError::Internal("GCS token response must include a positive expires_in".into())
        })?;
    let now = current_unix_secs();
    let expires_at = now
        .checked_add(ttl)
        .filter(|expires_at| *expires_at > now)
        .ok_or_else(|| {
            MidgeError::Internal(
                "GCS token response expires_in does not yield a future expiry".into(),
            )
        })?;
    Ok(CachedGcsToken::expiring(
        access_token.to_string(),
        expires_at,
    ))
}

fn parse_impersonated_access_token_json(body: &str) -> MidgeResult<CachedGcsToken> {
    let json: serde_json::Value = serde_json::from_str(body).map_err(|error| {
        MidgeError::Internal(format!(
            "GCS service account impersonation JSON parse: {error}"
        ))
    })?;
    let access_token = json
        .get("accessToken")
        .and_then(|value| value.as_str())
        .ok_or_else(|| {
            MidgeError::Internal(
                "GCS service account impersonation response missing accessToken".into(),
            )
        })?;
    if access_token.trim().is_empty() {
        return Err(MidgeError::Internal(
            "GCS service account impersonation response contains empty accessToken".into(),
        ));
    }
    let expire_time = json
        .get("expireTime")
        .and_then(|value| value.as_str())
        .ok_or_else(|| {
            MidgeError::Internal(
                "GCS service account impersonation response must include a future RFC3339 expireTime"
                    .into(),
            )
        })?;
    let expires_at = chrono::DateTime::parse_from_rfc3339(expire_time)
        .ok()
        .and_then(|datetime| u64::try_from(datetime.timestamp()).ok())
        .filter(|expires_at| *expires_at > current_unix_secs())
        .ok_or_else(|| {
            MidgeError::Internal(
                "GCS service account impersonation response must include a future RFC3339 expireTime"
                    .into(),
            )
        })?;
    Ok(CachedGcsToken::expiring(
        access_token.to_string(),
        expires_at,
    ))
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
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| MidgeError::InvalidArgument(format!("GCS credential JSON missing {field}")))
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
    #[cfg(test)]
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

    fn bodyless_request(mode: GcsBackendMode, method: Method, url: String) -> CloudRequest {
        let request = CloudRequest::new(method, url);
        if mode == GcsBackendMode::Xml {
            request.with_header("Content-Length", "0")
        } else {
            request
        }
    }

    fn canonical_key(key: &str) -> String {
        key.split('/')
            .map(|segment| urlencoding::encode(segment).into_owned())
            .collect::<Vec<_>>()
            .join("/")
    }

    fn json_object_path(key: &str) -> String {
        // The JSON API models the complete object name as one path parameter.
        // Encoding the complete key prevents slashes and percent escapes from
        // being interpreted as path structure or as a different object name.
        urlencoding::encode(key).into_owned()
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
                Self::canonical_key(key)
            ),
        }
    }

    /// Download URL (media): `https://storage.googleapis.com/download/storage/v1/b/{bucket}/o/{key}?alt=media`
    fn download_url(&self, key: &str) -> String {
        match self.mode {
            GcsBackendMode::Json => format!(
                "{}/download/storage/v1/b/{}/o/{}?alt=media",
                self.endpoint.trim_end_matches('/'),
                self.bucket,
                Self::json_object_path(key)
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
                Self::json_object_path(key)
            ),
            GcsBackendMode::Xml => self.upload_url(key),
        }
    }

    /// List URL: `https://storage.googleapis.com/storage/v1/b/{bucket}/o?prefix={prefix}`
    #[cfg(test)]
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
    budget: CloudListBudget,
    error: Option<CloudError>,
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
        let method = match self.mode {
            GcsBackendMode::Json => Method::POST,
            GcsBackendMode::Xml => Method::PUT,
        };
        let mode = self.mode;
        let conditional_mutation = headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("x-goog-if-generation-match")
                || name.eq_ignore_ascii_case("if-match")
                || (name.eq_ignore_ascii_case("if-none-match") && value.trim() == "*")
        });
        let mut url = self.upload_url(&key);
        let content_length = data.len();
        let mut request = CloudRequest::new(method, String::new())
            .with_body(data)
            .with_header("Content-Type", "application/octet-stream")
            .with_header("Content-Length", content_length.to_string());
        // JSON uploads express mutation preconditions as query parameters. The
        // standard ETag headers are read-only in GCS JSON mode, so never forward
        // them on a write where they could be ignored.
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
            } else if name.eq_ignore_ascii_case("x-goog-if-generation-match") {
                if self.mode == GcsBackendMode::Json {
                    url = append_query_param(&url, "ifGenerationMatch", &value);
                } else {
                    request = request.with_header(name, value);
                }
            } else if name.eq_ignore_ascii_case("x-goog-if-generation-not-match") {
                if self.mode == GcsBackendMode::Json {
                    url = append_query_param(&url, "ifGenerationNotMatch", &value);
                } else {
                    request = request.with_header(name, value);
                }
            } else if name.eq_ignore_ascii_case("if-none-match") && value.trim() == "*" {
                if self.mode == GcsBackendMode::Json {
                    url = append_query_param(&url, "ifGenerationMatch", "0");
                } else {
                    request = request.with_header("x-goog-if-generation-match", "0".to_string());
                }
            } else if name.eq_ignore_ascii_case("if-match")
                || name.eq_ignore_ascii_case("if-none-match")
            {
                let _ = callback.send(CloudEvent::Put {
                    key,
                    result: CloudOutcome::Err(CloudError::Protocol(format!(
                        "GCS PUT cannot enforce {name}; use a generation precondition"
                    ))),
                });
                return;
            } else {
                request = request.with_header(name, value);
            }
        }
        request.url = url;
        let mapper = move |ctx: String, result: MidgeResult<CloudResponse>| match result {
            Ok(resp) if resp.status == 200 => CloudEvent::Put {
                key: ctx,
                result: CloudOutcome::Ok(()),
            },
            Ok(resp) => CloudEvent::Put {
                key: ctx,
                result: CloudOutcome::Err(gcs_response_error(
                    &resp,
                    "GCS PUT",
                    mode,
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
        let mode = self.mode;
        let url = self.download_url(&key);
        let request = Self::bodyless_request(mode, Method::GET, url);
        let mapper = move |ctx: String, result: MidgeResult<CloudResponse>| match result {
            Ok(resp) if resp.status == 200 => CloudEvent::Get {
                key: ctx,
                result: CloudOutcome::Ok(resp.body),
            },
            Ok(resp) => CloudEvent::Get {
                key: ctx,
                result: CloudOutcome::Err(gcs_response_error(&resp, "GCS GET", mode, false)),
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
        let mode = self.mode;
        let request = Self::bodyless_request(mode, Method::GET, self.download_url(&key));
        let mapper = move |ctx: String, result: MidgeResult<CloudResponse>| match result {
            Ok(resp) if resp.status == 200 => {
                let metadata = parse_gcs_media_object_metadata(&resp, mode);
                CloudEvent::GetWithMetadata {
                    key: ctx,
                    result: metadata.map(|metadata| (resp.body, metadata)),
                }
            }
            Ok(resp) => CloudEvent::GetWithMetadata {
                key: ctx,
                result: CloudOutcome::Err(gcs_response_error(&resp, "GCS GET", mode, false)),
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
        let mode = self.mode;
        let url = self.download_url(&key);
        let range = match end {
            Some(e) => format!("bytes={}-{}", start, e.saturating_sub(1)),
            None => format!("bytes={start}-"),
        };
        let request = Self::bodyless_request(mode, Method::GET, url).with_header("Range", range);
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
                result: CloudOutcome::Err(gcs_response_error(&resp, "GCS GET_RANGE", mode, false)),
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
        let mode = self.mode;
        let conditional_mutation = headers.iter().any(|(name, _)| {
            name.eq_ignore_ascii_case("x-goog-if-generation-match")
                || name.eq_ignore_ascii_case("if-match")
        });
        let mut url = self.metadata_url(&key);
        let mut request = Self::bodyless_request(mode, Method::DELETE, String::new());
        for (name, value) in headers {
            if self.mode == GcsBackendMode::Json
                && name.eq_ignore_ascii_case("x-goog-if-generation-match")
            {
                url = append_query_param(&url, "ifGenerationMatch", &value);
            } else if self.mode == GcsBackendMode::Json
                && name.eq_ignore_ascii_case("x-goog-if-generation-not-match")
            {
                url = append_query_param(&url, "ifGenerationNotMatch", &value);
            } else if name.eq_ignore_ascii_case("if-match")
                || name.eq_ignore_ascii_case("if-none-match")
            {
                let _ = callback.send(CloudEvent::Delete {
                    key,
                    result: CloudOutcome::Err(CloudError::Protocol(format!(
                        "GCS DELETE cannot enforce {name}; use a generation precondition"
                    ))),
                });
                return;
            } else {
                request = request.with_header(name, value);
            }
        }
        request.url = url;
        let mapper = move |ctx: String, result: MidgeResult<CloudResponse>| match result {
            Ok(resp) if resp.status == 204 || resp.status == 200 => CloudEvent::Delete {
                key: ctx,
                result: CloudOutcome::Ok(()),
            },
            Ok(resp) if resp.status == 404 => CloudEvent::Delete {
                key: ctx,
                result: CloudOutcome::Ok(()), // idempotent delete
            },
            Ok(resp) => CloudEvent::Delete {
                key: ctx,
                result: CloudOutcome::Err(gcs_response_error(
                    &resp,
                    "GCS DELETE",
                    mode,
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
        let state = GcsListState {
            prefix: prefix.clone(),
            endpoint: self.endpoint.clone(),
            bucket: self.bucket.clone(),
            mode: self.mode,
            page_token: None,
            items: Vec::new(),
            budget: CloudListBudget::default(),
            error: None,
        };
        self.executor.spawn_request_loop(
            state,
            prefix,
            callback,
            |state| Ok(Self::bodyless_request(state.mode, Method::GET, state.url())),
            |state, resp| {
                if resp.status != 200 {
                    state.error = Some(gcs_response_error(&resp, "GCS LIST", state.mode, false));
                    return Ok(false);
                }
                let body = String::from_utf8_lossy(&resp.body);
                let (page_items, page_token) = match state.mode {
                    GcsBackendMode::Json => {
                        let (items, next_page_token) = extract_gcs_json_list(&body)?;
                        (items, next_page_token)
                    }
                    GcsBackendMode::Xml => {
                        super::validate_list_xml(&body, "ListBucketResult")?;
                        let items = extract_xml_tag_values(&body, "Key");
                        let truncated = extract_xml_tag_values(&body, "IsTruncated")
                            .first()
                            .is_some_and(|value| value.eq_ignore_ascii_case("true"));
                        let page_token = extract_xml_tag_values(&body, "NextMarker")
                            .into_iter()
                            .next()
                            .or_else(|| {
                                if truncated {
                                    items.last().cloned()
                                } else {
                                    None
                                }
                            })
                            .filter(|marker| !marker.is_empty());
                        if truncated && page_token.is_none() {
                            return Err(MidgeError::Internal(
                                "GCS XML list response was truncated without NextMarker"
                                    .to_string(),
                            ));
                        }
                        (items, truncated.then_some(page_token).flatten())
                    }
                };
                state
                    .budget
                    .record_page(&page_items, page_token.as_deref())?;
                state.items.extend(page_items);
                state.page_token = page_token;
                Ok(state.page_token.is_some())
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
        // GCS JSON: GET metadata URL. GCS XML: HEAD object URL.
        let url = self.metadata_url(&key);
        let method = match self.mode {
            GcsBackendMode::Json => Method::GET,
            GcsBackendMode::Xml => Method::HEAD,
        };
        let mode = self.mode;
        let request = Self::bodyless_request(mode, method, url);
        let mapper = move |ctx: String, result: MidgeResult<CloudResponse>| match result {
            Ok(resp) if resp.status == 200 => {
                let metadata = match mode {
                    GcsBackendMode::Json => parse_gcs_json_object_metadata(&resp.body),
                    GcsBackendMode::Xml => parse_gcs_xml_object_metadata(&resp),
                };
                CloudEvent::Head {
                    key: ctx,
                    result: metadata,
                }
            }
            Ok(resp) => CloudEvent::Head {
                key: ctx,
                result: CloudOutcome::Err(gcs_response_error(&resp, "GCS HEAD", mode, false)),
            },
            Err(err) => CloudEvent::Head {
                key: ctx,
                result: CloudOutcome::Err(CloudError::from_transport_error(err)),
            },
        };
        self.executor.spawn_request(request, key, callback, mapper);
    }
}

fn gcs_response_error(
    response: &CloudResponse,
    operation: &str,
    mode: GcsBackendMode,
    conditional_mutation: bool,
) -> CloudError {
    let body = String::from_utf8_lossy(&response.body);
    let (reason, message) = match mode {
        GcsBackendMode::Json => serde_json::from_slice::<serde_json::Value>(&response.body)
            .ok()
            .map_or((None, None), |json| {
                let reason = json
                    .pointer("/error/errors/0/reason")
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                let message = json
                    .pointer("/error/message")
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                (reason, message)
            }),
        GcsBackendMode::Xml => (
            extract_xml_tag_values(&body, "Code").into_iter().next(),
            extract_xml_tag_values(&body, "Message").into_iter().next(),
        ),
    };
    let detail = match (reason.as_deref(), message.as_deref()) {
        (Some(reason), Some(message)) => format!("{operation}: {reason}: {message}"),
        (Some(reason), None) => format!("{operation}: {reason}"),
        (None, Some(message)) => format!("{operation}: {message}"),
        (None, None) => operation.to_string(),
    };
    let predicate_failed = reason.as_deref().is_some_and(|reason| {
        reason.eq_ignore_ascii_case("conditionNotMet")
            || reason.eq_ignore_ascii_case("PreconditionFailed")
    });
    if conditional_mutation && response.status == 412 && predicate_failed {
        return CloudError::PreconditionFailed(format!("status {}: {detail}", response.status));
    }

    let policy_failure = reason.as_deref().is_some_and(|reason| {
        [
            "retentionPolicyNotMet",
            "orgPolicyConstraintFailed",
            "objectUnderActiveHold",
            "userProjectMissing",
            "billingNotEnabled",
        ]
        .iter()
        .any(|candidate| reason.eq_ignore_ascii_case(candidate))
    });
    if policy_failure && matches!(response.status, 403 | 409 | 412) {
        CloudError::InvalidRequest(format!("status {}: {detail}", response.status))
    } else {
        CloudError::from_http_status(response.status, detail)
    }
}

#[derive(serde::Deserialize)]
struct GcsJsonObjectMetadata {
    size: String,
    etag: String,
    generation: String,
}

fn parse_gcs_json_object_metadata(body: &[u8]) -> CloudOutcome<ObjectMetadata> {
    let metadata: GcsJsonObjectMetadata = serde_json::from_slice(body).map_err(|error| {
        CloudError::Protocol(format!("GCS JSON metadata response is invalid: {error}"))
    })?;
    let size = metadata.size.parse::<u64>().map_err(|error| {
        CloudError::Protocol(format!(
            "GCS JSON metadata response has invalid size '{}': {error}",
            metadata.size
        ))
    })?;
    let etag = metadata.etag.trim();
    if etag.is_empty() {
        return Err(CloudError::Protocol(
            "GCS JSON metadata response has an empty etag".to_string(),
        ));
    }
    let generation = metadata.generation.trim();
    if generation.is_empty() {
        return Err(CloudError::Protocol(
            "GCS JSON metadata response has an empty generation".to_string(),
        ));
    }
    generation.parse::<u64>().map_err(|error| {
        CloudError::Protocol(format!(
            "GCS JSON metadata response has invalid generation '{generation}': {error}"
        ))
    })?;

    Ok(ObjectMetadata::with_generation(
        size,
        etag.to_string(),
        generation,
    ))
}

fn parse_gcs_media_object_metadata(
    response: &CloudResponse,
    mode: GcsBackendMode,
) -> CloudOutcome<ObjectMetadata> {
    let size = crate::storage::cloud::executor::validate_get_response_length(response)?;
    let etag = required_gcs_metadata_header(response, "etag", "ETag")?;
    let generation = required_gcs_generation(response, mode)?;
    Ok(ObjectMetadata::with_generation(size, etag, generation))
}

fn parse_gcs_xml_object_metadata(response: &CloudResponse) -> CloudOutcome<ObjectMetadata> {
    let size = response
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .ok_or_else(|| {
            CloudError::Protocol("GCS XML metadata response is missing Content-Length".to_string())
        })?
        .1
        .parse::<u64>()
        .map_err(|error| {
            CloudError::Protocol(format!(
                "GCS XML metadata response has invalid Content-Length: {error}"
            ))
        })?;
    let etag = required_gcs_metadata_header(response, "etag", "ETag")?;
    let generation = required_gcs_generation(response, GcsBackendMode::Xml)?;
    Ok(ObjectMetadata::with_generation(size, etag, generation))
}

fn required_gcs_metadata_header(
    response: &CloudResponse,
    header_name: &str,
    display_name: &str,
) -> CloudOutcome<String> {
    response
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(header_name))
        .map(|(_, value)| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            CloudError::Protocol(format!("GCS metadata response is missing {display_name}"))
        })
}

fn required_gcs_generation(response: &CloudResponse, mode: GcsBackendMode) -> CloudOutcome<String> {
    let generation = response
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("x-goog-generation"))
        .map(|(_, value)| value.trim())
        .ok_or_else(|| {
            CloudError::Protocol(format!(
                "GCS {} metadata response is missing x-goog-generation",
                match mode {
                    GcsBackendMode::Json => "JSON",
                    GcsBackendMode::Xml => "XML",
                }
            ))
        })?;
    if generation.is_empty() {
        return Err(CloudError::Protocol(
            "GCS metadata response has an empty x-goog-generation".to_string(),
        ));
    }
    generation.parse::<u64>().map_err(|error| {
        CloudError::Protocol(format!(
            "GCS metadata response has invalid x-goog-generation '{generation}': {error}"
        ))
    })?;
    Ok(generation.to_string())
}

fn extract_gcs_json_list(body: &str) -> MidgeResult<(Vec<String>, Option<String>)> {
    let json: serde_json::Value = serde_json::from_str(body)
        .map_err(|error| MidgeError::Internal(format!("GCS list JSON parse: {error}")))?;
    let object = json.as_object().ok_or_else(|| {
        MidgeError::Internal("GCS list JSON response must be an object".to_string())
    })?;
    let items = match object.get("items") {
        None => Vec::new(),
        Some(value) => value
            .as_array()
            .ok_or_else(|| {
                MidgeError::Internal("GCS list JSON items must be an array".to_string())
            })?
            .iter()
            .map(|item| {
                item.get("name")
                    .and_then(|value| value.as_str())
                    .map(str::to_string)
                    .ok_or_else(|| {
                        MidgeError::Internal(
                            "GCS list JSON item must contain a string name".to_string(),
                        )
                    })
            })
            .collect::<MidgeResult<Vec<_>>>()?,
    };
    let next_page_token = match object.get("nextPageToken") {
        None => None,
        Some(value) => Some(
            value
                .as_str()
                .ok_or_else(|| {
                    MidgeError::Internal("GCS list JSON nextPageToken must be a string".to_string())
                })?
                .to_string(),
        )
        .filter(|value| !value.is_empty()),
    };
    Ok((items, next_page_token))
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
                .push(("Authorization".into(), format!("Bearer {token}")));
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

    fn canonicalized_extension_headers(headers: &[(String, String)]) -> String {
        let mut canonical = std::collections::BTreeMap::<String, Vec<String>>::new();
        for (name, value) in headers {
            let name = name.to_ascii_lowercase();
            if !name.starts_with("x-goog-") {
                continue;
            }
            let value = value.split_whitespace().collect::<Vec<_>>().join(" ");
            canonical.entry(name).or_default().push(value);
        }

        canonical
            .into_iter()
            .fold(String::new(), |mut output, (name, values)| {
                use std::fmt::Write as _;
                writeln!(&mut output, "{name}:{}", values.join(","))
                    .expect("write canonical GCS extension header");
                output
            })
    }

    fn string_to_sign(request: &CloudRequest, date: &str) -> MidgeResult<String> {
        let url = url::Url::parse(&request.url).map_err(|err| {
            crate::common::MidgeError::InvalidArgument(format!("url parse: {err}"))
        })?;
        let content_md5 = request
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-md5"))
            .map_or("", |(_, value)| value.as_str());
        let content_type = request
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
            .map_or("", |(_, value)| value.as_str());
        let resource = if url.path().is_empty() {
            "/"
        } else {
            url.path()
        };
        let extension_headers = Self::canonicalized_extension_headers(&request.headers);
        Ok(format!(
            "{}\n{}\n{}\n{}\n{}{}",
            request.method.as_str(),
            content_md5,
            content_type,
            date,
            extension_headers,
            resource
        ))
    }

    fn sign_with_date(&self, request: &mut CloudRequest, date: &str) -> MidgeResult<()> {
        request.headers.retain(|(name, _)| {
            !name.eq_ignore_ascii_case("authorization") && !name.eq_ignore_ascii_case("date")
        });
        request.headers.push(("Date".into(), date.to_string()));

        let string_to_sign = Self::string_to_sign(request, date)?;
        let key = BASE64
            .decode(&self.secret)
            .unwrap_or_else(|_| self.secret.as_bytes().to_vec());
        let mut mac = Hmac::<Sha1>::new_from_slice(&key)
            .map_err(|_| crate::common::MidgeError::Internal("gcs hmac init".to_string()))?;
        mac.update(string_to_sign.as_bytes());
        let signature = BASE64.encode(mac.finalize().into_bytes());
        request.headers.push((
            "Authorization".into(),
            format!("GOOG1 {}:{signature}", self.access_id),
        ));
        Ok(())
    }
}

impl CloudSigner for Goog1HmacSigner {
    fn sign(&self, request: &mut CloudRequest) -> MidgeResult<()> {
        let date = Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        self.sign_with_date(request, &date)
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
        let identity_headers = &[("etag", "identity"), ("x-goog-generation", "42")];
        // Act
        // Assert
        crate::storage::providers::test_support::assert_get_metadata_length_contract(
            identity_headers,
            |response| parse_gcs_media_object_metadata(response, GcsBackendMode::Json),
        );
    }

    use super::*;
    use crate::storage::providers::test_support::{
        receive_list_result, spawn_recording_http_server, spawn_recording_http_server_with_status,
        spawn_scripted_http_response_server, spawn_scripted_http_server,
    };

    const TEST_BEARER_TOKEN: &str = "gcs-json-test-token";

    fn recording_json_backend(endpoint: String) -> Arc<dyn CloudBackend> {
        GcsProvider::with_bearer_token_endpoint(
            "bucket".into(),
            "project".into(),
            TEST_BEARER_TOKEN.into(),
            Some(endpoint),
        )
        .expect("GCS bearer-token provider")
        .backend()
    }

    fn receive_put_result(receiver: &std::sync::mpsc::Receiver<CloudEvent>) -> CloudOutcome<()> {
        match receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("receive GCS PUT result")
        {
            CloudEvent::Put { result, .. } => result,
            event => panic!("expected GCS PUT event, got {event:?}"),
        }
    }

    fn receive_delete_result(receiver: &std::sync::mpsc::Receiver<CloudEvent>) -> CloudOutcome<()> {
        match receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("receive GCS DELETE result")
        {
            CloudEvent::Delete { result, .. } => result,
            event => panic!("expected GCS DELETE event, got {event:?}"),
        }
    }

    fn receive_head_result(
        receiver: &std::sync::mpsc::Receiver<CloudEvent>,
    ) -> CloudOutcome<ObjectMetadata> {
        match receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("receive GCS HEAD result")
        {
            CloudEvent::Head { result, .. } => result,
            event => panic!("expected GCS HEAD event, got {event:?}"),
        }
    }

    #[test]
    fn should_only_classify_gcs_condition_reason_as_precondition_conflict() {
        // Arrange
        let response = |status, reason: &str| {
            CloudResponse {
            status,
            headers: Vec::new(),
            body: format!(
                r#"{{"error":{{"code":{status},"message":"failure","errors":[{{"reason":"{reason}"}}]}}}}"#
            )
            .into_bytes(),
        }
        };
        let predicate = response(412, "conditionNotMet");
        let policy = response(412, "orgPolicyConstraintFailed");
        let retention = response(403, "retentionPolicyNotMet");

        // Act
        let predicate_error = gcs_response_error(&predicate, "GCS PUT", GcsBackendMode::Json, true);
        let policy_error = gcs_response_error(&policy, "GCS PUT", GcsBackendMode::Json, true);
        let retention_error =
            gcs_response_error(&retention, "GCS DELETE", GcsBackendMode::Json, true);

        // Assert
        assert!(matches!(predicate_error, CloudError::PreconditionFailed(_)));
        assert!(matches!(policy_error, CloudError::InvalidRequest(_)));
        assert!(matches!(retention_error, CloudError::InvalidRequest(_)));
    }

    #[test]
    fn should_preserve_gcs_authorization_error_when_listing() {
        // Arrange
        let server = spawn_scripted_http_response_server(vec![(
            403,
            "application/json".to_string(),
            r#"{"error":{"code":403,"message":"denied","errors":[{"reason":"forbidden"}]}}"#
                .to_string(),
        )]);
        let backend = recording_json_backend(server.endpoint.clone());

        // Act
        let result = receive_list_result(backend.as_ref());
        let requests = server.finish();

        // Assert
        assert!(matches!(
            result,
            CloudOutcome::Err(CloudError::Unauthorized(message))
                if message.contains("forbidden")
        ));
        assert_eq!(requests, 1);
    }

    #[test]
    fn should_reject_redirect_status_when_putting_gcs_object() {
        // Arrange
        let server = spawn_recording_http_server_with_status(302, Vec::new(), Vec::new());
        let backend = recording_json_backend(server.endpoint.clone());
        let (sender, receiver) = std::sync::mpsc::channel();

        // Act
        backend.submit_put("object", b"value".to_vec(), Vec::new(), sender);
        let result = receive_put_result(&receiver);
        let _ = server.finish();

        // Assert
        assert!(matches!(result, CloudOutcome::Err(CloudError::Protocol(_))));
    }

    #[test]
    fn should_use_object_size_given_gcs_json_metadata_response() {
        // Arrange
        let response = r#"{"size":"11","etag":"opaque-etag","generation":"42"}"#;
        assert_ne!(response.len(), 11, "fixture must distinguish response size");
        let server = spawn_scripted_http_server(vec![response.to_string()]);
        let backend = GcsBackend::json(
            "bucket".into(),
            Some(server.endpoint.clone()),
            CloudExecutor::new(None).expect("create cloud executor"),
        );
        let (sender, receiver) = std::sync::mpsc::channel();

        // Act
        backend.submit_head("object", sender);
        let metadata = receive_head_result(&receiver).expect("GCS JSON HEAD should succeed");

        // Assert
        assert_eq!(metadata.size, 11);
        assert_eq!(metadata.etag, "opaque-etag");
        assert_eq!(metadata.generation.as_deref(), Some("42"));
        assert_eq!(server.finish(), 1);
    }

    #[test]
    fn should_use_top_level_fields_when_gcs_json_metadata_contains_shadowing_metadata() {
        // Arrange
        let response = r#"{
            "metadata":{"size":"999","etag":"shadow-etag","generation":"999"},
            "size":"11",
            "etag":"object-etag",
            "generation":"42"
        }"#;
        let server = spawn_scripted_http_server(vec![response.to_string()]);
        let backend = GcsBackend::json(
            "bucket".into(),
            Some(server.endpoint.clone()),
            CloudExecutor::new(None).expect("create cloud executor"),
        );
        let (sender, receiver) = std::sync::mpsc::channel();

        // Act
        backend.submit_head("object", sender);
        let metadata = receive_head_result(&receiver).expect("GCS JSON HEAD should succeed");

        // Assert
        assert_eq!(metadata.size, 11);
        assert_eq!(metadata.etag, "object-etag");
        assert_eq!(metadata.generation.as_deref(), Some("42"));
        assert_eq!(server.finish(), 1);
    }

    #[test]
    fn should_fail_closed_when_gcs_json_metadata_omits_generation() {
        // Arrange
        let response = r#"{"size":"11","etag":"opaque-etag"}"#;
        let server = spawn_scripted_http_server(vec![response.to_string()]);
        let backend = GcsBackend::json(
            "bucket".into(),
            Some(server.endpoint.clone()),
            CloudExecutor::new(None).expect("create cloud executor"),
        );
        let (sender, receiver) = std::sync::mpsc::channel();

        // Act
        backend.submit_head("object", sender);
        let result = receive_head_result(&receiver);

        // Assert
        assert!(matches!(
            result,
            CloudOutcome::Err(CloudError::Protocol(message))
                if message.contains("generation")
        ));
        assert_eq!(server.finish(), 1);
    }

    #[test]
    fn should_fail_closed_when_gcs_json_metadata_has_invalid_size() {
        // Arrange
        let response = r#"{"size":"eleven","etag":"opaque-etag","generation":"42"}"#;
        let server = spawn_scripted_http_server(vec![response.to_string()]);
        let backend = GcsBackend::json(
            "bucket".into(),
            Some(server.endpoint.clone()),
            CloudExecutor::new(None).expect("create cloud executor"),
        );
        let (sender, receiver) = std::sync::mpsc::channel();

        // Act
        backend.submit_head("object", sender);
        let result = receive_head_result(&receiver);

        // Assert
        assert!(matches!(
            result,
            CloudOutcome::Err(CloudError::Protocol(message)) if message.contains("size")
        ));
        assert_eq!(server.finish(), 1);
    }

    #[test]
    fn should_fail_closed_when_gcs_json_media_response_omits_generation() {
        // Arrange
        let server = spawn_recording_http_server(
            vec![("ETag".to_string(), "object-etag".to_string())],
            b"lease".to_vec(),
        );
        let backend = recording_json_backend(server.endpoint.clone());
        let (sender, receiver) = std::sync::mpsc::channel();

        // Act
        backend.submit_get_with_metadata("lease/primary", sender);
        let result = match receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("receive GCS metadata-bearing GET result")
        {
            CloudEvent::GetWithMetadata { result, .. } => result,
            event => panic!("expected GCS metadata-bearing GET event, got {event:?}"),
        };
        let _ = server.finish();

        // Assert
        assert!(matches!(
            result,
            CloudOutcome::Err(CloudError::Protocol(message))
                if message.contains("x-goog-generation")
        ));
    }

    #[test]
    fn should_send_zero_content_length_when_gcs_xml_gets_object() {
        // Arrange
        let server = spawn_recording_http_server(Vec::new(), b"value".to_vec());
        let backend = GcsBackend::xml(
            "bucket".into(),
            Some(server.endpoint.clone()),
            CloudExecutor::new(None).expect("create cloud executor"),
        );
        let (sender, receiver) = std::sync::mpsc::channel();

        // Act
        backend.submit_get("object", sender);
        receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("receive GCS XML GET result");
        let request = server.finish();

        // Assert
        assert_eq!(request.header("content-length"), Some("0"));
    }

    #[test]
    fn should_send_zero_content_length_when_gcs_xml_gets_object_metadata() {
        // Arrange
        let server = spawn_recording_http_server(Vec::new(), b"value".to_vec());
        let backend = GcsBackend::xml(
            "bucket".into(),
            Some(server.endpoint.clone()),
            CloudExecutor::new(None).expect("create cloud executor"),
        );
        let (sender, receiver) = std::sync::mpsc::channel();

        // Act
        backend.submit_get_with_metadata("object", sender);
        receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("receive GCS XML metadata GET result");
        let request = server.finish();

        // Assert
        assert_eq!(request.header("content-length"), Some("0"));
    }

    #[test]
    fn should_send_zero_content_length_when_gcs_xml_reads_object_range() {
        // Arrange
        let server = spawn_recording_http_server(Vec::new(), b"value".to_vec());
        let backend = GcsBackend::xml(
            "bucket".into(),
            Some(server.endpoint.clone()),
            CloudExecutor::new(None).expect("create cloud executor"),
        );
        let (sender, receiver) = std::sync::mpsc::channel();

        // Act
        backend.submit_get_range("object", 0, Some(2), sender);
        receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("receive GCS XML range GET result");
        let request = server.finish();

        // Assert
        assert_eq!(request.header("content-length"), Some("0"));
    }

    #[test]
    fn should_send_zero_content_length_when_gcs_xml_deletes_object() {
        // Arrange
        let server = spawn_recording_http_server(Vec::new(), Vec::new());
        let backend = GcsBackend::xml(
            "bucket".into(),
            Some(server.endpoint.clone()),
            CloudExecutor::new(None).expect("create cloud executor"),
        );
        let (sender, receiver) = std::sync::mpsc::channel();

        // Act
        backend.submit_delete("object", Vec::new(), sender);
        let _ = receive_delete_result(&receiver);
        let request = server.finish();

        // Assert
        assert_eq!(request.header("content-length"), Some("0"));
    }

    #[test]
    fn should_send_zero_content_length_when_gcs_xml_lists_objects() {
        // Arrange
        let response =
            b"<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>".to_vec();
        let server = spawn_recording_http_server(Vec::new(), response);
        let backend = GcsBackend::xml(
            "bucket".into(),
            Some(server.endpoint.clone()),
            CloudExecutor::new(None).expect("create cloud executor"),
        );

        // Act
        let _ = receive_list_result(&backend);
        let request = server.finish();

        // Assert
        assert_eq!(request.header("content-length"), Some("0"));
    }

    #[test]
    fn should_send_zero_content_length_when_gcs_xml_heads_object() {
        // Arrange
        let server = spawn_recording_http_server(Vec::new(), Vec::new());
        let backend = GcsBackend::xml(
            "bucket".into(),
            Some(server.endpoint.clone()),
            CloudExecutor::new(None).expect("create cloud executor"),
        );
        let (sender, receiver) = std::sync::mpsc::channel();

        // Act
        backend.submit_head("object", sender);
        let _ = receive_head_result(&receiver);
        let request = server.finish();

        // Assert
        assert_eq!(request.header("content-length"), Some("0"));
    }

    #[test]
    fn should_fail_closed_when_gcs_xml_head_omits_identity_metadata() {
        // Arrange
        let response = CloudResponse {
            status: 200,
            headers: vec![("Content-Length".to_string(), "5".to_string())],
            body: Vec::new(),
        };

        // Act
        let result = parse_gcs_xml_object_metadata(&response);

        // Assert
        assert!(matches!(
            result,
            CloudOutcome::Err(CloudError::Protocol(message)) if message.contains("ETag")
        ));
    }

    #[test]
    fn should_require_generation_for_gcs_xml_mutation_proofs() {
        // Arrange
        let response = CloudResponse {
            status: 200,
            headers: vec![
                ("Content-Length".to_string(), "5".to_string()),
                ("ETag".to_string(), "object-etag".to_string()),
            ],
            body: Vec::new(),
        };

        // Act
        let result = parse_gcs_xml_object_metadata(&response);

        // Assert
        assert!(matches!(
            result,
            CloudOutcome::Err(CloudError::Protocol(message))
                if message.contains("x-goog-generation")
        ));
    }

    #[test]
    fn should_translate_gcs_xml_conditional_create_to_generation_zero() {
        // Arrange
        let server = spawn_recording_http_server(Vec::new(), Vec::new());
        let backend = GcsBackend::xml(
            "bucket".to_string(),
            Some(server.endpoint.clone()),
            CloudExecutor::new(None).expect("create cloud executor"),
        );
        let (sender, receiver) = std::sync::mpsc::channel();

        // Act
        backend.submit_put(
            "wal/segment",
            b"value".to_vec(),
            vec![("If-None-Match".to_string(), "*".to_string())],
            sender,
        );
        let result = receive_put_result(&receiver);
        let request = server.finish();

        // Assert
        assert!(matches!(result, CloudOutcome::Ok(())));
        assert_eq!(request.header("x-goog-if-generation-match"), Some("0"));
        assert_eq!(request.header("if-none-match"), None);
    }

    #[test]
    fn should_treat_missing_gcs_object_as_success_when_deleting() {
        // Arrange
        let server = spawn_recording_http_server_with_status(404, Vec::new(), Vec::new());
        let backend = recording_json_backend(server.endpoint.clone());
        let (sender, receiver) = std::sync::mpsc::channel();

        // Act
        backend.submit_delete("missing/object", Vec::new(), sender);
        let result = receive_delete_result(&receiver);
        let request = server.finish();

        // Assert
        assert!(matches!(result, CloudOutcome::Ok(())));
        assert_eq!(request.method, "DELETE");
    }

    fn query_value(target: &str, name: &str) -> Option<String> {
        url::Url::parse(&format!("http://recorded{target}"))
            .expect("parse recorded GCS request target")
            .query_pairs()
            .find_map(|(candidate, value)| (candidate == name).then(|| value.into_owned()))
    }

    #[test]
    fn should_translate_generation_match_to_query_when_gcs_json_puts_object() {
        // Arrange
        let server = spawn_recording_http_server(Vec::new(), Vec::new());
        let backend = recording_json_backend(server.endpoint.clone());
        let (sender, receiver) = std::sync::mpsc::channel();

        // Act
        backend.submit_put(
            "lease/primary",
            b"lease".to_vec(),
            vec![("x-goog-if-generation-match".into(), "42".into())],
            sender,
        );
        let result = receive_put_result(&receiver);
        let request = server.finish();

        // Assert
        assert!(matches!(result, CloudOutcome::Ok(())));
        assert_eq!(request.method, "POST");
        assert_eq!(
            query_value(&request.target, "ifGenerationMatch").as_deref(),
            Some("42")
        );
        assert!(request.header("x-goog-if-generation-match").is_none());
        assert_eq!(
            request.header("authorization"),
            Some("Bearer gcs-json-test-token")
        );
    }

    #[test]
    fn should_translate_if_none_match_to_generation_zero_when_gcs_json_puts_object() {
        // Arrange
        let server = spawn_recording_http_server(Vec::new(), Vec::new());
        let backend = recording_json_backend(server.endpoint.clone());
        let (sender, receiver) = std::sync::mpsc::channel();

        // Act
        backend.submit_put(
            "lease/primary",
            b"lease".to_vec(),
            vec![("If-None-Match".into(), "*".into())],
            sender,
        );
        let result = receive_put_result(&receiver);
        let request = server.finish();

        // Assert
        assert!(matches!(result, CloudOutcome::Ok(())));
        assert_eq!(
            query_value(&request.target, "ifGenerationMatch").as_deref(),
            Some("0")
        );
        assert!(request.header("if-none-match").is_none());
        assert_eq!(
            request.header("authorization"),
            Some("Bearer gcs-json-test-token")
        );
    }

    #[test]
    fn should_fail_closed_when_gcs_json_put_has_etag_precondition() {
        // Arrange
        let backend = recording_json_backend("http://127.0.0.1:1".into());
        let (sender, receiver) = std::sync::mpsc::channel();

        // Act
        backend.submit_put(
            "lease/primary",
            b"lease".to_vec(),
            vec![("If-Match".into(), "\"etag\"".into())],
            sender,
        );
        let result = receive_put_result(&receiver);

        // Assert
        assert!(matches!(
            result,
            CloudOutcome::Err(CloudError::Protocol(message))
                if message.contains("use a generation precondition")
        ));
    }

    #[test]
    fn should_translate_generation_match_to_query_when_gcs_json_deletes_object() {
        // Arrange
        let server = spawn_recording_http_server(Vec::new(), Vec::new());
        let backend = recording_json_backend(server.endpoint.clone());
        let (sender, receiver) = std::sync::mpsc::channel();

        // Act
        backend.submit_delete(
            "lease/primary",
            vec![("x-goog-if-generation-match".into(), "42".into())],
            sender,
        );
        let result = receive_delete_result(&receiver);
        let request = server.finish();

        // Assert
        assert!(matches!(result, CloudOutcome::Ok(())));
        assert_eq!(request.method, "DELETE");
        assert_eq!(
            query_value(&request.target, "ifGenerationMatch").as_deref(),
            Some("42")
        );
        assert!(request.header("x-goog-if-generation-match").is_none());
        assert_eq!(
            request.header("authorization"),
            Some("Bearer gcs-json-test-token")
        );
    }

    #[test]
    fn should_translate_generation_not_match_to_query_when_gcs_json_deletes_object() {
        // Arrange
        let server = spawn_recording_http_server(Vec::new(), Vec::new());
        let backend = recording_json_backend(server.endpoint.clone());
        let (sender, receiver) = std::sync::mpsc::channel();

        // Act
        backend.submit_delete(
            "lease/primary",
            vec![("x-goog-if-generation-not-match".into(), "42".into())],
            sender,
        );
        let result = receive_delete_result(&receiver);
        let request = server.finish();

        // Assert
        assert!(matches!(result, CloudOutcome::Ok(())));
        assert_eq!(
            query_value(&request.target, "ifGenerationNotMatch").as_deref(),
            Some("42")
        );
        assert!(request.header("x-goog-if-generation-not-match").is_none());
        assert_eq!(
            request.header("authorization"),
            Some("Bearer gcs-json-test-token")
        );
    }

    #[test]
    fn should_fail_closed_when_gcs_json_delete_has_etag_precondition() {
        // Arrange
        let backend = recording_json_backend("http://127.0.0.1:1".into());
        let (sender, receiver) = std::sync::mpsc::channel();

        // Act
        backend.submit_delete(
            "lease/primary",
            vec![("If-Match".into(), "\"etag\"".into())],
            sender,
        );
        let result = receive_delete_result(&receiver);

        // Assert
        assert!(matches!(
            result,
            CloudOutcome::Err(CloudError::Protocol(message))
                if message.contains("use a generation precondition")
        ));
    }

    #[test]
    fn should_canonicalize_generation_precondition_when_gcs_xml_signs_request() {
        // Arrange
        let signer = Goog1HmacSigner::new("GOOG123".into(), "c2VjcmV0".into());
        let date = "Sat, 08 Aug 2026 12:00:00 GMT";
        let mut request = CloudRequest::new(
            Method::PUT,
            "https://storage.googleapis.com/bucket/lease/primary".into(),
        )
        .with_body(b"lease".to_vec())
        .with_header("Content-Type", "application/octet-stream")
        .with_header("X-Goog-Meta-Owner", " first   holder ")
        .with_header("x-goog-if-generation-match", "42")
        .with_header("x-goog-meta-owner", "second holder");

        // Act
        let string_to_sign = Goog1HmacSigner::string_to_sign(&request, date)
            .expect("build deterministic GOOG1 string-to-sign");
        signer
            .sign_with_date(&mut request, date)
            .expect("sign deterministic GOOG1 request");

        // Assert
        assert_eq!(
            string_to_sign,
            "PUT\n\napplication/octet-stream\nSat, 08 Aug 2026 12:00:00 GMT\nx-goog-if-generation-match:42\nx-goog-meta-owner:first holder,second holder\n/bucket/lease/primary"
        );
        assert_eq!(
            request
                .headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("authorization"))
                .map(|(_, value)| value.as_str()),
            Some("GOOG1 GOOG123:rkS054NuU5pCTrAsV8U7cjpRaOY=")
        );
    }

    #[test]
    fn should_reject_repeated_gcs_json_list_page_token() {
        // Arrange
        let repeated_page = r#"{"items":[{"name":"sst/a.sst"}],"nextPageToken":"repeat"}"#;
        let server = spawn_scripted_http_server(vec![
            repeated_page.to_string(),
            repeated_page.to_string(),
            "{}".to_string(),
        ]);
        let backend = GcsBackend::json(
            "bucket".into(),
            Some(server.endpoint.clone()),
            CloudExecutor::new(None).unwrap(),
        );

        // Act
        let error = receive_list_result(&backend).expect_err("repeated token should fail LIST");

        // Assert
        assert!(
            error.to_string().contains("repeated continuation token"),
            "{error}"
        );
        assert_eq!(server.finish(), 2);
    }

    #[test]
    fn should_reject_repeated_gcs_xml_list_marker() {
        // Arrange
        let repeated_page = "<ListBucketResult><Contents><Key>sst/a.sst</Key></Contents><IsTruncated>true</IsTruncated><NextMarker>repeat</NextMarker></ListBucketResult>";
        let final_page = "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>";
        let server = spawn_scripted_http_server(vec![
            repeated_page.to_string(),
            repeated_page.to_string(),
            final_page.to_string(),
        ]);
        let backend = GcsBackend::xml(
            "bucket".into(),
            Some(server.endpoint.clone()),
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
            GcsCredential::HmacKey { .. } => panic!("Expected BearerToken credential"),
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
            GcsCredential::BearerToken { .. } => panic!("Expected HmacKey credential"),
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
        assert_eq!(
            url,
            "https://storage.googleapis.com/download/storage/v1/b/my-bucket/o/path%2Fto%2Fobject?alt=media"
        );
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
    fn should_fully_encode_reserved_gcs_json_object_path() {
        // Arrange
        let backend = GcsBackend::new("my-bucket".into(), make_noop_executor());

        // Act
        let url = backend.metadata_url("wal/a%2Fb +?");

        // Assert
        assert_eq!(
            url,
            "https://storage.googleapis.com/storage/v1/b/my-bucket/o/wal%2Fa%252Fb%20%2B%3F"
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
        // Arrange
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let token_url = format!("http://{}/token", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            for token in ["token-one", "token-two"] {
                let (mut stream, _) = listener.accept().unwrap();
                let mut buf = [0_u8; 2048];
                let _ = std::io::Read::read(&mut stream, &mut buf);
                let body = format!(r#"{{"access_token":"{token}","expires_in":1}}"#);
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
                    "token_uri":"{token_url}"
                }}"#
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

        // Act
        // Assert
        assert_eq!(first_auth, Some("Bearer token-one"));
        assert_eq!(second_auth, Some("Bearer token-two"));
        server.join().unwrap();
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn should_read_external_account_file_subject_token() {
        // Arrange
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

        // Act
        // Assert
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
        // Arrange
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
            // Act
            // Assert
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
    fn should_send_workforce_user_project_in_sts_options() {
        // Arrange
        let token_path = std::env::temp_dir().join(format!(
            "midge_gcs_workforce_subject_token_{}_{}.txt",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::write(&token_path, "subject-token").expect("write workforce token");
        let (token_url, server) = spawn_recording_json_response_server(
            r#"{"access_token":"sts-access-token","expires_in":3600}"#,
        );
        let credentials = json!({
            "type": "external_account",
            "audience": "//iam.googleapis.com/locations/global/workforcePools/pool/providers/provider",
            "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
            "token_url": token_url,
            "workforce_pool_user_project": "123456789",
            "credential_source": {
                "file": token_path,
                "format": {"type": "text"}
            }
        });

        // Act
        let token = fetch_external_account_token(&credentials).expect("workforce token exchange");
        let request = server.join().expect("STS server");
        let _ = std::fs::remove_file(
            credentials
                .pointer("/credential_source/file")
                .and_then(|value| value.as_str())
                .unwrap_or_default(),
        );

        // Assert
        assert_eq!(token.access_token, "sts-access-token");
        assert!(request.contains("options=%7B%22userProject%22%3A%22123456789%22%7D"));
    }

    #[test]
    fn should_follow_external_account_impersonation_contract() {
        // Arrange
        let token_path = std::env::temp_dir().join(format!(
            "midge_gcs_impersonation_subject_token_{}_{}.txt",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::write(&token_path, "subject-token").unwrap();
        let (token_url, token_server) = spawn_recording_json_response_server(
            r#"{"access_token":"sts-access-token","expires_in":60}"#,
        );
        let (impersonation_url, impersonation_server) = spawn_recording_json_response_server(
            r#"{"accessToken":"impersonated","expireTime":"2099-01-01T00:00:00Z"}"#,
        );
        let json = json!({
            "type": "external_account",
            "audience": "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
            "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
            "token_url": token_url,
            "service_account_impersonation_url": impersonation_url,
            "service_account_impersonation": {"token_lifetime_seconds": 900},
            "credential_source": {
                "file": token_path,
                "format": {"type": "text"}
            }
        });

        // Act
        let token = fetch_external_account_token(&json).expect("impersonated token exchange");
        let sts_request = token_server.join().expect("STS server");
        let impersonation_request = impersonation_server.join().expect("impersonation server");

        // Assert
        assert_eq!(token.access_token, "impersonated");
        assert!(
            sts_request.contains("scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fcloud-platform")
        );
        assert!(impersonation_request
            .to_ascii_lowercase()
            .contains("authorization: bearer sts-access-token"));
        assert!(impersonation_request.contains(r#""lifetime":"900s""#));
        assert!(impersonation_request
            .contains(r#""scope":["https://www.googleapis.com/auth/devstorage.full_control"]"#));
        let file = json
            .get("credential_source")
            .and_then(|source| source.get("file"))
            .and_then(|value| value.as_str())
            .unwrap_or_default();
        let _ = std::fs::remove_file(file);
    }

    #[test]
    fn should_read_url_sourced_external_account_subject_token_with_headers() {
        // Arrange
        let (url, server) =
            spawn_recording_json_response_server(r#"{"access_token":"azure-subject-token"}"#);
        let json = json!({
            "credential_source": {
                "url": url,
                "headers": {"Metadata": "True"},
                "format": {
                    "type": "json",
                    "subject_token_field_name": "access_token"
                }
            }
        });

        // Act
        let token = external_account_subject_token(&json).expect("URL subject token");
        let request = server.join().expect("URL credential server");

        // Assert
        assert_eq!(token, "azure-subject-token");
        assert!(request.to_ascii_lowercase().contains("metadata: true"));
    }

    #[test]
    fn should_reject_empty_json_external_account_subject_token() {
        // Arrange
        let source = json!({
            "format": {
                "type": "json",
                "subject_token_field_name": "access_token"
            }
        });

        // Act
        let error = parse_external_account_subject_token(
            &source,
            r#"{"access_token":"   "}"#,
            "test credential source",
        )
        .expect_err("empty JSON subject token must fail closed");

        // Assert
        assert!(error.to_string().contains("is empty"));
    }

    #[test]
    fn should_reject_aws_external_account_before_treating_metadata_url_as_subject_token() {
        // Arrange
        let json = json!({
            "credential_source": {
                "environment_id": "aws1",
                "url": "http://127.0.0.1:1/latest/meta-data/iam/security-credentials",
                "regional_cred_verification_url": "https://sts.{region}.amazonaws.com?Action=GetCallerIdentity&Version=2011-06-15"
            }
        });

        // Act
        let error = external_account_subject_token(&json).expect_err("AWS source is unsupported");

        // Assert
        assert!(error.to_string().contains("AWS external_account"));
    }

    #[test]
    fn should_reject_executable_external_account_credentials() {
        // Arrange
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

        // Act
        // Assert
        assert!(error.to_string().contains("process execution"));
    }

    #[test]
    fn should_reject_empty_access_token_in_token_response() {
        // Arrange
        let body = r#"{"access_token":"   ","expires_in":3600}"#;

        // Act
        let error = parse_access_token_json(body).expect_err("empty token must fail closed");

        // Assert
        assert!(error.to_string().contains("empty access_token"));
    }

    #[test]
    fn should_reject_missing_or_invalid_expiry_in_token_response() {
        // Arrange
        let bodies = [
            r#"{"access_token":"token"}"#,
            r#"{"access_token":"token","expires_in":0}"#,
            r#"{"access_token":"token","expires_in":-1}"#,
            r#"{"access_token":"token","expires_in":"3600"}"#,
        ];

        // Act
        let errors = bodies
            .into_iter()
            .map(|body| parse_access_token_json(body).expect_err("invalid expiry must fail closed"))
            .collect::<Vec<_>>();

        // Assert
        assert!(errors
            .iter()
            .all(|error| error.to_string().contains("positive expires_in")));
    }

    #[test]
    fn should_parse_access_token_with_future_expiry() {
        // Arrange
        let before = current_unix_secs();
        let body = r#"{"access_token":"token","expires_in":3600}"#;

        // Act
        let token = parse_access_token_json(body).expect("valid token response");

        // Assert
        assert_eq!(token.access_token, "token");
        assert!(token.expires_at.unwrap_or_default() > before);
    }

    #[test]
    fn should_reject_expiry_that_cannot_yield_future_token_expiration() {
        // Arrange
        let body = r#"{"access_token":"token","expires_in":18446744073709551615}"#;

        // Act
        let error = parse_access_token_json(body).expect_err("overflowing expiry must fail closed");

        // Assert
        assert!(error.to_string().contains("does not yield a future expiry"));
    }

    #[test]
    fn should_preserve_non_expiring_static_bearer_token() {
        // Arrange
        let provider = GcsTokenProvider::StaticBearer("emulator-token".into());

        // Act
        let token = provider.fetch_token().expect("static bearer token");

        // Assert
        assert_eq!(token.access_token, "emulator-token");
        assert_eq!(token.expires_at, None);
    }

    #[test]
    fn should_parse_impersonated_access_token() {
        // Arrange
        let token = parse_impersonated_access_token_json(
            r#"{"accessToken":"impersonated","expireTime":"2099-01-01T00:00:00Z"}"#,
        )
        .expect("parse impersonation token");

        // Act
        // Assert
        assert_eq!(token.access_token, "impersonated");
        assert!(token.expires_at.unwrap_or_default() > current_unix_secs());
    }

    #[test]
    fn should_reject_empty_access_token_in_impersonation_response() {
        // Arrange
        let body = r#"{"accessToken":"   ","expireTime":"2099-01-01T00:00:00Z"}"#;

        // Act
        let error = parse_impersonated_access_token_json(body)
            .expect_err("empty impersonated token must fail closed");

        // Assert
        assert!(error.to_string().contains("empty accessToken"));
    }

    #[test]
    fn should_reject_missing_malformed_or_expired_impersonation_expiry() {
        // Arrange
        let bodies = [
            r#"{"accessToken":"token"}"#,
            r#"{"accessToken":"token","expireTime":"not-a-timestamp"}"#,
            r#"{"accessToken":"token","expireTime":"2000-01-01T00:00:00Z"}"#,
        ];

        // Act
        let errors = bodies
            .into_iter()
            .map(|body| {
                parse_impersonated_access_token_json(body)
                    .expect_err("invalid impersonation expiry must fail closed")
            })
            .collect::<Vec<_>>();

        // Assert
        assert!(errors
            .iter()
            .all(|error| error.to_string().contains("future RFC3339 expireTime")));
    }

    #[test]
    fn should_extract_gcs_json_list_metadata_when_next_page_token_present() {
        // Arrange
        let body = r#"{
            "items": [
                {"name": "sst/a.sst"},
                {"name": "sst/b.sst"}
            ],
            "nextPageToken": "next-token"
        }"#;

        let (items, token) = extract_gcs_json_list(body).unwrap();

        // Act
        // Assert
        assert_eq!(
            items,
            vec!["sst/a.sst".to_string(), "sst/b.sst".to_string()]
        );
        assert_eq!(token, Some("next-token".to_string()));
    }

    #[test]
    fn should_build_gcs_list_url_with_page_token() {
        // Arrange
        let backend = GcsBackend::new("my-bucket".into(), make_noop_executor());

        let url = backend.list_url("sst/", Some("next token"));

        // Act
        // Assert
        assert!(url.contains("prefix=sst%2F"));
        assert!(url.contains("pageToken=next%20token"));
    }

    // =========== Helper ===========

    fn make_noop_executor() -> CloudExecutor {
        CloudExecutor::new(None).expect("Failed to create noop executor in test")
    }

    fn spawn_recording_json_response_server(
        response_body: &'static str,
    ) -> (String, std::thread::JoinHandle<String>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind token server");
        let endpoint = format!(
            "http://{}",
            listener.local_addr().expect("token server address")
        );
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept token request");
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(2)))
                .expect("configure token request timeout");

            let mut request = Vec::new();
            let mut chunk = [0_u8; 4096];
            let mut expected_length = None;
            loop {
                let read =
                    std::io::Read::read(&mut stream, &mut chunk).expect("read token request");
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..read]);
                if expected_length.is_none() {
                    if let Some(header_end) =
                        request.windows(4).position(|window| window == b"\r\n\r\n")
                    {
                        let headers = String::from_utf8_lossy(&request[..header_end]);
                        let content_length = headers.lines().find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        });
                        expected_length = Some(header_end + 4 + content_length.unwrap_or(0));
                    }
                }
                if expected_length.is_some_and(|length| request.len() >= length) {
                    break;
                }
            }

            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            std::io::Write::write_all(&mut stream, response.as_bytes())
                .expect("write token response");
            String::from_utf8(request).expect("token request must be UTF-8")
        });
        (endpoint, handle)
    }
}
