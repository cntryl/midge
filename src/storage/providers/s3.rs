//! Generic S3-compatible provider implementation
//!
//! Supports any S3-compatible storage:
//! - AWS S3 (with `SigV4` credential handling)
//! - Wasabi (simple access key/secret)
//! - `MinIO` (local or cloud)
//! - Oracle Cloud Infrastructure (OCI S3 compatibility)
//! - Any other S3-compatible service

use super::super::cloud::{
    CloudBackend, CloudExecutor, CloudListBudget, CloudRequest, CloudResponse, CloudSigner,
    ObjectMetadata,
};
use super::super::cloud::{CloudCallback, CloudError, CloudEvent, CloudOutcome};
use crate::common::{MidgeError, MidgeResult};
use chrono::Utc;
use hex;
use hmac::{Hmac, KeyInit, Mac};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use reqwest::blocking::Client;
use reqwest::Method;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use url::{Host, Url};
use urlencoding::encode;

// SigV4's UriEncode contract preserves only RFC 3986 unreserved bytes. Object
// key separators are handled before this set is applied so `/` remains the one
// intentional exception.
const S3_PATH_SEGMENT_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'!')
    .add(b'"')
    .add(b'#')
    .add(b'$')
    .add(b'%')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b',')
    .add(b'/')
    .add(b':')
    .add(b';')
    .add(b'<')
    .add(b'=')
    .add(b'>')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

fn aws_dns_suffix(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "amazonaws.com.cn"
    } else {
        "amazonaws.com"
    }
}

fn aws_sts_endpoint(region: &str) -> String {
    format!("https://sts.{region}.{}/", aws_dns_suffix(region))
}

fn aws_environment_flag(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| value.trim().eq_ignore_ascii_case("true"))
}

fn imds_endpoint() -> MidgeResult<String> {
    if let Ok(endpoint) = std::env::var("AWS_EC2_METADATA_SERVICE_ENDPOINT") {
        let endpoint = endpoint.trim_end_matches('/');
        let parsed = Url::parse(endpoint).map_err(|error| {
            MidgeError::InvalidArgument(format!("invalid IMDS endpoint: {error}"))
        })?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
            return Err(MidgeError::InvalidArgument(
                "IMDS endpoint must be an absolute HTTP(S) URL".into(),
            ));
        }
        return Ok(endpoint.to_string());
    }

    match std::env::var("AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE") {
        Ok(mode) if mode.eq_ignore_ascii_case("IPv6") => Ok("http://[fd00:ec2::254]".into()),
        Ok(mode) if !mode.eq_ignore_ascii_case("IPv4") => Err(MidgeError::InvalidArgument(
            format!("invalid AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE '{mode}'"),
        )),
        Ok(_) | Err(_) => Ok("http://169.254.169.254".into()),
    }
}

struct ContainerCredentialEndpoint {
    url: String,
    pinned_resolution: Option<(String, Vec<SocketAddr>)>,
}

fn container_credential_endpoint() -> MidgeResult<ContainerCredentialEndpoint> {
    if let Ok(relative_uri) = std::env::var("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") {
        if !relative_uri.starts_with('/')
            || relative_uri.starts_with("//")
            || relative_uri.contains('\\')
        {
            return Err(MidgeError::InvalidArgument(
                "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI must be an absolute path".to_string(),
            ));
        }
        let url = Url::parse(&format!("http://169.254.170.2{relative_uri}")).map_err(|error| {
            MidgeError::InvalidArgument(format!(
                "invalid AWS_CONTAINER_CREDENTIALS_RELATIVE_URI: {error}"
            ))
        })?;
        if url.fragment().is_some() || url.host_str() != Some("169.254.170.2") {
            return Err(MidgeError::InvalidArgument(
                "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI must remain on the ECS credential host"
                    .to_string(),
            ));
        }
        return Ok(ContainerCredentialEndpoint {
            url: url.to_string(),
            pinned_resolution: None,
        });
    }

    let full_uri = std::env::var("AWS_CONTAINER_CREDENTIALS_FULL_URI").map_err(|_| {
        MidgeError::InvalidArgument("ECS credential endpoint variables are not set".into())
    })?;
    validate_container_full_uri(&full_uri)
}

fn validate_container_full_uri(full_uri: &str) -> MidgeResult<ContainerCredentialEndpoint> {
    let url = Url::parse(full_uri).map_err(|error| {
        MidgeError::InvalidArgument(format!(
            "invalid AWS_CONTAINER_CREDENTIALS_FULL_URI: {error}"
        ))
    })?;
    if url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(MidgeError::InvalidArgument(
            "AWS_CONTAINER_CREDENTIALS_FULL_URI must be an absolute URL without credentials or a fragment"
                .to_string(),
        ));
    }
    if url.scheme() == "https" {
        return Ok(ContainerCredentialEndpoint {
            url: url.to_string(),
            pinned_resolution: None,
        });
    }
    if url.scheme() != "http" {
        return Err(MidgeError::InvalidArgument(
            "AWS_CONTAINER_CREDENTIALS_FULL_URI must use HTTP or HTTPS".to_string(),
        ));
    }

    let host = url.host().expect("checked container credential host");
    let literal_ip = match &host {
        Host::Ipv4(ip) => Some(IpAddr::V4(*ip)),
        Host::Ipv6(ip) => Some(IpAddr::V6(*ip)),
        Host::Domain(_) => None,
    };
    if let Some(ip) = literal_ip {
        if !allowed_container_credential_ip(ip) {
            return Err(MidgeError::InvalidArgument(
                "insecure AWS_CONTAINER_CREDENTIALS_FULL_URI host is not an allowed local credential endpoint"
                    .to_string(),
            ));
        }
        return Ok(ContainerCredentialEndpoint {
            url: url.to_string(),
            pinned_resolution: None,
        });
    }

    let Host::Domain(host) = host else {
        unreachable!("IP container credential hosts returned above");
    };

    let port = url.port_or_known_default().ok_or_else(|| {
        MidgeError::InvalidArgument("container credential endpoint has no usable port".to_string())
    })?;
    let addresses = (host, port)
        .to_socket_addrs()
        .map_err(|error| {
            MidgeError::InvalidArgument(format!(
                "failed to resolve AWS_CONTAINER_CREDENTIALS_FULL_URI host: {error}"
            ))
        })?
        .collect::<Vec<_>>();
    if addresses.is_empty()
        || addresses
            .iter()
            .any(|address| !allowed_container_credential_ip(address.ip()))
    {
        return Err(MidgeError::InvalidArgument(
            "insecure AWS_CONTAINER_CREDENTIALS_FULL_URI host did not resolve exclusively to allowed local credential addresses"
                .to_string(),
        ));
    }

    Ok(ContainerCredentialEndpoint {
        url: url.to_string(),
        pinned_resolution: Some((host.to_string(), addresses)),
    })
}

fn allowed_container_credential_ip(ip: IpAddr) -> bool {
    ip.is_loopback()
        || ip == IpAddr::V4(std::net::Ipv4Addr::new(169, 254, 170, 2))
        || ip == IpAddr::V4(std::net::Ipv4Addr::new(169, 254, 170, 23))
        || ip == IpAddr::V6("fd00:ec2::23".parse().expect("valid EKS IPv6 address"))
}

#[derive(Clone)]
pub struct AwsCredentials {
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    pub session_token: Option<String>,
}

impl AwsCredentials {
    #[cfg(test)]
    pub fn new(access_key: String, secret_key: String, region: String) -> Self {
        Self {
            access_key,
            secret_key,
            region,
            session_token: None,
        }
    }
}

#[derive(Clone)]
struct CachedAwsCredentials {
    creds: AwsCredentials,
    expires_at: Option<u64>,
}

impl CachedAwsCredentials {
    fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(expiry) => {
                let now = current_unix_secs();
                now >= expiry.saturating_sub(300)
            }
            None => false,
        }
    }
}

struct DefaultAwsCredentialProvider {
    region: String,
    cache: Arc<Mutex<Option<CachedAwsCredentials>>>,
}

impl DefaultAwsCredentialProvider {
    fn new(region: String) -> Self {
        Self {
            region,
            cache: Arc::new(Mutex::new(None)),
        }
    }

    fn resolve(&self) -> MidgeResult<AwsCredentials> {
        {
            let cache_guard = self
                .cache
                .lock()
                .map_err(|_| MidgeError::Internal("aws credential cache mutex poisoned".into()))?;
            if let Some(cached) = cache_guard.as_ref() {
                if !cached.is_expired() {
                    return Ok(cached.creds.clone());
                }
            }
        }

        let fresh = self.resolve_fresh()?;
        let creds = fresh.creds.clone();
        let mut cache_guard = self
            .cache
            .lock()
            .map_err(|_| MidgeError::Internal("aws credential cache mutex poisoned".into()))?;
        *cache_guard = Some(fresh);
        Ok(creds)
    }

    fn resolve_fresh(&self) -> MidgeResult<CachedAwsCredentials> {
        if let Some(creds) = self.resolve_env_credentials() {
            return Ok(CachedAwsCredentials {
                creds,
                expires_at: None,
            });
        }

        if let Some(creds) = self.resolve_profile_credentials()? {
            return Ok(CachedAwsCredentials {
                creds,
                expires_at: None,
            });
        }

        if std::env::var_os("AWS_ROLE_ARN").is_some()
            || std::env::var_os("AWS_WEB_IDENTITY_TOKEN_FILE").is_some()
        {
            return self.resolve_web_identity();
        }

        if std::env::var_os("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI").is_some()
            || std::env::var_os("AWS_CONTAINER_CREDENTIALS_FULL_URI").is_some()
        {
            return self.resolve_ecs_task_role();
        }

        self.resolve_ec2_imds()
    }

    fn resolve_env_credentials(&self) -> Option<AwsCredentials> {
        let access_key = std::env::var("AWS_ACCESS_KEY_ID")
            .or_else(|_| std::env::var("AWS_ACCESS_KEY"))
            .ok()
            .filter(|value| !value.trim().is_empty())?;
        let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
            .or_else(|_| std::env::var("AWS_SECRET_KEY"))
            .ok()
            .filter(|value| !value.trim().is_empty())?;
        let session_token = std::env::var("AWS_SESSION_TOKEN")
            .ok()
            .or_else(|| std::env::var("AWS_SECURITY_TOKEN").ok())
            .filter(|value| !value.trim().is_empty());

        Some(AwsCredentials {
            access_key,
            secret_key,
            region: self.region.clone(),
            session_token,
        })
    }

    fn resolve_profile_credentials(&self) -> MidgeResult<Option<AwsCredentials>> {
        let profile = std::env::var("AWS_PROFILE").unwrap_or_else(|_| "default".to_string());
        let credentials_file = std::env::var("AWS_SHARED_CREDENTIALS_FILE")
            .ok()
            .map(std::path::PathBuf::from)
            .or_else(|| default_home_file(".aws/credentials"));
        let config_file = std::env::var("AWS_CONFIG_FILE")
            .ok()
            .map(std::path::PathBuf::from)
            .or_else(|| default_home_file(".aws/config"));

        if let Some(path) = credentials_file {
            if let Some(creds) = read_aws_profile_credentials(&path, &profile, &self.region, false)?
            {
                return Ok(Some(creds));
            }
        }

        if let Some(path) = config_file {
            if let Some(creds) = read_aws_profile_credentials(&path, &profile, &self.region, true)?
            {
                return Ok(Some(creds));
            }
        }

        Ok(None)
    }

    fn resolve_web_identity(&self) -> MidgeResult<CachedAwsCredentials> {
        let role_arn = std::env::var("AWS_ROLE_ARN")
            .map_err(|_| MidgeError::InvalidArgument("AWS_ROLE_ARN is not set".into()))?;
        let token_file = std::env::var("AWS_WEB_IDENTITY_TOKEN_FILE").map_err(|_| {
            MidgeError::InvalidArgument("AWS_WEB_IDENTITY_TOKEN_FILE is not set".into())
        })?;
        let token = std::fs::read_to_string(&token_file).map_err(|e| {
            MidgeError::Internal(format!(
                "failed to read AWS_WEB_IDENTITY_TOKEN_FILE '{token_file}': {e}"
            ))
        })?;
        let token = token.trim();
        if token.is_empty() {
            return Err(MidgeError::InvalidArgument(
                "AWS_WEB_IDENTITY_TOKEN_FILE is empty".into(),
            ));
        }

        let session_name = std::env::var("AWS_ROLE_SESSION_NAME")
            .unwrap_or_else(|_| format!("midge-{}", current_unix_secs()));
        let endpoint = aws_sts_endpoint(&self.region);
        let body = format!(
            "Action=AssumeRoleWithWebIdentity&Version=2011-06-15&RoleArn={}&RoleSessionName={}&WebIdentityToken={}",
            encode(&role_arn),
            encode(&session_name),
            encode(token)
        );

        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| MidgeError::Internal(format!("sts client init failed: {e}")))?;
        let resp = client
            .post(&endpoint)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .map_err(|e| MidgeError::Internal(format!("sts assume role call failed: {e}")))?;

        if !resp.status().is_success() {
            return Err(MidgeError::Internal(format!(
                "sts assume role failed with status {}",
                resp.status()
            )));
        }

        let xml = resp
            .text()
            .map_err(|e| MidgeError::Internal(format!("failed to read sts response: {e}")))?;

        let access_key = required_temporary_credential_field(
            extract_xml_tag(&xml, "AccessKeyId")?,
            "AccessKeyId",
            "STS web identity",
        )?;
        let secret_key = required_temporary_credential_field(
            extract_xml_tag(&xml, "SecretAccessKey")?,
            "SecretAccessKey",
            "STS web identity",
        )?;
        let session_token = required_temporary_credential_field(
            extract_xml_tag(&xml, "SessionToken")?,
            "SessionToken",
            "STS web identity",
        )?;
        let expiry = required_temporary_credential_expiry(
            &extract_xml_tag(&xml, "Expiration")?,
            "STS web identity",
        )?;

        Ok(CachedAwsCredentials {
            creds: AwsCredentials {
                access_key,
                secret_key,
                region: self.region.clone(),
                session_token: Some(session_token),
            },
            expires_at: Some(expiry),
        })
    }

    fn resolve_ecs_task_role(&self) -> MidgeResult<CachedAwsCredentials> {
        let endpoint = container_credential_endpoint()?;
        let mut client_builder = Client::builder().timeout(std::time::Duration::from_secs(5));
        if let Some((host, addresses)) = endpoint.pinned_resolution.as_ref() {
            client_builder = client_builder.resolve_to_addrs(host, addresses);
        }
        let client = client_builder
            .build()
            .map_err(|e| MidgeError::Internal(format!("ecs client init failed: {e}")))?;

        let mut request = client.get(&endpoint.url);
        if let Ok(token_file) = std::env::var("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE") {
            let token = std::fs::read_to_string(&token_file)
                .map_err(|e| MidgeError::Internal(format!("failed to read token file: {e}")))?;
            request = request.header("Authorization", token.trim());
        } else if let Ok(auth_token) = std::env::var("AWS_CONTAINER_AUTHORIZATION_TOKEN") {
            request = request.header("Authorization", auth_token);
        }

        let resp = request
            .send()
            .map_err(|e| MidgeError::Internal(format!("ecs credential fetch failed: {e}")))?;
        if !resp.status().is_success() {
            return Err(MidgeError::Internal(format!(
                "ecs credential endpoint returned {}",
                resp.status()
            )));
        }
        let text = resp
            .text()
            .map_err(|e| MidgeError::Internal(format!("failed to read ecs response: {e}")))?;
        self.parse_json_credentials(&text)
    }

    fn resolve_ec2_imds(&self) -> MidgeResult<CachedAwsCredentials> {
        if aws_environment_flag("AWS_EC2_METADATA_DISABLED") {
            return Err(MidgeError::InvalidArgument(
                "EC2 IMDS credential resolution is disabled by AWS_EC2_METADATA_DISABLED".into(),
            ));
        }

        let endpoint = imds_endpoint()?;
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(3))
            .build()
            .map_err(|e| MidgeError::Internal(format!("imds client init failed: {e}")))?;

        let token_url = format!("{endpoint}/latest/api/token");
        let token_response = client
            .put(token_url)
            .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
            .send()
            .map_err(|error| MidgeError::Internal(format!("IMDSv2 token fetch failed: {error}")))?;
        if !token_response.status().is_success() {
            return Err(MidgeError::Internal(format!(
                "IMDSv2 token endpoint returned {}; IMDSv1 fallback is not supported",
                token_response.status()
            )));
        }
        let token = token_response.text().map_err(|error| {
            MidgeError::Internal(format!("failed to read IMDSv2 token: {error}"))
        })?;
        let token = token.trim();
        if token.is_empty() {
            return Err(MidgeError::Internal(
                "IMDSv2 token endpoint returned an empty token".into(),
            ));
        }

        let role_url = format!("{endpoint}/latest/meta-data/iam/security-credentials/");
        let role_req = client
            .get(role_url)
            .header("X-aws-ec2-metadata-token", token);
        let role_resp = role_req
            .send()
            .map_err(|e| MidgeError::Internal(format!("imds role name fetch failed: {e}")))?;
        if !role_resp.status().is_success() {
            return Err(MidgeError::Internal(format!(
                "imds role name endpoint returned {}",
                role_resp.status()
            )));
        }
        let role_name = role_resp
            .text()
            .map_err(|e| MidgeError::Internal(format!("failed to read imds role name: {e}")))?
            .lines()
            .next()
            .unwrap_or_default()
            .trim()
            .to_string();
        if role_name.is_empty() {
            return Err(MidgeError::Internal(
                "imds returned empty IAM role name".into(),
            ));
        }

        let creds_url = format!("{endpoint}/latest/meta-data/iam/security-credentials/{role_name}");
        let creds_req = client
            .get(&creds_url)
            .header("X-aws-ec2-metadata-token", token);
        let creds_resp = creds_req
            .send()
            .map_err(|e| MidgeError::Internal(format!("imds credential fetch failed: {e}")))?;
        if !creds_resp.status().is_success() {
            return Err(MidgeError::Internal(format!(
                "imds credentials endpoint returned {}",
                creds_resp.status()
            )));
        }
        let text = creds_resp
            .text()
            .map_err(|e| MidgeError::Internal(format!("failed to read imds credentials: {e}")))?;
        self.parse_json_credentials(&text)
    }

    fn parse_json_credentials(&self, payload: &str) -> MidgeResult<CachedAwsCredentials> {
        let json: Value = serde_json::from_str(payload)
            .map_err(|e| MidgeError::Internal(format!("invalid credential json: {e}")))?;

        let access_key = json
            .get("AccessKeyId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| MidgeError::Internal("missing AccessKeyId".into()))?
            .to_string();
        let access_key =
            required_temporary_credential_field(access_key, "AccessKeyId", "AWS metadata")?;
        let secret_key = json
            .get("SecretAccessKey")
            .and_then(|v| v.as_str())
            .ok_or_else(|| MidgeError::Internal("missing SecretAccessKey".into()))?
            .to_string();
        let secret_key =
            required_temporary_credential_field(secret_key, "SecretAccessKey", "AWS metadata")?;
        let session_token = json
            .get("Token")
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string)
            .or_else(|| {
                json.get("SessionToken")
                    .and_then(|v| v.as_str())
                    .map(std::string::ToString::to_string)
            })
            .ok_or_else(|| MidgeError::Internal("missing temporary credential token".into()))?;
        let session_token =
            required_temporary_credential_field(session_token, "Token", "AWS metadata")?;
        let expiry = json
            .get("Expiration")
            .and_then(|v| v.as_str())
            .ok_or_else(|| MidgeError::Internal("missing Expiration".into()))?;
        let expires_at = required_temporary_credential_expiry(expiry, "AWS metadata")?;

        Ok(CachedAwsCredentials {
            creds: AwsCredentials {
                access_key,
                secret_key,
                region: self.region.clone(),
                session_token: Some(session_token),
            },
            expires_at: Some(expires_at),
        })
    }
}

fn extract_xml_tag(xml: &str, tag: &str) -> MidgeResult<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    if let Some(start) = xml.find(&open) {
        let start_index = start + open.len();
        if let Some(end_rel) = xml[start_index..].find(&close) {
            let end_index = start_index + end_rel;
            return Ok(xml[start_index..end_index].trim().to_string());
        }
    }
    Err(MidgeError::Internal(format!(
        "missing {tag} in sts response"
    )))
}

pub(crate) fn resolve_static_env_credentials(region: &str) -> Option<AwsCredentials> {
    let access_key = std::env::var("AWS_ACCESS_KEY_ID")
        .or_else(|_| std::env::var("AWS_ACCESS_KEY"))
        .ok()?;
    let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
        .or_else(|_| std::env::var("AWS_SECRET_KEY"))
        .ok()?;
    let session_token = std::env::var("AWS_SESSION_TOKEN")
        .ok()
        .or_else(|| std::env::var("AWS_SECURITY_TOKEN").ok());

    Some(AwsCredentials {
        access_key,
        secret_key,
        region: region.to_string(),
        session_token,
    })
}

pub(crate) fn resolve_static_profile_credentials(
    region: &str,
    profile: Option<&str>,
    credentials_file: Option<&std::path::Path>,
    config_file: Option<&std::path::Path>,
) -> MidgeResult<Option<AwsCredentials>> {
    let profile = profile
        .map(str::to_string)
        .or_else(|| std::env::var("AWS_PROFILE").ok())
        .unwrap_or_else(|| "default".to_string());
    let credentials_file = credentials_file
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var("AWS_SHARED_CREDENTIALS_FILE")
                .ok()
                .map(std::path::PathBuf::from)
        })
        .or_else(|| default_home_file(".aws/credentials"));
    let config_file = config_file
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var("AWS_CONFIG_FILE")
                .ok()
                .map(std::path::PathBuf::from)
        })
        .or_else(|| default_home_file(".aws/config"));

    if let Some(path) = credentials_file {
        if let Some(creds) = read_aws_profile_credentials(&path, &profile, region, false)? {
            return Ok(Some(creds));
        }
    }

    if let Some(path) = config_file {
        if let Some(creds) = read_aws_profile_credentials(&path, &profile, region, true)? {
            return Ok(Some(creds));
        }
    }

    Ok(None)
}

fn default_home_file(relative: &str) -> Option<std::path::PathBuf> {
    std::env::var_os("HOME").map(|home| std::path::PathBuf::from(home).join(relative))
}

fn read_aws_profile_credentials(
    path: &std::path::Path,
    profile: &str,
    region: &str,
    is_config_file: bool,
) -> MidgeResult<Option<AwsCredentials>> {
    let content = match std::fs::read_to_string(path) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(MidgeError::Internal(format!(
                "failed to read AWS profile file '{}': {}",
                path.display(),
                error
            )))
        }
    };

    let section = profile_section_name(profile, is_config_file);
    let Some(values) = parse_ini_section(&content, &section) else {
        return Ok(None);
    };

    if values.contains_key("credential_process")
        || values.contains_key("sso_start_url")
        || values.contains_key("sso_session")
    {
        return Err(MidgeError::InvalidArgument(format!(
            "AWS profile '{profile}' uses credential_process or SSO, which Midge does not execute without SDK/CLI support"
        )));
    }

    let access_key = values
        .get("aws_access_key_id")
        .cloned()
        .or_else(|| values.get("access_key_id").cloned());
    let secret_key = values
        .get("aws_secret_access_key")
        .cloned()
        .or_else(|| values.get("secret_access_key").cloned());

    let (Some(access_key), Some(secret_key)) = (access_key, secret_key) else {
        return Ok(None);
    };

    Ok(Some(AwsCredentials {
        access_key,
        secret_key,
        region: region.to_string(),
        session_token: values
            .get("aws_session_token")
            .cloned()
            .or_else(|| values.get("aws_security_token").cloned()),
    }))
}

fn profile_section_name(profile: &str, is_config_file: bool) -> String {
    if !is_config_file || profile == "default" {
        profile.to_string()
    } else {
        format!("profile {profile}")
    }
}

fn parse_ini_section(
    content: &str,
    section: &str,
) -> Option<std::collections::HashMap<String, String>> {
    let mut current = None::<String>;
    let mut values = std::collections::HashMap::new();

    for raw_line in content.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }

        if line.starts_with('[') && line.ends_with(']') {
            current = Some(line[1..line.len() - 1].trim().to_string());
            continue;
        }

        if current.as_deref() != Some(section) {
            continue;
        }

        if let Some((key, value)) = line.split_once('=') {
            values.insert(key.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }

    if values.is_empty() {
        None
    } else {
        Some(values)
    }
}

fn parse_rfc3339_epoch(value: &str) -> Option<u64> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|dt| u64::try_from(dt.timestamp().max(0)).unwrap_or(0))
}

fn required_temporary_credential_field(
    value: String,
    field: &str,
    source: &str,
) -> MidgeResult<String> {
    if value.trim().is_empty() {
        Err(MidgeError::Internal(format!(
            "{source} returned an empty {field}"
        )))
    } else {
        Ok(value)
    }
}

fn required_temporary_credential_expiry(value: &str, source: &str) -> MidgeResult<u64> {
    let expiry = parse_rfc3339_epoch(value)
        .ok_or_else(|| MidgeError::Internal(format!("{source} returned an invalid Expiration")))?;
    if expiry <= current_unix_secs() {
        return Err(MidgeError::Internal(format!(
            "{source} returned an expired Expiration"
        )));
    }
    Ok(expiry)
}

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Configuration for S3-compatible storage
#[derive(Clone, Debug)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub path_style: bool,
}

impl S3Config {
    /// Create config for AWS S3 (default endpoint)
    pub fn aws(bucket: String, region: String) -> Self {
        Self {
            bucket,
            region,
            endpoint: None,
            path_style: false,
        }
    }

    /// Create config for custom S3-compatible endpoint
    pub fn custom(bucket: String, region: String, endpoint: String, path_style: bool) -> Self {
        Self {
            bucket,
            region,
            endpoint: Some(endpoint),
            path_style,
        }
    }
}

pub struct S3Provider {
    backend: Arc<dyn CloudBackend>,
}

impl S3Provider {
    /// Create provider with AWS credentials (`SigV4` signing)
    pub fn aws(bucket: String, region: String, creds: AwsCredentials) -> MidgeResult<Self> {
        let config = S3Config::aws(bucket, region);
        Self::with_config(config, Some(creds))
    }

    /// Create AWS provider using the default AWS credential chain.
    ///
    /// Resolution order:
    /// 1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional session token)
    /// 2. Shared credentials/config profile
    /// 3. EKS/IRSA web identity (`AWS_ROLE_ARN` + `AWS_WEB_IDENTITY_TOKEN_FILE`)
    /// 4. ECS/Fargate task role endpoint
    /// 5. EC2 Instance Metadata Service (`IMDSv2` only)
    pub fn aws_default(bucket: String, region: String) -> MidgeResult<Self> {
        let config = S3Config::aws(bucket, region.clone());
        let signer: Arc<dyn CloudSigner> = Arc::new(SigV4Signer::new_default_chain(region));
        Self::with_signer(config, Some(signer))
    }

    /// Create provider with custom S3-compatible endpoint
    #[cfg(test)]
    pub fn custom(config: S3Config, access_key: String, secret_key: String) -> MidgeResult<Self> {
        let creds = AwsCredentials {
            access_key,
            secret_key,
            region: config.region.clone(),
            session_token: None,
        };
        Self::with_config(config, Some(creds))
    }

    pub(crate) fn custom_with_credentials(
        config: S3Config,
        creds: AwsCredentials,
    ) -> MidgeResult<Self> {
        Self::with_config(config, Some(creds))
    }

    /// Create provider with full config and optional credentials
    fn with_config(config: S3Config, creds: Option<AwsCredentials>) -> MidgeResult<Self> {
        let signer = creds.map(|c| Arc::new(SigV4Signer::new(c)) as Arc<dyn CloudSigner>);
        Self::with_signer(config, signer)
    }

    fn with_signer(config: S3Config, signer: Option<Arc<dyn CloudSigner>>) -> MidgeResult<Self> {
        let executor = CloudExecutor::new(signer)?;
        let backend = Arc::new(S3Backend::new(config, executor));
        Ok(Self { backend })
    }

    /// Legacy constructor (AWS with explicit credentials)
    #[cfg(test)]
    pub fn new(bucket: String, region: String, creds: AwsCredentials) -> MidgeResult<Self> {
        Self::aws(bucket, region, creds)
    }

    pub fn backend(&self) -> Arc<dyn CloudBackend> {
        Arc::clone(&self.backend)
    }
}

impl Drop for S3Provider {
    fn drop(&mut self) {
        tracing::trace!("S3Provider dropping, cleanup will propagate to CloudExecutor");
    }
}

struct S3Backend {
    config: S3Config,
    executor: CloudExecutor,
}

impl S3Backend {
    fn new(config: S3Config, executor: CloudExecutor) -> Self {
        Self { config, executor }
    }

    fn canonical_key(key: &str) -> String {
        key.split('/')
            .map(|segment| utf8_percent_encode(segment, S3_PATH_SEGMENT_ENCODE_SET).to_string())
            .collect::<Vec<_>>()
            .join("/")
    }

    fn base_url(&self) -> String {
        if let Some(ref endpoint) = self.config.endpoint {
            if self.config.path_style {
                // Path-style: https://endpoint/bucket
                format!("{}/{}", endpoint.trim_end_matches('/'), self.config.bucket)
            } else {
                // Virtual-hosted style: https://bucket.endpoint
                let endpoint_without_protocol = endpoint
                    .trim_start_matches("https://")
                    .trim_start_matches("http://");
                let protocol = if endpoint.starts_with("https") {
                    "https"
                } else {
                    "http"
                };
                format!(
                    "{}://{}.{}",
                    protocol, self.config.bucket, endpoint_without_protocol
                )
            }
        } else {
            // Default AWS S3 endpoint
            if self.config.bucket.contains('.') {
                // Wildcard S3 TLS certificates do not match dotted bucket
                // names. AWS path-style addressing keeps those valid bucket
                // names on the signed request path instead of the hostname.
                format!(
                    "https://s3.{}.{}/{}",
                    self.config.region,
                    aws_dns_suffix(&self.config.region),
                    self.config.bucket
                )
            } else {
                format!(
                    "https://{}.s3.{}.{}",
                    self.config.bucket,
                    self.config.region,
                    aws_dns_suffix(&self.config.region)
                )
            }
        }
    }

    fn object_url(&self, key: &str) -> String {
        format!("{}/{}", self.base_url(), Self::canonical_key(key))
    }

    #[cfg(test)]
    fn list_url(&self, prefix: &str, continuation_token: Option<&str>) -> String {
        let mut url = format!("{}?list-type=2&prefix={}", self.base_url(), encode(prefix));
        if let Some(token) = continuation_token {
            url.push_str("&continuation-token=");
            url.push_str(&encode(token));
        }
        url
    }
}

struct S3ListState {
    prefix: String,
    base_url: String,
    continuation_token: Option<String>,
    items: Vec<String>,
    budget: CloudListBudget,
    error: Option<CloudError>,
}

impl S3ListState {
    fn url(&self) -> String {
        let mut url = format!(
            "{}?list-type=2&prefix={}",
            self.base_url,
            encode(&self.prefix)
        );
        if let Some(token) = self.continuation_token.as_deref() {
            url.push_str("&continuation-token=");
            url.push_str(&encode(token));
        }
        url
    }
}

impl CloudBackend for S3Backend {
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
        let content_length = data.len();
        let conditional_mutation = headers.iter().any(|(name, _)| {
            name.eq_ignore_ascii_case("if-match") || name.eq_ignore_ascii_case("if-none-match")
        });
        let mut request = CloudRequest::new(Method::PUT, url)
            .with_body(data)
            .with_header("Content-Length", content_length.to_string());
        if conditional_mutation {
            request = request.with_conditional_conflict_retries();
        }
        // Apply provided headers (e.g. conditional headers like If-None-Match)
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
            Ok(resp) if resp.status == 200 => CloudEvent::Put {
                key: ctx,
                result: CloudOutcome::Ok(()),
            },
            Ok(resp) => CloudEvent::Put {
                key: ctx,
                result: CloudOutcome::Err(s3_response_error(&resp, "S3 PUT", conditional_mutation)),
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
                result: CloudOutcome::Err(s3_response_error(&resp, "S3 GET", false)),
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
                let metadata = object_metadata_from_s3_response(
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
                result: CloudOutcome::Err(s3_response_error(&resp, "S3 GET", false)),
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
        let mut request = CloudRequest::new(Method::GET, url);
        let range = match end {
            Some(e) => format!("bytes={}-{}", start, e.saturating_sub(1)),
            None => format!("bytes={start}-"),
        };
        request = request.with_header("Range", range);
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
                result: CloudOutcome::Err(s3_response_error(&resp, "S3 GET_RANGE", false)),
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
        if conditional_mutation {
            request = request.with_conditional_conflict_retries();
        }
        for (name, value) in headers {
            request = request.with_header(name, value);
        }
        let mapper = move |ctx: String, result: MidgeResult<CloudResponse>| match result {
            Ok(resp) if matches!(resp.status, 200 | 204 | 404) => CloudEvent::Delete {
                key: ctx,
                result: CloudOutcome::Ok(()),
            },
            Ok(resp) => CloudEvent::Delete {
                key: ctx,
                result: CloudOutcome::Err(s3_response_error(
                    &resp,
                    "S3 DELETE",
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
        let base_url = self.base_url();
        let state = S3ListState {
            prefix: prefix.clone(),
            base_url,
            continuation_token: None,
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
                    state.error = Some(s3_response_error(&resp, "S3 LIST", false));
                    return Ok(false);
                }
                let body = String::from_utf8_lossy(&resp.body);
                super::validate_list_xml(&body, "ListBucketResult")?;
                let page_items = extract_xml_tag_values(&body, "Key");
                let truncated = extract_xml_tag_values(&body, "IsTruncated")
                    .first()
                    .is_some_and(|value| value.eq_ignore_ascii_case("true"));
                let continuation_token = extract_xml_tag_values(&body, "NextContinuationToken")
                    .into_iter()
                    .next()
                    .filter(|token| !token.is_empty());
                if truncated && continuation_token.is_none() {
                    return Err(MidgeError::Internal(
                        "S3 list response was truncated without NextContinuationToken".to_string(),
                    ));
                }
                let continuation_token = truncated.then_some(continuation_token).flatten();
                state
                    .budget
                    .record_page(&page_items, continuation_token.as_deref())?;
                state.items.extend(page_items);
                state.continuation_token = continuation_token;
                Ok(truncated)
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
                result: object_metadata_from_s3_response(&resp, None),
            },
            Ok(resp) => CloudEvent::Head {
                key: ctx,
                result: CloudOutcome::Err(s3_response_error(&resp, "S3 HEAD", false)),
            },
            Err(err) => CloudEvent::Head {
                key: ctx,
                result: CloudOutcome::Err(CloudError::from_transport_error(err)),
            },
        };
        self.executor.spawn_request(request, key, callback, mapper);
    }
}

fn object_metadata_from_s3_response(
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
                CloudError::Protocol("S3 metadata response is missing Content-Length".to_string())
            })?
            .1
            .parse::<u64>()
            .map_err(|error| {
                CloudError::Protocol(format!(
                    "S3 metadata response has invalid Content-Length: {error}"
                ))
            })?,
    };
    let etag = response
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("etag"))
        .map(|(_, value)| value.trim())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| CloudError::Protocol("S3 metadata response is missing ETag".to_string()))?;
    Ok(ObjectMetadata::new(size, etag.to_string()))
}

fn s3_response_error(
    response: &CloudResponse,
    operation: &str,
    conditional_mutation: bool,
) -> CloudError {
    let body = String::from_utf8_lossy(&response.body);
    let code = extract_xml_tag_values(&body, "Code").into_iter().next();
    let message = extract_xml_tag_values(&body, "Message").into_iter().next();
    let detail = match (code.as_deref(), message.as_deref()) {
        (Some(code), Some(message)) => format!("{operation}: {code}: {message}"),
        (Some(code), None) => format!("{operation}: {code}"),
        (None, _) => operation.to_string(),
    };

    if conditional_mutation
        && response.status == 412
        && code
            .as_deref()
            .is_some_and(|code| code.eq_ignore_ascii_case("PreconditionFailed"))
    {
        CloudError::PreconditionFailed(format!("status {}: {detail}", response.status))
    } else {
        CloudError::from_http_status(response.status, detail)
    }
}

impl CloudSigner for SigV4Signer {
    fn sign(&self, request: &mut CloudRequest) -> MidgeResult<()> {
        let creds = self.resolve_credentials()?;
        let url = Url::parse(&request.url)
            .map_err(|err| MidgeError::InvalidArgument(format!("url parse: {err}")))?;
        let host_name = url
            .host_str()
            .ok_or_else(|| MidgeError::InvalidArgument("missing host".to_string()))?;
        let host = match url.port() {
            Some(port) => format!("{host_name}:{port}"),
            None => host_name.to_string(),
        };
        request.headers.retain(|(name, _)| {
            !name.eq_ignore_ascii_case("host")
                && !name.eq_ignore_ascii_case("x-amz-date")
                && !name.eq_ignore_ascii_case("x-amz-content-sha256")
                && !name.eq_ignore_ascii_case("x-amz-security-token")
                && !name.eq_ignore_ascii_case("authorization")
        });
        request.headers.push(("Host".into(), host));
        let now = Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date = now.format("%Y%m%d").to_string();
        let payload_hash = if let Some(body) = request.body.as_ref() {
            let mut hasher = Sha256::new();
            hasher.update(body);
            hex::encode(hasher.finalize())
        } else {
            let mut hasher = Sha256::new();
            hasher.update([]);
            hex::encode(hasher.finalize())
        };
        request
            .headers
            .push(("X-Amz-Date".into(), amz_date.clone()));
        request
            .headers
            .push(("X-Amz-Content-Sha256".into(), payload_hash.clone()));
        if let Some(token) = creds.session_token.as_ref() {
            request
                .headers
                .push(("X-Amz-Security-Token".into(), token.clone()));
        }

        let mut header_pairs: Vec<(String, String)> = request
            .headers
            .iter()
            .map(|(name, value)| (name.to_lowercase(), value.trim().to_string()))
            .collect();
        header_pairs.sort_by(|a, b| a.0.cmp(&b.0));
        let signed_headers = header_pairs
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>()
            .join(";");
        let canonical_headers =
            header_pairs
                .iter()
                .fold(String::new(), |mut headers, (name, value)| {
                    use std::fmt::Write as _;
                    let _ = writeln!(headers, "{name}:{value}");
                    headers
                });

        let path = if url.path().is_empty() {
            "/"
        } else {
            url.path()
        };
        let canonical_query = if let Some(query) = url.query() {
            canonicalize_query(query)
        } else {
            String::new()
        };

        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            request.method.as_str(),
            percent_encode_path(path),
            canonical_query,
            canonical_headers,
            signed_headers,
            payload_hash
        );

        let mut hasher = Sha256::new();
        hasher.update(canonical_request.as_bytes());
        let canonical_hash = hex::encode(hasher.finalize());

        let credential_scope = format!("{}/{}/s3/aws4_request", date, self.region());
        let string_to_sign =
            format!("AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{canonical_hash}");

        let signing_key = self.signing_key(&creds.secret_key, &date)?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&signing_key)
            .map_err(|_| MidgeError::Internal("hmac init".to_string()))?;
        mac.update(string_to_sign.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        let auth_header = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            creds.access_key, credential_scope, signed_headers, signature
        );

        request.headers.push(("Authorization".into(), auth_header));
        Ok(())
    }
}

struct SigV4Signer {
    source: AwsCredentialSource,
    region: String,
}

impl SigV4Signer {
    fn new(creds: AwsCredentials) -> Self {
        Self {
            region: creds.region.clone(),
            source: AwsCredentialSource::Static(creds),
        }
    }

    fn new_default_chain(region: String) -> Self {
        Self {
            region: region.clone(),
            source: AwsCredentialSource::DefaultChain(DefaultAwsCredentialProvider::new(region)),
        }
    }

    fn resolve_credentials(&self) -> MidgeResult<AwsCredentials> {
        match &self.source {
            AwsCredentialSource::Static(creds) => Ok(creds.clone()),
            AwsCredentialSource::DefaultChain(provider) => provider.resolve(),
        }
    }

    fn signing_key(&self, secret_key: &str, date: &str) -> MidgeResult<Vec<u8>> {
        let k_date = hmac_sha256(format!("AWS4{secret_key}").as_bytes(), date.as_bytes())?;
        let k_region = hmac_sha256(&k_date, self.region.as_bytes())?;
        let k_service = hmac_sha256(&k_region, b"s3")?;
        hmac_sha256(&k_service, b"aws4_request")
    }

    fn region(&self) -> &str {
        &self.region
    }
}

enum AwsCredentialSource {
    Static(AwsCredentials),
    DefaultChain(DefaultAwsCredentialProvider),
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> MidgeResult<Vec<u8>> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key)
        .map_err(|_| MidgeError::Internal("hmac init".to_string()))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn percent_encode_path(path: &str) -> String {
    let bytes = path.as_bytes();
    let mut encoded = String::with_capacity(path.len());
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b'/' || byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
        {
            encoded.push(char::from(byte));
            index += 1;
        } else if byte == b'%'
            && index + 2 < bytes.len()
            && bytes[index + 1].is_ascii_hexdigit()
            && bytes[index + 2].is_ascii_hexdigit()
        {
            encoded.push('%');
            encoded.push(char::from(bytes[index + 1]).to_ascii_uppercase());
            encoded.push(char::from(bytes[index + 2]).to_ascii_uppercase());
            index += 3;
        } else {
            use std::fmt::Write as _;
            let _ = write!(encoded, "%{byte:02X}");
            index += 1;
        }
    }
    encoded
}

fn canonicalize_query(query: &str) -> String {
    let mut pairs: Vec<String> = url::form_urlencoded::parse(query.as_bytes())
        .map(|(key, value)| format!("{}={}", encode(&key), encode(&value)))
        .collect();
    pairs.sort();
    pairs.join("&")
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
            |response| object_metadata_from_s3_response(response, Some(3)),
        );
    }

    use super::*;
    use crate::storage::providers::test_support::{
        receive_list_result, spawn_recording_http_server, spawn_recording_http_server_with_status,
        spawn_scripted_http_response_server, spawn_scripted_http_server,
    };
    use reqwest::Method;
    use std::ffi::OsString;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex, OnceLock};

    fn s3_env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    struct ScopedEnvironment {
        previous: Vec<(&'static str, Option<OsString>)>,
    }

    impl ScopedEnvironment {
        fn apply(overrides: &[(&'static str, Option<String>)]) -> Self {
            let previous = overrides
                .iter()
                .map(|(name, _)| (*name, std::env::var_os(name)))
                .collect();
            for (name, value) in overrides {
                if let Some(value) = value {
                    std::env::set_var(name, value);
                } else {
                    std::env::remove_var(name);
                }
            }
            Self { previous }
        }
    }

    impl Drop for ScopedEnvironment {
        fn drop(&mut self) {
            for (name, value) in &self.previous {
                if let Some(value) = value {
                    std::env::set_var(name, value);
                } else {
                    std::env::remove_var(name);
                }
            }
        }
    }

    fn recording_backend(endpoint: String) -> S3Backend {
        let config = S3Config::custom("bucket".into(), "us-east-1".into(), endpoint, true);
        S3Backend::new(config, CloudExecutor::new(None).expect("cloud executor"))
    }

    #[test]
    fn should_reject_redirect_status_when_putting_s3_object() {
        // Arrange
        let server = spawn_recording_http_server_with_status(302, Vec::new(), Vec::new());
        let backend = recording_backend(server.endpoint.clone());
        let (sender, receiver) = std::sync::mpsc::channel();

        // Act
        backend.submit_put("object", b"value".to_vec(), Vec::new(), sender);
        let result = receive_put_result(&receiver);
        let _ = server.finish();

        // Assert
        assert!(matches!(result, CloudOutcome::Err(CloudError::Protocol(_))));
    }

    #[test]
    fn should_use_path_style_endpoint_given_dotted_native_aws_bucket() {
        // Arrange
        let config = S3Config::aws("database.example".into(), "us-east-1".into());
        let backend = S3Backend::new(config, CloudExecutor::new(None).expect("cloud executor"));

        // Act
        let endpoint = backend.base_url();

        // Assert
        assert_eq!(
            endpoint,
            "https://s3.us-east-1.amazonaws.com/database.example"
        );
    }

    #[test]
    fn should_uri_encode_s3_key_segments_once_when_signing() {
        // Arrange
        let key = "tenant$blue+1/test$file @2.json";

        // Act
        let encoded_key = S3Backend::canonical_key(key);
        let canonical_path = percent_encode_path(&format!("/bucket/{encoded_key}"));

        // Assert
        assert_eq!(encoded_key, "tenant%24blue%2B1/test%24file%20%402.json");
        assert_eq!(
            canonical_path,
            "/bucket/tenant%24blue%2B1/test%24file%20%402.json"
        );
    }

    #[test]
    fn should_use_aws_china_dns_suffix_for_s3_endpoint() {
        // Arrange
        let config = S3Config::aws("database".into(), "cn-north-1".into());
        let backend = S3Backend::new(config, CloudExecutor::new(None).expect("cloud executor"));

        // Act
        let s3_endpoint = backend.base_url();

        // Assert
        assert_eq!(
            s3_endpoint,
            "https://database.s3.cn-north-1.amazonaws.com.cn"
        );
    }

    #[test]
    fn should_use_aws_china_dns_suffix_for_sts_endpoint() {
        // Arrange
        let region = "cn-north-1";

        // Act
        let sts_endpoint = aws_sts_endpoint(region);

        // Assert
        assert_eq!(sts_endpoint, "https://sts.cn-north-1.amazonaws.com.cn/");
    }

    #[test]
    fn should_prefer_container_authorization_token_file_when_both_sources_exist() {
        // Arrange
        let _env_guard = s3_env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let token_directory = tempfile::tempdir().expect("create token directory");
        let token_path = token_directory.path().join("pod-identity-token");
        std::fs::write(&token_path, "file-token").expect("write token file");
        let server = spawn_recording_http_server(
            Vec::new(),
            br#"{"AccessKeyId":"access","SecretAccessKey":"secret","Token":"session","Expiration":"2099-01-01T00:00:00Z"}"#.to_vec(),
        );
        let _environment = ScopedEnvironment::apply(&[
            ("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", None),
            (
                "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                Some(server.endpoint.clone()),
            ),
            (
                "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
                Some(token_path.to_string_lossy().into_owned()),
            ),
            (
                "AWS_CONTAINER_AUTHORIZATION_TOKEN",
                Some("stale-environment-token".to_string()),
            ),
        ]);
        let provider = DefaultAwsCredentialProvider::new("us-east-1".into());

        // Act
        let credentials = provider.resolve_ecs_task_role();
        let request = server.finish();

        // Assert
        assert!(credentials.is_ok());
        assert_eq!(request.header("authorization"), Some("file-token"));
    }

    #[test]
    fn should_reject_insecure_remote_container_credential_endpoint() {
        // Arrange
        let insecure_remote = "http://192.0.2.1/credentials";
        let secure_remote = "https://credentials.example.com/credentials";

        // Act
        let insecure_result = validate_container_full_uri(insecure_remote);
        let secure_result = validate_container_full_uri(secure_remote);

        // Assert
        assert!(matches!(
            insecure_result,
            Err(MidgeError::InvalidArgument(message)) if message.contains("insecure")
        ));
        assert_eq!(
            secure_result.expect("HTTPS credential endpoint").url,
            secure_remote
        );
    }

    #[test]
    fn should_accept_bracketed_eks_ipv6_container_credential_endpoint() {
        // Arrange
        let endpoint = "http://[fd00:ec2::23]/v1/credentials";

        // Act
        let result = validate_container_full_uri(endpoint);

        // Assert
        assert_eq!(result.expect("EKS IPv6 credential endpoint").url, endpoint);
    }

    #[test]
    fn should_try_every_validated_container_credential_address() {
        // Arrange
        let _env_guard = s3_env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let response = r#"{"AccessKeyId":"access","SecretAccessKey":"secret","Token":"session","Expiration":"2099-01-01T00:00:00Z"}"#;
        let server = spawn_scripted_http_server(vec![response.to_string()]);
        let endpoint = server.endpoint.replacen("127.0.0.1", "localhost", 1);
        let _environment = ScopedEnvironment::apply(&[
            ("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", None),
            ("AWS_CONTAINER_CREDENTIALS_FULL_URI", Some(endpoint)),
            ("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", None),
            ("AWS_CONTAINER_AUTHORIZATION_TOKEN", None),
        ]);
        let provider = DefaultAwsCredentialProvider::new("us-east-1".into());

        // Act
        let credentials = provider.resolve_ecs_task_role();
        let requests_served = server.finish();

        // Assert
        assert!(credentials.is_ok());
        assert_eq!(requests_served, 1);
    }

    #[test]
    fn should_reject_container_relative_uri_that_can_replace_credential_host() {
        // Arrange
        let _env_guard = s3_env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _environment = ScopedEnvironment::apply(&[
            (
                "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
                Some("//192.0.2.1/credentials".to_string()),
            ),
            ("AWS_CONTAINER_CREDENTIALS_FULL_URI", None),
        ]);

        // Act
        let result = container_credential_endpoint();

        // Assert
        assert!(matches!(
            result,
            Err(MidgeError::InvalidArgument(message)) if message.contains("absolute path")
        ));
    }

    #[test]
    fn should_reject_temporary_aws_credentials_without_valid_expiration() {
        // Arrange
        let provider = DefaultAwsCredentialProvider::new("us-east-1".into());
        let payloads = [
            r#"{"AccessKeyId":"access","SecretAccessKey":"secret","Token":"session"}"#,
            r#"{"AccessKeyId":"access","SecretAccessKey":"secret","Token":"session","Expiration":"not-a-time"}"#,
            r#"{"AccessKeyId":"access","SecretAccessKey":"secret","Token":"session","Expiration":"2000-01-01T00:00:00Z"}"#,
        ];

        // Act
        let errors = payloads.map(|payload| {
            provider
                .parse_json_credentials(payload)
                .err()
                .expect("temporary credentials must have a future expiration")
        });

        // Assert
        assert!(errors
            .iter()
            .all(|error| error.to_string().contains("Expiration")));
    }

    #[test]
    fn should_skip_imds_when_aws_metadata_is_disabled() {
        // Arrange
        let _env_guard = s3_env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _environment = ScopedEnvironment::apply(&[
            ("AWS_EC2_METADATA_DISABLED", Some("true".to_string())),
            (
                "AWS_EC2_METADATA_SERVICE_ENDPOINT",
                Some("http://127.0.0.1:9".to_string()),
            ),
        ]);
        let provider = DefaultAwsCredentialProvider::new("us-east-1".into());

        // Act
        let result = provider.resolve_ec2_imds();

        // Assert
        assert!(matches!(
            result,
            Err(MidgeError::InvalidArgument(message)) if message.contains("disabled")
        ));
    }

    #[test]
    fn should_never_fall_back_to_imdsv1_when_token_request_fails() {
        // Arrange
        let _env_guard = s3_env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let server = spawn_recording_http_server_with_status(403, Vec::new(), Vec::new());
        let _environment = ScopedEnvironment::apply(&[
            ("AWS_EC2_METADATA_DISABLED", Some("false".to_string())),
            ("AWS_EC2_METADATA_V1_DISABLED", Some("false".to_string())),
            (
                "AWS_EC2_METADATA_SERVICE_ENDPOINT",
                Some(server.endpoint.clone()),
            ),
        ]);
        let provider = DefaultAwsCredentialProvider::new("us-east-1".into());

        // Act
        let result = provider.resolve_ec2_imds();
        let request = server.finish();

        // Assert
        assert!(matches!(
            result,
            Err(MidgeError::Internal(message))
                if message.contains("IMDSv1 fallback is not supported")
        ));
        assert_eq!(request.method, "PUT");
        assert_eq!(request.target, "/latest/api/token");
    }

    fn receive_put_result(receiver: &std::sync::mpsc::Receiver<CloudEvent>) -> CloudOutcome<()> {
        match receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("receive S3 PUT result")
        {
            CloudEvent::Put { result, .. } => result,
            event => panic!("expected S3 PUT event, got {event:?}"),
        }
    }

    fn receive_delete_result(receiver: &std::sync::mpsc::Receiver<CloudEvent>) -> CloudOutcome<()> {
        match receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("receive S3 DELETE result")
        {
            CloudEvent::Delete { result, .. } => result,
            event => panic!("expected S3 DELETE event, got {event:?}"),
        }
    }

    #[test]
    fn should_treat_missing_s3_object_as_success_when_deleting() {
        // Arrange
        let server = spawn_recording_http_server_with_status(404, Vec::new(), Vec::new());
        let backend = recording_backend(server.endpoint.clone());
        let (sender, receiver) = std::sync::mpsc::channel();

        // Act
        backend.submit_delete("missing/object", Vec::new(), sender);
        let result = receive_delete_result(&receiver);
        let request = server.finish();

        // Assert
        assert!(matches!(result, CloudOutcome::Ok(())));
        assert_eq!(request.method, "DELETE");
    }

    #[test]
    fn should_retry_s3_conditional_request_conflict_when_creating_object() {
        // Arrange
        let server = spawn_scripted_http_response_server(vec![
            (
                409,
                "application/xml".to_string(),
                "<Error><Code>ConditionalRequestConflict</Code></Error>".to_string(),
            ),
            (200, "application/xml".to_string(), String::new()),
        ]);
        let backend = recording_backend(server.endpoint.clone());
        let (sender, receiver) = std::sync::mpsc::channel();

        // Act
        backend.submit_put(
            "conditional/object",
            b"value".to_vec(),
            vec![("If-None-Match".to_string(), "*".to_string())],
            sender,
        );
        let result = receive_put_result(&receiver);
        let requests = server.finish();

        // Assert
        assert!(matches!(result, CloudOutcome::Ok(())));
        assert_eq!(requests, 2);
    }

    #[test]
    fn should_only_classify_s3_predicate_failure_as_precondition_conflict() {
        // Arrange
        let predicate = CloudResponse {
            status: 412,
            headers: Vec::new(),
            body:
                b"<Error><Code>PreconditionFailed</Code><Message>condition failed</Message></Error>"
                    .to_vec(),
        };
        let unrelated = CloudResponse {
            status: 409,
            headers: Vec::new(),
            body: b"<Error><Code>OperationAborted</Code><Message>try again</Message></Error>"
                .to_vec(),
        };

        // Act
        let predicate_error = s3_response_error(&predicate, "S3 PUT", true);
        let unrelated_error = s3_response_error(&unrelated, "S3 PUT", true);

        // Assert
        assert!(matches!(predicate_error, CloudError::PreconditionFailed(_)));
        assert!(matches!(unrelated_error, CloudError::InvalidRequest(_)));
    }

    #[test]
    fn should_preserve_s3_authorization_error_when_listing() {
        // Arrange
        let server = spawn_scripted_http_response_server(vec![(
            403,
            "application/xml".to_string(),
            "<Error><Code>AccessDenied</Code><Message>denied</Message></Error>".to_string(),
        )]);
        let backend = recording_backend(server.endpoint.clone());

        // Act
        let result = receive_list_result(&backend);
        let requests = server.finish();

        // Assert
        assert!(matches!(
            result,
            CloudOutcome::Err(CloudError::Unauthorized(message))
                if message.contains("AccessDenied")
        ));
        assert_eq!(requests, 1);
    }

    #[test]
    fn should_fail_closed_when_s3_head_omits_identity_metadata() {
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
            .expect("receive S3 HEAD result")
        {
            CloudEvent::Head { result, .. } => result,
            event => panic!("expected S3 HEAD event, got {event:?}"),
        };
        let _ = server.finish();

        // Assert
        assert!(matches!(
            result,
            CloudOutcome::Err(CloudError::Protocol(message)) if message.contains("ETag")
        ));
    }

    #[test]
    fn should_echo_provider_quoted_etag_given_s3_conditional_mutations() {
        // Arrange
        let head_server = spawn_recording_http_server(
            vec![
                ("Content-Length".into(), "5".into()),
                ("ETag".into(), "\"s3-etag\"".into()),
            ],
            Vec::new(),
        );
        let head_backend = recording_backend(head_server.endpoint.clone());
        let (head_sender, head_receiver) = std::sync::mpsc::channel();
        head_backend.submit_head("lease/primary", head_sender);
        let metadata = match head_receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("receive S3 HEAD result")
        {
            CloudEvent::Head {
                result: CloudOutcome::Ok(metadata),
                ..
            } => metadata,
            event => panic!("expected successful S3 HEAD event, got {event:?}"),
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
        assert_eq!(metadata.etag, "\"s3-etag\"");
        assert!(matches!(create_result, CloudOutcome::Ok(())));
        assert!(matches!(update_result, CloudOutcome::Ok(())));
        assert!(matches!(delete_result, CloudOutcome::Ok(())));
        assert_eq!(create_request.method, "PUT");
        assert!(
            create_request.target.ends_with("/bucket/lease/primary"),
            "unexpected S3 request target: {}",
            create_request.target
        );
        assert_eq!(create_request.header("if-none-match"), Some("*"));
        assert_eq!(update_request.header("if-match"), Some("\"s3-etag\""));
        assert_eq!(delete_request.header("if-match"), Some("\"s3-etag\""));
    }

    #[test]
    fn should_send_zero_content_length_when_put_body_is_empty() {
        // Arrange
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind strict PUT server");
        let endpoint = format!("http://{}", listener.local_addr().expect("server address"));
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept PUT request");
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(2)))
                .expect("set PUT request timeout");
            let mut bytes = Vec::new();
            let mut chunk = [0_u8; 1024];
            while !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = stream.read(&mut chunk).expect("read PUT request");
                if read == 0 {
                    break;
                }
                bytes.extend_from_slice(&chunk[..read]);
            }
            let request = String::from_utf8_lossy(&bytes).to_ascii_lowercase();
            let status = if request.contains("\r\ncontent-length: 0\r\n") {
                "200 OK"
            } else {
                "411 Length Required"
            };
            write!(
                stream,
                "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .expect("write PUT response");
            request
        });
        let config = S3Config::custom("bucket".into(), "us-east-1".into(), endpoint, true);
        let backend = S3Backend::new(config, CloudExecutor::new(None).expect("cloud executor"));
        let (tx, rx) = std::sync::mpsc::channel();

        // Act
        backend.submit_put("metadata/manifest.journal", Vec::new(), Vec::new(), tx);

        // Assert
        let event = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("receive empty PUT result");
        assert!(
            matches!(
                event,
                CloudEvent::Put {
                    result: CloudOutcome::Ok(()),
                    ..
                }
            ),
            "empty PUT should include Content-Length: 0: {event:?}"
        );
        let request = server.join().expect("strict PUT server panicked");
        assert!(request.contains("\r\ncontent-length: 0\r\n"));
    }

    #[test]
    fn should_reject_repeated_s3_list_continuation_token() {
        // Arrange
        let repeated_page = "<ListBucketResult><Contents><Key>sst/a.sst</Key></Contents><IsTruncated>true</IsTruncated><NextContinuationToken>repeat</NextContinuationToken></ListBucketResult>";
        let final_page = "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>";
        let server = spawn_scripted_http_server(vec![
            repeated_page.to_string(),
            repeated_page.to_string(),
            final_page.to_string(),
        ]);
        let config = S3Config::custom(
            "bucket".into(),
            "us-east-1".into(),
            server.endpoint.clone(),
            true,
        );
        let backend = S3Backend::new(config, CloudExecutor::new(None).unwrap());

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
    fn should_add_security_token_header_when_signing_with_session_credentials() {
        // Arrange
        let signer = SigV4Signer::new(AwsCredentials {
            access_key: "ASIAXXXXXTEST".into(),
            secret_key: "test-secret".into(),
            region: "us-east-1".into(),
            session_token: Some("session-token-abc".into()),
        });
        let mut request = CloudRequest::new(
            Method::GET,
            "https://bucket.s3.us-east-1.amazonaws.com/object".into(),
        );

        // Act
        let result = signer.sign(&mut request);

        // Assert
        assert!(result.is_ok());
        assert!(request.headers.iter().any(|(name, value)| name
            .eq_ignore_ascii_case("x-amz-security-token")
            && value == "session-token-abc"));
        assert!(request
            .headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("authorization")));
    }

    #[test]
    fn should_resolve_default_chain_from_environment_credentials() {
        // Arrange
        let _env_guard = s3_env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _environment = ScopedEnvironment::apply(&[
            ("AWS_ACCESS_KEY_ID", Some("AKIAXXXTESTENV".to_string())),
            ("AWS_SECRET_ACCESS_KEY", Some("test-env-secret".to_string())),
            ("AWS_SESSION_TOKEN", Some("test-env-token".to_string())),
        ]);
        let signer = SigV4Signer::new_default_chain("us-west-2".into());
        let mut request = CloudRequest::new(
            Method::PUT,
            "https://bucket.s3.us-west-2.amazonaws.com/key".into(),
        )
        .with_body(vec![1, 2, 3]);

        // Act
        let result = signer.sign(&mut request);

        // Assert
        assert!(result.is_ok());
        let auth = request
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("authorization"))
            .map(|(_, value)| value.clone())
            .unwrap_or_default();
        assert!(auth.contains("Credential=AKIAXXXTESTENV/"));
        assert!(request.headers.iter().any(|(name, value)| name
            .eq_ignore_ascii_case("x-amz-security-token")
            && value == "test-env-token"));
    }

    #[test]
    fn should_continue_default_chain_when_aws_environment_credentials_are_partial() {
        // Arrange
        let _env_guard = s3_env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let config_directory = tempfile::tempdir().expect("create AWS config directory");
        let credentials_file = config_directory.path().join("credentials");
        let config_file = config_directory.path().join("config");
        std::fs::write(
            &credentials_file,
            "[midge-partial-env]\naws_access_key_id=profile-access\naws_secret_access_key=profile-secret\n",
        )
        .expect("write profile credentials");
        std::fs::write(&config_file, "").expect("write empty AWS config");
        let _environment = ScopedEnvironment::apply(&[
            ("AWS_ACCESS_KEY_ID", Some("access-only".to_string())),
            ("AWS_ACCESS_KEY", None),
            ("AWS_SECRET_ACCESS_KEY", None),
            ("AWS_SECRET_KEY", None),
            (
                "AWS_SHARED_CREDENTIALS_FILE",
                Some(credentials_file.to_string_lossy().into_owned()),
            ),
            (
                "AWS_CONFIG_FILE",
                Some(config_file.to_string_lossy().into_owned()),
            ),
            ("AWS_PROFILE", Some("midge-partial-env".to_string())),
        ]);
        let provider = DefaultAwsCredentialProvider::new("us-east-1".into());

        // Act
        let result = provider.resolve_fresh().expect("profile fallback");

        // Assert
        assert_eq!(result.creds.access_key, "profile-access");
        assert_eq!(result.creds.secret_key, "profile-secret");
    }

    #[test]
    fn should_not_fall_through_when_configured_web_identity_fails() {
        // Arrange
        let _env_guard = s3_env_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let config_directory = tempfile::tempdir().expect("create empty AWS config directory");
        let credentials_file = config_directory.path().join("credentials");
        let config_file = config_directory.path().join("config");
        std::fs::write(&credentials_file, "").expect("write empty credentials file");
        std::fs::write(&config_file, "").expect("write empty config file");
        let missing_token = config_directory.path().join("missing-web-identity-token");
        let _environment = ScopedEnvironment::apply(&[
            ("AWS_ACCESS_KEY_ID", None),
            ("AWS_ACCESS_KEY", None),
            ("AWS_SECRET_ACCESS_KEY", None),
            ("AWS_SECRET_KEY", None),
            (
                "AWS_SHARED_CREDENTIALS_FILE",
                Some(credentials_file.to_string_lossy().into_owned()),
            ),
            (
                "AWS_CONFIG_FILE",
                Some(config_file.to_string_lossy().into_owned()),
            ),
            ("AWS_PROFILE", Some("midge-empty-profile".to_string())),
            (
                "AWS_ROLE_ARN",
                Some("arn:aws:iam::123456789012:role/midge".to_string()),
            ),
            (
                "AWS_WEB_IDENTITY_TOKEN_FILE",
                Some(missing_token.to_string_lossy().into_owned()),
            ),
            ("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", None),
            ("AWS_CONTAINER_CREDENTIALS_FULL_URI", None),
            ("AWS_EC2_METADATA_DISABLED", Some("true".to_string())),
        ]);
        let provider = DefaultAwsCredentialProvider::new("us-east-1".into());

        // Act
        let result = provider.resolve_fresh();

        // Assert
        assert!(matches!(
            result,
            Err(MidgeError::Internal(message))
                if message.contains("AWS_WEB_IDENTITY_TOKEN_FILE")
        ));
    }

    #[test]
    fn should_extract_all_s3_list_keys_from_compact_xml() {
        // Arrange
        let body = "<ListBucketResult><Contents><Key>a</Key></Contents><Contents><Key>b&amp;c</Key></Contents></ListBucketResult>";

        let keys = extract_xml_tag_values(body, "Key");

        // Act
        // Assert
        assert_eq!(keys, vec!["a".to_string(), "b&c".to_string()]);
    }

    #[test]
    fn should_build_s3_list_url_with_continuation_token() {
        // Arrange
        let config = S3Config::custom(
            "bucket".into(),
            "us-east-1".into(),
            "http://localhost:9000".into(),
            true,
        );
        let backend = S3Backend::new(config, CloudExecutor::new(None).unwrap());

        let url = backend.list_url("sst/", Some("token/with spaces"));

        // Act
        // Assert
        assert!(url.contains("list-type=2"));
        assert!(url.contains("prefix=sst%2F"));
        assert!(url.contains("continuation-token=token%2Fwith%20spaces"));
    }

    #[test]
    fn should_create_legacy_aws_provider_with_independent_backend() {
        // Arrange
        let creds = AwsCredentials::new("access".into(), "secret".into(), "us-east-1".into());

        // Act: `new()` is documented as a thin wrapper over `aws()` for callers pinned to the
        // legacy signature; the DTO plumbing itself (bucket/region/creds -> config) is exercised
        // end-to-end for the general S3-compatible path by the custom-endpoint test below, since
        // `aws()`'s hardcoded AWS host can't be pointed at a local mock server.
        let provider = S3Provider::new("aws-bucket".into(), "us-east-1".into(), creds)
            .expect("create legacy aws provider");

        // Assert: each provider owns a distinct backend instance rather than sharing state.
        let other = S3Provider::aws(
            "aws-bucket".into(),
            "us-east-1".into(),
            AwsCredentials::new("access".into(), "secret".into(), "us-east-1".into()),
        )
        .expect("create aws provider directly");
        assert!(!Arc::ptr_eq(&provider.backend(), &other.backend()));
    }

    #[test]
    fn should_create_custom_provider_targeting_path_style_endpoint() {
        // Arrange
        let server = spawn_recording_http_server(Vec::new(), Vec::new());
        let custom_config = S3Config::custom(
            "custom-bucket".into(),
            "us-east-1".into(),
            server.endpoint.clone(),
            true, // path-style: bucket appears in the URL path, not as a subdomain
        );

        // Act
        let provider = S3Provider::custom(custom_config, "access".into(), "secret".into())
            .expect("create custom provider");
        let backend = provider.backend();
        let (sender, receiver) = std::sync::mpsc::channel();
        backend.submit_put("object", b"value".to_vec(), Vec::new(), sender);
        let _ = receive_put_result(&receiver);
        let request = server.finish();

        // Assert: the request actually reached the configured endpoint, path-style, carrying the
        // configured bucket name in its request target.
        assert_eq!(request.method, "PUT");
        assert!(
            request.target.contains("/custom-bucket/"),
            "expected path-style target with bucket, got: {}",
            request.target
        );
    }
}
