//! Generic S3-compatible provider implementation
//!
//! Supports any S3-compatible storage:
//! - AWS S3 (with SigV4 credential handling)
//! - Wasabi (simple access key/secret)
//! - MinIO (local or cloud)
//! - Oracle Cloud Infrastructure (OCI S3 compatibility)
//! - Any other S3-compatible service

use super::super::cloud::{
    CloudBackend, CloudExecutor, CloudRequest, CloudResponse, CloudSigner, ObjectMetadata,
};
use super::super::cloud::{CloudCallback, CloudEvent, CloudOutcome};
use crate::common::{MidgeError, MidgeResult};
use chrono::Utc;
use hex;
use hmac::{Hmac, KeyInit, Mac};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use reqwest::blocking::Client;
use reqwest::Method;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use url::Url;
use urlencoding::encode;

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

#[derive(Clone)]
pub struct AwsCredentials {
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    pub session_token: Option<String>,
}

impl AwsCredentials {
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

        if let Ok(web_identity) = self.resolve_web_identity() {
            return Ok(web_identity);
        }

        if let Ok(ecs) = self.resolve_ecs_task_role() {
            return Ok(ecs);
        }

        if let Ok(ec2) = self.resolve_ec2_imds() {
            return Ok(ec2);
        }

        Err(MidgeError::InvalidArgument(
            "no AWS credentials found (checked env, IRSA web identity, ECS task role, and EC2 IMDS)"
                .into(),
        ))
    }

    fn resolve_env_credentials(&self) -> Option<AwsCredentials> {
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
            region: self.region.clone(),
            session_token,
        })
    }

    fn resolve_web_identity(&self) -> MidgeResult<CachedAwsCredentials> {
        let role_arn = std::env::var("AWS_ROLE_ARN")
            .map_err(|_| MidgeError::InvalidArgument("AWS_ROLE_ARN is not set".into()))?;
        let token_file = std::env::var("AWS_WEB_IDENTITY_TOKEN_FILE").map_err(|_| {
            MidgeError::InvalidArgument("AWS_WEB_IDENTITY_TOKEN_FILE is not set".into())
        })?;
        let token = std::fs::read_to_string(&token_file).map_err(|e| {
            MidgeError::Internal(format!(
                "failed to read AWS_WEB_IDENTITY_TOKEN_FILE '{}': {}",
                token_file, e
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
        let endpoint = format!("https://sts.{}.amazonaws.com/", self.region);
        let body = format!(
            "Action=AssumeRoleWithWebIdentity&Version=2011-06-15&RoleArn={}&RoleSessionName={}&WebIdentityToken={}",
            encode(&role_arn),
            encode(&session_name),
            encode(token)
        );

        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| MidgeError::Internal(format!("sts client init failed: {}", e)))?;
        let resp = client
            .post(&endpoint)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .map_err(|e| MidgeError::Internal(format!("sts assume role call failed: {}", e)))?;

        if !resp.status().is_success() {
            return Err(MidgeError::Internal(format!(
                "sts assume role failed with status {}",
                resp.status()
            )));
        }

        let xml = resp
            .text()
            .map_err(|e| MidgeError::Internal(format!("failed to read sts response: {}", e)))?;

        let access_key = extract_xml_tag(&xml, "AccessKeyId")?;
        let secret_key = extract_xml_tag(&xml, "SecretAccessKey")?;
        let session_token = extract_xml_tag(&xml, "SessionToken")?;
        let expiry = extract_xml_tag(&xml, "Expiration")
            .ok()
            .and_then(|s| parse_rfc3339_epoch(&s));

        Ok(CachedAwsCredentials {
            creds: AwsCredentials {
                access_key,
                secret_key,
                region: self.region.clone(),
                session_token: Some(session_token),
            },
            expires_at: expiry,
        })
    }

    fn resolve_ecs_task_role(&self) -> MidgeResult<CachedAwsCredentials> {
        let endpoint =
            if let Ok(relative_uri) = std::env::var("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") {
                format!("http://169.254.170.2{}", relative_uri)
            } else if let Ok(full_uri) = std::env::var("AWS_CONTAINER_CREDENTIALS_FULL_URI") {
                full_uri
            } else {
                return Err(MidgeError::InvalidArgument(
                    "ECS credential endpoint variables are not set".into(),
                ));
            };

        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .map_err(|e| MidgeError::Internal(format!("ecs client init failed: {}", e)))?;

        let mut request = client.get(&endpoint);
        if let Ok(auth_token) = std::env::var("AWS_CONTAINER_AUTHORIZATION_TOKEN") {
            request = request.header("Authorization", auth_token);
        } else if let Ok(token_file) = std::env::var("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE") {
            let token = std::fs::read_to_string(&token_file)
                .map_err(|e| MidgeError::Internal(format!("failed to read token file: {}", e)))?;
            request = request.header("Authorization", token.trim());
        }

        let resp = request
            .send()
            .map_err(|e| MidgeError::Internal(format!("ecs credential fetch failed: {}", e)))?;
        if !resp.status().is_success() {
            return Err(MidgeError::Internal(format!(
                "ecs credential endpoint returned {}",
                resp.status()
            )));
        }
        let text = resp
            .text()
            .map_err(|e| MidgeError::Internal(format!("failed to read ecs response: {}", e)))?;
        self.parse_json_credentials(&text)
    }

    fn resolve_ec2_imds(&self) -> MidgeResult<CachedAwsCredentials> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(3))
            .build()
            .map_err(|e| MidgeError::Internal(format!("imds client init failed: {}", e)))?;

        let token = client
            .put("http://169.254.169.254/latest/api/token")
            .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
            .send()
            .ok()
            .and_then(|resp| {
                if resp.status().is_success() {
                    resp.text().ok()
                } else {
                    None
                }
            });

        let mut role_req =
            client.get("http://169.254.169.254/latest/meta-data/iam/security-credentials/");
        if let Some(ref token_value) = token {
            role_req = role_req.header("X-aws-ec2-metadata-token", token_value);
        }
        let role_resp = role_req
            .send()
            .map_err(|e| MidgeError::Internal(format!("imds role name fetch failed: {}", e)))?;
        if !role_resp.status().is_success() {
            return Err(MidgeError::Internal(format!(
                "imds role name endpoint returned {}",
                role_resp.status()
            )));
        }
        let role_name = role_resp
            .text()
            .map_err(|e| MidgeError::Internal(format!("failed to read imds role name: {}", e)))?
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

        let creds_url = format!(
            "http://169.254.169.254/latest/meta-data/iam/security-credentials/{}",
            role_name
        );
        let mut creds_req = client.get(&creds_url);
        if let Some(ref token_value) = token {
            creds_req = creds_req.header("X-aws-ec2-metadata-token", token_value);
        }
        let creds_resp = creds_req
            .send()
            .map_err(|e| MidgeError::Internal(format!("imds credential fetch failed: {}", e)))?;
        if !creds_resp.status().is_success() {
            return Err(MidgeError::Internal(format!(
                "imds credentials endpoint returned {}",
                creds_resp.status()
            )));
        }
        let text = creds_resp
            .text()
            .map_err(|e| MidgeError::Internal(format!("failed to read imds credentials: {}", e)))?;
        self.parse_json_credentials(&text)
    }

    fn parse_json_credentials(&self, payload: &str) -> MidgeResult<CachedAwsCredentials> {
        let json: Value = serde_json::from_str(payload)
            .map_err(|e| MidgeError::Internal(format!("invalid credential json: {}", e)))?;

        let access_key = json
            .get("AccessKeyId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| MidgeError::Internal("missing AccessKeyId".into()))?
            .to_string();
        let secret_key = json
            .get("SecretAccessKey")
            .and_then(|v| v.as_str())
            .ok_or_else(|| MidgeError::Internal("missing SecretAccessKey".into()))?
            .to_string();
        let session_token = json
            .get("Token")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| {
                json.get("SessionToken")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            });
        let expires_at = json
            .get("Expiration")
            .and_then(|v| v.as_str())
            .and_then(parse_rfc3339_epoch);

        Ok(CachedAwsCredentials {
            creds: AwsCredentials {
                access_key,
                secret_key,
                region: self.region.clone(),
                session_token,
            },
            expires_at,
        })
    }
}

fn extract_xml_tag(xml: &str, tag: &str) -> MidgeResult<String> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    if let Some(start) = xml.find(&open) {
        let start_index = start + open.len();
        if let Some(end_rel) = xml[start_index..].find(&close) {
            let end_index = start_index + end_rel;
            return Ok(xml[start_index..end_index].trim().to_string());
        }
    }
    Err(MidgeError::Internal(format!(
        "missing {} in sts response",
        tag
    )))
}

fn parse_rfc3339_epoch(value: &str) -> Option<u64> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|dt| dt.timestamp().max(0) as u64)
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

    /// Create config for Wasabi
    pub fn wasabi(bucket: String, region: String) -> Self {
        Self {
            bucket,
            region: region.clone(),
            endpoint: Some(format!("https://s3.{}.wasabisys.com", region)),
            path_style: false,
        }
    }

    /// Create config for MinIO
    pub fn minio(bucket: String, endpoint: String) -> Self {
        Self {
            bucket,
            region: "us-east-1".to_string(),
            endpoint: Some(endpoint),
            path_style: true,
        }
    }

    /// Create config for OCI S3 compatibility
    pub fn oci_s3_compat(bucket: String, namespace: String, region: String) -> Self {
        Self {
            bucket,
            region: region.clone(),
            endpoint: Some(format!(
                "https://{}.compat.objectstorage.{}.oraclecloud.com",
                namespace, region
            )),
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
    /// Create provider with AWS credentials (SigV4 signing)
    pub fn aws(bucket: String, region: String, creds: AwsCredentials) -> MidgeResult<Self> {
        let config = S3Config::aws(bucket, region);
        Self::with_config(config, Some(creds))
    }

    /// Create AWS provider using the default AWS credential chain.
    ///
    /// Resolution order:
    /// 1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional session token)
    /// 2. EKS/IRSA web identity (`AWS_ROLE_ARN` + `AWS_WEB_IDENTITY_TOKEN_FILE`)
    /// 3. ECS/Fargate task role endpoint
    /// 4. EC2 Instance Metadata Service (IMDS)
    pub fn aws_default(bucket: String, region: String) -> MidgeResult<Self> {
        let config = S3Config::aws(bucket, region.clone());
        let signer: Arc<dyn CloudSigner> = Arc::new(SigV4Signer::new_default_chain(region));
        Self::with_signer(config, Some(signer))
    }

    /// Create provider for Wasabi (simple access key/secret)
    pub fn wasabi(
        bucket: String,
        region: String,
        access_key: String,
        secret_key: String,
    ) -> MidgeResult<Self> {
        let config = S3Config::wasabi(bucket, region.clone());
        let creds = AwsCredentials {
            access_key,
            secret_key,
            region,
            session_token: None,
        };
        Self::with_config(config, Some(creds))
    }

    /// Create provider for MinIO (access key/secret)
    pub fn minio(
        bucket: String,
        endpoint: String,
        access_key: String,
        secret_key: String,
    ) -> MidgeResult<Self> {
        let config = S3Config::minio(bucket, endpoint);
        let creds = AwsCredentials {
            access_key,
            secret_key,
            region: "us-east-1".to_string(),
            session_token: None,
        };
        Self::with_config(config, Some(creds))
    }

    /// Create provider for OCI S3 compatibility
    pub fn oci_s3_compat(
        bucket: String,
        namespace: String,
        region: String,
        access_key: String,
        secret_key: String,
    ) -> MidgeResult<Self> {
        let config = S3Config::oci_s3_compat(bucket, namespace, region.clone());
        let creds = AwsCredentials {
            access_key,
            secret_key,
            region,
            session_token: None,
        };
        Self::with_config(config, Some(creds))
    }

    /// Create provider with custom S3-compatible endpoint
    pub fn custom(config: S3Config, access_key: String, secret_key: String) -> MidgeResult<Self> {
        let creds = AwsCredentials {
            access_key,
            secret_key,
            region: config.region.clone(),
            session_token: None,
        };
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

    fn canonical_key(&self, key: &str) -> String {
        key.split('/')
            .map(|segment| utf8_percent_encode(segment, ENCODE_SET).to_string())
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
            format!(
                "https://{}.s3.{}.amazonaws.com",
                self.config.bucket, self.config.region
            )
        }
    }

    fn object_url(&self, key: &str) -> String {
        format!("{}/{}", self.base_url(), self.canonical_key(key))
    }

    fn list_url(&self, prefix: &str) -> String {
        format!("{}?list-type=2&prefix={}", self.base_url(), encode(prefix))
    }
}

impl CloudBackend for S3Backend {
    fn submit_put(
        &self,
        key: String,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        let url = self.object_url(&key);
        let mut request = CloudRequest::new(Method::PUT, url).with_body(data);
        // Apply provided headers (e.g. conditional headers like If-None-Match)
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
                result: CloudOutcome::Err(format!("unexpected status {}", resp.status)),
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
                result: CloudOutcome::Err(format!("status {}", resp.status)),
            },
            Err(err) => CloudEvent::GetComplete {
                key: ctx,
                result: CloudOutcome::Err(format!("{:?}", err)),
            },
        };
        self.executor.spawn_request(request, key, callback, mapper);
    }

    fn submit_get_range(&self, key: String, start: u64, end: Option<u64>, callback: CloudCallback) {
        let url = self.object_url(&key);
        let mut request = CloudRequest::new(Method::GET, url);
        let range = match end {
            Some(e) => format!("bytes={}-{}", start, e.saturating_sub(1)),
            None => format!("bytes={}-", start),
        };
        request = request.with_header("Range", range);
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
                result: CloudOutcome::Err(format!("status {}", resp.status)),
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
                result: CloudOutcome::Err(format!("status {}", resp.status)),
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
                for line in body.lines() {
                    if let Some(start) = line.find("<Key>") {
                        if let Some(end) = line.find("</Key>") {
                            items.push(line[start + 5..end].to_string());
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
                result: CloudOutcome::Err(format!("status {}", resp.status)),
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
                    .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                    .and_then(|(_, value)| value.parse().ok())
                    .unwrap_or(0);
                let etag = resp
                    .headers
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case("etag"))
                    .map(|(_, value)| value.trim_matches('"').to_string())
                    .unwrap_or_default();
                let metadata = ObjectMetadata::new(size, etag, 0);
                CloudEvent::HeadComplete {
                    key: ctx,
                    result: CloudOutcome::Ok(metadata),
                }
            }
            Ok(resp) => CloudEvent::HeadComplete {
                key: ctx,
                result: CloudOutcome::Err(format!("status {}", resp.status)),
            },
            Err(err) => CloudEvent::HeadComplete {
                key: ctx,
                result: CloudOutcome::Err(format!("{:?}", err)),
            },
        };
        self.executor.spawn_request(request, key, callback, mapper);
    }
}

impl CloudSigner for SigV4Signer {
    fn sign(&self, request: &mut CloudRequest) -> MidgeResult<()> {
        let creds = self.resolve_credentials()?;
        let url = Url::parse(&request.url)
            .map_err(|err| MidgeError::InvalidArgument(format!("url parse: {}", err)))?;
        let host = url
            .host_str()
            .ok_or_else(|| MidgeError::InvalidArgument("missing host".to_string()))?;
        request.headers.retain(|(name, _)| {
            !name.eq_ignore_ascii_case("host")
                && !name.eq_ignore_ascii_case("x-amz-date")
                && !name.eq_ignore_ascii_case("x-amz-content-sha256")
                && !name.eq_ignore_ascii_case("x-amz-security-token")
                && !name.eq_ignore_ascii_case("authorization")
        });
        request.headers.push(("Host".into(), host.to_string()));
        let now = Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date = now.format("%Y%m%d").to_string();
        request
            .headers
            .push(("X-Amz-Date".into(), amz_date.clone()));
        request
            .headers
            .push(("X-Amz-Content-Sha256".into(), "UNSIGNED-PAYLOAD".into()));
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
        let canonical_headers = header_pairs
            .iter()
            .map(|(name, value)| format!("{}:{}\n", name, value))
            .collect::<String>();

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
            "{}\n{}\n{}\n{}\n{}\nUNSIGNED-PAYLOAD",
            request.method.as_str(),
            percent_encode_path(path),
            canonical_query,
            canonical_headers,
            signed_headers
        );

        let mut hasher = Sha256::new();
        hasher.update(canonical_request.as_bytes());
        let canonical_hash = hex::encode(hasher.finalize());

        let credential_scope = format!("{}/s3/aws4_request", date);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}/{}\n{}",
            amz_date,
            date,
            self.region(),
            canonical_hash
        );

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
        let k_date = hmac_sha256(format!("AWS4{}", secret_key).as_bytes(), date.as_bytes())?;
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
    utf8_percent_encode(path, ENCODE_SET).to_string()
}

fn canonicalize_query(query: &str) -> String {
    let mut pairs: Vec<String> = url::form_urlencoded::parse(query.as_bytes())
        .map(|(key, value)| format!("{}={}", encode(&key), encode(&value)))
        .collect();
    pairs.sort();
    pairs.join("&")
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::Method;

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
        std::env::set_var("AWS_ACCESS_KEY_ID", "AKIAXXXTESTENV");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test-env-secret");
        std::env::set_var("AWS_SESSION_TOKEN", "test-env-token");
        let signer = SigV4Signer::new_default_chain("us-west-2".into());
        let mut request = CloudRequest::new(
            Method::PUT,
            "https://bucket.s3.us-west-2.amazonaws.com/key".into(),
        )
        .with_body(vec![1, 2, 3]);

        // Act
        let result = signer.sign(&mut request);

        // Assert
        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        std::env::remove_var("AWS_SESSION_TOKEN");
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
}
