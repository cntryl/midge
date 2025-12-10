#![cfg_attr(not(feature = "cloud-common"), allow(unused))]
//! Generic S3-compatible provider implementation
//!
//! Supports any S3-compatible storage:
//! - AWS S3 (with SigV4 credential handling)
//! - Wasabi (simple access key/secret)
//! - MinIO (local or cloud)
//! - Oracle Cloud Infrastructure (OCI S3 compatibility)
//! - Any other S3-compatible service
#![cfg_attr(not(feature = "cloud-common"), allow(dead_code))]

use crate::storage::cloud::{CloudCallback, CloudEvent, CloudOutcome};

#[cfg(feature = "cloud-common")]
use crate::common::{MidgeError, MidgeResult};
#[cfg(feature = "cloud-common")]
use crate::storage::cloud::executor::AwsCredentials;
#[cfg(feature = "cloud-common")]
use crate::storage::cloud::{
    CloudBackend, CloudExecutor, CloudRequest, CloudResponse, CloudSigner, ObjectMetadata,
};
#[cfg(feature = "cloud-common")]
use chrono::Utc;
#[cfg(feature = "cloud-common")]
use hex;
#[cfg(feature = "cloud-common")]
use hmac::{Hmac, Mac};
#[cfg(feature = "cloud-common")]
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
#[cfg(feature = "cloud-common")]
use reqwest::Method;
#[cfg(feature = "cloud-common")]
use sha2::Sha256;
#[cfg(feature = "cloud-common")]
use std::borrow::Cow;
#[cfg(feature = "cloud-common")]
use std::sync::Arc;
#[cfg(feature = "cloud-common")]
use url::Url;
#[cfg(feature = "cloud-common")]
use urlencoding::encode;

#[cfg(not(feature = "cloud-common"))]
/// Minimal stub provider when async cloud features are disabled.
pub struct S3Provider {
    bucket: String,
    region: String,
}

#[cfg(not(feature = "cloud-common"))]
impl S3Provider {
    pub fn new(bucket: String, region: String) -> Self {
        Self { bucket, region }
    }

    pub fn submit_put(&self, key: String, _data: Vec<u8>, callback: CloudCallback) {
        let event = CloudEvent::PutComplete {
            key,
            result: CloudOutcome::Err("cloud-common feature disabled".into()),
        };
        let _ = callback.send(event);
    }

    pub fn submit_get(&self, key: String, callback: CloudCallback) {
        let event = CloudEvent::GetComplete {
            key,
            result: CloudOutcome::Err("cloud-common feature disabled".into()),
        };
        let _ = callback.send(event);
    }

    pub fn submit_delete(&self, key: String, callback: CloudCallback) {
        let event = CloudEvent::DeleteComplete {
            key,
            result: CloudOutcome::Err("cloud-common feature disabled".into()),
        };
        let _ = callback.send(event);
    }

    pub fn submit_list(&self, prefix: String, callback: CloudCallback) {
        let event = CloudEvent::ListComplete {
            prefix,
            result: CloudOutcome::Err("cloud-common feature disabled".into()),
        };
        let _ = callback.send(event);
    }

    pub fn submit_head(&self, key: String, callback: CloudCallback) {
        let event = CloudEvent::HeadComplete {
            key,
            result: CloudOutcome::Err("cloud-common feature disabled".into()),
        };
        let _ = callback.send(event);
    }
}

#[cfg(feature = "cloud-common")]
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

/// Configuration for S3-compatible storage
#[cfg(feature = "cloud-common")]
#[derive(Clone, Debug)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub path_style: bool,
}

#[cfg(feature = "cloud-common")]
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

#[cfg(feature = "cloud-common")]
pub struct S3Provider {
    backend: Arc<dyn CloudBackend>,
}

#[cfg(feature = "cloud-common")]
impl S3Provider {
    /// Create provider with AWS credentials (SigV4 signing)
    pub fn aws(bucket: String, region: String, creds: AwsCredentials) -> Self {
        let config = S3Config::aws(bucket, region);
        Self::with_config(config, Some(creds))
    }

    /// Create provider for Wasabi (simple access key/secret)
    pub fn wasabi(bucket: String, region: String, access_key: String, secret_key: String) -> Self {
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
    pub fn minio(bucket: String, endpoint: String, access_key: String, secret_key: String) -> Self {
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
    ) -> Self {
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
    pub fn custom(
        config: S3Config,
        access_key: String,
        secret_key: String,
    ) -> Self {
        let creds = AwsCredentials {
            access_key,
            secret_key,
            region: config.region.clone(),
            session_token: None,
        };
        Self::with_config(config, Some(creds))
    }

    /// Create provider with full config and optional credentials
    fn with_config(config: S3Config, creds: Option<AwsCredentials>) -> Self {
        let signer = creds.map(|c| Arc::new(SigV4Signer::new(c)) as Arc<dyn CloudSigner>);
        let executor = CloudExecutor::new(signer);
        let backend = Arc::new(S3Backend::new(config, executor));
        Self { backend }
    }

    /// Legacy constructor (AWS with explicit credentials)
    pub fn new(bucket: String, region: String, creds: AwsCredentials) -> Self {
        Self::aws(bucket, region, creds)
    }

    pub fn backend(&self) -> Arc<dyn CloudBackend> {
        Arc::clone(&self.backend)
    }
}

#[cfg(feature = "cloud-common")]
struct S3Backend {
    config: S3Config,
    executor: CloudExecutor,
}

#[cfg(feature = "cloud-common")]
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
                let protocol = if endpoint.starts_with("https") { "https" } else { "http" };
                format!("{}://{}.{}", protocol, self.config.bucket, endpoint_without_protocol)
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

#[cfg(feature = "cloud-common")]
impl CloudBackend for S3Backend {
    fn submit_put(&self, key: String, data: Vec<u8>, callback: CloudCallback) {
        let url = self.object_url(&key);
        let request = CloudRequest::new(Method::PUT, url).with_body(data);
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

#[cfg(feature = "cloud-common")]
impl CloudSigner for SigV4Signer {
    fn sign(&self, request: &mut CloudRequest) -> MidgeResult<()> {
        let url = Url::parse(&request.url)
            .map_err(|err| MidgeError::InvalidArgument(format!("url parse: {}", err)))?;
        let host = url
            .host_str()
            .ok_or_else(|| MidgeError::InvalidArgument("missing host".to_string()))?;
        request
            .headers
            .retain(|(name, _)| !name.eq_ignore_ascii_case("host"));
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

        let signing_key = self.signing_key(&date)?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&signing_key)
            .map_err(|_| MidgeError::Internal("hmac init".to_string()))?;
        mac.update(string_to_sign.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        let auth_header = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            format!("{}/{}", self.creds.access_key, credential_scope),
            credential_scope,
            signed_headers,
            signature
        );

        request.headers.push(("Authorization".into(), auth_header));
        Ok(())
    }
}

#[cfg(feature = "cloud-common")]
struct SigV4Signer {
    creds: AwsCredentials,
}

#[cfg(feature = "cloud-common")]
impl SigV4Signer {
    fn new(creds: AwsCredentials) -> Self {
        Self { creds }
    }

    fn signing_key(&self, date: &str) -> MidgeResult<Vec<u8>> {
        let k_date = hmac_sha256(
            format!("AWS4{}", self.creds.secret_key).as_bytes(),
            date.as_bytes(),
        )?;
        let k_region = hmac_sha256(&k_date, self.creds.region.as_bytes())?;
        let k_service = hmac_sha256(&k_region, b"s3")?;
        hmac_sha256(&k_service, b"aws4_request")
    }

    fn region(&self) -> &str {
        &self.creds.region
    }
}

#[cfg(feature = "cloud-common")]
fn hmac_sha256(key: &[u8], data: &[u8]) -> MidgeResult<Vec<u8>> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key)
        .map_err(|_| MidgeError::Internal("hmac init".to_string()))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

#[cfg(feature = "cloud-common")]
fn percent_encode_path(path: &str) -> String {
    utf8_percent_encode(path, ENCODE_SET).to_string()
}

#[cfg(feature = "cloud-common")]
fn canonicalize_query(query: &str) -> String {
    let mut pairs: Vec<Cow<'_, str>> = url::form_urlencoded::parse(query.as_bytes())
        .map(|(key, value)| format!("{}={}", encode(&key), encode(&value)))
        .collect();
    pairs.sort();
    pairs.join("&")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(not(feature = "cloud-common"))]
    fn stub_provider_exists() {
        let provider = S3Provider::new("bucket".into(), "region".into());
        provider.submit_put("key".into(), vec![], std::sync::mpsc::channel().0);
    }
}
