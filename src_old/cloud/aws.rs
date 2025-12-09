//! AWS S3 cloud storage backend
//!
//! Provides S3 implementation using REST API with AWS Signature V4 authentication.
//! No heavy SDK dependencies - just HTTP + signing.
//! Requires the `cloud-aws` feature flag.
//!
//! # Architecture
//!
//! Uses `ureq` HTTP client with manual AWS Signature V4 signing.
//! Credentials are obtained from:
//! 1. **Environment variables** (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)
//! 2. **ECS task metadata** (Fargate, ECS on EC2) - via AWS_CONTAINER_CREDENTIALS_RELATIVE_URI
//! 3. **EC2 instance metadata** (IMDSv2) - via 169.254.169.254
//!
//! This supports running as an IAM role in any AWS environment:
//! - **Fargate**: Task role via 169.254.170.2 (ECS metadata endpoint)
//! - **ECS on EC2**: Task role via 169.254.170.2 (ECS metadata endpoint)
//! - **EC2**: Instance profile via 169.254.169.254 (IMDSv2)
//! - **Lambda**: Environment variables (automatically injected)
//! - **Local/Dev**: Environment variables (aws configure)
//!
//! # Example
//!
//! ```no_run
//! use cntryl_midge::cloud::aws::AwsS3Backend;
//! use cntryl_midge::cloud::StorageBackend;
//!
//! let backend = AwsS3Backend::new("my-bucket", "us-west-2").unwrap();
//! // Use backend for WAL/SST operations
//! ```

use crate::cloud::backend::{BlobMeta, StorageBackend};
use crate::error::{MidgeError, MidgeResult};
use base64;
use bytes::Bytes;
use hmac;
use md5;
use parking_lot::Mutex;
use sha2::Digest;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::{debug, info, warn};
use ureq;
use url;

/// AWS credentials
#[derive(Clone)]
struct AwsCredentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

impl AwsCredentials {
    /// Get credentials from environment variables or instance metadata
    fn load() -> MidgeResult<Self> {
        // Try environment variables first
        if let (Ok(access_key), Ok(secret_key)) = (
            std::env::var("AWS_ACCESS_KEY_ID"),
            std::env::var("AWS_SECRET_ACCESS_KEY"),
        ) {
            let session_token = std::env::var("AWS_SESSION_TOKEN").ok();
            debug!("Loaded AWS credentials from environment variables");
            return Ok(Self {
                access_key_id: access_key,
                secret_access_key: secret_key,
                session_token,
            });
        }

        // Try ECS task metadata (Fargate, ECS on EC2)
        if let Ok(uri) = std::env::var("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") {
            debug!("Detected ECS/Fargate environment");
            return Self::from_ecs(&uri);
        }

        // Fall back to EC2 instance metadata (IMDSv2)
        debug!("Falling back to EC2 instance metadata");
        Self::from_imds()
    }

    /// Get credentials from ECS task metadata service (Fargate, ECS on EC2)
    ///
    /// The ECS container agent provides credentials via a local HTTP endpoint.
    /// The relative URI is provided in AWS_CONTAINER_CREDENTIALS_RELATIVE_URI.
    fn from_ecs(relative_uri: &str) -> MidgeResult<Self> {
        debug!(
            "Fetching credentials from ECS task metadata: {}",
            relative_uri
        );

        // ECS metadata endpoint is at 169.254.170.2
        let url = format!("http://169.254.170.2{}", relative_uri);

        let creds_json: serde_json::Value = ureq::get(&url).call()?.into_body().read_json()?;

        Ok(Self {
            access_key_id: creds_json["AccessKeyId"]
                .as_str()
                .ok_or_else(|| MidgeError::cloud_error("Missing AccessKeyId in ECS credentials"))?
                .to_string(),
            secret_access_key: creds_json["SecretAccessKey"]
                .as_str()
                .ok_or_else(|| {
                    MidgeError::cloud_error("Missing SecretAccessKey in ECS credentials")
                })?
                .to_string(),
            session_token: creds_json["Token"].as_str().map(|s| s.to_string()),
        })
    }

    /// Get credentials from EC2 instance metadata service (IMDSv2)
    fn from_imds() -> MidgeResult<Self> {
        debug!("Attempting to fetch credentials from EC2 instance metadata");

        // Step 1: Get IMDSv2 session token
        let token = ureq::put("http://169.254.169.254/latest/api/token")
            .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
            .send(&[])?
            .into_body()
            .read_to_string()?;

        // Step 2: Get IAM role name
        let role_name =
            ureq::get("http://169.254.169.254/latest/meta-data/iam/security-credentials/")
                .header("X-aws-ec2-metadata-token", &token)
                .call()?
                .into_body()
                .read_to_string()?;

        // Step 3: Get credentials for the role
        let creds_json: serde_json::Value = ureq::get(&format!(
            "http://169.254.169.254/latest/meta-data/iam/security-credentials/{}",
            role_name.trim()
        ))
        .header("X-aws-ec2-metadata-token", &token)
        .call()?
        .into_body()
        .read_json()?;

        Ok(Self {
            access_key_id: creds_json["AccessKeyId"]
                .as_str()
                .ok_or_else(|| MidgeError::cloud_error("Missing AccessKeyId in credentials"))?
                .to_string(),
            secret_access_key: creds_json["SecretAccessKey"]
                .as_str()
                .ok_or_else(|| MidgeError::cloud_error("Missing SecretAccessKey in credentials"))?
                .to_string(),
            session_token: creds_json["Token"].as_str().map(|s| s.to_string()),
        })
    }
}

/// AWS S3 backend using REST API
pub struct AwsS3Backend {
    agent: ureq::Agent,
    bucket: String,
    region: String,
    credentials: AwsCredentials,
    /// ETag cache for conditional downloads (avoids re-downloading unchanged files)
    etag_cache: Arc<Mutex<std::collections::HashMap<String, String>>>,
    max_retries: u32,
    initial_retry_delay_ms: u64,
}

impl AwsS3Backend {
    /// Create a new S3 backend
    ///
    /// # Arguments
    /// * `bucket` - S3 bucket name
    /// * `region` - AWS region (e.g., "us-west-2")
    pub fn new(bucket: &str, region: &str) -> MidgeResult<Self> {
        info!(
            "Initializing AWS S3 backend: bucket={}, region={}",
            bucket, region
        );

        let credentials = AwsCredentials::load()?;

        // Configure ureq agent with connection pooling and timeouts
        // ureq 3.x handles connection pooling automatically
        let agent = ureq::agent();

        debug!("S3 backend initialized successfully");

        Ok(Self {
            agent,
            bucket: bucket.to_string(),
            region: region.to_string(),
            credentials,
            etag_cache: Arc::new(Mutex::new(std::collections::HashMap::new())),
            max_retries: 3,
            initial_retry_delay_ms: 100,
        })
    }

    /// Retry a closure with exponential backoff
    fn retry_with_backoff<F, T>(&self, mut operation: F) -> MidgeResult<T>
    where
        F: FnMut() -> MidgeResult<T>,
    {
        let mut attempt = 0;
        loop {
            match operation() {
                Ok(result) => return Ok(result),
                Err(e) => {
                    // Check if error is retryable
                    let is_retryable = match &e {
                        MidgeError::Http(msg) => {
                            // Retry on 5xx server errors, rate limiting, and throttling
                            msg.contains("500")
                                || msg.contains("502")
                                || msg.contains("503")
                                || msg.contains("504")
                                || msg.contains("429")
                                || msg.contains("SlowDown")
                        }
                        MidgeError::Io(_) => true, // Network errors are retryable
                        _ => false,
                    };

                    if !is_retryable || attempt >= self.max_retries {
                        return Err(e);
                    }

                    attempt += 1;
                    let delay = std::time::Duration::from_millis(
                        self.initial_retry_delay_ms * (1 << (attempt - 1)),
                    );
                    debug!("Retry attempt {} after {:?} due to: {}", attempt, delay, e);
                    std::thread::sleep(delay);
                }
            }
        }
    }

    /// Generate S3 URL
    fn s3_url(&self, key: &str) -> String {
        format!(
            "https://{}.s3.{}.amazonaws.com/{}",
            self.bucket, self.region, key
        )
    }

    /// Get object metadata without downloading (HEAD request)
    ///
    /// Returns (size_bytes, etag) or None if object doesn't exist
    fn head_object(&self, key: &str) -> MidgeResult<Option<(u64, String)>> {
        let url = self.s3_url(key);
        let empty_hash = format!("{:x}", sha2::Sha256::digest(b""));
        let auth = self.sign_request("HEAD", &url, &empty_hash, &[]);

        match self.agent.head(&url).header("Authorization", &auth).call() {
            Ok(response) => {
                let size = response
                    .headers()
                    .get("Content-Length")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);

                let etag = response
                    .headers()
                    .get("ETag")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .trim_matches('"')
                    .to_string();

                Ok(Some((size, etag)))
            }
            Err(_) => Ok(None), // Object doesn't exist
        }
    }

    /// Sign an AWS request with Signature V4
    fn sign_request(
        &self,
        method: &str,
        url: &str,
        payload_hash: &str,
        headers: &[(&str, &str)],
    ) -> String {
        use hmac::{Hmac, Mac};
        use sha2::{Digest, Sha256};

        type HmacSha256 = Hmac<Sha256>;

        // Parse URL
        let parsed_url = url::Url::parse(url).expect("Valid URL");
        let host = parsed_url.host_str().expect("Host");
        let path = parsed_url.path();
        let query = parsed_url.query().unwrap_or("");

        // Get timestamp
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time");
        let timestamp = chrono::DateTime::from_timestamp(now.as_secs() as i64, 0)
            .expect("Valid timestamp")
            .format("%Y%m%dT%H%M%SZ")
            .to_string();
        let date = &timestamp[0..8];

        // Canonical request
        let mut canonical_headers = format!("host:{}\nx-amz-date:{}\n", host, timestamp);
        if let Some(token) = &self.credentials.session_token {
            canonical_headers.push_str(&format!("x-amz-security-token:{}\n", token));
        }
        for (k, v) in headers {
            canonical_headers.push_str(&format!("{}:{}\n", k.to_lowercase(), v));
        }

        let signed_headers = if self.credentials.session_token.is_some() {
            "host;x-amz-date;x-amz-security-token"
        } else {
            "host;x-amz-date"
        };

        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method, path, query, canonical_headers, signed_headers, payload_hash
        );

        let canonical_hash = format!("{:x}", Sha256::digest(canonical_request.as_bytes()));

        // String to sign
        let credential_scope = format!("{}/{}/s3/aws4_request", date, self.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            timestamp, credential_scope, canonical_hash
        );

        // Signing key
        let k_date = HmacSha256::new_from_slice(
            format!("AWS4{}", self.credentials.secret_access_key).as_bytes(),
        )
        .expect("HMAC")
        .chain_update(date.as_bytes())
        .finalize()
        .into_bytes();

        let k_region = HmacSha256::new_from_slice(&k_date)
            .expect("HMAC")
            .chain_update(self.region.as_bytes())
            .finalize()
            .into_bytes();

        let k_service = HmacSha256::new_from_slice(&k_region)
            .expect("HMAC")
            .chain_update(b"s3")
            .finalize()
            .into_bytes();

        let k_signing = HmacSha256::new_from_slice(&k_service)
            .expect("HMAC")
            .chain_update(b"aws4_request")
            .finalize()
            .into_bytes();

        let signature = HmacSha256::new_from_slice(&k_signing)
            .expect("HMAC")
            .chain_update(string_to_sign.as_bytes())
            .finalize()
            .into_bytes();

        let signature_hex = hex::encode(signature);

        // Authorization header
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.credentials.access_key_id, credential_scope, signed_headers, signature_hex
        )
    }
}

impl StorageBackend for AwsS3Backend {
    fn put_blob(&self, key: &str, data: Bytes) -> MidgeResult<()> {
        debug!("Uploading blob to S3: {}", key);

        self.retry_with_backoff(|| {
            let url = self.s3_url(key);

            // Compute payload hash
            let payload_hash = format!("{:x}", sha2::Sha256::digest(&data));

            // Sign request
            let auth = self.sign_request(
                "PUT",
                &url,
                &payload_hash,
                &[("x-amz-content-sha256", &payload_hash)],
            );

            // Make request
            self.agent
                .put(&url)
                .header("Authorization", &auth)
                .header("x-amz-content-sha256", &payload_hash)
                .header("Content-Type", "application/octet-stream")
                .send(&data[..])?;

            Ok(())
        })?;

        debug!("Successfully uploaded blob: {}", key);
        Ok(())
    }

    fn get_blob(&self, key: &str) -> MidgeResult<Bytes> {
        debug!("Downloading blob from S3: {}", key);

        self.retry_with_backoff(|| {
            let url = self.s3_url(key);
            let empty_hash = format!("{:x}", sha2::Sha256::digest(b""));
            let auth = self.sign_request("GET", &url, &empty_hash, &[]);

            let response = self.agent.get(&url).header("Authorization", &auth).call()?;

            let data = response.into_body().read_to_vec()?;
            Ok(Bytes::from(data))
        })
    }

    fn get_blob_range(&self, key: &str, start: u64, end: Option<u64>) -> MidgeResult<Bytes> {
        let url = self.s3_url(key);
        let range_header = match end {
            Some(end_byte) => format!("bytes={}-{}", start, end_byte),
            None => format!("bytes={}-", start),
        };

        debug!("Downloading blob range from S3: {} ({})", key, range_header);

        let empty_hash = format!("{:x}", sha2::Sha256::digest(b""));
        let auth = self.sign_request("GET", &url, &empty_hash, &[("range", &range_header)]);

        let response = self
            .agent
            .get(&url)
            .header("Authorization", &auth)
            .header("Range", &range_header)
            .call()?;

        let data = response.into_body().read_to_vec()?;
        let bytes = Bytes::from(data);
        debug!(
            "Successfully downloaded blob range: {} ({} bytes)",
            key,
            bytes.len()
        );
        Ok(bytes)
    }

    fn delete_blob(&self, key: &str) -> MidgeResult<()> {
        let url = self.s3_url(key);
        debug!("Deleting blob from S3: {}", key);

        let empty_hash = format!("{:x}", sha2::Sha256::digest(b""));
        let auth = self.sign_request("DELETE", &url, &empty_hash, &[]);

        self.agent
            .delete(&url)
            .header("Authorization", &auth)
            .call()?;

        // Remove from ETag cache
        {
            let mut cache = self.etag_cache.lock();
            cache.remove(key);
        }

        debug!("Successfully deleted blob: {}", key);
        Ok(())
    }

    fn list_blobs(&self, prefix: &str) -> MidgeResult<Vec<String>> {
        debug!("Listing blobs from S3 with prefix: {}", prefix);

        let url = format!(
            "https://{}.s3.{}.amazonaws.com/?list-type=2&prefix={}",
            self.bucket, self.region, prefix
        );

        self.retry_with_backoff(|| {
            let empty_hash = format!("{:x}", sha2::Sha256::digest(b""));
            let auth = self.sign_request("GET", &url, &empty_hash, &[]);

            let response = self.agent.get(&url).header("Authorization", &auth).call()?;

            let xml = response.into_body().read_to_string()?;

            // Parse XML to extract object keys
            let mut blobs = Vec::new();

            for line in xml.lines() {
                let line = line.trim();

                // Extract key
                if let Some(key_start) = line.find("<Key>") {
                    if let Some(key_end) = line.find("</Key>") {
                        let key = &line[key_start + 5..key_end];
                        blobs.push(key.to_string());
                    }
                }
            }

            debug!("Found {} blobs", blobs.len());
            Ok(blobs)
        })
    }

    fn head_blob(&self, key: &str) -> MidgeResult<Option<BlobMeta>> {
        if let Some((size, etag)) = self.head_object(key)? {
            Ok(Some(BlobMeta {
                size,
                last_modified: Some(SystemTime::now()),
                etag: Some(etag),
            }))
        } else {
            Ok(None)
        }
    }

    fn put_blob_if_not_exists(&self, key: &str, data: Bytes) -> MidgeResult<String> {
        // Check if object exists first
        if self.head_blob(key)?.is_some() {
            return Err(MidgeError::internal("blob already exists"));
        }

        // Object doesn't exist, upload it
        self.put_blob(key, data)?;

        // Get the ETag of the uploaded blob
        let meta = self.head_blob(key)?;
        Ok(meta.and_then(|m| m.etag).unwrap_or_default())
    }
}

impl AwsS3Backend {
    /// Additional optimization methods (not part of CloudStorageBackend trait)
    ///
    /// Check if an object exists without downloading it
    /// Much faster than trying to download and catching errors
    pub fn exists(&self, key: &str) -> MidgeResult<bool> {
        Ok(self.head_object(key)?.is_some())
    }

    /// Get the size of an object without downloading it
    pub fn get_size(&self, key: &str) -> MidgeResult<Option<u64>> {
        Ok(self.head_object(key)?.map(|(size, _)| size))
    }

    /// Batch delete multiple objects (up to 1000 per call)
    ///
    /// This is MUCH more efficient than calling delete() in a loop:
    /// - Single API call instead of N calls
    /// - Single authentication signature
    /// - Reduced latency (1 RTT instead of N RTTs)
    /// - Lower cost (1 DELETE request instead of N)
    ///
    /// # Arguments
    /// * `keys` - List of object keys to delete (max 1000)
    ///
    /// # Example
    /// ```ignore
    /// // Delete 100 old SSTs in one call instead of 100 separate calls
    /// backend.batch_delete(&old_sst_keys)?;
    /// ```
    pub fn batch_delete(&self, keys: &[String]) -> MidgeResult<()> {
        if keys.is_empty() {
            return Ok(());
        }

        if keys.len() > 1000 {
            warn!("batch_delete limited to 1000 objects, got {}", keys.len());
            return Err(MidgeError::invalid_config(
                "batch_delete limited to 1000 objects",
            ));
        }

        // Build XML payload for batch delete
        let mut xml = String::from(r#"<?xml version="1.0" encoding="UTF-8"?><Delete>"#);
        for key in keys {
            xml.push_str(&format!("<Object><Key>{}</Key></Object>", xml_escape(key)));
        }
        xml.push_str("</Delete>");

        let url = format!(
            "https://{}.s3.{}.amazonaws.com/?delete",
            self.bucket, self.region
        );
        let payload_hash = format!("{:x}", sha2::Sha256::digest(xml.as_bytes()));
        let auth = self.sign_request(
            "POST",
            &url,
            &payload_hash,
            &[
                ("content-md5", &base64_md5(xml.as_bytes())),
                ("x-amz-content-sha256", &payload_hash),
            ],
        );

        self.agent
            .post(&url)
            .header("Authorization", &auth)
            .header("Content-Type", "application/xml")
            .header("Content-MD5", &base64_md5(xml.as_bytes()))
            .header("x-amz-content-sha256", &payload_hash)
            .send(xml.as_bytes())?;

        // Remove deleted keys from ETag cache
        {
            let mut cache = self.etag_cache.lock();
            for key in keys {
                cache.remove(key);
            }
        }

        debug!("Successfully batch deleted {} objects", keys.len());
        Ok(())
    }
}

/// Simple XML escaping for S3 batch operations
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Calculate base64-encoded MD5 hash (required for batch delete)
fn base64_md5(data: &[u8]) -> String {
    let hash = md5::compute(data);
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, hash.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_generate_correct_s3_url_for_wal_path() {
        // Arrange
        let etag_cache = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let backend = AwsS3Backend {
            agent: ureq::agent(),
            bucket: "test-bucket".to_string(),
            region: "us-west-2".to_string(),
            credentials: AwsCredentials {
                access_key_id: "test".to_string(),
                secret_access_key: "test".to_string(),
                session_token: None,
            },
            etag_cache,
            max_retries: 3,
            initial_retry_delay_ms: 100,
        };

        // Act
        let url = backend.s3_url("wal/0000000001.wal");

        // Assert
        assert_eq!(
            url,
            "https://test-bucket.s3.us-west-2.amazonaws.com/wal/0000000001.wal"
        );
    }

    #[test]
    fn should_generate_correct_s3_url_for_tenant_sst_path() {
        // Arrange
        let etag_cache = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let backend = AwsS3Backend {
            agent: ureq::agent(),
            bucket: "test-bucket".to_string(),
            region: "us-west-2".to_string(),
            credentials: AwsCredentials {
                access_key_id: "test".to_string(),
                secret_access_key: "test".to_string(),
                session_token: None,
            },
            etag_cache,
            max_retries: 3,
            initial_retry_delay_ms: 100,
        };

        // Act
        let url = backend.s3_url("tenant-123/sst/cf_0/L0_001.sst");

        // Assert
        assert_eq!(
            url,
            "https://test-bucket.s3.us-west-2.amazonaws.com/tenant-123/sst/cf_0/L0_001.sst"
        );
    }
}
