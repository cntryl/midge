//! AWS S3 provider with SigV4 authentication
//!
//! Specialized implementation of S3Provider for AWS with:
//! - SigV4 signature-based authentication
//! - AWS credential handling (access key, secret key, optional session token)
//! - Region-specific endpoint routing

use crate::storage::cloud::executor::AwsCredentials;
use super::s3::S3Provider;

/// AWS S3 provider with SigV4 authentication
///
/// Provides convenient constructors for AWS-specific credential types.
pub struct AwsS3Provider {
    inner: S3Provider,
}

impl AwsS3Provider {
    /// Create AWS S3 provider with access key and secret key
    ///
    /// # Arguments
    /// * `bucket` - S3 bucket name
    /// * `region` - AWS region (e.g., "us-east-1")
    /// * `access_key` - AWS access key ID
    /// * `secret_key` - AWS secret access key
    pub fn new(bucket: String, region: String, access_key: String, secret_key: String) -> Self {
        let creds = AwsCredentials {
            access_key,
            secret_key,
            region: region.clone(),
            session_token: None,
        };
        Self {
            inner: S3Provider::aws(bucket, region, creds),
        }
    }

    /// Create AWS S3 provider with temporary credentials (includes session token)
    ///
    /// Used for federated credentials from AssumeRole or similar
    ///
    /// # Arguments
    /// * `bucket` - S3 bucket name
    /// * `region` - AWS region
    /// * `access_key` - AWS access key ID
    /// * `secret_key` - AWS secret access key
    /// * `session_token` - Temporary session token
    pub fn with_session_token(
        bucket: String,
        region: String,
        access_key: String,
        secret_key: String,
        session_token: String,
    ) -> Self {
        let creds = AwsCredentials {
            access_key,
            secret_key,
            region: region.clone(),
            session_token: Some(session_token),
        };
        Self {
            inner: S3Provider::aws(bucket, region, creds),
        }
    }

    /// Create AWS S3 provider from AwsCredentials
    pub fn from_credentials(bucket: String, creds: AwsCredentials) -> Self {
        let region = creds.region.clone();
        Self {
            inner: S3Provider::aws(bucket, region, creds),
        }
    }

    /// Access the underlying S3Provider for lower-level operations
    pub fn inner(&self) -> &S3Provider {
        &self.inner
    }

    /// Convert into the underlying S3Provider
    pub fn into_inner(self) -> S3Provider {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_aws_provider_with_keys() {
        let provider = AwsS3Provider::new(
            "my-bucket".into(),
            "us-east-1".into(),
            "AKIAIOSFODNN7EXAMPLE".into(),
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
        );
        let _ = provider.inner();
    }

    #[test]
    fn should_create_aws_provider_with_session_token() {
        let provider = AwsS3Provider::with_session_token(
            "my-bucket".into(),
            "us-west-2".into(),
            "ASIATEMP0000000000".into(),
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYTEMPORARY".into(),
            "AQoDYXdzEJr...<token_omitted>".into(),
        );
        let _ = provider.inner();
    }

    #[test]
    fn should_create_aws_provider_from_credentials() {
        let creds = AwsCredentials {
            access_key: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            region: "eu-west-1".into(),
            session_token: None,
        };
        let provider = AwsS3Provider::from_credentials("my-bucket".into(), creds);
        let _ = provider.inner();
    }
}
