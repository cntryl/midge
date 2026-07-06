//! AWS S3 provider with `SigV4` authentication
//!
//! Specialized implementation of `S3Provider` for AWS with:
//! - `SigV4` signature-based authentication
//! - AWS credential handling (access key, secret key, optional session token)
//! - Region-specific endpoint routing

use super::s3::{AwsCredentials, S3Provider};
use crate::common::MidgeResult;

/// AWS S3 provider with `SigV4` authentication
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
    pub fn new(
        bucket: String,
        region: String,
        access_key: String,
        secret_key: String,
    ) -> MidgeResult<Self> {
        let creds = AwsCredentials {
            access_key,
            secret_key,
            region: region.clone(),
            session_token: None,
        };
        let inner = S3Provider::aws(bucket, region, creds)?;
        Ok(Self { inner })
    }

    /// Create AWS S3 provider using the default AWS credential chain.
    ///
    /// Works across AWS compute runtimes without embedding static keys:
    /// - EC2/VMs via Instance Metadata Service (IMDS)
    /// - EKS via IRSA (`AWS_ROLE_ARN` + `AWS_WEB_IDENTITY_TOKEN_FILE`)
    /// - ECS/Fargate via task role endpoint
    /// - Environment credentials (for local/dev and CI)
    pub fn from_default_chain(bucket: String, region: String) -> MidgeResult<Self> {
        let inner = S3Provider::aws_default(bucket, region)?;
        Ok(Self { inner })
    }

    /// Create AWS S3 provider with temporary credentials (includes session token)
    ///
    /// Used for federated credentials from `AssumeRole` or similar
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
    ) -> MidgeResult<Self> {
        let creds = AwsCredentials {
            access_key,
            secret_key,
            region: region.clone(),
            session_token: Some(session_token),
        };
        let inner = S3Provider::aws(bucket, region, creds)?;
        Ok(Self { inner })
    }

    /// Create AWS S3 provider from `AwsCredentials`
    pub fn from_credentials(bucket: String, creds: AwsCredentials) -> MidgeResult<Self> {
        let region = creds.region.clone();
        let inner = S3Provider::aws(bucket, region, creds)?;
        Ok(Self { inner })
    }

    /// Access the underlying `S3Provider` for lower-level operations
    pub fn inner(&self) -> &S3Provider {
        &self.inner
    }

    /// Convert into the underlying `S3Provider`
    pub fn into_inner(self) -> S3Provider {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn should_create_aws_provider_with_keys() {
        // Arrange

        // Act
        let provider = AwsS3Provider::new(
            "my-bucket".into(),
            "us-east-1".into(),
            "AKIAIOSFODNN7EXAMPLE".into(),
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
        )
        .expect("should create aws provider");

        let backend = provider.inner().backend();

        // Assert
        assert!(Arc::strong_count(&backend) >= 1);
    }

    #[test]
    fn should_create_aws_provider_with_session_token() {
        // Arrange

        // Act
        let provider = AwsS3Provider::with_session_token(
            "my-bucket".into(),
            "us-west-2".into(),
            "ASIATEMP0000000000".into(),
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYTEMPORARY".into(),
            "AQoDYXdzEJr...<token_omitted>".into(),
        )
        .expect("should create aws provider with session token");

        let backend = provider.inner().backend();

        // Assert
        assert!(Arc::strong_count(&backend) >= 1);
    }

    #[test]
    fn should_create_aws_provider_from_credentials() {
        // Arrange
        let creds = AwsCredentials {
            access_key: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            region: "eu-west-1".into(),
            session_token: None,
        };

        // Act
        let provider = AwsS3Provider::from_credentials("my-bucket".into(), creds)
            .expect("should create aws provider from credentials");

        let backend = provider.inner().backend();

        // Assert
        assert!(Arc::strong_count(&backend) >= 1);
    }

    #[test]
    fn should_convert_default_chain_provider_into_inner_backend() {
        // Arrange

        // Act
        let provider = AwsS3Provider::from_default_chain("my-bucket".into(), "us-east-1".into())
            .expect("should create aws provider from default chain");
        let backend = provider.into_inner().backend();

        // Assert
        assert!(Arc::strong_count(&backend) >= 1);
    }
}
