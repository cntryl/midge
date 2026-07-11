//! Public cloud-provider configuration types.
//!
//! Configuration owns these DTOs; storage providers consume them without
//! becoming part of the public configuration surface.

use std::fmt;
use std::path::PathBuf;

use crate::common::{MidgeError, MidgeResult};

/// Cloud provider credential source for S3-compatible providers.
#[derive(Clone, PartialEq, Eq)]
pub enum S3CredentialSource {
    /// Use an explicit access key / secret key pair.
    Static {
        access_key: String,
        secret_key: String,
        session_token: Option<String>,
    },
    /// Resolve static credentials from AWS-style environment variables.
    Environment,
    /// Resolve static credentials from AWS shared config/credentials files.
    SharedProfile {
        profile: Option<String>,
        credentials_file: Option<PathBuf>,
        config_file: Option<PathBuf>,
    },
    /// Use Midge's lightweight AWS default chain.
    ///
    /// Intended for AWS S3 only. S3-compatible providers should use `Static`,
    /// `Environment`, or `SharedProfile` so they do not accidentally contact AWS
    /// metadata/role endpoints.
    AwsDefaultChain,
}

impl fmt::Debug for S3CredentialSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Static { session_token, .. } => formatter
                .debug_struct("Static")
                .field("access_key", &"[REDACTED]")
                .field("secret_key", &"[REDACTED]")
                .field(
                    "session_token",
                    &if session_token.is_some() {
                        "[REDACTED]"
                    } else {
                        "<none>"
                    },
                )
                .finish(),
            Self::Environment => formatter.write_str("Environment"),
            Self::SharedProfile {
                profile,
                credentials_file,
                config_file,
            } => formatter
                .debug_struct("SharedProfile")
                .field("profile", profile)
                .field("credentials_file", credentials_file)
                .field("config_file", config_file)
                .finish(),
            Self::AwsDefaultChain => formatter.write_str("AwsDefaultChain"),
        }
    }
}

impl S3CredentialSource {
    /// Use an explicit access key / secret key pair.
    pub fn access_key(access_key: impl Into<String>, secret_key: impl Into<String>) -> Self {
        Self::Static {
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            session_token: None,
        }
    }

    /// Use explicit temporary/session credentials.
    pub fn session(
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        session_token: impl Into<String>,
    ) -> Self {
        Self::Static {
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            session_token: Some(session_token.into()),
        }
    }

    /// Resolve static credentials from AWS-style environment variables.
    #[must_use]
    pub fn environment() -> Self {
        Self::Environment
    }

    /// Resolve static credentials from the named AWS shared profile.
    pub fn shared_profile(profile: impl Into<String>) -> Self {
        Self::SharedProfile {
            profile: Some(profile.into()),
            credentials_file: None,
            config_file: None,
        }
    }

    /// Use Midge's lean AWS default credential chain.
    #[must_use]
    pub fn aws_default_chain() -> Self {
        Self::AwsDefaultChain
    }
}

/// Azure Blob credential source.
#[derive(Clone, PartialEq, Eq)]
pub enum AzureCredentialSource {
    SharedKey {
        account_key: String,
    },
    SasToken {
        token: String,
    },
    ConnectionString {
        connection_string: String,
    },
    StorageEnvironment,
    EnvironmentClientSecret,
    WorkloadIdentity {
        tenant_id: Option<String>,
        client_id: Option<String>,
        token_file: Option<PathBuf>,
    },
    ManagedIdentity {
        client_id: Option<String>,
    },
    LightweightDefaultChain,
}

impl fmt::Debug for AzureCredentialSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SharedKey { .. } => formatter
                .debug_struct("SharedKey")
                .field("account_key", &"[REDACTED]")
                .finish(),
            Self::SasToken { .. } => formatter
                .debug_struct("SasToken")
                .field("token", &"[REDACTED]")
                .finish(),
            Self::ConnectionString { .. } => formatter
                .debug_struct("ConnectionString")
                .field("connection_string", &"[REDACTED]")
                .finish(),
            Self::StorageEnvironment => formatter.write_str("StorageEnvironment"),
            Self::EnvironmentClientSecret => formatter.write_str("EnvironmentClientSecret"),
            Self::WorkloadIdentity {
                tenant_id,
                client_id,
                token_file,
            } => formatter
                .debug_struct("WorkloadIdentity")
                .field("tenant_id", tenant_id)
                .field("client_id", client_id)
                .field("token_file", token_file)
                .finish(),
            Self::ManagedIdentity { client_id } => formatter
                .debug_struct("ManagedIdentity")
                .field("client_id", client_id)
                .finish(),
            Self::LightweightDefaultChain => formatter.write_str("LightweightDefaultChain"),
        }
    }
}

impl AzureCredentialSource {
    /// Use Azure Storage shared-key authentication.
    pub fn shared_key(account_key: impl Into<String>) -> Self {
        Self::SharedKey {
            account_key: account_key.into(),
        }
    }

    /// Use an Azure Storage SAS token.
    pub fn sas_token(token: impl Into<String>) -> Self {
        Self::SasToken {
            token: token.into(),
        }
    }

    /// Use an Azure Storage connection string.
    pub fn connection_string(connection_string: impl Into<String>) -> Self {
        Self::ConnectionString {
            connection_string: connection_string.into(),
        }
    }

    /// Resolve Azure Storage key/SAS/connection-string environment credentials.
    #[must_use]
    pub fn storage_environment() -> Self {
        Self::StorageEnvironment
    }

    /// Resolve Microsoft Entra client-secret credentials from environment variables.
    #[must_use]
    pub fn environment_client_secret() -> Self {
        Self::EnvironmentClientSecret
    }

    /// Resolve Azure workload identity credentials.
    #[must_use]
    pub fn workload_identity() -> Self {
        Self::WorkloadIdentity {
            tenant_id: None,
            client_id: None,
            token_file: None,
        }
    }

    /// Resolve Azure workload identity credentials with explicit fields.
    #[must_use]
    pub fn workload_identity_with(
        tenant_id: Option<String>,
        client_id: Option<String>,
        token_file: Option<PathBuf>,
    ) -> Self {
        Self::WorkloadIdentity {
            tenant_id,
            client_id,
            token_file,
        }
    }

    /// Resolve Azure workload identity credentials for an explicit client ID.
    pub fn workload_identity_for_client(client_id: impl Into<String>) -> Self {
        Self::WorkloadIdentity {
            tenant_id: None,
            client_id: Some(client_id.into()),
            token_file: None,
        }
    }

    /// Resolve Azure workload identity credentials from an explicit token file.
    pub fn workload_identity_from_file(token_file: impl Into<PathBuf>) -> Self {
        Self::WorkloadIdentity {
            tenant_id: None,
            client_id: None,
            token_file: Some(token_file.into()),
        }
    }

    /// Resolve an Azure managed identity token.
    #[must_use]
    pub fn managed_identity() -> Self {
        Self::ManagedIdentity { client_id: None }
    }

    /// Resolve a user-assigned Azure managed identity token.
    pub fn user_assigned_managed_identity(client_id: impl Into<String>) -> Self {
        Self::ManagedIdentity {
            client_id: Some(client_id.into()),
        }
    }

    /// Use Midge's lean Azure identity chain.
    #[must_use]
    pub fn default_chain() -> Self {
        Self::LightweightDefaultChain
    }
}

/// Google Cloud Storage credential source.
#[derive(Clone, PartialEq, Eq)]
pub enum GcsCredentialSource {
    BearerToken { token: String },
    HmacKey { access_id: String, secret: String },
    ApplicationDefault,
    ServiceAccountJsonFile { path: PathBuf },
    AuthorizedUserJsonFile { path: PathBuf },
    MetadataServer,
}

impl fmt::Debug for GcsCredentialSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BearerToken { .. } => formatter
                .debug_struct("BearerToken")
                .field("token", &"[REDACTED]")
                .finish(),
            Self::HmacKey { .. } => formatter
                .debug_struct("HmacKey")
                .field("access_id", &"[REDACTED]")
                .field("secret", &"[REDACTED]")
                .finish(),
            Self::ApplicationDefault => formatter.write_str("ApplicationDefault"),
            Self::ServiceAccountJsonFile { path } => formatter
                .debug_struct("ServiceAccountJsonFile")
                .field("path", path)
                .finish(),
            Self::AuthorizedUserJsonFile { path } => formatter
                .debug_struct("AuthorizedUserJsonFile")
                .field("path", path)
                .finish(),
            Self::MetadataServer => formatter.write_str("MetadataServer"),
        }
    }
}

impl GcsCredentialSource {
    /// Use a static `OAuth2` bearer token.
    pub fn bearer_token(token: impl Into<String>) -> Self {
        Self::BearerToken {
            token: token.into(),
        }
    }

    /// Use a GCS HMAC access ID / secret pair through the XML API.
    pub fn hmac_key(access_id: impl Into<String>, secret: impl Into<String>) -> Self {
        Self::HmacKey {
            access_id: access_id.into(),
            secret: secret.into(),
        }
    }

    /// Use Google Application Default Credentials.
    #[must_use]
    pub fn application_default() -> Self {
        Self::ApplicationDefault
    }

    /// Use a service-account JSON key file.
    pub fn service_account_json_file(path: impl Into<PathBuf>) -> Self {
        Self::ServiceAccountJsonFile { path: path.into() }
    }

    /// Use an authorized-user ADC JSON file.
    pub fn authorized_user_json_file(path: impl Into<PathBuf>) -> Self {
        Self::AuthorizedUserJsonFile { path: path.into() }
    }

    /// Use the Google metadata server.
    #[must_use]
    pub fn metadata_server() -> Self {
        Self::MetadataServer
    }
}

/// GCS HTTP API flavor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcsApiStyle {
    /// Native JSON API (`/storage/v1`, `/upload/storage/v1`).
    Json,
    /// XML API with GOOG1 HMAC signing. This is the preferred Sqrzl path.
    Xml,
}

/// Public cloud provider configuration for real object-store backends.
#[derive(Clone, PartialEq, Eq)]
pub enum CloudProviderConfig {
    AwsS3 {
        bucket: String,
        region: String,
        credentials: S3CredentialSource,
    },
    S3Compatible {
        bucket: String,
        region: String,
        endpoint: String,
        path_style: bool,
        credentials: S3CredentialSource,
    },
    AzureBlob {
        account: String,
        container: String,
        endpoint: Option<String>,
        credential: AzureCredentialSource,
    },
    Gcs {
        bucket: String,
        project_id: String,
        endpoint: Option<String>,
        api: GcsApiStyle,
        credential: GcsCredentialSource,
    },
}

impl fmt::Debug for CloudProviderConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AwsS3 {
                bucket,
                region,
                credentials,
            } => formatter
                .debug_struct("AwsS3")
                .field("bucket", bucket)
                .field("region", region)
                .field("credentials", credentials)
                .finish(),
            Self::S3Compatible {
                bucket,
                region,
                endpoint,
                path_style,
                credentials,
            } => formatter
                .debug_struct("S3Compatible")
                .field("bucket", bucket)
                .field("region", region)
                .field("endpoint", endpoint)
                .field("path_style", path_style)
                .field("credentials", credentials)
                .finish(),
            Self::AzureBlob {
                account,
                container,
                endpoint,
                credential,
            } => formatter
                .debug_struct("AzureBlob")
                .field("account", account)
                .field("container", container)
                .field("endpoint", endpoint)
                .field("credential", credential)
                .finish(),
            Self::Gcs {
                bucket,
                project_id,
                endpoint,
                api,
                credential,
            } => formatter
                .debug_struct("Gcs")
                .field("bucket", bucket)
                .field("project_id", project_id)
                .field("endpoint", endpoint)
                .field("api", api)
                .field("credential", credential)
                .finish(),
        }
    }
}

/// Provider-neutral credential wrapper for fluent cloud configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloudCredentialSource {
    S3(S3CredentialSource),
    Azure(AzureCredentialSource),
    Gcs(GcsCredentialSource),
}

impl From<S3CredentialSource> for CloudCredentialSource {
    fn from(source: S3CredentialSource) -> Self {
        Self::S3(source)
    }
}

impl From<AzureCredentialSource> for CloudCredentialSource {
    fn from(source: AzureCredentialSource) -> Self {
        Self::Azure(source)
    }
}

impl From<GcsCredentialSource> for CloudCredentialSource {
    fn from(source: GcsCredentialSource) -> Self {
        Self::Gcs(source)
    }
}

impl CloudProviderConfig {
    /// Create AWS S3 config using Midge's lean AWS default credential chain.
    pub fn aws_s3(bucket: impl Into<String>, region: impl Into<String>) -> Self {
        Self::AwsS3 {
            bucket: bucket.into(),
            region: region.into(),
            credentials: S3CredentialSource::aws_default_chain(),
        }
    }

    /// Create AWS S3 config with explicit access keys.
    pub fn aws_s3_static(
        bucket: impl Into<String>,
        region: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        Self::AwsS3 {
            bucket: bucket.into(),
            region: region.into(),
            credentials: S3CredentialSource::access_key(access_key, secret_key),
        }
    }

    /// Create S3-compatible config using AWS-style environment credentials.
    pub fn s3_compatible_env(bucket: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self::S3Compatible {
            bucket: bucket.into(),
            region: "us-east-1".to_string(),
            endpoint: endpoint.into(),
            path_style: true,
            credentials: S3CredentialSource::environment(),
        }
    }

    /// Create S3-compatible config with explicit access keys.
    pub fn s3_compatible_static(
        bucket: impl Into<String>,
        endpoint: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        Self::S3Compatible {
            bucket: bucket.into(),
            region: "us-east-1".to_string(),
            endpoint: endpoint.into(),
            path_style: true,
            credentials: S3CredentialSource::access_key(access_key, secret_key),
        }
    }

    /// Create S3-compatible config with explicit region and static credentials.
    pub fn s3_compatible(
        bucket: impl Into<String>,
        region: impl Into<String>,
        endpoint: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        Self::S3Compatible {
            bucket: bucket.into(),
            region: region.into(),
            endpoint: endpoint.into(),
            path_style: true,
            credentials: S3CredentialSource::access_key(access_key, secret_key),
        }
    }

    /// Create Azure Blob config using Midge's lean Azure identity chain.
    pub fn azure_blob(account: impl Into<String>, container: impl Into<String>) -> Self {
        Self::AzureBlob {
            account: account.into(),
            container: container.into(),
            endpoint: None,
            credential: AzureCredentialSource::default_chain(),
        }
    }

    /// Create Azure Blob config with shared-key authentication.
    pub fn azure_blob_shared_key(
        account: impl Into<String>,
        container: impl Into<String>,
        account_key: impl Into<String>,
    ) -> Self {
        Self::AzureBlob {
            account: account.into(),
            container: container.into(),
            endpoint: None,
            credential: AzureCredentialSource::shared_key(account_key),
        }
    }

    /// Create Azure Blob config with SAS-token authentication.
    pub fn azure_blob_sas(
        account: impl Into<String>,
        container: impl Into<String>,
        token: impl Into<String>,
    ) -> Self {
        Self::AzureBlob {
            account: account.into(),
            container: container.into(),
            endpoint: None,
            credential: AzureCredentialSource::sas_token(token),
        }
    }

    /// Create Azure Blob config from a connection string.
    pub fn azure_blob_connection_string(
        container: impl Into<String>,
        connection_string: impl Into<String>,
    ) -> Self {
        let connection_string = connection_string.into();
        Self::AzureBlob {
            account: azure_connection_string_account(&connection_string).unwrap_or_default(),
            container: container.into(),
            endpoint: None,
            credential: AzureCredentialSource::connection_string(connection_string),
        }
    }

    /// Create GCS config using Application Default Credentials and the JSON API.
    pub fn gcs(bucket: impl Into<String>) -> Self {
        Self::Gcs {
            bucket: bucket.into(),
            project_id: String::new(),
            endpoint: None,
            api: GcsApiStyle::Json,
            credential: GcsCredentialSource::application_default(),
        }
    }

    /// Create GCS config using XML API HMAC credentials.
    pub fn gcs_hmac(
        bucket: impl Into<String>,
        access_id: impl Into<String>,
        secret: impl Into<String>,
    ) -> Self {
        Self::Gcs {
            bucket: bucket.into(),
            project_id: String::new(),
            endpoint: None,
            api: GcsApiStyle::Xml,
            credential: GcsCredentialSource::hmac_key(access_id, secret),
        }
    }

    /// Create GCS config using a service-account JSON file.
    pub fn gcs_service_account_file(bucket: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        Self::Gcs {
            bucket: bucket.into(),
            project_id: String::new(),
            endpoint: None,
            api: GcsApiStyle::Json,
            credential: GcsCredentialSource::service_account_json_file(path),
        }
    }

    /// Create GCS config using an authorized-user ADC JSON file.
    pub fn gcs_authorized_user_file(bucket: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        Self::Gcs {
            bucket: bucket.into(),
            project_id: String::new(),
            endpoint: None,
            api: GcsApiStyle::Json,
            credential: GcsCredentialSource::authorized_user_json_file(path),
        }
    }

    /// Create GCS config using a static `OAuth2` bearer token.
    pub fn gcs_bearer_token(bucket: impl Into<String>, token: impl Into<String>) -> Self {
        Self::Gcs {
            bucket: bucket.into(),
            project_id: String::new(),
            endpoint: None,
            api: GcsApiStyle::Json,
            credential: GcsCredentialSource::bearer_token(token),
        }
    }

    /// Create an S3-compatible configuration for the local Sqrzl emulator.
    pub fn sqrzl_s3(bucket: impl Into<String>) -> Self {
        Self::s3_compatible_static(bucket, "http://127.0.0.1:9000", "admin", "easy-peasy")
    }

    /// Create an Azure Blob configuration for the local Sqrzl emulator.
    pub fn sqrzl_azure(container: impl Into<String>) -> Self {
        Self::AzureBlob {
            account: "admin".to_string(),
            container: container.into(),
            endpoint: Some("http://127.0.0.1:9000".to_string()),
            credential: AzureCredentialSource::shared_key("easy-peasy"),
        }
    }

    /// Create a GCS XML configuration for the local Sqrzl emulator.
    pub fn sqrzl_gcs(bucket: impl Into<String>) -> Self {
        Self::Gcs {
            bucket: bucket.into(),
            project_id: "sqrzl".to_string(),
            endpoint: Some("http://127.0.0.1:9000".to_string()),
            api: GcsApiStyle::Xml,
            credential: GcsCredentialSource::hmac_key("admin", "easy-peasy"),
        }
    }

    /// Override a provider endpoint when that provider supports endpoint overrides.
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::InvalidArgument` when the selected provider does not
    /// support endpoint overrides.
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> MidgeResult<Self> {
        let endpoint = endpoint.into();
        match &mut self {
            Self::S3Compatible {
                endpoint: target, ..
            } => *target = endpoint,
            Self::AzureBlob {
                endpoint: target, ..
            }
            | Self::Gcs {
                endpoint: target, ..
            } => *target = Some(endpoint),
            Self::AwsS3 { .. } => {
                return Err(MidgeError::InvalidArgument(
                    "AWS S3 does not support endpoint overrides; use s3_compatible_* for custom endpoints"
                        .to_string(),
                ));
            }
        }
        Ok(self)
    }

    /// Override path-style addressing for S3-compatible providers that expose it.
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::InvalidArgument` when the selected provider does not
    /// support path-style overrides.
    pub fn with_path_style(mut self, path_style: bool) -> MidgeResult<Self> {
        match &mut self {
            Self::S3Compatible {
                path_style: target, ..
            } => *target = path_style,
            Self::AwsS3 { .. } | Self::AzureBlob { .. } | Self::Gcs { .. } => {
                return Err(MidgeError::InvalidArgument(format!(
                    "{} provider does not support path-style overrides",
                    self.kind()
                )));
            }
        }
        Ok(self)
    }

    /// Override the signing region for S3-family providers that carry a region.
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::InvalidArgument` when the selected provider does not
    /// support S3 region overrides.
    pub fn with_s3_region(mut self, region: impl Into<String>) -> MidgeResult<Self> {
        let region = region.into();
        match &mut self {
            Self::AwsS3 { region: target, .. } | Self::S3Compatible { region: target, .. } => {
                *target = region;
            }
            Self::AzureBlob { .. } | Self::Gcs { .. } => {
                return Err(MidgeError::InvalidArgument(format!(
                    "{} provider does not support S3 region overrides",
                    self.kind()
                )));
            }
        }
        Ok(self)
    }

    /// Override GCS project ID metadata for callers that need to preserve it.
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::InvalidArgument` when called for a non-GCS provider.
    pub fn with_gcs_project_id(mut self, project_id: impl Into<String>) -> MidgeResult<Self> {
        if let Self::Gcs {
            project_id: target, ..
        } = &mut self
        {
            *target = project_id.into();
            Ok(self)
        } else {
            Err(MidgeError::InvalidArgument(format!(
                "{} provider does not support GCS project IDs",
                self.kind()
            )))
        }
    }

    /// Override credentials when the credential kind matches the provider.
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::InvalidArgument` when the credential family does not
    /// match the provider family.
    pub fn with_credentials<C: Into<CloudCredentialSource>>(
        self,
        credentials: C,
    ) -> MidgeResult<Self> {
        self.try_with_credentials(credentials)
    }

    /// Override credentials for an S3-family provider.
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::InvalidArgument` when called for a non-S3-family provider.
    pub fn with_s3_credentials(self, credentials: S3CredentialSource) -> MidgeResult<Self> {
        self.try_with_credentials(credentials)
    }

    /// Override credentials for an Azure provider.
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::InvalidArgument` when called for a non-Azure provider.
    pub fn with_azure_credentials(self, credentials: AzureCredentialSource) -> MidgeResult<Self> {
        self.try_with_credentials(credentials)
    }

    /// Override credentials for a GCS provider, updating the API style as needed.
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::InvalidArgument` when called for a non-GCS provider.
    pub fn with_gcs_credentials(self, credentials: GcsCredentialSource) -> MidgeResult<Self> {
        self.try_with_credentials(credentials)
    }

    /// Fallible credential override for dynamic provider/credential configuration.
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::InvalidArgument` when the credential family does not
    /// match the provider family or when the Azure account requirements are violated.
    pub fn try_with_credentials<C: Into<CloudCredentialSource>>(
        mut self,
        credentials: C,
    ) -> MidgeResult<Self> {
        match (&mut self, credentials.into()) {
            (
                Self::AwsS3 {
                    credentials: target,
                    ..
                }
                | Self::S3Compatible {
                    credentials: target,
                    ..
                },
                CloudCredentialSource::S3(credentials),
            ) => *target = credentials,
            (
                Self::AzureBlob {
                    account,
                    credential,
                    ..
                },
                CloudCredentialSource::Azure(credentials),
            ) => {
                if account.is_empty()
                    && !matches!(&credentials, AzureCredentialSource::ConnectionString { .. })
                {
                    return Err(MidgeError::InvalidArgument(
                        "cannot replace Azure connection-string credentials without an account name; use azure_blob_* with an explicit account"
                            .to_string(),
                    ));
                }
                *credential = credentials;
            }
            (
                Self::Gcs {
                    api, credential, ..
                },
                CloudCredentialSource::Gcs(credentials),
            ) => {
                *api = match &credentials {
                    GcsCredentialSource::HmacKey { .. } => GcsApiStyle::Xml,
                    GcsCredentialSource::BearerToken { .. }
                    | GcsCredentialSource::ApplicationDefault
                    | GcsCredentialSource::ServiceAccountJsonFile { .. }
                    | GcsCredentialSource::AuthorizedUserJsonFile { .. }
                    | GcsCredentialSource::MetadataServer => GcsApiStyle::Json,
                };
                *credential = credentials;
            }
            (provider, credentials) => {
                return Err(MidgeError::InvalidArgument(format!(
                    "cannot apply {} credentials to {} provider",
                    credentials.kind(),
                    provider.kind()
                )));
            }
        }
        Ok(self)
    }

    #[must_use]
    pub fn bucket_or_container(&self) -> &str {
        match self {
            Self::AwsS3 { bucket, .. }
            | Self::S3Compatible { bucket, .. }
            | Self::Gcs { bucket, .. } => bucket,
            Self::AzureBlob { container, .. } => container,
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::AwsS3 { .. } | Self::S3Compatible { .. } => "S3-family",
            Self::AzureBlob { .. } => "Azure",
            Self::Gcs { .. } => "GCS",
        }
    }
}

impl CloudCredentialSource {
    fn kind(&self) -> &'static str {
        match self {
            Self::S3(_) => "S3-family",
            Self::Azure(_) => "Azure",
            Self::Gcs(_) => "GCS",
        }
    }
}

fn azure_connection_string_account(connection_string: &str) -> Option<String> {
    let mut use_development_storage = false;
    let mut account = None;

    for part in connection_string.split(';') {
        let mut kv = part.splitn(2, '=');
        let key = kv.next()?.trim().to_ascii_lowercase();
        let value = kv.next().unwrap_or_default().trim();
        match key.as_str() {
            "accountname" if !value.is_empty() => account = Some(value.to_string()),
            "usedevelopmentstorage" if value.eq_ignore_ascii_case("true") => {
                use_development_storage = true;
            }
            _ => {}
        }
    }

    account.or_else(|| use_development_storage.then(|| "devstoreaccount1".to_string()))
}

#[cfg(test)]
mod tests {
    use super::{CloudProviderConfig, GcsCredentialSource, S3CredentialSource};

    #[test]
    fn should_redact_all_static_provider_secrets_from_debug_output() {
        // Arrange
        let s3_secret = "s3-secret-do-not-log";
        let azure_secret = "azure-secret-do-not-log";
        let gcs_secret = "gcs-secret-do-not-log";
        let bearer_token = "bearer-token-do-not-log";
        let session_token = "session-token-do-not-log";
        let providers = [
            CloudProviderConfig::s3_compatible(
                "bucket",
                "region",
                "https://endpoint.example",
                "s3-access-do-not-log",
                s3_secret,
            )
            .with_s3_credentials(S3CredentialSource::session(
                "s3-access-do-not-log",
                s3_secret,
                session_token,
            ))
            .expect("S3 credential override should match provider"),
            CloudProviderConfig::azure_blob_connection_string(
                "container",
                "DefaultEndpointsProtocol=https;AccountName=account;AccountKey=azure-secret-do-not-log",
            ),
            CloudProviderConfig::gcs_hmac("bucket", "gcs-access-do-not-log", gcs_secret),
            CloudProviderConfig::gcs_bearer_token("bucket", bearer_token),
        ];

        // Act
        let output = format!("{providers:#?}");

        // Assert
        for secret in [
            s3_secret,
            azure_secret,
            gcs_secret,
            bearer_token,
            session_token,
            "s3-access-do-not-log",
            "gcs-access-do-not-log",
        ] {
            assert!(
                !output.contains(secret),
                "provider Debug output leaked configured credential {secret:?}: {output}"
            );
        }
        assert!(output.contains("https://endpoint.example"));
        assert!(output.contains("[REDACTED]"));
    }

    #[test]
    fn should_redact_direct_gcs_credential_debug_output() {
        // Arrange
        let credential = GcsCredentialSource::bearer_token("gcs-token-do-not-log");

        // Act
        let output = format!("{credential:?}");

        // Assert
        assert!(!output.contains("gcs-token-do-not-log"));
        assert!(output.contains("[REDACTED]"));
    }
}
