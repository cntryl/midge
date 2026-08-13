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

/// OCI's S3 compatibility API uses the same access-key credential shape.
pub type OciCredentialSource = S3CredentialSource;

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

/// AWS S3 configuration.
#[derive(Clone, PartialEq, Eq)]
pub struct AwsS3Config {
    pub(crate) bucket: String,
    pub(crate) region: String,
    pub(crate) credentials: S3CredentialSource,
}

impl AwsS3Config {
    pub fn new(bucket: impl Into<String>, region: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            region: region.into(),
            credentials: S3CredentialSource::aws_default_chain(),
        }
    }
    #[must_use]
    pub fn with_credentials(mut self, credentials: S3CredentialSource) -> Self {
        self.credentials = credentials;
        self
    }
    #[must_use]
    pub fn with_profile(self, profile: impl Into<String>) -> Self {
        self.with_credentials(S3CredentialSource::shared_profile(profile))
    }
    #[must_use]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }
    #[must_use]
    pub fn region(&self) -> &str {
        &self.region
    }
    #[must_use]
    pub fn credentials(&self) -> &S3CredentialSource {
        &self.credentials
    }
}

/// Generic S3-compatible object-store configuration.
#[derive(Clone, PartialEq, Eq)]
pub struct S3CompatibleConfig {
    pub(crate) bucket: String,
    pub(crate) region: String,
    pub(crate) endpoint: String,
    pub(crate) path_style: bool,
    pub(crate) credentials: S3CredentialSource,
}

impl S3CompatibleConfig {
    pub fn new(
        bucket: impl Into<String>,
        region: impl Into<String>,
        endpoint: impl Into<String>,
        credentials: S3CredentialSource,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            region: region.into(),
            endpoint: endpoint.into(),
            path_style: true,
            credentials,
        }
    }
    #[must_use]
    pub fn with_credentials(mut self, credentials: S3CredentialSource) -> Self {
        self.credentials = credentials;
        self
    }
    #[must_use]
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }
    #[must_use]
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = region.into();
        self
    }
    #[must_use]
    pub fn with_path_style(mut self, path_style: bool) -> Self {
        self.path_style = path_style;
        self
    }
    #[must_use]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }
    #[must_use]
    pub fn region(&self) -> &str {
        &self.region
    }
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
    #[must_use]
    pub fn path_style(&self) -> bool {
        self.path_style
    }
    #[must_use]
    pub fn credentials(&self) -> &S3CredentialSource {
        &self.credentials
    }
}

/// Oracle Cloud Infrastructure Object Storage configuration.
#[derive(Clone, PartialEq, Eq)]
pub struct OciObjectStorageConfig {
    pub(crate) namespace: String,
    pub(crate) bucket: String,
    pub(crate) region: String,
    pub(crate) endpoint: Option<String>,
    pub(crate) credentials: S3CredentialSource,
}

impl OciObjectStorageConfig {
    pub fn new(
        namespace: impl Into<String>,
        bucket: impl Into<String>,
        region: impl Into<String>,
        credentials: S3CredentialSource,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            bucket: bucket.into(),
            region: region.into(),
            endpoint: None,
            credentials,
        }
    }
    #[must_use]
    pub fn with_credentials(mut self, credentials: S3CredentialSource) -> Self {
        self.credentials = credentials;
        self
    }
    #[must_use]
    pub fn with_profile(self, profile: impl Into<String>) -> Self {
        self.with_credentials(S3CredentialSource::shared_profile(profile))
    }
    /// Override the OCI S3 Compatibility API base URL for another realm.
    #[must_use]
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
    #[must_use]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }
    #[must_use]
    pub fn region(&self) -> &str {
        &self.region
    }
    #[must_use]
    pub fn credentials(&self) -> &S3CredentialSource {
        &self.credentials
    }
    /// Return an explicitly configured OCI S3 Compatibility API base URL.
    #[must_use]
    pub fn endpoint_override(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }
    #[must_use]
    pub fn endpoint(&self) -> String {
        self.endpoint.clone().unwrap_or_else(|| {
            format!(
                "https://{}.compat.objectstorage.{}.oraclecloud.com",
                self.namespace, self.region
            )
        })
    }
}

/// Azure Blob Storage configuration.
#[derive(Clone, PartialEq, Eq)]
pub struct AzureBlobConfig {
    pub(crate) account: String,
    pub(crate) container: String,
    pub(crate) endpoint: Option<String>,
    pub(crate) credential: AzureCredentialSource,
}

impl AzureBlobConfig {
    pub fn new(account: impl Into<String>, container: impl Into<String>) -> Self {
        Self {
            account: account.into(),
            container: container.into(),
            endpoint: None,
            credential: AzureCredentialSource::default_chain(),
        }
    }
    #[must_use]
    pub fn with_credentials(mut self, credential: AzureCredentialSource) -> Self {
        self.credential = credential;
        self
    }
    #[must_use]
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }
    #[must_use]
    pub fn account(&self) -> &str {
        &self.account
    }
    #[must_use]
    pub fn container(&self) -> &str {
        &self.container
    }
    #[must_use]
    pub fn endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }
    #[must_use]
    pub fn credentials(&self) -> &AzureCredentialSource {
        &self.credential
    }
}

/// Google Cloud Storage configuration. The API style is derived from credentials.
#[derive(Clone, PartialEq, Eq)]
pub struct GcsConfig {
    pub(crate) bucket: String,
    pub(crate) project_id: String,
    pub(crate) endpoint: Option<String>,
    pub(crate) credential: GcsCredentialSource,
}

impl GcsConfig {
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            project_id: String::new(),
            endpoint: None,
            credential: GcsCredentialSource::application_default(),
        }
    }
    #[must_use]
    pub fn with_credentials(mut self, credential: GcsCredentialSource) -> Self {
        self.credential = credential;
        self
    }
    #[must_use]
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }
    #[must_use]
    pub fn with_project_id(mut self, project_id: impl Into<String>) -> Self {
        self.project_id = project_id.into();
        self
    }
    #[must_use]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }
    #[must_use]
    pub fn project_id(&self) -> &str {
        &self.project_id
    }
    #[must_use]
    pub fn endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }
    #[must_use]
    pub fn credentials(&self) -> &GcsCredentialSource {
        &self.credential
    }
    #[must_use]
    pub fn api_style(&self) -> GcsApiStyle {
        if matches!(self.credential, GcsCredentialSource::HmacKey { .. }) {
            GcsApiStyle::Xml
        } else {
            GcsApiStyle::Json
        }
    }
}

/// Public cloud provider configuration for real object-store backends.
#[derive(Clone, PartialEq, Eq)]
pub enum CloudProviderConfig {
    AwsS3(AwsS3Config),
    S3Compatible(S3CompatibleConfig),
    AzureBlob(AzureBlobConfig),
    Gcs(GcsConfig),
    OciObjectStorage(OciObjectStorageConfig),
}

impl From<AwsS3Config> for CloudProviderConfig {
    fn from(value: AwsS3Config) -> Self {
        Self::AwsS3(value)
    }
}
impl From<S3CompatibleConfig> for CloudProviderConfig {
    fn from(value: S3CompatibleConfig) -> Self {
        Self::S3Compatible(value)
    }
}
impl From<AzureBlobConfig> for CloudProviderConfig {
    fn from(value: AzureBlobConfig) -> Self {
        Self::AzureBlob(value)
    }
}
impl From<GcsConfig> for CloudProviderConfig {
    fn from(value: GcsConfig) -> Self {
        Self::Gcs(value)
    }
}
impl From<OciObjectStorageConfig> for CloudProviderConfig {
    fn from(value: OciObjectStorageConfig) -> Self {
        Self::OciObjectStorage(value)
    }
}

impl fmt::Debug for AwsS3Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        CloudProviderConfig::AwsS3(self.clone()).fmt(f)
    }
}
impl fmt::Debug for S3CompatibleConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        CloudProviderConfig::S3Compatible(self.clone()).fmt(f)
    }
}
impl fmt::Debug for AzureBlobConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        CloudProviderConfig::AzureBlob(self.clone()).fmt(f)
    }
}
impl fmt::Debug for GcsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        CloudProviderConfig::Gcs(self.clone()).fmt(f)
    }
}
impl fmt::Debug for OciObjectStorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        CloudProviderConfig::OciObjectStorage(self.clone()).fmt(f)
    }
}

impl fmt::Debug for CloudProviderConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AwsS3(config) => formatter
                .debug_struct("AwsS3")
                .field("bucket", &config.bucket)
                .field("region", &config.region)
                .field("credentials", &config.credentials)
                .finish(),
            Self::S3Compatible(config) => formatter
                .debug_struct("S3Compatible")
                .field("bucket", &config.bucket)
                .field("region", &config.region)
                .field("endpoint", &config.endpoint)
                .field("path_style", &config.path_style)
                .field("credentials", &config.credentials)
                .finish(),
            Self::AzureBlob(config) => formatter
                .debug_struct("AzureBlob")
                .field("account", &config.account)
                .field("container", &config.container)
                .field("endpoint", &config.endpoint)
                .field("credential", &config.credential)
                .finish(),
            Self::Gcs(config) => formatter
                .debug_struct("Gcs")
                .field("bucket", &config.bucket)
                .field("project_id", &config.project_id)
                .field("endpoint", &config.endpoint)
                .field("api", &config.api_style())
                .field("credential", &config.credential)
                .finish(),
            Self::OciObjectStorage(config) => formatter
                .debug_struct("OciObjectStorage")
                .field("namespace", &config.namespace)
                .field("bucket", &config.bucket)
                .field("region", &config.region)
                .field("endpoint", &config.endpoint)
                .field("credentials", &config.credentials)
                .finish(),
        }
    }
}

impl CloudProviderConfig {
    /// Create AWS S3 config using Midge's lean AWS default credential chain.
    pub fn aws_s3(bucket: impl Into<String>, region: impl Into<String>) -> Self {
        AwsS3Config::new(bucket, region).into()
    }

    /// Create AWS S3 config with explicit access keys.
    pub fn aws_s3_static(
        bucket: impl Into<String>,
        region: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        AwsS3Config::new(bucket, region)
            .with_credentials(S3CredentialSource::access_key(access_key, secret_key))
            .into()
    }

    /// Create S3-compatible config using AWS-style environment credentials.
    pub fn s3_compatible_env(bucket: impl Into<String>, endpoint: impl Into<String>) -> Self {
        S3CompatibleConfig::new(
            bucket,
            "us-east-1",
            endpoint,
            S3CredentialSource::environment(),
        )
        .into()
    }

    /// Create S3-compatible config with explicit access keys.
    pub fn s3_compatible_static(
        bucket: impl Into<String>,
        endpoint: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        S3CompatibleConfig::new(
            bucket,
            "us-east-1",
            endpoint,
            S3CredentialSource::access_key(access_key, secret_key),
        )
        .into()
    }

    /// Create S3-compatible config with explicit region and static credentials.
    pub fn s3_compatible(
        bucket: impl Into<String>,
        region: impl Into<String>,
        endpoint: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        S3CompatibleConfig::new(
            bucket,
            region,
            endpoint,
            S3CredentialSource::access_key(access_key, secret_key),
        )
        .into()
    }

    /// Create OCI Object Storage config through its S3 Compatibility API.
    pub fn oci_object_storage(
        namespace: impl Into<String>,
        bucket: impl Into<String>,
        region: impl Into<String>,
        credentials: OciCredentialSource,
    ) -> Self {
        OciObjectStorageConfig::new(namespace, bucket, region, credentials).into()
    }

    /// Create Azure Blob config using Midge's lean Azure identity chain.
    pub fn azure_blob(account: impl Into<String>, container: impl Into<String>) -> Self {
        AzureBlobConfig::new(account, container).into()
    }

    /// Create Azure Blob config with shared-key authentication.
    pub fn azure_blob_shared_key(
        account: impl Into<String>,
        container: impl Into<String>,
        account_key: impl Into<String>,
    ) -> Self {
        AzureBlobConfig::new(account, container)
            .with_credentials(AzureCredentialSource::shared_key(account_key))
            .into()
    }

    /// Create Azure Blob config with SAS-token authentication.
    pub fn azure_blob_sas(
        account: impl Into<String>,
        container: impl Into<String>,
        token: impl Into<String>,
    ) -> Self {
        AzureBlobConfig::new(account, container)
            .with_credentials(AzureCredentialSource::sas_token(token))
            .into()
    }

    /// Create Azure Blob config from a connection string.
    pub fn azure_blob_connection_string(
        container: impl Into<String>,
        connection_string: impl Into<String>,
    ) -> Self {
        let connection_string = connection_string.into();
        AzureBlobConfig::new(
            azure_connection_string_account(&connection_string).unwrap_or_default(),
            container,
        )
        .with_credentials(AzureCredentialSource::connection_string(connection_string))
        .into()
    }

    /// Create GCS config using Application Default Credentials and the JSON API.
    pub fn gcs(bucket: impl Into<String>) -> Self {
        GcsConfig::new(bucket).into()
    }

    /// Create GCS config using XML API HMAC credentials.
    pub fn gcs_hmac(
        bucket: impl Into<String>,
        access_id: impl Into<String>,
        secret: impl Into<String>,
    ) -> Self {
        GcsConfig::new(bucket)
            .with_credentials(GcsCredentialSource::hmac_key(access_id, secret))
            .into()
    }

    /// Create GCS config using a service-account JSON file.
    pub fn gcs_service_account_file(bucket: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        GcsConfig::new(bucket)
            .with_credentials(GcsCredentialSource::service_account_json_file(path))
            .into()
    }

    /// Create GCS config using an authorized-user ADC JSON file.
    pub fn gcs_authorized_user_file(bucket: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        GcsConfig::new(bucket)
            .with_credentials(GcsCredentialSource::authorized_user_json_file(path))
            .into()
    }

    /// Create GCS config using a static `OAuth2` bearer token.
    pub fn gcs_bearer_token(bucket: impl Into<String>, token: impl Into<String>) -> Self {
        GcsConfig::new(bucket)
            .with_credentials(GcsCredentialSource::bearer_token(token))
            .into()
    }

    /// Create an S3-compatible configuration for the local Sqrzl emulator.
    pub fn sqrzl_s3(bucket: impl Into<String>) -> Self {
        Self::s3_compatible_static(bucket, "http://127.0.0.1:9000", "admin", "easy-peasy")
    }

    /// Create an Azure Blob configuration for the local Sqrzl emulator.
    pub fn sqrzl_azure(container: impl Into<String>) -> Self {
        AzureBlobConfig::new("admin", container)
            .with_endpoint("http://127.0.0.1:9000")
            .with_credentials(AzureCredentialSource::shared_key("easy-peasy"))
            .into()
    }

    /// Create a GCS XML configuration for the local Sqrzl emulator.
    pub fn sqrzl_gcs(bucket: impl Into<String>) -> Self {
        GcsConfig::new(bucket)
            .with_project_id("sqrzl")
            .with_endpoint("http://127.0.0.1:9000")
            .with_credentials(GcsCredentialSource::hmac_key("admin", "easy-peasy"))
            .into()
    }

    /// Create a GCS JSON configuration for the local Sqrzl emulator.
    pub fn sqrzl_gcs_json(bucket: impl Into<String>) -> Self {
        Self::gcs_bearer_token(bucket, "admin")
            .with_endpoint("http://127.0.0.1:9000")
            .expect("GCS supports endpoint overrides")
    }

    /// Override a provider endpoint when that provider supports endpoint overrides.
    ///
    /// Azure Shared Key and SAS credentials retain path-style emulator addressing.
    /// Azure identity credentials treat this as an HTTPS account-scoped Blob
    /// service origin, for example a sovereign-cloud storage endpoint.
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::InvalidArgument` when the selected provider does not
    /// support endpoint overrides.
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> MidgeResult<Self> {
        let endpoint = endpoint.into();
        match &mut self {
            Self::S3Compatible(config) => config.endpoint = endpoint,
            Self::OciObjectStorage(config) => config.endpoint = Some(endpoint),
            Self::AzureBlob(config) => config.endpoint = Some(endpoint),
            Self::Gcs(config) => config.endpoint = Some(endpoint),
            Self::AwsS3(_) => {
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
            Self::S3Compatible(config) => config.path_style = path_style,
            Self::AwsS3(_) | Self::AzureBlob(_) | Self::Gcs(_) | Self::OciObjectStorage(_) => {
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
            Self::AwsS3(config) => config.region = region,
            Self::S3Compatible(config) => config.region = region,
            Self::OciObjectStorage(config) => config.region = region,
            Self::AzureBlob(_) | Self::Gcs(_) => {
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
        if let Self::Gcs(config) = &mut self {
            config.project_id = project_id.into();
            Ok(self)
        } else {
            Err(MidgeError::InvalidArgument(format!(
                "{} provider does not support GCS project IDs",
                self.kind()
            )))
        }
    }

    /// Override credentials for an S3-family provider.
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::InvalidArgument` when called for a non-S3-family provider.
    pub fn with_s3_credentials(mut self, credentials: S3CredentialSource) -> MidgeResult<Self> {
        match &mut self {
            Self::AwsS3(config) => config.credentials = credentials,
            Self::S3Compatible(config) => config.credentials = credentials,
            Self::OciObjectStorage(config) => config.credentials = credentials,
            _ => {
                return Err(MidgeError::InvalidArgument(
                    "provider does not accept S3-family credentials".to_string(),
                ))
            }
        }
        Ok(self)
    }

    /// Override credentials for an Azure provider.
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::InvalidArgument` when called for a non-Azure provider.
    pub fn with_azure_credentials(
        mut self,
        credentials: AzureCredentialSource,
    ) -> MidgeResult<Self> {
        let Self::AzureBlob(config) = &mut self else {
            return Err(MidgeError::InvalidArgument(
                "provider does not accept Azure credentials".to_string(),
            ));
        };
        if config.account.is_empty()
            && !matches!(&credentials, AzureCredentialSource::ConnectionString { .. })
        {
            return Err(MidgeError::InvalidArgument("cannot replace Azure connection-string credentials without an account name; use azure_blob_* with an explicit account".to_string()));
        }
        config.credential = credentials;
        Ok(self)
    }

    /// Override credentials for a GCS provider, updating the API style as needed.
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::InvalidArgument` when called for a non-GCS provider.
    pub fn with_gcs_credentials(mut self, credentials: GcsCredentialSource) -> MidgeResult<Self> {
        let Self::Gcs(config) = &mut self else {
            return Err(MidgeError::InvalidArgument(
                "provider does not accept GCS credentials".to_string(),
            ));
        };
        config.credential = credentials;
        Ok(self)
    }

    #[must_use]
    pub fn bucket_or_container(&self) -> &str {
        match self {
            Self::AwsS3(config) => &config.bucket,
            Self::S3Compatible(config) => &config.bucket,
            Self::Gcs(config) => &config.bucket,
            Self::AzureBlob(config) => &config.container,
            Self::OciObjectStorage(config) => &config.bucket,
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::AwsS3(_) | Self::S3Compatible(_) => "S3-family",
            Self::AzureBlob(_) => "Azure",
            Self::Gcs(_) => "GCS",
            Self::OciObjectStorage(_) => "OCI",
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
    use super::{
        AzureCredentialSource, CloudProviderConfig, GcsCredentialSource, S3CredentialSource,
    };

    #[test]
    fn should_redact_nested_credentials_given_provider_debug_format_when_formatting() {
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

    #[test]
    fn should_preserve_explicit_blob_endpoint_given_azure_identity_credentials() {
        // Arrange
        let endpoint = "https://account.blob.core.usgovcloudapi.net";

        // Act
        let provider = CloudProviderConfig::azure_blob("account", "container")
            .with_azure_credentials(AzureCredentialSource::managed_identity())
            .expect("Azure credential override")
            .with_endpoint(endpoint)
            .expect("Azure endpoint override");

        // Assert
        assert!(matches!(
            provider,
            CloudProviderConfig::AzureBlob(config) if config.endpoint() == Some(endpoint) && matches!(config.credentials(), AzureCredentialSource::ManagedIdentity { .. })
        ));
    }
}
