//! Database Configuration Options
//!
//! Smart configuration system with **automatic parameter derivation**.
//!
//! # Design Philosophy
//!
//! Instead of exposing hundreds of low-level tuning knobs, Midge asks **two core questions**:
//!
//! 1. **What's the performance goal?** (`Goal::Latency` | `Goal::Throughput` | `Goal::Economy`)
//! 2. **How much memory?** (`MemoryBudget::Auto` | `MemoryBudget::Bytes(n)`)
//!
//! All other parameters (block sizes, buffer sizes, compaction triggers, cache allocation, etc.)
//! are **derived automatically** from these three inputs plus optional workload hints.
//! Advanced callers can also override runtime memtable sizing explicitly while
//! leaving `MemoryBudget` semantics unchanged.
//!
//! # Example
//!
//! ```rust,no_run
//! use cntryl_midge::{MidgeEngine, OpenOptions};
//!
//! // Open a database with default options
//! let opts = OpenOptions::local("./my_db").build();
//! let engine = MidgeEngine::open(opts)?;
//! # Ok::<(), cntryl_midge::MidgeError>(())
//! ```

use std::path::PathBuf;

use crate::common::{MidgeError, MidgeResult};
use crate::sst::compression::{CompressionAlgo, CompressionPolicy};

/// Cloud provider credential source for S3-compatible providers.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    pub fn aws_default_chain() -> Self {
        Self::AwsDefaultChain
    }
}

/// Azure Blob credential source.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    pub fn storage_environment() -> Self {
        Self::StorageEnvironment
    }

    /// Resolve Microsoft Entra client-secret credentials from environment variables.
    pub fn environment_client_secret() -> Self {
        Self::EnvironmentClientSecret
    }

    /// Resolve Azure workload identity credentials.
    pub fn workload_identity() -> Self {
        Self::WorkloadIdentity {
            tenant_id: None,
            client_id: None,
            token_file: None,
        }
    }

    /// Resolve Azure workload identity credentials with explicit fields.
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
    pub fn default_chain() -> Self {
        Self::LightweightDefaultChain
    }
}

/// Google Cloud Storage credential source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GcsCredentialSource {
    BearerToken { token: String },
    HmacKey { access_id: String, secret: String },
    ApplicationDefault,
    ServiceAccountJsonFile { path: PathBuf },
    AuthorizedUserJsonFile { path: PathBuf },
    MetadataServer,
}

impl GcsCredentialSource {
    /// Use a static OAuth2 bearer token.
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
    pub fn metadata_server() -> Self {
        Self::MetadataServer
    }
}

/// GCS HTTP API flavor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcsApiStyle {
    /// Native JSON API (`/storage/v1`, `/upload/storage/v1`).
    Json,
    /// XML API with GOOG1 HMAC signing. This is the preferred Peas path.
    Xml,
}

/// Public cloud provider configuration for real object-store backends.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    Minio {
        bucket: String,
        endpoint: String,
        credentials: S3CredentialSource,
    },
    Wasabi {
        bucket: String,
        region: String,
        endpoint: Option<String>,
        credentials: S3CredentialSource,
    },
    OciS3Compatible {
        bucket: String,
        namespace: String,
        region: String,
        endpoint: Option<String>,
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

    /// Backward-compatible S3-compatible constructor with explicit region and keys.
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

    /// Create MinIO config with explicit access keys.
    pub fn minio_static(
        bucket: impl Into<String>,
        endpoint: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        Self::Minio {
            bucket: bucket.into(),
            endpoint: endpoint.into(),
            credentials: S3CredentialSource::access_key(access_key, secret_key),
        }
    }

    /// Backward-compatible MinIO constructor with explicit access keys.
    pub fn minio(
        bucket: impl Into<String>,
        endpoint: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        Self::minio_static(bucket, endpoint, access_key, secret_key)
    }

    /// Create Wasabi config using AWS-style environment credentials.
    pub fn wasabi(bucket: impl Into<String>, region: impl Into<String>) -> Self {
        Self::Wasabi {
            bucket: bucket.into(),
            region: region.into(),
            endpoint: None,
            credentials: S3CredentialSource::environment(),
        }
    }

    /// Create Wasabi config with explicit access keys.
    pub fn wasabi_static(
        bucket: impl Into<String>,
        region: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        Self::Wasabi {
            bucket: bucket.into(),
            region: region.into(),
            endpoint: None,
            credentials: S3CredentialSource::access_key(access_key, secret_key),
        }
    }

    /// Create OCI Object Storage config through OCI's S3-compatible front door.
    pub fn oci_s3_compatible(
        namespace: impl Into<String>,
        bucket: impl Into<String>,
        region: impl Into<String>,
    ) -> Self {
        Self::OciS3Compatible {
            bucket: bucket.into(),
            namespace: namespace.into(),
            region: region.into(),
            endpoint: None,
            path_style: false,
            credentials: S3CredentialSource::environment(),
        }
    }

    /// Create OCI Object Storage S3-compatible config with explicit access keys.
    pub fn oci_s3_compatible_static(
        namespace: impl Into<String>,
        bucket: impl Into<String>,
        region: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        Self::OciS3Compatible {
            bucket: bucket.into(),
            namespace: namespace.into(),
            region: region.into(),
            endpoint: None,
            path_style: false,
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

    /// Create GCS config using a static OAuth2 bearer token.
    pub fn gcs_bearer_token(bucket: impl Into<String>, token: impl Into<String>) -> Self {
        Self::Gcs {
            bucket: bucket.into(),
            project_id: String::new(),
            endpoint: None,
            api: GcsApiStyle::Json,
            credential: GcsCredentialSource::bearer_token(token),
        }
    }

    pub fn peas_s3(bucket: impl Into<String>) -> Self {
        Self::s3_compatible_static(bucket, "http://127.0.0.1:9000", "admin", "easy-peasy")
    }

    pub fn peas_azure(container: impl Into<String>) -> Self {
        Self::AzureBlob {
            account: "admin".to_string(),
            container: container.into(),
            endpoint: Some("http://127.0.0.1:9000".to_string()),
            credential: AzureCredentialSource::shared_key("easy-peasy"),
        }
    }

    pub fn peas_gcs(bucket: impl Into<String>) -> Self {
        Self::Gcs {
            bucket: bucket.into(),
            project_id: "peas".to_string(),
            endpoint: Some("http://127.0.0.1:9000".to_string()),
            api: GcsApiStyle::Xml,
            credential: GcsCredentialSource::hmac_key("admin", "easy-peasy"),
        }
    }

    /// Override a provider endpoint when that provider supports endpoint overrides.
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> MidgeResult<Self> {
        let endpoint = endpoint.into();
        match &mut self {
            Self::S3Compatible {
                endpoint: target, ..
            }
            | Self::Minio {
                endpoint: target, ..
            } => *target = endpoint,
            Self::Wasabi {
                endpoint: target, ..
            }
            | Self::OciS3Compatible {
                endpoint: target, ..
            }
            | Self::AzureBlob {
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
    pub fn with_path_style(mut self, path_style: bool) -> MidgeResult<Self> {
        match &mut self {
            Self::S3Compatible {
                path_style: target, ..
            }
            | Self::OciS3Compatible {
                path_style: target, ..
            } => *target = path_style,
            Self::AwsS3 { .. }
            | Self::Minio { .. }
            | Self::Wasabi { .. }
            | Self::AzureBlob { .. }
            | Self::Gcs { .. } => {
                return Err(MidgeError::InvalidArgument(format!(
                    "{} provider does not support path-style overrides",
                    self.kind()
                )));
            }
        }
        Ok(self)
    }

    /// Override the signing region for S3-family providers that carry a region.
    pub fn with_s3_region(mut self, region: impl Into<String>) -> MidgeResult<Self> {
        let region = region.into();
        match &mut self {
            Self::AwsS3 { region: target, .. }
            | Self::S3Compatible { region: target, .. }
            | Self::Wasabi { region: target, .. }
            | Self::OciS3Compatible { region: target, .. } => *target = region,
            Self::Minio { .. } | Self::AzureBlob { .. } | Self::Gcs { .. } => {
                return Err(MidgeError::InvalidArgument(format!(
                    "{} provider does not support S3 region overrides",
                    self.kind()
                )));
            }
        }
        Ok(self)
    }

    /// Override GCS project ID metadata for callers that need to preserve it.
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
    pub fn with_credentials<C: Into<CloudCredentialSource>>(
        self,
        credentials: C,
    ) -> MidgeResult<Self> {
        self.try_with_credentials(credentials)
    }

    /// Override credentials for an S3-family provider.
    pub fn with_s3_credentials(self, credentials: S3CredentialSource) -> MidgeResult<Self> {
        self.try_with_credentials(credentials)
    }

    /// Override credentials for an Azure provider.
    pub fn with_azure_credentials(self, credentials: AzureCredentialSource) -> MidgeResult<Self> {
        self.try_with_credentials(credentials)
    }

    /// Override credentials for a GCS provider, updating the API style as needed.
    pub fn with_gcs_credentials(self, credentials: GcsCredentialSource) -> MidgeResult<Self> {
        self.try_with_credentials(credentials)
    }

    /// Fallible credential override for dynamic provider/credential configuration.
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
                }
                | Self::Minio {
                    credentials: target,
                    ..
                }
                | Self::Wasabi {
                    credentials: target,
                    ..
                }
                | Self::OciS3Compatible {
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
                *credential = credentials
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

    pub fn bucket_or_container(&self) -> &str {
        match self {
            Self::AwsS3 { bucket, .. }
            | Self::S3Compatible { bucket, .. }
            | Self::Minio { bucket, .. }
            | Self::Wasabi { bucket, .. }
            | Self::OciS3Compatible { bucket, .. }
            | Self::Gcs { bucket, .. } => bucket,
            Self::AzureBlob { container, .. } => container,
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::AwsS3 { .. }
            | Self::S3Compatible { .. }
            | Self::Minio { .. }
            | Self::Wasabi { .. }
            | Self::OciS3Compatible { .. } => "S3-family",
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

/// Storage backend specification - MUST be explicit
///
/// This enum enforces unambiguous storage selection. There are NO defaults,
/// NO inference, and NO magic switching between backends.
///
/// Each variant clearly answers: "Where does this database live?"
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Storage {
    /// In-memory storage - no persistence
    ///
    /// Data is lost when the engine is dropped or process exits.
    /// Use for: testing, caching, ephemeral workloads
    InMemory,

    /// Local filesystem storage
    ///
    /// Data persists to local disk at the specified path.
    /// Use for: traditional deployments, single-node databases
    Local {
        /// Filesystem path to database directory
        path: PathBuf,
    },

    /// Real cloud object storage.
    ///
    /// Uses a hybrid model with a local cache/staging directory plus a
    /// provider-backed object store.
    Cloud {
        /// Local cache/staging path for performance
        local_cache_path: PathBuf,
        /// Provider and credential configuration.
        provider: CloudProviderConfig,
        /// Object key prefix (e.g., "databases/myapp/")
        prefix: String,
    },

    /// Filesystem-backed cloud simulation.
    ///
    /// This is intentionally separate from real cloud mode so tests can keep a
    /// deterministic in-process object-store stand-in without implying that a
    /// provider endpoint is being used.
    CloudSimulated {
        /// Local cache/staging path for performance
        local_cache_path: PathBuf,
        /// Bucket/container name (provider-specific terminology)
        bucket: String,
        /// Object key prefix (e.g., "databases/myapp/")
        prefix: String,
    },
}

/// Performance optimization goal.
///
/// Determines the primary optimization target for derived parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Goal {
    /// Optimize for low latency (p99 < 10ms for point queries).
    ///
    /// - Smaller block sizes (16 KiB)
    /// - More aggressive bloom filters
    /// - Lower compaction trigger thresholds
    /// - Higher cache allocation
    #[default]
    Latency,

    /// Optimize for high throughput (MB/s for bulk operations).
    ///
    /// - Larger block sizes (64 KiB)
    /// - Larger memtables (256 MB)
    /// - Higher compaction concurrency
    /// - Larger SST files
    Throughput,

    /// Optimize for cost (minimize memory/CPU usage).
    ///
    /// - Minimal cache allocation
    /// - Lower compaction concurrency
    /// - Smaller bloom filters
    /// - Higher compression
    Economy,
}

/// Memory budget specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MemoryBudget {
    /// Automatically determine memory budget from available system memory.
    ///
    /// Uses ~50% of the effective memory limit (cgroup-aware when possible).
    #[default]
    Auto,

    /// Explicit memory budget in bytes.
    ///
    /// All allocations (cache + memtables + overhead) must fit within this budget.
    Bytes(usize),
}

/// Workload profile for optimizing derived parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WorkloadProfile {
    /// Balanced read/write workload (default).
    #[default]
    Mixed,

    /// Write-heavy workload (>70% writes).
    ///
    /// - Larger memtables
    /// - More aggressive compaction
    /// - Lower bloom filter priority
    WriteHeavy,

    /// Read-mostly workload (>70% reads).
    ///
    /// - More aggressive bloom filters
    /// - Higher cache allocation
    /// - Lower compaction priority
    ReadMostly,

    /// Range scan workload.
    ///
    /// - Larger block sizes
    /// - Sequential access optimization
    /// - Lower bloom filter priority (not useful for ranges)
    RangeScan,

    /// TTL-heavy workload with frequent expirations.
    ///
    /// - More aggressive compaction
    /// - Higher tombstone cleanup priority
    TtlHeavy,
}

/// Recovery policy for engine open.
///
/// `Strict` fails engine open if manifest, intent-log, or WAL recovery cannot
/// establish a trustworthy state. `Salvage` preserves legacy best-effort
/// behavior and may open in a degraded state after logging warnings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RecoveryPolicy {
    #[default]
    Strict,
    Salvage,
}

/// Database open options with smart defaults.
///
/// Use the builder pattern to configure high-level knobs, and all low-level
/// parameters will be derived automatically.
///
/// Storage backend MUST be explicitly specified via constructors:
/// - OpenOptions::in_memory()
/// - OpenOptions::local(path)
/// - OpenOptions::cloud(cache_path, provider, prefix)
/// - OpenOptions::cloud_simulated(cache_path, bucket, prefix)
#[derive(Debug, Clone)]
pub struct OpenOptions {
    /// Storage backend (REQUIRED - no default)
    pub storage: Storage,

    /// Performance goal
    pub goal: Goal,

    /// Memory budget
    pub memory_budget: MemoryBudget,

    /// Workload profile hint
    pub workload: WorkloadProfile,

    /// Recovery policy used during engine open.
    pub recovery_policy: RecoveryPolicy,

    /// Explicit override for memtable size limit in bytes.
    explicit_memtable_size_limit: Option<usize>,

    /// Explicit override for memtable flush threshold in bytes.
    explicit_memtable_flush_threshold: Option<usize>,

    /// Derived memory budget in bytes (from build())
    pub(crate) derived_memory_budget: usize,

    // Derived parameters (populated by build())
    /// Block size in bytes (derived)
    pub(crate) block_size: usize,

    /// Memtable size limit (derived)
    pub(crate) memtable_size_limit: usize,

    /// Memtable flush threshold (derived)
    pub(crate) memtable_flush_threshold: usize,

    /// Target SST file size (derived)
    pub(crate) target_sst_size: usize,

    /// Block cache size (derived)
    pub(crate) block_cache_size: usize,

    /// WAL buffer size (derived)
    pub(crate) wal_buffer_size: usize,

    /// L0 compaction trigger (derived)
    pub(crate) l0_compaction_trigger: usize,

    /// Compression policy for SST blocks (derived from Goal)
    pub(crate) compression_policy: CompressionPolicy,

    /// Optional WAL batch configuration (from testkit for batched durability mode)
    pub(crate) wal_batch_config: Option<crate::wal::policy::BatchConfig>,
    pub(crate) cloud_runtime_policy: Option<crate::runtime::CloudRuntimePolicy>,
}

impl OpenOptions {
    /// Create in-memory database instance
    ///
    /// Data is NOT persisted and will be lost when engine is dropped.
    /// Ideal for: testing, caching, ephemeral workloads
    ///
    /// Memtable sizing is derived automatically by default. Advanced callers can
    /// override the runtime memtable size limit and flush threshold explicitly.
    ///
    pub fn in_memory() -> Self {
        Self {
            storage: Storage::InMemory,
            goal: Goal::default(),
            memory_budget: MemoryBudget::default(),
            workload: WorkloadProfile::default(),
            recovery_policy: RecoveryPolicy::default(),
            explicit_memtable_size_limit: None,
            explicit_memtable_flush_threshold: None,
            derived_memory_budget: 0,
            // Initial derived values until build() recomputes them
            block_size: 16 * 1024,
            memtable_size_limit: 64 * 1024 * 1024,
            memtable_flush_threshold: 64 * 1024 * 1024,
            target_sst_size: 256 * 1024 * 1024,
            block_cache_size: 128 * 1024 * 1024,
            wal_buffer_size: 256 * 1024,
            l0_compaction_trigger: 4,
            compression_policy: CompressionPolicy::default(),
            wal_batch_config: None,
            cloud_runtime_policy: None,
        }
    }

    /// Create local filesystem database instance
    ///
    /// Data persists to the specified path on local disk.
    /// Ideal for: traditional deployments, single-node databases
    ///
    pub fn local<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            storage: Storage::Local { path: path.into() },
            goal: Goal::default(),
            memory_budget: MemoryBudget::default(),
            workload: WorkloadProfile::default(),
            recovery_policy: RecoveryPolicy::default(),
            explicit_memtable_size_limit: None,
            explicit_memtable_flush_threshold: None,
            derived_memory_budget: 0,
            // Initial derived values until build() recomputes them
            block_size: 16 * 1024,
            memtable_size_limit: 64 * 1024 * 1024,
            memtable_flush_threshold: 64 * 1024 * 1024,
            target_sst_size: 256 * 1024 * 1024,
            block_cache_size: 128 * 1024 * 1024,
            wal_buffer_size: 256 * 1024,
            l0_compaction_trigger: 4,
            compression_policy: CompressionPolicy::default(),
            wal_batch_config: None,
            cloud_runtime_policy: None,
        }
    }

    /// Create cloud-backed database instance using a real object-store provider.
    ///
    /// Data persists to cloud object storage (S3, Azure, GCS, OCI, etc.).
    /// Uses hybrid model with local cache for performance.
    /// Ideal for: cloud-native deployments, serverless, distributed systems
    ///
    /// # Arguments
    /// * `local_cache_path` - Local directory for caching/staging
    /// * `provider` - Cloud provider, bucket/container, credentials, and endpoint
    /// * `prefix` - Object key prefix
    pub fn cloud<P: Into<PathBuf>, S: Into<String>>(
        local_cache_path: P,
        provider: CloudProviderConfig,
        prefix: S,
    ) -> Self {
        Self {
            storage: Storage::Cloud {
                local_cache_path: local_cache_path.into(),
                provider,
                prefix: prefix.into(),
            },
            goal: Goal::default(),
            memory_budget: MemoryBudget::default(),
            workload: WorkloadProfile::default(),
            recovery_policy: RecoveryPolicy::default(),
            explicit_memtable_size_limit: None,
            explicit_memtable_flush_threshold: None,
            derived_memory_budget: 0,
            // Initial derived values until build() recomputes them
            block_size: 16 * 1024,
            memtable_size_limit: 64 * 1024 * 1024,
            memtable_flush_threshold: 64 * 1024 * 1024,
            target_sst_size: 256 * 1024 * 1024,
            block_cache_size: 128 * 1024 * 1024,
            wal_buffer_size: 256 * 1024,
            l0_compaction_trigger: 4,
            compression_policy: CompressionPolicy::default(),
            wal_batch_config: None,
            cloud_runtime_policy: None,
        }
    }

    /// Create a filesystem-backed cloud simulation.
    ///
    /// This keeps deterministic CloudAsync tests available without pretending to
    /// connect to a real provider.
    pub fn cloud_simulated<P: Into<PathBuf>, S: Into<String>>(
        local_cache_path: P,
        bucket: S,
        prefix: S,
    ) -> Self {
        Self {
            storage: Storage::CloudSimulated {
                local_cache_path: local_cache_path.into(),
                bucket: bucket.into(),
                prefix: prefix.into(),
            },
            goal: Goal::default(),
            memory_budget: MemoryBudget::default(),
            workload: WorkloadProfile::default(),
            recovery_policy: RecoveryPolicy::default(),
            explicit_memtable_size_limit: None,
            explicit_memtable_flush_threshold: None,
            derived_memory_budget: 0,
            // Initial derived values until build() recomputes them
            block_size: 16 * 1024,
            memtable_size_limit: 64 * 1024 * 1024,
            memtable_flush_threshold: 64 * 1024 * 1024,
            target_sst_size: 256 * 1024 * 1024,
            block_cache_size: 128 * 1024 * 1024,
            wal_buffer_size: 256 * 1024,
            l0_compaction_trigger: 4,
            compression_policy: CompressionPolicy::default(),
            wal_batch_config: None,
            cloud_runtime_policy: None,
        }
    }

    /// Set performance goal.
    pub fn goal(mut self, goal: Goal) -> Self {
        self.goal = goal;
        self
    }

    /// Set memory budget.
    pub fn memory_budget(mut self, budget: MemoryBudget) -> Self {
        self.memory_budget = budget;
        self
    }

    /// Override the derived memtable size limit in bytes.
    ///
    /// By default, Midge derives memtable sizing from the goal, workload, and
    /// memory budget. This override applies the exact requested runtime size
    /// limit without changing `MemoryBudget`.
    pub fn with_memtable_size_limit(mut self, bytes: usize) -> Self {
        self.explicit_memtable_size_limit = Some(Self::sanitize_memtable_bytes(bytes));
        self
    }

    /// Override the runtime memtable flush threshold in bytes.
    ///
    /// By default, the flush threshold follows the derived memtable sizing. If
    /// only `with_memtable_size_limit` is set, the flush threshold uses that
    /// same value unless this override is also provided.
    pub fn with_memtable_flush_threshold(mut self, bytes: usize) -> Self {
        self.explicit_memtable_flush_threshold = Some(Self::sanitize_memtable_bytes(bytes));
        self
    }

    /// Set workload profile hint.
    pub fn workload(mut self, profile: WorkloadProfile) -> Self {
        self.workload = profile;
        self
    }

    /// Set recovery policy.
    pub fn recovery_policy(mut self, policy: RecoveryPolicy) -> Self {
        self.recovery_policy = policy;
        self
    }

    /// Build options with derived parameters.
    ///
    /// This automatically computes all low-level parameters based on the
    /// high-level knobs (goal, memory, workload) plus optional explicit
    /// memtable overrides for advanced callers.
    pub fn build(mut self) -> Self {
        // Derive memory budget
        let total_memory = match self.memory_budget {
            MemoryBudget::Auto => memory::auto_memory_budget_bytes().unwrap_or(512 * 1024 * 1024),
            MemoryBudget::Bytes(n) => n,
        };
        self.derived_memory_budget = total_memory;

        // Derive block size based on goal and workload
        self.block_size = match (self.goal, self.workload) {
            (Goal::Latency, _) => 16 * 1024, // 16KB for low latency
            (Goal::Economy, _) => 32 * 1024, // 32KB balanced
            (Goal::Throughput, WorkloadProfile::RangeScan) => 128 * 1024, // 128KB for bulk scans
            (Goal::Throughput, _) => 64 * 1024, // 64KB for throughput
        };

        // Derive memtable size based on goal and workload
        let base_memtable = match self.goal {
            Goal::Latency => 64 * 1024 * 1024,     // 64MB for latency
            Goal::Throughput => 256 * 1024 * 1024, // 256MB for throughput
            Goal::Economy => 32 * 1024 * 1024,     // 32MB for cost
        };

        self.memtable_size_limit = match self.workload {
            WorkloadProfile::WriteHeavy => base_memtable * 2, // Double for write-heavy
            WorkloadProfile::ReadMostly => base_memtable / 2, // Half for read-heavy (more cache)
            _ => base_memtable,
        };

        // Clamp memtable size to keep total memory usage within budget.
        let min_memtable = 4 * 1024 * 1024;
        let max_memtable = total_memory / 2;
        let max_allowed = max_memtable.max(min_memtable.min(total_memory));
        self.memtable_size_limit = self.memtable_size_limit.min(max_allowed).max(1);

        if let Some(explicit_memtable_size_limit) = self.explicit_memtable_size_limit {
            self.memtable_size_limit = Self::sanitize_memtable_bytes(explicit_memtable_size_limit);
        }

        self.memtable_flush_threshold = if let Some(explicit_memtable_flush_threshold) =
            self.explicit_memtable_flush_threshold
        {
            Self::sanitize_memtable_bytes(explicit_memtable_flush_threshold)
        } else if self.explicit_memtable_size_limit.is_some() {
            self.memtable_size_limit
        } else if let MemoryBudget::Bytes(n) = self.memory_budget {
            Self::sanitize_memtable_bytes(n / 2)
        } else {
            self.memtable_size_limit
        };

        // Derive target SST size
        self.target_sst_size = match self.goal {
            Goal::Latency => 128 * 1024 * 1024,    // 128MB
            Goal::Throughput => 512 * 1024 * 1024, // 512MB
            Goal::Economy => 256 * 1024 * 1024,    // 256MB
        };

        // Allocate remaining memory to block cache
        let cache_ratio = match self.workload {
            WorkloadProfile::ReadMostly => 0.7, // 70% to cache
            WorkloadProfile::WriteHeavy => 0.2, // 20% to cache
            _ => 0.5,                           // 50% to cache
        };

        let usable_memory = total_memory.saturating_sub(self.memtable_size_limit * 2); // 2 memtables
        self.block_cache_size = ((usable_memory as f64) * cache_ratio) as usize;

        // Cap cache size for Economy goal to minimize resource usage
        if self.goal == Goal::Economy {
            self.block_cache_size = self.block_cache_size.min(256 * 1024 * 1024);
            // 256MB max
        }

        // Derive WAL buffer size
        self.wal_buffer_size = match self.goal {
            Goal::Latency => 128 * 1024,     // 128KB
            Goal::Throughput => 1024 * 1024, // 1MB
            Goal::Economy => 256 * 1024,     // 256KB
        };
        self.wal_buffer_size = self.wal_buffer_size.min(total_memory.max(32 * 1024));

        // Derive compaction trigger
        self.l0_compaction_trigger = match (self.goal, self.workload) {
            (Goal::Latency, _) => 3,               // Aggressive
            (_, WorkloadProfile::WriteHeavy) => 8, // Relaxed for write-heavy
            (Goal::Throughput, _) => 6,            // Moderate
            _ => 4,                                // Default
        };

        // Derive compression policy from goal
        //   Latency  → fast codec, minimal CPU overhead
        //   Throughput → adaptive, try a few codecs per block
        //   Economy  → max compression ratio
        self.compression_policy = match self.goal {
            Goal::Latency => CompressionPolicy::Fixed(CompressionAlgo::Lz4),
            Goal::Throughput => CompressionPolicy::Adaptive {
                min_savings_bytes: 256,
                min_ratio: 1.05,
                check_algorithms: vec![CompressionAlgo::Lz4, CompressionAlgo::Zstd3],
            },
            Goal::Economy => CompressionPolicy::Fixed(CompressionAlgo::Zstd9),
        };

        self
    }

    // Getters for derived parameters

    /// Get derived block size
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Get derived memtable size limit
    pub fn memtable_size_limit(&self) -> usize {
        self.memtable_size_limit
    }

    /// Get derived target SST size
    pub fn target_sst_size(&self) -> usize {
        self.target_sst_size
    }

    /// Get derived block cache size
    pub fn block_cache_size(&self) -> usize {
        self.block_cache_size
    }

    /// Get derived WAL buffer size
    pub fn wal_buffer_size(&self) -> usize {
        self.wal_buffer_size
    }

    /// Get derived L0 compaction trigger
    pub fn l0_compaction_trigger(&self) -> usize {
        self.l0_compaction_trigger
    }

    /// Get derived compression policy
    pub fn compression_policy(&self) -> &CompressionPolicy {
        &self.compression_policy
    }

    pub(crate) fn runtime_memtable_size_limit(&self) -> usize {
        if self.explicit_memtable_size_limit.is_some() {
            self.memtable_size_limit
        } else if let MemoryBudget::Bytes(n) = self.memory_budget {
            Self::sanitize_memtable_bytes(n / 2)
        } else {
            self.memtable_size_limit
        }
    }

    pub(crate) fn runtime_memtable_flush_threshold(&self) -> usize {
        self.memtable_flush_threshold
    }

    fn sanitize_memtable_bytes(bytes: usize) -> usize {
        bytes.max(1)
    }
}

mod memory {
    #[cfg(target_os = "linux")]
    use std::fs;
    #[cfg(target_os = "linux")]
    use std::path::Path;

    pub fn auto_memory_budget_bytes() -> Option<usize> {
        let limit = effective_memory_limit_bytes()?;
        Some(budget_from_limit_bytes(limit))
    }

    fn budget_from_limit_bytes(limit: u64) -> usize {
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        let mut budget = limit / 2;
        let min_budget: usize = 64 * 1024 * 1024;
        if limit >= min_budget.saturating_mul(2) {
            budget = budget.max(min_budget);
        } else {
            budget = budget.max(1024 * 1024);
        }
        budget.min(limit).max(1)
    }

    fn effective_memory_limit_bytes() -> Option<u64> {
        let host_total = host_total_memory_bytes();
        let cgroup_limit = cgroup_memory_limit_bytes();

        match (host_total, cgroup_limit) {
            (Some(host), Some(limit)) => Some(host.min(limit)),
            (Some(host), None) => Some(host),
            (None, Some(limit)) => Some(limit),
            (None, None) => None,
        }
    }

    fn host_total_memory_bytes() -> Option<u64> {
        let mut system = sysinfo::System::new();
        system.refresh_memory();
        let total_kb = system.total_memory();
        if total_kb == 0 {
            return None;
        }
        Some(total_kb.saturating_mul(1024))
    }

    #[cfg(target_os = "linux")]
    fn cgroup_memory_limit_bytes() -> Option<u64> {
        let v2_limit = cgroup_v2_limit_bytes();
        if v2_limit.is_some() {
            return v2_limit;
        }
        cgroup_v1_limit_bytes()
    }

    #[cfg(not(target_os = "linux"))]
    fn cgroup_memory_limit_bytes() -> Option<u64> {
        None
    }

    #[cfg(target_os = "linux")]
    fn cgroup_v2_limit_bytes() -> Option<u64> {
        let controllers = Path::new("/sys/fs/cgroup/cgroup.controllers");
        if !controllers.exists() {
            return None;
        }
        let max_path = Path::new("/sys/fs/cgroup/memory.max");
        let value = fs::read_to_string(max_path).ok()?;
        let trimmed = value.trim();
        if trimmed.eq_ignore_ascii_case("max") {
            return None;
        }
        trimmed.parse::<u64>().ok().filter(|v| *v > 0)
    }

    #[cfg(target_os = "linux")]
    fn cgroup_v1_limit_bytes() -> Option<u64> {
        let max_path = Path::new("/sys/fs/cgroup/memory/memory.limit_in_bytes");
        let value = fs::read_to_string(max_path).ok()?;
        let limit = value.trim().parse::<u64>().ok()?;
        if limit == 0 {
            return None;
        }
        Some(limit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========== Goal Enum Tests ==========

    #[test]
    fn should_have_latency_as_default_goal() {
        assert_eq!(Goal::default(), Goal::Latency);
    }

    #[test]
    fn should_create_throughput_goal() {
        assert_eq!(Goal::Throughput, Goal::Throughput);
    }

    #[test]
    fn should_create_cost_goal() {
        assert_eq!(Goal::Economy, Goal::Economy);
    }

    #[test]
    fn should_distinguish_different_goals() {
        assert_ne!(Goal::Latency, Goal::Throughput);
        assert_ne!(Goal::Throughput, Goal::Economy);
        assert_ne!(Goal::Economy, Goal::Latency);
    }

    // ========== MemoryBudget Enum Tests ==========

    #[test]
    fn should_have_auto_as_default_memory_budget() {
        assert_eq!(MemoryBudget::default(), MemoryBudget::Auto);
    }

    #[test]
    fn should_create_explicit_memory_budget() {
        let budget = MemoryBudget::Bytes(4 * 1024 * 1024 * 1024);
        assert_eq!(budget, MemoryBudget::Bytes(4 * 1024 * 1024 * 1024));
    }

    #[test]
    fn should_distinguish_memory_budgets() {
        assert_ne!(MemoryBudget::Auto, MemoryBudget::Bytes(1000));
    }

    // ========== WorkloadProfile Enum Tests ==========

    #[test]
    fn should_have_mixed_as_default_workload() {
        assert_eq!(WorkloadProfile::default(), WorkloadProfile::Mixed);
    }

    #[test]
    fn should_create_write_heavy_workload() {
        assert_eq!(WorkloadProfile::WriteHeavy, WorkloadProfile::WriteHeavy);
    }

    #[test]
    fn should_create_read_mostly_workload() {
        assert_eq!(WorkloadProfile::ReadMostly, WorkloadProfile::ReadMostly);
    }

    #[test]
    fn should_create_range_scan_workload() {
        assert_eq!(WorkloadProfile::RangeScan, WorkloadProfile::RangeScan);
    }

    #[test]
    fn should_create_ttl_heavy_workload() {
        assert_eq!(WorkloadProfile::TtlHeavy, WorkloadProfile::TtlHeavy);
    }

    #[test]
    fn should_distinguish_workload_profiles() {
        // Arrange
        // (no setup required)

        // Act
        // (compare variants)

        // Assert
        assert_ne!(WorkloadProfile::Mixed, WorkloadProfile::WriteHeavy);
        assert_ne!(WorkloadProfile::WriteHeavy, WorkloadProfile::ReadMostly);
        assert_ne!(WorkloadProfile::ReadMostly, WorkloadProfile::RangeScan);
        assert_ne!(WorkloadProfile::RangeScan, WorkloadProfile::TtlHeavy);
    }

    // ========== Cloud Provider Constructor Tests ==========

    #[test]
    fn should_create_aws_s3_with_default_chain() {
        let provider = CloudProviderConfig::aws_s3("bucket", "us-east-1");

        assert_eq!(
            provider,
            CloudProviderConfig::AwsS3 {
                bucket: "bucket".to_string(),
                region: "us-east-1".to_string(),
                credentials: S3CredentialSource::AwsDefaultChain,
            }
        );
    }

    #[test]
    fn should_create_s3_compatible_env_with_safe_defaults() {
        let provider = CloudProviderConfig::s3_compatible_env("bucket", "http://localhost:9000");

        assert_eq!(
            provider,
            CloudProviderConfig::S3Compatible {
                bucket: "bucket".to_string(),
                region: "us-east-1".to_string(),
                endpoint: "http://localhost:9000".to_string(),
                path_style: true,
                credentials: S3CredentialSource::Environment,
            }
        );
    }

    #[test]
    fn should_create_s3_family_static_configs() {
        let minio =
            CloudProviderConfig::minio_static("bucket", "http://minio:9000", "key", "secret");
        let wasabi = CloudProviderConfig::wasabi_static("bucket", "us-east-2", "key", "secret");
        let oci = CloudProviderConfig::oci_s3_compatible_static(
            "namespace",
            "bucket",
            "us-phoenix-1",
            "key",
            "secret",
        );

        assert!(matches!(minio, CloudProviderConfig::Minio { .. }));
        assert!(matches!(
            wasabi,
            CloudProviderConfig::Wasabi {
                credentials: S3CredentialSource::Static { .. },
                endpoint: None,
                ..
            }
        ));
        assert!(matches!(
            oci,
            CloudProviderConfig::OciS3Compatible {
                path_style: false,
                credentials: S3CredentialSource::Static { .. },
                ..
            }
        ));
    }

    #[test]
    fn should_create_azure_configs_for_identity_and_storage_credentials() {
        let identity = CloudProviderConfig::azure_blob("account", "container");
        let shared_key =
            CloudProviderConfig::azure_blob_shared_key("account", "container", "account-key");
        let sas = CloudProviderConfig::azure_blob_sas("account", "container", "?sig=token");
        let conn = CloudProviderConfig::azure_blob_connection_string(
            "container",
            "AccountName=account;AccountKey=key",
        );

        assert!(matches!(
            identity,
            CloudProviderConfig::AzureBlob {
                credential: AzureCredentialSource::LightweightDefaultChain,
                ..
            }
        ));
        assert!(matches!(
            shared_key,
            CloudProviderConfig::AzureBlob {
                credential: AzureCredentialSource::SharedKey { .. },
                ..
            }
        ));
        assert!(matches!(
            sas,
            CloudProviderConfig::AzureBlob {
                credential: AzureCredentialSource::SasToken { .. },
                ..
            }
        ));
        assert!(matches!(
            conn,
            CloudProviderConfig::AzureBlob {
                account,
                credential: AzureCredentialSource::ConnectionString { .. },
                ..
            } if account == "account"
        ));
    }

    #[test]
    fn should_create_gcs_configs_with_matching_api_styles() {
        let adc = CloudProviderConfig::gcs("bucket");
        let hmac = CloudProviderConfig::gcs_hmac("bucket", "access", "secret");
        let bearer = CloudProviderConfig::gcs_bearer_token("bucket", "token");

        assert!(matches!(
            adc,
            CloudProviderConfig::Gcs {
                api: GcsApiStyle::Json,
                credential: GcsCredentialSource::ApplicationDefault,
                ..
            }
        ));
        assert!(matches!(
            hmac,
            CloudProviderConfig::Gcs {
                api: GcsApiStyle::Xml,
                credential: GcsCredentialSource::HmacKey { .. },
                ..
            }
        ));
        assert!(matches!(
            bearer,
            CloudProviderConfig::Gcs {
                api: GcsApiStyle::Json,
                credential: GcsCredentialSource::BearerToken { .. },
                ..
            }
        ));
    }

    #[test]
    fn should_apply_fluent_cloud_modifiers() {
        let s3 = CloudProviderConfig::s3_compatible_env("bucket", "http://old")
            .with_endpoint("http://new")
            .expect("endpoint override")
            .with_s3_region("eu-west-1")
            .expect("region override")
            .with_path_style(false)
            .expect("path-style override")
            .with_s3_credentials(S3CredentialSource::access_key("key", "secret"))
            .expect("s3 credentials");
        let gcs = CloudProviderConfig::gcs_hmac("bucket", "access", "secret")
            .with_gcs_credentials(GcsCredentialSource::application_default())
            .expect("gcs credentials");

        assert_eq!(
            s3,
            CloudProviderConfig::S3Compatible {
                bucket: "bucket".to_string(),
                region: "eu-west-1".to_string(),
                endpoint: "http://new".to_string(),
                path_style: false,
                credentials: S3CredentialSource::access_key("key", "secret"),
            }
        );
        assert!(matches!(
            gcs,
            CloudProviderConfig::Gcs {
                api: GcsApiStyle::Json,
                credential: GcsCredentialSource::ApplicationDefault,
                ..
            }
        ));
    }

    #[test]
    fn should_reject_mismatched_cloud_credentials() {
        let result = CloudProviderConfig::gcs("bucket")
            .try_with_credentials(S3CredentialSource::access_key("key", "secret"));

        assert!(result.is_err());
    }

    #[test]
    fn should_reject_unsupported_cloud_modifiers() {
        assert!(CloudProviderConfig::aws_s3("bucket", "us-east-1")
            .with_endpoint("http://localhost:9000")
            .is_err());
        assert!(CloudProviderConfig::gcs("bucket")
            .with_path_style(true)
            .is_err());
        assert!(
            CloudProviderConfig::minio_static("bucket", "http://minio:9000", "key", "secret")
                .with_s3_region("us-west-2")
                .is_err()
        );
    }

    #[test]
    fn should_parse_azure_account_from_connection_string_config() {
        let provider = CloudProviderConfig::azure_blob_connection_string(
            "container",
            "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=key",
        );

        assert!(matches!(
            provider,
            CloudProviderConfig::AzureBlob { account, .. } if account == "myaccount"
        ));
    }

    #[test]
    fn should_reject_connection_string_credential_override_without_account() {
        let provider = CloudProviderConfig::AzureBlob {
            account: String::new(),
            container: "container".to_string(),
            endpoint: None,
            credential: AzureCredentialSource::connection_string("AccountKey=key"),
        };

        let result = provider.with_azure_credentials(AzureCredentialSource::shared_key("key"));

        assert!(result.is_err());
    }

    #[test]
    fn should_create_workload_identity_credentials_without_none_annotations() {
        let client = AzureCredentialSource::workload_identity_for_client("client-id");
        let file = AzureCredentialSource::workload_identity_from_file("/var/run/token");
        let full = AzureCredentialSource::workload_identity_with(
            Some("tenant-id".to_string()),
            None,
            Some(PathBuf::from("/var/run/token")),
        );

        assert!(matches!(
            client,
            AzureCredentialSource::WorkloadIdentity {
                client_id: Some(_),
                ..
            }
        ));
        assert!(matches!(
            file,
            AzureCredentialSource::WorkloadIdentity {
                token_file: Some(_),
                ..
            }
        ));
        assert!(matches!(
            full,
            AzureCredentialSource::WorkloadIdentity {
                tenant_id: Some(_),
                client_id: None,
                token_file: Some(_),
            }
        ));
    }

    #[test]
    fn should_clamp_memtable_for_small_explicit_budget() {
        // Arrange
        let budget = 64 * 1024 * 1024;

        // Act
        let opts = OpenOptions::in_memory()
            .goal(Goal::Throughput)
            .memory_budget(MemoryBudget::Bytes(budget))
            .build();

        // Assert
        assert_eq!(opts.derived_memory_budget, budget);
        assert!(opts.memtable_size_limit() <= budget / 2);
        assert!(opts.block_cache_size() <= budget);
    }

    #[test]
    fn should_use_explicit_memtable_size_for_flush_threshold_when_only_size_override_is_set() {
        // Arrange
        let size_limit = 128 * 1024;

        // Act
        let opts = OpenOptions::in_memory()
            .with_memtable_size_limit(size_limit)
            .build();

        // Assert
        assert_eq!(opts.memtable_size_limit(), size_limit);
        assert_eq!(opts.memtable_flush_threshold, size_limit);
    }

    #[test]
    fn should_preserve_explicit_memtable_size_and_flush_threshold_when_both_are_set() {
        // Arrange
        let size_limit = 256 * 1024;
        let flush_threshold = 128 * 1024;

        // Act
        let opts = OpenOptions::in_memory()
            .with_memtable_size_limit(size_limit)
            .with_memtable_flush_threshold(flush_threshold)
            .build();

        // Assert
        assert_eq!(opts.memtable_size_limit(), size_limit);
        assert_eq!(opts.memtable_flush_threshold, flush_threshold);
    }

    #[test]
    fn should_clamp_zero_memtable_overrides_to_one() {
        // Arrange
        // (no setup required)

        // Act
        let opts = OpenOptions::in_memory()
            .with_memtable_size_limit(0)
            .with_memtable_flush_threshold(0)
            .build();

        // Assert
        assert_eq!(opts.memtable_size_limit(), 1);
        assert_eq!(opts.memtable_flush_threshold, 1);
    }

    // ========== OpenOptions Builder Tests ==========

    #[test]
    fn should_create_in_memory_options() {
        // Arrange
        // (no setup required)

        // Act
        let opts = OpenOptions::in_memory();

        // Assert
        assert_eq!(opts.storage, Storage::InMemory);
        assert_eq!(opts.goal, Goal::Latency);
        assert_eq!(opts.memory_budget, MemoryBudget::Auto);
        assert_eq!(opts.workload, WorkloadProfile::Mixed);
    }

    #[test]
    fn should_create_local_options_with_path() {
        // Arrange
        // (no setup required)

        // Act
        let opts = OpenOptions::local("./test_db");

        // Assert
        assert_eq!(
            opts.storage,
            Storage::Local {
                path: PathBuf::from("./test_db")
            }
        );
    }

    #[test]
    fn should_set_goal_when_calling_goal() {
        // Arrange
        // Act
        let opts = OpenOptions::in_memory().goal(Goal::Throughput);

        // Assert
        assert_eq!(opts.goal, Goal::Throughput);
    }

    #[test]
    fn should_set_memory_budget_when_calling_memory_budget() {
        // Arrange
        let budget = MemoryBudget::Bytes(2 * 1024 * 1024 * 1024);

        // Act
        let opts = OpenOptions::in_memory().memory_budget(budget);

        // Assert
        assert_eq!(opts.memory_budget, budget);
    }

    #[test]
    fn should_set_workload_when_calling_workload() {
        let opts = OpenOptions::in_memory().workload(WorkloadProfile::WriteHeavy);
        assert_eq!(opts.workload, WorkloadProfile::WriteHeavy);
    }

    #[test]
    fn should_support_fluent_builder_chain() {
        // Arrange
        // (no setup required)

        // Act
        let opts = OpenOptions::local("./db")
            .goal(Goal::Latency)
            .workload(WorkloadProfile::ReadMostly)
            .build();

        // Assert
        assert_eq!(
            opts.storage,
            Storage::Local {
                path: PathBuf::from("./db")
            }
        );
        assert_eq!(opts.goal, Goal::Latency);
        assert_eq!(opts.workload, WorkloadProfile::ReadMostly);
    }

    #[test]
    fn should_derive_parameters_when_building() {
        // Arrange
        // (no setup required)

        // Act
        let opts = OpenOptions::in_memory().goal(Goal::Latency).build();

        // Assert
        assert!(opts.block_size > 0);
        assert!(opts.memtable_size_limit > 0);
        assert!(opts.target_sst_size > 0);
        assert!(opts.block_cache_size > 0);
    }

    #[test]
    fn should_use_different_block_sizes_for_different_goals() {
        // Arrange
        // (no setup required)

        // Act
        let latency_opts = OpenOptions::in_memory().goal(Goal::Latency).build();
        let throughput_opts = OpenOptions::in_memory().goal(Goal::Throughput).build();

        // Assert
        assert_ne!(latency_opts.block_size, throughput_opts.block_size);
    }

    #[test]
    fn should_use_different_memtable_sizes_for_different_workloads() {
        // Arrange
        // (no setup required)

        // Act
        let normal = OpenOptions::in_memory()
            .workload(WorkloadProfile::Mixed)
            .build();
        let write_heavy = OpenOptions::in_memory()
            .workload(WorkloadProfile::WriteHeavy)
            .build();

        // Assert
        assert!(write_heavy.memtable_size_limit >= normal.memtable_size_limit);
    }

    #[test]
    fn should_provide_getter_methods() {
        // Arrange
        // (no setup required)

        // Act
        let opts = OpenOptions::in_memory().build();

        // Assert - getters should be callable
        let _ = opts.block_size();
        let _ = opts.memtable_size_limit();
        let _ = opts.target_sst_size();
        let _ = opts.block_cache_size();
        let _ = opts.wal_buffer_size();
        let _ = opts.l0_compaction_trigger();
    }

    #[test]
    fn should_respect_explicit_memory_budget() {
        // Arrange
        // Use a realistic budget larger than 2x memtable size to have cache allocation
        let budget = MemoryBudget::Bytes(512 * 1024 * 1024); // 512MB

        // Act
        let opts = OpenOptions::in_memory().memory_budget(budget).build();

        // Assert
        assert!(opts.block_cache_size > 0);
    }

    #[test]
    fn should_clone_options() {
        // Arrange
        let original = OpenOptions::in_memory().goal(Goal::Throughput);

        // Act
        let cloned = original.clone();

        // Assert
        assert_eq!(cloned.goal, original.goal);
    }
}

/// Durability level for runtime use
///
/// NOTE: This enum is for INTERNAL runtime durability tracking only.
/// It should NOT be exposed in OpenOptions or any user-facing configuration.
/// Write-time durability decisions use WriteOptions::DurabilityPolicy instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Strict - fsync on every write
    Strict,
    /// Steady - fsync every N ms
    Steady,
    /// CloudPersisted - wait for cloud backup
    CloudPersisted,
}
