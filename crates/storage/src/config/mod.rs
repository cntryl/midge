//! Provider and topology configuration owned by the storage crate.

pub(crate) mod cloud_validation;
mod provider;

pub use cloud_validation::{
    validate_location, validate_topology, CloudCheckCode, CloudCheckOutcome, CloudPreflightOptions,
    CloudProviderKind, CloudStorageRole, CloudValidationFinding, CloudValidationMode,
    CloudValidationReport,
};
pub use provider::{
    AwsS3Config, AzureBlobConfig, AzureCredentialSource, CloudProviderConfig, GcsApiStyle,
    GcsConfig, GcsCredentialSource, OciCredentialSource, OciObjectStorageConfig,
    S3CompatibleConfig, S3CredentialSource,
};

/// One provider-backed bucket/container and object namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudStorageLocation {
    provider: CloudProviderConfig,
    prefix: String,
}

impl CloudStorageLocation {
    #[must_use]
    pub fn new(provider: impl Into<CloudProviderConfig>, prefix: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            prefix: prefix.into().trim_matches('/').to_string(),
        }
    }

    #[must_use]
    pub fn provider(&self) -> &CloudProviderConfig {
        &self.provider
    }

    #[must_use]
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    #[must_use]
    pub fn validate(&self) -> CloudValidationReport {
        validate_location(self, &[CloudStorageRole::Standalone])
    }
}

/// Resolved locations for WAL, SST, and control objects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudStorageTopology {
    wal: CloudStorageLocation,
    sst: CloudStorageLocation,
    control: CloudStorageLocation,
}

impl CloudStorageTopology {
    #[must_use]
    pub fn new(shared: CloudStorageLocation) -> Self {
        Self {
            wal: shared.clone(),
            sst: shared.clone(),
            control: shared,
        }
    }

    #[must_use]
    pub fn with_wal(mut self, location: CloudStorageLocation) -> Self {
        self.wal = location;
        self
    }
    #[must_use]
    pub fn with_sst(mut self, location: CloudStorageLocation) -> Self {
        self.sst = location;
        self
    }
    #[must_use]
    pub fn with_control(mut self, location: CloudStorageLocation) -> Self {
        self.control = location;
        self
    }
    #[must_use]
    pub fn wal(&self) -> &CloudStorageLocation {
        &self.wal
    }
    #[must_use]
    pub fn sst(&self) -> &CloudStorageLocation {
        &self.sst
    }
    #[must_use]
    pub fn control(&self) -> &CloudStorageLocation {
        &self.control
    }
    #[must_use]
    pub fn validate(&self) -> CloudValidationReport {
        validate_topology(self)
    }
}
