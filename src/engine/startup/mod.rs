use std::path::PathBuf;
use std::sync::Arc;

#[cfg(test)]
use super::OpenOptions;
#[cfg(test)]
use crate::common::MidgeResult;
use crate::runtime::{Runtime, RuntimeState};

mod assembly;
mod deadline;
use crate::runtime::cloud_startup::CloudStartupRecovery;
mod epoch_floor;
mod recovery;
mod storage;
mod streaming_recovery;
mod timing;

struct StartupStoragePath {
    db_path: PathBuf,
    memory_mode: bool,
}

struct StartupLease {
    lease: Arc<dyn crate::lease::PrimaryLease>,
    lease_guard: Option<crate::lease::LeaseGuard>,
    writer_epoch: u64,
    leader_store: Option<Arc<dyn crate::lease::LeaderStore>>,
    lease_healthy: Arc<std::sync::atomic::AtomicBool>,
    lease_validity: Option<Arc<crate::lease::LeaseValidity>>,
    lease_heartbeat: Option<crate::lease::LeaseHeartbeat>,
    cleanup_observer: Option<Arc<dyn crate::runtime::StartupObserver>>,
}

struct RuntimeStorageMaterialization {
    state: RuntimeState,
    runtime_config: crate::runtime::RuntimeConfig,
    cloud_root: Option<PathBuf>,
    cloud_storage_for_restore: Option<Arc<crate::storage::cloud::CloudStorage>>,
    cloud_metadata_storage_for_mirror: Option<Arc<crate::storage::cloud::CloudStorage>>,
    streaming_wal: Option<streaming_recovery::CloudReplay>,
}

struct RuntimeRecoveryMaterialization {
    state: RuntimeState,
    runtime_config: crate::runtime::RuntimeConfig,
    recovered_sequence: u64,
    recovered_cf_metas: Vec<crate::metadata::ColumnFamilyMeta>,
}

struct StartedRuntime {
    runtime: Runtime,
    runtime_handle: crate::runtime::RuntimeHandle,
    recovered_sequence: u64,
    recovered_cf_metas: Vec<crate::metadata::ColumnFamilyMeta>,
    admission: Option<crate::runtime::StartupAdmission>,
}

struct FacadeAssembly;

pub(super) struct EngineStartup;

fn provider_kind(provider: &crate::config::CloudProviderConfig) -> &'static str {
    match provider {
        crate::config::CloudProviderConfig::AwsS3(_) => "aws-s3",
        crate::config::CloudProviderConfig::S3Compatible(_) => "s3-compatible",
        crate::config::CloudProviderConfig::AzureBlob(_) => "azure-blob",
        crate::config::CloudProviderConfig::Gcs(_) => "gcs",
        crate::config::CloudProviderConfig::OciObjectStorage(_) => "oci-object-storage",
    }
}

fn redact_endpoint_metadata(endpoint: &str) -> String {
    let endpoint = endpoint.split(['?', '#']).next().unwrap_or(endpoint);
    let Some((scheme, authority_and_path)) = endpoint.split_once("://") else {
        return endpoint.to_string();
    };
    let authority = authority_and_path
        .split('/')
        .next()
        .unwrap_or(authority_and_path);
    format!("{scheme}://{authority}")
}

#[cfg(test)]
mod tests;
