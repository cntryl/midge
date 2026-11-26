use std::path::PathBuf;
use std::sync::Arc;

#[cfg(test)]
use super::OpenOptions;
#[cfg(test)]
use cntryl_midge_core::common::MidgeResult;
use crate::runtime::{Runtime, RuntimeState};

mod assembly;
mod cloud_format_upgrade;
mod deadline;
use crate::runtime::cloud_startup::CloudStartupRecovery;
mod epoch_floor;
pub(super) mod format_upgrade;
mod format_upgrade_cloud;
pub(super) mod lease_selection;
mod recovery;
mod storage;
mod storage_root;
pub(super) use storage_root::existing_host_root;
mod streaming_recovery;
#[cfg(any(test, feature = "failpoints"))]
mod test_control;
#[cfg(feature = "failpoints")]
pub use test_control::{install_manual_caller_expiry, OwnerExit};
mod timing;

struct StartupStoragePath {
    db_path: PathBuf,
    memory_mode: bool,
    /// Every database file is reached through this root (#808).
    root: Arc<dyn cntryl_midge_core::io::Fs>,
}

struct StartupLease {
    lease: Arc<dyn cntryl_midge_durability::lease::PrimaryLease>,
    lease_guard: Option<cntryl_midge_durability::lease::LeaseGuard>,
    writer_epoch: u64,
    leader_store: Option<Arc<dyn cntryl_midge_durability::lease::LeaderStore>>,
    lease_healthy: Arc<std::sync::atomic::AtomicBool>,
    lease_validity: Option<Arc<cntryl_midge_durability::lease::LeaseValidity>>,
    lease_heartbeat: Option<cntryl_midge_durability::lease::LeaseHeartbeat>,
    cleanup_observer: Option<Arc<dyn crate::runtime::StartupObserver>>,
}

struct RuntimeStorageMaterialization {
    state: RuntimeState,
    runtime_config: crate::runtime::RuntimeConfig,
    /// The object store whose SSTs back the manifest, checked at startup.
    sst_inventory: Option<Arc<dyn cntryl_midge_storage::inventory::ObjectInventory>>,
    cloud_storage_for_restore: Option<Arc<cntryl_midge_storage::cloud::CloudStorage>>,
    cloud_metadata_storage_for_mirror: Option<Arc<cntryl_midge_storage::cloud::CloudStorage>>,
    /// The disk cache of remote SST ranges, so a recovery path that rebuilds
    /// the SST read view keeps serving from it (#938).
    remote_block_cache: Option<Arc<cntryl_midge_storage::remote_block_cache::RemoteBlockCache>>,
    streaming_wal: Option<streaming_recovery::CloudReplay>,
}

struct RuntimeRecoveryMaterialization {
    state: RuntimeState,
    runtime_config: crate::runtime::RuntimeConfig,
    recovered_sequence: u64,
    recovered_cf_metas: Vec<cntryl_midge_durability::metadata::ColumnFamilyMeta>,
}

struct StartedRuntime {
    #[cfg(feature = "internal-testing")]
    metadata_accounting: cntryl_midge_durability::metadata::accounting::MetricsHandle,
    runtime: Runtime,
    runtime_handle: crate::runtime::RuntimeHandle,
    recovered_sequence: u64,
    recovered_cf_metas: Vec<cntryl_midge_durability::metadata::ColumnFamilyMeta>,
    admission: Option<crate::runtime::StartupAdmission>,
}

struct FacadeAssembly;

pub(super) struct EngineStartup;

fn provider_kind(provider: &cntryl_midge_storage::config::CloudProviderConfig) -> &'static str {
    match provider {
        cntryl_midge_storage::config::CloudProviderConfig::AwsS3(_) => "aws-s3",
        cntryl_midge_storage::config::CloudProviderConfig::S3Compatible(_) => "s3-compatible",
        cntryl_midge_storage::config::CloudProviderConfig::AzureBlob(_) => "azure-blob",
        cntryl_midge_storage::config::CloudProviderConfig::Gcs(_) => "gcs",
        cntryl_midge_storage::config::CloudProviderConfig::OciObjectStorage(_) => "oci-object-storage",
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
