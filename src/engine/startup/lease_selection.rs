//! Resolve an engine storage mode into its writer lease.

use crate::config::Storage;
use cntryl_midge_core::io::Fs;
use cntryl_midge_durability::lease::{
    CloudLeaseConfig, CloudStorageLease, FileSystemLease, LeaseClock, LeaseError, LeaseValidity,
    PrimaryLease,
};
use cntryl_midge_storage::{cloud, providers, simulated::SimulatedCloud};
use std::sync::Arc;
use std::time::Duration;

pub(in crate::engine) struct CreatedLease {
    pub(crate) lease: Arc<dyn PrimaryLease>,
    pub(crate) validity: Option<Arc<LeaseValidity>>,
}

/// The database root a test lease coordinates through: in memory for
/// in-memory storage, otherwise the host directory.
#[cfg(test)]
pub(in crate::engine) fn test_lease_root(storage: &Storage) -> Arc<dyn Fs> {
    match storage {
        Storage::InMemory => Arc::new(cntryl_midge_core::io::MockFs::new()),
        Storage::Local { path } => cntryl_midge_core::io::real::test_root(path),
        Storage::Cloud {
            local_cache_path, ..
        }
        | Storage::CloudSimulated {
            local_cache_path, ..
        } => cntryl_midge_core::io::real::test_root(local_cache_path),
    }
}

#[cfg(test)]
pub(in crate::engine) fn create_lease_with_validity(
    storage: &Storage,
    clock_skew_tolerance: Duration,
) -> Result<CreatedLease, LeaseError> {
    create_lease_with_validity_and_timeout_and_ttl(
        storage,
        &test_lease_root(storage),
        None,
        clock_skew_tolerance,
        cntryl_midge_core::common::DEFAULT_STORAGE_IO_TIMEOUT,
        Duration::from_secs(30),
        &LeaseClock::default(),
    )
}

/// Create the primary lease for `storage`. Cloud and simulated-cloud leases
/// keep their pointer and fencing document in the shared object store. Every
/// lease reads wall time from `clock`.
pub(in crate::engine) fn create_lease_with_validity_and_timeout_and_ttl(
    storage: &Storage,
    root: &Arc<dyn Fs>,
    object_store: Option<Arc<dyn cloud::CloudBackend>>,
    clock_skew_tolerance: Duration,
    storage_io_timeout: Duration,
    lease_ttl: Duration,
    clock: &LeaseClock,
) -> Result<CreatedLease, LeaseError> {
    match storage {
        Storage::InMemory => Ok(CreatedLease {
            lease: Arc::new(FileSystemLease::with_root_and_timeout(
                Arc::clone(root),
                lease_ttl,
                clock_skew_tolerance,
                clock.clone(),
                storage_io_timeout,
            )),
            validity: None,
        }),
        Storage::Local { .. } => {
            let lease = Arc::new(FileSystemLease::with_root_and_timeout(
                Arc::clone(root),
                lease_ttl,
                clock_skew_tolerance,
                clock.clone(),
                storage_io_timeout,
            ));
            Ok(CreatedLease {
                validity: Some(lease.lease_validity()),
                lease,
            })
        }
        Storage::Cloud { topology, .. } => {
            // Cloud storage: use cloud lease with TTL-based coordination
            let control = topology.control();
            let lease_provider = control.provider();
            let lease_prefix = control.prefix();
            let config = CloudLeaseConfig {
                bucket: lease_provider.bucket_or_container().to_string(),
                prefix: lease_prefix.to_string(),
            };
            let cloud = providers::build_class_store(
                lease_provider,
                lease_prefix,
                storage_io_timeout,
                object_store,
            )
            .map_err(|error| LeaseError::IoError(format!("cloud lease backend: {error}")))?;
            let lease = Arc::new(
                CloudStorageLease::new_provider_backed_with_clock_skew_tolerance_and_ttl(
                    config,
                    cloud,
                    clock_skew_tolerance,
                    lease_ttl,
                    clock.clone(),
                ),
            );
            Ok(CreatedLease {
                validity: Some(lease.lease_validity()),
                lease,
            })
        }
        Storage::CloudSimulated { bucket, prefix, .. } => {
            let config = CloudLeaseConfig {
                bucket: bucket.clone(),
                prefix: prefix.clone(),
            };
            let store = SimulatedCloud::locate(root.as_ref(), object_store, storage_io_timeout)
                .and_then(|location| location.open())
                .map_err(|error| {
                    LeaseError::IoError(format!("simulated shared lease backend: {error}"))
                })?;
            let cloud = Arc::new(cloud::CloudStorage::from_object_store(
                store,
                storage_io_timeout,
            ));
            let lease = Arc::new(
                CloudStorageLease::new_provider_backed_with_clock_skew_tolerance_and_ttl(
                    config,
                    cloud,
                    clock_skew_tolerance,
                    lease_ttl,
                    clock.clone(),
                ),
            );
            Ok(CreatedLease {
                validity: Some(lease.lease_validity()),
                lease,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cntryl_midge_durability::lease::{
        CloudMetadataGeneration, CloudMetadataHead, CloudMetadataObject, SharedGenerationId,
    };

    #[test]
    fn should_attach_monotonic_watchdog_validity_to_local_filesystem_lease() {
        // Arrange
        let directory = tempfile::tempdir().expect("create local lease directory");
        let storage = Storage::Local {
            path: directory.path().to_path_buf(),
        };

        // Act
        let created = create_lease_with_validity(&storage, Duration::from_secs(15))
            .expect("create local lease");

        // Assert
        assert!(created.validity.is_some());
    }

    #[test]
    fn should_share_simulated_metadata_authority_across_distinct_cache_roots() {
        // Arrange
        let first_cache = tempfile::tempdir().unwrap();
        let second_cache = tempfile::tempdir().unwrap();
        let backend: Arc<dyn cloud::CloudBackend> = Arc::new(cloud::MockCloudBackend::new());
        let make = |directory: &std::path::Path| {
            create_lease_with_validity_and_timeout_and_ttl(
                &Storage::CloudSimulated {
                    local_cache_path: directory.into(),
                    bucket: "shared".into(),
                    prefix: String::new(),
                },
                &cntryl_midge_core::io::real::test_root(directory),
                Some(Arc::clone(&backend)),
                Duration::ZERO,
                Duration::from_secs(5),
                Duration::from_secs(30),
                &LeaseClock::default(),
            )
            .unwrap()
        };
        let first = make(first_cache.path());
        let guard = Arc::clone(&first.lease).try_acquire().unwrap();
        let first_epoch = first.lease.epoch();
        let first_store = first.lease.get_leader_store().unwrap();
        let generation = CloudMetadataGeneration {
            id: SharedGenerationId::FIRST,
            manifest_horizon: 10,
            objects: [
                cntryl_midge_durability::metadata::files::FORMAT,
                cntryl_midge_durability::metadata::files::MANIFEST_SNAPSHOT,
            ]
            .into_iter()
            .map(|file_name| CloudMetadataObject {
                file_name: file_name.into(),
                object_key: format!("metadata/generations/{}/{file_name}", uuid::Uuid::new_v4()),
                len: 1,
                crc32c: 1,
            })
            .collect(),
        };
        first_store
            .publish_committed_metadata(
                &first.lease.holder_id(),
                first.lease.epoch(),
                None,
                generation.clone(),
                Duration::from_secs(5),
            )
            .unwrap();
        guard.release();
        first.lease.release().unwrap();
        // Act
        let second = make(second_cache.path());
        let _guard = Arc::clone(&second.lease).try_acquire().unwrap();
        let current = second
            .lease
            .get_leader_store()
            .unwrap()
            .read_committed_metadata(Duration::from_secs(5))
            .unwrap();
        // Assert
        assert_eq!(current, CloudMetadataHead::Committed(generation));
        assert!(second.lease.epoch() > first_epoch);
    }
}
