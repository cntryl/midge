//! Runtime-owned catalog decoding with admitted storage and retained allocations.

use super::{
    contextualize_cloud_error, ControlObject, HybridStorage, MidgeError, MidgeResult,
    WalPublicationCatalog,
};
use crate::common::resource_budget::{ResourceBudget, ResourceReservation};

pub(super) struct CatalogAuthority {
    pub(super) primary: ControlObject,
    pub(super) catalog: AdmittedCatalog,
}

#[derive(Debug)]
pub(crate) struct AdmittedCatalog {
    catalog: WalPublicationCatalog,
    _memory: ResourceReservation,
}

impl std::ops::Deref for AdmittedCatalog {
    type Target = WalPublicationCatalog;
    fn deref(&self) -> &Self::Target {
        &self.catalog
    }
}
impl std::ops::DerefMut for AdmittedCatalog {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.catalog
    }
}
impl AdmittedCatalog {
    pub(super) fn empty(storage: &HybridStorage, epoch: u64) -> MidgeResult<Self> {
        Ok(Self {
            _memory: catalog_budget(storage).reserve(4096, "empty WAL catalog")?,
            catalog: WalPublicationCatalog::empty(epoch).map_err(MidgeError::Internal)?,
        })
    }
    fn decode(bytes: &[u8], budget: &ResourceBudget) -> MidgeResult<Self> {
        // Covers the decoded tree, keys, serde scratch, and one inserted entry.
        // Allocation is admitted before serde visits attacker-controlled lengths.
        let memory = budget.reserve(
            bytes.len().saturating_mul(16).saturating_add(4096),
            "decoded WAL catalog",
        )?;
        Ok(Self {
            catalog: WalPublicationCatalog::decode(bytes).map_err(MidgeError::Corruption)?,
            _memory: memory,
        })
    }
}

pub(super) fn catalog_budget(storage: &HybridStorage) -> ResourceBudget {
    storage
        .maintenance_memory()
        .unwrap_or_else(|| ResourceBudget::new(crate::compaction::DEFAULT_COMPACTION_MEMORY_LIMIT))
}

struct AdmittedEncoding {
    bytes: Vec<u8>,
    _memory: ResourceReservation,
}
impl AdmittedEncoding {
    fn new(catalog: &WalPublicationCatalog, budget: &ResourceBudget) -> MidgeResult<Self> {
        catalog.validate().map_err(MidgeError::Corruption)?;
        let mut count = crate::common::resource_budget::ByteCounter::default();
        serde_json::to_writer_pretty(&mut count, catalog)
            .map_err(|error| MidgeError::Internal(error.to_string()))?;
        let memory = budget.reserve(count.0, "encoded WAL catalog")?;
        let mut bytes = Vec::with_capacity(count.0);
        serde_json::to_writer_pretty(&mut bytes, catalog)
            .map_err(|error| MidgeError::Internal(error.to_string()))?;
        Ok(Self {
            bytes,
            _memory: memory,
        })
    }
}

pub(super) fn load_and_repair_catalog_within(
    storage: &HybridStorage,
    deadline: &crate::common::OperationDeadline,
) -> MidgeResult<Option<CatalogAuthority>> {
    let budget = catalog_budget(storage);
    let primary = storage
        .read_control_object(crate::wal::cloud_catalog::OBJECT_KEY, &budget, deadline)
        .map_err(|error| {
            contextualize_cloud_error(error, "cloud WAL publication catalog unavailable")
        })?;

    if let Some(primary) = primary {
        match AdmittedCatalog::decode(primary.bytes(), &budget) {
            Ok(catalog) => {
                sync_catalog_copy_within(
                    storage,
                    crate::wal::cloud_catalog::MIRROR_OBJECT_KEY,
                    primary.bytes(),
                    deadline,
                )?;
                return Ok(Some(CatalogAuthority { primary, catalog }));
            }
            Err(error @ MidgeError::ResourceLimit(_)) => return Err(error),
            Err(primary_error) => {
                let mirror = storage
                    .read_control_object(
                        crate::wal::cloud_catalog::MIRROR_OBJECT_KEY,
                        &budget, deadline,
                    )
                    .map_err(|error| {
                        contextualize_cloud_error(
                            error,
                            "cloud WAL publication catalog mirror unavailable",
                        )
                    })?
                    .ok_or_else(|| {
                        MidgeError::Corruption(format!(
                            "primary cloud WAL publication catalog is invalid and no mirror exists: {primary_error}"
                        ))
                    })?;
                let catalog = AdmittedCatalog::decode(mirror.bytes(), &budget).map_err(|mirror_error| {
                    if matches!(mirror_error, MidgeError::ResourceLimit(_)) { return mirror_error; }
                    MidgeError::Corruption(format!(
                        "both cloud WAL publication catalogs are invalid; primary: {primary_error}; mirror: {mirror_error}"
                    ))
                })?;
                tracing::warn!(
                    error = %primary_error,
                    "repairing invalid cloud WAL publication catalog from validated mirror"
                );
                let repaired = sync_catalog_copy_within(
                    storage,
                    crate::wal::cloud_catalog::OBJECT_KEY,
                    mirror.bytes(),
                    deadline,
                )?;
                return Ok(Some(CatalogAuthority {
                    primary: repaired,
                    catalog,
                }));
            }
        }
    }

    let Some(mirror) = storage
        .read_control_object(
            crate::wal::cloud_catalog::MIRROR_OBJECT_KEY,
            &budget,
            deadline,
        )
        .map_err(|error| {
            contextualize_cloud_error(error, "cloud WAL publication catalog mirror unavailable")
        })?
    else {
        return Ok(None);
    };
    let catalog = AdmittedCatalog::decode(mirror.bytes(), &budget).map_err(|error| {
        if matches!(error, MidgeError::ResourceLimit(_)) {
            return error;
        }
        MidgeError::Corruption(format!(
            "primary cloud WAL publication catalog is missing and its mirror is invalid: {error}"
        ))
    })?;
    tracing::warn!("restoring missing cloud WAL publication catalog from validated mirror");
    let repaired = sync_catalog_copy_within(
        storage,
        crate::wal::cloud_catalog::OBJECT_KEY,
        mirror.bytes(),
        deadline,
    )?;
    Ok(Some(CatalogAuthority {
        primary: repaired,
        catalog,
    }))
}

pub(super) fn commit_catalog_within(
    storage: &HybridStorage,
    expected_primary: Option<&ControlObject>,
    catalog: &WalPublicationCatalog,
    deadline: &crate::common::OperationDeadline,
) -> MidgeResult<ControlObject> {
    let budget = catalog_budget(storage);
    let encoded = AdmittedEncoding::new(catalog, &budget)?;
    let primary = storage.write_control_object(
        crate::wal::cloud_catalog::OBJECT_KEY,
        expected_primary.map(ControlObject::metadata),
        &encoded.bytes,
        &budget,
        deadline,
    )?;
    sync_catalog_copy_within(
        storage,
        crate::wal::cloud_catalog::MIRROR_OBJECT_KEY,
        &encoded.bytes,
        deadline,
    )?;
    Ok(primary)
}

fn sync_catalog_copy_within(
    storage: &HybridStorage,
    key: &str,
    bytes: &[u8],
    deadline: &crate::common::OperationDeadline,
) -> MidgeResult<ControlObject> {
    let budget = catalog_budget(storage);
    let existing = storage
        .read_control_object(key, &budget, deadline)
        .map_err(|error| contextualize_cloud_error(error, "cloud WAL catalog copy unavailable"))?;
    if existing
        .as_ref()
        .is_some_and(|existing| existing.bytes() == bytes)
    {
        return Ok(existing.expect("matching copy"));
    }
    storage
        .write_control_object(
            key,
            existing.as_ref().map(ControlObject::metadata),
            bytes,
            &budget,
            deadline,
        )
        .map_err(|error| contextualize_cloud_error(error, "cloud WAL catalog copy update failed"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::hybrid_persistence::{
        CloudStorage, HybridPersistence, PublishedWalSegment,
    };
    use std::sync::Arc;

    #[test]
    fn should_retain_catalog_decode_charge_until_authority_is_dropped() {
        // Arrange
        let bytes = WalPublicationCatalog::empty(7).unwrap().encode().unwrap();
        let budget = ResourceBudget::new(1024 * 1024);

        // Act
        let catalog = AdmittedCatalog::decode(&bytes, &budget).unwrap();
        let retained = budget.used();
        drop(catalog);

        // Assert
        assert!(retained >= bytes.len());
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn should_preserve_catalog_encoding_when_serialization_is_admitted() {
        // Arrange
        let catalog = WalPublicationCatalog::empty(7).unwrap();
        let expected = catalog.encode().unwrap();
        let budget = ResourceBudget::new(expected.len());

        // Act
        let encoded = AdmittedEncoding::new(&catalog, &budget).unwrap();

        // Assert
        assert_eq!(encoded.bytes, expected);
        assert_eq!(budget.used(), expected.len());
        drop(encoded);
        assert_eq!(budget.used(), 0);
        assert!(matches!(
            AdmittedEncoding::new(&catalog, &ResourceBudget::new(expected.len() - 1)),
            Err(MidgeError::ResourceLimit(_))
        ));
    }

    #[test]
    fn should_leave_catalog_authority_unchanged_when_decode_admission_fails() {
        // Arrange
        let directory = tempfile::tempdir().unwrap();
        let storage = HybridStorage::with_policy(
            Arc::new(crate::storage::filesystem::FileSystem::new(directory.path()).unwrap()),
            Arc::new(CloudStorage::with_mock()),
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        );
        let mut catalog = WalPublicationCatalog::empty(7).unwrap();
        for segment_id in 0..128 {
            catalog
                .publish(
                    7,
                    PublishedWalSegment {
                        segment_id,
                        writer_epoch: 7,
                        max_sequence: segment_id + 1,
                        size_bytes: 1,
                        content_crc32c: 0,
                        object_key: crate::wal::cloud_segment_object_key(segment_id, 7),
                    },
                )
                .unwrap();
        }
        let original = catalog.encode().unwrap();
        let key = crate::wal::cloud_catalog::OBJECT_KEY;
        storage
            .compare_exchange_remote_object(key, None, original.clone())
            .unwrap();
        storage.configure_maintenance_memory(256 * 1024);

        // Act
        let result = storage.fence_cloud_wal_catalog(8);

        // Assert
        assert!(
            matches!(result, Err(MidgeError::ResourceLimit(_))),
            "{result:?}"
        );
        assert_eq!(storage.remote_object_proof(key).unwrap().bytes(), original);
        assert!(storage
            .remote_object_proof_optional(crate::wal::cloud_catalog::MIRROR_OBJECT_KEY)
            .unwrap()
            .is_none());
        let budget = storage.maintenance_memory().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while budget.used() != 0 && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn should_release_decode_admission_when_catalog_is_corrupt() {
        // Arrange
        let budget = ResourceBudget::new(1024 * 1024);

        // Act
        let result = AdmittedCatalog::decode(b"{broken", &budget);

        // Assert
        assert!(matches!(result, Err(MidgeError::Corruption(_))));
        assert_eq!(budget.used(), 0);
    }
}
