//! Runtime-owned catalog decoding with admitted storage and retained allocations.

use super::{
    contextualize_cloud_error, ControlObject, HybridStorage, MidgeError, MidgeResult,
    WalPublicationCatalog,
};
use crate::common::resource_budget::{ResourceBudget, ResourceReservation};
use crate::wal::cloud_catalog::CatalogRefusal;

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
    // Startup normally installs the configured limit before catalog fencing.
    // Standalone callers also need one shared pool, even before runtime setup.
    storage
        .configure_maintenance_memory(crate::compaction::DEFAULT_COMPACTION_MEMORY_LIMIT)
        .with_contention_errors()
}

struct AdmittedEncoding {
    bytes: Vec<u8>,
    _memory: ResourceReservation,
}
impl AdmittedEncoding {
    fn new(catalog: &WalPublicationCatalog, budget: &ResourceBudget) -> MidgeResult<Self> {
        catalog.validate().map_err(MidgeError::Corruption)?;
        // JSON whitespace is not part of version-1 authority. Keep the counting
        // and writing serializers identical so admission precedes allocation.
        let mut count = crate::common::resource_budget::ByteCounter::default();
        serde_json::to_writer(&mut count, catalog)
            .map_err(|error| MidgeError::Internal(error.to_string()))?;
        let memory = budget.reserve(count.0, "encoded WAL catalog")?;
        let mut bytes = Vec::with_capacity(count.0);
        serde_json::to_writer(&mut bytes, catalog)
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
    load_and_repair_catalog_with_authority(storage, deadline, &|| storage.check_write_authority())
}

pub(super) fn load_and_repair_catalog_with_authority(
    storage: &HybridStorage,
    deadline: &crate::common::OperationDeadline,
    validate: &dyn Fn() -> MidgeResult<()>,
) -> MidgeResult<Option<CatalogAuthority>> {
    validate()?;
    let budget = catalog_budget(storage);
    let primary = storage
        .read_control_object(crate::wal::cloud_catalog::OBJECT_KEY, &budget, deadline)
        .map_err(|error| {
            contextualize_cloud_error(error, "cloud WAL publication catalog unavailable")
        })?;

    validate()?;
    if let Some(primary) = primary {
        match AdmittedCatalog::decode(primary.bytes(), &budget) {
            Ok(catalog) => {
                sync_catalog_copy_with_authority(
                    storage,
                    crate::wal::cloud_catalog::MIRROR_OBJECT_KEY,
                    primary.bytes(),
                    deadline,
                    validate,
                )?;
                return Ok(Some(CatalogAuthority { primary, catalog }));
            }
            Err(error @ MidgeError::ResourceLimit(_)) => return Err(error),
            Err(primary_error) => {
                let mirror = storage
                    .read_control_object(
                        crate::wal::cloud_catalog::MIRROR_OBJECT_KEY,
                        &budget,
                        deadline,
                    )
                    .map_err(|error| {
                        contextualize_cloud_error(
                            error,
                            "cloud WAL publication catalog mirror unavailable",
                        )
                    })?;
                validate()?;
                let mirror = mirror.ok_or_else(|| {
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
                let repaired = sync_catalog_copy_with_authority(
                    storage,
                    crate::wal::cloud_catalog::OBJECT_KEY,
                    mirror.bytes(),
                    deadline,
                    validate,
                )?;
                return Ok(Some(CatalogAuthority {
                    primary: repaired,
                    catalog,
                }));
            }
        }
    }

    let mirror = storage
        .read_control_object(
            crate::wal::cloud_catalog::MIRROR_OBJECT_KEY,
            &budget,
            deadline,
        )
        .map_err(|error| {
            contextualize_cloud_error(error, "cloud WAL publication catalog mirror unavailable")
        })?;
    validate()?;
    let Some(mirror) = mirror else {
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
    let repaired = sync_catalog_copy_with_authority(
        storage,
        crate::wal::cloud_catalog::OBJECT_KEY,
        mirror.bytes(),
        deadline,
        validate,
    )?;
    Ok(Some(CatalogAuthority {
        primary: repaired,
        catalog,
    }))
}

/// Whether a catalog mutation changed the catalog, with what it returns.
pub(super) enum Mutation<T> {
    /// Nothing to write.
    Unchanged(T),
    /// The catalog changed: compare-and-swap it.
    Changed(T),
}

impl<T> Mutation<T> {
    /// `value`, changed when `changed`.
    pub(super) fn of(changed: bool, value: T) -> Self {
        if changed {
            Self::Changed(value)
        } else {
            Self::Unchanged(value)
        }
    }
}

/// What a mutation does when no catalog exists yet.
#[derive(Clone, Copy)]
pub(super) enum Missing {
    /// Create an empty one owned by this epoch (lease acquisition).
    Create(u64),
    /// Refuse: every other mutation needs an existing catalog.
    Refuse,
}

/// The one read-modify-CAS path every catalog mutation takes (#858): under
/// the local catalog mutation lock, load the catalog (repairing a copy from
/// the other), apply `mutate`, a pure rule of the catalog's, and when it
/// changed commit the primary conditioned on the version read, then the
/// mirror. `validate` is the caller's authority check, run before each
/// provider call.
///
/// A rule refusal writes nothing and comes back as the inner `Err`. A lost
/// compare-and-swap re-reads the catalog: `Fenced` when a later epoch owns
/// it, `Busy` otherwise, which a later attempt may retry.
///
/// # Errors
///
/// The load or commit error.
pub(super) fn mutate_wal_catalog_within<T, R>(
    storage: &HybridStorage,
    label: &'static str,
    deadline: &crate::common::OperationDeadline,
    validate: &dyn Fn() -> MidgeResult<()>,
    missing: Missing,
    mutate: impl FnOnce(&mut WalPublicationCatalog) -> Result<Mutation<T>, R>,
) -> MidgeResult<Result<(T, AdmittedCatalog), R>> {
    validate()?;
    let _catalog_mutation = storage.lock_wal_catalog_mutation_within(deadline, label)?;
    let (mut catalog, expected) =
        match load_and_repair_catalog_with_authority(storage, deadline, validate)? {
            Some(authority) => (authority.catalog, Some(authority.primary)),
            None => match missing {
                Missing::Create(epoch) => (AdmittedCatalog::empty(storage, epoch)?, None),
                Missing::Refuse => {
                    return Err(MidgeError::Internal(
                        "cloud WAL publication catalog is missing".to_string(),
                    ))
                }
            },
        };
    let value = match mutate(&mut catalog) {
        Err(refusal) => return Ok(Err(refusal)),
        Ok(Mutation::Unchanged(value)) if expected.is_some() => return Ok(Ok((value, catalog))),
        Ok(Mutation::Unchanged(value) | Mutation::Changed(value)) => value,
    };
    match commit_catalog_with_authority(storage, expected.as_ref(), &catalog, deadline, validate) {
        Ok(_) => Ok(Ok((value, catalog))),
        Err(MidgeError::Busy(conflict)) => match current_fencing_epoch(storage, deadline)? {
            Some(winner) if winner > catalog.fencing_epoch() => Err(MidgeError::Fenced(format!(
                "cloud WAL catalog advanced to fencing epoch {winner}, writer epoch {} rejected during {label}",
                catalog.fencing_epoch()
            ))),
            _ => Err(MidgeError::Busy(conflict)),
        },
        Err(error) => Err(error),
    }
}

/// The fencing epoch of the catalog the primary holds now, read without
/// repairing either copy: `None` when it is missing or does not decode.
fn current_fencing_epoch(
    storage: &HybridStorage,
    deadline: &crate::common::OperationDeadline,
) -> MidgeResult<Option<u64>> {
    let budget = catalog_budget(storage);
    let primary = storage
        .read_control_object(crate::wal::cloud_catalog::OBJECT_KEY, &budget, deadline)
        .map_err(|error| {
            contextualize_cloud_error(error, "cloud WAL publication catalog unavailable")
        })?;
    Ok(primary
        .and_then(|primary| AdmittedCatalog::decode(primary.bytes(), &budget).ok())
        .map(|winner| winner.fencing_epoch()))
}

/// The engine error for a catalog rule refusal (#858): another epoch owns
/// the catalog, or the catalog contradicts the writer. No retry fixes
/// either.
pub(crate) fn refusal_error(refusal: CatalogRefusal) -> MidgeError {
    match refusal {
        CatalogRefusal::EpochMismatch { .. } => MidgeError::Fenced(refusal.to_string()),
        refusal => MidgeError::Corruption(format!("cloud WAL catalog refused: {refusal}")),
    }
}

fn commit_catalog_with_authority(
    storage: &HybridStorage,
    expected_primary: Option<&ControlObject>,
    catalog: &WalPublicationCatalog,
    deadline: &crate::common::OperationDeadline,
    validate: &dyn Fn() -> MidgeResult<()>,
) -> MidgeResult<ControlObject> {
    let budget = catalog_budget(storage);
    let encoded = AdmittedEncoding::new(catalog, &budget)?;
    validate()?;
    let primary = storage
        .write_control_object_with_authority(
            crate::wal::cloud_catalog::OBJECT_KEY,
            expected_primary.map(ControlObject::object_metadata),
            &encoded.bytes,
            &budget,
            deadline,
            validate,
        )
        .map_err(crate::storage::hybrid::backend::ControlWriteFailure::into_error)?;
    crate::failpoints::fail_point!("midge::cloud::after_catalog_primary_before_mirror");
    sync_catalog_copy_with_authority(
        storage,
        crate::wal::cloud_catalog::MIRROR_OBJECT_KEY,
        &encoded.bytes,
        deadline,
        validate,
    )?;
    Ok(primary)
}

fn sync_catalog_copy_with_authority(
    storage: &HybridStorage,
    key: &str,
    bytes: &[u8],
    deadline: &crate::common::OperationDeadline,
    validate: &dyn Fn() -> MidgeResult<()>,
) -> MidgeResult<ControlObject> {
    validate()?;
    let budget = catalog_budget(storage);
    let existing = storage
        .read_control_object(key, &budget, deadline)
        .map_err(|error| contextualize_cloud_error(error, "cloud WAL catalog copy unavailable"))?;
    validate()?;
    if existing
        .as_ref()
        .is_some_and(|existing| existing.bytes() == bytes)
    {
        return Ok(existing.expect("matching copy"));
    }
    storage
        .write_control_object_with_authority(
            key,
            existing.as_ref().map(ControlObject::object_metadata),
            bytes,
            &budget,
            deadline,
            validate,
        )
        .map_err(|failure| match failure {
            crate::storage::hybrid::backend::ControlWriteFailure::Authority(error) => error,
            crate::storage::hybrid::backend::ControlWriteFailure::Operation(error) => {
                contextualize_cloud_error(error, "cloud WAL catalog copy update failed")
            }
        })
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod encoding_tests;
