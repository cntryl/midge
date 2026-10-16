//! Runtime-owned catalog decoding with admitted storage and retained allocations.

use super::{
    contextualize_cloud_error, ControlObject, HybridStorage, MidgeError, MidgeResult,
    WalPublicationCatalog,
};
use crate::common::resource_budget::{ResourceBudget, ResourceReservation};
use crate::storage::StorageObjectMetadata;
use crate::wal::cloud_catalog::{CatalogDecodeError, CatalogRefusal};

pub(super) struct CatalogAuthority {
    pub(super) primary: ControlObject,
    pub(super) catalog: AdmittedCatalog,
}

/// A decoded catalog whose reservation always equals what it retains. Every
/// mutation goes through this type, so none can grow it unaccounted.
#[derive(Debug)]
pub(crate) struct AdmittedCatalog {
    catalog: WalPublicationCatalog,
    memory: ResourceReservation,
}

impl std::ops::Deref for AdmittedCatalog {
    type Target = WalPublicationCatalog;
    fn deref(&self) -> &Self::Target {
        &self.catalog
    }
}
impl AdmittedCatalog {
    pub(super) fn empty(storage: &HybridStorage, epoch: u64) -> MidgeResult<Self> {
        let catalog = WalPublicationCatalog::empty(epoch).map_err(MidgeError::Internal)?;
        Ok(Self {
            memory: catalog_budget(storage)
                .reserve(catalog.retained_allocation_bytes(), "empty WAL catalog")?,
            catalog,
        })
    }

    /// Three passes: the version probe, a structural pass that counts every
    /// vector without retaining rows, then the decode it admitted.
    fn decode(bytes: &[u8], budget: &ResourceBudget) -> MidgeResult<Self> {
        let bound = {
            let _scratch = budget.reserve(parser_scratch_bytes(bytes), "catalog parser scratch")?;
            WalPublicationCatalog::decode_allocation_bound(bytes)
                .map_err(|error| decode_error(&error))?
        };
        // The structural pass accepted only schema field names, the catalog's
        // only strings, so the decode's own scratch fits the bound's slack.
        // The bound covers vector growth while decoding; keep only what the
        // decoded catalog retains once growth has finished.
        let mut memory = budget.reserve(bound, "decoded WAL catalog")?;
        let catalog = WalPublicationCatalog::decode_known_format(bytes)
            .map_err(|error| decode_error(&error))?;
        memory.resize(catalog.retained_allocation_bytes(), "decoded WAL catalog")?;
        Ok(Self { catalog, memory })
    }

    /// Release what a mutation that only removes rows or frontiers freed.
    fn release_unretained(&mut self) {
        let retained = self.catalog.retained_allocation_bytes();
        if retained < self.memory.bytes() {
            self.memory
                .resize(retained, "retained WAL catalog")
                .expect("releasing a reservation never needs admission");
        }
    }

    pub(super) fn fence_to(&mut self, writer_epoch: u64) -> Result<bool, CatalogRefusal> {
        self.catalog.fence_to(writer_epoch)
    }

    pub(super) fn set_aside_suffix(
        &mut self,
        fencing_epoch: u64,
        from: Option<crate::wal::format::SegmentId>,
        expected: &[crate::wal::cloud_catalog::CatalogEntry],
        floor: u64,
    ) -> Result<bool, CatalogRefusal> {
        let changed = self
            .catalog
            .set_aside_suffix(fencing_epoch, from, expected, floor)?;
        self.release_unretained();
        Ok(changed)
    }

    pub(super) fn retire_prefix(
        &mut self,
        fencing_epoch: u64,
        expected: &[crate::wal::cloud_catalog::CatalogEntry],
        generation: crate::types::SharedGenerationId,
    ) -> Result<Vec<crate::wal::format::SegmentId>, CatalogRefusal> {
        let retired = self
            .catalog
            .retire_prefix(fencing_epoch, expected, generation)?;
        self.release_unretained();
        Ok(retired)
    }

    /// Refusals come first: a fenced or contradicting run is never reported
    /// as memory pressure, which the caller would retry.
    pub(super) fn append_run(
        &mut self,
        epoch: u64,
        run: &[crate::wal::cloud_catalog::CatalogEntry],
    ) -> Result<bool, super::PublicationError> {
        let added = self
            .catalog
            .append_additions(epoch, run)
            .map_err(super::PublicationError::from)?;
        let growth = self.catalog.append_growth_bytes(added);
        self.memory
            .resize(
                self.memory.bytes().saturating_add(growth),
                "catalog append storage",
            )
            .map_err(super::PublicationError::Retriable)?;
        let changed = self.catalog.extend_validated(added);
        // The replaced row buffer is gone; it must not stay charged during
        // the primary and mirror uploads.
        self.release_unretained();
        Ok(changed)
    }
}

/// Parser scratch for one pass, found by one scan that does not allocate.
/// The parser keeps a single scratch buffer, and its growth briefly holds the
/// old and the doubled allocation: under three times what it must hold. That
/// is an escaped string, copied whole (strings without escapes borrow from
/// the input), or one byte per open array or object while the version probe
/// skips a value, which has no depth limit.
fn parser_scratch_bytes(bytes: &[u8]) -> usize {
    let (mut depth, mut deepest, mut in_string, mut after_backslash, mut escapes) =
        (0_usize, 0_usize, false, false, false);
    for &byte in bytes {
        if in_string {
            match byte {
                _ if after_backslash => after_backslash = false,
                b'\\' => (after_backslash, escapes) = (true, true),
                b'"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'[' | b'{' => {
                depth += 1;
                deepest = deepest.max(depth);
            }
            b']' | b'}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    let held = if escapes { bytes.len() } else { deepest };
    held.saturating_mul(3).saturating_add(4096)
}

fn decode_error(error: &CatalogDecodeError) -> MidgeError {
    match error {
        CatalogDecodeError::Incompatible(_) => MidgeError::CompatibilityError(error.to_string()),
        CatalogDecodeError::Invalid(_) => MidgeError::Corruption(error.to_string()),
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

/// This writer's receipt-confirmed authority, decoded without reading either
/// copy, when the cache holds it. The caller holds the catalog mutation lock;
/// a successor's fence still fails its later compare-and-swap.
pub(super) fn cached_wal_catalog(storage: &HybridStorage) -> Option<MidgeResult<AdmittedCatalog>> {
    let (primary, mirror) = storage.take_wal_catalog_cache()?;
    let catalog = AdmittedCatalog::decode(primary.bytes(), &catalog_budget(storage));
    storage.restore_wal_catalog_cache(primary, mirror);
    Some(catalog)
}

pub(super) fn load_and_repair_catalog_within(
    storage: &HybridStorage,
    deadline: &crate::common::OperationDeadline,
) -> MidgeResult<Option<CatalogAuthority>> {
    load_and_repair_catalog_with_authority(storage, deadline, &|| storage.check_write_authority())
}

/// Whether a copy's decode error rules out repairing it from the other:
/// memory pressure, or another catalog format (#844).
fn unrepairable(error: &MidgeError) -> bool {
    matches!(
        error,
        MidgeError::ResourceLimit(_) | MidgeError::CompatibilityError(_)
    )
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
            Err(error) if unrepairable(&error) => return Err(error),
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
                    if unrepairable(&mirror_error) { return mirror_error; }
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
        if unrepairable(&error) {
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
    mutate: impl FnOnce(&mut AdmittedCatalog) -> Result<Mutation<T>, R>,
) -> MidgeResult<Result<(T, AdmittedCatalog), R>> {
    validate()?;
    let _catalog_mutation = storage.lock_wal_catalog_mutation_within(deadline, label)?;
    let cached = storage.take_wal_catalog_cache();
    let (mut catalog, expected, mirror) = if let Some((primary, mirror)) = cached {
        let catalog = match AdmittedCatalog::decode(primary.bytes(), &catalog_budget(storage)) {
            Ok(catalog) => catalog,
            Err(error) => {
                // Decode has not attempted any write. Keep confirmed authority
                // available when temporary admission pressure clears.
                storage.restore_wal_catalog_cache(primary, mirror);
                return Err(error);
            }
        };
        (catalog, Some(primary), Some(mirror))
    } else {
        match load_and_repair_catalog_with_authority(storage, deadline, validate)? {
            Some(authority) => (authority.catalog, Some(authority.primary), None),
            None => match missing {
                Missing::Create(epoch) => (AdmittedCatalog::empty(storage, epoch)?, None, None),
                Missing::Refuse => {
                    return Err(MidgeError::Internal(
                        "cloud WAL publication catalog is missing".to_string(),
                    ))
                }
            },
        }
    };
    let value = match mutate(&mut catalog) {
        Err(refusal) => {
            // A refused rule leaves the catalog unchanged, so the confirmed
            // authority stays this writer's latest; the retry need not reload.
            if let (Some(primary), Some(mirror)) = (expected, mirror) {
                storage.restore_wal_catalog_cache(primary, mirror);
            }
            return Ok(Err(refusal));
        }
        Ok(Mutation::Unchanged(value)) if expected.is_some() && mirror.is_none() => {
            return Ok(Ok((value, catalog)))
        }
        Ok(Mutation::Unchanged(value) | Mutation::Changed(value)) => value,
    };
    // Even an unchanged cached result crosses the CAS boundary: a successor
    // may have fenced this epoch since the cached response was received.
    match commit_catalog_with_authority(storage, expected, mirror, &catalog, deadline, validate) {
        Ok((primary, mirror)) => {
            storage.cache_wal_catalog(primary, mirror);
            Ok(Ok((value, catalog)))
        }
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
    expected_primary: Option<ControlObject>,
    expected_mirror: Option<StorageObjectMetadata>,
    catalog: &WalPublicationCatalog,
    deadline: &crate::common::OperationDeadline,
    validate: &dyn Fn() -> MidgeResult<()>,
) -> MidgeResult<(ControlObject, ControlObject)> {
    let budget = catalog_budget(storage);
    // CAS needs the pinned identity, not the obsolete encoded body.
    let expected_primary = expected_primary.map(ControlObject::into_metadata);
    let encoded = AdmittedEncoding::new(catalog, &budget)?;
    validate()?;
    let primary = storage
        .write_control_object_with_authority(
            crate::wal::cloud_catalog::OBJECT_KEY,
            expected_primary.as_ref(),
            &encoded.bytes,
            &budget,
            deadline,
            validate,
        )
        .map_err(crate::storage::hybrid::backend::ControlWriteFailure::into_error)?;
    // The confirmed primary holds exactly the encoded bytes. Upload the mirror
    // from it so the encoding is not held through the mirror workspace.
    drop(encoded);
    crate::failpoints::fail_point!("midge::cloud::after_catalog_primary_before_mirror");
    let mirror = if let Some(expected) = expected_mirror {
        storage
            .write_control_object_with_authority(
                crate::wal::cloud_catalog::MIRROR_OBJECT_KEY,
                Some(&expected),
                primary.bytes(),
                &budget,
                deadline,
                validate,
            )
            .map_err(crate::storage::hybrid::backend::ControlWriteFailure::into_error)?
    } else {
        sync_catalog_copy_with_authority(
            storage,
            crate::wal::cloud_catalog::MIRROR_OBJECT_KEY,
            primary.bytes(),
            deadline,
            validate,
        )?
    };
    Ok((primary, mirror))
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
    let expected = existing.map(ControlObject::into_metadata);
    storage
        .write_control_object_with_authority(
            key,
            expected.as_ref(),
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
