//! Discover only keys that this writer can never admit into selected history.

use super::{decode_generation_manifest, read_committed_metadata_bodies_within};
use crate::common::resource_budget::{ResourceBudget, ResourceReservation};
use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use crate::lease::{CloudMetadataGeneration, CloudMetadataHead};
use crate::runtime::hybrid_persistence::CloudMetadataMirrorAuthority;
use crate::storage::cloud::{CloudListPageRequest, CloudStorage, MAX_LIST_PAGE_KEYS};
use crate::storage::HybridStorage;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::Ordering;

const MAX_PROBES: usize = 8;

#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub(crate) struct InventoryCursor {
    pub(super) continuation: Option<String>,
    pub(super) pending: VecDeque<String>,
}

#[derive(Default)]
pub(crate) struct OrphanSweepProgress {
    pub(crate) cursor: InventoryCursor,
    pub(crate) pages: u64,
    pub(crate) probes: u64,
    pub(crate) deleted: u64,
    pub(crate) retained: u64,
    pub(crate) unavailable: u64,
    pub(crate) wal_cursor: Option<InventoryCursor>,
}

/// Constructed by the loop from its immutable startup floor and a pin-safe
/// source cut while no competing publication owns the manifest gate.
pub(crate) struct SstOrphanProtection {
    epoch: u64,
    floors: HashMap<u32, u64>,
    names: HashSet<String>,
    generations: HashSet<(u32, u64)>,
    _memory: ResourceReservation,
}

impl SstOrphanProtection {
    pub(crate) fn new(
        epoch: u64,
        floors: HashMap<u32, u64>,
        names: HashSet<String>,
        generations: HashSet<(u32, u64)>,
        memory: ResourceReservation,
    ) -> Self {
        Self {
            epoch,
            floors,
            names,
            generations,
            _memory: memory,
        }
    }

    fn unselectable(&self, name: &str) -> bool {
        let parsed = crate::cloud_layout::parse_file_name(name).or_else(|| {
            crate::cloud_layout::parse_compaction_file_name(name)
                .map(|(cf, level, seq, _)| (cf, level, seq))
        });
        let Some((cf, _, generation)) = parsed else {
            return false;
        };
        self.epoch != 0
            && self
                .floors
                .get(&cf)
                .is_some_and(|floor| generation < *floor)
            && !self.names.contains(name)
            && !self.generations.contains(&(cf, generation))
    }
}

#[derive(Clone, Copy)]
pub(crate) struct OrphanSweepContext<'a> {
    pub(crate) storage: &'a HybridStorage,
    pub(crate) metadata: &'a CloudStorage,
    pub(crate) authority: CloudMetadataMirrorAuthority<'a>,
    pub(crate) publication_lock: &'a crate::runtime::MetadataPublicationLock,
    pub(crate) deadline: &'a OperationDeadline,
}

pub(crate) fn sweep_sst_orphans_within(
    context: OrphanSweepContext<'_>,
    protection: &SstOrphanProtection,
    mut cursor: InventoryCursor,
    mut validate_lease: impl FnMut(&OperationDeadline) -> MidgeResult<()>,
) -> MidgeResult<OrphanSweepProgress> {
    if protection.epoch != context.authority.writer_epoch {
        return Err(MidgeError::Fenced(
            "orphan sweep capture has another writer epoch".into(),
        ));
    }
    let _capture = context
        .publication_lock
        .lock_for(
            context
                .deadline
                .remaining()
                .min(context.metadata.callback_timeout()),
        )
        .ok_or_else(|| MidgeError::Timeout("orphan sweep waits for metadata capture".into()))?;
    validate_lease(context.deadline)?;
    let (generation, selected, _memory) = selected_names(&context)?;
    let mut progress = OrphanSweepProgress::default();
    if cursor.pending.is_empty() {
        progress.pages = 1;
        context.storage.counters().record(|m| {
            m.cloud_orphan_list_attempts.fetch_add(1, Ordering::Relaxed);
        });
        let Some(page) = context.storage.list_remote_page_within(
            "sst/",
            CloudListPageRequest {
                continuation: cursor.continuation.clone(),
                max_keys: MAX_LIST_PAGE_KEYS,
            },
            context.deadline,
        )?
        else {
            progress.cursor = cursor;
            progress.unavailable = 1;
            context.storage.counters().record(|m| {
                m.cloud_orphan_inventory_unavailable
                    .fetch_add(1, Ordering::Relaxed);
            });
            return Ok(progress);
        };
        cursor.pending = page.keys.into();
        cursor.continuation = page.continuation;
    }
    for _ in 0..MAX_PROBES {
        let Some(key) = cursor.pending.pop_front() else {
            break;
        };
        let Some(name) = key.strip_prefix("sst/") else {
            record_retained(&context, &mut progress);
            continue;
        };
        if selected.contains(name) || !protection.unselectable(name) {
            record_retained(&context, &mut progress);
            continue;
        }
        progress.probes += 1;
        context.storage.counters().record(|m| {
            m.cloud_orphan_probes.fetch_add(1, Ordering::Relaxed);
        });
        let metadata = match context
            .storage
            .remote_range_metadata_optional_within(&key, context.deadline)
        {
            Ok(Some(metadata)) => metadata,
            Ok(None) => continue,
            Err(_) => {
                record_retained(&context, &mut progress);
                continue;
            }
        };
        if let Err(error) =
            validate_lease(context.deadline).and_then(|()| validate_selected(&context, &generation))
        {
            record_retained(&context, &mut progress);
            return Err(error);
        }
        context.storage.counters().record(|m| {
            m.cloud_orphan_delete_attempts
                .fetch_add(1, Ordering::Relaxed);
        });
        match context
            .storage
            .delete_remote_object_by_identity_blocking_within(&key, &metadata, context.deadline)
        {
            Ok(()) => progress.deleted += 1,
            Err(MidgeError::Fenced(detail)) => return Err(MidgeError::Fenced(detail)),
            Err(_) => record_retained(&context, &mut progress),
        }
    }
    progress.cursor = cursor;
    Ok(progress)
}

fn record_retained(context: &OrphanSweepContext<'_>, progress: &mut OrphanSweepProgress) {
    progress.retained += 1;
    context.storage.counters().record(|m| {
        m.cloud_orphan_retained.fetch_add(1, Ordering::Relaxed);
    });
}

fn selected_names(
    context: &OrphanSweepContext<'_>,
) -> MidgeResult<(
    CloudMetadataGeneration,
    HashSet<String>,
    ResourceReservation,
)> {
    context
        .authority
        .store
        .validate_epoch_with_timeout(
            context.authority.holder_id,
            context.authority.writer_epoch,
            context.deadline.remaining(),
        )
        .map_err(|error| error.into_validation_error("orphan sweep authority"))?;
    let CloudMetadataHead::Committed(generation) = context
        .authority
        .store
        .read_committed_metadata(context.deadline.remaining())
        .map_err(|error| error.into_validation_error("orphan sweep selected pointer"))?
    else {
        return Err(MidgeError::Fenced(
            "orphan sweep has no selected metadata".into(),
        ));
    };
    let bytes = generation
        .objects
        .iter()
        .try_fold(4096usize, |bytes, object| {
            usize::try_from(object.len)
                .ok()
                .and_then(|size| size.checked_mul(16))
                .and_then(|size| bytes.checked_add(size))
        })
        .ok_or_else(|| MidgeError::ResourceLimit("orphan reference admission overflow".into()))?;
    let budget: ResourceBudget = context.storage.maintenance_memory().ok_or_else(|| {
        MidgeError::ResourceLimit("orphan sweep has no maintenance memory".into())
    })?;
    let memory = budget.reserve(bytes, "orphan sweep selected references")?;
    let bodies =
        read_committed_metadata_bodies_within(context.metadata, &generation, context.deadline)?;
    let manifest = decode_generation_manifest(&generation, &bodies, context.deadline)?;
    let mut names = manifest
        .files
        .iter()
        .map(|file| file.name.clone())
        .collect::<HashSet<_>>();
    names.extend(manifest.retired_ssts.keys().cloned());
    for family in &manifest.column_families {
        names.extend(family.dropped_sst_names.iter().cloned());
    }
    let fs: std::sync::Arc<dyn crate::io::Fs> = std::sync::Arc::new(crate::io::MockFs::new());
    if let Some(intents) = bodies.get(crate::metadata::files::INTENT_LOG) {
        let mut file = fs
            .open(
                &crate::io::FsPath::new(crate::metadata::files::INTENT_LOG),
                crate::io::OpenOptions {
                    mode: crate::io::OpenMode::ReadWrite,
                    create: true,
                    create_new: true,
                    truncate: false,
                },
            )
            .map_err(crate::io::FsError::into_midge)?;
        file.write_at(0, bytes::Bytes::copy_from_slice(intents))
            .map_err(crate::io::FsError::into_midge)?;
        let intents = crate::runtime::IntentPersistence::load_with_fs_and_policy(
            &fs,
            crate::config::RecoveryPolicy::Strict,
        )
        .map_err(MidgeError::Corruption)?;
        for intent in &intents {
            visit_intent_sources(intent, |name| {
                names.insert(name.to_owned());
            });
        }
    }
    validate_selected(context, &generation)?;
    Ok((generation, names, memory))
}

fn validate_selected(
    context: &OrphanSweepContext<'_>,
    generation: &CloudMetadataGeneration,
) -> MidgeResult<()> {
    context
        .authority
        .store
        .validate_epoch_with_timeout(
            context.authority.holder_id,
            context.authority.writer_epoch,
            context.deadline.remaining(),
        )
        .map_err(|error| error.into_validation_error("orphan sweep final lease"))?;
    if context
        .authority
        .store
        .read_committed_metadata(context.deadline.remaining())
        .map_err(|error| error.into_validation_error("orphan sweep final pointer"))?
        != CloudMetadataHead::Committed(generation.clone())
    {
        return Err(MidgeError::Fenced(
            "orphan sweep selected references changed".into(),
        ));
    }
    Ok(())
}

pub(crate) fn visit_intent_sources(
    intent: &crate::runtime::IntentLogEntry,
    mut visit: impl FnMut(&str),
) {
    use crate::runtime::IntentLogEntry;
    match intent {
        IntentLogEntry::CompactionPlanned { input_files, .. } => {
            for name in input_files {
                visit(name);
            }
        }
        IntentLogEntry::FlushPublish { file_meta, .. } | IntentLogEntry::SstAdded { file_meta } => {
            visit(&file_meta.name)
        }
        IntentLogEntry::CompactionPublish { removed, added, .. }
        | IntentLogEntry::CompactionApplied { removed, added } => {
            for name in removed {
                visit(name);
            }
            for file in added {
                visit(&file.name);
            }
        }
        IntentLogEntry::CloudUploadComplete { resource, .. } => {
            visit(resource.strip_prefix("sst/").unwrap_or(resource))
        }
        _ => {}
    }
}
