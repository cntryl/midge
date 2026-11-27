//! Reclaim only manifest versions that can no longer become selected history.

use super::format10_admission::{AdmissionPhase, Format10AdmissionStore};
use super::metadata_snapshot::CloudMetadataMirrorContext;
use cntryl_midge_core::common::{MidgeError, MidgeResult};
use cntryl_midge_durability::lease::{CloudMetadataGeneration, CloudMetadataHead};
use cntryl_midge_storage::cloud::BlockingCloud;
use cntryl_midge_storage::{
    StorageBackend, StorageEvent, StorageObjectMetadata, StorageOutcome, StoragePrecondition,
    StorageRequest,
};
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

const RETAIN_ORDINALS: u64 = 3;
pub(crate) const INTERVAL: Duration = Duration::from_secs(30);
const LIST_BUDGET: usize = 4;
const CANDIDATE_BUDGET: usize = 8;

/// Idle maintenance observes already selected history. It must not publish a
/// new cut merely to reclaim old ones, or write-idle history never converges.
pub(crate) fn collect_selected_metadata_within(
    context: &CloudMetadataMirrorContext<'_>,
    mut validate_lease: impl FnMut(&cntryl_midge_core::common::OperationDeadline) -> MidgeResult<()>,
) -> MidgeResult<()> {
    let mut guard = context
        .publication_lock
        .lock_for(context.deadline.remaining())
        .ok_or_else(|| MidgeError::Timeout("metadata retention waits for local capture".into()))?;
    if context.publication_lock.pending_captures() != 0 {
        return Ok(());
    }
    validate_lease(context.deadline)?;
    context
        .authority
        .store
        .validate_metadata_epoch_with_timeout(
            context.authority.holder_id,
            context.authority.writer_epoch,
            context.deadline.remaining(),
        )
        .map_err(|error| error.into_validation_error("metadata retention authority"))?;
    let head = if cntryl_midge_core::failpoints::is_active(
        "midge::cloud::inject_metadata_retention_authority_read_failure",
    ) {
        Err(cntryl_midge_durability::lease::LeaseError::Indeterminate(
            "injected uncertain metadata authority read".into(),
        ))
    } else {
        context
            .authority
            .store
            .read_committed_metadata(context.deadline.remaining())
    };
    let head = match head {
        Ok(head) => head,
        Err(error) => {
            cntryl_midge_core::failpoints::fail_point!(
                "midge::cloud::metadata_retention_authority_read_failed"
            );
            return Err(error.into());
        }
    };
    if let CloudMetadataHead::Committed(generation) = head {
        collect_after_commit(context, &generation, &mut guard.retention);
    }
    Ok(())
}

#[cfg(test)]
mod tests;

pub(crate) struct MetadataRetentionState {
    last_run: Option<Instant>,
    cursor: u64,
    continuation: Option<String>,
    format_10_last_run: Option<Instant>,
    pub(crate) totals: RetentionCounters,
    pub(crate) interval: Duration,
}

impl Default for MetadataRetentionState {
    fn default() -> Self {
        Self {
            last_run: None,
            cursor: 0,
            continuation: None,
            format_10_last_run: None,
            totals: RetentionCounters::default(),
            interval: INTERVAL,
        }
    }
}

#[derive(Default)]
pub(crate) struct RetentionCounters {
    pub(crate) lists: u64,
    pub(crate) probes: u64,
    pub(crate) deleted: u64,
    pub(crate) retained: u64,
}

impl MetadataRetentionState {
    fn begin(&mut self, head: u64) -> Option<u64> {
        if self
            .last_run
            .is_some_and(|last| last.elapsed() < self.interval)
        {
            return None;
        }
        self.last_run = Some(Instant::now());
        let cutoff = head.saturating_sub(RETAIN_ORDINALS);
        if cutoff <= 1 {
            return None;
        }
        if self.cursor == 0 || self.cursor >= cutoff {
            self.cursor = 1;
            self.continuation = None;
        }
        Some(cutoff)
    }

    fn begin_format_10(&mut self) -> bool {
        if self
            .format_10_last_run
            .is_some_and(|last| last.elapsed() < self.interval)
        {
            return false;
        }
        self.format_10_last_run = Some(Instant::now());
        true
    }
}

/// The caller holds the publication lock. Readers hold that same lock from
/// pointer capture through body decoding; a pending writer cannot race this cut.
/// Higher ordinals never select an orphan discovered by LIST.
pub(super) fn collect_after_commit(
    context: &CloudMetadataMirrorContext<'_>,
    generation: &CloudMetadataGeneration,
    state: &mut MetadataRetentionState,
) {
    if context.publication_lock.pending_captures() != 0 {
        return;
    }
    if generation.uses_format_10() {
        if state.begin_format_10() {
            if let Err(error) = reclaim_finished_format10_stages(context, generation) {
                tracing::debug!(%error, "retaining unsettled FORMAT 10 stages");
            }
        }
        return;
    }
    if !super::metadata_chain::is_chain(generation) {
        return;
    }
    let Some(cutoff) = state.begin(generation.id.get()) else {
        return;
    };
    let protected = generation
        .objects
        .iter()
        .map(|object| object.object_key.as_str())
        .collect::<BTreeSet<_>>();
    let io = BlockingCloud::new(context.cloud, context.deadline);
    let mut candidates = 0;
    for _ in 0..LIST_BUDGET {
        if state.cursor >= cutoff || candidates >= CANDIDATE_BUDGET {
            break;
        }
        let ordinal = state.cursor;
        state.totals.lists += 1;
        let page = cntryl_midge_storage::cloud::CloudListPageRequest {
            continuation: state.continuation.clone(),
            max_keys: (CANDIDATE_BUDGET - candidates)
                .min(cntryl_midge_storage::cloud::MAX_LIST_PAGE_KEYS),
        };
        let Ok(Some(page)) = io.list_page(&format!("manifest/{ordinal:020}/"), page) else {
            state.totals.retained += 1;
            break;
        };
        for key in page.keys {
            let key = context.cloud.strip_namespace(&key);
            if protected.contains(key)
                || cntryl_midge_durability::metadata::cloud_manifest::validate_key(key, ordinal)
                    .is_err()
            {
                continue;
            }
            candidates += 1;
            state.totals.probes += 1;
            match reclaim_candidate(context, generation, key, ordinal) {
                Ok(true) => state.totals.deleted += 1,
                Ok(false) => state.totals.retained += 1,
                Err(error) => {
                    state.totals.retained += 1;
                    tracing::debug!(%error, "retaining uncertain cloud manifest version");
                    return;
                }
            }
        }
        state.continuation = page.continuation;
        if state.continuation.is_none() {
            state.cursor += 1;
        }
    }
    tracing::debug!(
        lists = state.totals.lists,
        probes = state.totals.probes,
        deleted = state.totals.deleted,
        retained = state.totals.retained,
        "cloud manifest retention counters"
    );
}

/// Reclaim only terminal reservation members. The caller holds the local
/// publication lock, and runtime staging serializes FORMAT 10 through CAS.
/// This keeps captured provider reads and pointer selection clear of cleanup.
pub(super) fn reclaim_finished_format10_stages(
    context: &CloudMetadataMirrorContext<'_>,
    generation: &CloudMetadataGeneration,
) -> MidgeResult<()> {
    let admission = Format10AdmissionStore::new(context.cloud, context.deadline);
    let protected = generation
        .objects
        .iter()
        .map(|object| object.object_key.as_str())
        .collect::<BTreeSet<_>>();
    let io = BlockingCloud::new(context.cloud, context.deadline);
    for mut record in admission.list_slots()?.into_iter().flatten() {
        if record.writer_epoch > context.authority.writer_epoch
            || record
                .objects
                .iter()
                .any(|object| protected.contains(object.object_key.as_str()))
        {
            continue;
        }
        if record.phase == AdmissionPhase::Uploaded
            && record.writer_epoch < context.authority.writer_epoch
        {
            // The successor has claimed the pointer. This stage's bodies
            // settled, and any delayed predecessor CAS can no longer select
            // them. Startup resolves pending migration before runtime GC.
            validate_current(context, generation)?;
            record = admission.finish(record.publication_id)?;
        }
        if record.phase != AdmissionPhase::Finished {
            continue;
        }
        validate_current(context, generation)?;
        let mut removed = BTreeSet::new();
        for object in &record.objects {
            if let Some(identity) = io.range_identity(&object.object_key)? {
                remove_format_10_member(context, generation, object, identity)?;
            }
            removed.insert(object.object_key.clone());
        }
        validate_current(context, generation)?;
        admission.delete_finished(&record, &removed)?;
    }
    Ok(())
}

fn remove_format_10_member(
    context: &CloudMetadataMirrorContext<'_>,
    generation: &CloudMetadataGeneration,
    object: &cntryl_midge_durability::lease::CloudMetadataObject,
    identity: StorageObjectMetadata,
) -> MidgeResult<()> {
    let key = &object.object_key;
    if identity.size != object.len
        || crc32c::crc32c(&read_identity(context, key, &identity)?) != object.crc32c
    {
        return Err(MidgeError::Corruption(
            "reserved FORMAT 10 object changed before cleanup".into(),
        ));
    }
    validate_current(context, generation)?;
    cntryl_midge_core::failpoints::fail_point!("midge::cloud::before_metadata_retention_delete");
    if !delete_identity(context, key, identity)? {
        return Err(MidgeError::OutcomeUnknown(
            "reserved FORMAT 10 body deletion remains unproven".into(),
        ));
    }
    Ok(())
}

fn reclaim_candidate(
    context: &CloudMetadataMirrorContext<'_>,
    generation: &CloudMetadataGeneration,
    key: &str,
    ordinal: u64,
) -> MidgeResult<bool> {
    let Ok(metadata) = range_identity(context, key) else {
        return Ok(false);
    };
    if metadata.size == 0
        || metadata.size > cntryl_midge_durability::metadata::cloud_manifest::MAX_CHAIN_BYTES
    {
        return Ok(false);
    }
    let Ok(bytes) = read_identity(context, key, &metadata) else {
        return Ok(false);
    };
    if !super::metadata_chain::reclaimable_version(
        &bytes,
        key,
        ordinal,
        context.authority.writer_epoch,
    )
    .unwrap_or(false)
    {
        return Ok(false);
    }
    validate_current(context, generation)?;
    cntryl_midge_core::failpoints::fail_point!("midge::cloud::before_metadata_retention_delete");
    delete_identity(context, key, metadata)
}

fn range_identity(
    context: &CloudMetadataMirrorContext<'_>,
    key: &str,
) -> MidgeResult<StorageObjectMetadata> {
    let request = request(context, key)?;
    let timeout = request.remaining_timeout();
    let (tx, rx) = std::sync::mpsc::channel();
    context.cloud.submit_range_head_request(request, tx);
    match rx.recv_timeout(timeout) {
        Ok(StorageEvent::HeadComplete {
            key: actual,
            result: StorageOutcome::Ok(metadata),
        }) if actual == key => Ok(metadata),
        result => Err(MidgeError::Io(std::io::Error::other(format!(
            "metadata retention range identity: {result:?}"
        )))),
    }
}

fn request(context: &CloudMetadataMirrorContext<'_>, key: &str) -> MidgeResult<StorageRequest> {
    let timeout = context
        .deadline
        .clamp_nonzero(context.cloud.callback_timeout())
        .ok_or_else(|| MidgeError::Timeout("metadata retention budget exhausted".into()))?;
    Ok(StorageRequest::new(key, *context.deadline, timeout))
}

fn read_identity(
    context: &CloudMetadataMirrorContext<'_>,
    key: &str,
    metadata: &StorageObjectMetadata,
) -> MidgeResult<Vec<u8>> {
    let request =
        request(context, key)?.with_precondition(StoragePrecondition::IfMatch(metadata.clone()));
    let timeout = request.remaining_timeout();
    let (tx, rx) = std::sync::mpsc::channel();
    context
        .cloud
        .submit_range_read_request(request, 0..metadata.size, tx);
    let bytes = rx
        .recv_timeout(timeout)
        .map_err(|error| {
            MidgeError::Io(std::io::Error::other(format!(
                "metadata retention read: {error}"
            )))
        })?
        .map_err(|error| {
            MidgeError::Io(std::io::Error::other(format!(
                "metadata retention read: {error}"
            )))
        })?;
    if bytes.len() as u64 != metadata.size {
        return Err(MidgeError::Corruption(
            "metadata retention read length differs from pinned identity".into(),
        ));
    }
    Ok(bytes)
}

fn validate_current(
    context: &CloudMetadataMirrorContext<'_>,
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
        .map_err(|error| error.into_validation_error("metadata retention lease"))?;
    let current = context
        .authority
        .store
        .read_committed_metadata(context.deadline.remaining())
        .map_err(|error| error.into_validation_error("metadata retention pointer"))?;
    if current != CloudMetadataHead::Committed(generation.clone()) {
        return Err(MidgeError::Fenced(
            "metadata authority changed before retention delete".into(),
        ));
    }
    Ok(())
}

fn delete_identity(
    context: &CloudMetadataMirrorContext<'_>,
    key: &str,
    metadata: StorageObjectMetadata,
) -> MidgeResult<bool> {
    let request = request(context, key)?.with_precondition(StoragePrecondition::IfMatch(metadata));
    let timeout = request.remaining_timeout();
    let (tx, rx) = std::sync::mpsc::channel();
    context.cloud.submit_delete_request(request, tx);
    match rx.recv_timeout(timeout) {
        Ok(StorageEvent::DeleteComplete {
            key: actual,
            result: StorageOutcome::Ok(()),
        }) if actual == key => Ok(true),
        // A failed condition, unknown effect or malformed completion retains
        // the obligation. The next ordinal scan rediscovers the exact key.
        _ => Ok(false),
    }
}
