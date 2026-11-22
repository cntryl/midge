//! Reclaim only manifest versions that can no longer become selected history.

use super::metadata_snapshot::CloudMetadataMirrorContext;
use crate::common::{MidgeError, MidgeResult};
use crate::lease::{CloudMetadataGeneration, CloudMetadataHead};
use crate::runtime::MetadataCutProof;
use crate::storage::cloud::BlockingCloud;
use crate::storage::{
    StorageBackend, StorageEvent, StorageObjectMetadata, StorageOutcome, StoragePrecondition,
    StorageRequest,
};
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

const RETAIN_ORDINALS: u64 = 3;
pub(crate) const INTERVAL: Duration = Duration::from_secs(30);
const LIST_BUDGET: usize = 4;
const CANDIDATE_BUDGET: usize = 8;
const MAX_RETENTION_CONTINUATION_BYTES: usize = 4096;
const MAX_FORMAT_10_OBJECT_KEY_BYTES: usize = 512;

/// Idle maintenance observes already selected history. It must not publish a
/// new cut merely to reclaim old ones, or write-idle history never converges.
pub(crate) fn collect_selected_metadata_within(
    context: &CloudMetadataMirrorContext<'_>,
    mut validate_lease: impl FnMut(&crate::common::OperationDeadline) -> MidgeResult<()>,
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
    let head = if crate::failpoints::is_active(
        "midge::cloud::inject_metadata_retention_authority_read_failure",
    ) {
        Err(crate::lease::LeaseError::Indeterminate(
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
            crate::failpoints::fail_point!(
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
    format_10_continuation: Option<String>,
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
            format_10_continuation: None,
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
        collect_format_10_after_commit(context, generation, state);
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
        let page = crate::storage::cloud::CloudListPageRequest {
            continuation: state.continuation.clone(),
            max_keys: (CANDIDATE_BUDGET - candidates)
                .min(crate::storage::cloud::MAX_LIST_PAGE_KEYS),
        };
        let Ok(Some(page)) = io.list_page(&format!("manifest/{ordinal:020}/"), page) else {
            state.totals.retained += 1;
            break;
        };
        for key in page.keys {
            let key = context.cloud.strip_namespace(&key);
            if protected.contains(key)
                || crate::metadata::cloud_manifest::validate_key(key, ordinal).is_err()
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

fn collect_format_10_after_commit(
    context: &CloudMetadataMirrorContext<'_>,
    generation: &CloudMetadataGeneration,
    state: &mut MetadataRetentionState,
) {
    if generation.objects.len() > crate::metadata::local_authority::MAX_OBJECTS + 3
        || !state.begin_format_10()
    {
        return;
    }
    let protected = generation
        .objects
        .iter()
        .map(|object| object.object_key.as_str())
        .collect::<BTreeSet<_>>();
    let io = BlockingCloud::new(context.cloud, context.deadline);
    let mut candidates = 0;
    for _ in 0..LIST_BUDGET {
        if candidates >= CANDIDATE_BUDGET {
            break;
        }
        let page_request = crate::storage::cloud::CloudListPageRequest {
            continuation: state.format_10_continuation.clone(),
            max_keys: (CANDIDATE_BUDGET - candidates)
                .min(crate::storage::cloud::MAX_LIST_PAGE_KEYS),
        };
        state.totals.lists += 1;
        let Ok(Some(page)) = io.list_page("metadata/generations/", page_request) else {
            state.totals.retained += 1;
            return;
        };
        if page
            .continuation
            .as_ref()
            .is_some_and(|token| token.len() > MAX_RETENTION_CONTINUATION_BYTES)
        {
            state.totals.retained += 1;
            return;
        }
        for key in &page.keys {
            let key = context.cloud.strip_namespace(key);
            let Some(kind) = format_10_candidate_kind(key) else {
                continue;
            };
            candidates += 1;
            if protected.contains(key) {
                state.totals.retained += 1;
                continue;
            }
            state.totals.probes += 1;
            match reclaim_format_10_candidate(context, generation, key, kind) {
                Ok(true) => state.totals.deleted += 1,
                Ok(false) => state.totals.retained += 1,
                Err(error) => {
                    state.totals.retained += 1;
                    tracing::debug!(%error, "retaining uncertain cloud FORMAT 10 body");
                    return;
                }
            }
        }
        state.format_10_continuation = page.continuation;
        if state.format_10_continuation.is_none() {
            break;
        }
    }
}

#[derive(Clone, Copy)]
enum Format10CandidateKind {
    Format,
    Head,
    CutProof,
    Closure,
}

fn format_10_candidate_kind(key: &str) -> Option<Format10CandidateKind> {
    use crate::metadata::local_authority::HEAD_PATH;

    if key.len() > MAX_FORMAT_10_OBJECT_KEY_BYTES {
        return None;
    }
    let (publication_id, relative_path) =
        key.strip_prefix("metadata/generations/")?.split_once('/')?;
    let parsed_id = uuid::Uuid::parse_str(publication_id).ok()?;
    if parsed_id.to_string() != publication_id {
        return None;
    }
    match relative_path {
        crate::metadata::files::FORMAT => Some(Format10CandidateKind::Format),
        HEAD_PATH => Some(Format10CandidateKind::Head),
        MetadataCutProof::CLOUD_OBJECT_NAME => Some(Format10CandidateKind::CutProof),
        path if crate::metadata::local_authority::closure_path_parts(path).is_ok() => {
            Some(Format10CandidateKind::Closure)
        }
        _ => None,
    }
}

fn reclaim_format_10_candidate(
    context: &CloudMetadataMirrorContext<'_>,
    generation: &CloudMetadataGeneration,
    key: &str,
    kind: Format10CandidateKind,
) -> MidgeResult<bool> {
    use crate::metadata::local_authority::{MAX_CLOSURE_BYTES, MAX_HEAD_BYTES};

    let metadata = range_identity(context, key)?;
    let limit = match kind {
        Format10CandidateKind::Format => 64,
        Format10CandidateKind::Head => MAX_HEAD_BYTES as u64,
        Format10CandidateKind::CutProof => MetadataCutProof::MAX_CLOUD_OBJECT_BYTES as u64,
        Format10CandidateKind::Closure => MAX_CLOSURE_BYTES,
    };
    if metadata.size == 0 || metadata.size > limit {
        return Ok(false);
    }
    if matches!(kind, Format10CandidateKind::Format)
        && read_identity(context, key, &metadata)? != b"midge-format-version=10\n"
    {
        // The v9 marker shares the outer immutable-body namespace. Preserve it
        // without changing the legacy retention policy.
        return Ok(false);
    }
    validate_current(context, generation)?;
    crate::failpoints::fail_point!("midge::cloud::before_metadata_retention_delete");
    delete_identity(context, key, metadata)
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
    if metadata.size == 0 || metadata.size > crate::metadata::cloud_manifest::MAX_CHAIN_BYTES {
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
    crate::failpoints::fail_point!("midge::cloud::before_metadata_retention_delete");
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
