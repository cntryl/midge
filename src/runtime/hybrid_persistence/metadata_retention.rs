//! Reclaim only manifest versions that can no longer become selected history.

use super::metadata_snapshot::CloudMetadataMirrorContext;
use crate::common::{MidgeError, MidgeResult};
use crate::lease::{CloudMetadataGeneration, CloudMetadataHead};
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

#[cfg(test)]
mod tests;

pub(crate) struct MetadataRetentionState {
    last_run: Option<Instant>,
    cursor: u64,
    pub(crate) totals: RetentionCounters,
    pub(crate) interval: Duration,
}

impl Default for MetadataRetentionState {
    fn default() -> Self {
        Self {
            last_run: None,
            cursor: 0,
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
        }
        Some(cutoff)
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
        state.cursor += 1;
        state.totals.lists += 1;
        let Ok(keys) = io.list(&format!("manifest/{ordinal:020}/")) else {
            state.totals.retained += 1;
            break;
        };
        for key in keys {
            if candidates >= CANDIDATE_BUDGET {
                break;
            }
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
    }
    tracing::debug!(
        lists = state.totals.lists,
        probes = state.totals.probes,
        deleted = state.totals.deleted,
        retained = state.totals.retained,
        "cloud manifest retention counters"
    );
}

fn reclaim_candidate(
    context: &CloudMetadataMirrorContext<'_>,
    generation: &CloudMetadataGeneration,
    key: &str,
    ordinal: u64,
) -> MidgeResult<bool> {
    let io = BlockingCloud::new(context.cloud, context.deadline);
    let Some(metadata) = io.head_optional(key)? else {
        return Ok(false);
    };
    if metadata.size == 0 || metadata.size > crate::metadata::cloud_manifest::MAX_CHAIN_BYTES {
        return Ok(false);
    }
    let bytes = read_identity(context, key, &metadata)?;
    if !super::metadata_chain::reclaimable_version(
        &bytes,
        key,
        ordinal,
        context.authority.writer_epoch,
    )? {
        return Ok(false);
    }
    validate_current(context, generation)?;
    delete_identity(context, key, metadata)
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
