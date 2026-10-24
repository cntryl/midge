//! Rediscover the catalog's permanently closed WAL id range after queue loss.

use super::catalog::{load_and_repair_catalog_within, CatalogAuthority};
use super::{InventoryCursor, OrphanSweepContext, OrphanSweepProgress};
use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use crate::lease::CloudMetadataHead;
use crate::storage::cloud::{CloudListPageRequest, MAX_LIST_PAGE_KEYS};
use crate::wal::format::{ChainLink, SegmentHeader, HEADER_LEN};
use std::sync::atomic::Ordering;

#[cfg(test)]
mod tests;

pub(crate) fn sweep_retired_wal_within(
    context: OrphanSweepContext<'_>,
    mut cursor: InventoryCursor,
    max_probes: usize,
    mut validate_lease: impl FnMut(&OperationDeadline) -> MidgeResult<()>,
) -> MidgeResult<OrphanSweepProgress> {
    let mut progress = OrphanSweepProgress::default();
    if max_probes == 0 {
        progress.cursor = cursor;
        return Ok(progress);
    }
    let _mutation = context
        .storage
        .lock_wal_catalog_mutation_within(context.deadline, "retired WAL inventory")?;
    validate_lease(context.deadline)?;
    let authority = load_and_repair_catalog_within(context.storage, context.deadline)?
        .ok_or_else(|| MidgeError::Corruption("retired WAL inventory lacks its catalog".into()))?;
    if authority.catalog.fencing_epoch() != context.authority.writer_epoch {
        return Err(MidgeError::Fenced(
            "retired WAL inventory catalog has another epoch".into(),
        ));
    }
    if authority.catalog.anchor() == ChainLink::GENESIS {
        progress.cursor = cursor;
        return Ok(progress);
    }
    validate_shared_generation(&context, &authority)?;
    if cursor.pending.is_empty() {
        progress.pages = 1;
        context.storage.counters().record(|m| {
            m.cloud_orphan_list_attempts.fetch_add(1, Ordering::Relaxed);
        });
        let Some(page) = context.storage.list_remote_page_within(
            "wal/",
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
    for _ in 0..max_probes.min(8) {
        let Some(key) = cursor.pending.pop_front() else {
            break;
        };
        reclaim_candidate(
            &context,
            &authority,
            &key,
            &mut progress,
            &mut validate_lease,
        )?;
    }
    progress.cursor = cursor;
    Ok(progress)
}

fn reclaim_candidate(
    context: &OrphanSweepContext<'_>,
    authority: &CatalogAuthority,
    key: &str,
    progress: &mut OrphanSweepProgress,
    validate_lease: &mut impl FnMut(&OperationDeadline) -> MidgeResult<()>,
) -> MidgeResult<()> {
    let Some((epoch, id)) = crate::wal::parse_segment_object_key(key) else {
        retain(context, progress);
        return Ok(());
    };
    if epoch == 0
        || epoch > authority.catalog.fencing_epoch()
        || id == 0
        || id > authority.catalog.anchor().id
    {
        retain(context, progress);
        return Ok(());
    }
    progress.probes += 1;
    context.storage.counters().record(|m| {
        m.cloud_orphan_probes.fetch_add(1, Ordering::Relaxed);
    });
    let metadata = match context
        .storage
        .remote_range_metadata_optional_within(key, context.deadline)
    {
        Ok(Some(metadata)) if metadata.size >= HEADER_LEN as u64 => metadata,
        _ => {
            retain(context, progress);
            return Ok(());
        }
    };
    let valid = context
        .storage
        .read_remote_range_within(key, &metadata, 0..HEADER_LEN as u64, context.deadline)
        .and_then(|bytes| SegmentHeader::decode(&bytes))
        .is_ok_and(|header| {
            header_matches_closed_range(&header, epoch, id, authority.catalog.anchor())
        });
    if !valid {
        retain(context, progress);
        return Ok(());
    }
    validate_lease(context.deadline)?;
    validate_catalog_unchanged(context, authority)?;
    context.storage.counters().record(|m| {
        m.cloud_orphan_delete_attempts
            .fetch_add(1, Ordering::Relaxed);
    });
    match context
        .storage
        .delete_remote_object_by_identity_blocking_within(key, &metadata, context.deadline)
    {
        Ok(()) => progress.deleted += 1,
        Err(MidgeError::Fenced(detail)) => return Err(MidgeError::Fenced(detail)),
        Err(_) => retain(context, progress),
    }
    Ok(())
}

fn header_matches_closed_range(
    header: &SegmentHeader,
    epoch: u64,
    id: u64,
    anchor: ChainLink,
) -> bool {
    header.epoch == epoch
        && header.id == id
        && header.first_seq <= anchor.last_seq
        && (header.prev.id != 0 || header.prev == ChainLink::GENESIS)
        && crate::wal::format::follows(header.prev, None, &header.open_facts()).is_ok()
}

fn validate_shared_generation(
    context: &OrphanSweepContext<'_>,
    authority: &CatalogAuthority,
) -> MidgeResult<()> {
    context
        .authority
        .store
        .validate_epoch_with_timeout(
            context.authority.holder_id,
            context.authority.writer_epoch,
            context.deadline.remaining(),
        )
        .map_err(|error| error.into_validation_error("retired WAL sweep lease"))?;
    match context
        .authority
        .store
        .read_committed_metadata(context.deadline.remaining())
        .map_err(|error| error.into_validation_error("retired WAL sweep shared generation"))?
    {
        CloudMetadataHead::Committed(generation)
            if generation.id >= authority.catalog.retired_by() =>
        {
            Ok(())
        }
        _ => Err(MidgeError::Corruption(
            "retired WAL anchor exceeds selected shared generation".into(),
        )),
    }
}

fn validate_catalog_unchanged(
    context: &OrphanSweepContext<'_>,
    expected: &CatalogAuthority,
) -> MidgeResult<()> {
    validate_shared_generation(context, expected)?;
    let current =
        load_and_repair_catalog_within(context.storage, context.deadline)?.ok_or_else(|| {
            MidgeError::Corruption("retired WAL catalog disappeared before delete".into())
        })?;
    if current.catalog.fencing_epoch() != context.authority.writer_epoch
        || current.catalog.anchor() != expected.catalog.anchor()
        || current.catalog.retired_by() != expected.catalog.retired_by()
        || current.primary.bytes() != expected.primary.bytes()
    {
        return Err(MidgeError::Fenced(
            "retired WAL catalog changed before delete".into(),
        ));
    }
    Ok(())
}

fn retain(context: &OrphanSweepContext<'_>, progress: &mut OrphanSweepProgress) {
    progress.retained += 1;
    context.storage.counters().record(|m| {
        m.cloud_orphan_retained.fetch_add(1, Ordering::Relaxed);
    });
}
