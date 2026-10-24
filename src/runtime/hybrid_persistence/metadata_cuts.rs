//! Optimistic metadata publication of locally ordered, immutable control cuts.

use super::metadata_snapshot::{
    capture_local_metadata, validate_manifest_successor, MetadataMirrorCache,
};
use super::{
    decode_generation_manifest, read_committed_metadata_bodies_within, CloudMetadataMirrorContext,
};
use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use crate::lease::{CloudMetadataGeneration, CloudMetadataHead, LeaseError, SharedGenerationId};
use crate::runtime::CapturedMetadataCut;
use std::collections::BTreeMap;

#[cfg(test)]
mod tests;

struct SelectedCut {
    generation: Option<CloudMetadataGeneration>,
    cache: Option<MetadataMirrorCache>,
    bodies: Option<BTreeMap<String, Vec<u8>>>,
}

/// Upload and CAS run without the local publication mutex. A rejected CAS is
/// reconciled against exact selected lineage; an older capture never rebases
/// over a newer capture and no uncertain effect grants a successful receipt.
pub(crate) fn mirror_captured_control_metadata_within(
    context: CloudMetadataMirrorContext<'_>,
    cut: &CapturedMetadataCut,
    mut validate_lease: impl FnMut(&OperationDeadline) -> MidgeResult<()>,
) -> MidgeResult<()> {
    if cut.writer_epoch() != context.authority.writer_epoch {
        return Err(MidgeError::Fenced(
            "metadata capture belongs to another writer epoch".into(),
        ));
    }
    let context = CloudMetadataMirrorContext {
        fs: cut.fs().as_ref(),
        ..context
    };
    let local = capture_local_metadata(&context)?;
    cut.proof.validate(&local)?;
    let candidate = super::metadata_snapshot::decode_manifest_bodies(&local, context.deadline)?;
    let mut fresh = false;
    loop {
        validate_publication_authority(&context, &mut validate_lease)?;
        let selected = selected_cut(&context, fresh)?;
        if covered(&selected, cut)? {
            cut.mark_committed();
            collect_if_idle(&context, selected.generation.as_ref());
            return Ok(());
        }
        if let Some(cache) = &selected.cache {
            validate_manifest_successor(&candidate, &cache.manifest)?;
        }
        let generation = stage_cut(&context, &local, &candidate, &selected, cut)?;
        validate_publication_authority(&context, &mut validate_lease)?;
        match context.authority.store.publish_committed_metadata(
            context.authority.holder_id,
            context.authority.writer_epoch,
            selected.generation.as_ref(),
            generation.clone(),
            context.deadline.remaining(),
        ) {
            Ok(()) => {
                // A native validated checksum receipt, or the store's exact
                // pinned readback, proves this cut selected even when a later
                // cut has already advanced the pointer before this returns.
                let mut cache = MetadataMirrorCache::new(
                    generation.clone(),
                    context.authority.writer_epoch,
                    candidate.clone(),
                    &local,
                );
                cache.cut = Some(cut.proof.clone());
                install_cache(&context, cache)?;
                cut.mark_committed();
                collect_if_idle(&context, Some(&generation));
                return Ok(());
            }
            Err(LeaseError::AcquisitionFailed(_)) => {
                fresh = true;
            }
            Err(
                error @ (LeaseError::IoError(_)
                | LeaseError::Indeterminate(_)
                | LeaseError::Timeout(_)),
            ) => {
                // Unknown effects remain owned unless an exact later selected
                // body proves coverage. Do not retry their write blindly.
                return resolve_uncertain(&context, cut, error, &mut validate_lease);
            }
            Err(error) => return Err(error.into_validation_error("captured metadata publication")),
        }
    }
}

fn validate_publication_authority(
    context: &CloudMetadataMirrorContext<'_>,
    validate: &mut impl FnMut(&OperationDeadline) -> MidgeResult<()>,
) -> MidgeResult<()> {
    if context.deadline.is_expired() {
        return Err(MidgeError::Timeout(
            "captured metadata publication deadline exhausted".into(),
        ));
    }
    validate(context.deadline)?;
    context
        .authority
        .store
        .validate_metadata_epoch_with_timeout(
            context.authority.holder_id,
            context.authority.writer_epoch,
            context.deadline.remaining(),
        )
        .map_err(|error| error.into_validation_error("captured metadata publication lease"))
}

fn selected_cut(context: &CloudMetadataMirrorContext<'_>, fresh: bool) -> MidgeResult<SelectedCut> {
    let head = if fresh {
        context
            .authority
            .store
            .validate_epoch_with_timeout(
                context.authority.holder_id,
                context.authority.writer_epoch,
                context.deadline.remaining(),
            )
            .map_err(|error| error.into_validation_error("metadata conflict lease"))?;
        context
            .authority
            .store
            .read_committed_metadata(context.deadline.remaining())
    } else {
        context.authority.store.read_committed_metadata_for_writer(
            context.authority.holder_id,
            context.authority.writer_epoch,
            context.deadline.remaining(),
        )
    }
    .map_err(|error| error.into_validation_error("captured metadata pointer"))?;
    let generation = match head {
        CloudMetadataHead::Committed(generation) => generation,
        CloudMetadataHead::Uncommitted => {
            return Ok(SelectedCut {
                generation: None,
                cache: None,
                bodies: None,
            })
        }
        CloudMetadataHead::MissingLease => {
            return Err(MidgeError::Fenced(
                "metadata publication lost its lease".into(),
            ))
        }
    };
    let cached = context
        .publication_lock
        .lock_for(context.deadline.remaining())
        .ok_or_else(|| MidgeError::Timeout("metadata cache read waits for local turn".into()))?
        .cached
        .as_ref()
        .filter(|cache| cache.matches(&generation, context.authority.writer_epoch))
        .cloned();
    if let Some(cache) = cached {
        return Ok(SelectedCut {
            generation: Some(generation),
            cache: Some(cache),
            bodies: None,
        });
    }
    let (bodies, cut) = if super::metadata_chain::is_chain(&generation) {
        super::metadata_chain::read_with_cut(context.cloud, &generation, context.deadline)?
    } else {
        (
            read_committed_metadata_bodies_within(context.cloud, &generation, context.deadline)?,
            None,
        )
    };
    let manifest = decode_generation_manifest(&generation, &bodies, context.deadline)?;
    let mut cache = MetadataMirrorCache::new(
        generation.clone(),
        context.authority.writer_epoch,
        manifest,
        &bodies,
    );
    cache.cut = cut;
    install_cache(context, cache.clone())?;
    Ok(SelectedCut {
        generation: Some(generation),
        cache: Some(cache),
        bodies: Some(bodies),
    })
}

fn covered(selected: &SelectedCut, cut: &CapturedMetadataCut) -> MidgeResult<bool> {
    let Some(proof) = selected.cache.as_ref().and_then(|cache| cache.cut.as_ref()) else {
        return Ok(false);
    };
    if proof.covers(&cut.proof) {
        return Ok(true);
    }
    if proof.writer_epoch > cut.writer_epoch() {
        return Err(MidgeError::Fenced(
            "selected metadata cut belongs to a successor epoch".into(),
        ));
    }
    if proof.writer_epoch == cut.writer_epoch()
        && (!proof.same_lineage(&cut.proof) || proof.ordinal >= cut.cut_ordinal())
    {
        return Err(MidgeError::Busy(
            "newer selected metadata cut does not prove this capture covered".into(),
        ));
    }
    Ok(false)
}

fn stage_cut(
    context: &CloudMetadataMirrorContext<'_>,
    local: &BTreeMap<String, Vec<u8>>,
    candidate: &crate::metadata::Manifest,
    selected: &SelectedCut,
    cut: &CapturedMetadataCut,
) -> MidgeResult<CloudMetadataGeneration> {
    let id = selected
        .generation
        .as_ref()
        .map_or(Some(SharedGenerationId::FIRST), |generation| {
            generation.id.checked_next()
        })
        .ok_or_else(|| {
            MidgeError::ResourceLimit("shared metadata generation ordinal exhausted".into())
        })?;
    let objects = super::metadata_chain::publish_with_cut(
        context,
        local,
        selected.generation.as_ref(),
        selected.bodies.as_ref(),
        selected.cache.as_ref().map(|cache| &cache.fingerprints),
        id,
        Some(&cut.proof),
    )?;
    Ok(CloudMetadataGeneration {
        id,
        manifest_horizon: candidate.edit_checkpoint_id,
        objects,
    })
}

fn install_cache(
    context: &CloudMetadataMirrorContext<'_>,
    cache: MetadataMirrorCache,
) -> MidgeResult<()> {
    let mut guard = context
        .publication_lock
        .lock_for(context.deadline.remaining())
        .ok_or_else(|| MidgeError::Timeout("metadata receipt cache waits for local turn".into()))?;
    if guard.cached.as_ref().is_none_or(|current| {
        current.writer_epoch < cache.writer_epoch
            || (current.writer_epoch == cache.writer_epoch
                && current.generation.id <= cache.generation.id)
    }) {
        guard.cached = Some(cache);
    }
    Ok(())
}

fn resolve_uncertain(
    context: &CloudMetadataMirrorContext<'_>,
    cut: &CapturedMetadataCut,
    error: LeaseError,
    validate: &mut impl FnMut(&OperationDeadline) -> MidgeResult<()>,
) -> MidgeResult<()> {
    validate_publication_authority(context, validate)?;
    let selected = selected_cut(context, true)?;
    if covered(&selected, cut)? {
        cut.mark_committed();
        return Ok(());
    }
    Err(error.into_validation_error("captured metadata mutation outcome remains unknown"))
}

fn collect_if_idle(
    context: &CloudMetadataMirrorContext<'_>,
    generation: Option<&CloudMetadataGeneration>,
) {
    let Some(generation) = generation else {
        return;
    };
    let Some(mut guard) = context.publication_lock.lock_for(std::time::Duration::ZERO) else {
        return;
    };
    if context.publication_lock.pending_captures() == 0 {
        super::metadata_retention::collect_after_commit(context, generation, &mut guard.retention);
    }
}
