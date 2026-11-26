//! Optimistic metadata publication of locally ordered, immutable control cuts.

use super::format10_closure::finish_format10_stage;
use super::metadata_snapshot::{
    capture_local_metadata, validate_manifest_successor, MetadataMirrorCache,
};
use super::{decode_generation_manifest, CloudMetadataMirrorContext};
use cntryl_midge_core::common::{MidgeError, MidgeResult, OperationDeadline};
use cntryl_midge_durability::lease::{CloudMetadataGeneration, CloudMetadataHead, LeaseError, SharedGenerationId};
use crate::runtime::CapturedMetadataCut;
use std::collections::BTreeMap;

#[cfg(test)]
pub(super) mod tests;

struct SelectedCut {
    generation: Option<CloudMetadataGeneration>,
    cache: Option<MetadataMirrorCache>,
    bodies: Option<BTreeMap<String, Vec<u8>>>,
    _memory: Option<std::sync::Arc<cntryl_midge_core::common::resource_budget::ResourceReservation>>,
    _decode_memory: Option<cntryl_midge_core::common::resource_budget::ResourceReservation>,
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
    let format_10 = local
        .get(cntryl_midge_durability::metadata::files::FORMAT)
        .is_some_and(|format| format == b"midge-format-version=10\n");
    let _format_10_writer = format_10
        .then(|| {
            context
                .publication_lock
                .lock_format_10_writer(context.deadline.remaining())
        })
        .transpose()?;
    let candidate = if format_10 {
        decode_format10_cut_manifest(&local, context.deadline)?
    } else {
        super::metadata_snapshot::decode_manifest_bodies(&local, context.deadline)?
    };
    let mut fresh = false;
    loop {
        validate_publication_authority(&context, &mut validate_lease)?;
        let selected = selected_with_admission(&context, fresh)?;
        if covered(&selected, cut)? {
            cut.mark_committed();
            collect_if_idle(&context, selected.generation.as_ref());
            return Ok(());
        }
        if let Some(cache) = &selected.cache {
            validate_manifest_successor(&candidate, &cache.manifest)?;
        }
        let (generation, publication_id) =
            stage_cut(&context, &local, &candidate, &selected, cut, format_10)?;
        let mut staged = MetadataMirrorCache::new(
            generation.clone(),
            context.authority.writer_epoch,
            candidate.clone(),
            &local,
        );
        staged.cut = Some(cut.proof.clone());
        staged.terminal_witnesses = cut.terminal_witnesses();
        cut.remember_staged(staged.clone());
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
                finish_format10_stage(context.cloud, context.deadline, publication_id);
                install_cache(&context, staged)?;
                cut.mark_committed();
                collect_if_idle(&context, Some(&generation));
                return Ok(());
            }
            Err(LeaseError::AcquisitionFailed(_)) => {
                finish_format10_stage(context.cloud, context.deadline, publication_id);
                fresh = true;
            }
            Err(
                error @ (LeaseError::IoError(_)
                | LeaseError::Indeterminate(_)
                | LeaseError::Timeout(_)),
            ) => {
                // Unknown effects remain owned unless an exact later selected
                // body proves coverage. Do not retry their write blindly.
                resolve_uncertain(&context, cut, error, &mut validate_lease)?;
                finish_format10_stage(context.cloud, context.deadline, publication_id);
                return Ok(());
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

fn selected_with_admission(
    context: &CloudMetadataMirrorContext<'_>,
    fresh: bool,
) -> MidgeResult<SelectedCut> {
    loop {
        if context.deadline.is_expired() {
            return Err(MidgeError::Timeout(
                "selected metadata admission exhausted its original budget".into(),
            ));
        }
        match selected_cut(context, fresh) {
            Err(error) if context.publication_lock.capture_admission_retryable(&error) => {
                std::thread::sleep(
                    context
                        .deadline
                        .remaining()
                        .min(std::time::Duration::from_millis(1)),
                );
            }
            result => return result,
        }
    }
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
                _memory: None,
                _decode_memory: None,
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
            _memory: None,
            _decode_memory: None,
        });
    }
    if let Some((cache, memory)) = context
        .publication_lock
        .staged_metadata_cache(&generation, context.authority.writer_epoch)
    {
        install_cache(context, cache.clone())?;
        return Ok(SelectedCut {
            generation: Some(generation),
            cache: Some(cache),
            bodies: None,
            _memory: Some(memory),
            _decode_memory: None,
        });
    }
    let encoded = generation
        .objects
        .iter()
        .try_fold(0_u64, |total, object| total.checked_add(object.len))
        .ok_or_else(|| MidgeError::ResourceLimit("selected metadata length overflow".into()))?;
    let memory = context
        .publication_lock
        .reserve_metadata_body_memory(encoded)?;
    load_selected_cut(context, generation, memory)
}

fn load_selected_cut(
    context: &CloudMetadataMirrorContext<'_>,
    generation: CloudMetadataGeneration,
    memory: std::sync::Arc<cntryl_midge_core::common::resource_budget::ResourceReservation>,
) -> MidgeResult<SelectedCut> {
    let (bodies, chain_cut) = if super::metadata_chain::is_chain(&generation) {
        super::metadata_chain::read_with_cut_reserved(
            context.cloud,
            &generation,
            context.deadline,
            Some(std::sync::Arc::clone(&memory)),
        )?
    } else {
        (
            super::metadata_snapshot::read_committed_metadata_bodies_reserved_within(
                context.cloud,
                &generation,
                context.deadline,
                Some(std::sync::Arc::clone(&memory)),
            )?,
            None,
        )
    };
    let cut = if generation.uses_format_10() {
        read_format10_cut_proof(context, &generation, &bodies, &memory)?
    } else {
        chain_cut
    };
    let decode_memory = context.publication_lock.reserve_selected_decode_workspace(
        &memory,
        &bodies,
        context.deadline,
    )?;
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
        _memory: Some(memory),
        _decode_memory: decode_memory,
    })
}

fn read_format10_cut_proof(
    context: &CloudMetadataMirrorContext<'_>,
    generation: &CloudMetadataGeneration,
    bodies: &BTreeMap<String, Vec<u8>>,
    memory: &std::sync::Arc<cntryl_midge_core::common::resource_budget::ResourceReservation>,
) -> MidgeResult<Option<crate::runtime::MetadataCutProof>> {
    let Some(object) = generation
        .objects
        .iter()
        .find(|object| object.file_name == cntryl_midge_durability::metadata::files::CLOUD_CUT_PROOF)
    else {
        return Ok(None);
    };
    if object.len > cntryl_midge_durability::metadata::files::MAX_CLOUD_CUT_PROOF_BYTES as u64 {
        return Err(MidgeError::ResourceLimit(
            "cloud metadata cut proof exceeds its byte bound".into(),
        ));
    }
    let bytes = cntryl_midge_storage::cloud::BlockingCloud::new(context.cloud, context.deadline)
        .with_reservation(std::sync::Arc::clone(memory))
        .get_exact_len(&object.object_key, object.len)?
        .ok_or_else(|| {
            MidgeError::Corruption("committed FORMAT 10 metadata cut proof is missing".into())
        })?;
    if crc32c::crc32c(&bytes) != object.crc32c {
        return Err(MidgeError::Corruption(
            "committed FORMAT 10 metadata cut proof mismatches its checksum".into(),
        ));
    }
    crate::runtime::MetadataCutProof::decode_cloud_body(&bytes, bodies).map(Some)
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
    candidate: &cntryl_midge_durability::metadata::Manifest,
    selected: &SelectedCut,
    cut: &CapturedMetadataCut,
    format_10: bool,
) -> MidgeResult<(CloudMetadataGeneration, Option<uuid::Uuid>)> {
    let id = selected
        .generation
        .as_ref()
        .map_or(Some(SharedGenerationId::FIRST), |generation| {
            generation.id.checked_next()
        })
        .ok_or_else(|| {
            MidgeError::ResourceLimit("shared metadata generation ordinal exhausted".into())
        })?;
    let publication_id = format_10.then(uuid::Uuid::new_v4);
    let objects = if let Some(publication_id) = publication_id {
        if let Some(previous) = &selected.generation {
            let _turn = context
                .publication_lock
                .lock_for(context.deadline.remaining())
                .ok_or_else(|| {
                    MidgeError::Timeout("FORMAT 10 staging waits for retention authority".into())
                })?;
            super::metadata_retention::reclaim_finished_format10_stages(context, previous)?;
        }
        let proof_body = cut.proof.cloud_body()?;
        let (horizon, objects) = super::publish_format10_closure(
            context.cloud,
            context.deadline,
            publication_id,
            context.authority.writer_epoch,
            local,
            Some(&proof_body),
            &cut.memory_reservation(),
        )?;
        if horizon != candidate.edit_checkpoint_id {
            return Err(MidgeError::Corruption(
                "captured FORMAT 10 manifest horizon changed before publication".into(),
            ));
        }
        objects
    } else {
        super::metadata_chain::publish_with_cut(
            context,
            local,
            selected.generation.as_ref(),
            selected.bodies.as_ref(),
            selected.cache.as_ref().map(|cache| &cache.fingerprints),
            id,
            Some(cut),
        )?
    };
    cntryl_midge_core::failpoints::fail_point!("midge::cloud::after_metadata_chain_upload_before_pointer_cas");
    Ok((
        CloudMetadataGeneration {
            id,
            manifest_horizon: candidate.edit_checkpoint_id,
            objects,
        },
        publication_id,
    ))
}

fn decode_format10_cut_manifest(
    bodies: &BTreeMap<String, Vec<u8>>,
    deadline: &OperationDeadline,
) -> MidgeResult<cntryl_midge_durability::metadata::Manifest> {
    use cntryl_midge_durability::metadata::local_authority::{LocalMetadataAuthority, Stream};
    let stream = LocalMetadataAuthority::read_cloud_stream(bodies, Stream::Manifest)?;
    cntryl_midge_durability::metadata::ManifestPersistence::decode_format_10_stream(
        stream.base,
        stream.checkpoint,
        stream.segments,
        Some(&cntryl_midge_core::common::DeadlineScope::new(*deadline)),
    )
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
        if let Some(proof) = &cache.cut {
            context.publication_lock.note_selected_terminal_witnesses(
                cache.generation.id,
                proof,
                cache.terminal_witnesses.clone(),
            )?;
        }
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
    let selected = selected_with_admission(context, true)?;
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
