//! Admission of concrete typed metadata work before its allocating decode.

use super::{MetadataPublicationLock, SelectedTerminalWitnesses};
use crate::common::resource_budget::ResourceReservation;
use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use crate::metadata::files;
use crate::runtime::IntentPersistence;
use std::collections::BTreeMap;
use std::sync::Arc;

#[cfg(test)]
mod tests;

impl MetadataPublicationLock {
    pub(crate) fn reserve_selected_decode_workspace(
        &self,
        memory: &Arc<ResourceReservation>,
        bodies: &BTreeMap<String, Vec<u8>>,
        deadline: &OperationDeadline,
    ) -> MidgeResult<Option<ResourceReservation>> {
        self.prune_released_cut_receipts();
        let snapshot = bodies.get(files::MANIFEST_SNAPSHOT).ok_or_else(|| {
            MidgeError::Corruption("selected metadata lacks manifest snapshot".into())
        })?;
        let journal = bodies.get(files::JOURNAL).map_or(&[][..], Vec::as_slice);
        let manifest = crate::metadata::codec::manifest_workspace::decoded_workspace_bound(
            snapshot, journal, deadline,
        )?;
        let encoded = bodies.values().try_fold(0_usize, |sum, body| {
            sum.checked_add(body.len()).ok_or_else(overflow)
        })?;
        let required = encoded
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_add(manifest.bytes))
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(overflow)?;
        let limit = memory.limit();
        if required > limit {
            return Err(MidgeError::ResourceLimit(format!(
                "selected metadata typed workspace requires {required} bytes beyond its configured maintenance pool of {limit} bytes"
            )));
        }
        if required <= memory.bytes() {
            return Ok(None);
        }
        memory
            .reserve_related(
                required - memory.bytes(),
                "selected metadata typed decode workspace",
            )
            .map(Some)
    }

    pub(super) fn ensure_capture_workspace(
        &self,
        memory: &mut Arc<ResourceReservation>,
        bodies: &BTreeMap<String, Vec<u8>>,
        deadline: &OperationDeadline,
    ) -> MidgeResult<()> {
        let snapshot = bodies
            .get(files::MANIFEST_SNAPSHOT)
            .ok_or_else(|| MidgeError::Corruption("metadata cut lacks manifest snapshot".into()))?;
        let journal = bodies.get(files::JOURNAL).map_or(&[][..], Vec::as_slice);
        let intents = bodies.get(files::INTENT_LOG).map_or(&[][..], Vec::as_slice);
        let manifest = crate::metadata::codec::manifest_workspace::decoded_workspace_bound(
            snapshot, journal, deadline,
        )?;
        let intents = IntentPersistence::decoded_workspace_bound(intents, deadline)?;
        let encoded = bodies.values().try_fold(0_usize, |sum, body| {
            sum.checked_add(body.len()).ok_or_else(overflow)
        })?;
        let compact =
            SelectedTerminalWitnesses::allocation_bytes(memory, intents.publication_entries)?;
        let typed = required_private_workspace(encoded, &manifest, &intents)?
            .checked_add(compact)
            .ok_or_else(overflow)?;
        let limit = self
            .capture_memory
            .get()
            .expect("capture pool admitted source")
            .limit();
        if typed > limit {
            return Err(MidgeError::ResourceLimit(format!(
                "metadata typed workspace requires {typed} bytes beyond its configured maintenance pool of {limit} bytes (encoded={encoded}, manifest={}, authority={}, intent={}, entries={}, publications={})",
                manifest.bytes, manifest.authority, intents.bytes, intents.entries, intents.publication_entries
            )));
        }
        if typed > memory.bytes() {
            Arc::get_mut(memory)
                .ok_or_else(|| {
                    MidgeError::Internal("metadata preflight workspace was already shared".into())
                })?
                .resize(typed, "typed metadata decode and publication workspace")?;
        }
        Ok(())
    }
}

fn required_private_workspace(
    encoded: usize,
    manifest: &crate::metadata::codec::manifest_workspace::ManifestDecodeWorkspace,
    intents: &crate::runtime::intent_persistence::IntentDecodeWorkspace,
) -> MidgeResult<usize> {
    // Raw control copies and journal payload reads coexist with manifest
    // replay. Their typed enum/collection expansion is measured separately.
    let manifest_peak = encoded
        .checked_mul(4)
        .and_then(|bytes| bytes.checked_add(manifest.bytes))
        .ok_or_else(overflow)?;
    // Decoding and normalization are separate phases. The append Vec has
    // already dropped when normalization owns the retained state and encodes
    // one publication at a time; legacy entries create no publication guards.
    let guards = intents
        .publication_entries
        .checked_mul(384)
        .ok_or_else(overflow)?;
    let intent_decode_peak = encoded
        .checked_mul(3)
        .and_then(|bytes| bytes.checked_add(manifest.authority))
        .and_then(|bytes| bytes.checked_add(intents.bytes))
        .ok_or_else(overflow)?;
    let normalization_peak = encoded
        .checked_mul(3)
        .and_then(|bytes| bytes.checked_add(manifest.authority))
        .and_then(|bytes| bytes.checked_add(intents.retained_state_bytes))
        .and_then(|bytes| bytes.checked_add(intents.max_single_normalization_scratch))
        .and_then(|bytes| bytes.checked_add(guards))
        .ok_or_else(overflow)?;
    // Capture, candidate, staged/cache authority clones and the immutable
    // frame serializer may coexist during physical publication.
    let publication_peak = encoded
        .checked_mul(6)
        .and_then(|bytes| bytes.checked_add(manifest.authority.checked_mul(6)?))
        .and_then(|bytes| bytes.checked_add(guards))
        .ok_or_else(overflow)?;
    manifest_peak
        .max(intent_decode_peak)
        .max(normalization_peak)
        .max(publication_peak)
        .checked_add(4096)
        .ok_or_else(overflow)
}

fn overflow() -> MidgeError {
    MidgeError::ResourceLimit("typed metadata workspace overflow".into())
}
