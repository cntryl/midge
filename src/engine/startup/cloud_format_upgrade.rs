//! Early cloud FORMAT 10 migration reconciliation.
//!
//! This runs after lease acquisition and before any provider DDL or WAL
//! catalog mutation. Pending FORMAT 10 publication is reconciled against the
//! exact selected closure before runtime recovery proceeds.

use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use crate::lease::{
    CloudFormatMigrationGate, CloudFormatMigrationPending, CloudMetadataGeneration,
    CloudMetadataGenerationIdentity, CloudMetadataHead, LeaderStore, SharedGenerationId,
};
use crate::runtime::hybrid_persistence::{
    decode_generation_manifest, metadata_chain, read_committed_metadata_bodies_within,
};
use crate::storage::cloud::{BlockingCloud, CloudEvent, CloudStorage};
use crate::storage::StoragePrecondition;
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::collections::BTreeMap;
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

const FORMAT_10_MARKER: &[u8] = b"midge-format-version=10\n";
const MAX_CANDIDATE_DESCRIPTOR_BYTES: usize = 64 * 1024;
const SOURCE_DIGEST_DOMAIN: &[u8] = b"midge-cloud-format10-source-v1\n";

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CloudFormatGateResolution {
    Format9,
    Format10Selected,
}

/// Strict wire view for the migration-bound candidate descriptor.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CandidateDescriptor {
    id: SharedGenerationId,
    manifest_horizon: u64,
    objects: Vec<crate::lease::CloudMetadataObject>,
}

impl From<&CloudMetadataGeneration> for CandidateDescriptor {
    fn from(generation: &CloudMetadataGeneration) -> Self {
        Self {
            id: generation.id,
            manifest_horizon: generation.manifest_horizon,
            objects: generation.objects.clone(),
        }
    }
}

impl From<CandidateDescriptor> for CloudMetadataGeneration {
    fn from(descriptor: CandidateDescriptor) -> Self {
        Self {
            id: descriptor.id,
            manifest_horizon: descriptor.manifest_horizon,
            objects: descriptor.objects,
        }
    }
}

/// Store the exact candidate descriptor before installing a pending lease gate.
/// The create is immutable; an ambiguous result is accepted only after an
/// exact readback of the same bytes.
pub(super) fn stage_candidate_descriptor(
    cloud: &CloudStorage,
    pending: &CloudFormatMigrationPending,
    candidate: &CloudMetadataGeneration,
    deadline: &OperationDeadline,
) -> MidgeResult<()> {
    validate_candidate_identity(pending, candidate)?;
    stage_immutable_generation_descriptor(
        cloud,
        &pending.candidate.descriptor_key,
        candidate,
        "FORMAT 10 candidate",
        deadline,
    )
}

/// Retain the exact v9 source descriptor outside the v10 body namespace. This
/// descriptor remains available after pending gate identities are compacted
/// into the permanent active target gate.
pub(super) fn stage_source_descriptor(
    cloud: &CloudStorage,
    pending: &CloudFormatMigrationPending,
    source: &CloudMetadataGeneration,
    deadline: &OperationDeadline,
) -> MidgeResult<()> {
    let key = CloudFormatMigrationPending::source_descriptor_key(&pending.migration_id);
    if pending.source != CloudMetadataGenerationIdentity::from_generation(source) {
        return Err(MidgeError::Corruption(
            "FORMAT 9 source does not match its pending immutable identity".into(),
        ));
    }
    stage_immutable_generation_descriptor(cloud, &key, source, "FORMAT 9 source", deadline)
}

fn stage_immutable_generation_descriptor(
    cloud: &CloudStorage,
    key: &str,
    generation: &CloudMetadataGeneration,
    description: &str,
    deadline: &OperationDeadline,
) -> MidgeResult<()> {
    let wire = serde_json::to_vec(&CandidateDescriptor::from(generation)).map_err(|error| {
        MidgeError::Internal(format!("encode {description} descriptor: {error}"))
    })?;
    if wire.len() > MAX_CANDIDATE_DESCRIPTOR_BYTES {
        return Err(MidgeError::ResourceLimit(format!(
            "{description} descriptor exceeds its 64 KiB bound"
        )));
    }
    let blocking = BlockingCloud::new(cloud, deadline);
    if let Some(existing) = blocking.get_bounded(key, MAX_CANDIDATE_DESCRIPTOR_BYTES as u64)? {
        return compare_generation_descriptor(&existing, &wire, generation, description);
    }

    let timeout = deadline
        .clamp_nonzero(cloud.callback_timeout())
        .ok_or_else(|| {
            MidgeError::Timeout(format!("{description} descriptor write deadline expired"))
        })?;
    let mut headers = StoragePrecondition::IfAbsent
        .headers()
        .map_err(|error| MidgeError::Internal(error.to_string()))?;
    crate::storage::cloud::set_request_timeout_header(&mut headers, timeout);
    let (sender, receiver) = std::sync::mpsc::channel();
    cloud.submit_put(key, wire.clone(), headers, sender);
    let write_result = match receiver.recv_timeout(timeout) {
        Ok(CloudEvent::Put { result, .. }) => result.map_err(|error| {
            crate::storage::cloud::contextualize_operation_error(
                &error,
                format!("immutable {description} descriptor write"),
                deadline,
            )
        }),
        Ok(event) => Err(MidgeError::Internal(format!(
            "unexpected candidate descriptor write response: {event:?}"
        ))),
        Err(RecvTimeoutError::Timeout) => Err(MidgeError::Timeout(format!(
            "{description} descriptor write exceeded startup deadline"
        ))),
        Err(RecvTimeoutError::Disconnected) => Err(MidgeError::Internal(format!(
            "{description} descriptor write callback closed"
        ))),
    };
    if deadline.is_expired() {
        return Err(MidgeError::Timeout(format!(
            "{description} descriptor write exceeded startup deadline"
        )));
    }

    let readback = if crate::failpoints::is_active(
        "midge::cloud::inject_missing_format_descriptor_readback",
    ) {
        None
    } else {
        blocking.get_exact_len(key, wire.len() as u64)?
    };
    match readback {
        Some(existing) => compare_generation_descriptor(&existing, &wire, generation, description),
        None => Err(write_result.err().unwrap_or_else(|| {
            MidgeError::RecoveryFailed(format!(
                "{description} descriptor was not durably readable after write"
            ))
        })),
    }
}

fn compare_generation_descriptor(
    existing: &[u8],
    expected: &[u8],
    generation: &CloudMetadataGeneration,
    description: &str,
) -> MidgeResult<()> {
    let parsed = decode_candidate_descriptor(existing)?;
    if existing != expected || &parsed != generation {
        return Err(MidgeError::Corruption(format!(
            "immutable {description} descriptor changed"
        )));
    }
    Ok(())
}

fn decode_candidate_descriptor(bytes: &[u8]) -> MidgeResult<CloudMetadataGeneration> {
    if bytes.len() > MAX_CANDIDATE_DESCRIPTOR_BYTES {
        return Err(MidgeError::Corruption(
            "FORMAT 10 candidate descriptor exceeds its 64 KiB bound".into(),
        ));
    }
    let descriptor: CandidateDescriptor = serde_json::from_slice(bytes).map_err(|error| {
        MidgeError::Corruption(format!("malformed FORMAT 10 candidate descriptor: {error}"))
    })?;
    Ok(descriptor.into())
}

fn validate_candidate_identity(
    pending: &CloudFormatMigrationPending,
    candidate: &CloudMetadataGeneration,
) -> MidgeResult<()> {
    if pending.target_format != 10
        || pending.candidate.generation
            != CloudMetadataGenerationIdentity::from_generation(candidate)
        || pending.candidate.descriptor_key
            != CloudFormatMigrationPending::candidate_descriptor_key(&pending.migration_id)
    {
        return Err(MidgeError::Corruption(
            "FORMAT 10 candidate does not match its pending immutable identity".into(),
        ));
    }
    Ok(())
}

/// Reconcile pending/active cloud state under the newly acquired owner's
/// holder and epoch. A v4 lease with no committed generation remains eligible
/// for ordinary empty-database bootstrap.
pub(super) fn reconcile_cloud_format_gate(
    cloud: &CloudStorage,
    store: &dyn LeaderStore,
    holder_id: &str,
    epoch: u64,
    deadline: &OperationDeadline,
) -> MidgeResult<CloudFormatGateResolution> {
    let gate = store
        .read_format_migration_gate(holder_id, epoch, remaining(cloud, deadline)?)
        .map_err(map_lease_error)?;
    let selected = read_selected_generation(store, holder_id, epoch, cloud, deadline)?;
    match gate {
        None => match selected {
            None => Ok(CloudFormatGateResolution::Format9),
            Some(generation) if metadata_chain::is_chain(&generation) => {
                // A v9 chain never represents a v10 selector. Ordinary v9
                // hydration checks its reconstructed marker and semantics.
                Ok(CloudFormatGateResolution::Format9)
            }
            Some(generation) => {
                let marker = read_generation_format(cloud, &generation, deadline)?;
                if marker != crate::metadata::format::current_format_marker_bytes() {
                    return Err(MidgeError::NotSupported(
                        "cloud FORMAT 10 authority requires the migration compatibility gate and a v10-capable runtime".into(),
                    ));
                }
                Ok(CloudFormatGateResolution::Format9)
            }
        },
        Some(CloudFormatMigrationGate::Pending(pending)) => {
            reconcile_pending(cloud, store, holder_id, epoch, selected, &pending, deadline)?;
            Ok(CloudFormatGateResolution::Format10Selected)
        }
        Some(CloudFormatMigrationGate::Active { target_format: 10 }) => {
            let generation = selected.ok_or_else(|| {
                MidgeError::LeaseIndeterminate(
                    "active FORMAT 10 gate has no selected metadata generation".into(),
                )
            })?;
            let bodies = read_committed_metadata_bodies_within(cloud, &generation, deadline)?;
            let marker = bodies.get(crate::metadata::files::FORMAT).ok_or_else(|| {
                MidgeError::Corruption("active FORMAT 10 marker body is missing".into())
            })?;
            if marker.as_slice() != FORMAT_10_MARKER {
                return Err(MidgeError::LeaseIndeterminate(
                    "active FORMAT 10 gate does not select a FORMAT 10 generation".into(),
                ));
            }
            decode_generation_manifest(&generation, &bodies, deadline)?;
            Ok(CloudFormatGateResolution::Format10Selected)
        }
        Some(CloudFormatMigrationGate::Active { .. }) => Err(MidgeError::LeaseIndeterminate(
            "active cloud FORMAT migration has an unsupported target".into(),
        )),
    }
}

fn reconcile_pending(
    cloud: &CloudStorage,
    store: &dyn LeaderStore,
    holder_id: &str,
    epoch: u64,
    selected: Option<CloudMetadataGeneration>,
    pending: &CloudFormatMigrationPending,
    deadline: &OperationDeadline,
) -> MidgeResult<()> {
    let selected_state = match selected.as_ref() {
        Some(source)
            if CloudMetadataGenerationIdentity::from_generation(source) == pending.source =>
        {
            false
        }
        Some(candidate)
            if CloudMetadataGenerationIdentity::from_generation(candidate)
                == pending.candidate.generation =>
        {
            true
        }
        _ => {
            return Err(MidgeError::Fenced(
                "pending FORMAT migration found an unexpected cloud pointer selection".into(),
            ));
        }
    };
    let candidate = read_candidate_descriptor(cloud, pending, deadline)?;
    let candidate_bodies = read_committed_metadata_bodies_within(cloud, &candidate, deadline)?;
    if candidate_bodies
        .get(crate::metadata::files::FORMAT)
        .map(Vec::as_slice)
        != Some(FORMAT_10_MARKER)
    {
        return Err(MidgeError::Corruption(
            "pending FORMAT 10 candidate descriptor does not select a FORMAT 10 marker".into(),
        ));
    }
    decode_generation_manifest(&candidate, &candidate_bodies, deadline)?;
    if !selected_state {
        let source = selected.as_ref().ok_or_else(|| {
            MidgeError::LeaseIndeterminate("pending cloud source pointer disappeared".into())
        })?;
        let recorded_source = read_source_descriptor(cloud, pending, deadline)?;
        if &recorded_source != source {
            return Err(MidgeError::LeaseIndeterminate(
                "selected FORMAT 9 source differs from its immutable migration descriptor".into(),
            ));
        }
        let source_bodies = read_v9_generation(cloud, &source, deadline)?;
        if source_payload_sha256(&source_bodies) != pending.source_sha256 {
            return Err(MidgeError::Corruption(
                "pending FORMAT 10 source payload digest changed".into(),
            ));
        }
        let outcome = store.publish_committed_metadata(
            holder_id,
            epoch,
            Some(source),
            candidate.clone(),
            remaining(cloud, deadline)?,
        );
        let selected_after = read_selected_generation(store, holder_id, epoch, cloud, deadline)?;
        if selected_after.as_ref() != Some(&candidate) {
            if let Some(source_after) = selected_after.as_ref() {
                if CloudMetadataGenerationIdentity::from_generation(source_after) == pending.source
                {
                    return Err(outcome.err().map_or_else(
                        || {
                            MidgeError::Fenced(
                                "pending cloud pointer did not select its exact candidate".into(),
                            )
                        },
                        map_lease_error,
                    ));
                }
            }
            return Err(MidgeError::LeaseIndeterminate(
                "cloud pointer selection is neither the pending source nor exact candidate".into(),
            ));
        }
        // The exact pointer readback resolves even a lost CAS response.
        let _ = outcome;
    } else if selected.as_ref() != Some(&candidate) {
        return Err(MidgeError::LeaseIndeterminate(
            "selected FORMAT 10 candidate differs from its immutable descriptor".into(),
        ));
    }
    store
        .activate_format_10_gate(
            holder_id,
            epoch,
            &pending.migration_id,
            &candidate,
            remaining(cloud, deadline)?,
        )
        .map_err(map_lease_error)
}

fn read_candidate_descriptor(
    cloud: &CloudStorage,
    pending: &CloudFormatMigrationPending,
    deadline: &OperationDeadline,
) -> MidgeResult<CloudMetadataGeneration> {
    if pending.candidate.descriptor_key
        != CloudFormatMigrationPending::candidate_descriptor_key(&pending.migration_id)
    {
        return Err(MidgeError::Corruption(
            "pending candidate descriptor key is not migration-bound".into(),
        ));
    }
    let bytes = BlockingCloud::new(cloud, deadline)
        .get_bounded(
            &pending.candidate.descriptor_key,
            MAX_CANDIDATE_DESCRIPTOR_BYTES as u64,
        )?
        .ok_or_else(|| {
            MidgeError::RecoveryFailed("pending cloud candidate descriptor is missing".into())
        })?;
    let candidate = decode_candidate_descriptor(&bytes)?;
    validate_candidate_identity(pending, &candidate)?;
    Ok(candidate)
}

fn read_source_descriptor(
    cloud: &CloudStorage,
    pending: &CloudFormatMigrationPending,
    deadline: &OperationDeadline,
) -> MidgeResult<CloudMetadataGeneration> {
    let key = CloudFormatMigrationPending::source_descriptor_key(&pending.migration_id);
    let bytes = BlockingCloud::new(cloud, deadline)
        .get_bounded(&key, MAX_CANDIDATE_DESCRIPTOR_BYTES as u64)?
        .ok_or_else(|| {
            MidgeError::RecoveryFailed("pending cloud source descriptor is missing".into())
        })?;
    let source = decode_candidate_descriptor(&bytes)?;
    if CloudMetadataGenerationIdentity::from_generation(&source) != pending.source {
        return Err(MidgeError::Corruption(
            "immutable FORMAT 9 source descriptor differs from the pending identity".into(),
        ));
    }
    Ok(source)
}

fn read_selected_generation(
    store: &dyn LeaderStore,
    holder_id: &str,
    epoch: u64,
    cloud: &CloudStorage,
    deadline: &OperationDeadline,
) -> MidgeResult<Option<CloudMetadataGeneration>> {
    // Refresh the pointer cache before using the writer-scoped read. The
    // latter rejects a pointer whose owner or epoch differs from this lease.
    let _ = store
        .read_committed_metadata(remaining(cloud, deadline)?)
        .map_err(map_lease_error)?;
    match store
        .read_committed_metadata_for_writer(holder_id, epoch, remaining(cloud, deadline)?)
        .map_err(map_lease_error)?
    {
        CloudMetadataHead::Committed(generation) => Ok(Some(generation)),
        CloudMetadataHead::Uncommitted => Ok(None),
        CloudMetadataHead::MissingLease => Err(MidgeError::LeaseIndeterminate(
            "cloud lease disappeared before FORMAT gate reconciliation".into(),
        )),
    }
}

fn read_v9_generation(
    cloud: &CloudStorage,
    generation: &CloudMetadataGeneration,
    deadline: &OperationDeadline,
) -> MidgeResult<BTreeMap<String, Vec<u8>>> {
    let bodies = read_committed_metadata_bodies_within(cloud, generation, deadline)?;
    let marker = bodies
        .get(crate::metadata::files::FORMAT)
        .ok_or_else(|| MidgeError::Corruption("selected v9 FORMAT marker is missing".into()))?;
    if marker.as_slice() != crate::metadata::format::current_format_marker_bytes().as_slice() {
        return Err(MidgeError::NotSupported(
            "selected cloud source is not a FORMAT 9 generation".into(),
        ));
    }
    validate_v9_bodies_strict(&bodies, deadline)?;
    Ok(bodies)
}

fn validate_v9_bodies_strict(
    bodies: &BTreeMap<String, Vec<u8>>,
    deadline: &OperationDeadline,
) -> MidgeResult<()> {
    let fs: std::sync::Arc<dyn crate::io::Fs> = std::sync::Arc::new(crate::io::MockFs::new());
    for (name, body) in bodies {
        if deadline.is_expired() {
            return Err(MidgeError::Timeout(
                "cloud FORMAT 9 strict source validation exceeded startup deadline".into(),
            ));
        }
        let temp = crate::io::FsPath::new(format!("{name}.migration.tmp"));
        let target = crate::io::FsPath::new(name.as_str());
        crate::io::staging::stage_bytes_typed(&fs, &temp, &target, body, || {
            if deadline.is_expired() {
                Err(MidgeError::Timeout(
                    "cloud FORMAT 9 strict source validation exceeded startup deadline".into(),
                ))
            } else {
                Ok(())
            }
        })?;
    }
    crate::metadata::validate_format_marker(fs.as_ref(), std::path::Path::new("cloud source"))?;
    crate::metadata::ManifestPersistence::load_with_fs_and_policy_typed(
        &fs,
        crate::config::RecoveryPolicy::Strict,
    )?;
    crate::runtime::IntentPersistence::load_with_fs_and_policy_typed(
        &fs,
        crate::config::RecoveryPolicy::Strict,
    )?;
    Ok(())
}

fn read_generation_format(
    cloud: &CloudStorage,
    generation: &CloudMetadataGeneration,
    deadline: &OperationDeadline,
) -> MidgeResult<Vec<u8>> {
    let object = generation
        .objects
        .iter()
        .find(|object| object.file_name == crate::metadata::files::FORMAT)
        .ok_or_else(|| MidgeError::Corruption("cloud FORMAT marker reference is missing".into()))?;
    if object.len != FORMAT_10_MARKER.len() as u64
        && object.len != FORMAT_10_MARKER.len() as u64 - 1
    {
        return Err(MidgeError::Corruption(
            "cloud FORMAT marker length is invalid".into(),
        ));
    }
    let body = BlockingCloud::new(cloud, deadline)
        .get_exact_len(&object.object_key, object.len)?
        .ok_or_else(|| {
            MidgeError::RecoveryFailed("cloud FORMAT marker object is missing".into())
        })?;
    if u64::try_from(body.len()).ok() != Some(object.len) || crc32c::crc32c(&body) != object.crc32c
    {
        return Err(MidgeError::Corruption(
            "cloud FORMAT marker changed from its selected descriptor".into(),
        ));
    }
    Ok(body)
}

/// Hash the reconstructed exact v9 body map; chain-object identity is checked
/// separately by the selected generation descriptor.
pub(super) fn source_payload_sha256(bodies: &BTreeMap<String, Vec<u8>>) -> String {
    let mut digest = sha2::Sha256::new();
    digest.update(SOURCE_DIGEST_DOMAIN);
    for (name, body) in bodies {
        digest.update(u64::try_from(name.len()).unwrap_or(u64::MAX).to_be_bytes());
        digest.update(name.as_bytes());
        digest.update(u64::try_from(body.len()).unwrap_or(u64::MAX).to_be_bytes());
        digest.update(body);
    }
    hex::encode(digest.finalize())
}

fn remaining(cloud: &CloudStorage, deadline: &OperationDeadline) -> MidgeResult<Duration> {
    deadline
        .clamp_nonzero(cloud.callback_timeout())
        .ok_or_else(|| MidgeError::Timeout("cloud FORMAT gate deadline expired".into()))
}

fn map_lease_error(error: crate::lease::LeaseError) -> MidgeError {
    match error {
        crate::lease::LeaseError::Timeout(message) => MidgeError::Timeout(message),
        crate::lease::LeaseError::Indeterminate(message) => MidgeError::LeaseIndeterminate(message),
        crate::lease::LeaseError::RenewalFailed(message) => MidgeError::Fenced(message),
        other => {
            MidgeError::RecoveryFailed(format!("cloud FORMAT gate reconciliation failed: {other}"))
        }
    }
}
