//! Versioned cloud metadata snapshots and bounded suffix-edit chains.

use super::metadata_snapshot::CloudMetadataMirrorContext;
use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use crate::lease::{CloudMetadataGeneration, CloudMetadataObject, SharedGenerationId};
use crate::metadata::cloud_manifest::{CHAIN_FILE_NAME, MAX_CHAIN_BYTES, MAX_CHAIN_OBJECTS};
use crate::metadata::{codec, files};
use crate::storage::cloud::{BlockingCloud, CloudStorage};
use crate::storage::{StorageBackend, StoragePrecondition, StorageRequest};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const CHAIN_KIND: u8 = 5;
const CHECKPOINT_EDIT_BYTES: u64 = 16 * 1024;
type DecodedChain = (
    BTreeMap<String, Vec<u8>>,
    Option<crate::runtime::metadata_publication::MetadataCutProof>,
);

#[derive(Serialize, Deserialize)]
enum FileEdit {
    Replace(Bytes),
    Suffix { keep: usize, bytes: Bytes },
    Remove,
}

#[derive(Serialize, Deserialize)]
struct ManifestVersion {
    id: SharedGenerationId,
    writer_epoch: u64,
    operation_id: String,
    parent: Option<CloudMetadataObject>,
    changes: BTreeMap<String, FileEdit>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cut: Option<crate::runtime::metadata_publication::MetadataCutProof>,
}

pub(super) fn reclaimable_version(
    encoded: &[u8],
    key: &str,
    ordinal: u64,
    writer_epoch: u64,
) -> MidgeResult<bool> {
    crate::metadata::cloud_manifest::validate_key(key, ordinal)?;
    let version: ManifestVersion = codec::decode(CHAIN_KIND, encoded)?;
    Ok(version.id.get() == ordinal
        && version.writer_epoch != 0
        && version.writer_epoch <= writer_epoch
        && key.rsplit('/').next() == Some(version.operation_id.as_str()))
}

pub(crate) fn is_chain(generation: &CloudMetadataGeneration) -> bool {
    generation
        .objects
        .first()
        .is_some_and(|object| object.file_name == CHAIN_FILE_NAME)
}

pub(crate) fn validate_reference(object: &CloudMetadataObject, ordinal: u64) -> MidgeResult<()> {
    crate::metadata::cloud_manifest::validate_key(&object.object_key, ordinal)?;
    if object.file_name != CHAIN_FILE_NAME || object.len == 0 {
        return Err(MidgeError::Corruption(
            "invalid cloud manifest object reference".into(),
        ));
    }
    Ok(())
}

pub(crate) fn read(
    cloud: &CloudStorage,
    generation: &CloudMetadataGeneration,
    deadline: &OperationDeadline,
) -> MidgeResult<BTreeMap<String, Vec<u8>>> {
    read_with_cut(cloud, generation, deadline).map(|(bodies, _)| bodies)
}

pub(super) fn read_with_cut(
    cloud: &CloudStorage,
    generation: &CloudMetadataGeneration,
    deadline: &OperationDeadline,
) -> MidgeResult<DecodedChain> {
    read_with_cut_reserved(cloud, generation, deadline, None)
}

pub(super) fn read_with_cut_reserved(
    cloud: &CloudStorage,
    generation: &CloudMetadataGeneration,
    deadline: &OperationDeadline,
    reservation: Option<std::sync::Arc<crate::common::resource_budget::ResourceReservation>>,
) -> MidgeResult<DecodedChain> {
    if generation.objects.is_empty() || generation.objects.len() > MAX_CHAIN_OBJECTS {
        return Err(MidgeError::Corruption(
            "cloud manifest chain length is invalid".into(),
        ));
    }
    let total = generation
        .objects
        .iter()
        .try_fold(0_u64, |total, object| total.checked_add(object.len))
        .filter(|total| *total <= MAX_CHAIN_BYTES)
        .ok_or_else(|| {
            MidgeError::ResourceLimit("cloud manifest chain exceeds read bound".into())
        })?;
    let _ = total;
    let mut io = BlockingCloud::new(cloud, deadline);
    if let Some(reservation) = reservation {
        io = io.with_reservation(reservation);
    }
    let mut versions = Vec::with_capacity(generation.objects.len());
    let mut successor_epoch = u64::MAX;
    for (index, object) in generation.objects.iter().enumerate() {
        let ordinal = generation
            .id
            .get()
            .checked_sub(index as u64)
            .ok_or_else(|| MidgeError::Corruption("cloud manifest ordinal underflow".into()))?;
        validate_reference(object, ordinal)?;
        let encoded = io.get_optional(&object.object_key)?.ok_or_else(|| {
            MidgeError::Corruption("committed cloud manifest object is missing".into())
        })?;
        if encoded.len() as u64 != object.len || crc32c::crc32c(&encoded) != object.crc32c {
            return Err(MidgeError::Corruption(
                "cloud manifest object identity differs".into(),
            ));
        }
        let version: ManifestVersion = codec::decode(CHAIN_KIND, &encoded)?;
        if version.id.get() != ordinal
            || version.writer_epoch == 0
            || version.writer_epoch > successor_epoch
            || version.parent.as_ref() != generation.objects.get(index + 1)
            || !object.object_key.ends_with(&version.operation_id)
        {
            return Err(MidgeError::Corruption(
                "cloud manifest predecessor lineage differs".into(),
            ));
        }
        successor_epoch = version.writer_epoch;
        versions.push(version);
    }
    let cut = versions.first().and_then(|version| version.cut.clone());
    let head_epoch = versions.first().map(|version| version.writer_epoch);
    let mut bodies = BTreeMap::new();
    for version in versions.into_iter().rev() {
        apply(version, &mut bodies)?;
    }
    if let Some(cut) = &cut {
        cut.validate(&bodies)?;
        if Some(cut.writer_epoch) != head_epoch {
            return Err(MidgeError::Corruption(
                "cloud metadata cut epoch differs from its manifest version".into(),
            ));
        }
    }
    Ok((bodies, cut))
}

fn apply(version: ManifestVersion, bodies: &mut BTreeMap<String, Vec<u8>>) -> MidgeResult<()> {
    for (name, edit) in version.changes {
        if !files::CLOUD_MIRRORED.contains(&name.as_str()) {
            return Err(MidgeError::Corruption(
                "cloud manifest edits an unknown control file".into(),
            ));
        }
        match edit {
            FileEdit::Replace(bytes) => {
                bodies.insert(name, bytes.to_vec());
            }
            FileEdit::Remove if version.parent.is_some() => {
                bodies.remove(&name);
            }
            FileEdit::Suffix { keep, bytes } if version.parent.is_some() => {
                let previous = bodies
                    .get_mut(&name)
                    .filter(|body| keep <= body.len())
                    .ok_or_else(|| {
                        MidgeError::Corruption("cloud manifest suffix has no valid prefix".into())
                    })?;
                previous.truncate(keep);
                previous.extend_from_slice(&bytes);
            }
            _ => {
                return Err(MidgeError::Corruption(
                    "cloud manifest snapshot contains a delta".into(),
                ))
            }
        }
    }
    Ok(())
}

fn changes(
    local: &BTreeMap<String, Vec<u8>>,
    previous: Option<&BTreeMap<String, Vec<u8>>>,
) -> BTreeMap<String, FileEdit> {
    let mut changes = BTreeMap::new();
    for (name, bytes) in local {
        if let Some(old) = previous.and_then(|previous| previous.get(name)) {
            if bytes == old {
                continue;
            }
            let keep = bytes
                .iter()
                .zip(old)
                .take_while(|(left, right)| left == right)
                .count();
            changes.insert(
                name.clone(),
                FileEdit::Suffix {
                    keep,
                    bytes: Bytes::copy_from_slice(&bytes[keep..]),
                },
            );
        } else {
            changes.insert(
                name.clone(),
                FileEdit::Replace(Bytes::copy_from_slice(bytes)),
            );
        }
    }
    if let Some(previous) = previous {
        for name in previous.keys().filter(|name| !local.contains_key(*name)) {
            changes.insert(name.clone(), FileEdit::Remove);
        }
    }
    changes
}

fn cached_changes(
    local: &BTreeMap<String, Vec<u8>>,
    fingerprints: &BTreeMap<String, super::metadata_snapshot::FileFingerprint>,
) -> BTreeMap<String, FileEdit> {
    let mut edits = BTreeMap::new();
    for (name, bytes) in local {
        match fingerprints
            .get(name)
            .and_then(|fingerprint| fingerprint.unchanged_prefix(bytes))
        {
            Some(keep) if keep == bytes.len() => {}
            Some(keep) => {
                edits.insert(
                    name.clone(),
                    FileEdit::Suffix {
                        keep,
                        bytes: Bytes::copy_from_slice(&bytes[keep..]),
                    },
                );
            }
            None => {
                edits.insert(
                    name.clone(),
                    FileEdit::Replace(Bytes::copy_from_slice(bytes)),
                );
            }
        }
    }
    for name in fingerprints
        .keys()
        .filter(|name| !local.contains_key(*name))
    {
        edits.insert(name.clone(), FileEdit::Remove);
    }
    edits
}

pub(super) fn publish(
    context: &CloudMetadataMirrorContext<'_>,
    local: &BTreeMap<String, Vec<u8>>,
    previous: Option<&CloudMetadataGeneration>,
    old_bodies: Option<&BTreeMap<String, Vec<u8>>>,
    fingerprints: Option<&BTreeMap<String, super::metadata_snapshot::FileFingerprint>>,
    id: SharedGenerationId,
) -> MidgeResult<Vec<CloudMetadataObject>> {
    publish_with_cut(context, local, previous, old_bodies, fingerprints, id, None)
}

pub(super) fn publish_with_cut(
    context: &CloudMetadataMirrorContext<'_>,
    local: &BTreeMap<String, Vec<u8>>,
    previous: Option<&CloudMetadataGeneration>,
    old_bodies: Option<&BTreeMap<String, Vec<u8>>>,
    fingerprints: Option<&BTreeMap<String, super::metadata_snapshot::FileFingerprint>>,
    id: SharedGenerationId,
    cut: Option<&crate::runtime::CapturedMetadataCut>,
) -> MidgeResult<Vec<CloudMetadataObject>> {
    let mut history = previous
        .filter(|generation| is_chain(generation))
        .map_or_else(Vec::new, |generation| generation.objects.clone());
    let edit_bytes = history
        .iter()
        .take(history.len().saturating_sub(1))
        .fold(0_u64, |total, object| total.saturating_add(object.len));
    if history.len() >= MAX_CHAIN_OBJECTS || edit_bytes >= CHECKPOINT_EDIT_BYTES {
        history.clear();
    }
    let mut version = ManifestVersion {
        id,
        writer_epoch: context.authority.writer_epoch,
        operation_id: uuid::Uuid::new_v4().to_string(),
        cut: cut.map(|cut| cut.proof.clone()),
        parent: history.first().cloned(),
        changes: if history.is_empty() {
            changes(local, None)
        } else if let Some(fingerprints) = fingerprints {
            cached_changes(local, fingerprints)
        } else {
            changes(local, old_bodies)
        },
    };
    let mut encoded = codec::encode(CHAIN_KIND, &version)?;
    if version.parent.is_some()
        && (encoded.len() as u64 > CHECKPOINT_EDIT_BYTES
            || history.iter().fold(encoded.len() as u64, |sum, object| {
                sum.saturating_add(object.len)
            }) > MAX_CHAIN_BYTES)
    {
        history.clear();
        version.parent = None;
        version.changes = changes(local, None);
        encoded = codec::encode(CHAIN_KIND, &version)?;
    }
    if encoded.len() as u64 > MAX_CHAIN_BYTES {
        return Err(MidgeError::ResourceLimit(
            "cloud manifest snapshot exceeds read bound".into(),
        ));
    }
    let key = format!("manifest/{:020}/{}", id.get(), version.operation_id);
    publish_bytes(
        context.cloud,
        &key,
        &encoded,
        context.deadline,
        cut.map(crate::runtime::CapturedMetadataCut::memory_reservation),
    )?;
    let object = CloudMetadataObject {
        file_name: CHAIN_FILE_NAME.into(),
        object_key: key,
        len: encoded.len() as u64,
        crc32c: crc32c::crc32c(&encoded),
    };
    history.insert(0, object);
    Ok(history)
}

fn publish_bytes(
    cloud: &CloudStorage,
    key: &str,
    bytes: &[u8],
    deadline: &OperationDeadline,
    reservation: Option<std::sync::Arc<crate::common::resource_budget::ResourceReservation>>,
) -> MidgeResult<()> {
    let (tx, rx) = std::sync::mpsc::channel();
    let mut request = StorageRequest::new(key, *deadline, cloud.callback_timeout())
        .with_precondition(StoragePrecondition::IfAbsent);
    if let Some(reservation) = reservation {
        request = request.with_reservation(reservation);
    }
    cloud.submit_write_receipt_request(request, bytes.to_vec(), tx);
    let timeout = deadline
        .clamp_nonzero(cloud.callback_timeout())
        .ok_or_else(|| MidgeError::Timeout("cloud manifest PUT deadline exhausted".into()))?;
    let outcome = rx.recv_timeout(timeout);
    match outcome {
        Ok(Ok(Some(receipt))) if receipt.matches(bytes) => return Ok(()),
        Ok(Ok(Some(_))) => {
            return Err(MidgeError::Corruption(
                "cloud manifest receipt differs".into(),
            ))
        }
        _ => {}
    }
    // A missing receipt or a lost conditional PUT response needs exact bytes
    // under the operation's stable immutable key before pointer publication.
    let actual = BlockingCloud::new(cloud, deadline).get_optional(key)?;
    if actual.as_deref() == Some(bytes) {
        Ok(())
    } else {
        Err(MidgeError::Corruption(
            "cloud manifest PUT was not proven by readback".into(),
        ))
    }
}
