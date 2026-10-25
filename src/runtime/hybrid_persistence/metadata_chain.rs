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
    #[serde(deserialize_with = "deserialize_control_changes")]
    changes: BTreeMap<String, FileEdit>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cut: Option<crate::runtime::metadata_publication::MetadataCutProof>,
}

fn deserialize_control_changes<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> Result<BTreeMap<String, FileEdit>, D::Error> {
    struct Changes;
    impl<'de> serde::de::Visitor<'de> for Changes {
        type Value = BTreeMap<String, FileEdit>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("at most four known control-file changes")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut map: A,
        ) -> Result<Self::Value, A::Error> {
            if map
                .size_hint()
                .is_some_and(|hint| hint > files::CLOUD_MIRRORED.len())
            {
                return Err(serde::de::Error::custom(
                    "cloud control change count exceeds limit",
                ));
            }
            let mut changes = BTreeMap::new();
            while let Some(name) = map.next_key::<String>()? {
                if changes.len() == files::CLOUD_MIRRORED.len()
                    || !files::CLOUD_MIRRORED.contains(&name.as_str())
                    || changes.contains_key(&name)
                {
                    return Err(serde::de::Error::custom(
                        "cloud manifest repeats or names an unknown control file",
                    ));
                }
                changes.insert(name, map.next_value()?);
            }
            Ok(changes)
        }
    }
    decoder.deserialize_map(Changes)
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
    if let Some(reservation) = &reservation {
        request = request.with_reservation(std::sync::Arc::clone(reservation));
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
    let mut io = BlockingCloud::new(cloud, deadline);
    if let Some(reservation) = reservation {
        io = io.with_reservation(reservation);
    }
    let actual = io.get_optional(key)?;
    if actual.as_deref() == Some(bytes) {
        Ok(())
    } else {
        Err(MidgeError::Corruption(
            "cloud manifest PUT was not proven by readback".into(),
        ))
    }
}

#[cfg(test)]
mod decoder_tests {
    use super::*;

    #[test]
    fn should_bound_control_change_storage_before_decoding_announced_members() {
        // Arrange: malformed member values must not be decoded before an
        // oversized map count is rejected.
        let raw = serde_json::json!({
            "id": 1,
            "writer_epoch": 1,
            "operation_id": uuid::Uuid::new_v4().to_string(),
            "parent": null,
            "changes": {"bad0": null, "bad1": null, "bad2": null, "bad3": null, "bad4": null},
        });
        let oversized = codec::encode(CHAIN_KIND, &raw).unwrap();
        let valid = ManifestVersion {
            id: SharedGenerationId::FIRST,
            writer_epoch: 1,
            operation_id: uuid::Uuid::new_v4().to_string(),
            parent: None,
            changes: files::CLOUD_MIRRORED
                .iter()
                .map(|name| {
                    (
                        (*name).into(),
                        FileEdit::Replace(Bytes::from_static(b"body")),
                    )
                })
                .collect(),
            cut: None,
        };
        let maximum = codec::encode(CHAIN_KIND, &valid).unwrap();

        // Act
        let rejected = codec::decode::<ManifestVersion>(CHAIN_KIND, &oversized);
        let accepted = codec::decode::<ManifestVersion>(CHAIN_KIND, &maximum).unwrap();

        // Assert
        assert!(matches!(rejected, Err(MidgeError::Corruption(message))
            if message.contains("control change count exceeds limit")));
        assert_eq!(accepted.changes.len(), files::CLOUD_MIRRORED.len());
    }
}

#[cfg(test)]
mod publish_tests {
    use super::*;
    use crate::storage::cloud::{
        CloudBackend, CloudCallback, CloudEvent, CloudHeadCapability, CloudListCapability,
        CloudReadCapability, CloudWriteCapability, MockCloudBackend,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct LostConditionalPut {
        inner: MockCloudBackend,
        puts: AtomicUsize,
        gets: AtomicUsize,
    }

    impl CloudReadCapability for LostConditionalPut {
        fn submit_get(&self, key: &str, callback: CloudCallback) {
            self.gets.fetch_add(1, Ordering::Relaxed);
            self.inner.submit_get(key, callback);
        }

        crate::storage::cloud::forward_cloud_backend!(inner;
            submit_get_with_metadata,
            submit_get_range,
            submit_get_range_with_identity,
        );
    }

    impl CloudWriteCapability for LostConditionalPut {
        fn submit_put(
            &self,
            key: &str,
            bytes: Vec<u8>,
            headers: Vec<(String, String)>,
            callback: CloudCallback,
        ) {
            self.puts.fetch_add(1, Ordering::Relaxed);
            let (tx, rx) = std::sync::mpsc::channel();
            self.inner.submit_put(key, bytes, headers, tx);
            assert!(matches!(
                rx.recv().expect("conditional catalog PUT completion"),
                CloudEvent::Put { result: Ok(()), .. }
            ));
            callback
                .send(CloudEvent::Put {
                    key: key.into(),
                    result: Err(crate::storage::cloud::CloudError::Transport(
                        "injected lost conditional catalog PUT response".into(),
                    )),
                })
                .expect("deliver ambiguous PUT outcome");
        }
    }

    impl crate::storage::cloud::CloudDeleteCapability for LostConditionalPut {
        crate::storage::cloud::forward_cloud_backend!(inner; submit_delete);
    }

    impl CloudListCapability for LostConditionalPut {
        crate::storage::cloud::forward_cloud_backend!(inner; submit_list);
    }

    impl CloudHeadCapability for LostConditionalPut {
        crate::storage::cloud::forward_cloud_backend!(inner; submit_head);
    }

    impl CloudBackend for LostConditionalPut {}

    #[test]
    fn should_reconcile_lost_conditional_catalog_put_without_replaying_mutation() -> MidgeResult<()>
    {
        // Arrange: the mock commits the first conditional PUT, then only its
        // response is lost. Publication must read back the exact immutable key.
        let backend = std::sync::Arc::new(LostConditionalPut {
            inner: MockCloudBackend::new(),
            puts: AtomicUsize::new(0),
            gets: AtomicUsize::new(0),
        });
        let cloud = CloudStorage::new(backend.clone(), String::new());
        let key = "metadata/chains/generation-17";
        let bytes = b"exact immutable catalog body";
        let deadline = OperationDeadline::from_budget(std::time::Duration::from_secs(5));

        // Act
        publish_bytes(&cloud, key, bytes, &deadline, None)?;

        // Assert: caller reconciliation proves the exact body with one PUT;
        // the conditional mutation is never blindly replayed.
        assert_eq!(backend.puts.load(Ordering::Relaxed), 1);
        assert_eq!(backend.gets.load(Ordering::Relaxed), 1);
        assert_eq!(
            backend.inner.get_uploads(),
            [(key.to_string(), bytes.len() as u64)]
        );
        assert_eq!(backend.inner.get_downloads(), [key.to_string()]);
        Ok(())
    }
}
