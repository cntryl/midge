//! Immutable publication of the exact local FORMAT 10 metadata closure.

use crate::common::resource_budget::ResourceReservation;
use crate::common::{DeadlineScope, MidgeError, MidgeResult, OperationDeadline};
use crate::lease::CloudMetadataObject;
use crate::storage::cloud::{BlockingCloud, CloudStorage};
use crate::storage::{StorageBackend, StoragePrecondition, StorageRequest};
use std::collections::BTreeMap;
use std::sync::Arc;

pub(crate) struct CapturedFormat10Closure {
    pub(crate) horizon: u64,
    pub(crate) bodies: BTreeMap<String, Vec<u8>>,
    pub(crate) reservation: Arc<ResourceReservation>,
}

/// Capture the validated selected bytes while the shared manifest writer lock
/// prevents HEAD and its immutable members from being retired during capture.
pub(crate) fn capture_selected_format10_closure(
    fs: &dyn crate::io::Fs,
    publication_lock: &crate::runtime::MetadataPublicationLock,
    deadline: &OperationDeadline,
) -> MidgeResult<CapturedFormat10Closure> {
    let scope = DeadlineScope::new(*deadline);
    crate::metadata::journal::with_manifest_writer_view_lock_within(fs, &scope, || {
        capture_selected_under_writer_lock(fs, publication_lock, deadline)
    })
}

fn capture_selected_under_writer_lock(
    fs: &dyn crate::io::Fs,
    publication_lock: &crate::runtime::MetadataPublicationLock,
    deadline: &OperationDeadline,
) -> MidgeResult<CapturedFormat10Closure> {
    let mut paths = vec![crate::metadata::files::FORMAT.to_owned()];
    paths.extend(
        crate::metadata::local_authority::LocalMetadataAuthority::selected_relative_paths_from(fs)?,
    );
    paths.sort();
    paths.dedup();

    let mut expected_lengths = BTreeMap::new();
    let mut total = 0_u64;
    for path in paths {
        DeadlineScope::new(*deadline).check("FORMAT 10 selected closure preflight")?;
        validate_relative_path(&path)?;
        let metadata = fs
            .metadata(&crate::io::FsPath::new(&path))
            .map_err(crate::io::FsError::into_midge)?;
        let maximum = if path == crate::metadata::local_authority::HEAD_PATH {
            crate::metadata::local_authority::MAX_HEAD_BYTES as u64
        } else {
            crate::metadata::local_authority::MAX_CLOSURE_BYTES
        };
        if metadata.is_dir || metadata.len > maximum {
            return Err(MidgeError::ResourceLimit(format!(
                "FORMAT 10 selected object '{path}' exceeds its byte bound"
            )));
        }
        total = total
            .checked_add(metadata.len)
            .filter(|length| *length <= crate::metadata::local_authority::MAX_CLOSURE_BYTES)
            .ok_or_else(|| {
                MidgeError::ResourceLimit(
                    "FORMAT 10 selected closure exceeds its byte bound".into(),
                )
            })?;
        expected_lengths.insert(path, metadata.len);
    }
    let reservation = publication_lock.reserve_metadata_body_memory(total)?;
    let mut bodies = BTreeMap::new();
    for (path, expected_len) in expected_lengths {
        DeadlineScope::new(*deadline).check("FORMAT 10 selected closure capture")?;
        let file = fs
            .open_read(&crate::io::FsPath::new(&path))
            .map_err(crate::io::FsError::into_midge)?;
        let actual_len = file.len().map_err(crate::io::FsError::into_midge)?;
        if actual_len != expected_len {
            return Err(MidgeError::Corruption(format!(
                "FORMAT 10 selected object '{path}' changed during capture"
            )));
        }
        let bytes = file
            .read_at(0, actual_len)
            .map_err(crate::io::FsError::into_midge)?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != actual_len {
            return Err(MidgeError::Corruption(format!(
                "FORMAT 10 selected object '{path}' was short during capture"
            )));
        }
        bodies.insert(path, bytes.to_vec());
    }
    let horizon =
        crate::metadata::local_authority::LocalMetadataAuthority::validate_cloud_bodies(&bodies)?;
    Ok(CapturedFormat10Closure {
        horizon,
        bodies,
        reservation,
    })
}

/// Upload one already captured, validated selected closure under a fresh
/// publication UUID. The caller owns the local writer lock and memory
/// reservation until this completes and the pointer CAS is resolved.
pub(crate) fn publish_format10_closure(
    cloud: &CloudStorage,
    deadline: &OperationDeadline,
    publication_id: uuid::Uuid,
    bodies: &BTreeMap<String, Vec<u8>>,
    reservation: &Arc<ResourceReservation>,
) -> MidgeResult<(u64, Vec<CloudMetadataObject>)> {
    let horizon =
        crate::metadata::local_authority::LocalMetadataAuthority::validate_cloud_bodies(bodies)?;
    if bodies.is_empty() || bodies.len() > crate::metadata::local_authority::MAX_OBJECTS + 2 {
        return Err(MidgeError::ResourceLimit(
            "FORMAT 10 metadata closure exceeds its object bound".into(),
        ));
    }
    let total_bytes = bodies.values().try_fold(0_u64, |total, body| {
        total
            .checked_add(u64::try_from(body.len()).unwrap_or(u64::MAX))
            .filter(|next| *next <= crate::metadata::local_authority::MAX_CLOSURE_BYTES)
    });
    if total_bytes.is_none() {
        return Err(MidgeError::ResourceLimit(
            "FORMAT 10 metadata closure exceeds its byte bound".into(),
        ));
    }

    let mut objects = Vec::with_capacity(bodies.len());
    for (relative_path, body) in bodies {
        DeadlineScope::new(*deadline).check("FORMAT 10 metadata closure upload")?;
        validate_relative_path(relative_path)?;
        let object_key = format!("metadata/generations/{publication_id}/{relative_path}");
        publish_bytes(cloud, &object_key, body, deadline, reservation)?;
        objects.push(CloudMetadataObject {
            file_name: relative_path.clone(),
            object_key,
            len: u64::try_from(body.len()).unwrap_or(u64::MAX),
            crc32c: crc32c::crc32c(body),
        });
    }
    Ok((horizon, objects))
}

fn validate_relative_path(path: &str) -> MidgeResult<()> {
    if path.is_empty()
        || path.starts_with('/')
        || path
            .split('/')
            .any(|component| component.is_empty() || matches!(component, "." | ".."))
        || path.contains('\\')
    {
        return Err(MidgeError::Corruption(
            "FORMAT 10 metadata closure contains a noncanonical local path".into(),
        ));
    }
    Ok(())
}

fn publish_bytes(
    cloud: &CloudStorage,
    key: &str,
    bytes: &[u8],
    deadline: &OperationDeadline,
    reservation: &Arc<ResourceReservation>,
) -> MidgeResult<()> {
    let timeout = deadline
        .clamp_nonzero(cloud.callback_timeout())
        .ok_or_else(|| MidgeError::Timeout("FORMAT 10 metadata PUT deadline exhausted".into()))?;
    let request = StorageRequest::new(key, *deadline, timeout)
        .with_precondition(StoragePrecondition::IfAbsent)
        .with_reservation(Arc::clone(reservation));
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_write_receipt_request(request, bytes.to_vec(), tx);
    match rx.recv_timeout(timeout) {
        Ok(Ok(Some(receipt))) if receipt.matches(bytes) => return Ok(()),
        Ok(Ok(Some(_))) => {
            return Err(MidgeError::Corruption(
                "FORMAT 10 metadata PUT receipt differs".into(),
            ));
        }
        _ => {}
    }

    let actual = BlockingCloud::new(cloud, deadline)
        .with_reservation(Arc::clone(reservation))
        .get_optional(key)?;
    if actual.as_deref() == Some(bytes) {
        Ok(())
    } else {
        Err(MidgeError::Corruption(
            "FORMAT 10 metadata PUT was not proven by exact readback".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{Fs, MockFs, OpenOptions};
    use crate::lease::SharedGenerationId;
    use crate::metadata::{codec, Manifest};
    use std::time::Duration;

    #[test]
    fn should_publish_only_selected_format10_bodies_under_canonical_publication_keys(
    ) -> MidgeResult<()> {
        // Arrange
        let fs: Arc<dyn Fs> = Arc::new(MockFs::new());
        let manifest = codec::encode(codec::MANIFEST, &Manifest::default())?;
        crate::metadata::local_authority::LocalMetadataAuthority::initialize(
            Arc::clone(&fs),
            "selected-generation",
            &manifest,
            b"[]",
        )?;
        fs.open(
            &crate::io::FsPath::new(crate::metadata::files::FORMAT),
            OpenOptions {
                mode: crate::io::OpenMode::ReadWrite,
                create: true,
                create_new: true,
                truncate: false,
            },
        )?
        .append(bytes::Bytes::from_static(b"midge-format-version=10\n"))?;
        let publication_lock = crate::runtime::MetadataPublicationLock::default();
        let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
        let captured =
            capture_selected_format10_closure(fs.as_ref(), &publication_lock, &deadline)?;
        let backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
        let cloud = CloudStorage::new(backend.clone(), "f10-closure-test".into());
        let publication_id = uuid::Uuid::new_v4();

        // Act
        let (horizon, objects) = publish_format10_closure(
            &cloud,
            &deadline,
            publication_id,
            &captured.bodies,
            &captured.reservation,
        )?;

        // Assert
        assert_eq!(horizon, 0);
        assert_eq!(horizon, captured.horizon);
        assert_eq!(objects.len(), captured.bodies.len());
        assert!(objects.iter().all(|object| {
            object.object_key
                == format!("metadata/generations/{publication_id}/{}", object.file_name)
        }));
        assert!(objects
            .iter()
            .any(|object| object.file_name == "metadata/HEAD"));
        assert!(!objects
            .iter()
            .any(|object| object.file_name == "manifest.snapshot.json"));
        let generation = crate::lease::CloudMetadataGeneration {
            id: SharedGenerationId::FIRST,
            manifest_horizon: horizon,
            objects,
        };
        crate::lease::cloud::validate_metadata_generation(&generation)
            .expect("strictly valid FORMAT 10 cloud generation");
        for object in generation.objects {
            let bytes = BlockingCloud::new(&cloud, &deadline)
                .get_optional(&object.object_key)?
                .expect("published immutable body");
            assert_eq!(bytes, captured.bodies[&object.file_name]);
        }
        Ok(())
    }
}
