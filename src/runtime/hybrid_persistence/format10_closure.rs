//! Immutable publication of the exact local FORMAT 10 metadata closure.

use super::format10_admission::Format10AdmissionStore;
use cntryl_midge_core::common::resource_budget::ResourceReservation;
use cntryl_midge_core::common::{DeadlineScope, MidgeError, MidgeResult, OperationDeadline};
use cntryl_midge_durability::lease::CloudMetadataObject;
use cntryl_midge_storage::cloud::{BlockingCloud, CloudStorage};
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
    fs: &dyn cntryl_midge_core::io::Fs,
    publication_lock: &crate::runtime::MetadataPublicationLock,
    deadline: &OperationDeadline,
) -> MidgeResult<CapturedFormat10Closure> {
    let scope = DeadlineScope::new(*deadline);
    cntryl_midge_durability::metadata::journal::with_manifest_writer_view_lock_within(fs, &scope, || {
        capture_selected_under_writer_lock(fs, publication_lock, deadline)
    })
}

fn capture_selected_under_writer_lock(
    fs: &dyn cntryl_midge_core::io::Fs,
    publication_lock: &crate::runtime::MetadataPublicationLock,
    deadline: &OperationDeadline,
) -> MidgeResult<CapturedFormat10Closure> {
    let mut paths = vec![cntryl_midge_durability::metadata::files::FORMAT.to_owned()];
    paths.extend(
        cntryl_midge_durability::metadata::local_authority::LocalMetadataAuthority::selected_relative_paths_from(fs)?,
    );
    paths.sort();
    paths.dedup();

    let mut expected_lengths = BTreeMap::new();
    let mut total = 0_u64;
    for path in paths {
        DeadlineScope::new(*deadline).check("FORMAT 10 selected closure preflight")?;
        validate_relative_path(&path)?;
        let metadata = fs
            .metadata(&cntryl_midge_core::io::FsPath::new(&path))
            .map_err(cntryl_midge_core::io::FsError::into_midge)?;
        let maximum = if path == cntryl_midge_durability::metadata::local_authority::HEAD_PATH {
            cntryl_midge_durability::metadata::local_authority::MAX_HEAD_BYTES as u64
        } else {
            cntryl_midge_durability::metadata::local_authority::MAX_CLOSURE_BYTES
        };
        if metadata.is_dir || metadata.len > maximum {
            return Err(MidgeError::ResourceLimit(format!(
                "FORMAT 10 selected object '{path}' exceeds its byte bound"
            )));
        }
        total = total
            .checked_add(metadata.len)
            .filter(|length| *length <= cntryl_midge_durability::metadata::local_authority::MAX_CLOSURE_BYTES)
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
            .open_read(&cntryl_midge_core::io::FsPath::new(&path))
            .map_err(cntryl_midge_core::io::FsError::into_midge)?;
        let actual_len = file.len().map_err(cntryl_midge_core::io::FsError::into_midge)?;
        if actual_len != expected_len {
            return Err(MidgeError::Corruption(format!(
                "FORMAT 10 selected object '{path}' changed during capture"
            )));
        }
        let bytes = file
            .read_at(0, actual_len)
            .map_err(cntryl_midge_core::io::FsError::into_midge)?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != actual_len {
            return Err(MidgeError::Corruption(format!(
                "FORMAT 10 selected object '{path}' was short during capture"
            )));
        }
        bodies.insert(path, bytes.to_vec());
    }
    let horizon =
        cntryl_midge_durability::metadata::local_authority::LocalMetadataAuthority::validate_cloud_bodies(&bodies)?;
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
    writer_epoch: u64,
    bodies: &BTreeMap<String, Vec<u8>>,
    cut_proof: Option<&[u8]>,
    reservation: &Arc<ResourceReservation>,
) -> MidgeResult<(u64, Vec<CloudMetadataObject>)> {
    let horizon =
        cntryl_midge_durability::metadata::local_authority::LocalMetadataAuthority::validate_cloud_bodies(bodies)?;
    if bodies.is_empty() || bodies.len() > cntryl_midge_durability::metadata::local_authority::MAX_OBJECTS + 2 {
        return Err(MidgeError::ResourceLimit(
            "FORMAT 10 metadata closure exceeds its object bound".into(),
        ));
    }
    let total_bytes = bodies.values().try_fold(
        cut_proof.map_or(0, |proof| proof.len() as u64),
        |total, body| {
            total
                .checked_add(u64::try_from(body.len()).unwrap_or(u64::MAX))
                .filter(|next| *next <= cntryl_midge_durability::metadata::local_authority::MAX_CLOSURE_BYTES)
        },
    );
    if total_bytes.is_none() {
        return Err(MidgeError::ResourceLimit(
            "FORMAT 10 metadata closure exceeds its byte bound".into(),
        ));
    }

    let payloads = || {
        bodies
            .iter()
            .map(|(path, bytes)| (path.as_str(), bytes.as_slice()))
            .chain(
                cut_proof.map(|bytes| (cntryl_midge_durability::metadata::files::CLOUD_CUT_PROOF, bytes)),
            )
    };
    let objects = payloads()
        .map(|(relative_path, body)| {
            validate_relative_path(relative_path)?;
            Ok(CloudMetadataObject {
                file_name: relative_path.to_owned(),
                object_key: format!("metadata/generations/{publication_id}/{relative_path}"),
                len: body.len() as u64,
                crc32c: crc32c::crc32c(body),
            })
        })
        .collect::<MidgeResult<Vec<_>>>()?;
    let admission = Format10AdmissionStore::new(cloud, deadline);
    admission.reserve(publication_id, writer_epoch, &objects)?;

    let io = BlockingCloud::new(cloud, deadline).with_reservation(Arc::clone(reservation));
    for (object, (_, bytes)) in objects.iter().zip(payloads()) {
        io.put_immutable_terminal(&object.object_key, bytes)?;
    }
    admission.mark_uploaded(publication_id, &objects)?;
    Ok((horizon, objects))
}

/// The pointer has already settled. An uncertain admission update may retain
/// capacity, but must not turn a proven commit into an unacknowledged write.
pub(super) fn finish_format10_stage(
    cloud: &CloudStorage,
    deadline: &OperationDeadline,
    publication_id: Option<uuid::Uuid>,
) {
    let Some(publication_id) = publication_id else {
        return;
    };
    if let Err(error) = Format10AdmissionStore::new(cloud, deadline).finish(publication_id) {
        tracing::warn!(%error, %publication_id, "retaining unsettled FORMAT 10 stage reservation");
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use cntryl_midge_core::io::{Fs, MockFs, OpenOptions};
    use cntryl_midge_durability::lease::SharedGenerationId;
    use cntryl_midge_durability::metadata::{codec, Manifest};
    use cntryl_midge_storage::cloud::BlockingCloud;
    use std::time::Duration;

    #[test]
    fn should_publish_only_selected_format10_bodies_under_canonical_publication_keys(
    ) -> MidgeResult<()> {
        // Arrange
        let fs: Arc<dyn Fs> = Arc::new(MockFs::new());
        let manifest = codec::encode(codec::MANIFEST, &Manifest::default())?;
        cntryl_midge_durability::metadata::local_authority::LocalMetadataAuthority::initialize(
            Arc::clone(&fs),
            "selected-generation",
            &manifest,
            b"[]",
        )?;
        fs.open(
            &cntryl_midge_core::io::FsPath::new(cntryl_midge_durability::metadata::files::FORMAT),
            OpenOptions {
                mode: cntryl_midge_core::io::OpenMode::ReadWrite,
                create: true,
                create_new: true,
                truncate: false,
            },
        )
        .map_err(cntryl_midge_core::io::FsError::into_midge)?
        .append(bytes::Bytes::from_static(b"midge-format-version=10\n"))
        .map_err(cntryl_midge_core::io::FsError::into_midge)?;
        let publication_lock = crate::runtime::MetadataPublicationLock::default();
        let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
        let captured =
            capture_selected_format10_closure(fs.as_ref(), &publication_lock, &deadline)?;
        let backend = Arc::new(cntryl_midge_storage::cloud::MockCloudBackend::new());
        let cloud = CloudStorage::new(backend.clone(), "f10-closure-test".into());
        let publication_id = uuid::Uuid::new_v4();

        // Act
        let (horizon, objects) = publish_format10_closure(
            &cloud,
            &deadline,
            publication_id,
            1,
            &captured.bodies,
            None,
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
        let generation = cntryl_midge_durability::lease::CloudMetadataGeneration {
            id: SharedGenerationId::FIRST,
            manifest_horizon: horizon,
            objects,
        };
        cntryl_midge_durability::lease::cloud::validate_metadata_generation(&generation)
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
