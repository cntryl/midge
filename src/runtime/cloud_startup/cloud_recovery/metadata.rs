use super::{BlockingCloudIo, CloudStartupRecovery, MidgeError, MidgeResult, RecoveryPolicy};
use crate::io::FsError;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

impl CloudStartupRecovery {
    pub(crate) fn reject_legacy_cloud_metadata_without_generation(
        cloud: &crate::storage::cloud::CloudStorage,
    ) -> MidgeResult<()> {
        for file_name in crate::metadata::files::CLOUD_MIRRORED
            .iter()
            .copied()
            .chain(std::iter::once(crate::metadata::files::MANIFEST))
        {
            let key = crate::cloud_layout::CloudObjectLayout::metadata_key(file_name);
            if BlockingCloudIo::new(cloud).head_optional(&key)?.is_some() {
                return Err(MidgeError::RecoveryFailed(format!(
                    "cloud metadata '{key}' has no committed lease generation; offline migration is required"
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn read_committed_cloud_metadata(
        cloud: &crate::storage::cloud::CloudStorage,
        generation: &crate::lease::CloudMetadataGeneration,
    ) -> MidgeResult<BTreeMap<String, Vec<u8>>> {
        let mut objects = BTreeMap::new();
        for object in &generation.objects {
            if !crate::metadata::files::CLOUD_MIRRORED.contains(&object.file_name.as_str()) {
                return Err(MidgeError::RecoveryFailed(format!(
                    "committed cloud metadata has an unknown file '{}'",
                    object.file_name
                )));
            }
            let Some(relative) = object.object_key.strip_prefix("metadata/generations/") else {
                return Err(MidgeError::RecoveryFailed(format!(
                    "committed cloud metadata '{}' has an invalid generation key",
                    object.file_name
                )));
            };
            let Some((generation_id, name)) = relative.split_once('/') else {
                return Err(MidgeError::RecoveryFailed(format!(
                    "committed cloud metadata '{}' has an invalid generation key",
                    object.file_name
                )));
            };
            if uuid::Uuid::parse_str(generation_id).is_err() || name != object.file_name {
                return Err(MidgeError::RecoveryFailed(format!(
                    "committed cloud metadata '{}' has an invalid generation key",
                    object.file_name
                )));
            }
            if objects.contains_key(&object.file_name) {
                return Err(MidgeError::RecoveryFailed(
                    "committed cloud metadata generation has duplicate file identities".into(),
                ));
            }
            let data = BlockingCloudIo::new(cloud)
                .get_optional(&object.object_key)
                .map_err(|error| {
                    MidgeError::RecoveryFailed(format!(
                        "failed to read committed cloud metadata '{}': {error}",
                        object.file_name
                    ))
                })?
                .ok_or_else(|| {
                    MidgeError::RecoveryFailed(format!(
                        "committed cloud metadata '{}' is missing",
                        object.file_name
                    ))
                })?;
            let len = u64::try_from(data.len()).map_err(|error| {
                MidgeError::RecoveryFailed(format!(
                    "committed cloud metadata '{}' length is invalid: {error}",
                    object.file_name
                ))
            })?;
            if len != object.len || crc32c::crc32c(&data) != object.crc32c {
                return Err(MidgeError::RecoveryFailed(format!(
                    "committed cloud metadata '{}' does not match its length and checksum",
                    object.file_name
                )));
            }
            if let Some(sequence) =
                crate::metadata::files::manifest_sequence(&object.file_name, &data)?
            {
                if sequence > generation.manifest_sequence {
                    return Err(MidgeError::RecoveryFailed(format!(
                        "committed cloud manifest sequence {sequence} exceeds generation sequence {}",
                        generation.manifest_sequence
                    )));
                }
            }
            objects.insert(object.file_name.clone(), data);
        }
        for file_name in [
            crate::metadata::files::FORMAT,
            crate::metadata::files::MANIFEST_SNAPSHOT,
        ] {
            if !objects.contains_key(file_name) {
                return Err(MidgeError::RecoveryFailed(format!(
                    "committed cloud metadata generation is missing '{file_name}'"
                )));
            }
        }
        Ok(objects)
    }

    pub(crate) fn load_local_manifest_for_cloud_metadata_mirror(
        db_path: &Path,
    ) -> MidgeResult<crate::metadata::Manifest> {
        let fs: Arc<dyn crate::io::traits::Fs> =
            Arc::new(crate::io::RealFs::new(db_path).map_err(FsError::into_midge)?);
        crate::metadata::ManifestPersistence::load_with_fs_and_policy(&fs, RecoveryPolicy::Strict)
            .map_err(MidgeError::Internal)
    }
}
