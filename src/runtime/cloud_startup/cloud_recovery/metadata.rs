use super::{BlockingCloudIo, CloudStartupRecovery, MidgeError, MidgeResult, RecoveryPolicy};
use crate::common::DeadlineScope;
use crate::io::FsError;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

impl CloudStartupRecovery {
    pub(crate) fn reject_legacy_cloud_metadata_without_generation_within(
        cloud: &crate::storage::cloud::CloudStorage,
        scope: &DeadlineScope,
    ) -> MidgeResult<()> {
        for file_name in crate::metadata::files::CLOUD_MIRRORED
            .iter()
            .copied()
            .chain(std::iter::once(crate::metadata::files::MANIFEST))
        {
            scope.check("legacy cloud metadata inventory")?;
            let key = crate::cloud_layout::CloudObjectLayout::metadata_key(file_name);
            if BlockingCloudIo::within(cloud, scope)
                .head_optional(&key)?
                .is_some()
            {
                return Err(MidgeError::RecoveryFailed(format!(
                    "cloud metadata '{key}' has no committed lease generation; offline migration is required"
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn read_committed_cloud_metadata_within(
        cloud: &crate::storage::cloud::CloudStorage,
        generation: &crate::lease::CloudMetadataGeneration,
        scope: &DeadlineScope,
    ) -> MidgeResult<BTreeMap<String, Vec<u8>>> {
        let mut objects = BTreeMap::new();
        for object in &generation.objects {
            scope.check("committed cloud metadata object")?;
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
            let data = BlockingCloudIo::within(cloud, scope)
                .get_optional(&object.object_key)
                .map_err(|error| {
                    super::preserve_timeout(error, "failed to read committed cloud metadata")
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
            scope.check("committed cloud metadata validation")?;
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

    pub(crate) fn load_local_manifest_for_cloud_metadata_mirror_within(
        db_path: &Path,
        scope: &DeadlineScope,
    ) -> MidgeResult<crate::metadata::Manifest> {
        scope.check("local cloud mirror manifest")?;
        let fs: Arc<dyn crate::io::traits::Fs> =
            Arc::new(crate::io::RealFs::new(db_path).map_err(FsError::into_midge)?);
        let fs = crate::io::scope_fs(fs, scope.clone());
        crate::metadata::ManifestPersistence::load_with_fs_and_policy_within(
            &fs,
            RecoveryPolicy::Strict,
            scope,
        )
    }
}
