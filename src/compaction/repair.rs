//! Bounded, non-authoritative raw runs for large same-level repair components.

use crate::common::{MidgeError, MidgeResult};
use crate::io::{Fs, FsError};
use crate::sst::traits::SstFactory;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(crate) struct RepairScratch {
    fs: Arc<dyn Fs>,
    directory: PathBuf,
    paths: HashMap<String, usize>,
    admitted_bytes: usize,
    used_bytes: usize,
}

impl RepairScratch {
    /// Admit repair scratch against a conservative snapshot of free space on
    /// the filesystem that owns the SST directory. One eighth remains a
    /// finite upper bound for this repair, leaving room for normal writes and
    /// final output publication. The snapshot is advisory; filesystem errors
    /// still abort repair while preserving the authoritative inputs.
    pub(crate) fn local_capacity(factory: &dyn SstFactory) -> MidgeResult<usize> {
        let fs = factory.output_fs();
        let addressing = fs.host_addressing().ok_or_else(|| {
            MidgeError::ResourceLimit(
                "overlap repair cannot identify local scratch capacity".into(),
            )
        })?;
        let disks = sysinfo::Disks::new_with_refreshed_list();
        let available = disks
            .iter()
            .filter_map(|disk| {
                canonical_mount_depth(addressing.root, disk.mount_point())
                    .map(|depth| (depth, disk.available_space()))
            })
            .max_by_key(|(depth, _)| *depth)
            .map(|(_, available)| available)
            .ok_or_else(|| {
                MidgeError::ResourceLimit(
                    "overlap repair cannot determine local scratch capacity".into(),
                )
            })?;
        admitted_local_capacity(available)
    }

    pub(crate) fn new(
        factory: &dyn SstFactory,
        output_dir: &Path,
        admitted_bytes: usize,
    ) -> MidgeResult<Self> {
        if admitted_bytes == 0 {
            return Err(MidgeError::ResourceLimit(
                "overlap repair has no admitted local scratch capacity".into(),
            ));
        }
        let fs = factory.output_fs();
        let directory = output_dir.join(".compaction-repair");
        let key = crate::sst::fs::fs_relative_sst_path(&fs, &directory)?;
        if fs.exists(&key).map_err(FsError::into_midge)?
            && !fs.list_dir(&key).map_err(FsError::into_midge)?.is_empty()
        {
            return Err(MidgeError::ResourceLimit(
                "stale overlap-repair scratch requires startup cleanup".into(),
            ));
        }
        fs.create_dir_all(&key).map_err(FsError::into_midge)?;
        Ok(Self {
            fs,
            directory,
            paths: HashMap::new(),
            admitted_bytes,
            used_bytes: 0,
        })
    }

    pub(crate) fn merge_runs(
        &mut self,
        factory: &dyn SstFactory,
        originals: &[String],
        fan_in: usize,
        budget: &crate::common::resource_budget::ResourceBudget,
        abort_check: Option<&dyn Fn() -> bool>,
    ) -> MidgeResult<Vec<String>> {
        if fan_in < 2 {
            return Err(MidgeError::ResourceLimit(
                "overlap repair requires at least two admitted merge streams".into(),
            ));
        }
        let mut runs = originals.to_vec();
        while runs.len() > fan_in {
            let mut next = Vec::with_capacity(runs.len().div_ceil(fan_in));
            for batch in runs.chunks(fan_in) {
                super::executor::ensure_compaction_not_aborted(abort_check)?;
                if batch.len() == 1 {
                    next.push(batch[0].clone());
                    continue;
                }
                let path = self
                    .directory
                    .join(format!("repair-{}.sst", uuid::Uuid::new_v4().simple()));
                let key = crate::sst::fs::fs_relative_sst_path(&self.fs, &path)?;
                // Record before writing so a failed finalization cannot leave
                // a newly created run outside the owned cleanup set.
                self.paths.insert(key.0.clone(), 0);
                crate::compaction::executor::merge_repair_inputs_to_run(
                    factory,
                    batch,
                    &path,
                    budget,
                    self.admitted_bytes.saturating_sub(self.used_bytes),
                    abort_check,
                )?;
                let size = self.fs.metadata(&key).map_err(FsError::into_midge)?.len;
                let size = usize::try_from(size).map_err(|_| {
                    MidgeError::ResourceLimit("overlap repair scratch size overflow".into())
                })?;
                self.used_bytes = self.used_bytes.saturating_add(size);
                if self.used_bytes > self.admitted_bytes {
                    return Err(MidgeError::ResourceLimit(
                        "overlap repair scratch exceeds admitted local capacity".into(),
                    ));
                }
                *self.paths.get_mut(&key.0).expect("recorded scratch run") = size;
                // Consumed scratch was never authoritative. Reclaim it only
                // after its replacement run is durable so the next batch can
                // reuse the admitted disk allowance.
                self.retire_consumed(batch)?;
                next.push(key.0);
            }
            runs = next;
        }
        Ok(runs)
    }

    fn retire_consumed(&mut self, inputs: &[String]) -> MidgeResult<()> {
        for key in inputs {
            let Some(size) = self.paths.get(key) else {
                continue;
            };
            self.fs
                .remove_file(&crate::io::FsPath::new(key))
                .map_err(FsError::into_midge)?;
            self.used_bytes = self.used_bytes.saturating_sub(*size);
            self.paths.remove(key);
        }
        Ok(())
    }

    pub(crate) fn cleanup(&mut self) -> MidgeResult<()> {
        let mut first_error = None;
        self.paths.retain(|key, _| {
            let result = self
                .fs
                .remove_file(&crate::io::FsPath::new(key))
                .map_err(FsError::into_midge);
            match result {
                Ok(()) | Err(MidgeError::NotFound) => false,
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                    true
                }
            }
        });
        first_error.map_or(Ok(()), Err)
    }
}

/// Compare mounts in the canonical host-path frame exposed by the filesystem.
/// Windows native drive mounts otherwise differ from verbatim canonical roots.
/// Inaccessible mounts cannot establish a safe capacity allowance.
fn canonical_mount_depth(root: &Path, mount: &Path) -> Option<usize> {
    let mount = std::fs::canonicalize(mount).ok()?;
    if !root.starts_with(&mount) {
        return None;
    }
    Some(mount.components().count())
}

fn admitted_local_capacity(available_bytes: u64) -> MidgeResult<usize> {
    let admitted = usize::try_from(available_bytes / 8).unwrap_or(usize::MAX);
    if admitted == 0 {
        return Err(MidgeError::ResourceLimit(
            "overlap repair has no admitted local scratch capacity".into(),
        ));
    }
    Ok(admitted)
}

impl Drop for RepairScratch {
    fn drop(&mut self) {
        if let Err(error) = self.cleanup() {
            tracing::warn!(%error, "retaining non-authoritative overlap-repair scratch");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_admit_only_one_eighth_of_reported_local_capacity() -> MidgeResult<()> {
        // Arrange
        let available_bytes = 8_000;

        // Act
        let admitted = admitted_local_capacity(available_bytes)?;

        // Assert
        assert_eq!(admitted, 1_000);
        Ok(())
    }

    #[test]
    fn should_reject_zero_local_scratch_capacity() {
        // Arrange
        let available_bytes = 7;

        // Act
        let result = admitted_local_capacity(available_bytes);

        // Assert
        assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
    }

    #[test]
    fn should_find_native_capacity_when_real_filesystem_root_is_canonical() -> MidgeResult<()> {
        // Arrange: real bytes and independently observed canonical native mounts.
        let directory = tempfile::tempdir()?;
        let fs: Arc<dyn Fs> =
            Arc::new(crate::io::RealFs::new(directory.path()).map_err(FsError::into_midge)?);
        let factory = crate::sst::FsSstFactoryIo::new(fs.clone(), 4096);
        let root = fs.host_addressing().expect("real host identity").root;
        let retained = directory.path().join("retained");
        std::fs::write(&retained, b"actual retained filesystem bytes")?;
        let disks = sysinfo::Disks::new_with_refreshed_list();
        let native_matches: Vec<_> = disks
            .iter()
            .filter_map(|disk| {
                let canonical = std::fs::canonicalize(disk.mount_point()).ok()?;
                root.strip_prefix(&canonical).ok()?;
                Some((canonical, disk.available_space()))
            })
            .collect();
        assert!(
            native_matches.iter().any(|(_, available)| *available >= 8),
            "real positive native capacity prerequisite: root={root:?}, mounts={native_matches:?}"
        );

        // Act: run production selection and its conservative admission policy.
        let admitted = RepairScratch::local_capacity(&factory)?;

        // Assert: an actual matching volume yields capacity without changing bytes.
        assert_ne!(admitted, 0);
        assert_eq!(
            std::fs::read(retained)?.as_slice(),
            b"actual retained filesystem bytes"
        );
        Ok(())
    }

    #[test]
    fn should_refuse_capacity_when_filesystem_has_no_host_identity() {
        // Arrange: this filesystem deliberately exposes no host path capability.
        let factory = crate::sst::FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096);

        // Act: native disk enumeration cannot establish this backend's identity.
        let result = RepairScratch::local_capacity(&factory);

        // Assert: preserve the exact conservative missing-identity classification.
        assert!(matches!(result, Err(MidgeError::ResourceLimit(message))
            if message == "overlap repair cannot identify local scratch capacity"));
    }

    #[test]
    fn should_reject_unusable_mount_when_local_root_is_real() -> MidgeResult<()> {
        // Arrange: actual distinct directories and one path that does not exist.
        let root_directory = tempfile::tempdir()?;
        let unrelated = tempfile::tempdir()?;
        let root = std::fs::canonicalize(root_directory.path())?;
        let absent = root_directory.path().join("absent-native-mount");
        assert!(!absent.exists());

        // Act: canonicalization failure and an unrelated identity are both refused.
        let absent_depth = canonical_mount_depth(&root, &absent);
        let unrelated_depth = canonical_mount_depth(&root, unrelated.path());
        let matching_depth = canonical_mount_depth(&root, root_directory.path());

        // Assert: only the actual canonical equivalent can establish membership.
        assert_eq!(absent_depth, None);
        assert_eq!(unrelated_depth, None);
        assert_eq!(matching_depth, Some(root.components().count()));
        Ok(())
    }
}
