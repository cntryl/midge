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
    pub(crate) fn new(
        factory: &dyn SstFactory,
        output_dir: &Path,
        admitted_bytes: usize,
    ) -> MidgeResult<Self> {
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

impl Drop for RepairScratch {
    fn drop(&mut self) {
        if let Err(error) = self.cleanup() {
            tracing::warn!(%error, "retaining non-authoritative overlap-repair scratch");
        }
    }
}
