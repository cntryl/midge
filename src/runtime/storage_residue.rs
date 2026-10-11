//! Runtime assessment of files that are present but not authoritative.

use crate::common::MidgeResult;
use crate::config::EngineHealth;
use crate::io::{Fs, FsError, FsPath};
use crate::metadata::Manifest;
use std::collections::HashSet;
use std::path::Path;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct StorageResidueAssessment {
    pub(crate) orphan_ssts: Vec<String>,
    /// Temporary residue inside `sst/` that is never authoritative.
    pub(crate) sst_temp_files: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct HealthInputs {
    pub(crate) opened_in_salvage_mode: bool,
    pub(crate) write_stalled: bool,
    pub(crate) persistence_anomaly_detected: bool,
    pub(crate) pending_intents: usize,
    pub(crate) orphan_ssts: usize,
}

impl StorageResidueAssessment {
    /// Inventory the SST directory of `fs` against the manifest. `check` runs
    /// before each entry, so a startup deadline can stop a long listing. A
    /// directory that cannot be listed holds no known residue, except when
    /// the listing itself ran out of time.
    pub(crate) fn scan(
        fs: &dyn Fs,
        manifest: &Manifest,
        mut check: impl FnMut(&'static str) -> MidgeResult<()>,
    ) -> MidgeResult<Self> {
        let entries = match fs.list_dir(&FsPath::new(crate::sst::fs::SST_DIR)) {
            Ok(entries) => entries,
            Err(error @ FsError::Timeout(_)) => return Err(error.into_midge()),
            Err(_) => return Ok(Self::default()),
        };
        let published: HashSet<_> = manifest
            .files
            .iter()
            .map(|file| file.name.as_str())
            .collect();
        let mut residue = Self::default();
        for entry in entries {
            check("storage residue inventory entry")?;
            let name = entry.name;
            let path = Path::new(&name);
            if name.ends_with(".sst.tmp")
                || path
                    .extension()
                    .is_some_and(|extension| extension.eq_ignore_ascii_case("tmp"))
            {
                residue.sst_temp_files.push(name);
            } else if path.extension().is_some_and(|extension| extension == "sst")
                && !published.contains(name.as_str())
            {
                residue.orphan_ssts.push(name);
            }
        }
        residue.orphan_ssts.sort();
        residue.sst_temp_files.sort();
        Ok(residue)
    }
}

pub(crate) fn classify_engine_health(inputs: HealthInputs) -> EngineHealth {
    if inputs.opened_in_salvage_mode {
        EngineHealth::SalvageMode
    } else if inputs.write_stalled {
        EngineHealth::WriteStalled
    } else if inputs.persistence_anomaly_detected
        || inputs.pending_intents > 0
        || inputs.orphan_ssts > 0
    {
        EngineHealth::Degraded
    } else {
        EngineHealth::Healthy
    }
}
