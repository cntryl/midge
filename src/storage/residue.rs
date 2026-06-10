use crate::engine::EngineHealth;
use crate::metadata::Manifest;
use std::collections::HashSet;
use std::path::Path;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct StorageResidueAssessment {
    pub orphan_ssts: Vec<String>,
    /// Temporary residue inside `sst/` that is never authoritative.
    pub sst_temp_files: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct HealthInputs {
    pub opened_in_salvage_mode: bool,
    pub write_stalled: bool,
    pub persistence_anomaly_detected: bool,
    pub pending_intents: usize,
    pub orphan_ssts: usize,
}

impl StorageResidueAssessment {
    pub(crate) fn scan_sst_dir(db_path: &Path, manifest: &Manifest) -> Self {
        let sst_dir = db_path.join("sst");
        let published: HashSet<_> = manifest
            .files
            .iter()
            .map(|file| file.name.as_str())
            .collect();
        let mut orphan_ssts = Vec::new();
        let mut sst_temp_files = Vec::new();

        if let Ok(entries) = std::fs::read_dir(sst_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                    continue;
                };

                if name.ends_with(".sst.tmp") || name.ends_with(".tmp") {
                    sst_temp_files.push(name.to_string());
                    continue;
                }

                if path.extension().and_then(|ext| ext.to_str()) == Some("sst")
                    && !published.contains(name)
                {
                    orphan_ssts.push(name.to_string());
                }
            }
        }

        orphan_ssts.sort();
        sst_temp_files.sort();

        Self {
            orphan_ssts,
            sst_temp_files,
        }
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
