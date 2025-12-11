pub mod executor;
pub mod merge;
pub mod planner;
pub mod strategy;

pub use executor::CompactionVersion;
pub use merge::MergeIterator;
pub use planner::{CompactionLog, CompactionTask};
pub use strategy::{CompactionPlan, Compactor, LeveledCompactionConfig};

use crate::common::{MidgeError, MidgeResult};
use std::path::{Path, PathBuf};

/// Executes a compaction plan by streaming merged key/value pairs into one or more
/// output SST files. This function performs:
///   1. Input SST discovery
///   2. Streaming merge across all inputs (sorted, deduped)
///   3. Tombstone filtering + merge operand resolution
///   4. Delegation to `SstFactory` to create new SST files
///
/// This is intentionally thin — the heavy lifting is inside `executor::*`
/// which performs the actual merge and write pipeline.
///
/// **Important**: `output_dir` must be the CF-specific directory (e.g., `cf_00/`),
/// not the DB root. Output filename is sequence-only: `{seq:08}.sst`.
pub fn execute_compaction(
    plan: &CompactionPlan,
    sst_factory: &dyn crate::sst::SstFactory,
    output_dir: &Path,
) -> MidgeResult<Vec<String>> {
    // --- 1. Collect versions from all input files ---------------------------
    //
    // For now, we load versions into memory. Future: streaming merge iterator.
    let versions = executor::collect_versions(sst_factory, &plan.input_files)?;

    if versions.is_empty() {
        return Ok(Vec::new());
    }

    // --- 2. Deduplicate and keep only latest versions -----------------------
    let deduplicated = executor::deduplicate_versions(&versions);

    // --- 3. Filter out tombstones for final output --------------------------
    let final_versions = executor::filter_tombstones(&deduplicated);

    // --- 4. Prepare output file path ----------------------------------------
    let output_file = output_filename(plan, output_dir);
    let output_file_str = output_file.to_str().ok_or(MidgeError::InvalidPath)?;

    // --- 5. Write merged versions to SST ------------------------------------
    executor::write_versions_to_sst(sst_factory, output_file_str, &final_versions)?;

    Ok(vec![output_file
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("output.sst")
        .to_owned()])
}

/// Construct the output filename for a completed compaction.
/// This is stable and predictable for crash recovery and manifest logging.
///
/// Follows LSM-tree industry standard: CF → directory, sequence → filename.
/// The directory is assumed to already be CF-specific (e.g., `cf_00/`).
fn output_filename(plan: &CompactionPlan, cf_dir: &Path) -> PathBuf {
    // File naming rules (aligned with RocksDB, TiKV, Pebble):
    // - filename encodes only ordering information (sequence)
    // - zero-padded to maintain lexicographic sort
    // - CF identity is encoded in the directory structure, not the filename
    let name = format!("{:08}.sst", plan.output_seq);
    cf_dir.join(name)
}
