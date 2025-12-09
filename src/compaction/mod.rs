pub mod executor;
pub mod merge;
pub mod planner;
pub mod strategy;

pub use executor::CompactionVersion;
pub use merge::MergeIterator;
pub use planner::{CompactionLog, CompactionTask};
pub use strategy::{CompactionPlan, Compactor, LeveledCompactionConfig};

use crate::common::MidgeResult;
use std::path::Path;

/// Execute a compaction plan: merge input SSTs into output SST(s)
pub fn execute_compaction(
    plan: &CompactionPlan,
    sst_factory: &dyn crate::sst::SstFactory,
    output_dir: &Path,
) -> MidgeResult<Vec<String>> {
    // Collect versions from all input files
    let versions = executor::collect_versions(sst_factory, &plan.input_files)?;

    if versions.is_empty() {
        return Ok(Vec::new());
    }

    // Deduplicate and keep only latest versions
    let deduplicated = executor::deduplicate_versions(&versions);

    // Filter out tombstones for final output
    let final_versions = executor::filter_tombstones(&deduplicated);

    // Write to output SST file
    let output_filename = format!("sst_L{}_cf{}_compaction.sst", plan.target_level, plan.cf_id);
    let output_path = output_dir.join(&output_filename);

    executor::write_versions_to_sst(
        sst_factory,
        output_path.to_str().unwrap_or("output.sst"),
        &final_versions,
    )?;

    Ok(vec![output_filename])
}
