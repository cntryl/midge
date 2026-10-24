use crate::compaction::{strategy::CompactionPurpose, CompactionPlan};
use crate::metadata::FileMeta;

pub(super) fn eligible(plan: &CompactionPlan, files: &[FileMeta]) -> bool {
    if !matches!(plan.purpose, CompactionPurpose::Merge)
        || !plan.target_files.is_empty()
        || plan.source_files.is_empty()
        || plan.input_files.len() != plan.source_files.len()
        || !plan
            .input_files
            .iter()
            .all(|name| plan.source_files.contains(name))
    {
        return false;
    }
    let captured: Vec<_> = files
        .iter()
        .filter(|file| plan.source_files.contains(&file.name))
        .cloned()
        .collect();
    if captured.len() != plan.source_files.len()
        || captured.iter().any(|file| {
            file.level != plan.source_level
                || file.cf_id != plan.cf_id
                || file.size_bytes > plan.target_sst_size as u64
                || file
                    .smallest_key
                    .as_deref()
                    .zip(file.largest_key.as_deref())
                    .is_none_or(|(lower, upper)| {
                        !plan.grandparent_overlap.permits_whole_file(lower, upper)
                    })
        })
    {
        return false;
    }
    matches!(
        crate::metadata::manifest::validate_sst_move(files, &captured, plan.target_level),
        Ok(false)
    )
}
