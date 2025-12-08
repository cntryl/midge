//! Deterministic compaction planner for Phase 2.
//!
//! The planner is a pure function that analyzes the current manifest and produces
//! a deterministic sequence of `CompactionTask`s to execute. This enables:
//! - Reproducible compaction behavior across restarts
//! - Logging and replaying compaction decisions
//! - Deterministic testing of LSM behavior
//!
//! Key properties:
//! - Same manifest input always yields same plan output (determinism)
//! - Plans are ordered consistently (by level, then by key range)
//! - No randomness or hash-based ordering

use serde::{Deserialize, Serialize};
use std::time::SystemTime;

use crate::core::manifest::Manifest;
use crate::manifest::FileMeta;
use crate::error::MidgeResult;

use super::CompactionPlan;

/// Uniquely identifies a compaction task across restarts
pub type TaskId = u64;

/// A single deterministic compaction operation
/// Stores the plan details directly (serializable)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionTask {
    /// Unique task ID (monotonically increasing)
    pub task_id: TaskId,
    /// Source level
    pub source_level: u32,
    /// Target level
    pub target_level: u32,
    /// Column family ID
    pub cf_id: u32,
    /// Input file names
    pub input_files: Vec<String>,
    /// Output file names (populated after execution)
    pub output_files: Vec<String>,
    /// When this task was created
    pub created_at: SystemTime,
}

impl CompactionTask {
    pub fn new(task_id: TaskId, plan: &CompactionPlan) -> Self {
        Self {
            task_id,
            source_level: plan.source_level,
            target_level: plan.target_level,
            cf_id: plan.cf_id,
            input_files: plan.input_files.clone(),
            output_files: plan.output_files.clone(),
            created_at: SystemTime::now(),
        }
    }

    /// Serialize for durable log storage
    pub fn to_bytes(&self) -> MidgeResult<Vec<u8>> {
        serde_json::to_vec(self)
            .map_err(|e| crate::error::MidgeError::internal(format!("Failed to serialize CompactionTask: {}", e)))
    }

    /// Deserialize from durable log storage
    pub fn from_bytes(bytes: &[u8]) -> MidgeResult<Self> {
        serde_json::from_slice(bytes)
            .map_err(|e| crate::error::MidgeError::internal(format!("Failed to deserialize CompactionTask: {}", e)))
    }
}

/// Append-only log of compaction tasks executed
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CompactionLog {
    /// All tasks executed in order
    pub tasks: Vec<CompactionTask>,
    /// Next task ID to assign
    pub next_task_id: TaskId,
}

impl CompactionLog {
    pub fn new() -> Self {
        Self {
            tasks: Vec::new(),
            next_task_id: 1, // Task IDs start at 1 (0 is reserved)
        }
    }

    /// Add a task to the log (idempotent if task_id already exists)
    pub fn append(&mut self, task: CompactionTask) {
        // Only append if this task ID hasn't been seen before
        if !self.tasks.iter().any(|t| t.task_id == task.task_id) {
            self.next_task_id = self.next_task_id.max(task.task_id + 1);
            self.tasks.push(task);
        }
    }

    /// Get the next task ID to assign
    pub fn next_id(&mut self) -> TaskId {
        let id = self.next_task_id;
        self.next_task_id += 1;
        id
    }

    /// Serialize for durable storage
    pub fn to_bytes(&self) -> MidgeResult<Vec<u8>> {
        serde_json::to_vec(self)
            .map_err(|e| crate::error::MidgeError::internal(format!("Failed to serialize CompactionLog: {}", e)))
    }

    /// Deserialize from durable storage
    pub fn from_bytes(bytes: &[u8]) -> MidgeResult<Self> {
        serde_json::from_slice(bytes)
            .map_err(|e| crate::error::MidgeError::internal(format!("Failed to deserialize CompactionLog: {}", e)))
    }
}

/// Deterministic compaction planner
pub struct Planner {
    /// Maximum levels in the LSM tree
    max_levels: usize,
    /// L0 compaction threshold (bytes)
    l0_threshold_bytes: u64,
    /// Size multiplier between levels
    level_multiplier: f64,
}

impl Default for Planner {
    fn default() -> Self {
        Self {
            max_levels: 7,
            l0_threshold_bytes: 4 * 1024 * 1024, // 4MB
            level_multiplier: 10.0,
        }
    }
}

impl Planner {
    pub fn new() -> Self {
        Self::default()
    }

    /// Generate a deterministic compaction plan from the current manifest.
    ///
    /// # Determinism Contract
    /// Same manifest input always produces the same plan output.
    /// Plans are ordered consistently:
    /// 1. L0 compactions (if needed)
    /// 2. Level compactions (L1, L2, ..., L_max) in order
    /// 3. Files within each level sorted by smallest_key, then by sst_seq
    ///
    /// # Returns
    /// A list of `CompactionPlan`s ready for execution, or empty if no compaction needed.
    pub fn plan(&self, manifest: &Manifest) -> Vec<CompactionPlan> {
        if manifest.files.is_empty() {
            return Vec::new();
        }

        let mut plans = Vec::new();

        // Group files by column family
        let mut files_by_cf: std::collections::HashMap<u32, Vec<&FileMeta>> =
            std::collections::HashMap::new();

        for file in &manifest.files {
            files_by_cf.entry(file.cf_id).or_insert_with(Vec::new).push(file);
        }

        // Sort CFs by ID for deterministic ordering
        let mut cf_ids: Vec<u32> = files_by_cf.keys().copied().collect();
        cf_ids.sort_unstable();

        // Process each CF independently
        for cf_id in cf_ids {
            let cf_files = &files_by_cf[&cf_id];
            plans.extend(self.plan_for_cf(cf_id, cf_files));
        }

        plans
    }

    /// Generate plans for a single column family
    fn plan_for_cf(&self, cf_id: u32, files: &[&FileMeta]) -> Vec<CompactionPlan> {
        // Group files by level
        let mut levels: Vec<Vec<&FileMeta>> = vec![Vec::new(); self.max_levels];
        for file in files {
            if (file.level as usize) < self.max_levels {
                levels[file.level as usize].push(file);
            }
        }

        let mut plans = Vec::new();

        // Check L0 first (special case: overlapping files OK, but size matters)
        let l0_files = &levels[0];
        if !l0_files.is_empty() {
            let l0_size: u64 = l0_files.iter().map(|f| f.size_bytes).sum();
            if l0_size > self.l0_threshold_bytes || l0_files.len() >= 4 {
                plans.push(self.create_l0_compaction_plan(cf_id, l0_files));
            }
        }

        // Check Ln levels (L1 through L_max-1)
        for level in 1..self.max_levels - 1 {
            let level_size: u64 = levels[level]
                .iter()
                .map(|f| f.size_bytes)
                .sum();
            let target_size = self.compute_level_target(level);

            if level_size > target_size {
                // Create a compaction plan for this level
                plans.push(self.create_level_compaction_plan(cf_id, level, &levels));
            }
        }

        plans
    }

    /// Compute target size for a given level
    fn compute_level_target(&self, level: usize) -> u64 {
        let l1_target = 10 * 1024 * 1024; // 10MB for L1
        let multiplier = self.level_multiplier.powi((level - 1) as i32);
        (l1_target as f64 * multiplier).ceil() as u64
    }

    /// Create a deterministic L0 → L1 compaction plan
    fn create_l0_compaction_plan(&self, cf_id: u32, l0_files: &[&FileMeta]) -> CompactionPlan {
        // Sort L0 files deterministically: by sublevel (oldest first), then by smallest_key, then by sst_seq
        let mut sorted_files: Vec<&FileMeta> = l0_files.to_vec();
        sorted_files.sort_by(|a, b| {
            match a.sublevel.cmp(&b.sublevel) {
                std::cmp::Ordering::Equal => {
                    match (&a.smallest_key, &b.smallest_key) {
                        (Some(ak), Some(bk)) => match ak.cmp(bk) {
                            std::cmp::Ordering::Equal => a.sst_seq.cmp(&b.sst_seq),
                            other => other,
                        },
                        (Some(_), None) => std::cmp::Ordering::Less,
                        (None, Some(_)) => std::cmp::Ordering::Greater,
                        (None, None) => a.sst_seq.cmp(&b.sst_seq),
                    }
                }
                other => other,
            }
        });

        CompactionPlan {
            source_level: 0,
            target_level: 1,
            cf_id,
            input_files: sorted_files.iter().map(|f| f.name.clone()).collect(),
            output_files: Vec::new(), // Will be filled in after execution
        }
    }

    /// Create a deterministic Ln → L(n+1) compaction plan
    fn create_level_compaction_plan(
        &self,
        cf_id: u32,
        level: usize,
        levels: &[Vec<&FileMeta>],
    ) -> CompactionPlan {
        // For level > 0, files don't overlap, so just sort by smallest_key then sst_seq
        let mut sorted_files: Vec<&FileMeta> = levels[level].to_vec();
        sorted_files.sort_by(|a, b| {
            match (&a.smallest_key, &b.smallest_key) {
                (Some(ak), Some(bk)) => match ak.cmp(bk) {
                    std::cmp::Ordering::Equal => a.sst_seq.cmp(&b.sst_seq),
                    other => other,
                },
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => a.sst_seq.cmp(&b.sst_seq),
            }
        });

        CompactionPlan {
            source_level: level as u32,
            target_level: (level + 1) as u32,
            cf_id,
            input_files: sorted_files.iter().map(|f| f.name.clone()).collect(),
            output_files: Vec::new(), // Will be filled in after execution
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_return_empty_plan_given_empty_manifest() {
        let planner = Planner::new();
        let manifest = Manifest::default();
        let plans = planner.plan(&manifest);
        assert!(plans.is_empty());
    }

    #[test]
    fn should_be_deterministic_given_same_manifest() {
        let planner = Planner::new();

        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "sst_001.blob".to_string(),
            level: 0,
            size_bytes: 1024 * 1024,
            cf_id: 0,
            sst_seq: 1,
            smallest_key: Some(b"a".to_vec()),
            largest_key: Some(b"m".to_vec()),
            sublevel: 0,
            ..Default::default()
        });
        manifest.files.push(FileMeta {
            name: "sst_002.blob".to_string(),
            level: 0,
            size_bytes: 2 * 1024 * 1024,
            cf_id: 0,
            sst_seq: 2,
            smallest_key: Some(b"n".to_vec()),
            largest_key: Some(b"z".to_vec()),
            sublevel: 1,
            ..Default::default()
        });

        let plan1 = planner.plan(&manifest);
        let plan2 = planner.plan(&manifest);

        // Same manifest should always produce identical plans
        assert_eq!(plan1.len(), plan2.len());
        for (p1, p2) in plan1.iter().zip(plan2.iter()) {
            assert_eq!(p1.input_files, p2.input_files);
            assert_eq!(p1.source_level, p2.source_level);
            assert_eq!(p1.target_level, p2.target_level);
        }
    }

    #[test]
    fn should_not_plan_l0_compaction_when_below_threshold() {
        let planner = Planner::new();

        let mut manifest = Manifest::default();
        // Add a single small L0 file
        manifest.files.push(FileMeta {
            name: "sst_001.blob".to_string(),
            level: 0,
            size_bytes: 1024 * 1024, // 1MB, well below 4MB threshold
            cf_id: 0,
            sst_seq: 1,
            ..Default::default()
        });

        let plans = planner.plan(&manifest);
        assert!(plans.is_empty(), "Should not plan compaction when L0 is below threshold");
    }

    #[test]
    fn should_plan_l0_compaction_when_size_exceeds_threshold() {
        let planner = Planner::new();

        let mut manifest = Manifest::default();
        // Add L0 files totaling 5MB (exceeds 4MB threshold)
        for i in 1..=5 {
            manifest.files.push(FileMeta {
                name: format!("sst_{:03}.blob", i),
                level: 0,
                size_bytes: 1024 * 1024, // 1MB each = 5MB total
                cf_id: 0,
                sst_seq: i as u64,
                sublevel: (i - 1) as u32,
                smallest_key: Some(vec![i as u8]),
                largest_key: Some(vec![i as u8 + 1]),
                ..Default::default()
            });
        }

        let plans = planner.plan(&manifest);
        assert!(!plans.is_empty(), "Should plan L0 compaction when size exceeds threshold");
        assert_eq!(plans[0].source_level, 0);
        assert_eq!(plans[0].target_level, 1);
    }

    #[test]
    fn should_order_l0_files_by_sublevel_then_key() {
        let planner = Planner::new();

        let mut manifest = Manifest::default();
        // Add L0 files with specific sublevels and keys
        manifest.files.push(FileMeta {
            name: "sst_high_level.blob".to_string(),
            level: 0,
            size_bytes: 5 * 1024 * 1024,
            cf_id: 0,
            sst_seq: 3,
            sublevel: 5,
            smallest_key: Some(b"b".to_vec()),
            ..Default::default()
        });
        manifest.files.push(FileMeta {
            name: "sst_low_level.blob".to_string(),
            level: 0,
            size_bytes: 5 * 1024 * 1024,
            cf_id: 0,
            sst_seq: 1,
            sublevel: 0,
            smallest_key: Some(b"a".to_vec()),
            ..Default::default()
        });

        let plans = planner.plan(&manifest);
        assert!(!plans.is_empty());
        // Low sublevel (older) should come first
        assert_eq!(plans[0].input_files[0], "sst_low_level.blob");
        assert_eq!(plans[0].input_files[1], "sst_high_level.blob");
    }

    #[test]
    fn should_plan_multi_cf_compactions_in_cf_id_order() {
        let planner = Planner::new();

        let mut manifest = Manifest::default();
        // Add files for CF 1
        manifest.files.push(FileMeta {
            name: "sst_cf1_001.blob".to_string(),
            level: 0,
            size_bytes: 5 * 1024 * 1024,
            cf_id: 1,
            sst_seq: 1,
            sublevel: 0,
            ..Default::default()
        });
        // Add files for CF 0
        manifest.files.push(FileMeta {
            name: "sst_cf0_001.blob".to_string(),
            level: 0,
            size_bytes: 5 * 1024 * 1024,
            cf_id: 0,
            sst_seq: 1,
            sublevel: 0,
            ..Default::default()
        });

        let plans = planner.plan(&manifest);
        // CF 0 should be processed before CF 1 (by ID order)
        assert_eq!(plans[0].cf_id, 0);
        assert_eq!(plans[1].cf_id, 1);
    }
}
