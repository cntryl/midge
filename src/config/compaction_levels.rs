use cntryl_midge_core::common::{MidgeError, MidgeResult};
use crate::compaction::LeveledCompactionConfig;

/// Leveled-compaction level sizing. Defaults match the engine's built-in
/// leveled layout: 40 MiB at L1, a 10x multiplier, and seven levels.
///
/// Level `n >= 2` targets `l1_target_bytes * level_multiplier^(n - 1)`. A
/// store that already holds SSTs at level `max_levels` or deeper refuses to
/// open with a smaller `max_levels`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionLevelConfig {
    /// Target size of level 1 in bytes. Must be positive.
    pub l1_target_bytes: u64,
    /// Size ratio between adjacent levels, within `2..=100`.
    pub level_multiplier: u64,
    /// Number of levels including L0, within `3..=7`.
    pub max_levels: usize,
    /// Soft maximum bytes of the level two below an output that one compaction
    /// output may overlap before it is cut. Zero disables output cuts and
    /// restores legacy source fragmentation.
    pub max_grandparent_overlap_bytes: u64,
}

impl Default for CompactionLevelConfig {
    fn default() -> Self {
        let leveled = LeveledCompactionConfig::default();
        Self {
            l1_target_bytes: leveled.l1_target_size,
            level_multiplier: leveled.level_multiplier,
            max_levels: leveled.max_levels,
            max_grandparent_overlap_bytes: leveled.max_grandparent_overlap_bytes,
        }
    }
}

impl CompactionLevelConfig {
    const MULTIPLIER_RANGE: std::ops::RangeInclusive<u64> = 2..=100;
    const LEVELS_RANGE: std::ops::RangeInclusive<usize> = 3..=7;

    pub(crate) fn validate(&self) -> MidgeResult<()> {
        if self.l1_target_bytes == 0 {
            return Err(MidgeError::InvalidArgument(
                "compaction l1_target_bytes must be positive".into(),
            ));
        }
        if !Self::MULTIPLIER_RANGE.contains(&self.level_multiplier) {
            return Err(MidgeError::InvalidArgument(format!(
                "compaction level_multiplier must be within 2..=100, got {}",
                self.level_multiplier
            )));
        }
        if !Self::LEVELS_RANGE.contains(&self.max_levels) {
            return Err(MidgeError::InvalidArgument(format!(
                "compaction max_levels must be within 3..=7, got {}",
                self.max_levels
            )));
        }
        Ok(())
    }

    pub(crate) fn apply_to(&self, target: &mut LeveledCompactionConfig) {
        target.l1_target_size = self.l1_target_bytes;
        target.level_multiplier = self.level_multiplier;
        target.max_levels = self.max_levels;
        target.max_grandparent_overlap_bytes = self.max_grandparent_overlap_bytes;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_match_leveled_defaults_when_using_default_level_config() {
        // Arrange
        let leveled = LeveledCompactionConfig::default();
        let mut applied = LeveledCompactionConfig {
            l1_target_size: 1,
            level_multiplier: 2,
            max_levels: 3,
            max_grandparent_overlap_bytes: 1,
            ..LeveledCompactionConfig::default()
        };

        // Act
        CompactionLevelConfig::default().apply_to(&mut applied);

        // Assert
        assert_eq!(applied.l1_target_size, leveled.l1_target_size);
        assert_eq!(applied.level_multiplier, leveled.level_multiplier);
        assert_eq!(applied.max_levels, leveled.max_levels);
        assert_eq!(
            applied.max_grandparent_overlap_bytes,
            leveled.max_grandparent_overlap_bytes
        );
        assert!(CompactionLevelConfig::default().validate().is_ok());
    }
}
