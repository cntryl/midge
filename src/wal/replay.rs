//! Pure admission of complete WAL units against durable family frontiers.

use crate::common::MidgeResult;
use crate::metadata::frontiers::{FamilyFrontier, FlushFrontiers};

#[derive(Clone, Copy, Default)]
pub(crate) enum ReplaySelection<'a> {
    #[default]
    All,
    Frontiers(&'a FlushFrontiers),
    ValidateOnly,
}

impl ReplaySelection<'_> {
    pub(crate) fn validate_family(self, cf_id: u32) -> MidgeResult<()> {
        if let Self::Frontiers(frontiers) = self {
            frontiers.get(cf_id)?;
        }
        Ok(())
    }

    pub(crate) fn should_apply(self, cf_id: u32, commit_sequence: u64) -> MidgeResult<bool> {
        match self {
            Self::All => Ok(true),
            Self::ValidateOnly => Ok(false),
            Self::Frontiers(frontiers) => Ok(match frontiers.get(cf_id)? {
                FamilyFrontier::Live { through_sequence } => commit_sequence > through_sequence,
                FamilyFrontier::Dropped => false,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frontiers() -> FlushFrontiers {
        serde_json::from_value(serde_json::json!([
            {"cf_id": 0, "through_sequence": 7},
            {"cf_id": 1, "through_sequence": u64::MAX},
            {"cf_id": 2, "through_sequence": 3}
        ]))
        .unwrap()
    }

    #[test]
    fn should_select_whole_unit_when_commit_exceeds_its_family_frontier() {
        // Arrange
        let authority = frontiers();
        let selection = ReplaySelection::Frontiers(&authority);
        // Act
        let decisions = [(0, 7), (0, 8), (2, 4), (2, 3)]
            .map(|(cf, sequence)| selection.should_apply(cf, sequence).unwrap());
        // Assert
        assert_eq!(decisions, [false, true, true, false]);
    }

    #[test]
    fn should_skip_dropped_family_when_replaying_any_commit() {
        // Arrange
        let authority = frontiers();
        let selection = ReplaySelection::Frontiers(&authority);
        // Act
        let decisions =
            [1, 8, u64::MAX].map(|sequence| selection.should_apply(1, sequence).unwrap());
        // Assert
        assert_eq!(decisions, [false; 3]);
    }

    #[test]
    fn should_fail_closed_when_replay_family_has_no_persisted_frontier() {
        // Arrange
        let authority = frontiers();
        // Act
        let result = ReplaySelection::Frontiers(&authority).should_apply(3, 9);
        // Assert
        assert!(matches!(
            result,
            Err(crate::common::MidgeError::Corruption(_))
        ));
    }
}
