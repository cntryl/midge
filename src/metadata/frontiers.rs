//! Persisted, per-family flush authority. Dropped ids are retained forever.

use crate::common::{MidgeError, MidgeResult};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FamilyFrontier {
    Live { through_sequence: u64 },
    Dropped,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FlushFrontiers {
    through: BTreeMap<u32, u64>,
}

impl Default for FlushFrontiers {
    fn default() -> Self {
        Self {
            through: BTreeMap::from([(0, 0)]),
        }
    }
}

fn corruption(message: impl Into<String>) -> MidgeError {
    MidgeError::Corruption(message.into())
}

impl FlushFrontiers {
    /// Look up a family's persisted flush state.
    ///
    /// # Errors
    /// Returns corruption when the family has no persisted authority.
    pub fn get(&self, cf_id: u32) -> MidgeResult<FamilyFrontier> {
        self.through
            .get(&cf_id)
            .copied()
            .map(|sequence| {
                if sequence == u64::MAX {
                    FamilyFrontier::Dropped
                } else {
                    FamilyFrontier::Live {
                        through_sequence: sequence,
                    }
                }
            })
            .ok_or_else(|| corruption(format!("flush frontier is missing for family {cf_id}")))
    }

    pub fn iter(&self) -> impl Iterator<Item = (u32, FamilyFrontier)> + '_ {
        self.through.iter().map(|(&id, &sequence)| {
            (
                id,
                if sequence == u64::MAX {
                    FamilyFrontier::Dropped
                } else {
                    FamilyFrontier::Live {
                        through_sequence: sequence,
                    }
                },
            )
        })
    }

    /// Check that a proposed publication retains all preceding authority.
    ///
    /// # Errors
    /// Returns corruption if a prior family disappears, regresses or revives.
    pub fn dominates(&self, previous: &Self) -> MidgeResult<()> {
        for (id, old) in previous.iter() {
            match (old, self.get(id)?) {
                (
                    FamilyFrontier::Dropped | FamilyFrontier::Live { .. },
                    FamilyFrontier::Dropped,
                ) => {}
                (
                    FamilyFrontier::Live {
                        through_sequence: old,
                    },
                    FamilyFrontier::Live {
                        through_sequence: new,
                    },
                ) if new >= old => {}
                _ => {
                    return Err(corruption(format!(
                        "flush frontier regressed for family {id}"
                    )))
                }
            }
        }
        Ok(())
    }

    pub(super) fn set_validated(&mut self, cf_id: u32, value: FamilyFrontier) {
        self.through.insert(
            cf_id,
            match value {
                FamilyFrontier::Live { through_sequence } => through_sequence,
                FamilyFrontier::Dropped => u64::MAX,
            },
        );
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FrontierRow {
    cf_id: u32,
    through_sequence: u64,
}

impl Serialize for FlushFrontiers {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.through
            .iter()
            .map(|(&cf_id, &through_sequence)| FrontierRow {
                cf_id,
                through_sequence,
            })
            .collect::<Vec<_>>()
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for FlushFrontiers {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let rows = Vec::<FrontierRow>::deserialize(deserializer)?;
        if rows
            .first()
            .is_none_or(|row| row.cf_id != 0 || row.through_sequence == u64::MAX)
            || rows.windows(2).any(|pair| pair[0].cf_id >= pair[1].cf_id)
        {
            return Err(serde::de::Error::custom(
                "flush frontiers require live default0 and strictly increasing family ids",
            ));
        }
        Ok(Self {
            through: rows
                .into_iter()
                .map(|row| (row.cf_id, row.through_sequence))
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_start_default_family_at_zero_when_frontiers_are_new() {
        // Arrange
        let frontiers = FlushFrontiers::default();

        // Act
        let default = frontiers.get(0).unwrap();
        let unknown = frontiers.get(1);

        // Assert
        assert_eq!(
            default,
            FamilyFrontier::Live {
                through_sequence: 0
            }
        );
        assert!(unknown.is_err());
    }

    #[test]
    fn should_reject_noncanonical_rows_when_decoding_frontiers() {
        // Arrange
        for encoded in [
            "[]", "[{\"cf_id\":1,\"through_sequence\":0}]",
            "[{\"cf_id\":0,\"through_sequence\":18446744073709551615}]",
            "[{\"cf_id\":0,\"through_sequence\":0},{\"cf_id\":0,\"through_sequence\":1}]",
            "[{\"cf_id\":0,\"through_sequence\":0},{\"cf_id\":2,\"through_sequence\":1},{\"cf_id\":1,\"through_sequence\":1}]",
        ] {
            // Act
            let decoded = serde_json::from_str::<FlushFrontiers>(encoded);
            // Assert
            assert!(decoded.is_err(), "{encoded}");
        }
    }

    #[test]
    fn should_refuse_nonmonotonic_authority_when_checking_dominance() {
        // Arrange
        let mut previous = FlushFrontiers::default();
        previous.set_validated(1, FamilyFrontier::Dropped);
        previous.set_validated(
            0,
            FamilyFrontier::Live {
                through_sequence: 7,
            },
        );
        let mut candidate = previous.clone();

        candidate.set_validated(
            0,
            FamilyFrontier::Live {
                through_sequence: 6,
            },
        );
        let regressed = candidate.clone();
        candidate.set_validated(
            0,
            FamilyFrontier::Live {
                through_sequence: 7,
            },
        );
        candidate.set_validated(
            1,
            FamilyFrontier::Live {
                through_sequence: 0,
            },
        );

        // Act
        let regression = regressed.dominates(&previous);
        let revival = candidate.dominates(&previous);
        let unchanged = previous.dominates(&previous);

        // Assert
        assert!(regression.is_err());
        assert!(revival.is_err());
        assert!(unchanged.is_ok());
    }
}
