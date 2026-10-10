//! Portable counterexamples and deterministic deletion shrinking.

use super::oracle::{FamilyObservation, Transaction};
use super::scheduler::{Action, TraceEvent};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const MODEL_VERSION: u32 = 1;

/// Evidence needed to reproduce a campaign's exact generated history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counterexample {
    pub model_version: u32,
    pub source_sha: String,
    pub seed: u64,
    pub transactions: Vec<Transaction>,
    pub actions: Vec<Action>,
    pub trace: Vec<TraceEvent>,
    pub observed: Vec<FamilyObservation>,
    pub resource_counters: BTreeMap<String, u64>,
}

impl Counterexample {
    /// Decode only the current model. Older traces cannot silently change meaning.
    ///
    /// # Errors
    /// Rejects malformed JSON or an unsupported model version.
    pub fn decode(json: &[u8]) -> Result<Self, String> {
        let value: Self = serde_json::from_slice(json).map_err(|error| error.to_string())?;
        if value.model_version != MODEL_VERSION {
            return Err(format!(
                "unsupported simulation model {}",
                value.model_version
            ));
        }
        Ok(value)
    }
}

/// Minimize a failing history by deleting chunks, then individual actions.
/// Invalid schedules must return false from `still_fails`; they are not evidence.
/// The predicate executes every candidate from a fresh device and scheduler.
#[must_use]
pub fn shrink<T: Clone>(history: &[T], mut still_fails: impl FnMut(&[T]) -> bool) -> Vec<T> {
    let mut result = history.to_vec();
    if !still_fails(&result) {
        return result;
    }
    let mut width = result.len().div_ceil(2).max(1);
    loop {
        let mut start = 0;
        while start < result.len() {
            let end = (start + width).min(result.len());
            let mut candidate = result[..start].to_vec();
            candidate.extend_from_slice(&result[end..]);
            if still_fails(&candidate) {
                result = candidate;
            } else {
                start = end;
            }
        }
        if width == 1 {
            break;
        }
        width = width.div_ceil(2);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_preserve_counterexample_metadata_when_round_tripping_trace() {
        // Arrange
        let example = Counterexample {
            model_version: MODEL_VERSION,
            source_sha: "qualified-source".into(),
            seed: 42,
            transactions: Vec::new(),
            actions: vec![Action::Effect(7), Action::LoseResponse(7)],
            trace: Vec::new(),
            observed: Vec::new(),
            resource_counters: BTreeMap::from([("wal_bytes".into(), 128)]),
        };
        // Act
        let encoded = serde_json::to_vec(&example).unwrap();
        let recovered = Counterexample::decode(&encoded).unwrap();
        // Assert
        assert_eq!(recovered, example);
        let mut unsupported = example;
        unsupported.model_version += 1;
        assert!(Counterexample::decode(&serde_json::to_vec(&unsupported).unwrap()).is_err());
    }

    #[test]
    fn should_remove_irrelevant_actions_when_shrinking_reproducible_failure() {
        // Arrange
        let actions = vec![1, 2, 3, 4, 5, 6, 7];
        // Act
        let reduced = shrink(&actions, |candidate| {
            candidate.contains(&2) && candidate.contains(&6)
        });
        // Assert
        assert_eq!(reduced, vec![2, 6]);
        assert_eq!(shrink(&actions, |_| false), actions);
    }
}
